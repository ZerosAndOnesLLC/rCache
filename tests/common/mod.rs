//! Shared test harness: spawn rcache on an ephemeral port and clean up.
//!
//! Each test gets its own server process and port so test isolation is
//! perfect and tests can run in parallel.

use std::io::{BufRead, BufReader};
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

pub struct Server {
    child: Child,
    pub port: u16,
}

impl Server {
    pub fn spawn() -> Self {
        Self::spawn_with_args(&[])
    }

    pub fn spawn_with_args(extra: &[&str]) -> Self {
        let port = pick_port();
        let bin = env!("CARGO_BIN_EXE_rcache");

        let mut cmd = Command::new(bin);
        cmd.arg("--port")
            .arg(port.to_string())
            .arg("--bind")
            .arg("127.0.0.1");
        for a in extra {
            cmd.arg(a);
        }
        let mut child = cmd
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to spawn rcache binary");

        // The child's stderr must be drained continuously, otherwise a full
        // pipe will block the server. Spawn a background reader that signals
        // when "Listening on" appears and then keeps the pipe drained for the
        // lifetime of the child.
        let stderr = child.stderr.take().expect("piped stderr");
        let (tx, rx) = mpsc::channel::<()>();
        std::thread::spawn(move || {
            let reader = BufReader::new(stderr);
            let mut signalled = false;
            for line in reader.lines().map_while(Result::ok) {
                if !signalled && line.contains("Listening on") {
                    let _ = tx.send(());
                    signalled = true;
                }
                // keep consuming after signalling so the child does not block
            }
        });

        // Wait for the readiness signal, with a connect-poll fallback.
        let ready = rx.recv_timeout(Duration::from_secs(10)).is_ok();
        if !ready {
            let until = Instant::now() + Duration::from_secs(5);
            let mut connected = false;
            while Instant::now() < until {
                if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
                    connected = true;
                    break;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            assert!(connected, "rcache did not start listening on port {port}");
        }

        Server { child, port }
    }

    pub fn url(&self) -> String {
        format!("redis://127.0.0.1:{}/", self.port)
    }

    pub fn client(&self) -> redis::Client {
        redis::Client::open(self.url()).expect("client open")
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn pick_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("ephemeral bind");
    let port = listener.local_addr().expect("local_addr").port();
    drop(listener);
    port
}
