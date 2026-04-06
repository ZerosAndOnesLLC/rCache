mod connection;

use std::sync::Arc;
use std::time::Instant;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

use crate::config::Config;
use crate::command::CommandRegistry;
use crate::storage::Store;
use crate::storage::ExpirationManager;

pub struct SharedState {
    pub store: Mutex<Store>,
    pub config: Config,
    pub registry: CommandRegistry,
    pub start_time: Instant,
}

pub struct Server {
    config: Config,
}

impl Server {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    pub async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        let addr = format!("{}:{}", self.config.bind, self.config.port);
        let listener = TcpListener::bind(&addr).await?;
        tracing::info!("Listening on {}", addr);

        let state = Arc::new(SharedState {
            store: Mutex::new(Store::new(self.config.databases)),
            config: self.config.clone(),
            registry: CommandRegistry::new(),
            start_time: Instant::now(),
        });

        // Start active expiration task
        let expire_state = Arc::clone(&state);
        let hz = self.config.hz;
        tokio::spawn(async move {
            let manager = ExpirationManager::new(20);
            let interval = std::time::Duration::from_millis(1000 / hz);
            loop {
                tokio::time::sleep(interval).await;
                let mut store = expire_state.store.lock().await;
                manager.run_cycle(&mut store);
            }
        });

        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.config.maxclients));

        loop {
            let (socket, addr) = listener.accept().await?;
            let state = Arc::clone(&state);
            let permit = match semaphore.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    tracing::warn!("Max clients reached, rejecting {}", addr);
                    continue;
                }
            };

            tokio::spawn(async move {
                tracing::debug!("New connection from {}", addr);
                let mut conn = connection::Connection::new(socket, state);
                if let Err(e) = conn.handle().await {
                    tracing::debug!("Connection {} error: {}", addr, e);
                }
                drop(permit);
                tracing::debug!("Connection {} closed", addr);
            });
        }
    }
}
