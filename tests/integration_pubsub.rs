mod common;

use common::Server;
use std::sync::mpsc;
use std::time::Duration;

#[test]
fn subscribe_then_receive_published_message() {
    let s = Server::spawn();
    let port = s.port;

    // Subscriber runs on its own thread because get_message() blocks. We hand
    // the received payload back over a channel so we can apply a hard timeout.
    let (tx, rx) = mpsc::channel::<String>();
    std::thread::spawn(move || {
        let client = redis::Client::open(format!("redis://127.0.0.1:{port}/")).unwrap();
        let mut conn = client.get_connection().unwrap();
        let mut pubsub = conn.as_pubsub();
        pubsub.subscribe("ch1").unwrap();
        let msg = pubsub.get_message().unwrap();
        let payload: String = msg.get_payload().unwrap();
        let _ = tx.send(payload);
    });

    // Give the subscriber time to register before publishing.
    std::thread::sleep(Duration::from_millis(200));

    let mut pub_conn = s.client().get_connection().unwrap();
    let n: i64 = redis::cmd("PUBLISH")
        .arg("ch1")
        .arg("hello")
        .query(&mut pub_conn)
        .unwrap();
    assert_eq!(n, 1, "exactly one subscriber should have received");

    let received = rx
        .recv_timeout(Duration::from_secs(5))
        .expect("subscriber did not deliver message within 5s");
    assert_eq!(received, "hello");
}

#[test]
fn pattern_subscribe_matches_glob() {
    let s = Server::spawn();
    let port = s.port;

    let (tx, rx) = mpsc::channel::<(String, String)>();
    std::thread::spawn(move || {
        let client = redis::Client::open(format!("redis://127.0.0.1:{port}/")).unwrap();
        let mut conn = client.get_connection().unwrap();
        let mut pubsub = conn.as_pubsub();
        pubsub.psubscribe("news.*").unwrap();
        let msg = pubsub.get_message().unwrap();
        let channel: String = msg.get_channel_name().to_string();
        let payload: String = msg.get_payload().unwrap();
        let _ = tx.send((channel, payload));
    });

    std::thread::sleep(Duration::from_millis(200));

    let mut pub_conn = s.client().get_connection().unwrap();
    let _: i64 = redis::cmd("PUBLISH")
        .arg("news.sports")
        .arg("goal")
        .query(&mut pub_conn)
        .unwrap();

    let (ch, payload) = rx
        .recv_timeout(Duration::from_secs(5))
        .expect("subscriber did not deliver pattern message within 5s");
    assert_eq!(ch, "news.sports");
    assert_eq!(payload, "goal");
}
