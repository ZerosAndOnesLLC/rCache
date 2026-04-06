mod connection;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use bytes::Bytes;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

use crate::config::Config;
use crate::command::CommandRegistry;
use crate::persistence::aof::{AofWriter, FsyncMode};
use crate::protocol::RespValue;
use crate::storage::Store;
use crate::storage::ExpirationManager;

pub struct PubSubManager {
    /// channel -> set of client IDs subscribed to it
    pub channels: HashMap<Bytes, HashSet<u64>>,
    /// pattern -> set of client IDs subscribed to it
    pub patterns: HashMap<Bytes, HashSet<u64>>,
    /// client ID -> sender for pushing messages
    pub subscribers: HashMap<u64, tokio::sync::mpsc::UnboundedSender<RespValue>>,
}

impl PubSubManager {
    pub fn new() -> Self {
        Self {
            channels: HashMap::new(),
            patterns: HashMap::new(),
            subscribers: HashMap::new(),
        }
    }

    pub fn subscribe(&mut self, client_id: u64, channel: Bytes, sender: tokio::sync::mpsc::UnboundedSender<RespValue>) {
        self.channels.entry(channel).or_default().insert(client_id);
        self.subscribers.entry(client_id).or_insert(sender);
    }

    pub fn unsubscribe(&mut self, client_id: u64, channel: &Bytes) {
        if let Some(subs) = self.channels.get_mut(channel) {
            subs.remove(&client_id);
            if subs.is_empty() {
                self.channels.remove(channel);
            }
        }
    }

    pub fn psubscribe(&mut self, client_id: u64, pattern: Bytes, sender: tokio::sync::mpsc::UnboundedSender<RespValue>) {
        self.patterns.entry(pattern).or_default().insert(client_id);
        self.subscribers.entry(client_id).or_insert(sender);
    }

    pub fn punsubscribe(&mut self, client_id: u64, pattern: &Bytes) {
        if let Some(subs) = self.patterns.get_mut(pattern) {
            subs.remove(&client_id);
            if subs.is_empty() {
                self.patterns.remove(pattern);
            }
        }
    }

    pub fn publish(&self, channel: &Bytes, message: &Bytes) -> i64 {
        let mut count = 0i64;

        // Exact channel subscribers
        if let Some(subs) = self.channels.get(channel) {
            for client_id in subs {
                if let Some(sender) = self.subscribers.get(client_id) {
                    let msg = RespValue::array(vec![
                        RespValue::bulk_string(Bytes::from("message")),
                        RespValue::bulk_string(channel.clone()),
                        RespValue::bulk_string(message.clone()),
                    ]);
                    if sender.send(msg).is_ok() {
                        count += 1;
                    }
                }
            }
        }

        // Pattern subscribers
        let channel_str = String::from_utf8_lossy(channel);
        for (pattern, subs) in &self.patterns {
            let pattern_str = String::from_utf8_lossy(pattern);
            if crate::storage::db::glob_match(&pattern_str, &channel_str) {
                for client_id in subs {
                    if let Some(sender) = self.subscribers.get(client_id) {
                        let msg = RespValue::array(vec![
                            RespValue::bulk_string(Bytes::from("pmessage")),
                            RespValue::bulk_string(pattern.clone()),
                            RespValue::bulk_string(channel.clone()),
                            RespValue::bulk_string(message.clone()),
                        ]);
                        if sender.send(msg).is_ok() {
                            count += 1;
                        }
                    }
                }
            }
        }

        count
    }

    pub fn remove_client(&mut self, client_id: u64) {
        self.subscribers.remove(&client_id);
        self.channels.retain(|_, subs| {
            subs.remove(&client_id);
            !subs.is_empty()
        });
        self.patterns.retain(|_, subs| {
            subs.remove(&client_id);
            !subs.is_empty()
        });
    }

    pub fn client_channel_count(&self, client_id: u64) -> usize {
        self.channels.values().filter(|subs| subs.contains(&client_id)).count()
    }

    pub fn client_pattern_count(&self, client_id: u64) -> usize {
        self.patterns.values().filter(|subs| subs.contains(&client_id)).count()
    }
}

pub struct SharedState {
    pub store: Mutex<Store>,
    pub config: Config,
    pub registry: CommandRegistry,
    pub start_time: Instant,
    pub pubsub: Mutex<PubSubManager>,
    pub next_client_id: AtomicU64,
    pub aof_writer: Mutex<Option<AofWriter>>,
}

pub struct Server {
    config: Config,
    store: Store,
}

impl Server {
    pub fn new(config: Config, store: Store) -> Self {
        Self { config, store }
    }

    pub async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        let addr = format!("{}:{}", self.config.bind, self.config.port);
        let listener = TcpListener::bind(&addr).await?;
        tracing::info!("Listening on {}", addr);

        let aof_writer = if self.config.aof_enabled {
            let path = std::path::Path::new(&self.config.aof_filename);
            let fsync = FsyncMode::from_str(&self.config.appendfsync);
            match AofWriter::open(path, fsync) {
                Ok(writer) => {
                    tracing::info!("AOF enabled: {}", self.config.aof_filename);
                    Some(writer)
                }
                Err(e) => {
                    tracing::error!("Failed to open AOF file: {}", e);
                    None
                }
            }
        } else {
            None
        };

        let state = Arc::new(SharedState {
            store: Mutex::new(self.store),
            config: self.config.clone(),
            registry: CommandRegistry::new(),
            start_time: Instant::now(),
            pubsub: Mutex::new(PubSubManager::new()),
            next_client_id: AtomicU64::new(1),
            aof_writer: Mutex::new(aof_writer),
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

            let client_id = state.next_client_id.fetch_add(1, Ordering::Relaxed);

            tokio::spawn(async move {
                tracing::debug!("New connection from {} (client_id={})", addr, client_id);
                let mut conn = connection::Connection::new(socket, state, client_id);
                if let Err(e) = conn.handle().await {
                    tracing::debug!("Connection {} error: {}", addr, e);
                }
                drop(permit);
                tracing::debug!("Connection {} closed", addr);
            });
        }
    }
}
