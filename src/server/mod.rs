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
use crate::scripting_engine::{ScriptCache, FunctionLibrary};

// MaybeTls is used internally by the server module for TLS support.

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

    /// Publish to channel + matching patterns. Returns the number of clients
    /// that received the message. Also opportunistically prunes subscribers
    /// whose receiver has been dropped (abrupt disconnect path).
    pub fn publish(&mut self, channel: &Bytes, message: &Bytes) -> i64 {
        let mut count = 0i64;
        let mut dead: Vec<u64> = Vec::new();

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
                    } else {
                        dead.push(*client_id);
                    }
                }
            }
        }

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
                        } else {
                            dead.push(*client_id);
                        }
                    }
                }
            }
        }

        for client_id in dead {
            self.remove_client(client_id);
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

/// Per-command latency tracking.
#[derive(Debug, Clone, Default)]
pub struct LatencyStats {
    pub count: u64,
    pub total_us: u64,
    pub min_us: u64,
    pub max_us: u64,
}

/// A slow log entry.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct SlowLogEntry {
    pub id: u64,
    pub timestamp: u64,
    pub duration_us: u64,
    pub args: Vec<String>,
    pub client_addr: String,
    pub client_name: String,
}

/// A named namespace containing its own store.
#[derive(Debug)]
#[allow(dead_code)]
pub struct NamespaceInfo {
    pub name: String,
    pub store: Store,
}

#[allow(dead_code)]
pub struct SharedState {
    pub store: Mutex<Store>,
    pub config: Config,
    pub registry: CommandRegistry,
    pub start_time: Instant,
    pub pubsub: Mutex<PubSubManager>,
    pub next_client_id: AtomicU64,
    pub aof_writer: Mutex<Option<AofWriter>>,
    /// Total commands processed (all connections).
    pub commands_processed: AtomicU64,
    /// Total connections accepted since startup.
    pub connections_total: AtomicU64,
    /// Number of keyspace hits (successful key lookups).
    pub keyspace_hits: AtomicU64,
    /// Number of keyspace misses (failed key lookups).
    pub keyspace_misses: AtomicU64,
    /// Currently connected client count.
    pub connected_clients: AtomicU64,
    /// Lua script cache.
    pub script_cache: ScriptCache,
    /// Function library.
    pub function_library: FunctionLibrary,
    /// Per-command latency statistics.
    pub latency_stats: Mutex<HashMap<String, LatencyStats>>,
    /// Slow log entries.
    pub slowlog: Mutex<Vec<SlowLogEntry>>,
    /// Next slow log entry ID.
    pub slowlog_next_id: AtomicU64,
    /// Named namespaces (multi-tenancy).
    pub namespaces: Mutex<HashMap<String, Store>>,
    /// Per-IP AUTH failure tracking for brute-force backoff that survives
    /// reconnects: maps a peer IP to (consecutive failures, last-failure time).
    pub auth_failures: Mutex<HashMap<std::net::IpAddr, (u32, Instant)>>,
    /// Client tracking state. A single mutex protects the forward index
    /// (client_id -> keys it tracks), the senders, and the reverse index
    /// (key -> client_ids interested in it). Consolidating into one mutex
    /// eliminates lock-ordering hazards and lets writes consult the reverse
    /// index in O(1) instead of iterating every tracked client.
    pub tracking: Mutex<TrackingState>,
}

#[derive(Default)]
pub struct TrackingState {
    /// client_id -> keys it tracks.
    pub clients: HashMap<u64, HashSet<Bytes>>,
    /// client_id -> invalidation sender.
    pub senders: HashMap<u64, tokio::sync::mpsc::UnboundedSender<RespValue>>,
    /// key -> client_ids tracking this key (reverse index).
    pub keys: HashMap<Bytes, HashSet<u64>>,
}

impl TrackingState {
    pub fn track(&mut self, client_id: u64, key: Bytes) {
        self.clients.entry(client_id).or_default().insert(key.clone());
        self.keys.entry(key).or_default().insert(client_id);
    }

    pub fn enable(&mut self, client_id: u64, sender: tokio::sync::mpsc::UnboundedSender<RespValue>) {
        self.clients.entry(client_id).or_default();
        self.senders.insert(client_id, sender);
    }

    pub fn remove_client(&mut self, client_id: u64) {
        self.senders.remove(&client_id);
        if let Some(keys) = self.clients.remove(&client_id) {
            for key in keys {
                if let Some(set) = self.keys.get_mut(&key) {
                    set.remove(&client_id);
                    if set.is_empty() {
                        self.keys.remove(&key);
                    }
                }
            }
        }
    }

    /// Iterate clients interested in a mutated key, skipping `exclude`.
    /// Returns owned senders to avoid holding an iterator across `.send()`.
    pub fn invalidation_targets(
        &self,
        key: &Bytes,
        exclude: u64,
    ) -> Vec<tokio::sync::mpsc::UnboundedSender<RespValue>> {
        match self.keys.get(key) {
            Some(set) => set
                .iter()
                .filter(|&&id| id != exclude)
                .filter_map(|id| self.senders.get(id).cloned())
                .collect(),
            None => Vec::new(),
        }
    }
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

        // Apply requirepass to the shared ACL registry (the single source of
        // truth read by both the RESP and HTTP enforcement paths).
        crate::command::acl::init_default_password(self.config.requirepass.as_deref());

        let state = Arc::new(SharedState {
            store: Mutex::new(self.store),
            config: self.config.clone(),
            registry: CommandRegistry::new(),
            start_time: Instant::now(),
            pubsub: Mutex::new(PubSubManager::new()),
            next_client_id: AtomicU64::new(1),
            aof_writer: Mutex::new(aof_writer),
            commands_processed: AtomicU64::new(0),
            connections_total: AtomicU64::new(0),
            keyspace_hits: AtomicU64::new(0),
            keyspace_misses: AtomicU64::new(0),
            connected_clients: AtomicU64::new(0),
            script_cache: ScriptCache::new(),
            function_library: FunctionLibrary::new(),
            latency_stats: Mutex::new(HashMap::new()),
            slowlog: Mutex::new(Vec::new()),
            slowlog_next_id: AtomicU64::new(0),
            namespaces: Mutex::new(HashMap::new()),
            tracking: Mutex::new(TrackingState::default()),
            auth_failures: Mutex::new(HashMap::new()),
        });

        // Start HTTP/REST API server if configured
        if let Some(http_port) = self.config.http_port {
            let http_state = Arc::clone(&state);
            tokio::spawn(async move {
                if let Err(e) = crate::http::run_http_server(http_state, http_port).await {
                    tracing::error!("HTTP server error: {}", e);
                }
            });
        }

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

        // Start TLS listener if configured
        if let Some(tls_port) = self.config.tls_port {
            let cert_file = self.config.tls_cert_file.as_ref()
                .expect("--tls-cert-file required when --tls-port is set");
            let key_file = self.config.tls_key_file.as_ref()
                .expect("--tls-key-file required when --tls-port is set");

            let tls_acceptor = build_tls_acceptor(cert_file, key_file)?;
            let tls_addr = format!("{}:{}", self.config.bind, tls_port);
            let tls_listener = TcpListener::bind(&tls_addr).await?;
            tracing::info!("TLS listening on {}", tls_addr);

            let tls_state = Arc::clone(&state);
            let tls_semaphore = Arc::clone(&semaphore);
            tokio::spawn(async move {
                loop {
                    let (socket, addr) = match tls_listener.accept().await {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::error!("TLS accept error: {}", e);
                            continue;
                        }
                    };

                    let state = Arc::clone(&tls_state);

                    // Protected mode: refuse non-loopback clients when no auth is set.
                    if protected_mode_reject(&state, addr.ip()) {
                        tracing::warn!("Protected mode: rejecting TLS connection from {}", addr);
                        continue;
                    }

                    let permit = match tls_semaphore.clone().try_acquire_owned() {
                        Ok(p) => p,
                        Err(_) => {
                            tracing::warn!("Max clients reached, rejecting TLS {}", addr);
                            continue;
                        }
                    };

                    let acceptor = tls_acceptor.clone();
                    let client_id = state.next_client_id.fetch_add(1, Ordering::Relaxed);
                    state.connections_total.fetch_add(1, Ordering::Relaxed);
                    state.connected_clients.fetch_add(1, Ordering::Relaxed);

                    tokio::spawn(async move {
                        // Bound the TLS handshake so a client that completes TCP
                        // but stalls the handshake can't hold a client slot open.
                        let handshake = tokio::time::timeout(
                            std::time::Duration::from_secs(10),
                            acceptor.accept(socket),
                        )
                        .await;
                        match handshake {
                            Ok(Ok(tls_stream)) => {
                                tracing::debug!("New TLS connection from {} (client_id={})", addr, client_id);
                                let stream = connection::MaybeTls::Tls(tls_stream);
                                let mut conn = connection::Connection::new(stream, state.clone(), client_id, Some(addr.ip()));
                                if let Err(e) = conn.handle().await {
                                    tracing::debug!("TLS connection {} error: {}", addr, e);
                                }
                            }
                            Ok(Err(e)) => {
                                tracing::debug!("TLS handshake failed from {}: {}", addr, e);
                            }
                            Err(_) => {
                                tracing::debug!("TLS handshake timed out from {}", addr);
                            }
                        }
                        state.connected_clients.fetch_sub(1, Ordering::Relaxed);
                        drop(permit);
                        tracing::debug!("TLS connection {} closed", addr);
                    });
                }
            });
        }

        loop {
            let (socket, addr) = listener.accept().await?;
            let state = Arc::clone(&state);

            // Protected mode: refuse non-loopback clients when no auth is set.
            if protected_mode_reject(&state, addr.ip()) {
                tracing::warn!("Protected mode: rejecting connection from {}", addr);
                let mut socket = socket;
                tokio::spawn(async move {
                    use tokio::io::AsyncWriteExt;
                    let _ = socket.write_all(PROTECTED_MODE_ERROR).await;
                    let _ = socket.shutdown().await;
                });
                continue;
            }

            let permit = match semaphore.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    tracing::warn!("Max clients reached, rejecting {}", addr);
                    continue;
                }
            };

            let client_id = state.next_client_id.fetch_add(1, Ordering::Relaxed);
            state.connections_total.fetch_add(1, Ordering::Relaxed);
            state.connected_clients.fetch_add(1, Ordering::Relaxed);

            tokio::spawn(async move {
                tracing::debug!("New connection from {} (client_id={})", addr, client_id);
                let stream = connection::MaybeTls::Plain(socket);
                let mut conn = connection::Connection::new(stream, state.clone(), client_id, Some(addr.ip()));
                if let Err(e) = conn.handle().await {
                    tracing::debug!("Connection {} error: {}", addr, e);
                }
                state.connected_clients.fetch_sub(1, Ordering::Relaxed);
                drop(permit);
                tracing::debug!("Connection {} closed", addr);
            });
        }
    }
}

/// RESP error sent to a client rejected by protected mode.
const PROTECTED_MODE_ERROR: &[u8] =
    b"-DENIED rCache is running in protected mode because protected mode is enabled \
and no authentication is configured. Connect from the loopback interface, set a \
password (requirepass / ACL), or disable protected mode with '--protected-mode no'.\r\n";

/// Whether a peer should be refused under protected mode: it is enabled, no
/// authentication is configured, and the peer is not on the loopback interface.
fn protected_mode_reject(state: &SharedState, ip: std::net::IpAddr) -> bool {
    state.config.protected_mode
        && state.config.requirepass.is_none()
        && !crate::command::acl::any_password_required()
        && !ip.is_loopback()
}

/// Build a TLS acceptor from PEM cert and key files.
fn build_tls_acceptor(
    cert_path: &str,
    key_path: &str,
) -> Result<tokio_rustls::TlsAcceptor, Box<dyn std::error::Error>> {
    use rustls::pki_types::PrivateKeyDer;
    use std::io::BufReader;

    let cert_file = std::fs::File::open(cert_path)
        .map_err(|e| format!("Failed to open TLS cert file '{}': {}", cert_path, e))?;
    let mut cert_reader = BufReader::new(cert_file);
    let certs: Vec<_> = rustls_pemfile::certs(&mut cert_reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("Failed to parse TLS certs: {}", e))?;

    if certs.is_empty() {
        return Err("No certificates found in TLS cert file".into());
    }

    let key_file = std::fs::File::open(key_path)
        .map_err(|e| format!("Failed to open TLS key file '{}': {}", key_path, e))?;
    let mut key_reader = BufReader::new(key_file);

    let key: PrivateKeyDer = rustls_pemfile::private_key(&mut key_reader)
        .map_err(|e| format!("Failed to parse TLS key: {}", e))?
        .ok_or("No private key found in TLS key file")?;

    // Build a server config restricted to TLS 1.2 and 1.3. rustls 0.23's default
    // crypto provider already excludes weak ciphers; pinning protocol versions
    // guards against downgrade-style misconfiguration if defaults change.
    let config = rustls::ServerConfig::builder_with_protocol_versions(&[
        &rustls::version::TLS13,
        &rustls::version::TLS12,
    ])
    .with_no_client_auth()
    .with_single_cert(certs, key)
    .map_err(|e| format!("Failed to build TLS config: {}", e))?;

    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
}
