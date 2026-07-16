use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};
use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use crate::protocol::{Parser, RespValue};
use crate::command::CommandContext;
use crate::storage::db::glob_match;
use super::SharedState;

/// Maximum size of a connection's unprocessed query buffer. A single bulk
/// argument may be up to 512 MB (the parser's `MAX_BULK_LEN`), so this ceiling
/// sits above that to allow one max-size argument while still bounding an
/// attacker who streams a partial/incomplete frame forever. Mirrors Redis's
/// client query-buffer cap.
const MAX_QUERY_BUFFER: usize = 1024 * 1024 * 1024;

/// Idle read timeout. When it fires, a connection that is mid-command (has a
/// partial request buffered) or not yet authenticated is dropped — the
/// slow-loris shapes. A fully idle, authenticated client with an empty buffer
/// (a normal pooled connection) is left alone.
const READ_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// A stream that may or may not be TLS-wrapped.
pub enum MaybeTls {
    Plain(TcpStream),
    Tls(tokio_rustls::server::TlsStream<TcpStream>),
}

impl AsyncRead for MaybeTls {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTls::Plain(s) => Pin::new(s).poll_read(cx, buf),
            MaybeTls::Tls(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MaybeTls {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            MaybeTls::Plain(s) => Pin::new(s).poll_write(cx, buf),
            MaybeTls::Tls(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTls::Plain(s) => Pin::new(s).poll_flush(cx),
            MaybeTls::Tls(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTls::Plain(s) => Pin::new(s).poll_shutdown(cx),
            MaybeTls::Tls(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

pub struct Connection {
    stream: MaybeTls,
    state: Arc<SharedState>,
    client_id: u64,
    db_index: usize,
    authenticated: bool,
    auth_username: String,
    buffer: BytesMut,
    client_name: Option<String>,
    // RESP protocol version (2 or 3)
    resp_version: u8,
    // Pub/Sub state
    subscribed_channels: HashSet<Bytes>,
    subscribed_patterns: HashSet<Bytes>,
    pubsub_rx: Option<mpsc::UnboundedReceiver<RespValue>>,
    pubsub_tx: Option<mpsc::UnboundedSender<RespValue>>,
    // Transaction state
    in_multi: bool,
    tx_queue: Vec<Vec<Bytes>>,
    tx_error: bool,
    watch_keys: Vec<(Bytes, Option<u64>)>,
    // Client-side caching
    tracking_enabled: bool,
    tracked_keys: HashSet<Bytes>,
    tracking_tx: Option<mpsc::UnboundedSender<RespValue>>,
    #[allow(dead_code)] // receiver kept on Connection for future tracking-channel reads
    tracking_rx: Option<mpsc::UnboundedReceiver<RespValue>>,
    // Multi-tenancy namespace
    namespace: Option<String>,
    // AUTH brute-force protection: count consecutive failures, reset on success.
    auth_failures: u32,
}

impl Connection {
    pub fn new(stream: MaybeTls, state: Arc<SharedState>, client_id: u64) -> Self {
        let authenticated = state.config.requirepass.is_none();
        let (tx, rx) = mpsc::unbounded_channel();
        let (tracking_tx, tracking_rx) = mpsc::unbounded_channel();
        Self {
            stream,
            state,
            client_id,
            db_index: 0,
            authenticated,
            auth_username: "default".to_string(),
            buffer: BytesMut::with_capacity(4096),
            client_name: None,
            resp_version: 2,
            subscribed_channels: HashSet::new(),
            subscribed_patterns: HashSet::new(),
            pubsub_rx: Some(rx),
            pubsub_tx: Some(tx),
            in_multi: false,
            tx_queue: Vec::new(),
            tx_error: false,
            watch_keys: Vec::new(),
            tracking_enabled: false,
            tracked_keys: HashSet::new(),
            tracking_tx: Some(tracking_tx),
            tracking_rx: Some(tracking_rx),
            auth_failures: 0,
            namespace: None,
        }
    }

    pub async fn handle(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        loop {
            let in_pubsub = !self.subscribed_channels.is_empty() || !self.subscribed_patterns.is_empty();

            if in_pubsub {
                // In pub/sub mode: select between incoming commands and pub/sub messages
                let mut pubsub_rx = self.pubsub_rx.take().unwrap();

                tokio::select! {
                    result = self.stream.read_buf(&mut self.buffer) => {
                        self.pubsub_rx = Some(pubsub_rx);
                        let n = result?;
                        if n == 0 {
                            self.cleanup_pubsub().await;
                            return Ok(());
                        }
                        if self.buffer.len() > MAX_QUERY_BUFFER {
                            let err = RespValue::error("ERR Protocol error: unbalanced/too-large query buffer");
                            let _ = self.stream.write_all(&err.serialize()).await;
                            self.cleanup_pubsub().await;
                            return Ok(());
                        }
                        while let Some(response) = self.try_process_command().await? {
                            let data = response.serialize();
                            self.stream.write_all(&data).await?;
                        }
                    }
                    Some(msg) = pubsub_rx.recv() => {
                        self.pubsub_rx = Some(pubsub_rx);
                        let msg = if self.resp_version == 3 {
                            // Convert pub/sub Array messages to Push type
                            match msg {
                                RespValue::Array(items) => RespValue::Push(items),
                                other => other,
                            }
                        } else {
                            msg
                        };
                        let data = msg.serialize();
                        self.stream.write_all(&data).await?;
                    }
                }
            } else {
                // Normal mode: process commands from buffer first
                while let Some(response) = self.try_process_command().await? {
                    let data = response.serialize();
                    self.stream.write_all(&data).await?;
                }

                // Read more data from the socket, bounded by an idle timeout.
                let n = match tokio::time::timeout(
                    READ_IDLE_TIMEOUT,
                    self.stream.read_buf(&mut self.buffer),
                )
                .await
                {
                    Ok(res) => res?,
                    Err(_) => {
                        // Idle timeout: reap slow/partial or pre-auth connections,
                        // but let an authenticated client with no pending data
                        // keep its connection open.
                        if self.buffer.is_empty() && self.authenticated {
                            continue;
                        }
                        self.cleanup_pubsub().await;
                        return Ok(());
                    }
                };
                if n == 0 {
                    self.cleanup_pubsub().await;
                    return Ok(());
                }
                if self.buffer.len() > MAX_QUERY_BUFFER {
                    let err = RespValue::error("ERR Protocol error: unbalanced/too-large query buffer");
                    let _ = self.stream.write_all(&err.serialize()).await;
                    self.cleanup_pubsub().await;
                    return Ok(());
                }
            }
        }
    }

    async fn cleanup_pubsub(&mut self) {
        let mut pubsub = self.state.pubsub.lock().await;
        pubsub.remove_client(self.client_id);
        drop(pubsub);
        if self.tracking_enabled {
            let mut tracking = self.state.tracking.lock().await;
            tracking.remove_client(self.client_id);
        }
    }

    async fn try_process_command(&mut self) -> Result<Option<RespValue>, Box<dyn std::error::Error + Send + Sync>> {
        if self.buffer.is_empty() {
            return Ok(None);
        }

        match Parser::parse(&self.buffer) {
            Ok((value, consumed)) => {
                let _ = self.buffer.split_to(consumed);

                // Extract command name for latency tracking
                let cmd_name = match &value {
                    RespValue::Array(items) if !items.is_empty() => {
                        match &items[0] {
                            RespValue::BulkString(b) => String::from_utf8_lossy(b).to_uppercase(),
                            other => other.to_string_lossy().to_uppercase().into(),
                        }
                    }
                    _ => "UNKNOWN".into(),
                };

                let start = std::time::Instant::now();
                let response = self.execute_command(value).await;
                let elapsed_us = start.elapsed().as_micros() as u64;
                let response = self.convert_to_resp3(response);

                // Record latency stats. Skip on auth-related rejections so an
                // unauthenticated probe can't populate command-name histograms
                // visible via INFO commandstats / LATENCY HISTORY.
                let auth_rejected = matches!(
                    &response,
                    RespValue::Error(e) if e.starts_with("NOAUTH") || e.starts_with("WRONGPASS")
                );
                if !auth_rejected {
                    let mut stats = self.state.latency_stats.lock().await;
                    let entry = stats.entry(cmd_name.to_string()).or_default();
                    entry.count += 1;
                    entry.total_us += elapsed_us;
                    if entry.min_us == 0 || elapsed_us < entry.min_us {
                        entry.min_us = elapsed_us;
                    }
                    if elapsed_us > entry.max_us {
                        entry.max_us = elapsed_us;
                    }
                }

                // Record to slowlog if above threshold
                let threshold = self.state.config.slowlog_log_slower_than;
                if threshold >= 0 && elapsed_us > threshold as u64 {
                    let id = self.state.slowlog_next_id.fetch_add(1, Ordering::Relaxed);
                    let timestamp = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    let client_name = self.client_name.clone().unwrap_or_default();

                    // Write to SharedState slowlog
                    {
                        let entry = super::SlowLogEntry {
                            id,
                            timestamp,
                            duration_us: elapsed_us,
                            args: vec![cmd_name.to_string()],
                            client_addr: String::new(),
                            client_name: client_name.clone(),
                        };
                        let mut slowlog = self.state.slowlog.lock().await;
                        let max_len = self.state.config.slowlog_max_len;
                        slowlog.push(entry);
                        if slowlog.len() > max_len {
                            slowlog.remove(0);
                        }
                    }

                    // Also write to global slowlog accessible by SLOWLOG command
                    if let Ok(mut global_log) = crate::command::server_cmds::GLOBAL_SLOWLOG.lock() {
                        global_log.push(crate::command::server_cmds::SlowLogEntryGlobal {
                            id,
                            timestamp,
                            duration_us: elapsed_us,
                            args: vec![cmd_name.to_string()],
                            client_addr: String::new(),
                            client_name,
                        });
                        let max_len = self.state.config.slowlog_max_len;
                        if global_log.len() > max_len {
                            global_log.remove(0);
                        }
                    }
                }

                self.state.commands_processed.fetch_add(1, Ordering::Relaxed);
                Ok(Some(response))
            }
            Err(crate::protocol::parser::ParseError::Incomplete) => Ok(None),
            Err(crate::protocol::parser::ParseError::Invalid(msg)) => {
                Ok(Some(RespValue::error(format!("ERR protocol error: {}", msg))))
            }
        }
    }

    async fn execute_command(&mut self, value: RespValue) -> RespValue {
        let args = match value {
            RespValue::Array(items) => {
                items.into_iter().map(|v| match v {
                    RespValue::BulkString(b) => b,
                    other => Bytes::from(other.to_string_lossy()),
                }).collect::<Vec<_>>()
            }
            _ => return RespValue::error("ERR invalid command format"),
        };

        if args.is_empty() {
            return RespValue::error("ERR empty command");
        }

        let cmd_name = String::from_utf8_lossy(&args[0]).to_uppercase();

        // Handle QUIT
        if cmd_name == "QUIT" {
            return RespValue::ok();
        }

        // Handle AUTH
        if cmd_name == "AUTH" {
            return self.handle_auth(&args).await;
        }

        // Check authentication
        if !self.authenticated {
            // Allow HELLO before auth
            if cmd_name != "HELLO" {
                return RespValue::error("NOAUTH Authentication required.");
            }
        }

        // ACL check: verify user is allowed to execute this command. This reads
        // the shared ACL registry that `ACL SETUSER` mutates, so runtime rule
        // changes are enforced.
        {
            use crate::command::acl;
            if !acl::is_command_allowed(&self.auth_username, &cmd_name) {
                return RespValue::error(format!(
                    "NOPERM this user has no permissions to run the '{}' command",
                    cmd_name.to_lowercase()
                ));
            }
            // Check key patterns for commands that have keys
            if args.len() > 1 && !acl::user_has_all_keys(&self.auth_username) {
                let key_str = String::from_utf8_lossy(&args[1]);
                if !acl::is_key_allowed(&self.auth_username, &key_str) {
                    return RespValue::error(
                        "NOPERM this user has no permissions to access one of the keys used as arguments"
                            .to_string(),
                    );
                }
            }
        }

        // Handle CLIENT TRACKING specially
        if cmd_name == "CLIENT" && args.len() >= 3 {
            let subcmd = String::from_utf8_lossy(&args[1]).to_uppercase();
            if subcmd == "TRACKING" {
                let onoff = String::from_utf8_lossy(&args[2]).to_uppercase();
                if onoff == "ON" {
                    self.tracking_enabled = true;
                    if let Some(tx) = self.tracking_tx.clone() {
                        let mut tracking = self.state.tracking.lock().await;
                        tracking.enable(self.client_id, tx);
                    }
                    return RespValue::ok();
                } else if onoff == "OFF" {
                    self.tracking_enabled = false;
                    self.tracked_keys.clear();
                    let mut tracking = self.state.tracking.lock().await;
                    tracking.remove_client(self.client_id);
                    return RespValue::ok();
                }
            }
        }

        // Handle NAMESPACE commands
        if cmd_name == "NAMESPACE" {
            return self.handle_namespace(&args).await;
        }

        // Handle connection-level commands that need special handling

        // Pub/Sub commands handled in connection layer
        if cmd_name == "SUBSCRIBE" {
            return self.handle_subscribe(&args).await;
        }
        if cmd_name == "UNSUBSCRIBE" {
            return self.handle_unsubscribe(&args).await;
        }
        if cmd_name == "PSUBSCRIBE" {
            return self.handle_psubscribe(&args).await;
        }
        if cmd_name == "PUNSUBSCRIBE" {
            return self.handle_punsubscribe(&args).await;
        }
        if cmd_name == "PUBLISH" {
            return self.handle_publish(&args).await;
        }
        if cmd_name == "PUBSUB" {
            return self.handle_pubsub_cmd(&args).await;
        }

        // In pub/sub mode, only SUBSCRIBE/UNSUBSCRIBE/PSUBSCRIBE/PUNSUBSCRIBE/PING/RESET/QUIT are allowed
        if !self.subscribed_channels.is_empty() || !self.subscribed_patterns.is_empty() {
            if cmd_name != "PING" && cmd_name != "RESET" {
                return RespValue::error(format!(
                    "ERR Can't execute '{}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
                    cmd_name.to_lowercase()
                ));
            }
        }

        // Transaction commands
        if cmd_name == "MULTI" {
            return self.handle_multi();
        }
        if cmd_name == "EXEC" {
            return self.handle_exec().await;
        }
        if cmd_name == "DISCARD" {
            return self.handle_discard();
        }
        if cmd_name == "WATCH" {
            return self.handle_watch(&args).await;
        }
        if cmd_name == "UNWATCH" {
            return self.handle_unwatch();
        }

        // If in MULTI, queue commands (except EXEC/DISCARD/MULTI/WATCH)
        if self.in_multi {
            self.tx_queue.push(args);
            return RespValue::simple_string("QUEUED");
        }

        // Handle SELECT specially (modifies connection state)
        if cmd_name == "SELECT" {
            if args.len() != 2 {
                return RespValue::wrong_arity("select");
            }
            let index: usize = match String::from_utf8_lossy(&args[1]).parse() {
                Ok(v) => v,
                Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
            };
            let store = self.state.store.lock().await;
            if index >= store.db_count() {
                return RespValue::error("ERR DB index is out of range");
            }
            drop(store);
            self.db_index = index;
            return RespValue::ok();
        }

        // Handle CLIENT SETNAME/GETNAME/ID specially
        if cmd_name == "CLIENT" && args.len() >= 2 {
            let subcmd = String::from_utf8_lossy(&args[1]).to_uppercase();
            match subcmd.as_str() {
                "SETNAME" => {
                    if args.len() < 3 {
                        return RespValue::wrong_arity("client|setname");
                    }
                    self.client_name = Some(String::from_utf8_lossy(&args[2]).to_string());
                    return RespValue::ok();
                }
                "GETNAME" => {
                    return match &self.client_name {
                        Some(name) => RespValue::bulk_string(Bytes::from(name.clone())),
                        None => RespValue::Null,
                    };
                }
                "ID" => {
                    return RespValue::integer(self.client_id as i64);
                }
                _ => {} // fall through to registry
            }
        }

        // Handle HELLO (needs connection-level access for protocol negotiation)
        if cmd_name == "HELLO" {
            // Parse requested protocol version
            let proto = if args.len() >= 2 {
                match String::from_utf8_lossy(&args[1]).parse::<u8>() {
                    Ok(2) => 2u8,
                    Ok(3) => 3u8,
                    Ok(_) => return RespValue::error("NOPROTO unsupported protocol version"),
                    Err(_) => return RespValue::error("ERR Protocol version is not an integer or out of range"),
                }
            } else {
                self.resp_version
            };

            // Process AUTH and SETNAME if present
            let mut i = 2;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                match opt.as_str() {
                    "AUTH" => {
                        if i + 2 >= args.len() {
                            return RespValue::error("ERR syntax error");
                        }
                        let auth_result = self.handle_auth_hello(&args[i + 1], &args[i + 2]).await;
                        if let Some(err) = auth_result {
                            return err;
                        }
                        i += 3;
                    }
                    "SETNAME" => {
                        if i + 1 >= args.len() {
                            return RespValue::error("ERR syntax error");
                        }
                        self.client_name = Some(String::from_utf8_lossy(&args[i + 1]).to_string());
                        i += 2;
                    }
                    _ => { i += 1; }
                }
            }

            self.resp_version = proto;

            // Build HELLO response
            return self.build_hello_response(proto);
        }

        // Handle RESET
        if cmd_name == "RESET" {
            self.in_multi = false;
            self.tx_queue.clear();
            self.tx_error = false;
            self.watch_keys.clear();
            self.db_index = 0;
            self.resp_version = 2;
            // Unsubscribe from all channels and patterns with a single lock acquisition
            {
                let channels: Vec<Bytes> = self.subscribed_channels.iter().cloned().collect();
                let patterns: Vec<Bytes> = self.subscribed_patterns.iter().cloned().collect();
                if !channels.is_empty() || !patterns.is_empty() {
                    let mut pubsub = self.state.pubsub.lock().await;
                    for ch in &channels {
                        pubsub.unsubscribe(self.client_id, ch);
                    }
                    for pat in &patterns {
                        pubsub.punsubscribe(self.client_id, pat);
                    }
                }
                self.subscribed_channels.clear();
                self.subscribed_patterns.clear();
            }
            return RespValue::simple_string("RESET");
        }

        // Only clone args for AOF/tracking when this is a write command or the
        // connection is in client-tracking mode — saves a Vec<Bytes> clone per
        // read for every connection (P7 from the audit). cmd_name is always
        // cloned; it's a tiny String compared to args.
        let cmd_name_for_aof = cmd_name.clone();
        let is_write = crate::persistence::aof::is_write_command(&cmd_name);
        let need_args_copy = is_write || self.tracking_enabled;
        let args_for_aof: Vec<Bytes> = if need_args_copy { args.clone() } else { Vec::new() };

        // Execute command with store lock
        let mut store = self.state.store.lock().await;

        // Check memory eviction before write commands
        if is_write {
            let cfg = &self.state.config;
            if store
                .check_memory_limit(
                    cfg.maxmemory,
                    &cfg.maxmemory_policy,
                    cfg.maxmemory_samples,
                    cfg.lfu_log_factor,
                    cfg.lfu_decay_time,
                )
                .is_err()
            {
                return RespValue::error("OOM command not allowed when used memory > 'maxmemory'.");
            }
        }

        let mut ctx = CommandContext {
            store: &mut store,
            db_index: self.db_index,
            args,
            start_time: self.state.start_time,
        };

        let result = self.state.registry.execute(&mut ctx);

        // Update db_index in case SELECT was called via some path
        self.db_index = ctx.db_index;

        drop(store);

        // Client-side caching: track read keys
        if self.tracking_enabled && is_read_command(&cmd_name_for_aof) && args_for_aof.len() > 1 {
            let key = args_for_aof[1].clone();
            self.tracked_keys.insert(key.clone());
            let mut tracking = self.state.tracking.lock().await;
            tracking.track(self.client_id, key);
        }

        // Append to AOF if this was a write command and it succeeded
        if is_write {
            if !matches!(result, RespValue::Error(_)) {
                let mut aof = self.state.aof_writer.lock().await;
                if let Some(writer) = aof.as_mut() {
                    if let Err(e) = writer.append(&args_for_aof) {
                        tracing::error!("AOF write error: {}", e);
                    }
                }
            }

            // Client-side caching: send invalidation to tracking clients.
            // O(1) lookup via reverse index; senders are cloned out so we don't
            // hold the tracking lock across send() (which can block on a full
            // unbounded channel — extremely rare but the cleaner pattern).
            if args_for_aof.len() > 1 {
                let mutated_key = &args_for_aof[1];
                let targets = {
                    let tracking = self.state.tracking.lock().await;
                    tracking.invalidation_targets(mutated_key, self.client_id)
                };
                if !targets.is_empty() {
                    let invalidation = RespValue::Push(vec![
                        RespValue::bulk_string(Bytes::from("invalidate")),
                        RespValue::array(vec![
                            RespValue::bulk_string(mutated_key.clone()),
                        ]),
                    ]);
                    for sender in &targets {
                        let _ = sender.send(invalidation.clone());
                    }
                }
            }
        }

        result
    }

    /// Build the HELLO response, using Map for RESP3 and flat Array for RESP2.
    fn build_hello_response(&self, proto: u8) -> RespValue {
        let entries = vec![
            (RespValue::bulk_string(Bytes::from("server")), RespValue::bulk_string(Bytes::from("rcache"))),
            (RespValue::bulk_string(Bytes::from("version")), RespValue::bulk_string(Bytes::from(env!("CARGO_PKG_VERSION")))),
            (RespValue::bulk_string(Bytes::from("proto")), RespValue::integer(proto as i64)),
            (RespValue::bulk_string(Bytes::from("id")), RespValue::integer(self.client_id as i64)),
            (RespValue::bulk_string(Bytes::from("mode")), RespValue::bulk_string(Bytes::from("standalone"))),
            (RespValue::bulk_string(Bytes::from("role")), RespValue::bulk_string(Bytes::from("master"))),
            (RespValue::bulk_string(Bytes::from("modules")), RespValue::array(vec![])),
        ];

        if proto == 3 {
            RespValue::Map(entries)
        } else {
            // RESP2: flat array of key, value, key, value, ...
            let mut flat = Vec::with_capacity(entries.len() * 2);
            for (k, v) in entries {
                flat.push(k);
                flat.push(v);
            }
            RespValue::array(flat)
        }
    }

    /// Convert a response to RESP3 format when appropriate.
    /// This converts Null to Resp3Null and pub/sub messages to Push type.
    fn convert_to_resp3(&self, value: RespValue) -> RespValue {
        if self.resp_version < 3 {
            return value;
        }
        match value {
            RespValue::Null => RespValue::Resp3Null,
            RespValue::NullArray => RespValue::Resp3Null,
            _ => value,
        }
    }

    async fn handle_namespace(&mut self, args: &[Bytes]) -> RespValue {
        if args.len() < 2 {
            return RespValue::error("ERR wrong number of arguments for 'namespace' command");
        }
        let subcmd = String::from_utf8_lossy(&args[1]).to_uppercase();
        match subcmd.as_str() {
            "CREATE" => {
                if args.len() < 3 {
                    return RespValue::wrong_arity("namespace|create");
                }
                let name = String::from_utf8_lossy(&args[2]).to_string();
                let mut namespaces = self.state.namespaces.lock().await;
                if namespaces.contains_key(&name) {
                    return RespValue::error(format!("ERR namespace '{}' already exists", name));
                }
                let databases = self.state.config.databases;
                namespaces.insert(name, crate::storage::Store::new(databases));
                RespValue::ok()
            }
            "SELECT" => {
                if args.len() < 3 {
                    return RespValue::wrong_arity("namespace|select");
                }
                let name = String::from_utf8_lossy(&args[2]).to_string();
                if name == "default" || name.is_empty() {
                    self.namespace = None;
                    return RespValue::ok();
                }
                let namespaces = self.state.namespaces.lock().await;
                if namespaces.contains_key(&name) {
                    drop(namespaces);
                    self.namespace = Some(name);
                    self.db_index = 0;
                    RespValue::ok()
                } else {
                    RespValue::error(format!("ERR namespace '{}' does not exist", name))
                }
            }
            "LIST" => {
                let namespaces = self.state.namespaces.lock().await;
                let mut names: Vec<RespValue> = vec![
                    RespValue::bulk_string(Bytes::from("default")),
                ];
                for name in namespaces.keys() {
                    names.push(RespValue::bulk_string(Bytes::from(name.clone())));
                }
                RespValue::array(names)
            }
            "DELETE" => {
                if args.len() < 3 {
                    return RespValue::wrong_arity("namespace|delete");
                }
                let name = String::from_utf8_lossy(&args[2]).to_string();
                if name == "default" {
                    return RespValue::error("ERR cannot delete the default namespace");
                }
                let mut namespaces = self.state.namespaces.lock().await;
                if namespaces.remove(&name).is_some() {
                    // If current connection was using this namespace, reset to default
                    if self.namespace.as_deref() == Some(&name) {
                        self.namespace = None;
                        self.db_index = 0;
                    }
                    RespValue::ok()
                } else {
                    RespValue::error(format!("ERR namespace '{}' does not exist", name))
                }
            }
            "INFO" => {
                if args.len() < 3 {
                    // Info about current namespace
                    let name = self.namespace.clone().unwrap_or_else(|| "default".to_string());
                    return RespValue::array(vec![
                        RespValue::bulk_string(Bytes::from("name")),
                        RespValue::bulk_string(Bytes::from(name)),
                    ]);
                }
                let name = String::from_utf8_lossy(&args[2]).to_string();
                if name == "default" {
                    let store = self.state.store.lock().await;
                    let total_keys: usize = (0..store.db_count()).map(|i| store.db(i).len()).sum();
                    RespValue::array(vec![
                        RespValue::bulk_string(Bytes::from("name")),
                        RespValue::bulk_string(Bytes::from("default")),
                        RespValue::bulk_string(Bytes::from("keys")),
                        RespValue::integer(total_keys as i64),
                    ])
                } else {
                    let namespaces = self.state.namespaces.lock().await;
                    if let Some(ns_store) = namespaces.get(&name) {
                        let total_keys: usize = (0..ns_store.db_count()).map(|i| ns_store.db(i).len()).sum();
                        RespValue::array(vec![
                            RespValue::bulk_string(Bytes::from("name")),
                            RespValue::bulk_string(Bytes::from(name)),
                            RespValue::bulk_string(Bytes::from("keys")),
                            RespValue::integer(total_keys as i64),
                        ])
                    } else {
                        RespValue::error(format!("ERR namespace '{}' does not exist", name))
                    }
                }
            }
            _ => RespValue::error(format!(
                "ERR unknown subcommand or wrong number of arguments for 'namespace|{}'",
                subcmd.to_lowercase()
            )),
        }
    }

    /// After `BACKOFF_AFTER` consecutive failed AUTH/HELLO attempts on this
    /// connection, sleep before returning the failure response. Doubles each
    /// time, capped at 5 s. Resets to zero on a successful auth.
    async fn auth_backoff(&mut self) {
        const BACKOFF_AFTER: u32 = 5;
        const MAX_BACKOFF: std::time::Duration = std::time::Duration::from_secs(5);

        self.auth_failures = self.auth_failures.saturating_add(1);
        if self.auth_failures <= BACKOFF_AFTER {
            return;
        }
        let over = self.auth_failures - BACKOFF_AFTER;
        let ms: u64 = 100u64.saturating_mul(1u64 << over.min(10));
        let delay = std::time::Duration::from_millis(ms).min(MAX_BACKOFF);
        tokio::time::sleep(delay).await;
    }

    fn auth_success(&mut self, username: String) {
        self.authenticated = true;
        self.auth_username = username;
        self.auth_failures = 0;
    }

    async fn handle_auth(&mut self, args: &[Bytes]) -> RespValue {
        if args.len() < 2 {
            return RespValue::wrong_arity("auth");
        }

        let (username, password) = if args.len() >= 3 {
            (
                String::from_utf8_lossy(&args[1]).to_string(),
                String::from_utf8_lossy(&args[2]).to_string(),
            )
        } else {
            ("default".to_string(), String::from_utf8_lossy(&args[1]).to_string())
        };

        if username == "default" {
            if let Some(ref req_pass) = self.state.config.requirepass {
                if crate::command::acl::verify_secret(&password, req_pass) {
                    self.auth_success(username);
                    return RespValue::ok();
                }
            } else {
                return RespValue::error("ERR Client sent AUTH, but no password is set. Did you mean ACL SETUSER with >password?");
            }
        }

        match crate::command::acl::check_password(&username, &password) {
            crate::command::acl::AuthOutcome::Ok => {
                self.auth_success(username);
                RespValue::ok()
            }
            _ => {
                self.auth_backoff().await;
                RespValue::error("WRONGPASS invalid username-password pair or user is disabled.")
            }
        }
    }

    async fn handle_auth_hello(&mut self, username: &Bytes, password: &Bytes) -> Option<RespValue> {
        let username = String::from_utf8_lossy(username).to_string();
        let password = String::from_utf8_lossy(password).to_string();

        if username == "default" {
            if let Some(ref req_pass) = self.state.config.requirepass {
                if crate::command::acl::verify_secret(&password, req_pass) {
                    self.auth_success(username);
                    return None;
                }
            }
        }

        match crate::command::acl::check_password(&username, &password) {
            crate::command::acl::AuthOutcome::Ok => {
                self.auth_success(username);
                None
            }
            _ => {
                self.auth_backoff().await;
                Some(RespValue::error("WRONGPASS invalid username-password pair or user is disabled."))
            }
        }
    }

    // === Pub/Sub handlers ===

    async fn handle_subscribe(&mut self, args: &[Bytes]) -> RespValue {
        let channels: Vec<Bytes> = args[1..].to_vec();
        let mut responses = Vec::new();

        for channel in channels {
            self.subscribed_channels.insert(channel.clone());
            let mut pubsub = self.state.pubsub.lock().await;
            let tx = self.pubsub_tx.clone().unwrap();
            pubsub.subscribe(self.client_id, channel.clone(), tx);
            let sub_count = pubsub.client_channel_count(self.client_id) + pubsub.client_pattern_count(self.client_id);
            drop(pubsub);

            responses.push(RespValue::array(vec![
                RespValue::bulk_string(Bytes::from("subscribe")),
                RespValue::bulk_string(channel),
                RespValue::integer(sub_count as i64),
            ]));
        }

        // Return the last subscribe acknowledgment (Redis sends one per channel)
        // We need to write all of them
        if responses.len() == 1 {
            responses.into_iter().next().unwrap()
        } else {
            // Write all but the last directly, return the last
            for resp in &responses[..responses.len() - 1] {
                let data = resp.serialize();
                let _ = self.stream.write_all(&data).await;
            }
            responses.into_iter().last().unwrap()
        }
    }

    async fn handle_unsubscribe(&mut self, args: &[Bytes]) -> RespValue {
        let channels: Vec<Bytes> = if args.len() > 1 {
            args[1..].to_vec()
        } else {
            self.subscribed_channels.iter().cloned().collect()
        };

        let mut responses = Vec::new();

        for channel in channels {
            self.subscribed_channels.remove(&channel);
            let mut pubsub = self.state.pubsub.lock().await;
            pubsub.unsubscribe(self.client_id, &channel);
            let sub_count = pubsub.client_channel_count(self.client_id) + pubsub.client_pattern_count(self.client_id);
            drop(pubsub);

            responses.push(RespValue::array(vec![
                RespValue::bulk_string(Bytes::from("unsubscribe")),
                RespValue::bulk_string(channel),
                RespValue::integer(sub_count as i64),
            ]));
        }

        if responses.is_empty() {
            RespValue::array(vec![
                RespValue::bulk_string(Bytes::from("unsubscribe")),
                RespValue::Null,
                RespValue::integer(0),
            ])
        } else if responses.len() == 1 {
            responses.into_iter().next().unwrap()
        } else {
            for resp in &responses[..responses.len() - 1] {
                let data = resp.serialize();
                let _ = self.stream.write_all(&data).await;
            }
            responses.into_iter().last().unwrap()
        }
    }

    async fn handle_psubscribe(&mut self, args: &[Bytes]) -> RespValue {
        let patterns: Vec<Bytes> = args[1..].to_vec();
        let mut responses = Vec::new();

        for pattern in patterns {
            self.subscribed_patterns.insert(pattern.clone());
            let mut pubsub = self.state.pubsub.lock().await;
            let tx = self.pubsub_tx.clone().unwrap();
            pubsub.psubscribe(self.client_id, pattern.clone(), tx);
            let sub_count = pubsub.client_channel_count(self.client_id) + pubsub.client_pattern_count(self.client_id);
            drop(pubsub);

            responses.push(RespValue::array(vec![
                RespValue::bulk_string(Bytes::from("psubscribe")),
                RespValue::bulk_string(pattern),
                RespValue::integer(sub_count as i64),
            ]));
        }

        if responses.len() == 1 {
            responses.into_iter().next().unwrap()
        } else {
            for resp in &responses[..responses.len() - 1] {
                let data = resp.serialize();
                let _ = self.stream.write_all(&data).await;
            }
            responses.into_iter().last().unwrap()
        }
    }

    async fn handle_punsubscribe(&mut self, args: &[Bytes]) -> RespValue {
        let patterns: Vec<Bytes> = if args.len() > 1 {
            args[1..].to_vec()
        } else {
            self.subscribed_patterns.iter().cloned().collect()
        };

        let mut responses = Vec::new();

        for pattern in patterns {
            self.subscribed_patterns.remove(&pattern);
            let mut pubsub = self.state.pubsub.lock().await;
            pubsub.punsubscribe(self.client_id, &pattern);
            let sub_count = pubsub.client_channel_count(self.client_id) + pubsub.client_pattern_count(self.client_id);
            drop(pubsub);

            responses.push(RespValue::array(vec![
                RespValue::bulk_string(Bytes::from("punsubscribe")),
                RespValue::bulk_string(pattern),
                RespValue::integer(sub_count as i64),
            ]));
        }

        if responses.is_empty() {
            RespValue::array(vec![
                RespValue::bulk_string(Bytes::from("punsubscribe")),
                RespValue::Null,
                RespValue::integer(0),
            ])
        } else if responses.len() == 1 {
            responses.into_iter().next().unwrap()
        } else {
            for resp in &responses[..responses.len() - 1] {
                let data = resp.serialize();
                let _ = self.stream.write_all(&data).await;
            }
            responses.into_iter().last().unwrap()
        }
    }

    async fn handle_publish(&self, args: &[Bytes]) -> RespValue {
        if args.len() != 3 {
            return RespValue::wrong_arity("publish");
        }
        let channel = args[1].clone();
        let message = args[2].clone();
        let mut pubsub = self.state.pubsub.lock().await;
        let count = pubsub.publish(&channel, &message);
        RespValue::integer(count)
    }

    async fn handle_pubsub_cmd(&self, args: &[Bytes]) -> RespValue {
        if args.len() < 2 {
            return RespValue::wrong_arity("pubsub");
        }

        let subcmd = String::from_utf8_lossy(&args[1]).to_uppercase();
        let pubsub = self.state.pubsub.lock().await;

        match subcmd.as_str() {
            "CHANNELS" => {
                let pattern = if args.len() > 2 {
                    Some(String::from_utf8_lossy(&args[2]).to_string())
                } else {
                    None
                };

                let channels: Vec<RespValue> = pubsub.channels.keys()
                    .filter(|ch| {
                        if let Some(ref pat) = pattern {
                            let ch_str = String::from_utf8_lossy(ch);
                            glob_match(pat, &ch_str)
                        } else {
                            true
                        }
                    })
                    .map(|ch| RespValue::bulk_string(ch.clone()))
                    .collect();
                RespValue::array(channels)
            }
            "NUMSUB" => {
                let mut result = Vec::new();
                for ch_name in &args[2..] {
                    result.push(RespValue::bulk_string(ch_name.clone()));
                    let count = pubsub.channels.get(ch_name)
                        .map(|s| s.len())
                        .unwrap_or(0);
                    result.push(RespValue::integer(count as i64));
                }
                RespValue::array(result)
            }
            "NUMPAT" => {
                let count: usize = pubsub.patterns.values().map(|s| s.len()).sum();
                RespValue::integer(count as i64)
            }
            _ => RespValue::error(format!("ERR unknown subcommand or wrong number of arguments for 'pubsub|{}'", subcmd.to_lowercase())),
        }
    }

    // === Transaction handlers ===

    fn handle_multi(&mut self) -> RespValue {
        if self.in_multi {
            return RespValue::error("ERR MULTI calls can not be nested");
        }
        self.in_multi = true;
        self.tx_queue.clear();
        self.tx_error = false;
        RespValue::ok()
    }

    async fn handle_exec(&mut self) -> RespValue {
        if !self.in_multi {
            return RespValue::error("ERR EXEC without MULTI");
        }

        self.in_multi = false;

        if self.tx_error {
            self.tx_queue.clear();
            self.tx_error = false;
            self.watch_keys.clear();
            return RespValue::error("EXECABORT Transaction discarded because of previous errors.");
        }

        // Acquire store lock ONCE for the entire transaction (WATCH check + all commands)
        let mut store = self.state.store.lock().await;

        // Check WATCH keys under the same lock
        if !self.watch_keys.is_empty() {
            let db = store.db_mut(self.db_index);
            for (key, version) in &self.watch_keys {
                let current_version = compute_key_version(db, key);
                if *version != current_version {
                    self.tx_queue.clear();
                    self.watch_keys.clear();
                    return RespValue::NullArray;
                }
            }
        }
        self.watch_keys.clear();

        // Execute all queued commands atomically under the same lock
        let queued = std::mem::take(&mut self.tx_queue);
        let mut results = Vec::with_capacity(queued.len());

        for args in queued {
            let mut ctx = CommandContext {
                store: &mut store,
                db_index: self.db_index,
                args,
                start_time: self.state.start_time,
            };
            let result = self.state.registry.execute(&mut ctx);
            self.db_index = ctx.db_index;
            results.push(result);
        }

        drop(store);
        RespValue::array(results)
    }

    fn handle_discard(&mut self) -> RespValue {
        if !self.in_multi {
            return RespValue::error("ERR DISCARD without MULTI");
        }
        self.in_multi = false;
        self.tx_queue.clear();
        self.tx_error = false;
        self.watch_keys.clear();
        RespValue::ok()
    }

    async fn handle_watch(&mut self, args: &[Bytes]) -> RespValue {
        if self.in_multi {
            return RespValue::error("ERR WATCH inside MULTI is not allowed");
        }

        let keys: Vec<Bytes> = args[1..].to_vec();
        let mut store = self.state.store.lock().await;
        let db = store.db_mut(self.db_index);

        for key in keys {
            let version = compute_key_version(db, &key);
            self.watch_keys.push((key, version));
        }

        RespValue::ok()
    }

    fn handle_unwatch(&mut self) -> RespValue {
        self.watch_keys.clear();
        RespValue::ok()
    }
}

/// Compute a simple version/fingerprint of a key's value for WATCH.
/// Returns None if the key doesn't exist.
///
/// Fast path: every Database mutation bumps `db.key_version(key)`, so for any
/// existing key we just return that counter — O(1) regardless of value size.
/// We keep the content-hashing fallback below for keys that pre-date the
/// version counter (e.g., freshly loaded RDB) where the version is unset.
fn compute_key_version(db: &mut crate::storage::Database, key: &Bytes) -> Option<u64> {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    if let Some(v) = db.key_version(key) {
        if v != 0 {
            return Some(v);
        }
    }

    let obj = db.get(key)?;
    let mut hasher = DefaultHasher::new();

    match obj {
        crate::storage::RedisObject::String(b) => {
            0u8.hash(&mut hasher);
            b.hash(&mut hasher);
        }
        crate::storage::RedisObject::List(l) => {
            1u8.hash(&mut hasher);
            l.len().hash(&mut hasher);
            for item in l {
                item.hash(&mut hasher);
            }
        }
        crate::storage::RedisObject::Set(s) => {
            2u8.hash(&mut hasher);
            s.len().hash(&mut hasher);
            // Sort for deterministic hashing
            let mut items: Vec<&Bytes> = s.iter().collect();
            items.sort();
            for item in items {
                item.hash(&mut hasher);
            }
        }
        crate::storage::RedisObject::Hash(h) => {
            3u8.hash(&mut hasher);
            h.len().hash(&mut hasher);
            let mut entries: Vec<(&Bytes, &Bytes)> = h.iter().collect();
            entries.sort_by_key(|(k, _)| *k);
            for (k, v) in entries {
                k.hash(&mut hasher);
                v.hash(&mut hasher);
            }
        }
        crate::storage::RedisObject::SortedSet(z) => {
            4u8.hash(&mut hasher);
            z.len().hash(&mut hasher);
            for (member, score) in &z.members {
                member.hash(&mut hasher);
                score.to_bits().hash(&mut hasher);
            }
        }
        crate::storage::RedisObject::Stream(s) => {
            5u8.hash(&mut hasher);
            s.entries.len().hash(&mut hasher);
            s.last_id.ms.hash(&mut hasher);
            s.last_id.seq.hash(&mut hasher);
        }
        crate::storage::RedisObject::Json(v) => {
            6u8.hash(&mut hasher);
            let s = serde_json::to_string(v).unwrap_or_default();
            s.hash(&mut hasher);
        }
    }

    Some(hasher.finish())
}

/// Determine if a command is a read command (for client-side caching tracking).
fn is_read_command(cmd: &str) -> bool {
    matches!(
        cmd,
        "GET" | "MGET" | "HGET" | "HMGET" | "HGETALL" | "HKEYS" | "HVALS"
            | "LRANGE" | "LINDEX" | "LLEN"
            | "SISMEMBER" | "SMEMBERS" | "SCARD" | "SMISMEMBER"
            | "ZSCORE" | "ZRANGE" | "ZRANK" | "ZCARD" | "ZRANGEBYSCORE"
            | "XRANGE" | "XREVRANGE" | "XLEN"
            | "JSON.GET" | "JSON.TYPE"
            | "STRLEN" | "GETRANGE" | "EXISTS" | "TYPE" | "TTL" | "PTTL"
    )
}
