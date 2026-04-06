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
    buffer: BytesMut,
    client_name: Option<String>,
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
}

impl Connection {
    pub fn new(stream: MaybeTls, state: Arc<SharedState>, client_id: u64) -> Self {
        let authenticated = state.config.requirepass.is_none();
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            stream,
            state,
            client_id,
            db_index: 0,
            authenticated,
            buffer: BytesMut::with_capacity(4096),
            client_name: None,
            subscribed_channels: HashSet::new(),
            subscribed_patterns: HashSet::new(),
            pubsub_rx: Some(rx),
            pubsub_tx: Some(tx),
            in_multi: false,
            tx_queue: Vec::new(),
            tx_error: false,
            watch_keys: Vec::new(),
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
                        while let Some(response) = self.try_process_command().await? {
                            let data = response.serialize();
                            self.stream.write_all(&data).await?;
                        }
                    }
                    Some(msg) = pubsub_rx.recv() => {
                        self.pubsub_rx = Some(pubsub_rx);
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

                // Read more data from the socket
                let n = self.stream.read_buf(&mut self.buffer).await?;
                if n == 0 {
                    self.cleanup_pubsub().await;
                    return Ok(());
                }
            }
        }
    }

    async fn cleanup_pubsub(&mut self) {
        let mut pubsub = self.state.pubsub.lock().await;
        pubsub.remove_client(self.client_id);
    }

    async fn try_process_command(&mut self) -> Result<Option<RespValue>, Box<dyn std::error::Error + Send + Sync>> {
        if self.buffer.is_empty() {
            return Ok(None);
        }

        match Parser::parse(&self.buffer) {
            Ok((value, consumed)) => {
                let _ = self.buffer.split_to(consumed);
                let response = self.execute_command(value).await;
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
            return self.handle_auth(&args);
        }

        // Check authentication
        if !self.authenticated {
            // Allow HELLO before auth
            if cmd_name != "HELLO" {
                return RespValue::error("NOAUTH Authentication required.");
            }
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
            // Process AUTH and SETNAME if present
            let mut i = 2;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                match opt.as_str() {
                    "AUTH" => {
                        if i + 2 >= args.len() {
                            return RespValue::error("ERR syntax error");
                        }
                        // args[i+1] = username, args[i+2] = password
                        let auth_result = self.handle_auth_hello(&args[i + 1], &args[i + 2]);
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
            // Fall through to registry for the HELLO response
        }

        // Handle RESET
        if cmd_name == "RESET" {
            self.in_multi = false;
            self.tx_queue.clear();
            self.tx_error = false;
            self.watch_keys.clear();
            self.db_index = 0;
            // Unsubscribe from all
            let channels: Vec<Bytes> = self.subscribed_channels.iter().cloned().collect();
            for ch in channels {
                let mut pubsub = self.state.pubsub.lock().await;
                pubsub.unsubscribe(self.client_id, &ch);
                self.subscribed_channels.remove(&ch);
            }
            let patterns: Vec<Bytes> = self.subscribed_patterns.iter().cloned().collect();
            for pat in patterns {
                let mut pubsub = self.state.pubsub.lock().await;
                pubsub.punsubscribe(self.client_id, &pat);
                self.subscribed_patterns.remove(&pat);
            }
            return RespValue::simple_string("RESET");
        }

        let cmd_name_for_aof = cmd_name.clone();
        let args_for_aof = args.clone();

        // Execute command with store lock
        let mut store = self.state.store.lock().await;

        // Check memory eviction before write commands
        if crate::persistence::aof::is_write_command(&cmd_name) {
            let maxmemory = self.state.config.maxmemory;
            let policy = self.state.config.maxmemory_policy.clone();
            let samples = self.state.config.maxmemory_samples;
            let lfu_log_factor = self.state.config.lfu_log_factor;
            let lfu_decay_time = self.state.config.lfu_decay_time;
            if store.check_memory_limit(maxmemory, &policy, samples, lfu_log_factor, lfu_decay_time).is_err() {
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

        // Append to AOF if this was a write command and it succeeded
        if crate::persistence::aof::is_write_command(&cmd_name_for_aof) {
            if !matches!(result, RespValue::Error(_)) {
                let mut aof = self.state.aof_writer.lock().await;
                if let Some(writer) = aof.as_mut() {
                    if let Err(e) = writer.append(&args_for_aof) {
                        tracing::error!("AOF write error: {}", e);
                    }
                }
            }
        }

        result
    }

    fn handle_auth(&mut self, args: &[Bytes]) -> RespValue {
        if args.len() < 2 {
            return RespValue::wrong_arity("auth");
        }

        match &self.state.config.requirepass {
            Some(password) => {
                let provided = if args.len() >= 3 {
                    // AUTH username password (Redis 6+ ACL style, ignore username for now)
                    String::from_utf8_lossy(&args[2]).to_string()
                } else {
                    String::from_utf8_lossy(&args[1]).to_string()
                };

                if provided == *password {
                    self.authenticated = true;
                    RespValue::ok()
                } else {
                    RespValue::error("WRONGPASS invalid username-password pair or user is disabled.")
                }
            }
            None => {
                RespValue::error("ERR Client sent AUTH, but no password is set. Did you mean ACL SETUSER with >password?")
            }
        }
    }

    fn handle_auth_hello(&mut self, _username: &Bytes, password: &Bytes) -> Option<RespValue> {
        match &self.state.config.requirepass {
            Some(pass) => {
                let provided = String::from_utf8_lossy(password).to_string();
                if provided == *pass {
                    self.authenticated = true;
                    None
                } else {
                    Some(RespValue::error("WRONGPASS invalid username-password pair or user is disabled."))
                }
            }
            None => {
                Some(RespValue::error("ERR Client sent AUTH, but no password is set."))
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
        let pubsub = self.state.pubsub.lock().await;
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

        // Check WATCH keys
        if !self.watch_keys.is_empty() {
            let mut store = self.state.store.lock().await;
            let db = store.db_mut(self.db_index);
            for (key, version) in &self.watch_keys {
                let current_version = compute_key_version(db, key);
                if *version != current_version {
                    self.tx_queue.clear();
                    self.watch_keys.clear();
                    return RespValue::NullArray;
                }
            }
            drop(store);
        }
        self.watch_keys.clear();

        let queued = std::mem::take(&mut self.tx_queue);
        let mut results = Vec::with_capacity(queued.len());

        for args in queued {
            let mut store = self.state.store.lock().await;
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
fn compute_key_version(db: &mut crate::storage::Database, key: &Bytes) -> Option<u64> {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

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
