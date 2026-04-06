use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde_json::{json, Value};
use tokio::net::TcpListener;

use crate::command::CommandContext;
use crate::protocol::RespValue;
use crate::server::SharedState;

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub async fn run_http_server(state: Arc<SharedState>, port: u16) -> Result<(), BoxError> {
    let addr = format!("{}:{}", state.config.bind, port);
    let listener = TcpListener::bind(&addr).await?;
    tracing::info!("HTTP API listening on {}", addr);

    loop {
        let (stream, _addr) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let state = Arc::clone(&state);

        tokio::spawn(async move {
            let state = state;
            let svc = service_fn(move |req: Request<Incoming>| {
                let state = Arc::clone(&state);
                async move {
                    Ok::<_, hyper::Error>(
                        handle_request(req, state)
                            .await
                            .unwrap_or_else(|e| {
                                let body = serde_json::to_vec(&json!({"error": e.to_string()}))
                                    .unwrap_or_default();
                                Response::builder()
                                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                                    .header("Content-Type", "application/json")
                                    .body(Full::new(Bytes::from(body)))
                                    .unwrap()
                            }),
                    )
                }
            });

            if let Err(e) = http1::Builder::new().serve_connection(io, svc).await {
                tracing::debug!("HTTP connection error: {}", e);
            }
        });
    }
}

async fn handle_request(
    req: Request<Incoming>,
    state: Arc<SharedState>,
) -> Result<Response<Full<Bytes>>, BoxError> {
    let method = req.method().clone();
    let path = req.uri().path().to_string();

    let result = match (method, path.as_str()) {
        (Method::GET, "/health") => handle_health(&state),
        (Method::GET, "/info") => handle_info(&state).await,
        (Method::GET, "/metrics") => handle_metrics(&state).await,
        (Method::POST, "/api/v1/command") => {
            let body = req.collect().await?.to_bytes();
            handle_command(&state, &body).await
        }
        (Method::GET, p) if p.starts_with("/api/v1/") => {
            let key = &p["/api/v1/".len()..];
            handle_get_key(&state, key).await
        }
        (Method::PUT, p) if p.starts_with("/api/v1/") => {
            let key = &p["/api/v1/".len()..];
            let body = req.collect().await?.to_bytes();
            handle_put_key(&state, key, &body).await
        }
        (Method::DELETE, p) if p.starts_with("/api/v1/") => {
            let key = &p["/api/v1/".len()..];
            handle_delete_key(&state, key).await
        }
        _ => json_response(StatusCode::NOT_FOUND, &json!({"error": "not found"})),
    };

    result
}

fn handle_health(state: &SharedState) -> Result<Response<Full<Bytes>>, BoxError> {
    let uptime = state.start_time.elapsed().as_secs();
    json_response(StatusCode::OK, &json!({
        "status": "ok",
        "uptime_secs": uptime,
    }))
}

async fn handle_info(state: &SharedState) -> Result<Response<Full<Bytes>>, BoxError> {
    let uptime = state.start_time.elapsed();
    let store = state.store.lock().await;

    let mut keyspace = serde_json::Map::new();
    for i in 0..store.db_count() {
        let db = store.db(i);
        let keys = db.len();
        let expires = db.expires_len();
        if keys > 0 {
            keyspace.insert(
                format!("db{}", i),
                json!({"keys": keys, "expires": expires}),
            );
        }
    }

    let used_memory = store.total_used_memory();
    drop(store);

    let info = json!({
        "server": {
            "redis_version": "7.2.0",
            "rcache_version": env!("CARGO_PKG_VERSION"),
            "redis_mode": "standalone",
            "os": std::env::consts::OS,
            "uptime_in_seconds": uptime.as_secs(),
            "uptime_in_days": uptime.as_secs() / 86400,
        },
        "clients": {
            "connected_clients": state.connected_clients.load(Ordering::Relaxed),
        },
        "memory": {
            "used_memory": used_memory,
        },
        "stats": {
            "total_connections_received": state.connections_total.load(Ordering::Relaxed),
            "total_commands_processed": state.commands_processed.load(Ordering::Relaxed),
            "keyspace_hits": state.keyspace_hits.load(Ordering::Relaxed),
            "keyspace_misses": state.keyspace_misses.load(Ordering::Relaxed),
        },
        "replication": {
            "role": "master",
            "connected_slaves": 0,
        },
        "keyspace": keyspace,
    });

    json_response(StatusCode::OK, &info)
}

async fn handle_metrics(state: &SharedState) -> Result<Response<Full<Bytes>>, BoxError> {
    let uptime = state.start_time.elapsed().as_secs();
    let store = state.store.lock().await;
    let used_memory = store.total_used_memory();

    let mut keyspace_lines = String::new();
    for i in 0..store.db_count() {
        let db = store.db(i);
        let keys = db.len();
        if keys > 0 {
            keyspace_lines.push_str(&format!(
                "rcache_keys_total{{db=\"{}\"}} {}\n",
                i, keys
            ));
        }
    }
    drop(store);

    let commands_processed = state.commands_processed.load(Ordering::Relaxed);
    let connected_clients = state.connected_clients.load(Ordering::Relaxed);
    let keyspace_hits = state.keyspace_hits.load(Ordering::Relaxed);
    let keyspace_misses = state.keyspace_misses.load(Ordering::Relaxed);
    let connections_total = state.connections_total.load(Ordering::Relaxed);

    let body = format!(
        "# HELP rcache_commands_total Total commands processed\n\
         # TYPE rcache_commands_total counter\n\
         rcache_commands_total {commands_processed}\n\
         # HELP rcache_connected_clients Number of connected clients\n\
         # TYPE rcache_connected_clients gauge\n\
         rcache_connected_clients {connected_clients}\n\
         # HELP rcache_used_memory_bytes Memory used by the store in bytes\n\
         # TYPE rcache_used_memory_bytes gauge\n\
         rcache_used_memory_bytes {used_memory}\n\
         # HELP rcache_keyspace_hits_total Successful key lookups\n\
         # TYPE rcache_keyspace_hits_total counter\n\
         rcache_keyspace_hits_total {keyspace_hits}\n\
         # HELP rcache_keyspace_misses_total Failed key lookups\n\
         # TYPE rcache_keyspace_misses_total counter\n\
         rcache_keyspace_misses_total {keyspace_misses}\n\
         # HELP rcache_connections_total Total connections accepted\n\
         # TYPE rcache_connections_total counter\n\
         rcache_connections_total {connections_total}\n\
         # HELP rcache_uptime_seconds Server uptime in seconds\n\
         # TYPE rcache_uptime_seconds gauge\n\
         rcache_uptime_seconds {uptime}\n\
         # HELP rcache_keys_total Number of keys per database\n\
         # TYPE rcache_keys_total gauge\n\
         {keyspace_lines}",
    );

    let response = Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        .body(Full::new(Bytes::from(body)))?;
    Ok(response)
}

async fn handle_get_key(
    state: &SharedState,
    key: &str,
) -> Result<Response<Full<Bytes>>, BoxError> {
    let key_bytes = Bytes::from(key.to_string());
    let mut store = state.store.lock().await;
    let db = store.db_mut(0);

    let ttl = db.ttl_ms(&key_bytes).unwrap_or(-2);

    match db.get(&key_bytes) {
        Some(obj) => {
            state.keyspace_hits.fetch_add(1, Ordering::Relaxed);
            let type_name = obj.type_name().to_string();
            let value = resp_object_to_json(obj);
            drop(store);

            json_response(StatusCode::OK, &json!({
                "value": value,
                "type": type_name,
                "ttl": ttl,
            }))
        }
        None => {
            state.keyspace_misses.fetch_add(1, Ordering::Relaxed);
            drop(store);
            json_response(StatusCode::NOT_FOUND, &json!({"error": "key not found"}))
        }
    }
}

async fn handle_put_key(
    state: &SharedState,
    key: &str,
    body: &[u8],
) -> Result<Response<Full<Bytes>>, BoxError> {
    let key_bytes = Bytes::from(key.to_string());
    let value = Bytes::from(body.to_vec());
    let mut store = state.store.lock().await;
    let db = store.db_mut(0);
    db.set(key_bytes, crate::storage::RedisObject::String(value));
    drop(store);

    json_response(StatusCode::OK, &json!({"status": "OK"}))
}

async fn handle_delete_key(
    state: &SharedState,
    key: &str,
) -> Result<Response<Full<Bytes>>, BoxError> {
    let key_bytes = Bytes::from(key.to_string());
    let mut store = state.store.lock().await;
    let db = store.db_mut(0);
    let deleted = if db.remove(&key_bytes).is_some() { 1 } else { 0 };
    drop(store);

    json_response(StatusCode::OK, &json!({"deleted": deleted}))
}

async fn handle_command(
    state: &SharedState,
    body: &[u8],
) -> Result<Response<Full<Bytes>>, BoxError> {
    let parsed: Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": format!("invalid JSON: {}", e)}),
            );
        }
    };

    let cmd_array = match parsed.get("command").and_then(|v| v.as_array()) {
        Some(arr) => arr,
        None => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": "expected {\"command\": [\"CMD\", \"arg1\", ...]}"}),
            );
        }
    };

    let args: Vec<Bytes> = cmd_array
        .iter()
        .map(|v| {
            let s = match v.as_str() {
                Some(s) => s.to_string(),
                None => v.to_string(),
            };
            Bytes::from(s)
        })
        .collect();

    if args.is_empty() {
        return json_response(
            StatusCode::BAD_REQUEST,
            &json!({"error": "empty command"}),
        );
    }

    let mut store = state.store.lock().await;
    let mut ctx = CommandContext {
        store: &mut store,
        db_index: 0,
        args,
        start_time: state.start_time,
    };
    let result = state.registry.execute(&mut ctx);
    drop(store);
    state.commands_processed.fetch_add(1, Ordering::Relaxed);

    let json_result = resp_value_to_json(&result);
    json_response(StatusCode::OK, &json!({"result": json_result}))
}

fn resp_object_to_json(obj: &crate::storage::RedisObject) -> Value {
    use crate::storage::RedisObject;
    match obj {
        RedisObject::String(b) => {
            Value::String(String::from_utf8_lossy(b).to_string())
        }
        RedisObject::List(l) => {
            Value::Array(l.iter().map(|v| Value::String(String::from_utf8_lossy(v).to_string())).collect())
        }
        RedisObject::Set(s) => {
            Value::Array(s.iter().map(|v| Value::String(String::from_utf8_lossy(v).to_string())).collect())
        }
        RedisObject::Hash(h) => {
            let map: serde_json::Map<String, Value> = h
                .iter()
                .map(|(k, v)| {
                    (
                        String::from_utf8_lossy(k).to_string(),
                        Value::String(String::from_utf8_lossy(v).to_string()),
                    )
                })
                .collect();
            Value::Object(map)
        }
        RedisObject::SortedSet(z) => {
            let members: Vec<Value> = z
                .scores
                .iter()
                .map(|(k, _)| {
                    json!({
                        "member": String::from_utf8_lossy(&k.member).to_string(),
                        "score": k.score,
                    })
                })
                .collect();
            Value::Array(members)
        }
        RedisObject::Stream(s) => {
            json!({
                "length": s.entries.len(),
                "last_id": s.last_id.to_string(),
            })
        }
        RedisObject::Json(v) => v.clone(),
    }
}

fn resp_value_to_json(value: &RespValue) -> Value {
    match value {
        RespValue::SimpleString(s) => Value::String(s.clone()),
        RespValue::Error(s) => json!({"error": s}),
        RespValue::Integer(n) => json!(n),
        RespValue::BulkString(b) => Value::String(String::from_utf8_lossy(b).to_string()),
        RespValue::Array(items) => {
            Value::Array(items.iter().map(resp_value_to_json).collect())
        }
        RespValue::Null => Value::Null,
        RespValue::NullArray => Value::Null,
    }
}

fn json_response(
    status: StatusCode,
    body: &Value,
) -> Result<Response<Full<Bytes>>, BoxError> {
    let json_bytes = serde_json::to_vec(body)?;
    let response = Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .body(Full::new(Bytes::from(json_bytes)))?;
    Ok(response)
}
