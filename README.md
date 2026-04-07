# rCache

A high-performance, Redis/Valkey-compatible in-memory data store written in Rust.

## Features

- **287 commands** — 85%+ wire compatibility with Redis 7.2+ / Valkey 8+
- **60-80% faster** than Valkey on point operations (SET, GET, LPUSH, ZADD, etc.)
- **All core data types** — Strings, Lists, Sets, Hashes, Sorted Sets, Streams, JSON
- **Pub/Sub** — Channel and pattern subscriptions with RESP3 push messages
- **Transactions** — Atomic MULTI/EXEC with WATCH optimistic locking
- **Persistence** — RDB snapshots + AOF with three fsync modes
- **Memory management** — LRU, LFU, random, and TTL-based eviction policies
- **Lua scripting** — EVAL/EVALSHA with sandboxed Lua 5.4 runtime
- **ACL** — Per-user command/key/channel permissions
- **RESP2 + RESP3** — Full protocol support with per-connection negotiation
- **Blocking operations** — BLPOP/BRPOP/BZPOPMIN with async wake-on-push
- **TLS** — Built-in via rustls (no OpenSSL dependency)
- **HTTP API** — REST endpoints + Prometheus metrics (`/metrics`)
- **Keyspace notifications** — Configurable event publishing on key mutations

### Beyond Redis

- **JSON data type** — JSON.SET/GET/DEL with JSONPath queries
- **Time series** — TS.CREATE/ADD/RANGE with aggregation and retention
- **Vector search** — VEC.CREATE/ADD/SEARCH with cosine/L2/IP distance metrics
- **Probabilistic structures** — Bloom filters, Count-Min Sketch, Top-K
- **Rate limiting** — RATELIMIT.CHECK (sliding window) and RATELIMIT.ACQUIRE (token bucket)
- **Multi-tenancy** — NAMESPACE isolation with independent keyspaces
- **Data compression** — Transparent LZ4 for large values

## Quick Start

```bash
# Build
cargo build --release

# Run (default port 6379)
./target/release/rcache

# Run with options
./target/release/rcache --port 6380 --requirepass secret --http-port 8080

# Connect with any Redis client
redis-cli -p 6380
```

## Configuration

| Flag | Default | Description |
|------|---------|-------------|
| `--bind` | 0.0.0.0 | Bind address |
| `--port` | 6379 | RESP protocol port |
| `--requirepass` | (none) | Password for AUTH |
| `--databases` | 16 | Number of databases |
| `--maxclients` | 10000 | Max concurrent connections |
| `--maxmemory` | 0 (unlimited) | Memory limit in bytes |
| `--maxmemory-policy` | noeviction | Eviction policy (noeviction, allkeys-lru, volatile-lru, allkeys-lfu, volatile-lfu, allkeys-random, volatile-random, volatile-ttl) |
| `--http-port` | (disabled) | HTTP API + metrics port |
| `--tls-port` | (disabled) | TLS port |
| `--tls-cert-file` | (none) | TLS certificate PEM file |
| `--tls-key-file` | (none) | TLS private key PEM file |
| `--appendonly` | no | Enable AOF persistence |
| `--appendfsync` | everysec | AOF fsync policy (always, everysec, no) |
| `--dbfilename` | dump.rdb | RDB snapshot filename |
| `--notify-keyspace-events` | (disabled) | Keyspace notification flags (KEg$lshzxetA) |
| `--compression-threshold` | 1024 | Compress values larger than N bytes (0 = disabled) |

## HTTP API

When `--http-port` is set:

```bash
# Health check
curl http://localhost:8080/health

# Get/Set/Delete keys
curl http://localhost:8080/api/v1/mykey
curl -X PUT -d "myvalue" http://localhost:8080/api/v1/mykey
curl -X DELETE http://localhost:8080/api/v1/mykey

# Execute any command
curl -X POST -H "Content-Type: application/json" \
  -d '{"command": ["SET", "key", "value", "EX", "60"]}' \
  http://localhost:8080/api/v1/command

# Prometheus metrics
curl http://localhost:8080/metrics
```

## Benchmark

100k requests, 50 concurrent connections (same host):

| Command | rCache | Valkey 8.1.3 | Delta |
|---------|--------|--------------|-------|
| SET | 171,821 rps | 29,700 rps | +479% |
| GET | 173,010 rps | 105,708 rps | +64% |
| LPUSH | 179,211 rps | 102,459 rps | +75% |
| ZADD | 178,571 rps | 100,301 rps | +78% |
| HSET | 177,620 rps | 104,384 rps | +70% |
| MSET | 153,610 rps | 96,154 rps | +60% |
| PING | 173,611 rps | 98,425 rps | +76% |

See [FEATURES.md](FEATURES.md) for a full feature comparison matrix.

## License

MIT License - Copyright (c) 2026 Zeros And Ones LLC
