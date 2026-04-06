# rCache v2 — 100% Valkey Parity + New Features

## Part A: Closing the Gap (100% Valkey Parity)

### A1: Missing Command Registrations (Quick Wins)

Commands that are handled in connection.rs but not registered (clients probing via COMMAND INFO will think they're missing):

- [ ] Register `AUTH` (currently handled in connection layer only)
- [ ] Register `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH` (handled in connection layer)
- [ ] Register `SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE`, `PUBLISH`, `PUBSUB` (handled in connection layer)
- [ ] Register `QUIT` (handled in connection layer)

Missing aliases and deprecated commands (trivial to add):

- [ ] `HMSET` — alias for `HSET` (deprecated but widely used)
- [ ] `RPOPLPUSH` — alias for `LMOVE src dst RIGHT LEFT` (deprecated)
- [ ] `BRPOPLPUSH` — alias for `BLMOVE src dst RIGHT LEFT timeout` (deprecated)
- [ ] `HSTRLEN` — return string length of hash field value
- [ ] `GEORADIUS` / `GEORADIUSBYMEMBER` — deprecated, wrap `GEOSEARCH`
- [ ] `GEORADIUS_RO` / `GEORADIUSBYMEMBER_RO` — read-only variants
- [ ] `ZRANGEBYLEX`, `ZREVRANGEBYLEX` — deprecated, wrap `ZRANGE BYLEX`
- [ ] `ZREMRANGEBYLEX`, `ZREMRANGEBYRANK`, `ZREMRANGEBYSCORE` — missing removal commands
- [ ] `XSETID` — set stream last ID without adding entry
- [ ] `MOVE key db` — move key to another database
- [ ] `ROLE` — return replication role info (master, list of replicas)
- [ ] `SHUTDOWN [NOSAVE|SAVE]` — graceful server shutdown
- [ ] `DUMP` / `RESTORE` / `RESTORE-ASKING` — serialize/deserialize keys
- [ ] `MIGRATE` — transfer key to another server
- [ ] `MONITOR` — stream all commands (connection-level, like pub/sub)
- [ ] `SYNC` / `PSYNC` — internal replication commands (stub)
- [ ] `FAILOVER` — trigger manual failover (stub)
- [ ] `WAITAOF` — wait for AOF fsync confirmation
- [ ] `EVAL_RO` / `EVALSHA_RO` — read-only script execution variants (stub)
- [ ] `SPUBLISH`, `SSUBSCRIBE`, `SUNSUBSCRIBE` — sharded pub/sub (stub)
- [ ] `COMMANDLOG` — command logging (Valkey 8+ replacement for SLOWLOG)
- [ ] `PFDEBUG`, `PFSELFTEST` — HyperLogLog internal debug commands (stub)

### A2: RESP3 Protocol

- [ ] Add RESP3 types to `RespValue`: Null, Double, Boolean, BlobError, VerbatimString, BigNumber, Map, Set, Push, Attribute
- [ ] RESP3 serializer alongside RESP2
- [ ] Per-connection protocol version tracking (`resp_version: u8`)
- [ ] `HELLO 3` upgrades connection to RESP3
- [ ] RESP3-aware response formatting (maps instead of flat arrays for HGETALL, etc.)
- [ ] Push messages (`>` type) for pub/sub in RESP3 mode

### A3: Blocking Operations (Full Implementation)

Replace non-blocking stubs with real blocking infrastructure:

- [ ] Blocking waitlist in SharedState: `HashMap<(db_index, key), Vec<BlockedClient>>`
- [ ] `BlockedClient` struct: client_id, oneshot sender, timeout, operation type
- [ ] On LPUSH/RPUSH: check waitlist, wake first blocked client with the pushed value
- [ ] On ZADD: check waitlist for BZPOPMIN/BZPOPMAX
- [ ] On XADD: check waitlist for XREAD BLOCK/XREADGROUP BLOCK
- [ ] Timeout handling: tokio::time::timeout wrapping the blocked wait
- [ ] `BLPOP key [key ...] timeout` — full blocking implementation
- [ ] `BRPOP key [key ...] timeout`
- [ ] `BLMOVE source destination LEFT|RIGHT LEFT|RIGHT timeout`
- [ ] `BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT count]`
- [ ] `BZPOPMIN key [key ...] timeout`
- [ ] `BZPOPMAX key [key ...] timeout`
- [ ] `BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT count]`
- [ ] `XREAD ... BLOCK ms ...` — full blocking
- [ ] `XREADGROUP ... BLOCK ms ...` — full blocking
- [ ] `CLIENT UNBLOCK client-id [TIMEOUT|ERROR]`

### A4: Lua Scripting

- [ ] Add `mlua` dependency (Lua 5.4 runtime)
- [ ] Sandbox creation: limited Lua environment (no os, io, file access)
- [ ] `redis.call(cmd, ...)` — execute Redis command from Lua, propagate errors
- [ ] `redis.pcall(cmd, ...)` — protected call, return error as table
- [ ] `redis.log(level, message)` — server logging from scripts
- [ ] `redis.error_reply(msg)` / `redis.status_reply(msg)` — response helpers
- [ ] KEYS/ARGV binding: pass declared keys and arguments to script
- [ ] `EVAL script numkeys key [key ...] arg [arg ...]` — full execution
- [ ] `EVALSHA sha1 numkeys ...` — execute cached script by SHA1
- [ ] `EVAL_RO` / `EVALSHA_RO` — read-only variants
- [ ] SHA1-based script cache (`HashMap<String, String>`)
- [ ] `SCRIPT LOAD script` — cache and return SHA1
- [ ] `SCRIPT EXISTS sha1 [sha1 ...]` — check cache
- [ ] `SCRIPT FLUSH [ASYNC|SYNC]` — clear cache
- [ ] `SCRIPT KILL` — interrupt running read-only script
- [ ] `lua-time-limit` config (default 5000ms)
- [ ] Atomic execution: script holds command lock for duration
- [ ] Type conversion: Lua -> RESP and RESP -> Lua

### A5: Redis Functions

- [ ] Function library storage: `HashMap<String, FunctionLibrary>`
- [ ] `FUNCTION LOAD [REPLACE] library-code` — parse and register Lua functions
- [ ] `FCALL function numkeys key [key ...] arg [arg ...]` — call named function
- [ ] `FCALL_RO` — read-only variant
- [ ] `FUNCTION LIST [LIBRARYNAME pattern] [WITHCODE]`
- [ ] `FUNCTION DELETE library-name`
- [ ] `FUNCTION DUMP` / `FUNCTION RESTORE` — serialize/deserialize
- [ ] `FUNCTION STATS` — execution statistics
- [ ] `FUNCTION FLUSH [ASYNC|SYNC]`
- [ ] Persist functions in RDB and AOF

### A6: Full ACL System

- [ ] User registry: `HashMap<String, AclUser>`
- [ ] `AclUser` struct: enabled, passwords (hashed), allowed_commands (bitmask), allowed_categories, key_patterns, channel_patterns
- [ ] Command category bitmasks (@read, @write, @admin, @dangerous, @slow, @fast, etc.)
- [ ] ACL check on every command dispatch (before execution)
- [ ] Key pattern matching: `~pattern`, `%R~pattern` (read-only), `%W~pattern` (write-only)
- [ ] Channel pattern matching: `&pattern`
- [ ] `ACL SETUSER username [rules ...]` — full rule parsing
- [ ] `ACL GETUSER username` — return user details
- [ ] `ACL DELUSER username [username ...]`
- [ ] `ACL GENPASS [bits]` — generate random password
- [ ] `ACL DRYRUN username command [args ...]` — test permission without executing
- [ ] `ACL SAVE` / `ACL LOAD` — persist to/from file
- [ ] `AUTH username password` — full multi-user auth

### A7: Persistence Hardening

- [ ] Standard RDB format compatibility (match Redis/Valkey wire format)
- [ ] CRC64 checksum generation and verification
- [ ] Background BGSAVE via `libc::fork()` (true copy-on-write)
- [ ] `save` config directive: periodic auto-save based on changes + time
- [ ] Save on graceful SHUTDOWN
- [ ] Multi-Part AOF (MP-AOF): base file + incremental files + manifest
- [ ] `aof-use-rdb-preamble yes` — hybrid AOF format
- [ ] `WAITAOF numlocal numreplicas timeout`

### A8: Compact Memory Encodings

- [ ] `listpack` encoding for small lists, sets, hashes, sorted sets (< 128 entries, < 64 bytes each)
- [ ] `quicklist` encoding for large lists (linked list of listpacks)
- [ ] `intset` encoding for small integer-only sets
- [ ] Automatic encoding promotion when thresholds are exceeded
- [ ] `list-max-listpack-size`, `set-max-listpack-entries`, `hash-max-listpack-entries`, `zset-max-listpack-entries` configs

### A9: LFU Eviction

- [ ] LFU counter: 8-bit logarithmic counter + 16-bit decay timestamp
- [ ] Probabilistic increment: `1 / (counter * lfu-log-factor + 1)`
- [ ] Time-based decay: decrement based on `lfu-decay-time` minutes
- [ ] `allkeys-lfu` policy
- [ ] `volatile-lfu` policy
- [ ] `OBJECT FREQ key` — return LFU frequency counter
- [ ] Eviction pool: maintain 16 best eviction candidates across sampling rounds

### A10: Replication

- [ ] Replication state machine per replica: handshake -> full sync -> streaming
- [ ] Replication backlog: circular buffer of serialized write commands
- [ ] Replication ID (40-char random hex) + offset tracking
- [ ] Second replication ID for failover continuity
- [ ] Full sync: generate RDB, stream to replica, send buffered writes
- [ ] Partial resync: `PSYNC repl-id offset` — resume from backlog
- [ ] Command propagation: every write appended to backlog + sent to replicas
- [ ] `REPLICAOF host port` — initiate replication as replica
- [ ] `REPLICAOF NO ONE` — promote to master
- [ ] `WAIT numreplicas timeout` — block until N replicas ack
- [ ] `replica-read-only yes` enforcement
- [ ] `replica-serve-stale-data` configuration
- [ ] `min-replicas-to-write`, `min-replicas-max-lag`
- [ ] Replica-side expiry: wait for master DEL, don't independently expire
- [ ] `ROLE` command with proper replication info

### A11: Cluster Mode

- [ ] Cluster config: `cluster-enabled yes`
- [ ] Hash slot ownership table: `[Option<NodeId>; 16384]`
- [ ] Cluster bus: separate TCP port (port + 10000) for node-to-node gossip
- [ ] Binary gossip protocol: PING/PONG with node state, slot bitmap, flags
- [ ] `CLUSTER MEET ip port` — node discovery
- [ ] Failure detection: PFAIL (suspected) -> FAIL (confirmed by majority)
- [ ] `-MOVED slot host:port` redirections
- [ ] `-ASK slot host:port` for in-progress migrations
- [ ] `ASKING` + single-command passthrough
- [ ] `-CROSSSLOTS` error for multi-key commands spanning slots
- [ ] `CLUSTER ADDSLOTS` / `DELSLOTS` / `ADDSLOTSRANGE` / `DELSLOTSRANGE`
- [ ] `CLUSTER SETSLOT slot MIGRATING|IMPORTING|NODE|STABLE`
- [ ] `MIGRATE` command for slot migration
- [ ] Raft-like leader election for automatic failover
- [ ] Epoch-based configuration versioning
- [ ] `CLUSTER FAILOVER [FORCE|TAKEOVER]`
- [ ] `CLUSTER REPLICATE node-id`
- [ ] Sharded pub/sub: `SSUBSCRIBE`, `SUNSUBSCRIBE`, `SPUBLISH`
- [ ] `READONLY` / `READWRITE` — read from replicas in cluster

### A12: Keyspace Notifications

- [ ] `notify-keyspace-events` config (K, E, g, $, l, s, h, z, x, e, t, m flags)
- [ ] Publish on `__keyevent@<db>__:<event>` channels on key mutation
- [ ] Publish on `__keyspace@<db>__:<key>` channels on key mutation
- [ ] Integrate with pub/sub system
- [ ] Notification on: SET, DEL, EXPIRE, RENAME, eviction, expiration

### A13: Client-Side Caching

- [ ] Track which keys each client has read (per-client key set)
- [ ] On key mutation: send invalidation message to tracking clients
- [ ] `CLIENT TRACKING ON|OFF [REDIRECT id] [PREFIX prefix] [BCAST] [OPTIN] [OPTOUT] [NOLOOP]`
- [ ] Broadcasting mode: subscribe to key prefixes, invalidate on any match
- [ ] OPTIN/OPTOUT modes: per-command tracking control
- [ ] `CLIENT CACHING YES|NO`
- [ ] RESP3 push invalidation messages

### A14: Module / Plugin System

- [ ] `RCacheModule` trait for Rust-based plugins
- [ ] Dynamic library loading via `libloading` crate
- [ ] Command registration from modules
- [ ] Custom data type registration with RDB save/load, AOF rewrite, memory reporting
- [ ] Timer API: schedule callbacks at intervals
- [ ] Event hooks: key mutations, client connect/disconnect, server lifecycle
- [ ] Thread-safe context for background operations
- [ ] `MODULE LOAD path [arg ...]`
- [ ] `MODULE UNLOAD name`
- [ ] `MODULE LIST`
- [ ] `MODULE LOADEX path [CONFIG name value ...] [ARGS arg ...]`

---

## Part B: New Features (rCache Differentiators)

### B1: Native JSON Data Type

Built-in JSON support (equivalent to RedisJSON module, but native):

- [ ] `JSON.SET key path value` — set JSON value at path
- [ ] `JSON.GET key [path ...]` — get JSON value(s)
- [ ] `JSON.DEL key [path]` — delete JSON value at path
- [ ] `JSON.NUMINCRBY key path value` — increment number
- [ ] `JSON.STRAPPEND key path value` — append to string
- [ ] `JSON.ARRAPPEND key path value [value ...]` — append to array
- [ ] `JSON.ARRLEN key [path]` — array length
- [ ] `JSON.ARRPOP key [path [index]]` — pop from array
- [ ] `JSON.TYPE key [path]` — return JSON type
- [ ] `JSON.OBJKEYS key [path]` — object keys
- [ ] `JSON.OBJLEN key [path]` — object key count
- [ ] JSONPath query support
- [ ] Stored as `RedisObject::Json(serde_json::Value)`
- [ ] Indexes on JSON fields for fast lookups

### B2: Probabilistic Data Structures

Built-in (equivalent to RedisBloom module):

- [ ] `BF.ADD key item` / `BF.EXISTS key item` — Bloom filter
- [ ] `BF.MADD` / `BF.MEXISTS` — multi-item variants
- [ ] `BF.RESERVE key error_rate capacity` — create with parameters
- [ ] `BF.INFO key` — filter stats
- [ ] `CF.ADD` / `CF.EXISTS` / `CF.DEL` — Cuckoo filter (supports deletion)
- [ ] `CF.RESERVE`, `CF.INFO`
- [ ] `CMS.INCRBY key item increment` — Count-Min Sketch
- [ ] `CMS.QUERY key item [item ...]` — query frequency
- [ ] `CMS.MERGE destkey numkeys src [src ...]`
- [ ] `TOPK.ADD key item [item ...]` — Top-K tracking
- [ ] `TOPK.QUERY key item [item ...]` — check if in top-k
- [ ] `TOPK.LIST key [WITHCOUNT]` — list top-k items
- [ ] `TOPK.RESERVE key topk [width depth decay]`

### B3: Time Series Data Type

Built-in (equivalent to RedisTimeSeries module):

- [ ] `TS.CREATE key [RETENTION ms] [LABELS label value ...]`
- [ ] `TS.ADD key timestamp value [LABELS ...]`
- [ ] `TS.MADD key timestamp value [key timestamp value ...]`
- [ ] `TS.GET key` — latest sample
- [ ] `TS.RANGE key fromTimestamp toTimestamp [COUNT count] [AGGREGATION type timeBucket]`
- [ ] `TS.REVRANGE` — reverse range
- [ ] `TS.MRANGE` / `TS.MREVRANGE` — multi-key with label filters
- [ ] Aggregation types: avg, sum, min, max, count, first, last, std.p, std.s, var.p, var.s
- [ ] Automatic downsampling rules: `TS.CREATERULE src dst AGGREGATION type timeBucket`
- [ ] Retention policy: auto-delete samples older than threshold
- [ ] Label-based indexing for `TS.MRANGE` queries

### B4: Vector Similarity Search

For AI/ML embedding workloads:

- [ ] `VEC.CREATE key DIM dimensions DISTANCE_METRIC [COSINE|L2|IP] [CAPACITY initial_cap]`
- [ ] `VEC.ADD key id vector [PAYLOAD json]` — add vector with optional metadata
- [ ] `VEC.DEL key id` — remove vector
- [ ] `VEC.SEARCH key vector K [FILTER expression]` — K-nearest neighbors
- [ ] `VEC.INFO key` — index statistics
- [ ] HNSW index for approximate nearest neighbor
- [ ] Flat (brute-force) index for exact search
- [ ] Filter expressions on payload fields
- [ ] Stored as `RedisObject::VectorIndex(VectorData)`
- [ ] Batch insertion: `VEC.MADD`

### B5: Built-in HTTP/REST API

Serve an HTTP API alongside RESP protocol:

- [ ] `--http-port 8080` config option
- [ ] `GET /api/v1/{key}` — get key value
- [ ] `PUT /api/v1/{key}` — set key value (body = value)
- [ ] `DELETE /api/v1/{key}` — delete key
- [ ] `POST /api/v1/command` — execute arbitrary command (JSON body)
- [ ] `GET /api/v1/health` — health check
- [ ] `GET /api/v1/info` — server info
- [ ] `GET /metrics` — Prometheus-compatible metrics endpoint
- [ ] JSON request/response format
- [ ] Optional auth via Bearer token (maps to requirepass)
- [ ] CORS support

### B6: Built-in TLS

Native TLS support without external proxy:

- [ ] `--tls-port 6380` config
- [ ] `--tls-cert-file`, `--tls-key-file`, `--tls-ca-cert-file`
- [ ] `rustls` based (no OpenSSL dependency)
- [ ] Support TLS on both RESP and HTTP ports
- [ ] `--tls-auth-clients yes|no|optional` — mutual TLS

### B7: Multi-Tenancy / Namespaces

Isolated keyspaces beyond the 16 database limit:

- [ ] `NAMESPACE CREATE name [MAXMEMORY bytes]` — create isolated namespace
- [ ] `NAMESPACE SELECT name` — switch to namespace (connection-scoped)
- [ ] `NAMESPACE LIST` — list namespaces
- [ ] `NAMESPACE DELETE name` — delete namespace and all keys
- [ ] `NAMESPACE INFO name` — stats per namespace
- [ ] Per-namespace maxmemory limits
- [ ] Per-namespace ACL: restrict users to specific namespaces
- [ ] Full keyspace isolation (no cross-namespace access)

### B8: Built-in Rate Limiting

First-class rate limiting primitives:

- [ ] `RATELIMIT.CHECK key limit window_ms` — check if under limit, increment counter
- [ ] `RATELIMIT.GET key` — get current count and remaining
- [ ] `RATELIMIT.RESET key` — reset counter
- [ ] Sliding window algorithm (more accurate than fixed window)
- [ ] Token bucket variant: `RATELIMIT.ACQUIRE key rate capacity`
- [ ] Lua-free atomic rate limiting (single command, no race conditions)

### B9: Observability

Built-in monitoring beyond INFO:

- [ ] `GET /metrics` — Prometheus metrics (commands_processed, connections, memory, keyspace_hits/misses, latency histograms)
- [ ] `--metrics-port 9090` or combined with HTTP API
- [ ] Per-command latency tracking (p50, p95, p99)
- [ ] Slow command log with configurable threshold
- [ ] Connection tracking (per-client command count, bytes in/out)
- [ ] Real-time MONITOR streaming via WebSocket

### B10: Data Compression

Transparent compression for large values:

- [ ] Auto-compress values above configurable threshold (default 1KB)
- [ ] LZ4 compression (fast, good ratio for typical workloads)
- [ ] `--compression-threshold 1024` config
- [ ] `--compression-algorithm lz4|zstd|none`
- [ ] Transparent to clients (decompress on read, compress on write)
- [ ] MEMORY USAGE reports compressed vs uncompressed size
- [ ] Skip compression for values that don't compress well (< 10% savings)

---

## Implementation Priority

### Tier 1 — High Impact, Achievable Now
1. **A1**: Missing command registrations (1 day)
2. **A2**: RESP3 protocol (3 days)
3. **A3**: Blocking operations (3 days)
4. **A9**: LFU eviction (1 day)
5. **B5**: HTTP/REST API + Prometheus metrics (2 days)
6. **B6**: Built-in TLS (1 day)

### Tier 2 — Important for Production Use
7. **A4**: Lua scripting (5 days)
8. **A6**: Full ACL system (3 days)
9. **A7**: Persistence hardening (3 days)
10. **A12**: Keyspace notifications (1 day)
11. **B9**: Observability / metrics (2 days)
12. **B10**: Data compression (2 days)

### Tier 3 — Competitive Differentiators
13. **B1**: Native JSON data type (3 days)
14. **B2**: Probabilistic data structures (3 days)
15. **B3**: Time series data type (3 days)
16. **B4**: Vector similarity search (5 days)
17. **B8**: Built-in rate limiting (1 day)
18. **B7**: Multi-tenancy (2 days)

### Tier 4 — Enterprise / Scale
19. **A10**: Replication (10 days)
20. **A11**: Cluster mode (15 days)
21. **A14**: Module / plugin system (5 days)
22. **A8**: Compact memory encodings (5 days)
23. **A5**: Redis Functions (2 days)
24. **A13**: Client-side caching (2 days)
