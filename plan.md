# rCache — Rust Implementation of Valkey/Redis

## Overview

A high-performance, Redis/Valkey-compatible in-memory data store written in Rust. Uses async I/O via tokio with a single-threaded command execution model (matching Redis's design) for simplicity and correctness, with optional I/O threading for throughput.

---

## Phase 1: Foundation

### 1.1 Project Setup
- [x] Initialize Cargo project (lib + binary)
- [x] Set up dependencies: `tokio`, `bytes`, `thiserror`, `tracing`, `tracing-subscriber`
- [x] Create module skeleton: `server`, `protocol`, `storage`, `command`
- [x] Basic config struct (bind address, port, number of databases)
- [x] Entry point: parse CLI args / config file, start server

### 1.2 RESP2 Protocol Parser & Serializer
- [x] RESP2 types: Simple String, Error, Integer, Bulk String, Array, Null
- [x] Streaming parser over `bytes::BytesMut` (handle partial reads)
- [x] Inline command parsing (space-delimited plain text)
- [x] Serializer: `RespValue -> bytes`
- [x] Unit tests for all types including edge cases (empty bulk string, null array, nested arrays)

### 1.3 TCP Server & Connection Handling
- [x] Tokio TCP listener with accept loop
- [x] Per-connection async task: read -> parse -> dispatch -> respond
- [x] Connection state struct (database index, authenticated flag, client name, protocol version)
- [x] Graceful shutdown via tokio signal handling
- [x] Backpressure: limit max connections (`maxclients`)

### 1.4 Core Keyspace & Storage Engine
- [x] `Database` struct: `HashMap<Bytes, RedisObject>` for keys + `HashMap<Bytes, Instant>` for expiry
- [x] `RedisObject` enum for value types (String, List, Set, Hash, SortedSet, Stream)
- [x] Multi-database support: `Vec<Database>` (default 16)
- [x] Thread-safe shared state (single `Arc<Mutex<Store>>` or channel-based command dispatch)
- [x] Key lookup with lazy expiration check

### 1.5 Command Dispatch Framework
- [x] Command registry: name -> handler function mapping
- [x] Parse incoming RESP array into command name + args
- [x] Case-insensitive command lookup
- [x] Arity validation (min/max argument count per command)
- [x] Error responses for unknown commands and wrong arg count

### 1.6 Basic String Commands
- [x] `SET key value [EX seconds] [PX ms] [EXAT ts] [PXAT ms-ts] [NX] [XX] [KEEPTTL] [GET]`
- [x] `GET key`
- [x] `DEL key [key ...]` (returns count of deleted)
- [x] `UNLINK key [key ...]` (async free — can be same as DEL initially)
- [x] `EXISTS key [key ...]` (returns count)
- [x] `SETNX key value`, `SETEX key seconds value`, `PSETEX key ms value`
- [x] `MGET key [key ...]`, `MSET key value [key value ...]`, `MSETNX`
- [x] `INCR`, `DECR`, `INCRBY`, `DECRBY`, `INCRBYFLOAT`
- [x] `APPEND key value`, `STRLEN key`
- [x] `GETRANGE key start end`, `SETRANGE key offset value`
- [x] `GETDEL key`, `GETEX key [EX|PX|EXAT|PXAT|PERSIST]`

### 1.7 Key Expiration
- [x] `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT` (with NX, XX, GT, LT flags)
- [x] `TTL`, `PTTL`, `EXPIRETIME`, `PEXPIRETIME`
- [x] `PERSIST` (remove expiration)
- [x] Lazy expiration: check on every key access
- [x] Active expiration: periodic task (10 Hz default) — sample 20 keys, delete expired, repeat if >25% expired

### 1.8 Server Essentials
- [x] `PING [message]`, `ECHO message`
- [x] `SELECT db`
- [x] `DBSIZE`
- [x] `FLUSHDB [ASYNC|SYNC]`, `FLUSHALL [ASYNC|SYNC]`
- [x] `SWAPDB db1 db2`
- [x] `TIME` (server time as [seconds, microseconds])
- [x] `QUIT` / connection close handling
- [x] `RESET` (reset connection state)
- [x] `COMMAND COUNT`, `COMMAND LIST`, `COMMAND INFO`
- [x] `AUTH password` (simple `requirepass` config)
- [x] Basic `INFO` response (server section: version, uptime, tcp_port)

---

## Phase 2: Data Structures

### 2.1 Lists
- [x] Internal representation: `VecDeque<Bytes>` (simple first, optimize later)
- [x] `LPUSH`, `RPUSH`, `LPUSHX`, `RPUSHX`
- [x] `LPOP`, `RPOP` (with optional count)
- [x] `LLEN`, `LINDEX`, `LRANGE`
- [x] `LSET`, `LINSERT BEFORE|AFTER`
- [x] `LREM count value`
- [x] `LTRIM key start stop`
- [x] `LPOS key element [RANK rank] [COUNT count] [MAXLEN maxlen]`
- [x] `LMOVE source destination LEFT|RIGHT LEFT|RIGHT`
- [x] `LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT count]`

### 2.2 Blocking List Operations
- [ ] Blocking waitlist: per-key queue of waiting clients
- [x] `BLPOP key [key ...] timeout`
- [x] `BRPOP key [key ...] timeout`
- [x] `BLMOVE source destination LEFT|RIGHT LEFT|RIGHT timeout`
- [x] `BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT count]`
- [ ] Wake blocked clients on LPUSH/RPUSH to watched keys

### 2.3 Sets
- [x] Internal representation: `HashSet<Bytes>`
- [x] `SADD`, `SREM`, `SISMEMBER`, `SMISMEMBER`
- [x] `SMEMBERS`, `SCARD`, `SRANDMEMBER [count]`, `SPOP [count]`
- [x] `SDIFF`, `SDIFFSTORE`, `SINTER`, `SINTERSTORE`, `SINTERCARD numkeys key [key ...] [LIMIT limit]`
- [x] `SUNION`, `SUNIONSTORE`
- [x] `SMOVE source destination member`

### 2.4 Hashes
- [x] Internal representation: `HashMap<Bytes, Bytes>`
- [x] `HSET key field value [field value ...]`, `HSETNX`
- [x] `HGET`, `HMGET`
- [x] `HDEL field [field ...]`, `HEXISTS`, `HLEN`
- [x] `HKEYS`, `HVALS`, `HGETALL`
- [x] `HINCRBY`, `HINCRBYFLOAT`
- [x] `HRANDFIELD [count [WITHVALUES]]`

### 2.5 Sorted Sets
- [x] Skiplist implementation: random levels, forward pointers, span tracking, backward pointer
- [x] Dual index: skiplist (by score) + `HashMap<Bytes, f64>` (by member)
- [x] `ZADD key [NX|XX] [GT|LT] [CH] score member [score member ...]`
- [x] `ZREM`, `ZSCORE`, `ZMSCORE`, `ZINCRBY`
- [x] `ZCARD`, `ZCOUNT min max`, `ZLEXCOUNT min max`
- [x] `ZRANGE key min max [BYSCORE|BYLEX] [REV] [LIMIT offset count] [WITHSCORES]`
- [x] `ZRANGESTORE dst src min max [BYSCORE|BYLEX] [REV] [LIMIT offset count]`
- [x] `ZRANK`, `ZREVRANK`
- [x] `ZPOPMIN [count]`, `ZPOPMAX [count]`
- [x] `BZPOPMIN`, `BZPOPMAX`, `ZMPOP`, `BZMPOP` (blocking variants)
- [x] `ZRANDMEMBER [count [WITHSCORES]]`
- [x] `ZUNIONSTORE`, `ZINTERSTORE`, `ZDIFFSTORE`
- [x] `ZUNION`, `ZINTER`, `ZDIFF`, `ZINTERCARD`

### 2.6 Key Scanning & Management
- [x] `SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]` (cursor-based iteration)
- [x] `SSCAN`, `HSCAN`, `ZSCAN` (per-type scanning)
- [x] `KEYS pattern` (glob matching)
- [x] `RANDOMKEY`
- [x] `RENAME key newkey`, `RENAMENX key newkey`
- [x] `TYPE key`
- [x] `OBJECT ENCODING key`, `OBJECT REFCOUNT key`, `OBJECT IDLETIME key`, `OBJECT FREQ key`, `OBJECT HELP`
- [x] `COPY source destination [DB destination-db] [REPLACE]`
- [ ] `DUMP key`, `RESTORE key ttl serialized-value [REPLACE] [ABSTTL]`
- [x] `TOUCH key [key ...]`
- [x] `SORT key [BY pattern] [LIMIT offset count] [GET pattern ...] [ASC|DESC] [ALPHA] [STORE dest]`
- [x] `SORT_RO`

---

## Phase 3: Pub/Sub & Transactions

### 3.1 Pub/Sub — Channel-Based
- [x] Subscription registry: `channel -> HashSet<ClientId>`
- [x] `SUBSCRIBE channel [channel ...]`
- [x] `UNSUBSCRIBE [channel ...]`
- [x] `PUBLISH channel message` (returns receiver count)
- [x] Pub/sub connection mode restrictions (only sub/unsub/ping/quit/reset allowed)
- [x] Client disconnect cleanup (remove from all subscriptions)
- [x] `PUBSUB CHANNELS [pattern]`, `PUBSUB NUMSUB [channel ...]`, `PUBSUB NUMPAT`

### 3.2 Pub/Sub — Pattern-Based
- [x] Pattern subscription registry: `pattern -> HashSet<ClientId>`
- [x] `PSUBSCRIBE pattern [pattern ...]`
- [x] `PUNSUBSCRIBE [pattern ...]`
- [x] Glob pattern matching engine (`*`, `?`, `[abc]`, `[a-z]`)
- [x] On PUBLISH: check both exact channel matches and pattern matches

### 3.3 Transactions
- [x] Per-client transaction state: queue of commands, watched keys, dirty flag
- [x] `MULTI` — enter transaction mode, queue subsequent commands (reply `+QUEUED`)
- [x] `EXEC` — execute all queued commands atomically, return array of results
- [x] `DISCARD` — cancel transaction, clear queue
- [x] `WATCH key [key ...]` — optimistic locking
- [x] `UNWATCH` — cancel all watches
- [x] WATCH dictionary: global `key -> Vec<ClientId>`, flag clients dirty on key mutation
- [x] Guarantee no command interleaving during EXEC

---

## Phase 4: Extended Commands & RESP3

### 4.1 Bitmap Commands
- [x] `SETBIT key offset value`, `GETBIT key offset`
- [x] `BITCOUNT key [start end [BYTE|BIT]]`
- [x] `BITPOS key bit [start [end [BYTE|BIT]]]`
- [x] `BITOP AND|OR|XOR|NOT destkey key [key ...]`
- [x] `BITFIELD key [GET encoding offset] [SET encoding offset value] [INCRBY encoding offset increment] [OVERFLOW WRAP|SAT|FAIL]`
- [x] `BITFIELD_RO`

### 4.2 HyperLogLog
- [x] Sparse representation (run-length encoded) and dense representation (16384 6-bit registers)
- [x] `PFADD key [element ...]`
- [x] `PFCOUNT key [key ...]`
- [x] `PFMERGE destkey sourcekey [sourcekey ...]`
- [x] Automatic sparse -> dense promotion

### 4.3 Geospatial Commands
- [x] Geohash encoding/decoding (52-bit interleaved lat/lng, stored as sorted set scores)
- [x] `GEOADD key [NX|XX] [CH] longitude latitude member [...]`
- [x] `GEOPOS key [member ...]`
- [x] `GEODIST key member1 member2 [M|KM|FT|MI]`
- [x] `GEOSEARCH key FROMMEMBER member|FROMLONLAT lng lat BYRADIUS radius M|KM|FT|MI|BYBOX width height M|KM|FT|MI [ASC|DESC] [COUNT count [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH]`
- [x] `GEOSEARCHSTORE destination source [options]`
- [x] `GEOHASH key [member ...]`

### 4.4 RESP3 Protocol
- [ ] New types: Null, Double, Boolean, Blob Error, Verbatim String, Big Number, Map, Set, Push
- [x] `HELLO [protover [AUTH username password] [SETNAME clientname]]`
- [ ] Per-connection protocol version tracking (default RESP2, upgrade to RESP3)
- [ ] Push messages (`>` type) for pub/sub in RESP3 mode

### 4.5 Server Commands Expansion
- [x] Full `INFO` sections: server, clients, memory, persistence, stats, replication, cpu, keyspace, commandstats
- [x] `CONFIG GET pattern`, `CONFIG SET param value [...]`, `CONFIG REWRITE`, `CONFIG RESETSTAT`
- [x] `CLIENT LIST [TYPE type]`, `CLIENT GETNAME`, `CLIENT SETNAME name`
- [x] `CLIENT ID`, `CLIENT INFO`, `CLIENT KILL [filters]`
- [ ] `CLIENT PAUSE timeout [WRITE|ALL]`, `CLIENT UNPAUSE`
- [x] `SLOWLOG GET [count]`, `SLOWLOG LEN`, `SLOWLOG RESET`
- [x] `LATENCY LATEST`, `LATENCY HISTORY event`, `LATENCY RESET`
- [x] `MEMORY USAGE key [SAMPLES count]`
- [x] `DEBUG OBJECT key`, `DEBUG SLEEP seconds`
- [x] `LOLWUT [VERSION version]`

---

## Phase 5: Persistence

### 5.1 RDB Snapshots — Save
- [x] RDB binary format: magic header, aux fields, per-db entries, EOF + CRC64
- [x] Serialize all data types: strings (int/embstr/raw encoding), lists, sets, sorted sets, hashes
- [x] Include expiry timestamps in serialized entries
- [x] `SAVE` (blocking, foreground)
- [x] `BGSAVE` — snapshot current state in background task (clone/fork approach)
- [ ] `save` config directive: periodic auto-save based on changes + time thresholds
- [ ] Save on graceful shutdown

### 5.2 RDB Snapshots — Load
- [x] Parse RDB file on startup
- [x] Reconstruct all data types from binary format
- [x] Skip expired keys during load
- [ ] CRC64 checksum verification
- [x] `LASTSAVE` (timestamp of last successful save)

### 5.3 AOF — Append Only File
- [x] Write every write command to AOF buffer in RESP format
- [x] `appendfsync` policy: `always`, `everysec` (default), `no`
- [x] Background fsync task for `everysec` mode
- [x] AOF replay on startup (after RDB load if both exist)
- [x] `BGREWRITEAOF` — compact AOF by writing minimal commands from current state
- [ ] Multi-Part AOF (MP-AOF): base file (RDB format) + incremental files + manifest

### 5.4 Hybrid Persistence
- [ ] `aof-use-rdb-preamble yes` — AOF base is RDB format (faster loads)
- [x] Startup loading priority: AOF preferred over RDB if AOF enabled
- [ ] `WAITAOF numlocal numreplicas timeout`

---

## Phase 6: Memory Management & Eviction

### 6.1 Memory Tracking
- [x] Track `used_memory` (via jemalloc stats or manual accounting)
- [x] `maxmemory` configuration
- [x] Per-key LRU clock (24-bit, seconds resolution) stored in key metadata
- [x] Update LRU clock on every key access

### 6.2 Eviction Policies
- [x] `noeviction` — return OOM errors on writes
- [x] `allkeys-lru` — approximate LRU across all keys (sample N, evict oldest)
- [x] `volatile-lru` — approximate LRU among keys with TTL
- [x] `allkeys-random`, `volatile-random`
- [x] `volatile-ttl` — evict shortest TTL
- [ ] Eviction pool (16 best candidates across sampling rounds)
- [x] Run eviction check before every write command when at memory limit

### 6.3 LFU Eviction
- [ ] LFU counter: 8-bit logarithmic counter + 16-bit decay timestamp (reuses LRU field)
- [ ] Probabilistic increment: `1 / (counter * lfu-log-factor + 1)`
- [ ] Time-based decay: decrement based on `lfu-decay-time`
- [ ] `allkeys-lfu`, `volatile-lfu` policies
- [ ] `OBJECT FREQ key`

---

## Phase 7: ACL & Lua Scripting

### 7.1 ACL System
- [x] User registry with default user
- [ ] Per-user: enabled/disabled, passwords, allowed commands, allowed keys, allowed channels
- [ ] Command categories (`@read`, `@write`, `@admin`, `@dangerous`, etc.) with bitmask checks
- [ ] Key pattern matching (`~pattern`, `%R~pattern`, `%W~pattern`)
- [x] `ACL SETUSER`, `ACL GETUSER`, `ACL DELUSER`, `ACL LIST`, `ACL USERS`
- [x] `ACL WHOAMI`, `ACL CAT [category]`
- [x] `ACL LOG [count|RESET]`
- [ ] `ACL SAVE`, `ACL LOAD` (persist to file)
- [x] `AUTH username password` (RESP2 + RESP3)
- [ ] ACL check on every command dispatch (fast path with bitmasks)

### 7.2 Lua Scripting
- [ ] Embed Lua runtime (`mlua` crate)
- [x] `EVAL script numkeys key [key ...] arg [arg ...]`
- [x] `EVALSHA sha1 numkeys key [key ...] arg [arg ...]`
- [ ] `redis.call()` and `redis.pcall()` callbacks into command engine
- [ ] SHA1-based script cache
- [x] `SCRIPT LOAD`, `SCRIPT EXISTS`, `SCRIPT FLUSH`
- [ ] Atomic execution (block other commands during script)
- [ ] `lua-time-limit` config, `SCRIPT KILL` for read-only scripts
- [ ] `redis.log()`, `redis.error_reply()`, `redis.status_reply()` helpers

### 7.3 Redis Functions
- [x] `FUNCTION LOAD [REPLACE] library-code`
- [x] `FCALL function numkeys key [key ...] arg [arg ...]`
- [x] `FCALL_RO` (read-only variant)
- [x] `FUNCTION LIST`, `FUNCTION DELETE`, `FUNCTION DUMP`, `FUNCTION RESTORE`
- [x] `FUNCTION STATS`, `FUNCTION FLUSH`
- [ ] Persist functions in RDB/AOF

---

## Phase 8: Replication

### 8.1 Full Synchronization
- [x] Replication handshake: `REPLCONF`, `PSYNC ? -1`
- [ ] Master generates RDB and streams to replica
- [ ] Replication backlog: circular buffer of serialized write commands
- [ ] Replication ID + offset tracking
- [ ] Buffer new writes during RDB generation for replay after transfer
- [x] `REPLICAOF host port` / `REPLICAOF NO ONE`

### 8.2 Partial Resynchronization
- [ ] `PSYNC repl-id offset` — resume from backlog if offset is within range
- [ ] Fall back to full sync when offset is too old
- [ ] Second replication ID (`repl-id-2`) for failover continuity

### 8.3 Command Propagation
- [ ] Propagate every write command to all connected replicas
- [ ] Propagate to replication backlog simultaneously
- [x] `WAIT numreplicas timeout` — synchronous replication acknowledgment
- [ ] `replica-read-only` enforcement
- [ ] `replica-serve-stale-data` configuration
- [ ] `min-replicas-to-write`, `min-replicas-max-lag` write quorum

### 8.4 Replication of Expiry
- [ ] Master sends DEL to replicas on key expiration (replicas do not independently expire)
- [ ] Skip expired keys in RDB sent to replicas

---

## Phase 9: Streams

### 9.1 Stream Data Structure
- [x] Stream entry storage (radix tree of entries, or simpler B-tree/Vec as first pass)
- [x] Auto-generated IDs: `<ms-timestamp>-<sequence>`
- [x] `XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold [LIMIT count]] *|id field value [...]`
- [x] `XLEN key`
- [x] `XRANGE key start end [COUNT count]`
- [x] `XREVRANGE key end start [COUNT count]`
- [x] `XDEL key id [id ...]` (tombstone)
- [x] `XTRIM key MAXLEN|MINID [=|~] threshold [LIMIT count]`
- [x] `XINFO STREAM key [FULL [COUNT count]]`

### 9.2 Stream Reading
- [x] `XREAD [COUNT count] [BLOCK ms] STREAMS key [key ...] id [id ...]`
- [x] Special ID `$` (new entries only)
- [ ] Blocking XREAD with wake-on-XADD

### 9.3 Consumer Groups
- [x] Per-group state: last delivered ID, PEL (pending entries list)
- [x] Per-consumer state: name, PEL subset, seen-time, active-time
- [x] `XGROUP CREATE key groupname id|$ [MKSTREAM] [ENTRIESREAD n]`
- [x] `XGROUP DESTROY`, `XGROUP SETID`, `XGROUP CREATECONSUMER`, `XGROUP DELCONSUMER`
- [x] `XREADGROUP GROUP group consumer [COUNT count] [BLOCK ms] [NOACK] STREAMS key [key ...] id [id ...]`
- [x] Special ID `>` (undelivered entries) vs specific ID (re-read pending)
- [x] `XACK key group id [id ...]`
- [x] `XPENDING key group [[IDLE min-idle] start end count [consumer]]`
- [x] `XCLAIM key group consumer min-idle-time id [id ...] [options]`
- [x] `XAUTOCLAIM key group consumer min-idle-time start [COUNT count]`
- [x] `XINFO GROUPS key`, `XINFO CONSUMERS key group`

---

## Phase 10: Cluster

### 10.1 Hash Slot Routing
- [x] CRC16 implementation, `slot = CRC16(key) % 16384`
- [x] Hash tag support: `{tag}` extraction for multi-key co-location
- [ ] Slot ownership table: `[u16; 16384]` mapping slot -> node
- [ ] Multi-key command validation: all keys must resolve to same slot
- [x] `CLUSTER KEYSLOT key`, `CLUSTER COUNTKEYSINSLOT slot`, `CLUSTER GETKEYSINSLOT slot count`

### 10.2 Cluster Bus (Gossip Protocol)
- [ ] Separate TCP port (main port + 10000) for node-to-node communication
- [ ] Binary gossip protocol: ping/pong with node state, slot info, flags
- [ ] Node discovery: `CLUSTER MEET ip port`
- [ ] Failure detection: mark nodes as PFAIL (suspected) -> FAIL (confirmed by majority)
- [x] `CLUSTER NODES`, `CLUSTER INFO`, `CLUSTER SHARDS`, `CLUSTER SLOTS`

### 10.3 Client Redirection
- [ ] `-MOVED slot host:port` for permanent redirects
- [ ] `-ASK slot host:port` for in-progress migrations
- [x] `ASKING` command (allow next command on importing node)
- [ ] `-CROSSSLOTS` error for multi-slot commands
- [x] `READONLY` / `READWRITE` (allow reads on replicas in cluster mode)

### 10.4 Slot Migration
- [ ] `CLUSTER SETSLOT slot MIGRATING node-id` / `IMPORTING node-id` / `NODE node-id` / `STABLE`
- [ ] `MIGRATE host port key|"" dest-db timeout [COPY] [REPLACE] [AUTH pw] [KEYS key ...]`
- [ ] Atomic key transfer during migration
- [ ] `CLUSTER ADDSLOTS`, `CLUSTER DELSLOTS`, `CLUSTER ADDSLOTSRANGE`, `CLUSTER DELSLOTSRANGE`

### 10.5 Failover & Election
- [ ] Raft-like leader election: replica requests votes from masters
- [ ] Epoch-based configuration versioning
- [ ] `CLUSTER FAILOVER [FORCE|TAKEOVER]`
- [ ] `CLUSTER REPLICATE node-id`
- [x] `CLUSTER RESET [HARD|SOFT]`
- [ ] `CLUSTER FORGET node-id`
- [ ] Automatic failover when master is detected as FAIL

### 10.6 Sharded Pub/Sub
- [ ] `SSUBSCRIBE`, `SUNSUBSCRIBE`, `SPUBLISH`
- [ ] Route messages to node owning the channel's hash slot
- [ ] `PUBSUB SHARDCHANNELS`, `PUBSUB SHARDNUMSUB`

---

## Phase 11: Advanced & Ecosystem

### 11.1 Keyspace Notifications
- [ ] `notify-keyspace-events` config (K, E, g, $, l, s, h, z, x, e, t, m flags)
- [ ] Publish on `__keyevent@<db>__:<event>` and `__keyspace@<db>__:<key>` channels
- [ ] Integrate with Pub/Sub system

### 11.2 Client-Side Caching
- [x] `CLIENT TRACKING ON|OFF [REDIRECT id] [PREFIX prefix] [BCAST] [OPTIN] [OPTOUT] [NOLOOP]`
- [ ] Track keys read by each client
- [ ] Send invalidation messages on key mutation
- [ ] Broadcasting mode with prefix matching
- [x] `CLIENT CACHING YES|NO` (per-command opt-in/out)
- [x] `CLIENT NO-TOUCH ON|OFF`

### 11.3 I/O Threading
- [ ] Offload socket reads to I/O thread pool
- [ ] Offload response writes to I/O thread pool
- [ ] Keep command execution single-threaded
- [ ] `io-threads` and `io-threads-do-reads` configuration

### 11.4 Module / Plugin System
- [ ] Trait-based plugin API: `RCacheModule` trait for custom commands and data types
- [ ] Command registration from modules
- [ ] Custom data type registration with RDB save/load, AOF rewrite hooks
- [x] `MODULE LOAD`, `MODULE UNLOAD`, `MODULE LIST`
- [ ] Timer and event hook APIs for modules

### 11.5 Per-Field Hash Expiration
- [ ] Per-field TTL tracking within hash data structure
- [x] `HEXPIRE`, `HPEXPIRE`, `HEXPIREAT`, `HPEXPIREAT`
- [x] `HPERSIST`, `HTTL`, `HPTTL`, `HEXPIRETIME`, `HPEXPIRETIME`
- [ ] Active + lazy expiration for hash fields

### 11.6 LCS & Advanced String Operations
- [x] `LCS key1 key2 [LEN] [IDX] [MINMATCHLEN len] [WITHMATCHLEN]`
- [x] `SUBSTR key start end` (alias for GETRANGE)

---

## Architectural Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Async runtime | `tokio` | Industry standard, mature, excellent performance |
| Command execution | Single-threaded via channel dispatch | Matches Redis model, eliminates data races, simplest correctness |
| Data store | `HashMap` (std or hashbrown) | O(1) lookups, can swap later |
| Sorted set internals | Custom skiplist + HashMap | Required for O(log N) range queries + O(1) member lookup |
| Byte handling | `bytes::Bytes` | Zero-copy, reference-counted, standard |
| Persistence snapshots | Fork via `libc::fork()` or COW data structures (`im` crate) | Needed for non-blocking BGSAVE |
| Lua runtime | `mlua` crate | Well-maintained, supports Lua 5.4 |
| Config format | Redis-compatible config file syntax | Drop-in compatibility |

---

## Compatibility Target

Primary goal: **wire-compatible with Redis 7.2+ / Valkey 8+** clients. Any standard Redis client library (redis-py, ioredis, jedis, redis-rs) should work without modification.

---

## Testing Strategy

- **Unit tests**: Protocol parser, data structure operations, individual commands
- **Integration tests**: Full client-server round-trips using `redis-rs` client
- **Compatibility tests**: Run Redis's own test suite (`redis/tests`) against rCache
- **Benchmarks**: `redis-benchmark` tool for throughput/latency comparison
- **Fuzz testing**: RESP parser fuzzing with `cargo-fuzz`
