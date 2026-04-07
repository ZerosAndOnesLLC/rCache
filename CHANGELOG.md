# Changelog

## 0.10.3 (2026-04-07)

### Bug Fixes
- MULTI/EXEC now holds store lock for entire transaction (was releasing between commands)
- WATCH check and command execution are atomic under single lock
- RENAME properly tracks lru_map/lfu_map/used_memory for source and destination
- COPY properly tracks used_memory and creates lru/lfu entries
- MOVE now preserves TTL (was silently dropped)
- GETEX with past EXAT/PXAT now expires the key immediately (was no-op)
- SET with past EXAT/PXAT returns error (matches Redis 7.x)
- set_raw() (persistence loading) now creates lru/lfu entries
- set_expire_raw() validates key exists before inserting expiry
- Client tracking lock ordering made consistent (prevents deadlock)
- RESET acquires pubsub lock once for all unsubscriptions
- swap_with() now swaps used_memory between databases
- keys() cleans up expired keys during iteration

### Performance
- HSCAN/ZSCAN use HashMap for O(1) lookups (was O(n) linear search)
- JSON estimate_memory uses recursive walk instead of serializing

## 0.10.0 (2026-04-06)

### Features
- **RESP3 protocol** — 9 new types, per-connection negotiation via HELLO 2/3
- **Blocking operations** — Full async BLPOP/BRPOP/BZPOPMIN/BZPOPMAX with wake-on-push
- **Lua scripting** — EVAL/EVALSHA with sandboxed Lua 5.4 via mlua
- **Redis Functions** — FUNCTION LOAD/LIST/DELETE, FCALL/FCALL_RO
- **Full ACL system** — Per-user command/key/channel permissions, ACL SETUSER
- **Time series** — TS.CREATE/ADD/RANGE/REVRANGE with aggregation
- **Vector search** — VEC.CREATE/ADD/SEARCH with cosine/L2/IP metrics
- **Multi-tenancy** — NAMESPACE CREATE/SELECT/LIST/DELETE
- **Observability** — Per-command latency tracking, real slowlog entries
- **Data compression** — Transparent LZ4 for large values
- **CRC64 checksums** in RDB save/load
- **Client-side caching** — CLIENT TRACKING ON/OFF with invalidation

## 0.9.0 (2026-04-06)

### Features
- **HTTP REST API** — GET/PUT/DELETE /api/v1/{key}, POST /api/v1/command
- **Prometheus metrics** — GET /metrics endpoint
- **Built-in TLS** — rustls-based, --tls-port/--tls-cert-file/--tls-key-file
- **Native JSON type** — JSON.SET/GET/DEL with JSONPath support
- **Probabilistic structures** — Bloom filter, Count-Min Sketch, Top-K
- **Rate limiting** — RATELIMIT.CHECK (sliding window), RATELIMIT.ACQUIRE (token bucket)

## 0.8.0 (2026-04-06)

### Features
- **LFU eviction** — allkeys-lfu and volatile-lfu with logarithmic counter + decay
- **Eviction pool** — 16 best candidates for improved eviction accuracy
- **Keyspace notifications** — Publish on __keyevent__ and __keyspace__ channels

## 0.7.0 (2026-04-06)

### Features
- **250 registered commands** for full Valkey COMMAND INFO parity
- Deprecated command aliases: HMSET, RPOPLPUSH, BRPOPLPUSH, GEORADIUS
- ZREMRANGEBYLEX, ZREMRANGEBYRANK, ZREMRANGEBYSCORE
- HSTRLEN, XSETID, MOVE, ROLE, SHUTDOWN, DUMP/RESTORE stubs
- MONITOR, WAITAOF, COMMANDLOG stubs

## 0.5.0 (2026-04-06)

### Features
- **ACL stubs** — WHOAMI, LIST, USERS, CAT
- **Scripting stubs** — EVAL/EVALSHA/SCRIPT/FUNCTION
- **Replication stubs** — REPLICAOF, REPLCONF, WAIT
- **Streams** — XADD, XREAD, XREADGROUP, consumer groups, XACK, XCLAIM, XAUTOCLAIM
- **Cluster stubs** — CLUSTER INFO/KEYSLOT/NODES, READONLY/READWRITE
- **LCS** — Longest Common Substring with DP algorithm
- **Module stubs** — MODULE LIST/LOAD/UNLOAD
- **Per-field hash expiration stubs**

## 0.4.0 (2026-04-06)

### Features
- **Pub/Sub** — SUBSCRIBE/UNSUBSCRIBE/PSUBSCRIBE/PUNSUBSCRIBE/PUBLISH
- **Transactions** — MULTI/EXEC/DISCARD/WATCH/UNWATCH
- **Bitmaps** — SETBIT/GETBIT/BITCOUNT/BITPOS/BITOP/BITFIELD
- **HyperLogLog** — PFADD/PFCOUNT/PFMERGE (dense representation)
- **Geospatial** — GEOADD/GEOPOS/GEODIST/GEOSEARCH/GEOSEARCHSTORE/GEOHASH
- **RDB persistence** — Save/load with all data types
- **AOF persistence** — Write/replay/rewrite with three fsync modes
- **Memory eviction** — noeviction, allkeys-lru, volatile-lru, allkeys-random, volatile-random, volatile-ttl
- **Server commands** — CONFIG GET/SET, CLIENT LIST/ID, SLOWLOG, MEMORY USAGE, LOLWUT, HELLO

## 0.2.1 (2026-04-06)

### Bug Fixes
- Fixed range_by_score sentinel bound bug (data loss for long keys)
- Fixed exclusive score bounds ignored in ZRANGEBYSCORE/ZCOUNT
- Fixed ZREVRANGE returning lowest elements instead of highest
- Fixed TTL rounding (floor division, not ceiling)
- Fixed SORT with non-numeric values (now returns error)
- Added NaN/Infinity check in ZINCRBY
- Added 25ms time budget to active expiration

## 0.2.0 (2026-04-06)

### Features
- **RESP2 protocol** — Full parser and serializer with inline command support
- **TCP server** — Async I/O via tokio, per-connection tasks, maxclients backpressure
- **Strings** — SET (all flags), GET, MGET/MSET, INCR/DECR, APPEND, GETRANGE/SETRANGE
- **Lists** — LPUSH/RPUSH, LPOP/RPOP, LRANGE, LINSERT, LREM, LTRIM, LPOS, LMOVE
- **Sets** — SADD/SREM, SMEMBERS, SDIFF/SINTER/SUNION + STORE, SMOVE
- **Hashes** — HSET/HGET, HGETALL, HINCRBY, HRANDFIELD
- **Sorted Sets** — ZADD, ZRANGE, ZPOPMIN/MAX, ZUNIONSTORE/ZINTERSTORE
- **Key management** — EXPIRE/TTL (all variants), RENAME, TYPE, OBJECT, COPY, SORT
- **SCAN** — SCAN/SSCAN/HSCAN/ZSCAN with MATCH/COUNT/TYPE
- **Key expiration** — Lazy + active (10Hz sampling)
- **Server** — PING, SELECT, DBSIZE, FLUSHDB/FLUSHALL, SWAPDB, INFO, AUTH
