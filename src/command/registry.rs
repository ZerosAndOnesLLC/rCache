use bytes::Bytes;
use std::collections::HashMap;
use std::time::Instant;

use crate::protocol::RespValue;
use crate::storage::Store;

/// Context passed to command handlers.
pub struct CommandContext<'a> {
    pub store: &'a mut Store,
    pub db_index: usize,
    pub args: Vec<Bytes>,
    /// Server start time for uptime calculations.
    pub start_time: Instant,
}

impl<'a> CommandContext<'a> {
    pub fn db(&mut self) -> &mut crate::storage::Database {
        self.store.db_mut(self.db_index)
    }
}

type CommandHandler = fn(&mut CommandContext) -> RespValue;

struct CommandEntry {
    handler: CommandHandler,
    /// Arity: positive = exact count, negative = minimum count (including command name).
    arity: i32,
}

pub struct CommandRegistry {
    commands: HashMap<String, CommandEntry>,
}

impl CommandRegistry {
    pub fn new() -> Self {
        let mut registry = Self {
            commands: HashMap::new(),
        };
        registry.register_all();
        registry
    }

    fn register(&mut self, name: &str, handler: CommandHandler, arity: i32) {
        self.commands.insert(name.to_uppercase(), CommandEntry { handler, arity });
    }

    pub fn execute(&self, ctx: &mut CommandContext) -> RespValue {
        if ctx.args.is_empty() {
            return RespValue::error("ERR empty command");
        }

        let cmd_name = String::from_utf8_lossy(&ctx.args[0]).to_uppercase();

        let entry = match self.commands.get(&cmd_name) {
            Some(e) => e,
            None => return RespValue::error(format!("ERR unknown command '{}'", cmd_name)),
        };

        // Check arity
        let argc = ctx.args.len() as i32;
        if entry.arity > 0 && argc != entry.arity {
            return RespValue::wrong_arity(&cmd_name.to_lowercase());
        }
        if entry.arity < 0 && argc < -entry.arity {
            return RespValue::wrong_arity(&cmd_name.to_lowercase());
        }

        (entry.handler)(ctx)
    }

    pub fn command_count(&self) -> usize {
        self.commands.len()
    }

    pub fn command_names(&self) -> Vec<String> {
        self.commands.keys().cloned().collect()
    }

    fn register_all(&mut self) {
        // === String commands ===
        self.register("SET", super::strings::cmd_set, -3);
        self.register("GET", super::strings::cmd_get, 2);
        self.register("DEL", super::keys::cmd_del, -2);
        self.register("UNLINK", super::keys::cmd_del, -2); // same as DEL for now
        self.register("EXISTS", super::keys::cmd_exists, -2);
        self.register("SETNX", super::strings::cmd_setnx, 3);
        self.register("SETEX", super::strings::cmd_setex, 4);
        self.register("PSETEX", super::strings::cmd_psetex, 4);
        self.register("MGET", super::strings::cmd_mget, -2);
        self.register("MSET", super::strings::cmd_mset, -3);
        self.register("MSETNX", super::strings::cmd_msetnx, -3);
        self.register("INCR", super::strings::cmd_incr, 2);
        self.register("DECR", super::strings::cmd_decr, 2);
        self.register("INCRBY", super::strings::cmd_incrby, 3);
        self.register("DECRBY", super::strings::cmd_decrby, 3);
        self.register("INCRBYFLOAT", super::strings::cmd_incrbyfloat, 3);
        self.register("APPEND", super::strings::cmd_append, 3);
        self.register("STRLEN", super::strings::cmd_strlen, 2);
        self.register("GETRANGE", super::strings::cmd_getrange, 4);
        self.register("SUBSTR", super::strings::cmd_getrange, 4); // alias
        self.register("SETRANGE", super::strings::cmd_setrange, 4);
        self.register("GETDEL", super::strings::cmd_getdel, 2);
        self.register("GETEX", super::strings::cmd_getex, -2);
        self.register("GETSET", super::strings::cmd_getset, 3);

        // === Key management ===
        self.register("EXPIRE", super::keys::cmd_expire, -3);
        self.register("PEXPIRE", super::keys::cmd_pexpire, -3);
        self.register("EXPIREAT", super::keys::cmd_expireat, -3);
        self.register("PEXPIREAT", super::keys::cmd_pexpireat, -3);
        self.register("TTL", super::keys::cmd_ttl, 2);
        self.register("PTTL", super::keys::cmd_pttl, 2);
        self.register("PERSIST", super::keys::cmd_persist, 2);
        self.register("EXPIRETIME", super::keys::cmd_expiretime, 2);
        self.register("PEXPIRETIME", super::keys::cmd_pexpiretime, 2);
        self.register("TYPE", super::keys::cmd_type, 2);
        self.register("RENAME", super::keys::cmd_rename, 3);
        self.register("RENAMENX", super::keys::cmd_renamenx, 3);
        self.register("RANDOMKEY", super::keys::cmd_randomkey, 1);
        self.register("KEYS", super::keys::cmd_keys, 2);
        self.register("OBJECT", super::keys::cmd_object, -2);
        self.register("COPY", super::keys::cmd_copy, -3);
        self.register("TOUCH", super::keys::cmd_touch, -2);

        // === Scan ===
        self.register("SCAN", super::scan::cmd_scan, -2);
        self.register("SSCAN", super::scan::cmd_sscan, -3);
        self.register("HSCAN", super::scan::cmd_hscan, -3);
        self.register("ZSCAN", super::scan::cmd_zscan, -3);

        // === Server commands ===
        self.register("PING", super::server_cmds::cmd_ping, -1);
        self.register("ECHO", super::server_cmds::cmd_echo, 2);
        self.register("SELECT", super::server_cmds::cmd_select, 2);
        self.register("DBSIZE", super::server_cmds::cmd_dbsize, 1);
        self.register("FLUSHDB", super::server_cmds::cmd_flushdb, -1);
        self.register("FLUSHALL", super::server_cmds::cmd_flushall, -1);
        self.register("SWAPDB", super::server_cmds::cmd_swapdb, 3);
        self.register("TIME", super::server_cmds::cmd_time, 1);
        self.register("INFO", super::server_cmds::cmd_info, -1);
        self.register("COMMAND", super::server_cmds::cmd_command, -1);

        // === List commands ===
        self.register("LPUSH", super::list::cmd_lpush, -3);
        self.register("RPUSH", super::list::cmd_rpush, -3);
        self.register("LPUSHX", super::list::cmd_lpushx, -3);
        self.register("RPUSHX", super::list::cmd_rpushx, -3);
        self.register("LPOP", super::list::cmd_lpop, -2);
        self.register("RPOP", super::list::cmd_rpop, -2);
        self.register("LLEN", super::list::cmd_llen, 2);
        self.register("LINDEX", super::list::cmd_lindex, 3);
        self.register("LRANGE", super::list::cmd_lrange, 4);
        self.register("LSET", super::list::cmd_lset, 4);
        self.register("LINSERT", super::list::cmd_linsert, 5);
        self.register("LREM", super::list::cmd_lrem, 4);
        self.register("LTRIM", super::list::cmd_ltrim, 4);
        self.register("LPOS", super::list::cmd_lpos, -3);
        self.register("LMOVE", super::list::cmd_lmove, 5);
        self.register("LMPOP", super::list::cmd_lmpop, -4);

        // === Set commands ===
        self.register("SADD", super::set::cmd_sadd, -3);
        self.register("SREM", super::set::cmd_srem, -3);
        self.register("SISMEMBER", super::set::cmd_sismember, 3);
        self.register("SMISMEMBER", super::set::cmd_smismember, -3);
        self.register("SMEMBERS", super::set::cmd_smembers, 2);
        self.register("SCARD", super::set::cmd_scard, 2);
        self.register("SRANDMEMBER", super::set::cmd_srandmember, -2);
        self.register("SPOP", super::set::cmd_spop, -2);
        self.register("SDIFF", super::set::cmd_sdiff, -2);
        self.register("SDIFFSTORE", super::set::cmd_sdiffstore, -3);
        self.register("SINTER", super::set::cmd_sinter, -2);
        self.register("SINTERSTORE", super::set::cmd_sinterstore, -3);
        self.register("SINTERCARD", super::set::cmd_sintercard, -3);
        self.register("SUNION", super::set::cmd_sunion, -2);
        self.register("SUNIONSTORE", super::set::cmd_sunionstore, -3);
        self.register("SMOVE", super::set::cmd_smove, 4);

        // === Hash commands ===
        self.register("HSET", super::hash::cmd_hset, -4);
        self.register("HSETNX", super::hash::cmd_hsetnx, 4);
        self.register("HGET", super::hash::cmd_hget, 3);
        self.register("HMGET", super::hash::cmd_hmget, -3);
        self.register("HDEL", super::hash::cmd_hdel, -3);
        self.register("HEXISTS", super::hash::cmd_hexists, 3);
        self.register("HLEN", super::hash::cmd_hlen, 2);
        self.register("HKEYS", super::hash::cmd_hkeys, 2);
        self.register("HVALS", super::hash::cmd_hvals, 2);
        self.register("HGETALL", super::hash::cmd_hgetall, 2);
        self.register("HINCRBY", super::hash::cmd_hincrby, 4);
        self.register("HINCRBYFLOAT", super::hash::cmd_hincrbyfloat, 4);
        self.register("HRANDFIELD", super::hash::cmd_hrandfield, -2);

        // === Sorted set commands ===
        self.register("ZADD", super::sorted_set::cmd_zadd, -4);
        self.register("ZREM", super::sorted_set::cmd_zrem, -3);
        self.register("ZSCORE", super::sorted_set::cmd_zscore, 3);
        self.register("ZMSCORE", super::sorted_set::cmd_zmscore, -3);
        self.register("ZINCRBY", super::sorted_set::cmd_zincrby, 4);
        self.register("ZCARD", super::sorted_set::cmd_zcard, 2);
        self.register("ZCOUNT", super::sorted_set::cmd_zcount, 4);
        self.register("ZRANGE", super::sorted_set::cmd_zrange, -4);
        self.register("ZRANGEBYSCORE", super::sorted_set::cmd_zrangebyscore, -4);
        self.register("ZREVRANGE", super::sorted_set::cmd_zrevrange, -4);
        self.register("ZREVRANGEBYSCORE", super::sorted_set::cmd_zrevrangebyscore, -4);
        self.register("ZRANK", super::sorted_set::cmd_zrank, 3);
        self.register("ZREVRANK", super::sorted_set::cmd_zrevrank, 3);
        self.register("ZPOPMIN", super::sorted_set::cmd_zpopmin, -2);
        self.register("ZPOPMAX", super::sorted_set::cmd_zpopmax, -2);
        self.register("ZRANDMEMBER", super::sorted_set::cmd_zrandmember, -2);
        self.register("ZUNIONSTORE", super::sorted_set::cmd_zunionstore, -4);
        self.register("ZINTERSTORE", super::sorted_set::cmd_zinterstore, -4);
        self.register("ZDIFFSTORE", super::sorted_set::cmd_zdiffstore, -4);
        self.register("ZLEXCOUNT", super::sorted_set::cmd_zlexcount, 4);

        self.register("ZRANGESTORE", super::sorted_set::cmd_zrangestore, -5);
        self.register("ZUNION", super::sorted_set::cmd_zunion, -3);
        self.register("ZINTER", super::sorted_set::cmd_zinter, -3);
        self.register("ZDIFF", super::sorted_set::cmd_zdiff, -3);
        self.register("ZINTERCARD", super::sorted_set::cmd_zintercard, -3);
        self.register("ZMPOP", super::sorted_set::cmd_zmpop, -4);

        // === Blocking sorted set stubs ===
        self.register("BZPOPMIN", super::sorted_set::cmd_bzpopmin, -3);
        self.register("BZPOPMAX", super::sorted_set::cmd_bzpopmax, -3);
        self.register("BZMPOP", super::sorted_set::cmd_bzmpop, -5);

        // === Blocking list stubs ===
        self.register("BLPOP", super::list::cmd_blpop, -3);
        self.register("BRPOP", super::list::cmd_brpop, -3);
        self.register("BLMOVE", super::list::cmd_blmove, 6);
        self.register("BLMPOP", super::list::cmd_blmpop, -5);

        // === Pub/Sub ===
        self.register("PUBLISH", super::pubsub::cmd_publish, 3);
        self.register("SUBSCRIBE", super::pubsub::cmd_subscribe, -2);
        self.register("UNSUBSCRIBE", super::pubsub::cmd_unsubscribe, -1);
        self.register("PSUBSCRIBE", super::pubsub::cmd_psubscribe, -2);
        self.register("PUNSUBSCRIBE", super::pubsub::cmd_punsubscribe, -1);
        self.register("PUBSUB", super::pubsub::cmd_pubsub, -2);

        // === Transactions ===
        self.register("MULTI", super::transaction::cmd_multi, 1);
        self.register("EXEC", super::transaction::cmd_exec, 1);
        self.register("DISCARD", super::transaction::cmd_discard, 1);
        self.register("WATCH", super::transaction::cmd_watch, -2);
        self.register("UNWATCH", super::transaction::cmd_unwatch, 1);

        // === Bitmap commands ===
        self.register("SETBIT", super::bitmap::cmd_setbit, 4);
        self.register("GETBIT", super::bitmap::cmd_getbit, 3);
        self.register("BITCOUNT", super::bitmap::cmd_bitcount, -2);
        self.register("BITPOS", super::bitmap::cmd_bitpos, -3);
        self.register("BITOP", super::bitmap::cmd_bitop, -4);
        self.register("BITFIELD", super::bitmap::cmd_bitfield, -2);
        self.register("BITFIELD_RO", super::bitmap::cmd_bitfield_ro, -2);

        // === HyperLogLog ===
        self.register("PFADD", super::hyperloglog::cmd_pfadd, -2);
        self.register("PFCOUNT", super::hyperloglog::cmd_pfcount, -2);
        self.register("PFMERGE", super::hyperloglog::cmd_pfmerge, -2);

        // === Geospatial ===
        self.register("GEOADD", super::geo::cmd_geoadd, -5);
        self.register("GEOPOS", super::geo::cmd_geopos, -2);
        self.register("GEODIST", super::geo::cmd_geodist, -4);
        self.register("GEOSEARCH", super::geo::cmd_geosearch, -7);
        self.register("GEOSEARCHSTORE", super::geo::cmd_geosearchstore, -7);
        self.register("GEOHASH", super::geo::cmd_geohash, -2);

        // === Server commands expansion ===
        self.register("CONFIG", super::server_cmds::cmd_config, -2);
        self.register("CLIENT", super::server_cmds::cmd_client, -2);
        self.register("SLOWLOG", super::server_cmds::cmd_slowlog, -2);
        self.register("MEMORY", super::server_cmds::cmd_memory, -2);
        self.register("LOLWUT", super::server_cmds::cmd_lolwut, -1);
        self.register("HELLO", super::server_cmds::cmd_hello, -1);
        self.register("RESET", super::server_cmds::cmd_reset, 1);
        self.register("DEBUG", super::server_cmds::cmd_debug, -1);
        self.register("WAIT", super::server_cmds::cmd_wait, 3);

        // === Sort ===
        self.register("SORT", super::keys::cmd_sort, -2);
        self.register("SORT_RO", super::keys::cmd_sort, -2);

        // === Persistence commands ===
        self.register("SAVE", super::persistence_cmds::cmd_save, 1);
        self.register("BGSAVE", super::persistence_cmds::cmd_bgsave, 1);
        self.register("LASTSAVE", super::persistence_cmds::cmd_lastsave, 1);
        self.register("BGREWRITEAOF", super::persistence_cmds::cmd_bgrewriteaof, 1);
    }
}
