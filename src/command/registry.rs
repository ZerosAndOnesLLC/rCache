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

        // === Sort ===
        self.register("SORT", super::keys::cmd_sort, -2);
        self.register("SORT_RO", super::keys::cmd_sort, -2);

        // === ACL commands ===
        self.register("ACL", super::acl::cmd_acl, -2);

        // === Scripting stubs ===
        self.register("EVAL", super::scripting::cmd_eval, -3);
        self.register("EVALSHA", super::scripting::cmd_evalsha, -3);
        self.register("SCRIPT", super::scripting::cmd_script, -2);
        self.register("FUNCTION", super::scripting::cmd_function, -2);
        self.register("FCALL", super::scripting::cmd_fcall, -3);
        self.register("FCALL_RO", super::scripting::cmd_fcall_ro, -3);

        // === Replication stubs ===
        self.register("REPLICAOF", super::replication::cmd_replicaof, 3);
        self.register("SLAVEOF", super::replication::cmd_replicaof, 3); // alias
        self.register("REPLCONF", super::replication::cmd_replconf, -1);
        self.register("PSYNC", super::replication::cmd_psync, 3);
        self.register("WAIT", super::replication::cmd_wait, 3);

        // === Stream commands ===
        self.register("XADD", super::stream::cmd_xadd, -5);
        self.register("XLEN", super::stream::cmd_xlen, 2);
        self.register("XRANGE", super::stream::cmd_xrange, -4);
        self.register("XREVRANGE", super::stream::cmd_xrevrange, -4);
        self.register("XDEL", super::stream::cmd_xdel, -3);
        self.register("XTRIM", super::stream::cmd_xtrim, -4);
        self.register("XREAD", super::stream::cmd_xread, -4);
        self.register("XINFO", super::stream::cmd_xinfo, -3);
        self.register("XGROUP", super::stream::cmd_xgroup, -2);
        self.register("XREADGROUP", super::stream::cmd_xreadgroup, -7);
        self.register("XACK", super::stream::cmd_xack, -4);
        self.register("XPENDING", super::stream::cmd_xpending, -3);
        self.register("XCLAIM", super::stream::cmd_xclaim, -6);
        self.register("XAUTOCLAIM", super::stream::cmd_xautoclaim, -7);

        // === Blocking list ops (non-blocking stubs) ===
        self.register("BLPOP", super::list::cmd_blpop, -3);
        self.register("BRPOP", super::list::cmd_brpop, -3);
        self.register("BLMOVE", super::list::cmd_blmove, 6);
        self.register("BLMPOP", super::list::cmd_blmpop, -5);

        // === Remaining sorted set ===
        self.register("ZRANGESTORE", super::sorted_set::cmd_zrangestore, -5);
        self.register("ZUNION", super::sorted_set::cmd_zunion, -3);
        self.register("ZINTER", super::sorted_set::cmd_zinter, -3);
        self.register("ZDIFF", super::sorted_set::cmd_zdiff, -3);
        self.register("ZINTERCARD", super::sorted_set::cmd_zintercard, -3);
        self.register("ZMPOP", super::sorted_set::cmd_zmpop, -4);
        self.register("BZPOPMIN", super::sorted_set::cmd_bzpopmin, -3);
        self.register("BZPOPMAX", super::sorted_set::cmd_bzpopmax, -3);
        self.register("BZMPOP", super::sorted_set::cmd_bzmpop, -5);

        // === Bitmap ===
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

        // === Persistence commands ===
        self.register("SAVE", super::persistence_cmds::cmd_save, 1);
        self.register("BGSAVE", super::persistence_cmds::cmd_bgsave, 1);
        self.register("LASTSAVE", super::persistence_cmds::cmd_lastsave, 1);
        self.register("BGREWRITEAOF", super::persistence_cmds::cmd_bgrewriteaof, 1);

        // === Cluster commands (Phase 10) ===
        self.register("CLUSTER", super::cluster::cmd_cluster, -2);
        self.register("READONLY", super::cluster::cmd_readonly, 1);
        self.register("READWRITE", super::cluster::cmd_readwrite, 1);
        self.register("ASKING", super::cluster::cmd_asking, 1);

        // === Connection-level command stubs (Group 1) ===
        // These are intercepted in connection.rs but registered for COMMAND INFO.
        self.register("AUTH", super::stubs::cmd_auth_stub, -2);
        self.register("MULTI", super::stubs::cmd_multi_stub, 1);
        self.register("EXEC", super::stubs::cmd_exec_stub, 1);
        self.register("DISCARD", super::stubs::cmd_discard_stub, 1);
        self.register("WATCH", super::stubs::cmd_watch_stub, -2);
        self.register("UNWATCH", super::stubs::cmd_unwatch_stub, 1);
        self.register("SUBSCRIBE", super::stubs::cmd_subscribe_stub, -2);
        self.register("UNSUBSCRIBE", super::stubs::cmd_unsubscribe_stub, -1);
        self.register("PSUBSCRIBE", super::stubs::cmd_psubscribe_stub, -2);
        self.register("PUNSUBSCRIBE", super::stubs::cmd_punsubscribe_stub, -1);
        self.register("PUBLISH", super::stubs::cmd_publish_stub, 3);
        self.register("PUBSUB", super::stubs::cmd_pubsub_stub, -2);
        self.register("QUIT", super::stubs::cmd_quit_stub, 1);

        // === Deprecated aliases (Group 2) ===
        self.register("HMSET", super::hash::cmd_hset, -4);
        self.register("RPOPLPUSH", super::list::cmd_rpoplpush, 3);
        self.register("BRPOPLPUSH", super::list::cmd_brpoplpush, 4);
        self.register("HSTRLEN", super::hash::cmd_hstrlen, 3);
        self.register("GEORADIUS", super::stubs::cmd_georadius, -6);
        self.register("GEORADIUSBYMEMBER", super::stubs::cmd_georadiusbymember, -5);
        self.register("GEORADIUS_RO", super::stubs::cmd_georadius, -6);
        self.register("GEORADIUSBYMEMBER_RO", super::stubs::cmd_georadiusbymember, -5);

        // === Missing sorted set commands (Group 3) ===
        self.register("ZRANGEBYLEX", super::sorted_set::cmd_zrangebylex, -4);
        self.register("ZREVRANGEBYLEX", super::sorted_set::cmd_zrevrangebylex, -4);
        self.register("ZREMRANGEBYLEX", super::sorted_set::cmd_zremrangebylex, 4);
        self.register("ZREMRANGEBYRANK", super::sorted_set::cmd_zremrangebyrank, 4);
        self.register("ZREMRANGEBYSCORE", super::sorted_set::cmd_zremrangebyscore, 4);

        // === Other missing commands (Group 4) ===
        self.register("XSETID", super::stream::cmd_xsetid, -3);
        self.register("MOVE", super::keys::cmd_move, 3);
        self.register("ROLE", super::replication::cmd_role, 1);
        self.register("SHUTDOWN", super::server_cmds::cmd_shutdown, -1);
        self.register("DUMP", super::keys::cmd_dump, 2);
        self.register("RESTORE", super::keys::cmd_restore, -4);
        self.register("RESTORE-ASKING", super::keys::cmd_restore, -4);
        self.register("MONITOR", super::stubs::cmd_monitor, 1);
        self.register("WAITAOF", super::stubs::cmd_waitaof, 3);
        self.register("EVAL_RO", super::scripting::cmd_eval, -3);
        self.register("EVALSHA_RO", super::scripting::cmd_evalsha, -3);
        self.register("SPUBLISH", super::stubs::cmd_spublish, 3);
        self.register("SSUBSCRIBE", super::stubs::cmd_ssubscribe_stub, -2);
        self.register("SUNSUBSCRIBE", super::stubs::cmd_sunsubscribe_stub, -1);
        self.register("COMMANDLOG", super::stubs::cmd_commandlog, -2);
        self.register("PFDEBUG", super::stubs::cmd_pfdebug, -1);
        self.register("PFSELFTEST", super::stubs::cmd_pfselftest, 1);
        self.register("FAILOVER", super::stubs::cmd_failover, -1);
        self.register("SYNC", super::stubs::cmd_sync, 1);

        // === Namespace commands (Multi-tenancy) ===
        self.register("NAMESPACE", super::stubs::cmd_namespace_stub, -2);

        // === Module stubs (Phase 11) ===
        self.register("MODULE", super::advanced::cmd_module, -2);

        // === Latency (Phase 11) ===
        self.register("LATENCY", super::advanced::cmd_latency, -2);

        // === Per-field hash expiration stubs (Phase 11) ===
        self.register("HEXPIRE", super::advanced::cmd_hexpire, -4);
        self.register("HPEXPIRE", super::advanced::cmd_hpexpire, -4);
        self.register("HEXPIREAT", super::advanced::cmd_hexpireat, -4);
        self.register("HPEXPIREAT", super::advanced::cmd_hpexpireat, -4);
        self.register("HPERSIST", super::advanced::cmd_hpersist, -3);
        self.register("HTTL", super::advanced::cmd_httl, -3);
        self.register("HPTTL", super::advanced::cmd_hpttl, -3);
        self.register("HEXPIRETIME", super::advanced::cmd_hexpiretime, -3);
        self.register("HPEXPIRETIME", super::advanced::cmd_hpexpiretime, -3);

        // === LCS (Phase 11) ===
        self.register("LCS", super::advanced::cmd_lcs, -3);

        // === JSON commands ===
        self.register("JSON.SET", super::json::cmd_json_set, -4);
        self.register("JSON.GET", super::json::cmd_json_get, -2);
        self.register("JSON.DEL", super::json::cmd_json_del, -2);
        self.register("JSON.NUMINCRBY", super::json::cmd_json_numincrby, 4);
        self.register("JSON.STRAPPEND", super::json::cmd_json_strappend, 4);
        self.register("JSON.ARRAPPEND", super::json::cmd_json_arrappend, -4);
        self.register("JSON.ARRLEN", super::json::cmd_json_arrlen, -2);
        self.register("JSON.ARRPOP", super::json::cmd_json_arrpop, -2);
        self.register("JSON.TYPE", super::json::cmd_json_type, -2);
        self.register("JSON.OBJKEYS", super::json::cmd_json_objkeys, -2);
        self.register("JSON.OBJLEN", super::json::cmd_json_objlen, -2);
        self.register("JSON.TOGGLE", super::json::cmd_json_toggle, 3);
        self.register("JSON.NUMMULTBY", super::json::cmd_json_nummultby, 4);
        self.register("JSON.CLEAR", super::json::cmd_json_clear, -2);

        // === Bloom Filter commands ===
        self.register("BF.ADD", super::probabilistic::cmd_bf_add, 3);
        self.register("BF.EXISTS", super::probabilistic::cmd_bf_exists, 3);
        self.register("BF.MADD", super::probabilistic::cmd_bf_madd, -3);
        self.register("BF.MEXISTS", super::probabilistic::cmd_bf_mexists, -3);
        self.register("BF.RESERVE", super::probabilistic::cmd_bf_reserve, 4);
        self.register("BF.INFO", super::probabilistic::cmd_bf_info, 2);

        // === Count-Min Sketch commands ===
        self.register("CMS.INITBYDIM", super::probabilistic::cmd_cms_initbydim, 4);
        self.register("CMS.INITBYPROB", super::probabilistic::cmd_cms_initbyprob, 4);
        self.register("CMS.INCRBY", super::probabilistic::cmd_cms_incrby, -4);
        self.register("CMS.QUERY", super::probabilistic::cmd_cms_query, -3);
        self.register("CMS.MERGE", super::probabilistic::cmd_cms_merge, -4);
        self.register("CMS.INFO", super::probabilistic::cmd_cms_info, 2);

        // === Top-K commands ===
        self.register("TOPK.RESERVE", super::probabilistic::cmd_topk_reserve, -3);
        self.register("TOPK.ADD", super::probabilistic::cmd_topk_add, -3);
        self.register("TOPK.QUERY", super::probabilistic::cmd_topk_query, -3);
        self.register("TOPK.LIST", super::probabilistic::cmd_topk_list, -2);
        self.register("TOPK.COUNT", super::probabilistic::cmd_topk_count, -3);
        self.register("TOPK.INFO", super::probabilistic::cmd_topk_info, 2);

        // === Rate Limiting commands ===
        self.register("RATELIMIT.CHECK", super::ratelimit::cmd_ratelimit_check, 4);
        self.register("RATELIMIT.GET", super::ratelimit::cmd_ratelimit_get, 2);
        self.register("RATELIMIT.RESET", super::ratelimit::cmd_ratelimit_reset, 2);
        self.register("RATELIMIT.ACQUIRE", super::ratelimit::cmd_ratelimit_acquire, 4);
    }
}
