use bytes::Bytes;
use crate::protocol::RespValue;
use super::registry::CommandContext;

const ACL_CATEGORIES: &[&str] = &[
    "@read", "@write", "@admin", "@dangerous", "@slow", "@fast",
    "@string", "@list", "@set", "@hash", "@sortedset", "@pubsub",
    "@connection", "@server", "@generic", "@keyspace", "@scripting",
];

pub fn cmd_acl(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 2 {
        return RespValue::error("ERR wrong number of arguments for 'acl' command");
    }

    let subcmd = String::from_utf8_lossy(&ctx.args[1]).to_uppercase();

    match subcmd.as_str() {
        "WHOAMI" => cmd_acl_whoami(),
        "LIST" => cmd_acl_list(),
        "USERS" => cmd_acl_users(),
        "CAT" => cmd_acl_cat(ctx),
        "SETUSER" => cmd_acl_setuser(ctx),
        "GETUSER" => cmd_acl_getuser(ctx),
        "DELUSER" => cmd_acl_deluser(ctx),
        "LOG" => cmd_acl_log(ctx),
        _ => RespValue::error(format!(
            "ERR unknown subcommand or wrong number of arguments for 'acl|{}' command",
            subcmd.to_lowercase()
        )),
    }
}

fn cmd_acl_whoami() -> RespValue {
    RespValue::bulk_string(Bytes::from("default"))
}

fn cmd_acl_list() -> RespValue {
    RespValue::array(vec![
        RespValue::bulk_string(Bytes::from("user default on ~* &* +@all")),
    ])
}

fn cmd_acl_users() -> RespValue {
    RespValue::array(vec![
        RespValue::bulk_string(Bytes::from("default")),
    ])
}

fn cmd_acl_cat(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() >= 3 {
        let category = String::from_utf8_lossy(&ctx.args[2]).to_lowercase();
        let cat_with_at = if category.starts_with('@') {
            category.clone()
        } else {
            format!("@{}", category)
        };

        // Check if it's a valid category
        if !ACL_CATEGORIES.iter().any(|c| *c == cat_with_at) {
            return RespValue::error(format!("ERR Unknown ACL cat category '{}'", category));
        }

        // Return some representative commands for each category
        let commands = match cat_with_at.as_str() {
            "@read" => vec!["get", "mget", "hget", "lrange", "sismember", "zrange", "xrange", "xread"],
            "@write" => vec!["set", "del", "hset", "lpush", "sadd", "zadd", "xadd"],
            "@admin" => vec!["acl", "config", "debug", "replicaof", "slaveof"],
            "@dangerous" => vec!["flushdb", "flushall", "keys", "sort"],
            "@slow" => vec!["sort", "keys", "smembers", "hgetall", "lrange"],
            "@fast" => vec!["get", "set", "sismember", "hget", "lpush", "rpush"],
            "@string" => vec!["set", "get", "mset", "mget", "incr", "decr", "append", "strlen"],
            "@list" => vec!["lpush", "rpush", "lpop", "rpop", "llen", "lrange", "lindex"],
            "@set" => vec!["sadd", "srem", "sismember", "smembers", "scard", "sdiff", "sinter", "sunion"],
            "@hash" => vec!["hset", "hget", "hdel", "hlen", "hkeys", "hvals", "hgetall"],
            "@sortedset" => vec!["zadd", "zrem", "zscore", "zrange", "zrank", "zcard"],
            "@pubsub" => vec!["subscribe", "unsubscribe", "publish"],
            "@connection" => vec!["ping", "echo", "select", "auth", "quit"],
            "@server" => vec!["info", "dbsize", "time", "command", "config"],
            "@generic" => vec!["del", "exists", "expire", "ttl", "type", "rename", "keys", "scan"],
            "@keyspace" => vec!["del", "exists", "expire", "ttl", "type", "rename", "unlink"],
            "@scripting" => vec!["eval", "evalsha", "script"],
            _ => vec![],
        };

        RespValue::array(
            commands.into_iter()
                .map(|c| RespValue::bulk_string(Bytes::from(c)))
                .collect()
        )
    } else {
        // Return all categories
        RespValue::array(
            ACL_CATEGORIES.iter()
                .map(|c| RespValue::bulk_string(Bytes::from(*c)))
                .collect()
        )
    }
}

fn cmd_acl_setuser(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 3 {
        return RespValue::error("ERR wrong number of arguments for 'acl|setuser' command");
    }
    // Stub: accept but no-op
    RespValue::ok()
}

fn cmd_acl_getuser(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 3 {
        return RespValue::error("ERR wrong number of arguments for 'acl|getuser' command");
    }

    let username = String::from_utf8_lossy(&ctx.args[2]);
    if username == "default" {
        RespValue::array(vec![
            RespValue::bulk_string(Bytes::from("flags")),
            RespValue::array(vec![RespValue::bulk_string(Bytes::from("on"))]),
            RespValue::bulk_string(Bytes::from("passwords")),
            RespValue::array(vec![]),
            RespValue::bulk_string(Bytes::from("commands")),
            RespValue::bulk_string(Bytes::from("+@all")),
            RespValue::bulk_string(Bytes::from("keys")),
            RespValue::bulk_string(Bytes::from("~*")),
            RespValue::bulk_string(Bytes::from("channels")),
            RespValue::bulk_string(Bytes::from("&*")),
        ])
    } else {
        RespValue::Null
    }
}

fn cmd_acl_deluser(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 3 {
        return RespValue::error("ERR wrong number of arguments for 'acl|deluser' command");
    }

    let username = String::from_utf8_lossy(&ctx.args[2]);
    if username == "default" {
        RespValue::error("ERR The 'default' user cannot be removed")
    } else {
        // Stub: user doesn't exist, return 0
        RespValue::integer(0)
    }
}

fn cmd_acl_log(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() >= 3 {
        let subcmd = String::from_utf8_lossy(&ctx.args[2]).to_uppercase();
        if subcmd == "RESET" {
            return RespValue::ok();
        }
    }
    // Return empty array
    RespValue::array(vec![])
}
