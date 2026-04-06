use bytes::Bytes;
use crate::protocol::RespValue;
use super::registry::CommandContext;

const ACL_CATEGORIES: &[&str] = &[
    "@read", "@write", "@admin", "@dangerous", "@slow", "@fast",
    "@string", "@list", "@set", "@hash", "@sortedset", "@pubsub",
    "@connection", "@server", "@generic", "@keyspace", "@scripting",
];

/// Commands belonging to each category (representative subset).
fn category_commands(cat: &str) -> Vec<&'static str> {
    match cat {
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
    }
}

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
        "GENPASS" => cmd_acl_genpass(ctx),
        "LOG" => cmd_acl_log(ctx),
        _ => RespValue::error(format!(
            "ERR unknown subcommand or wrong number of arguments for 'acl|{}' command",
            subcmd.to_lowercase()
        )),
    }
}

fn cmd_acl_whoami() -> RespValue {
    // The actual user is tracked in the connection; we return "default" from here
    // as the command handler doesn't have access to connection state.
    RespValue::bulk_string(Bytes::from("default"))
}

fn cmd_acl_list() -> RespValue {
    if let Ok(users) = ACL_USERS.lock() {
        let mut result = Vec::new();
        for (name, user) in users.iter() {
            let mut desc = format!("user {}", name);
            if user.enabled {
                desc.push_str(" on");
            } else {
                desc.push_str(" off");
            }
            if user.all_keys {
                desc.push_str(" ~*");
            } else {
                for pat in &user.key_patterns {
                    desc.push_str(&format!(" ~{}", pat));
                }
            }
            desc.push_str(" &*"); // channel patterns
            if user.all_commands {
                desc.push_str(" +@all");
            } else {
                for cmd in &user.allowed_commands {
                    desc.push_str(&format!(" +{}", cmd.to_lowercase()));
                }
                for cmd in &user.denied_commands {
                    desc.push_str(&format!(" -{}", cmd.to_lowercase()));
                }
            }
            result.push(RespValue::bulk_string(Bytes::from(desc)));
        }
        RespValue::array(result)
    } else {
        RespValue::array(vec![
            RespValue::bulk_string(Bytes::from("user default on ~* &* +@all")),
        ])
    }
}

fn cmd_acl_users() -> RespValue {
    if let Ok(users) = ACL_USERS.lock() {
        let names: Vec<RespValue> = users
            .keys()
            .map(|n| RespValue::bulk_string(Bytes::from(n.clone())))
            .collect();
        RespValue::array(names)
    } else {
        RespValue::array(vec![
            RespValue::bulk_string(Bytes::from("default")),
        ])
    }
}

fn cmd_acl_cat(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() >= 3 {
        let category = String::from_utf8_lossy(&ctx.args[2]).to_lowercase();
        let cat_with_at = if category.starts_with('@') {
            category.clone()
        } else {
            format!("@{}", category)
        };

        if !ACL_CATEGORIES.iter().any(|c| *c == cat_with_at) {
            return RespValue::error(format!("ERR Unknown ACL cat category '{}'", category));
        }

        let commands = category_commands(&cat_with_at);
        RespValue::array(
            commands
                .into_iter()
                .map(|c| RespValue::bulk_string(Bytes::from(c)))
                .collect(),
        )
    } else {
        RespValue::array(
            ACL_CATEGORIES
                .iter()
                .map(|c| RespValue::bulk_string(Bytes::from(*c)))
                .collect(),
        )
    }
}

fn cmd_acl_setuser(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 3 {
        return RespValue::error("ERR wrong number of arguments for 'acl|setuser' command");
    }

    let username = String::from_utf8_lossy(&ctx.args[2]).to_string();

    if let Ok(mut users) = ACL_USERS.lock() {
        let user = users.entry(username).or_insert_with(|| AclUserEntry {
            enabled: false,
            passwords: Vec::new(),
            allowed_commands: std::collections::HashSet::new(),
            denied_commands: std::collections::HashSet::new(),
            key_patterns: Vec::new(),
            channel_patterns: Vec::new(),
            all_commands: false,
            all_keys: false,
            no_pass: false,
        });

        // Parse rules
        for i in 3..ctx.args.len() {
            let rule = String::from_utf8_lossy(&ctx.args[i]).to_string();
            match rule.as_str() {
                "on" => user.enabled = true,
                "off" => user.enabled = false,
                "allcommands" => {
                    user.all_commands = true;
                    user.denied_commands.clear();
                }
                "nocommands" => {
                    user.all_commands = false;
                    user.allowed_commands.clear();
                }
                "allkeys" => {
                    user.all_keys = true;
                    user.key_patterns = vec!["*".to_string()];
                }
                "resetkeys" => {
                    user.all_keys = false;
                    user.key_patterns.clear();
                }
                "nopass" => {
                    user.no_pass = true;
                    user.passwords.clear();
                }
                "resetpass" => {
                    user.no_pass = false;
                    user.passwords.clear();
                }
                "allchannels" => {
                    user.channel_patterns = vec!["*".to_string()];
                }
                "resetchannels" => {
                    user.channel_patterns.clear();
                }
                _ if rule.starts_with('>') => {
                    // Add password (>password)
                    let pass = &rule[1..];
                    use sha2::{Sha256, Digest};
                    let hash = format!("{:x}", Sha256::digest(pass.as_bytes()));
                    if !user.passwords.contains(&hash) {
                        user.passwords.push(hash);
                    }
                    user.no_pass = false;
                }
                _ if rule.starts_with('<') => {
                    // Remove password (<password)
                    let pass = &rule[1..];
                    use sha2::{Sha256, Digest};
                    let hash = format!("{:x}", Sha256::digest(pass.as_bytes()));
                    user.passwords.retain(|p| p != &hash);
                }
                _ if rule.starts_with("+@") => {
                    // Allow category (+@category)
                    let cat = &rule[1..]; // includes the @
                    let cmds = category_commands(cat);
                    for cmd in cmds {
                        user.allowed_commands.insert(cmd.to_uppercase());
                    }
                }
                _ if rule.starts_with("-@") => {
                    // Deny category (-@category)
                    let cat = &rule[1..];
                    let cmds = category_commands(cat);
                    for cmd in cmds {
                        user.denied_commands.insert(cmd.to_uppercase());
                        user.allowed_commands.remove(&cmd.to_uppercase());
                    }
                }
                _ if rule.starts_with('+') => {
                    // Allow command (+cmd)
                    let cmd = rule[1..].to_uppercase();
                    user.allowed_commands.insert(cmd.clone());
                    user.denied_commands.remove(&cmd);
                }
                _ if rule.starts_with('-') => {
                    // Deny command (-cmd)
                    let cmd = rule[1..].to_uppercase();
                    user.denied_commands.insert(cmd.clone());
                    user.allowed_commands.remove(&cmd);
                }
                _ if rule.starts_with('~') => {
                    // Key pattern (~pattern)
                    let pattern = rule[1..].to_string();
                    if pattern == "*" {
                        user.all_keys = true;
                    }
                    user.key_patterns.push(pattern);
                }
                _ if rule.starts_with('&') => {
                    // Channel pattern (&pattern)
                    let pattern = rule[1..].to_string();
                    user.channel_patterns.push(pattern);
                }
                _ => {
                    return RespValue::error(format!("ERR Unknown ACL rule '{}'", rule));
                }
            }
        }

        RespValue::ok()
    } else {
        RespValue::error("ERR internal error")
    }
}

fn cmd_acl_getuser(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 3 {
        return RespValue::error("ERR wrong number of arguments for 'acl|getuser' command");
    }

    let username = String::from_utf8_lossy(&ctx.args[2]).to_string();

    if let Ok(users) = ACL_USERS.lock() {
        if let Some(user) = users.get(&username) {
            let mut flags = Vec::new();
            if user.enabled {
                flags.push(RespValue::bulk_string(Bytes::from("on")));
            } else {
                flags.push(RespValue::bulk_string(Bytes::from("off")));
            }
            if user.all_keys {
                flags.push(RespValue::bulk_string(Bytes::from("allkeys")));
            }
            if user.all_commands {
                flags.push(RespValue::bulk_string(Bytes::from("allcommands")));
            }
            if user.no_pass {
                flags.push(RespValue::bulk_string(Bytes::from("nopass")));
            }

            let passwords: Vec<RespValue> = user
                .passwords
                .iter()
                .map(|p| RespValue::bulk_string(Bytes::from(p.clone())))
                .collect();

            let mut commands_str = String::new();
            if user.all_commands {
                commands_str.push_str("+@all");
            } else {
                let cmds: Vec<String> = user.allowed_commands.iter().map(|c| format!("+{}", c.to_lowercase())).collect();
                commands_str.push_str(&cmds.join(" "));
            }
            for cmd in &user.denied_commands {
                commands_str.push_str(&format!(" -{}", cmd.to_lowercase()));
            }

            let keys_str = if user.all_keys {
                "~*".to_string()
            } else {
                user.key_patterns
                    .iter()
                    .map(|p| format!("~{}", p))
                    .collect::<Vec<_>>()
                    .join(" ")
            };

            let channels_str = user
                .channel_patterns
                .iter()
                .map(|p| format!("&{}", p))
                .collect::<Vec<_>>()
                .join(" ");

            RespValue::array(vec![
                RespValue::bulk_string(Bytes::from("flags")),
                RespValue::array(flags),
                RespValue::bulk_string(Bytes::from("passwords")),
                RespValue::array(passwords),
                RespValue::bulk_string(Bytes::from("commands")),
                RespValue::bulk_string(Bytes::from(commands_str)),
                RespValue::bulk_string(Bytes::from("keys")),
                RespValue::bulk_string(Bytes::from(keys_str)),
                RespValue::bulk_string(Bytes::from("channels")),
                RespValue::bulk_string(Bytes::from(channels_str)),
            ])
        } else {
            RespValue::Null
        }
    } else {
        RespValue::Null
    }
}

fn cmd_acl_deluser(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 3 {
        return RespValue::error("ERR wrong number of arguments for 'acl|deluser' command");
    }

    let mut deleted = 0i64;
    if let Ok(mut users) = ACL_USERS.lock() {
        for i in 2..ctx.args.len() {
            let username = String::from_utf8_lossy(&ctx.args[i]).to_string();
            if username == "default" {
                return RespValue::error("ERR The 'default' user cannot be removed");
            }
            if users.remove(&username).is_some() {
                deleted += 1;
            }
        }
    }
    RespValue::integer(deleted)
}

fn cmd_acl_genpass(ctx: &mut CommandContext) -> RespValue {
    let bits = if ctx.args.len() >= 3 {
        String::from_utf8_lossy(&ctx.args[2])
            .parse::<usize>()
            .unwrap_or(256)
    } else {
        256
    };

    let bytes_needed = (bits + 7) / 8;
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let random_bytes: Vec<u8> = (0..bytes_needed).map(|_| rng.r#gen()).collect();
    let hex: String = random_bytes.iter().map(|b| format!("{:02x}", b)).collect();
    // Truncate to the requested number of hex chars (bits / 4)
    let hex_chars = bits / 4;
    let result = if hex.len() > hex_chars {
        &hex[..hex_chars]
    } else {
        &hex
    };
    RespValue::bulk_string(Bytes::from(result.to_string()))
}

fn cmd_acl_log(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() >= 3 {
        let subcmd = String::from_utf8_lossy(&ctx.args[2]).to_uppercase();
        if subcmd == "RESET" {
            return RespValue::ok();
        }
    }
    RespValue::array(vec![])
}

// Global ACL user registry (accessible from command handlers).
// This mirrors the one in SharedState for use by commands that don't have
// access to the async SharedState.
use std::sync::Mutex;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
struct AclUserEntry {
    enabled: bool,
    passwords: Vec<String>,
    allowed_commands: HashSet<String>,
    denied_commands: HashSet<String>,
    key_patterns: Vec<String>,
    channel_patterns: Vec<String>,
    all_commands: bool,
    all_keys: bool,
    no_pass: bool,
}

static ACL_USERS: std::sync::LazyLock<Mutex<HashMap<String, AclUserEntry>>> =
    std::sync::LazyLock::new(|| {
        let mut users = HashMap::new();
        users.insert(
            "default".to_string(),
            AclUserEntry {
                enabled: true,
                passwords: Vec::new(),
                allowed_commands: HashSet::new(),
                denied_commands: HashSet::new(),
                key_patterns: vec!["*".to_string()],
                channel_patterns: vec!["*".to_string()],
                all_commands: true,
                all_keys: true,
                no_pass: true,
            },
        );
        Mutex::new(users)
    });
