use bytes::Bytes;
use std::time::{SystemTime, UNIX_EPOCH};
use crate::protocol::RespValue;
use crate::storage::RedisObject;
use super::registry::CommandContext;

pub fn cmd_ping(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() > 1 {
        RespValue::bulk_string(ctx.args[1].clone())
    } else {
        RespValue::simple_string("PONG")
    }
}

pub fn cmd_echo(ctx: &mut CommandContext) -> RespValue {
    RespValue::bulk_string(ctx.args[1].clone())
}

pub fn cmd_select(ctx: &mut CommandContext) -> RespValue {
    let index: usize = match String::from_utf8_lossy(&ctx.args[1]).parse() {
        Ok(v) => v,
        Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
    };
    if index >= ctx.store.db_count() {
        return RespValue::error("ERR DB index is out of range");
    }
    ctx.db_index = index;
    RespValue::ok()
}

pub fn cmd_dbsize(ctx: &mut CommandContext) -> RespValue {
    RespValue::integer(ctx.db().len() as i64)
}

pub fn cmd_flushdb(ctx: &mut CommandContext) -> RespValue {
    ctx.db().flush();
    RespValue::ok()
}

pub fn cmd_flushall(ctx: &mut CommandContext) -> RespValue {
    ctx.store.flush_all();
    RespValue::ok()
}

pub fn cmd_swapdb(ctx: &mut CommandContext) -> RespValue {
    let a: usize = match String::from_utf8_lossy(&ctx.args[1]).parse() {
        Ok(v) => v,
        Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
    };
    let b: usize = match String::from_utf8_lossy(&ctx.args[2]).parse() {
        Ok(v) => v,
        Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
    };
    if a >= ctx.store.db_count() || b >= ctx.store.db_count() {
        return RespValue::error("ERR invalid DB index");
    }
    ctx.store.swap_db(a, b);
    RespValue::ok()
}

pub fn cmd_time(_ctx: &mut CommandContext) -> RespValue {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let secs = now.as_secs();
    let micros = now.subsec_micros();
    RespValue::array(vec![
        RespValue::bulk_string(bytes::Bytes::from(secs.to_string())),
        RespValue::bulk_string(bytes::Bytes::from(micros.to_string())),
    ])
}

pub fn cmd_info(ctx: &mut CommandContext) -> RespValue {
    let uptime = ctx.start_time.elapsed();
    let mut keyspace = String::new();

    for i in 0..ctx.store.db_count() {
        let db = ctx.store.db(i);
        let keys = db.len();
        let expires = db.expires_len();
        if keys > 0 {
            keyspace.push_str(&format!("db{}:keys={},expires={}\r\n", i, keys, expires));
        }
    }

    let info = format!(
        "# Server\r\n\
         redis_version:7.2.0\r\n\
         rcache_version:{}\r\n\
         redis_mode:standalone\r\n\
         os:{}\r\n\
         tcp_port:6379\r\n\
         uptime_in_seconds:{}\r\n\
         uptime_in_days:{}\r\n\
         \r\n\
         # Clients\r\n\
         connected_clients:1\r\n\
         \r\n\
         # Memory\r\n\
         used_memory:0\r\n\
         used_memory_human:0B\r\n\
         \r\n\
         # Stats\r\n\
         total_connections_received:0\r\n\
         total_commands_processed:0\r\n\
         \r\n\
         # Keyspace\r\n\
         {}\
         ",
        env!("CARGO_PKG_VERSION"),
        std::env::consts::OS,
        uptime.as_secs(),
        uptime.as_secs() / 86400,
        keyspace,
    );

    RespValue::bulk_string(bytes::Bytes::from(info))
}

pub fn cmd_command(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() >= 2 {
        let subcmd = String::from_utf8_lossy(&ctx.args[1]).to_uppercase();
        match subcmd.as_str() {
            "COUNT" => {
                // We report the count from the registry, but we don't have access here.
                // Return a reasonable number.
                RespValue::integer(100)
            }
            "LIST" => {
                RespValue::array(vec![])
            }
            "INFO" => {
                RespValue::array(vec![])
            }
            "DOCS" => {
                RespValue::array(vec![])
            }
            _ => RespValue::error(format!("ERR unknown subcommand '{}'", subcmd)),
        }
    } else {
        RespValue::array(vec![])
    }
}

pub fn cmd_config(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 2 {
        return RespValue::wrong_arity("config");
    }

    let subcmd = String::from_utf8_lossy(&ctx.args[1]).to_uppercase();
    match subcmd.as_str() {
        "GET" => {
            if ctx.args.len() < 3 {
                return RespValue::wrong_arity("config|get");
            }
            let pattern = String::from_utf8_lossy(&ctx.args[2]).to_string();
            let mut results = Vec::new();

            // Return known config parameters matching the pattern
            let configs: Vec<(&str, String)> = vec![
                ("maxmemory", "0".to_string()),
                ("maxmemory-policy", "noeviction".to_string()),
                ("hz", "10".to_string()),
                ("databases", "16".to_string()),
                ("maxclients", "10000".to_string()),
                ("timeout", "0".to_string()),
                ("tcp-keepalive", "300".to_string()),
                ("lfu-log-factor", "10".to_string()),
                ("lfu-decay-time", "1".to_string()),
                ("save", "".to_string()),
                ("appendonly", "no".to_string()),
                ("bind", "0.0.0.0".to_string()),
            ];

            for (name, value) in &configs {
                if pattern == "*" || crate::storage::db::glob_match(&pattern, name) {
                    results.push(RespValue::bulk_string(Bytes::from(name.to_string())));
                    results.push(RespValue::bulk_string(Bytes::from(value.clone())));
                }
            }

            RespValue::array(results)
        }
        "SET" => {
            if ctx.args.len() < 4 {
                return RespValue::wrong_arity("config|set");
            }
            // Accept but mostly ignore config sets for compatibility
            RespValue::ok()
        }
        "RESETSTAT" => RespValue::ok(),
        "REWRITE" => RespValue::ok(),
        _ => RespValue::error(format!("ERR unknown subcommand or wrong number of arguments for 'config|{}'", subcmd.to_lowercase())),
    }
}

pub fn cmd_client(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 2 {
        return RespValue::wrong_arity("client");
    }

    let subcmd = String::from_utf8_lossy(&ctx.args[1]).to_uppercase();
    match subcmd.as_str() {
        "LIST" => {
            // Return minimal client info
            let info = "id=1 addr=127.0.0.1:0 fd=5 name= db=0 sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=0 obl=0 oll=0 omem=0 events=r cmd=client\r\n";
            RespValue::bulk_string(Bytes::from(info))
        }
        "GETNAME" => {
            // Client name is handled in connection layer; return null as default
            RespValue::Null
        }
        "SETNAME" => {
            if ctx.args.len() < 3 {
                return RespValue::wrong_arity("client|setname");
            }
            // Accept but handled in connection layer
            RespValue::ok()
        }
        "ID" => {
            // Return a default client ID; actual ID is in connection layer
            RespValue::integer(1)
        }
        "INFO" => {
            let info = "id=1 addr=127.0.0.1:0 fd=5 name= db=0 sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=0 obl=0 oll=0 omem=0 events=r cmd=client\r\n";
            RespValue::bulk_string(Bytes::from(info))
        }
        "NO-EVICT" => RespValue::ok(),
        "NO-TOUCH" => RespValue::ok(),
        _ => RespValue::error(format!("ERR unknown subcommand or wrong number of arguments for 'client|{}'", subcmd.to_lowercase())),
    }
}

pub fn cmd_slowlog(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 2 {
        return RespValue::wrong_arity("slowlog");
    }

    let subcmd = String::from_utf8_lossy(&ctx.args[1]).to_uppercase();
    match subcmd.as_str() {
        "GET" => {
            // Return empty slow log for now
            RespValue::array(vec![])
        }
        "LEN" => RespValue::integer(0),
        "RESET" => RespValue::ok(),
        _ => RespValue::error(format!("ERR unknown subcommand or wrong number of arguments for 'slowlog|{}'", subcmd.to_lowercase())),
    }
}

pub fn cmd_memory(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 2 {
        return RespValue::wrong_arity("memory");
    }

    let subcmd = String::from_utf8_lossy(&ctx.args[1]).to_uppercase();
    match subcmd.as_str() {
        "USAGE" => {
            if ctx.args.len() < 3 {
                return RespValue::wrong_arity("memory|usage");
            }
            let key = ctx.args[2].clone();
            let db = ctx.db();
            match db.get(&key) {
                Some(obj) => {
                    // Approximate memory usage
                    let size = match obj {
                        RedisObject::String(b) => 56 + b.len(),
                        RedisObject::List(l) => 128 + l.iter().map(|v| 64 + v.len()).sum::<usize>(),
                        RedisObject::Set(s) => 240 + s.iter().map(|v| 64 + v.len()).sum::<usize>(),
                        RedisObject::Hash(h) => 240 + h.iter().map(|(k, v)| 64 + k.len() + v.len()).sum::<usize>(),
                        RedisObject::SortedSet(z) => 240 + z.members.iter().map(|(k, _)| 128 + k.len()).sum::<usize>(),
                    };
                    RespValue::integer(size as i64)
                }
                None => RespValue::Null,
            }
        }
        "DOCTOR" => RespValue::bulk_string(Bytes::from("Sam, I have no memory problems")),
        "HELP" => {
            RespValue::array(vec![
                RespValue::simple_string("MEMORY USAGE <key> [SAMPLES <count>] - Estimate memory usage of key"),
                RespValue::simple_string("MEMORY DOCTOR - Outputs memory problems report"),
                RespValue::simple_string("MEMORY HELP - Show this help"),
            ])
        }
        _ => RespValue::error(format!("ERR unknown subcommand or wrong number of arguments for 'memory|{}'", subcmd.to_lowercase())),
    }
}

pub fn cmd_lolwut(_ctx: &mut CommandContext) -> RespValue {
    let art = r#"
   _____       _____           _
  / ____|     / ____|         | |
 | |     __ _| |     __ _  ___| |__   ___
 | |    / _` | |    / _` |/ __| '_ \ / _ \
 | |___| (_| | |___| (_| | (__| | | |  __/
  \_____\__,_|\_____\__,_|\___|_| |_|\___|

rCache - Redis-compatible in-memory data store
"#;
    RespValue::bulk_string(Bytes::from(art.trim_start_matches('\n')))
}

pub fn cmd_hello(ctx: &mut CommandContext) -> RespValue {
    // HELLO [protover [AUTH username password] [SETNAME clientname]]
    // We only support RESP2, so we acknowledge the command but stay in RESP2 mode
    let mut _proto = 2;

    if ctx.args.len() > 1 {
        _proto = match String::from_utf8_lossy(&ctx.args[1]).parse::<i64>() {
            Ok(v) if v == 2 || v == 3 => v,
            Ok(v) => return RespValue::error(format!("NOPROTO unsupported protocol version {}", v)),
            Err(_) => return RespValue::error("ERR Protocol version is not an integer or out of range"),
        };
    }

    // Return server info as an array of key-value pairs (RESP2 compatible)
    RespValue::array(vec![
        RespValue::bulk_string(Bytes::from("server")),
        RespValue::bulk_string(Bytes::from("rcache")),
        RespValue::bulk_string(Bytes::from("version")),
        RespValue::bulk_string(Bytes::from(env!("CARGO_PKG_VERSION"))),
        RespValue::bulk_string(Bytes::from("proto")),
        RespValue::integer(2),
        RespValue::bulk_string(Bytes::from("id")),
        RespValue::integer(1),
        RespValue::bulk_string(Bytes::from("mode")),
        RespValue::bulk_string(Bytes::from("standalone")),
        RespValue::bulk_string(Bytes::from("role")),
        RespValue::bulk_string(Bytes::from("master")),
        RespValue::bulk_string(Bytes::from("modules")),
        RespValue::array(vec![]),
    ])
}

pub fn cmd_reset(_ctx: &mut CommandContext) -> RespValue {
    // RESET command resets the connection state
    RespValue::simple_string("RESET")
}

pub fn cmd_debug(_ctx: &mut CommandContext) -> RespValue {
    RespValue::ok()
}

pub fn cmd_wait(_ctx: &mut CommandContext) -> RespValue {
    // In standalone mode, WAIT always returns 0 (no replicas)
    RespValue::integer(0)
}

