use std::time::{SystemTime, UNIX_EPOCH};
use crate::protocol::RespValue;
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
         # Replication\r\n\
         role:master\r\n\
         connected_slaves:0\r\n\
         master_failover_state:no-failover\r\n\
         master_replid:0000000000000000000000000000000000000000\r\n\
         master_replid2:0000000000000000000000000000000000000000\r\n\
         master_repl_offset:0\r\n\
         second_repl_offset:-1\r\n\
         repl_backlog_active:0\r\n\
         repl_backlog_size:1048576\r\n\
         repl_backlog_first_byte_offset:0\r\n\
         repl_backlog_histlen:0\r\n\
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

/// CLIENT command dispatcher.
pub fn cmd_client(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 2 {
        return RespValue::error("ERR wrong number of arguments for 'client' command");
    }

    let subcmd = String::from_utf8_lossy(&ctx.args[1]).to_uppercase();
    match subcmd.as_str() {
        "SETNAME" => RespValue::ok(),
        "GETNAME" => RespValue::Null,
        "ID" => RespValue::integer(1),
        "LIST" => RespValue::bulk_string(bytes::Bytes::new()),
        "INFO" => RespValue::bulk_string(bytes::Bytes::new()),
        "KILL" => RespValue::ok(),
        "REPLY" => RespValue::ok(),
        "NO-EVICT" | "NOEVICT" => RespValue::ok(),
        "NO-TOUCH" | "NOTOUCH" => RespValue::ok(),
        "TRACKING" => RespValue::ok(),
        "CACHING" => RespValue::ok(),
        _ => RespValue::error(format!(
            "ERR unknown subcommand or wrong number of arguments for 'client|{}'",
            subcmd.to_lowercase()
        )),
    }
}

/// CONFIG command dispatcher.
pub fn cmd_config(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 2 {
        return RespValue::error("ERR wrong number of arguments for 'config' command");
    }

    let subcmd = String::from_utf8_lossy(&ctx.args[1]).to_uppercase();
    match subcmd.as_str() {
        "GET" => {
            if ctx.args.len() < 3 {
                return RespValue::wrong_arity("config|get");
            }
            let param = String::from_utf8_lossy(&ctx.args[2]).to_lowercase();
            match param.as_str() {
                "notify-keyspace-events" => RespValue::array(vec![
                    RespValue::bulk_string(bytes::Bytes::from("notify-keyspace-events")),
                    RespValue::bulk_string(bytes::Bytes::new()),
                ]),
                "save" => RespValue::array(vec![
                    RespValue::bulk_string(bytes::Bytes::from("save")),
                    RespValue::bulk_string(bytes::Bytes::new()),
                ]),
                "appendonly" => RespValue::array(vec![
                    RespValue::bulk_string(bytes::Bytes::from("appendonly")),
                    RespValue::bulk_string(bytes::Bytes::from("no")),
                ]),
                _ => RespValue::array(vec![]),
            }
        }
        "SET" => {
            if ctx.args.len() < 4 {
                return RespValue::wrong_arity("config|set");
            }
            RespValue::ok()
        }
        "RESETSTAT" => RespValue::ok(),
        "REWRITE" => RespValue::ok(),
        _ => RespValue::error(format!(
            "ERR unknown subcommand or wrong number of arguments for 'config|{}'",
            subcmd.to_lowercase()
        )),
    }
}

/// SLOWLOG command stub.
pub fn cmd_slowlog(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 2 {
        return RespValue::error("ERR wrong number of arguments for 'slowlog' command");
    }
    let subcmd = String::from_utf8_lossy(&ctx.args[1]).to_uppercase();
    match subcmd.as_str() {
        "GET" => RespValue::array(vec![]),
        "LEN" => RespValue::integer(0),
        "RESET" => RespValue::ok(),
        _ => RespValue::error(format!(
            "ERR unknown subcommand or wrong number of arguments for 'slowlog|{}'",
            subcmd.to_lowercase()
        )),
    }
}

/// MEMORY command stub.
pub fn cmd_memory(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 2 {
        return RespValue::error("ERR wrong number of arguments for 'memory' command");
    }
    let subcmd = String::from_utf8_lossy(&ctx.args[1]).to_uppercase();
    match subcmd.as_str() {
        "USAGE" => {
            if ctx.args.len() < 3 {
                return RespValue::wrong_arity("memory|usage");
            }
            let key = ctx.args[2].clone();
            match ctx.db().get(&key) {
                Some(obj) => RespValue::integer(obj.estimate_memory() as i64),
                None => RespValue::Null,
            }
        }
        "DOCTOR" => RespValue::bulk_string(bytes::Bytes::from("Sam, I have no memory problems")),
        "MALLOC-STATS" => RespValue::bulk_string(bytes::Bytes::from("Memory allocator stats not available")),
        "PURGE" => RespValue::ok(),
        "STATS" => RespValue::bulk_string(bytes::Bytes::from("peak.allocated:0\r\ntotal.allocated:0\r\n")),
        _ => RespValue::error(format!(
            "ERR unknown subcommand or wrong number of arguments for 'memory|{}'",
            subcmd.to_lowercase()
        )),
    }
}

/// LOLWUT command - display server art.
pub fn cmd_lolwut(_ctx: &mut CommandContext) -> RespValue {
    let art = format!(
        "rCache v{}\n\
         ____   ____          _          \n\
         |  _ \\ / ___|__ _ ___| |__   ___ \n\
         | |_) | |   / _` / __| '_ \\ / _ \\\n\
         |  _ <| |__| (_| \\__ \\ | | |  __/\n\
         |_| \\_\\\\____\\__,_|___/_| |_|\\___|\n",
        env!("CARGO_PKG_VERSION")
    );
    RespValue::bulk_string(bytes::Bytes::from(art))
}

/// HELLO command - switch protocol / handshake.
pub fn cmd_hello(ctx: &mut CommandContext) -> RespValue {
    // HELLO [protover [AUTH username password] [SETNAME clientname]]
    let proto = if ctx.args.len() >= 2 {
        match String::from_utf8_lossy(&ctx.args[1]).parse::<i64>() {
            Ok(2) => 2,
            Ok(3) => {
                return RespValue::error("NOPROTO unsupported protocol version");
            }
            _ => 2,
        }
    } else {
        2
    };
    RespValue::array(vec![
        RespValue::bulk_string(bytes::Bytes::from("server")),
        RespValue::bulk_string(bytes::Bytes::from("rcache")),
        RespValue::bulk_string(bytes::Bytes::from("version")),
        RespValue::bulk_string(bytes::Bytes::from(env!("CARGO_PKG_VERSION"))),
        RespValue::bulk_string(bytes::Bytes::from("proto")),
        RespValue::integer(proto),
        RespValue::bulk_string(bytes::Bytes::from("id")),
        RespValue::integer(1),
        RespValue::bulk_string(bytes::Bytes::from("mode")),
        RespValue::bulk_string(bytes::Bytes::from("standalone")),
        RespValue::bulk_string(bytes::Bytes::from("role")),
        RespValue::bulk_string(bytes::Bytes::from("master")),
        RespValue::bulk_string(bytes::Bytes::from("modules")),
        RespValue::array(vec![]),
    ])
}

/// RESET command - reset connection state.
pub fn cmd_reset(_ctx: &mut CommandContext) -> RespValue {
    RespValue::simple_string("RESET")
}

/// DEBUG command stub.
pub fn cmd_debug(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 2 {
        return RespValue::error("ERR wrong number of arguments for 'debug' command");
    }
    let subcmd = String::from_utf8_lossy(&ctx.args[1]).to_uppercase();
    match subcmd.as_str() {
        "SLEEP" => {
            if ctx.args.len() >= 3 {
                if let Ok(secs) = String::from_utf8_lossy(&ctx.args[2]).parse::<f64>() {
                    std::thread::sleep(std::time::Duration::from_secs_f64(secs));
                }
            }
            RespValue::ok()
        }
        "SET-ACTIVE-EXPIRE" => RespValue::ok(),
        "JMAP" => RespValue::ok(),
        "RELOAD" => RespValue::ok(),
        "LOADAOF" => RespValue::ok(),
        "OBJECT" => RespValue::ok(),
        _ => RespValue::error(format!("ERR unknown subcommand '{}'", subcmd)),
    }
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
