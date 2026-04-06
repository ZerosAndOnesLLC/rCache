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

    let used_memory = ctx.store.total_used_memory();
    let used_memory_human = if used_memory >= 1024 * 1024 * 1024 {
        format!("{:.2}G", used_memory as f64 / (1024.0 * 1024.0 * 1024.0))
    } else if used_memory >= 1024 * 1024 {
        format!("{:.2}M", used_memory as f64 / (1024.0 * 1024.0))
    } else if used_memory >= 1024 {
        format!("{:.2}K", used_memory as f64 / 1024.0)
    } else {
        format!("{}B", used_memory)
    };

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
         used_memory:{}\r\n\
         used_memory_human:{}\r\n\
         \r\n\
         # Stats\r\n\
         total_connections_received:0\r\n\
         total_commands_processed:0\r\n\
         keyspace_hits:0\r\n\
         keyspace_misses:0\r\n\
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
        used_memory,
        used_memory_human,
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

/// SLOWLOG command - returns actual slow log entries from the global slowlog.
pub fn cmd_slowlog(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 2 {
        return RespValue::error("ERR wrong number of arguments for 'slowlog' command");
    }
    let subcmd = String::from_utf8_lossy(&ctx.args[1]).to_uppercase();
    match subcmd.as_str() {
        "GET" => {
            let count = if ctx.args.len() >= 3 {
                String::from_utf8_lossy(&ctx.args[2])
                    .parse::<usize>()
                    .unwrap_or(128)
            } else {
                128
            };
            if let Ok(log) = GLOBAL_SLOWLOG.lock() {
                let entries: Vec<RespValue> = log
                    .iter()
                    .rev()
                    .take(count)
                    .map(|entry| {
                        let args: Vec<RespValue> = entry
                            .args
                            .iter()
                            .map(|a| RespValue::bulk_string(bytes::Bytes::from(a.clone())))
                            .collect();
                        RespValue::array(vec![
                            RespValue::integer(entry.id as i64),
                            RespValue::integer(entry.timestamp as i64),
                            RespValue::integer(entry.duration_us as i64),
                            RespValue::array(args),
                            RespValue::bulk_string(bytes::Bytes::from(entry.client_addr.clone())),
                            RespValue::bulk_string(bytes::Bytes::from(entry.client_name.clone())),
                        ])
                    })
                    .collect();
                RespValue::array(entries)
            } else {
                RespValue::array(vec![])
            }
        }
        "LEN" => {
            let len = GLOBAL_SLOWLOG
                .lock()
                .map(|log| log.len())
                .unwrap_or(0);
            RespValue::integer(len as i64)
        }
        "RESET" => {
            if let Ok(mut log) = GLOBAL_SLOWLOG.lock() {
                log.clear();
            }
            RespValue::ok()
        }
        _ => RespValue::error(format!(
            "ERR unknown subcommand or wrong number of arguments for 'slowlog|{}'",
            subcmd.to_lowercase()
        )),
    }
}

/// Global slowlog accessible from command handlers.
use std::sync::Mutex;

#[derive(Debug, Clone)]
pub struct SlowLogEntryGlobal {
    pub id: u64,
    pub timestamp: u64,
    pub duration_us: u64,
    pub args: Vec<String>,
    pub client_addr: String,
    pub client_name: String,
}

pub static GLOBAL_SLOWLOG: std::sync::LazyLock<Mutex<Vec<SlowLogEntryGlobal>>> =
    std::sync::LazyLock::new(|| Mutex::new(Vec::new()));

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
                Some(obj) => {
                    let mut size = obj.estimate_memory();
                    // If it's a compressed string, report the original (uncompressed) size
                    if let crate::storage::types::RedisObject::String(b) = obj {
                        size = crate::compression::original_size(b) + 32;
                    }
                    RespValue::integer(size as i64)
                }
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
/// This is a stub for the registry; the actual HELLO logic is in connection.rs.
pub fn cmd_hello(_ctx: &mut CommandContext) -> RespValue {
    // HELLO is fully handled in connection.rs before reaching the registry.
    // This stub exists for COMMAND INFO.
    RespValue::ok()
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

/// SHUTDOWN [NOSAVE|SAVE] - saves RDB and exits.
pub fn cmd_shutdown(ctx: &mut CommandContext) -> RespValue {
    let nosave = ctx.args.len() >= 2
        && String::from_utf8_lossy(&ctx.args[1]).to_uppercase() == "NOSAVE";

    if !nosave {
        // Save RDB before exit if there is data
        let total_keys: usize = (0..ctx.store.db_count()).map(|i| ctx.store.db(i).len()).sum();
        if total_keys > 0 {
            let path = std::path::Path::new("dump.rdb");
            match crate::persistence::rdb::save(ctx.store, path) {
                Ok(()) => {
                    tracing::info!("RDB saved on SHUTDOWN ({} keys)", total_keys);
                }
                Err(e) => {
                    tracing::error!("Failed to save RDB on SHUTDOWN: {}", e);
                }
            }
        }
    }

    // Signal shutdown by returning OK; in a full impl we'd use process::exit
    // but that would prevent proper cleanup
    RespValue::ok()
}
