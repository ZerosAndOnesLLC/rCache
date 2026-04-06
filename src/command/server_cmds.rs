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
