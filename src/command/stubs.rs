use bytes::Bytes;
use crate::protocol::RespValue;
use super::registry::CommandContext;

// ============================================================
// Group 1: Connection-level command stubs
// These commands are intercepted in connection.rs before reaching
// the registry, but we register them so COMMAND INFO reports them.
// ============================================================

pub fn cmd_auth_stub(_ctx: &mut CommandContext) -> RespValue {
    RespValue::error("ERR AUTH handled at connection level")
}

pub fn cmd_multi_stub(_ctx: &mut CommandContext) -> RespValue {
    RespValue::error("ERR MULTI handled at connection level")
}

pub fn cmd_exec_stub(_ctx: &mut CommandContext) -> RespValue {
    RespValue::error("ERR EXEC handled at connection level")
}

pub fn cmd_discard_stub(_ctx: &mut CommandContext) -> RespValue {
    RespValue::error("ERR DISCARD handled at connection level")
}

pub fn cmd_watch_stub(_ctx: &mut CommandContext) -> RespValue {
    RespValue::error("ERR WATCH handled at connection level")
}

pub fn cmd_unwatch_stub(_ctx: &mut CommandContext) -> RespValue {
    RespValue::error("ERR UNWATCH handled at connection level")
}

pub fn cmd_subscribe_stub(_ctx: &mut CommandContext) -> RespValue {
    RespValue::error("ERR SUBSCRIBE handled at connection level")
}

pub fn cmd_unsubscribe_stub(_ctx: &mut CommandContext) -> RespValue {
    RespValue::error("ERR UNSUBSCRIBE handled at connection level")
}

pub fn cmd_psubscribe_stub(_ctx: &mut CommandContext) -> RespValue {
    RespValue::error("ERR PSUBSCRIBE handled at connection level")
}

pub fn cmd_punsubscribe_stub(_ctx: &mut CommandContext) -> RespValue {
    RespValue::error("ERR PUNSUBSCRIBE handled at connection level")
}

pub fn cmd_publish_stub(_ctx: &mut CommandContext) -> RespValue {
    RespValue::error("ERR PUBLISH handled at connection level")
}

pub fn cmd_pubsub_stub(_ctx: &mut CommandContext) -> RespValue {
    RespValue::error("ERR PUBSUB handled at connection level")
}

pub fn cmd_quit_stub(_ctx: &mut CommandContext) -> RespValue {
    RespValue::ok()
}

// ============================================================
// Group 4: Other missing command stubs
// ============================================================

/// MONITOR - stub that returns OK then does nothing
pub fn cmd_monitor(_ctx: &mut CommandContext) -> RespValue {
    RespValue::ok()
}

/// WAITAOF - stub returning [0, 0]
pub fn cmd_waitaof(_ctx: &mut CommandContext) -> RespValue {
    RespValue::array(vec![
        RespValue::integer(0),
        RespValue::integer(0),
    ])
}

/// SPUBLISH channel message - sharded pub/sub stub
pub fn cmd_spublish(_ctx: &mut CommandContext) -> RespValue {
    RespValue::integer(0)
}

/// SSUBSCRIBE - sharded subscribe stub
pub fn cmd_ssubscribe_stub(_ctx: &mut CommandContext) -> RespValue {
    RespValue::error("ERR SSUBSCRIBE handled at connection level")
}

/// SUNSUBSCRIBE - sharded unsubscribe stub
pub fn cmd_sunsubscribe_stub(_ctx: &mut CommandContext) -> RespValue {
    RespValue::error("ERR SUNSUBSCRIBE handled at connection level")
}

/// COMMANDLOG subcommand - stub
pub fn cmd_commandlog(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 2 {
        return RespValue::wrong_arity("commandlog");
    }
    let subcmd = String::from_utf8_lossy(&ctx.args[1]).to_uppercase();
    match subcmd.as_str() {
        "GET" => RespValue::array(vec![]),
        "LEN" => RespValue::integer(0),
        "RESET" => RespValue::ok(),
        _ => RespValue::error(format!(
            "ERR unknown subcommand or wrong number of arguments for 'commandlog|{}'",
            subcmd.to_lowercase()
        )),
    }
}

/// PFDEBUG - stub returning OK
pub fn cmd_pfdebug(_ctx: &mut CommandContext) -> RespValue {
    RespValue::ok()
}

/// PFSELFTEST - stub returning OK
pub fn cmd_pfselftest(_ctx: &mut CommandContext) -> RespValue {
    RespValue::ok()
}

/// FAILOVER - stub returning OK
pub fn cmd_failover(_ctx: &mut CommandContext) -> RespValue {
    RespValue::ok()
}

/// SYNC - stub returning error (not supported)
pub fn cmd_sync(_ctx: &mut CommandContext) -> RespValue {
    RespValue::error("ERR SYNC not supported")
}

/// GEORADIUS key lng lat radius unit [options] - deprecated, wraps GEOSEARCH
pub fn cmd_georadius(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 6 {
        return RespValue::wrong_arity("georadius");
    }
    let mut new_args = vec![
        Bytes::from("GEOSEARCH"),
        ctx.args[1].clone(),
        Bytes::from("FROMLONLAT"),
        ctx.args[2].clone(),
        ctx.args[3].clone(),
        Bytes::from("BYRADIUS"),
        ctx.args[4].clone(),
        ctx.args[5].clone(),
    ];
    for arg in &ctx.args[6..] {
        new_args.push(arg.clone());
    }
    ctx.args = new_args;
    super::geo::cmd_geosearch(ctx)
}

/// GEORADIUSBYMEMBER key member radius unit [options] - deprecated, wraps GEOSEARCH
pub fn cmd_georadiusbymember(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 5 {
        return RespValue::wrong_arity("georadiusbymember");
    }
    let mut new_args = vec![
        Bytes::from("GEOSEARCH"),
        ctx.args[1].clone(),
        Bytes::from("FROMMEMBER"),
        ctx.args[2].clone(),
        Bytes::from("BYRADIUS"),
        ctx.args[3].clone(),
        ctx.args[4].clone(),
    ];
    for arg in &ctx.args[5..] {
        new_args.push(arg.clone());
    }
    ctx.args = new_args;
    super::geo::cmd_geosearch(ctx)
}
