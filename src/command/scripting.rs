use bytes::Bytes;
use crate::protocol::RespValue;
use super::registry::CommandContext;

pub fn cmd_eval(_ctx: &mut CommandContext) -> RespValue {
    RespValue::error("ERR scripting not enabled")
}

pub fn cmd_evalsha(_ctx: &mut CommandContext) -> RespValue {
    RespValue::error("ERR scripting not enabled")
}

pub fn cmd_script(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 2 {
        return RespValue::error("ERR wrong number of arguments for 'script' command");
    }

    let subcmd = String::from_utf8_lossy(&ctx.args[1]).to_uppercase();
    match subcmd.as_str() {
        "LOAD" => RespValue::error("ERR scripting not enabled"),
        "EXISTS" => {
            // Return array of 0s for each sha provided
            let count = ctx.args.len().saturating_sub(2);
            RespValue::array(
                (0..count).map(|_| RespValue::integer(0)).collect()
            )
        }
        "FLUSH" => RespValue::ok(),
        _ => RespValue::error(format!(
            "ERR unknown subcommand or wrong number of arguments for 'script|{}' command",
            subcmd.to_lowercase()
        )),
    }
}

pub fn cmd_function(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 2 {
        return RespValue::error("ERR wrong number of arguments for 'function' command");
    }

    let subcmd = String::from_utf8_lossy(&ctx.args[1]).to_uppercase();
    match subcmd.as_str() {
        "LOAD" => RespValue::error("ERR scripting not enabled"),
        "LIST" => RespValue::array(vec![]),
        "DELETE" => {
            if ctx.args.len() < 3 {
                return RespValue::error("ERR wrong number of arguments for 'function|delete' command");
            }
            RespValue::error("ERR no such library")
        }
        "DUMP" => RespValue::bulk_string(Bytes::new()),
        "RESTORE" => RespValue::error("ERR scripting not enabled"),
        "STATS" => RespValue::array(vec![
            RespValue::bulk_string(Bytes::from("running_script")),
            RespValue::integer(0),
            RespValue::bulk_string(Bytes::from("engines")),
            RespValue::array(vec![]),
        ]),
        "FLUSH" => RespValue::ok(),
        _ => RespValue::error(format!(
            "ERR unknown subcommand or wrong number of arguments for 'function|{}' command",
            subcmd.to_lowercase()
        )),
    }
}

pub fn cmd_fcall(_ctx: &mut CommandContext) -> RespValue {
    RespValue::error("ERR scripting not enabled")
}

pub fn cmd_fcall_ro(_ctx: &mut CommandContext) -> RespValue {
    RespValue::error("ERR scripting not enabled")
}
