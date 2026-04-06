use crate::protocol::RespValue;
use super::registry::CommandContext;

/// MULTI - handled in connection layer (sets in_multi flag)
pub fn cmd_multi(_ctx: &mut CommandContext) -> RespValue {
    // Actual logic is in connection.rs
    RespValue::ok()
}

/// EXEC - handled in connection layer (executes queued commands)
pub fn cmd_exec(_ctx: &mut CommandContext) -> RespValue {
    // Actual logic is in connection.rs
    RespValue::error("ERR EXEC without MULTI")
}

/// DISCARD - handled in connection layer (clears queue)
pub fn cmd_discard(_ctx: &mut CommandContext) -> RespValue {
    // Actual logic is in connection.rs
    RespValue::error("ERR DISCARD without MULTI")
}

/// WATCH - handled in connection layer
pub fn cmd_watch(_ctx: &mut CommandContext) -> RespValue {
    // Actual logic is in connection.rs
    RespValue::ok()
}

/// UNWATCH - handled in connection layer
pub fn cmd_unwatch(_ctx: &mut CommandContext) -> RespValue {
    // Actual logic is in connection.rs
    RespValue::ok()
}
