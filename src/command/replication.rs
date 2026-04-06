use crate::protocol::RespValue;
use super::registry::CommandContext;

pub fn cmd_replicaof(_ctx: &mut CommandContext) -> RespValue {
    // Accept but no-op, return OK
    RespValue::ok()
}

pub fn cmd_replconf(_ctx: &mut CommandContext) -> RespValue {
    // Accept and return OK for compatibility
    RespValue::ok()
}

pub fn cmd_psync(_ctx: &mut CommandContext) -> RespValue {
    RespValue::error("ERR replication not supported")
}

pub fn cmd_wait(_ctx: &mut CommandContext) -> RespValue {
    // No replicas, return 0
    RespValue::integer(0)
}

/// ROLE - return replication role info
pub fn cmd_role(_ctx: &mut CommandContext) -> RespValue {
    RespValue::array(vec![
        RespValue::bulk_string(bytes::Bytes::from("master")),
        RespValue::integer(0),
        RespValue::array(vec![]),
    ])
}
