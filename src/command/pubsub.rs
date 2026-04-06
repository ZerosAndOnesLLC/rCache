use crate::protocol::RespValue;
use super::registry::CommandContext;

/// PUBLISH command: publish a message to a channel.
/// The actual fan-out is handled by the connection layer via PubSubManager.
/// This handler returns the number of subscribers that received the message,
/// but since we can't access PubSubManager from CommandContext, we store
/// the publish request and let the connection layer handle it.
/// For now, we return 0 and let the connection handle the actual publish.
pub fn cmd_publish(_ctx: &mut CommandContext) -> RespValue {
    // This is a placeholder - actual publishing is handled in connection.rs
    // which has access to the PubSubManager. The connection layer intercepts
    // PUBLISH commands before they reach here.
    RespValue::integer(0)
}

/// PUBSUB subcommand dispatcher
pub fn cmd_pubsub(_ctx: &mut CommandContext) -> RespValue {
    // Handled in connection layer which has access to PubSubManager
    RespValue::array(vec![])
}

/// SUBSCRIBE - handled in connection layer
pub fn cmd_subscribe(_ctx: &mut CommandContext) -> RespValue {
    RespValue::ok()
}

/// UNSUBSCRIBE - handled in connection layer
pub fn cmd_unsubscribe(_ctx: &mut CommandContext) -> RespValue {
    RespValue::ok()
}

/// PSUBSCRIBE - handled in connection layer
pub fn cmd_psubscribe(_ctx: &mut CommandContext) -> RespValue {
    RespValue::ok()
}

/// PUNSUBSCRIBE - handled in connection layer
pub fn cmd_punsubscribe(_ctx: &mut CommandContext) -> RespValue {
    RespValue::ok()
}
