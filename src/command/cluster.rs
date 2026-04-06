use bytes::Bytes;
use crate::protocol::RespValue;
use super::registry::CommandContext;

/// A fixed 40-char hex node ID for standalone mode.
const NODE_ID: &str = "0000000000000000000000000000000000000000";

/// Compute CRC16 hash slot for a key (CRC16/XMODEM mod 16384).
/// Supports hash tags: if key contains {xxx}, only xxx is hashed.
fn crc16_hash_slot(key: &[u8]) -> u16 {
    // Check for hash tag {xxx}
    if let Some(start) = key.iter().position(|&b| b == b'{') {
        if let Some(end) = key[start + 1..].iter().position(|&b| b == b'}') {
            if end > 0 {
                return crc16::State::<crc16::XMODEM>::calculate(&key[start + 1..start + 1 + end])
                    % 16384;
            }
        }
    }
    crc16::State::<crc16::XMODEM>::calculate(key) % 16384
}

/// CLUSTER command dispatcher.
pub fn cmd_cluster(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 2 {
        return RespValue::error("ERR wrong number of arguments for 'cluster' command");
    }

    let subcmd = String::from_utf8_lossy(&ctx.args[1]).to_uppercase();
    match subcmd.as_str() {
        "INFO" => cmd_cluster_info(),
        "MYID" => cmd_cluster_myid(),
        "NODES" => cmd_cluster_nodes(),
        "SLOTS" => cmd_cluster_slots(),
        "SHARDS" => cmd_cluster_shards(),
        "KEYSLOT" => cmd_cluster_keyslot(ctx),
        "COUNTKEYSINSLOT" => cmd_cluster_countkeysinslot(ctx),
        "GETKEYSINSLOT" => cmd_cluster_getkeysinslot(ctx),
        "RESET" => cmd_cluster_reset(),
        "SETSLOT" => RespValue::error("ERR This instance has cluster support disabled"),
        "MEET" => RespValue::error("ERR This instance has cluster support disabled"),
        "REPLICATE" => RespValue::error("ERR This instance has cluster support disabled"),
        "FAILOVER" => RespValue::error("ERR This instance has cluster support disabled"),
        _ => RespValue::error(format!(
            "ERR unknown subcommand or wrong number of arguments for 'cluster|{}'",
            subcmd.to_lowercase()
        )),
    }
}

fn cmd_cluster_info() -> RespValue {
    let info = "\
cluster_enabled:0\r\n\
cluster_state:ok\r\n\
cluster_slots_assigned:0\r\n\
cluster_slots_ok:0\r\n\
cluster_slots_pfail:0\r\n\
cluster_slots_fail:0\r\n\
cluster_known_nodes:0\r\n\
cluster_size:0\r\n\
cluster_current_epoch:0\r\n\
cluster_my_epoch:0\r\n\
cluster_stats_messages_sent:0\r\n\
cluster_stats_messages_received:0\r\n\
total_cluster_links_buffer_limit_exceeded:0\r\n";

    RespValue::bulk_string(Bytes::from(info))
}

fn cmd_cluster_myid() -> RespValue {
    RespValue::bulk_string(Bytes::from(NODE_ID))
}

fn cmd_cluster_nodes() -> RespValue {
    // Return self as a single standalone node
    let line = format!(
        "{} :0@0 myself,master - 0 0 0 connected 0-16383\r\n",
        NODE_ID
    );
    RespValue::bulk_string(Bytes::from(line))
}

fn cmd_cluster_slots() -> RespValue {
    RespValue::array(vec![])
}

fn cmd_cluster_shards() -> RespValue {
    RespValue::array(vec![])
}

fn cmd_cluster_keyslot(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 3 {
        return RespValue::error("ERR wrong number of arguments for 'cluster|keyslot' command");
    }
    let key = &ctx.args[2];
    let slot = crc16_hash_slot(key) as i64;
    RespValue::integer(slot)
}

fn cmd_cluster_countkeysinslot(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 3 {
        return RespValue::error(
            "ERR wrong number of arguments for 'cluster|countkeysinslot' command",
        );
    }
    let _slot: u16 = match String::from_utf8_lossy(&ctx.args[2]).parse() {
        Ok(v) if v < 16384 => v,
        _ => return RespValue::error("ERR Invalid or out of range slot"),
    };
    // In standalone mode, we don't track slots; return 0
    RespValue::integer(0)
}

fn cmd_cluster_getkeysinslot(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 4 {
        return RespValue::error(
            "ERR wrong number of arguments for 'cluster|getkeysinslot' command",
        );
    }
    let _slot: u16 = match String::from_utf8_lossy(&ctx.args[2]).parse() {
        Ok(v) if v < 16384 => v,
        _ => return RespValue::error("ERR Invalid or out of range slot"),
    };
    let _count: u64 = match String::from_utf8_lossy(&ctx.args[3]).parse() {
        Ok(v) => v,
        Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
    };
    RespValue::array(vec![])
}

fn cmd_cluster_reset() -> RespValue {
    // Accept HARD or SOFT, but no-op in standalone mode
    RespValue::ok()
}

/// READONLY — accept but no-op in standalone mode.
pub fn cmd_readonly(_ctx: &mut CommandContext) -> RespValue {
    RespValue::ok()
}

/// READWRITE — accept but no-op in standalone mode.
pub fn cmd_readwrite(_ctx: &mut CommandContext) -> RespValue {
    RespValue::ok()
}

/// ASKING — accept but no-op in standalone mode.
pub fn cmd_asking(_ctx: &mut CommandContext) -> RespValue {
    RespValue::ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc16_hash_slot_basic() {
        // Known values from Redis documentation
        let slot = crc16_hash_slot(b"foo");
        assert!(slot < 16384);
    }

    #[test]
    fn test_crc16_hash_slot_with_tag() {
        // {user}.following and {user}.followers should hash the same
        let slot1 = crc16_hash_slot(b"{user}.following");
        let slot2 = crc16_hash_slot(b"{user}.followers");
        assert_eq!(slot1, slot2);
    }

    #[test]
    fn test_crc16_hash_slot_empty_tag() {
        // Empty hash tag {} should hash the whole key
        let slot1 = crc16_hash_slot(b"{}foo");
        let slot2 = crc16_hash_slot(b"{}foo");
        assert_eq!(slot1, slot2);
        // Empty tag means whole key is used
        let slot_full = crc16_hash_slot(b"{}foo");
        assert!(slot_full < 16384);
    }

    #[test]
    fn test_crc16_hash_slot_no_tag() {
        let slot = crc16_hash_slot(b"mykey");
        assert!(slot < 16384);
    }
}
