use bytes::Bytes;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::protocol::RespValue;
use crate::storage::RedisObject;
use super::registry::CommandContext;

// ============================================================================
// Rate Limiting Commands
// ============================================================================

// Stored as RedisObject::String with magic header "RLIM"
// Layout: magic(4) + type(1) + data...
//
// Type 0 = Sliding Window:
//   magic(4) + type(1) + max_requests(u64) + window_ms(u64) + count(u64) + timestamps...
//   Each timestamp is a u64 (ms since epoch)
//
// Type 1 = Token Bucket:
//   magic(4) + type(1) + rate(f64) + capacity(f64) + tokens(f64) + last_refill_ms(u64)

const RL_HEADER: &[u8] = b"RLIM";
const RL_HEADER_LEN: usize = 4;
const RL_TYPE_SLIDING: u8 = 0;
const RL_TYPE_BUCKET: u8 = 1;

// Sliding window layout offsets
const SW_META_LEN: usize = RL_HEADER_LEN + 1 + 8 + 8 + 8; // header + type + max_requests + window_ms + count

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ============================================================================
// Sliding Window helpers
// ============================================================================

fn new_sliding_window(max_requests: u64, window_ms: u64) -> Vec<u8> {
    let mut data = Vec::with_capacity(SW_META_LEN);
    data.extend_from_slice(RL_HEADER);
    data.push(RL_TYPE_SLIDING);
    data.extend_from_slice(&max_requests.to_le_bytes());
    data.extend_from_slice(&window_ms.to_le_bytes());
    data.extend_from_slice(&0u64.to_le_bytes()); // count = 0
    data
}

fn is_sliding_window(data: &[u8]) -> bool {
    data.len() >= SW_META_LEN
        && &data[0..RL_HEADER_LEN] == RL_HEADER
        && data[RL_HEADER_LEN] == RL_TYPE_SLIDING
}

fn sw_max_requests(data: &[u8]) -> u64 {
    u64::from_le_bytes(data[5..13].try_into().unwrap())
}

fn sw_window_ms(data: &[u8]) -> u64 {
    u64::from_le_bytes(data[13..21].try_into().unwrap())
}

fn sw_count(data: &[u8]) -> u64 {
    u64::from_le_bytes(data[21..29].try_into().unwrap())
}

fn set_sw_count(data: &mut [u8], count: u64) {
    data[21..29].copy_from_slice(&count.to_le_bytes());
}

fn set_sw_max_requests(data: &mut [u8], max_requests: u64) {
    data[5..13].copy_from_slice(&max_requests.to_le_bytes());
}

fn set_sw_window_ms(data: &mut [u8], window_ms: u64) {
    data[13..21].copy_from_slice(&window_ms.to_le_bytes());
}

fn sw_timestamps(data: &[u8]) -> Vec<u64> {
    let count = sw_count(data) as usize;
    let mut timestamps = Vec::with_capacity(count);
    for i in 0..count {
        let offset = SW_META_LEN + i * 8;
        if offset + 8 <= data.len() {
            timestamps.push(u64::from_le_bytes(
                data[offset..offset + 8].try_into().unwrap(),
            ));
        }
    }
    timestamps
}

/// Prune expired timestamps and return the cleaned data with updated count
fn sw_prune(data: &mut Vec<u8>, now: u64) {
    let window_ms = sw_window_ms(data);
    let cutoff = now.saturating_sub(window_ms);
    let timestamps = sw_timestamps(data);

    let valid: Vec<u64> = timestamps.into_iter().filter(|&ts| ts > cutoff).collect();

    // Rebuild data: keep header+meta, replace timestamps
    data.truncate(SW_META_LEN);
    set_sw_count(data, valid.len() as u64);
    for ts in &valid {
        data.extend_from_slice(&ts.to_le_bytes());
    }
}

fn sw_add_timestamp(data: &mut Vec<u8>, ts: u64) {
    let count = sw_count(data);
    set_sw_count(data, count + 1);
    data.extend_from_slice(&ts.to_le_bytes());
}

// ============================================================================
// Token Bucket helpers
// ============================================================================

const TB_META_LEN: usize = RL_HEADER_LEN + 1 + 8 + 8 + 8 + 8; // header + type + rate + capacity + tokens + last_refill_ms

fn new_token_bucket(rate: f64, capacity: f64) -> Vec<u8> {
    let now = now_ms();
    let mut data = Vec::with_capacity(TB_META_LEN);
    data.extend_from_slice(RL_HEADER);
    data.push(RL_TYPE_BUCKET);
    data.extend_from_slice(&rate.to_le_bytes());
    data.extend_from_slice(&capacity.to_le_bytes());
    data.extend_from_slice(&capacity.to_le_bytes()); // tokens = capacity (starts full)
    data.extend_from_slice(&now.to_le_bytes());
    data
}

fn is_token_bucket(data: &[u8]) -> bool {
    data.len() >= TB_META_LEN
        && &data[0..RL_HEADER_LEN] == RL_HEADER
        && data[RL_HEADER_LEN] == RL_TYPE_BUCKET
}

fn tb_rate(data: &[u8]) -> f64 {
    f64::from_le_bytes(data[5..13].try_into().unwrap())
}

fn tb_capacity(data: &[u8]) -> f64 {
    f64::from_le_bytes(data[13..21].try_into().unwrap())
}

fn tb_tokens(data: &[u8]) -> f64 {
    f64::from_le_bytes(data[21..29].try_into().unwrap())
}

fn set_tb_tokens(data: &mut [u8], tokens: f64) {
    data[21..29].copy_from_slice(&tokens.to_le_bytes());
}

fn tb_last_refill(data: &[u8]) -> u64 {
    u64::from_le_bytes(data[29..37].try_into().unwrap())
}

fn set_tb_last_refill(data: &mut [u8], ms: u64) {
    data[29..37].copy_from_slice(&ms.to_le_bytes());
}

fn tb_refill(data: &mut Vec<u8>) {
    let now = now_ms();
    let last = tb_last_refill(data);
    let elapsed_ms = now.saturating_sub(last);
    if elapsed_ms > 0 {
        let rate = tb_rate(data);
        let capacity = tb_capacity(data);
        let tokens = tb_tokens(data);
        let new_tokens = (tokens + rate * (elapsed_ms as f64 / 1000.0)).min(capacity);
        set_tb_tokens(data, new_tokens);
        set_tb_last_refill(data, now);
    }
}

fn is_ratelimit(data: &[u8]) -> bool {
    data.len() >= RL_HEADER_LEN + 1 && &data[0..RL_HEADER_LEN] == RL_HEADER
}

// ============================================================================
// Commands
// ============================================================================

/// RATELIMIT.CHECK key max_requests window_ms
/// Returns array: [allowed (0/1), current_count, remaining, retry_after_ms, reset_at_ms]
pub fn cmd_ratelimit_check(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();

    let max_requests: u64 = match std::str::from_utf8(&ctx.args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(v) if v > 0 => v,
        _ => return RespValue::error("ERR invalid max_requests"),
    };

    let window_ms: u64 = match std::str::from_utf8(&ctx.args[3])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(v) if v > 0 => v,
        _ => return RespValue::error("ERR invalid window_ms"),
    };

    let now = now_ms();

    // Get or create sliding window
    let mut data = {
        let db = ctx.db();
        match db.get(&key) {
            Some(RedisObject::String(b)) => {
                if is_sliding_window(b) {
                    b.to_vec()
                } else if b.is_empty() {
                    new_sliding_window(max_requests, window_ms)
                } else if is_ratelimit(b) {
                    return RespValue::error(
                        "WRONGTYPE Key holds a different rate limit type",
                    );
                } else {
                    return RespValue::error("WRONGTYPE Key is not a rate limit counter");
                }
            }
            Some(_) => return RespValue::wrong_type(),
            None => new_sliding_window(max_requests, window_ms),
        }
    };

    // Update params in case they changed
    set_sw_max_requests(&mut data, max_requests);
    set_sw_window_ms(&mut data, window_ms);

    // Prune expired timestamps
    sw_prune(&mut data, now);

    let current_count = sw_count(&data);

    if current_count < max_requests {
        // Allowed
        sw_add_timestamp(&mut data, now);
        let new_count = sw_count(&data);
        let remaining = max_requests.saturating_sub(new_count);

        // Reset at = earliest timestamp + window_ms
        let timestamps = sw_timestamps(&data);
        let reset_at = timestamps
            .iter()
            .min()
            .map(|&ts| ts + window_ms)
            .unwrap_or(now + window_ms);

        ctx.db()
            .set_keep_ttl(key, RedisObject::String(Bytes::from(data)));

        RespValue::Array(vec![
            RespValue::integer(1), // allowed
            RespValue::integer(new_count as i64),
            RespValue::integer(remaining as i64),
            RespValue::integer(0), // retry_after_ms (allowed, so 0)
            RespValue::integer(reset_at as i64),
        ])
    } else {
        // Not allowed
        let timestamps = sw_timestamps(&data);
        let earliest = timestamps.iter().min().copied().unwrap_or(now);
        let retry_after = (earliest + window_ms).saturating_sub(now);
        let reset_at = earliest + window_ms;

        ctx.db()
            .set_keep_ttl(key, RedisObject::String(Bytes::from(data)));

        RespValue::Array(vec![
            RespValue::integer(0), // not allowed
            RespValue::integer(current_count as i64),
            RespValue::integer(0), // remaining
            RespValue::integer(retry_after as i64),
            RespValue::integer(reset_at as i64),
        ])
    }
}

/// RATELIMIT.GET key
/// Returns array: [current_count, remaining, ttl_ms]
pub fn cmd_ratelimit_get(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let now = now_ms();

    let db = ctx.db();
    match db.get(&key) {
        Some(RedisObject::String(b)) => {
            if is_sliding_window(b) {
                let mut data = b.to_vec();
                sw_prune(&mut data, now);

                let current_count = sw_count(&data);
                let max_requests = sw_max_requests(&data);
                let window_ms = sw_window_ms(&data);
                let remaining = max_requests.saturating_sub(current_count);

                let timestamps = sw_timestamps(&data);
                let ttl = timestamps
                    .iter()
                    .max()
                    .map(|&ts| (ts + window_ms).saturating_sub(now))
                    .unwrap_or(0);

                RespValue::Array(vec![
                    RespValue::integer(current_count as i64),
                    RespValue::integer(remaining as i64),
                    RespValue::integer(ttl as i64),
                ])
            } else if is_token_bucket(b) {
                let mut data = b.to_vec();
                tb_refill(&mut data);

                let tokens = tb_tokens(&data);
                let capacity = tb_capacity(&data);

                RespValue::Array(vec![
                    RespValue::integer(tokens.floor() as i64),
                    RespValue::integer(capacity.floor() as i64),
                    RespValue::integer(0), // ttl doesn't apply for token bucket
                ])
            } else if is_ratelimit(b) {
                RespValue::error("WRONGTYPE Unknown rate limit type")
            } else {
                RespValue::error("WRONGTYPE Key is not a rate limit counter")
            }
        }
        Some(_) => RespValue::wrong_type(),
        None => {
            // Key doesn't exist, return zeros
            RespValue::Array(vec![
                RespValue::integer(0),
                RespValue::integer(0),
                RespValue::integer(0),
            ])
        }
    }
}

/// RATELIMIT.RESET key
/// Returns 1 if key existed, 0 otherwise
pub fn cmd_ratelimit_reset(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();

    let db = ctx.db();
    match db.remove(&key) {
        Some(_) => RespValue::integer(1),
        None => RespValue::integer(0),
    }
}

/// RATELIMIT.ACQUIRE key rate capacity
/// Token bucket: returns 1 if token acquired, 0 if bucket empty
pub fn cmd_ratelimit_acquire(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();

    let rate: f64 = match std::str::from_utf8(&ctx.args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(v) if v > 0.0 => v,
        _ => return RespValue::error("ERR invalid rate"),
    };

    let capacity: f64 = match std::str::from_utf8(&ctx.args[3])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(v) if v > 0.0 => v,
        _ => return RespValue::error("ERR invalid capacity"),
    };

    // Get or create token bucket
    let mut data = {
        let db = ctx.db();
        match db.get(&key) {
            Some(RedisObject::String(b)) => {
                if is_token_bucket(b) {
                    b.to_vec()
                } else if b.is_empty() {
                    new_token_bucket(rate, capacity)
                } else if is_ratelimit(b) {
                    return RespValue::error(
                        "WRONGTYPE Key holds a different rate limit type",
                    );
                } else {
                    return RespValue::error("WRONGTYPE Key is not a rate limit counter");
                }
            }
            Some(_) => return RespValue::wrong_type(),
            None => new_token_bucket(rate, capacity),
        }
    };

    // Refill tokens based on elapsed time
    tb_refill(&mut data);

    let tokens = tb_tokens(&data);
    if tokens >= 1.0 {
        set_tb_tokens(&mut data, tokens - 1.0);
        ctx.db()
            .set_keep_ttl(key, RedisObject::String(Bytes::from(data)));
        RespValue::integer(1)
    } else {
        ctx.db()
            .set_keep_ttl(key, RedisObject::String(Bytes::from(data)));
        RespValue::integer(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sliding_window_basic() {
        let mut data = new_sliding_window(5, 10000);
        assert!(is_sliding_window(&data));
        assert_eq!(sw_max_requests(&data), 5);
        assert_eq!(sw_window_ms(&data), 10000);
        assert_eq!(sw_count(&data), 0);

        // Add some timestamps
        let now = now_ms();
        sw_add_timestamp(&mut data, now);
        assert_eq!(sw_count(&data), 1);

        sw_add_timestamp(&mut data, now + 1);
        assert_eq!(sw_count(&data), 2);

        let timestamps = sw_timestamps(&data);
        assert_eq!(timestamps.len(), 2);
    }

    #[test]
    fn test_sliding_window_prune() {
        let mut data = new_sliding_window(5, 1000);
        let now = now_ms();

        // Add old timestamp (should be pruned) and new one
        sw_add_timestamp(&mut data, now.saturating_sub(2000));
        sw_add_timestamp(&mut data, now);
        assert_eq!(sw_count(&data), 2);

        sw_prune(&mut data, now);
        assert_eq!(sw_count(&data), 1);

        let timestamps = sw_timestamps(&data);
        assert_eq!(timestamps.len(), 1);
        assert_eq!(timestamps[0], now);
    }

    #[test]
    fn test_token_bucket_basic() {
        let data = new_token_bucket(10.0, 100.0);
        assert!(is_token_bucket(&data));
        assert_eq!(tb_rate(&data), 10.0);
        assert_eq!(tb_capacity(&data), 100.0);
        assert_eq!(tb_tokens(&data), 100.0); // starts full
    }

    #[test]
    fn test_token_bucket_consume() {
        let mut data = new_token_bucket(10.0, 5.0);
        assert_eq!(tb_tokens(&data), 5.0);

        // Consume tokens
        let tokens = tb_tokens(&data) - 1.0;
        set_tb_tokens(&mut data, tokens);
        assert_eq!(tb_tokens(&data), 4.0);

        let tokens = tb_tokens(&data) - 1.0;
        set_tb_tokens(&mut data, tokens);
        assert_eq!(tb_tokens(&data), 3.0);
    }

    #[test]
    fn test_ratelimit_type_detection() {
        let sw = new_sliding_window(10, 1000);
        assert!(is_sliding_window(&sw));
        assert!(!is_token_bucket(&sw));
        assert!(is_ratelimit(&sw));

        let tb = new_token_bucket(1.0, 10.0);
        assert!(is_token_bucket(&tb));
        assert!(!is_sliding_window(&tb));
        assert!(is_ratelimit(&tb));
    }
}
