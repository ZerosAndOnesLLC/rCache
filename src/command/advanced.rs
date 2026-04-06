use bytes::Bytes;
use crate::protocol::RespValue;
use crate::storage::RedisObject;
use super::registry::CommandContext;

// ============================================================
// MODULE commands — stubs
// ============================================================

/// MODULE command dispatcher.
pub fn cmd_module(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 2 {
        return RespValue::error("ERR wrong number of arguments for 'module' command");
    }
    let subcmd = String::from_utf8_lossy(&ctx.args[1]).to_uppercase();
    match subcmd.as_str() {
        "LIST" => RespValue::array(vec![]),
        "LOAD" | "LOADEX" => {
            RespValue::error("ERR module system not supported")
        }
        "UNLOAD" => {
            RespValue::error("ERR module system not supported")
        }
        _ => RespValue::error(format!(
            "ERR unknown subcommand or wrong number of arguments for 'module|{}'",
            subcmd.to_lowercase()
        )),
    }
}

// ============================================================
// LATENCY commands — stubs
// ============================================================

/// LATENCY command dispatcher.
pub fn cmd_latency(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 2 {
        return RespValue::error("ERR wrong number of arguments for 'latency' command");
    }
    let subcmd = String::from_utf8_lossy(&ctx.args[1]).to_uppercase();
    match subcmd.as_str() {
        "LATEST" => RespValue::array(vec![]),
        "HISTORY" => RespValue::array(vec![]),
        "RESET" => RespValue::array(vec![]),
        "GRAPH" => RespValue::bulk_string(Bytes::new()),
        "HELP" => RespValue::array(vec![
            RespValue::simple_string("LATENCY subcommand [arguments]"),
            RespValue::simple_string("LATEST - Return the latest latency samples for all events."),
            RespValue::simple_string("HISTORY <event> - Return latency time series for <event>."),
            RespValue::simple_string("RESET [<event> ...] - Reset latency data."),
            RespValue::simple_string("HELP - Return this help message."),
        ]),
        _ => RespValue::error(format!(
            "ERR unknown subcommand or wrong number of arguments for 'latency|{}'",
            subcmd.to_lowercase()
        )),
    }
}

// ============================================================
// Per-field hash expiration — stubs (Phase 11.5)
// ============================================================

/// HEXPIRE — stub, returns error not supported.
pub fn cmd_hexpire(_ctx: &mut CommandContext) -> RespValue {
    RespValue::error("ERR not supported")
}

/// HPEXPIRE — stub, returns error not supported.
pub fn cmd_hpexpire(_ctx: &mut CommandContext) -> RespValue {
    RespValue::error("ERR not supported")
}

/// HEXPIREAT — stub, returns error not supported.
pub fn cmd_hexpireat(_ctx: &mut CommandContext) -> RespValue {
    RespValue::error("ERR not supported")
}

/// HPEXPIREAT — stub, returns error not supported.
pub fn cmd_hpexpireat(_ctx: &mut CommandContext) -> RespValue {
    RespValue::error("ERR not supported")
}

/// HPERSIST — returns -1 (no expiry).
pub fn cmd_hpersist(_ctx: &mut CommandContext) -> RespValue {
    RespValue::integer(-1)
}

/// HTTL — returns -1 (no expiry).
pub fn cmd_httl(_ctx: &mut CommandContext) -> RespValue {
    RespValue::integer(-1)
}

/// HPTTL — returns -1 (no expiry).
pub fn cmd_hpttl(_ctx: &mut CommandContext) -> RespValue {
    RespValue::integer(-1)
}

/// HEXPIRETIME — returns -1 (no expiry).
pub fn cmd_hexpiretime(_ctx: &mut CommandContext) -> RespValue {
    RespValue::integer(-1)
}

/// HPEXPIRETIME — returns -1 (no expiry).
pub fn cmd_hpexpiretime(_ctx: &mut CommandContext) -> RespValue {
    RespValue::integer(-1)
}

// ============================================================
// CLIENT subcommand extensions (Phase 11.2)
// These are handled inline in server_cmds.rs cmd_client
// ============================================================

// ============================================================
// CONFIG subcommand extension for keyspace-notifications
// Handled inline in the CONFIG handler
// ============================================================

// ============================================================
// LCS — Longest Common Substring (Phase 11.6)
// ============================================================

/// LCS key1 key2 [LEN] [IDX] [MINMATCHLEN len] [WITHMATCHLEN]
pub fn cmd_lcs(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 3 {
        return RespValue::wrong_arity("lcs");
    }

    let key1 = ctx.args[1].clone();
    let key2 = ctx.args[2].clone();

    // Parse options
    let mut len_only = false;
    let mut idx_mode = false;
    let mut min_match_len: usize = 0;
    let mut with_match_len = false;

    let mut i = 3;
    while i < ctx.args.len() {
        let opt = String::from_utf8_lossy(&ctx.args[i]).to_uppercase();
        match opt.as_str() {
            "LEN" => len_only = true,
            "IDX" => idx_mode = true,
            "MINMATCHLEN" => {
                i += 1;
                if i >= ctx.args.len() {
                    return RespValue::error("ERR syntax error");
                }
                min_match_len = match String::from_utf8_lossy(&ctx.args[i]).parse() {
                    Ok(v) => v,
                    Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
                };
            }
            "WITHMATCHLEN" => with_match_len = true,
            _ => return RespValue::error("ERR syntax error"),
        }
        i += 1;
    }

    // Get the two string values
    let s1 = {
        let db = ctx.db();
        match db.get(&key1) {
            Some(RedisObject::String(b)) => b.clone(),
            Some(_) => return RespValue::wrong_type(),
            None => Bytes::new(),
        }
    };

    let s2 = {
        let db = ctx.db();
        match db.get(&key2) {
            Some(RedisObject::String(b)) => b.clone(),
            Some(_) => return RespValue::wrong_type(),
            None => Bytes::new(),
        }
    };

    let a = &s1[..];
    let b = &s2[..];
    let m = a.len();
    let n = b.len();

    // Build LCS DP table
    // dp[i][j] = length of LCS of a[0..i] and b[0..j]
    let mut dp = vec![vec![0u32; n + 1]; m + 1];
    for i in 1..=m {
        for j in 1..=n {
            if a[i - 1] == b[j - 1] {
                dp[i][j] = dp[i - 1][j - 1] + 1;
            } else {
                dp[i][j] = dp[i - 1][j].max(dp[i][j - 1]);
            }
        }
    }

    let lcs_len = dp[m][n] as i64;

    if len_only {
        return RespValue::integer(lcs_len);
    }

    if idx_mode {
        // Backtrack to find all match ranges
        let matches = lcs_matches(a, b, &dp, min_match_len);
        let total_len = matches.iter().map(|m| m.len).sum::<usize>() as i64;

        let mut match_array = Vec::new();
        for mat in &matches {
            let mut entry = vec![
                RespValue::array(vec![
                    RespValue::integer(mat.a_start as i64),
                    RespValue::integer(mat.a_end as i64),
                ]),
                RespValue::array(vec![
                    RespValue::integer(mat.b_start as i64),
                    RespValue::integer(mat.b_end as i64),
                ]),
            ];
            if with_match_len {
                entry.push(RespValue::integer(mat.len as i64));
            }
            match_array.push(RespValue::array(entry));
        }

        return RespValue::array(vec![
            RespValue::bulk_string(Bytes::from("matches")),
            RespValue::array(match_array),
            RespValue::bulk_string(Bytes::from("len")),
            RespValue::integer(total_len),
        ]);
    }

    // Default: return the LCS string itself
    let lcs_string = backtrack_lcs(a, b, &dp);
    RespValue::bulk_string(Bytes::from(lcs_string))
}

struct LcsMatch {
    a_start: usize,
    a_end: usize,
    b_start: usize,
    b_end: usize,
    len: usize,
}

/// Backtrack the DP table to reconstruct the LCS string.
fn backtrack_lcs(a: &[u8], b: &[u8], dp: &[Vec<u32>]) -> Vec<u8> {
    let mut result = Vec::new();
    let mut i = a.len();
    let mut j = b.len();
    while i > 0 && j > 0 {
        if a[i - 1] == b[j - 1] {
            result.push(a[i - 1]);
            i -= 1;
            j -= 1;
        } else if dp[i - 1][j] > dp[i][j - 1] {
            i -= 1;
        } else {
            j -= 1;
        }
    }
    result.reverse();
    result
}

/// Backtrack the DP table to find matching index ranges.
fn lcs_matches(a: &[u8], b: &[u8], dp: &[Vec<u32>], min_match_len: usize) -> Vec<LcsMatch> {
    // First reconstruct the LCS with positions
    let mut positions: Vec<(usize, usize)> = Vec::new(); // (a_idx, b_idx)
    let mut i = a.len();
    let mut j = b.len();
    while i > 0 && j > 0 {
        if a[i - 1] == b[j - 1] {
            positions.push((i - 1, j - 1));
            i -= 1;
            j -= 1;
        } else if dp[i - 1][j] > dp[i][j - 1] {
            i -= 1;
        } else {
            j -= 1;
        }
    }
    positions.reverse();

    // Group consecutive positions into match ranges
    let mut matches = Vec::new();
    if positions.is_empty() {
        return matches;
    }

    let mut start = 0;
    for k in 1..=positions.len() {
        let is_end = if k == positions.len() {
            true
        } else {
            // Check if positions are consecutive in both a and b
            positions[k].0 != positions[k - 1].0 + 1
                || positions[k].1 != positions[k - 1].1 + 1
        };

        if is_end {
            let len = k - start;
            if len >= min_match_len {
                matches.push(LcsMatch {
                    a_start: positions[start].0,
                    a_end: positions[k - 1].0,
                    b_start: positions[start].1,
                    b_end: positions[k - 1].1,
                    len,
                });
            }
            if k < positions.len() {
                start = k;
            }
        }
    }

    matches
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backtrack_lcs() {
        let a = b"ohmytext";
        let b_str = b"mynewtext";
        let m = a.len();
        let n = b_str.len();
        let mut dp = vec![vec![0u32; n + 1]; m + 1];
        for i in 1..=m {
            for j in 1..=n {
                if a[i - 1] == b_str[j - 1] {
                    dp[i][j] = dp[i - 1][j - 1] + 1;
                } else {
                    dp[i][j] = dp[i - 1][j].max(dp[i][j - 1]);
                }
            }
        }
        let lcs = backtrack_lcs(a, b_str, &dp);
        assert_eq!(lcs, b"mytext");
    }
}
