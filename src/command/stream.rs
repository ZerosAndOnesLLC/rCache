use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::protocol::RespValue;
use crate::storage::types::{
    ConsumerData, ConsumerGroup, PendingEntry, RedisObject, StreamData, StreamId,
};
use super::registry::CommandContext;

// === Helper functions ===

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn parse_stream_id(s: &str) -> Result<StreamId, RespValue> {
    if s == "-" {
        return Ok(StreamId::min());
    }
    if s == "+" {
        return Ok(StreamId::max());
    }

    let parts: Vec<&str> = s.splitn(2, '-').collect();
    let ms: u64 = parts[0]
        .parse()
        .map_err(|_| RespValue::error("ERR Invalid stream ID specified as stream command argument"))?;
    let seq: u64 = if parts.len() > 1 {
        parts[1]
            .parse()
            .map_err(|_| RespValue::error("ERR Invalid stream ID specified as stream command argument"))?
    } else {
        0
    };

    Ok(StreamId { ms, seq })
}

fn parse_stream_id_for_range_start(s: &str) -> Result<StreamId, RespValue> {
    if s == "-" {
        return Ok(StreamId::min());
    }
    if s == "+" {
        return Ok(StreamId::max());
    }
    let parts: Vec<&str> = s.splitn(2, '-').collect();
    let ms: u64 = parts[0]
        .parse()
        .map_err(|_| RespValue::error("ERR Invalid stream ID specified as stream command argument"))?;
    let seq: u64 = if parts.len() > 1 {
        parts[1]
            .parse()
            .map_err(|_| RespValue::error("ERR Invalid stream ID specified as stream command argument"))?
    } else {
        0
    };
    Ok(StreamId { ms, seq })
}

fn parse_stream_id_for_range_end(s: &str) -> Result<StreamId, RespValue> {
    if s == "+" {
        return Ok(StreamId::max());
    }
    if s == "-" {
        return Ok(StreamId::min());
    }
    let parts: Vec<&str> = s.splitn(2, '-').collect();
    let ms: u64 = parts[0]
        .parse()
        .map_err(|_| RespValue::error("ERR Invalid stream ID specified as stream command argument"))?;
    let seq: u64 = if parts.len() > 1 {
        parts[1]
            .parse()
            .map_err(|_| RespValue::error("ERR Invalid stream ID specified as stream command argument"))?
    } else {
        u64::MAX
    };
    Ok(StreamId { ms, seq })
}

fn generate_id(stream: &StreamData) -> StreamId {
    let ms = current_time_ms();
    let seq = if ms == stream.last_id.ms {
        stream.last_id.seq + 1
    } else if ms > stream.last_id.ms {
        0
    } else {
        // Clock went backwards, use last_id.ms
        stream.last_id.seq + 1
    };
    let actual_ms = if ms >= stream.last_id.ms {
        ms
    } else {
        stream.last_id.ms
    };
    StreamId { ms: actual_ms, seq }
}

fn validate_new_id(stream: &StreamData, id: &StreamId) -> Result<(), RespValue> {
    if *id <= stream.last_id {
        return Err(RespValue::error(
            "ERR The ID specified in XADD is equal or smaller than the target stream top item",
        ));
    }
    // ID 0-0 is not valid when the stream is non-empty
    if id.ms == 0 && id.seq == 0 {
        return Err(RespValue::error(
            "ERR The ID specified in XADD must be greater than 0-0",
        ));
    }
    Ok(())
}

fn format_entry(id: &StreamId, fields: &[(Bytes, Bytes)]) -> RespValue {
    let mut field_values: Vec<RespValue> = Vec::with_capacity(fields.len() * 2);
    for (k, v) in fields {
        field_values.push(RespValue::bulk_string(k.clone()));
        field_values.push(RespValue::bulk_string(v.clone()));
    }
    RespValue::array(vec![
        RespValue::bulk_string(Bytes::from(id.to_string())),
        RespValue::array(field_values),
    ])
}

fn trim_stream_maxlen(stream: &mut StreamData, maxlen: usize, approximate: bool) {
    if approximate {
        // Approximate: only trim if significantly over
        let threshold = maxlen + (maxlen / 10).max(10);
        if stream.entries.len() <= threshold {
            return;
        }
    }
    while stream.entries.len() > maxlen {
        stream.entries.pop_first();
    }
}

fn trim_stream_minid(stream: &mut StreamData, minid: &StreamId, approximate: bool) {
    if approximate {
        // Approximate: be less aggressive
        let count_below: usize = stream
            .entries
            .range(..minid)
            .count();
        if count_below < 10 {
            return;
        }
    }
    let to_remove: Vec<StreamId> = stream
        .entries
        .range(..minid)
        .map(|(k, _)| k.clone())
        .collect();
    for id in to_remove {
        stream.entries.remove(&id);
    }
}

fn parse_trim_args(
    args: &[Bytes],
    start: usize,
) -> Result<Option<TrimConfig>, RespValue> {
    if start >= args.len() {
        return Ok(None);
    }
    let strategy = String::from_utf8_lossy(&args[start]).to_uppercase();
    let mut idx = start + 1;
    let approximate = if idx < args.len() {
        let s = String::from_utf8_lossy(&args[idx]);
        if s == "~" {
            idx += 1;
            true
        } else if s == "=" {
            idx += 1;
            false
        } else {
            false
        }
    } else {
        false
    };

    if idx >= args.len() {
        return Err(RespValue::error("ERR syntax error"));
    }

    let threshold_str = String::from_utf8_lossy(&args[idx]);

    match strategy.as_str() {
        "MAXLEN" => {
            let maxlen: usize = threshold_str
                .parse()
                .map_err(|_| RespValue::error("ERR value is not an integer or out of range"))?;
            idx += 1;
            // Parse optional LIMIT
            let _limit = parse_limit_arg(args, &mut idx)?;
            Ok(Some(TrimConfig::MaxLen {
                maxlen,
                approximate,
            }))
        }
        "MINID" => {
            let minid = parse_stream_id(&threshold_str)?;
            idx += 1;
            let _limit = parse_limit_arg(args, &mut idx)?;
            Ok(Some(TrimConfig::MinId {
                minid,
                approximate,
            }))
        }
        _ => Err(RespValue::error("ERR syntax error")),
    }
}

fn parse_limit_arg(args: &[Bytes], idx: &mut usize) -> Result<Option<usize>, RespValue> {
    if *idx < args.len() {
        let s = String::from_utf8_lossy(&args[*idx]).to_uppercase();
        if s == "LIMIT" {
            *idx += 1;
            if *idx >= args.len() {
                return Err(RespValue::error("ERR syntax error"));
            }
            let limit: usize = String::from_utf8_lossy(&args[*idx])
                .parse()
                .map_err(|_| RespValue::error("ERR value is not an integer or out of range"))?;
            *idx += 1;
            return Ok(Some(limit));
        }
    }
    Ok(None)
}

enum TrimConfig {
    MaxLen { maxlen: usize, approximate: bool },
    MinId { minid: StreamId, approximate: bool },
}

fn apply_trim(stream: &mut StreamData, trim: &TrimConfig) -> usize {
    let before = stream.entries.len();
    match trim {
        TrimConfig::MaxLen { maxlen, approximate } => {
            trim_stream_maxlen(stream, *maxlen, *approximate);
        }
        TrimConfig::MinId { minid, approximate } => {
            trim_stream_minid(stream, minid, *approximate);
        }
    }
    before - stream.entries.len()
}

// === XADD ===
pub fn cmd_xadd(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 4 {
        return RespValue::wrong_arity("xadd");
    }

    let key = ctx.args[1].clone();
    let mut idx = 2;
    let mut nomkstream = false;
    let mut trim_config: Option<TrimConfig> = None;

    // Parse options before the ID
    loop {
        if idx >= ctx.args.len() {
            return RespValue::error("ERR syntax error");
        }
        let arg = String::from_utf8_lossy(&ctx.args[idx]).to_uppercase();
        match arg.as_str() {
            "NOMKSTREAM" => {
                nomkstream = true;
                idx += 1;
            }
            "MAXLEN" | "MINID" => {
                let args_clone: Vec<Bytes> = ctx.args.iter().cloned().collect();
                match parse_trim_args(&args_clone, idx) {
                    Ok(Some(tc)) => {
                        trim_config = Some(tc);
                        // Advance idx past the trim args
                        idx += 1; // strategy
                        if idx < ctx.args.len() {
                            let s = String::from_utf8_lossy(&ctx.args[idx]);
                            if s == "~" || s == "=" {
                                idx += 1;
                            }
                        }
                        idx += 1; // threshold
                        if idx < ctx.args.len() {
                            let s = String::from_utf8_lossy(&ctx.args[idx]).to_uppercase();
                            if s == "LIMIT" {
                                idx += 2; // LIMIT + count
                            }
                        }
                    }
                    Ok(None) => break,
                    Err(e) => return e,
                }
            }
            _ => break,
        }
    }

    if idx >= ctx.args.len() {
        return RespValue::error("ERR syntax error");
    }

    // Parse ID
    let id_str = String::from_utf8_lossy(&ctx.args[idx]).to_string();
    idx += 1;

    // Remaining args are field-value pairs
    let remaining = ctx.args.len() - idx;
    if remaining == 0 || remaining % 2 != 0 {
        return RespValue::error("ERR wrong number of arguments for 'xadd' command");
    }

    let mut fields: Vec<(Bytes, Bytes)> = Vec::with_capacity(remaining / 2);
    while idx + 1 < ctx.args.len() {
        let field = ctx.args[idx].clone();
        let value = ctx.args[idx + 1].clone();
        fields.push((field, value));
        idx += 2;
    }

    let db = ctx.db();

    // Check if key exists and is wrong type
    if let Some(obj) = db.get(&key) {
        if !matches!(obj, RedisObject::Stream(_)) {
            return RespValue::wrong_type();
        }
    }

    // Check NOMKSTREAM: if key doesn't exist, return Null
    let exists = db.exists(&key);
    if nomkstream && !exists {
        return RespValue::Null;
    }

    // Get or create stream
    if !exists {
        db.set(key.clone(), RedisObject::Stream(StreamData::new()));
    }

    let stream = match db.get_mut(&key) {
        Some(RedisObject::Stream(s)) => s,
        _ => return RespValue::wrong_type(),
    };

    // Generate or parse ID
    let new_id = if id_str == "*" {
        generate_id(stream)
    } else {
        // Parse explicit ID, handle partial IDs
        let parts: Vec<&str> = id_str.splitn(2, '-').collect();
        let ms: u64 = match parts[0].parse() {
            Ok(v) => v,
            Err(_) => {
                return RespValue::error(
                    "ERR Invalid stream ID specified as stream command argument",
                )
            }
        };
        let seq: u64 = if parts.len() > 1 {
            if parts[1] == "*" {
                // Auto-sequence for given ms
                if ms == stream.last_id.ms {
                    stream.last_id.seq + 1
                } else if ms > stream.last_id.ms {
                    0
                } else {
                    stream.last_id.seq + 1
                }
            } else {
                match parts[1].parse() {
                    Ok(v) => v,
                    Err(_) => {
                        return RespValue::error(
                            "ERR Invalid stream ID specified as stream command argument",
                        )
                    }
                }
            }
        } else {
            // No seq specified, auto-assign
            if ms == stream.last_id.ms {
                stream.last_id.seq + 1
            } else {
                0
            }
        };

        let id = StreamId { ms, seq };
        if let Err(e) = validate_new_id(stream, &id) {
            return e;
        }
        id
    };

    let id_string = new_id.to_string();
    stream.last_id = new_id.clone();
    stream.entries.insert(new_id, fields);

    // Apply trim if configured
    if let Some(ref tc) = trim_config {
        apply_trim(stream, tc);
    }

    RespValue::bulk_string(Bytes::from(id_string))
}

// === XLEN ===
pub fn cmd_xlen(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let db = ctx.db();

    match db.get(&key) {
        Some(RedisObject::Stream(s)) => RespValue::integer(s.len() as i64),
        Some(_) => RespValue::wrong_type(),
        None => RespValue::integer(0),
    }
}

// === XRANGE ===
pub fn cmd_xrange(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 4 {
        return RespValue::wrong_arity("xrange");
    }

    let key = ctx.args[1].clone();
    let start_str = String::from_utf8_lossy(&ctx.args[2]).to_string();
    let end_str = String::from_utf8_lossy(&ctx.args[3]).to_string();

    let start = match parse_stream_id_for_range_start(&start_str) {
        Ok(id) => id,
        Err(e) => return e,
    };
    let end = match parse_stream_id_for_range_end(&end_str) {
        Ok(id) => id,
        Err(e) => return e,
    };

    let mut count: Option<usize> = None;
    if ctx.args.len() >= 6 {
        let opt = String::from_utf8_lossy(&ctx.args[4]).to_uppercase();
        if opt == "COUNT" {
            count = Some(
                match String::from_utf8_lossy(&ctx.args[5]).parse() {
                    Ok(v) => v,
                    Err(_) => {
                        return RespValue::error("ERR value is not an integer or out of range")
                    }
                },
            );
        }
    }

    let db = ctx.db();
    match db.get(&key) {
        Some(RedisObject::Stream(s)) => {
            let mut results: Vec<RespValue> = Vec::new();
            for (id, fields) in s.entries.range(&start..=&end) {
                results.push(format_entry(id, fields));
                if let Some(c) = count {
                    if results.len() >= c {
                        break;
                    }
                }
            }
            RespValue::array(results)
        }
        Some(_) => RespValue::wrong_type(),
        None => RespValue::array(vec![]),
    }
}

// === XREVRANGE ===
pub fn cmd_xrevrange(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 4 {
        return RespValue::wrong_arity("xrevrange");
    }

    let key = ctx.args[1].clone();
    // Note: XREVRANGE has end first, then start
    let end_str = String::from_utf8_lossy(&ctx.args[2]).to_string();
    let start_str = String::from_utf8_lossy(&ctx.args[3]).to_string();

    let start = match parse_stream_id_for_range_start(&start_str) {
        Ok(id) => id,
        Err(e) => return e,
    };
    let end = match parse_stream_id_for_range_end(&end_str) {
        Ok(id) => id,
        Err(e) => return e,
    };

    let mut count: Option<usize> = None;
    if ctx.args.len() >= 6 {
        let opt = String::from_utf8_lossy(&ctx.args[4]).to_uppercase();
        if opt == "COUNT" {
            count = Some(
                match String::from_utf8_lossy(&ctx.args[5]).parse() {
                    Ok(v) => v,
                    Err(_) => {
                        return RespValue::error("ERR value is not an integer or out of range")
                    }
                },
            );
        }
    }

    let db = ctx.db();
    match db.get(&key) {
        Some(RedisObject::Stream(s)) => {
            let mut results: Vec<RespValue> = Vec::new();
            for (id, fields) in s.entries.range(&start..=&end).rev() {
                results.push(format_entry(id, fields));
                if let Some(c) = count {
                    if results.len() >= c {
                        break;
                    }
                }
            }
            RespValue::array(results)
        }
        Some(_) => RespValue::wrong_type(),
        None => RespValue::array(vec![]),
    }
}

// === XDEL ===
pub fn cmd_xdel(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 3 {
        return RespValue::wrong_arity("xdel");
    }

    let key = ctx.args[1].clone();

    // Parse all IDs first
    let mut ids: Vec<StreamId> = Vec::new();
    for i in 2..ctx.args.len() {
        let id_str = String::from_utf8_lossy(&ctx.args[i]).to_string();
        match parse_stream_id(&id_str) {
            Ok(id) => ids.push(id),
            Err(e) => return e,
        }
    }

    let db = ctx.db();
    match db.get_mut(&key) {
        Some(RedisObject::Stream(s)) => {
            let mut deleted = 0i64;
            for id in &ids {
                if s.entries.remove(id).is_some() {
                    deleted += 1;
                }
            }
            RespValue::integer(deleted)
        }
        Some(_) => RespValue::wrong_type(),
        None => RespValue::integer(0),
    }
}

// === XTRIM ===
pub fn cmd_xtrim(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 4 {
        return RespValue::wrong_arity("xtrim");
    }

    let key = ctx.args[1].clone();
    let args_clone: Vec<Bytes> = ctx.args.iter().cloned().collect();
    let trim = match parse_trim_args(&args_clone, 2) {
        Ok(Some(tc)) => tc,
        Ok(None) => return RespValue::error("ERR syntax error"),
        Err(e) => return e,
    };

    let db = ctx.db();
    match db.get_mut(&key) {
        Some(RedisObject::Stream(s)) => {
            let removed = apply_trim(s, &trim);
            RespValue::integer(removed as i64)
        }
        Some(_) => RespValue::wrong_type(),
        None => RespValue::integer(0),
    }
}

// === XREAD ===
pub fn cmd_xread(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 4 {
        return RespValue::wrong_arity("xread");
    }

    let mut idx = 1;
    let mut count: Option<usize> = None;

    // Parse options
    loop {
        if idx >= ctx.args.len() {
            return RespValue::error("ERR syntax error");
        }
        let arg = String::from_utf8_lossy(&ctx.args[idx]).to_uppercase();
        match arg.as_str() {
            "COUNT" => {
                idx += 1;
                if idx >= ctx.args.len() {
                    return RespValue::error("ERR syntax error");
                }
                count = Some(
                    match String::from_utf8_lossy(&ctx.args[idx]).parse() {
                        Ok(v) => v,
                        Err(_) => {
                            return RespValue::error(
                                "ERR value is not an integer or out of range",
                            )
                        }
                    },
                );
                idx += 1;
            }
            "BLOCK" => {
                idx += 1;
                if idx >= ctx.args.len() {
                    return RespValue::error("ERR syntax error");
                }
                // Skip the timeout value, we don't support blocking
                idx += 1;
            }
            "STREAMS" => {
                idx += 1;
                break;
            }
            _ => return RespValue::error("ERR syntax error"),
        }
    }

    // Remaining args: keys then IDs
    let remaining = ctx.args.len() - idx;
    if remaining == 0 || remaining % 2 != 0 {
        return RespValue::error(
            "ERR Unbalanced 'xread' list of streams: for each stream key an ID or '$' must be specified.",
        );
    }

    let num_streams = remaining / 2;
    let keys: Vec<Bytes> = ctx.args[idx..idx + num_streams].to_vec();
    let id_strs: Vec<String> = ctx.args[idx + num_streams..idx + 2 * num_streams]
        .iter()
        .map(|b| String::from_utf8_lossy(b).to_string())
        .collect();

    let db = ctx.db();

    let mut results: Vec<RespValue> = Vec::new();
    for (i, key) in keys.iter().enumerate() {
        let id_str = &id_strs[i];

        let start = if id_str == "$" {
            // $ means "new entries from now on" - for non-blocking, return nothing
            continue;
        } else {
            match parse_stream_id(id_str) {
                Ok(id) => id,
                Err(e) => return e,
            }
        };

        match db.get(key) {
            Some(RedisObject::Stream(s)) => {
                // XREAD returns entries with ID > start (exclusive)
                let next = StreamId {
                    ms: start.ms,
                    seq: start.seq.saturating_add(1),
                };
                let next = if start.seq == u64::MAX {
                    StreamId {
                        ms: start.ms + 1,
                        seq: 0,
                    }
                } else {
                    next
                };

                let mut entries: Vec<RespValue> = Vec::new();
                for (id, fields) in s.entries.range(&next..) {
                    entries.push(format_entry(id, fields));
                    if let Some(c) = count {
                        if entries.len() >= c {
                            break;
                        }
                    }
                }

                if !entries.is_empty() {
                    results.push(RespValue::array(vec![
                        RespValue::bulk_string(key.clone()),
                        RespValue::array(entries),
                    ]));
                }
            }
            Some(_) => return RespValue::wrong_type(),
            None => {}
        }
    }

    if results.is_empty() {
        RespValue::NullArray
    } else {
        RespValue::array(results)
    }
}

// === XINFO ===
pub fn cmd_xinfo(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 3 {
        return RespValue::wrong_arity("xinfo");
    }

    let subcmd = String::from_utf8_lossy(&ctx.args[1]).to_uppercase();
    match subcmd.as_str() {
        "STREAM" => cmd_xinfo_stream(ctx),
        "GROUPS" => cmd_xinfo_groups(ctx),
        "CONSUMERS" => cmd_xinfo_consumers(ctx),
        "HELP" => RespValue::array(vec![
            RespValue::bulk_string(Bytes::from("XINFO STREAM <key>")),
            RespValue::bulk_string(Bytes::from("XINFO GROUPS <key>")),
            RespValue::bulk_string(Bytes::from("XINFO CONSUMERS <key> <groupname>")),
        ]),
        _ => RespValue::error(format!(
            "ERR unknown subcommand or wrong number of arguments for 'xinfo|{}' command",
            subcmd.to_lowercase()
        )),
    }
}

fn cmd_xinfo_stream(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[2].clone();
    let db = ctx.db();

    match db.get(&key) {
        Some(RedisObject::Stream(s)) => {
            let length = s.len() as i64;
            let last_id_str = s.last_id.to_string();
            let groups = s.groups.len() as i64;

            let first_entry = if let Some((id, fields)) = s.entries.iter().next() {
                format_entry(id, fields)
            } else {
                RespValue::Null
            };

            let last_entry = if let Some((id, fields)) = s.entries.iter().next_back() {
                format_entry(id, fields)
            } else {
                RespValue::Null
            };

            RespValue::array(vec![
                RespValue::bulk_string(Bytes::from("length")),
                RespValue::integer(length),
                RespValue::bulk_string(Bytes::from("radix-tree-keys")),
                RespValue::integer(1),
                RespValue::bulk_string(Bytes::from("radix-tree-nodes")),
                RespValue::integer(2),
                RespValue::bulk_string(Bytes::from("last-generated-id")),
                RespValue::bulk_string(Bytes::from(last_id_str)),
                RespValue::bulk_string(Bytes::from("groups")),
                RespValue::integer(groups),
                RespValue::bulk_string(Bytes::from("first-entry")),
                first_entry,
                RespValue::bulk_string(Bytes::from("last-entry")),
                last_entry,
            ])
        }
        Some(_) => RespValue::wrong_type(),
        None => RespValue::error("ERR no such key"),
    }
}

fn cmd_xinfo_groups(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[2].clone();
    let db = ctx.db();

    match db.get(&key) {
        Some(RedisObject::Stream(s)) => {
            let mut results: Vec<RespValue> = Vec::new();
            for (name, group) in &s.groups {
                results.push(RespValue::array(vec![
                    RespValue::bulk_string(Bytes::from("name")),
                    RespValue::bulk_string(name.clone()),
                    RespValue::bulk_string(Bytes::from("consumers")),
                    RespValue::integer(group.consumers.len() as i64),
                    RespValue::bulk_string(Bytes::from("pending")),
                    RespValue::integer(group.pel.len() as i64),
                    RespValue::bulk_string(Bytes::from("last-delivered-id")),
                    RespValue::bulk_string(Bytes::from(group.last_delivered.to_string())),
                    RespValue::bulk_string(Bytes::from("entries-read")),
                    RespValue::Null,
                    RespValue::bulk_string(Bytes::from("lag")),
                    RespValue::integer(0),
                ]));
            }
            RespValue::array(results)
        }
        Some(_) => RespValue::wrong_type(),
        None => RespValue::error("ERR no such key"),
    }
}

fn cmd_xinfo_consumers(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 4 {
        return RespValue::wrong_arity("xinfo");
    }

    let key = ctx.args[2].clone();
    let group_name = ctx.args[3].clone();
    let db = ctx.db();

    match db.get(&key) {
        Some(RedisObject::Stream(s)) => {
            let group = match s.groups.get(&group_name) {
                Some(g) => g,
                None => {
                    return RespValue::error(
                        "NOGROUP No such consumer group for key name",
                    )
                }
            };

            let mut results: Vec<RespValue> = Vec::new();
            for (name, consumer) in &group.consumers {
                results.push(RespValue::array(vec![
                    RespValue::bulk_string(Bytes::from("name")),
                    RespValue::bulk_string(name.clone()),
                    RespValue::bulk_string(Bytes::from("pending")),
                    RespValue::integer(consumer.pel.len() as i64),
                    RespValue::bulk_string(Bytes::from("idle")),
                    RespValue::integer({
                        let now = current_time_ms();
                        now.saturating_sub(consumer.seen_time) as i64
                    }),
                    RespValue::bulk_string(Bytes::from("inactive")),
                    RespValue::integer({
                        let now = current_time_ms();
                        now.saturating_sub(consumer.seen_time) as i64
                    }),
                ]));
            }
            RespValue::array(results)
        }
        Some(_) => RespValue::wrong_type(),
        None => RespValue::error("ERR no such key"),
    }
}

// === XGROUP ===
pub fn cmd_xgroup(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 2 {
        return RespValue::wrong_arity("xgroup");
    }

    let subcmd = String::from_utf8_lossy(&ctx.args[1]).to_uppercase();
    match subcmd.as_str() {
        "CREATE" => cmd_xgroup_create(ctx),
        "DESTROY" => cmd_xgroup_destroy(ctx),
        "SETID" => cmd_xgroup_setid(ctx),
        "CREATECONSUMER" => cmd_xgroup_createconsumer(ctx),
        "DELCONSUMER" => cmd_xgroup_delconsumer(ctx),
        "HELP" => RespValue::array(vec![
            RespValue::bulk_string(Bytes::from(
                "XGROUP CREATE <key> <groupname> <id|$> [MKSTREAM]",
            )),
            RespValue::bulk_string(Bytes::from("XGROUP DESTROY <key> <groupname>")),
            RespValue::bulk_string(Bytes::from("XGROUP SETID <key> <groupname> <id|$>")),
            RespValue::bulk_string(Bytes::from(
                "XGROUP CREATECONSUMER <key> <groupname> <consumername>",
            )),
            RespValue::bulk_string(Bytes::from(
                "XGROUP DELCONSUMER <key> <groupname> <consumername>",
            )),
        ]),
        _ => RespValue::error(format!(
            "ERR unknown subcommand or wrong number of arguments for 'xgroup|{}' command",
            subcmd.to_lowercase()
        )),
    }
}

fn cmd_xgroup_create(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 5 {
        return RespValue::wrong_arity("xgroup");
    }

    let key = ctx.args[2].clone();
    let group_name = ctx.args[3].clone();
    let id_str = String::from_utf8_lossy(&ctx.args[4]).to_string();

    let mkstream = ctx.args.len() > 5
        && String::from_utf8_lossy(&ctx.args[5]).to_uppercase() == "MKSTREAM";

    let db = ctx.db();

    // Check if key exists
    let exists = db.exists(&key);
    if !exists {
        if mkstream {
            db.set(key.clone(), RedisObject::Stream(StreamData::new()));
        } else {
            return RespValue::error("ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.");
        }
    }

    let stream = match db.get_mut(&key) {
        Some(RedisObject::Stream(s)) => s,
        Some(_) => return RespValue::wrong_type(),
        None => return RespValue::error("ERR The XGROUP subcommand requires the key to exist."),
    };

    let last_delivered = if id_str == "$" {
        stream.last_id.clone()
    } else if id_str == "0" || id_str == "0-0" {
        StreamId::zero()
    } else {
        match parse_stream_id(&id_str) {
            Ok(id) => id,
            Err(e) => return e,
        }
    };

    if stream.groups.contains_key(&group_name) {
        return RespValue::error("BUSYGROUP Consumer Group name already exists");
    }

    stream
        .groups
        .insert(group_name, ConsumerGroup::new(last_delivered));
    RespValue::ok()
}

fn cmd_xgroup_destroy(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 4 {
        return RespValue::wrong_arity("xgroup");
    }

    let key = ctx.args[2].clone();
    let group_name = ctx.args[3].clone();

    let db = ctx.db();
    match db.get_mut(&key) {
        Some(RedisObject::Stream(s)) => {
            if s.groups.remove(&group_name).is_some() {
                RespValue::integer(1)
            } else {
                RespValue::integer(0)
            }
        }
        Some(_) => RespValue::wrong_type(),
        None => RespValue::integer(0),
    }
}

fn cmd_xgroup_setid(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 5 {
        return RespValue::wrong_arity("xgroup");
    }

    let key = ctx.args[2].clone();
    let group_name = ctx.args[3].clone();
    let id_str = String::from_utf8_lossy(&ctx.args[4]).to_string();

    let db = ctx.db();
    let stream = match db.get_mut(&key) {
        Some(RedisObject::Stream(s)) => s,
        Some(_) => return RespValue::wrong_type(),
        None => return RespValue::error("ERR The XGROUP subcommand requires the key to exist."),
    };

    let new_id = if id_str == "$" {
        stream.last_id.clone()
    } else if id_str == "0" || id_str == "0-0" {
        StreamId::zero()
    } else {
        match parse_stream_id(&id_str) {
            Ok(id) => id,
            Err(e) => return e,
        }
    };

    match stream.groups.get_mut(&group_name) {
        Some(group) => {
            group.last_delivered = new_id;
            RespValue::ok()
        }
        None => RespValue::error("NOGROUP No such consumer group for key name"),
    }
}

fn cmd_xgroup_createconsumer(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 5 {
        return RespValue::wrong_arity("xgroup");
    }

    let key = ctx.args[2].clone();
    let group_name = ctx.args[3].clone();
    let consumer_name = ctx.args[4].clone();

    let db = ctx.db();
    let stream = match db.get_mut(&key) {
        Some(RedisObject::Stream(s)) => s,
        Some(_) => return RespValue::wrong_type(),
        None => return RespValue::error("ERR The XGROUP subcommand requires the key to exist."),
    };

    let group = match stream.groups.get_mut(&group_name) {
        Some(g) => g,
        None => return RespValue::error("NOGROUP No such consumer group for key name"),
    };

    if group.consumers.contains_key(&consumer_name) {
        RespValue::integer(0)
    } else {
        group.consumers.insert(
            consumer_name,
            ConsumerData {
                pel: HashSet::new(),
                seen_time: current_time_ms(),
            },
        );
        RespValue::integer(1)
    }
}

fn cmd_xgroup_delconsumer(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 5 {
        return RespValue::wrong_arity("xgroup");
    }

    let key = ctx.args[2].clone();
    let group_name = ctx.args[3].clone();
    let consumer_name = ctx.args[4].clone();

    let db = ctx.db();
    let stream = match db.get_mut(&key) {
        Some(RedisObject::Stream(s)) => s,
        Some(_) => return RespValue::wrong_type(),
        None => return RespValue::error("ERR The XGROUP subcommand requires the key to exist."),
    };

    let group = match stream.groups.get_mut(&group_name) {
        Some(g) => g,
        None => return RespValue::error("NOGROUP No such consumer group for key name"),
    };

    if let Some(consumer) = group.consumers.remove(&consumer_name) {
        let pending_count = consumer.pel.len() as i64;
        // Remove consumer's entries from group PEL
        for id in &consumer.pel {
            group.pel.remove(id);
        }
        RespValue::integer(pending_count)
    } else {
        RespValue::integer(0)
    }
}

// === XREADGROUP ===
pub fn cmd_xreadgroup(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 7 {
        return RespValue::wrong_arity("xreadgroup");
    }

    let mut idx = 1;

    // Parse GROUP group consumer
    let arg = String::from_utf8_lossy(&ctx.args[idx]).to_uppercase();
    if arg != "GROUP" {
        return RespValue::error("ERR syntax error");
    }
    idx += 1;

    if idx + 1 >= ctx.args.len() {
        return RespValue::error("ERR syntax error");
    }
    let group_name = ctx.args[idx].clone();
    idx += 1;
    let consumer_name = ctx.args[idx].clone();
    idx += 1;

    let mut count: Option<usize> = None;
    let mut noack = false;

    // Parse options
    loop {
        if idx >= ctx.args.len() {
            return RespValue::error("ERR syntax error");
        }
        let arg = String::from_utf8_lossy(&ctx.args[idx]).to_uppercase();
        match arg.as_str() {
            "COUNT" => {
                idx += 1;
                if idx >= ctx.args.len() {
                    return RespValue::error("ERR syntax error");
                }
                count = Some(
                    match String::from_utf8_lossy(&ctx.args[idx]).parse() {
                        Ok(v) => v,
                        Err(_) => {
                            return RespValue::error(
                                "ERR value is not an integer or out of range",
                            )
                        }
                    },
                );
                idx += 1;
            }
            "BLOCK" => {
                idx += 1;
                if idx >= ctx.args.len() {
                    return RespValue::error("ERR syntax error");
                }
                idx += 1; // Skip timeout
            }
            "NOACK" => {
                noack = true;
                idx += 1;
            }
            "STREAMS" => {
                idx += 1;
                break;
            }
            _ => return RespValue::error("ERR syntax error"),
        }
    }

    let remaining = ctx.args.len() - idx;
    if remaining == 0 || remaining % 2 != 0 {
        return RespValue::error(
            "ERR Unbalanced 'xreadgroup' list of streams: for each stream key an ID or '>' must be specified.",
        );
    }

    let num_streams = remaining / 2;
    let keys: Vec<Bytes> = ctx.args[idx..idx + num_streams].to_vec();
    let id_strs: Vec<String> = ctx.args[idx + num_streams..idx + 2 * num_streams]
        .iter()
        .map(|b| String::from_utf8_lossy(b).to_string())
        .collect();

    let db = ctx.db();
    let now = current_time_ms();
    let mut results: Vec<RespValue> = Vec::new();

    for (i, key) in keys.iter().enumerate() {
        let id_str = &id_strs[i];

        let stream = match db.get_mut(key) {
            Some(RedisObject::Stream(s)) => s,
            Some(_) => return RespValue::wrong_type(),
            None => {
                return RespValue::error("ERR The XREADGROUP subcommand requires the key to exist.")
            }
        };

        let group = match stream.groups.get_mut(&group_name) {
            Some(g) => g,
            None => {
                return RespValue::error(
                    "NOGROUP No such consumer group for key name",
                )
            }
        };

        // Ensure consumer exists
        if !group.consumers.contains_key(&consumer_name) {
            group.consumers.insert(
                consumer_name.clone(),
                ConsumerData {
                    pel: HashSet::new(),
                    seen_time: now,
                },
            );
        }

        if id_str == ">" {
            // Deliver new messages (after last_delivered)
            let start = StreamId {
                ms: group.last_delivered.ms,
                seq: group.last_delivered.seq.saturating_add(1),
            };
            let start = if group.last_delivered.seq == u64::MAX {
                StreamId {
                    ms: group.last_delivered.ms + 1,
                    seq: 0,
                }
            } else {
                start
            };

            let mut entries: Vec<RespValue> = Vec::new();
            let mut new_last_delivered = group.last_delivered.clone();

            let entry_list: Vec<(StreamId, Vec<(Bytes, Bytes)>)> = stream
                .entries
                .range(&start..)
                .map(|(id, fields)| (id.clone(), fields.clone()))
                .collect();

            for (id, fields) in entry_list {
                entries.push(format_entry(&id, &fields));
                new_last_delivered = id.clone();

                if !noack {
                    // Add to PEL
                    group.pel.insert(
                        id.clone(),
                        PendingEntry {
                            consumer: consumer_name.clone(),
                            delivery_time: now,
                            delivery_count: 1,
                        },
                    );
                    if let Some(consumer) = group.consumers.get_mut(&consumer_name) {
                        consumer.pel.insert(id.clone());
                        consumer.seen_time = now;
                    }
                }

                if let Some(c) = count {
                    if entries.len() >= c {
                        break;
                    }
                }
            }

            if !entries.is_empty() {
                group.last_delivered = new_last_delivered;
                results.push(RespValue::array(vec![
                    RespValue::bulk_string(key.clone()),
                    RespValue::array(entries),
                ]));
            }
        } else {
            // Return pending entries for this consumer
            let start_id = if id_str == "0" || id_str == "0-0" {
                StreamId::zero()
            } else {
                match parse_stream_id(id_str) {
                    Ok(id) => id,
                    Err(e) => return e,
                }
            };

            let mut entries: Vec<RespValue> = Vec::new();

            // Get pending entries for this consumer
            let pending_ids: Vec<StreamId> = group
                .pel
                .range(&start_id..)
                .filter(|(_, pe)| pe.consumer == consumer_name)
                .map(|(id, _)| id.clone())
                .collect();

            for id in pending_ids {
                if let Some(fields) = stream.entries.get(&id) {
                    entries.push(format_entry(&id, fields));
                    // Update delivery info
                    if let Some(pe) = group.pel.get_mut(&id) {
                        pe.delivery_count += 1;
                        pe.delivery_time = now;
                    }
                }
                if let Some(c) = count {
                    if entries.len() >= c {
                        break;
                    }
                }
            }

            if let Some(consumer) = group.consumers.get_mut(&consumer_name) {
                consumer.seen_time = now;
            }

            if !entries.is_empty() {
                results.push(RespValue::array(vec![
                    RespValue::bulk_string(key.clone()),
                    RespValue::array(entries),
                ]));
            }
        }
    }

    if results.is_empty() {
        RespValue::NullArray
    } else {
        RespValue::array(results)
    }
}

// === XACK ===
pub fn cmd_xack(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 4 {
        return RespValue::wrong_arity("xack");
    }

    let key = ctx.args[1].clone();
    let group_name = ctx.args[2].clone();

    let mut ids: Vec<StreamId> = Vec::new();
    for i in 3..ctx.args.len() {
        let id_str = String::from_utf8_lossy(&ctx.args[i]).to_string();
        match parse_stream_id(&id_str) {
            Ok(id) => ids.push(id),
            Err(e) => return e,
        }
    }

    let db = ctx.db();
    let stream = match db.get_mut(&key) {
        Some(RedisObject::Stream(s)) => s,
        Some(_) => return RespValue::wrong_type(),
        None => return RespValue::integer(0),
    };

    let group = match stream.groups.get_mut(&group_name) {
        Some(g) => g,
        None => return RespValue::integer(0),
    };

    let mut acked = 0i64;
    for id in &ids {
        if let Some(pe) = group.pel.remove(id) {
            if let Some(consumer) = group.consumers.get_mut(&pe.consumer) {
                consumer.pel.remove(id);
            }
            acked += 1;
        }
    }

    RespValue::integer(acked)
}

// === XPENDING ===
pub fn cmd_xpending(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 3 {
        return RespValue::wrong_arity("xpending");
    }

    let key = ctx.args[1].clone();
    let group_name = ctx.args[2].clone();
    let args_len = ctx.args.len();

    // Clone all needed args before borrowing ctx.db()
    let is_summary = args_len == 3
        || (args_len == 4
            && String::from_utf8_lossy(&ctx.args[3]).to_uppercase() == "IDLE");

    // Pre-parse detailed form args
    let (min_idle, start_str, end_str, detail_count, consumer_filter) = if !is_summary {
        let mut idx = 3;
        let mut min_idle_val: Option<u64> = None;

        if idx < args_len {
            let s = String::from_utf8_lossy(&ctx.args[idx]).to_uppercase();
            if s == "IDLE" {
                idx += 1;
                if idx >= args_len {
                    return RespValue::error("ERR syntax error");
                }
                min_idle_val = Some(
                    match String::from_utf8_lossy(&ctx.args[idx]).parse() {
                        Ok(v) => v,
                        Err(_) => {
                            return RespValue::error(
                                "ERR value is not an integer or out of range",
                            )
                        }
                    },
                );
                idx += 1;
            }
        }

        if idx + 2 >= args_len {
            return RespValue::wrong_arity("xpending");
        }

        let s_str = String::from_utf8_lossy(&ctx.args[idx]).to_string();
        let e_str = String::from_utf8_lossy(&ctx.args[idx + 1]).to_string();
        let cnt: usize = match String::from_utf8_lossy(&ctx.args[idx + 2]).parse() {
            Ok(v) => v,
            Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
        };
        idx += 3;

        let cf: Option<Bytes> = if idx < args_len {
            Some(ctx.args[idx].clone())
        } else {
            None
        };

        (min_idle_val, Some(s_str), Some(e_str), Some(cnt), cf)
    } else {
        (None, None, None, None, None)
    };

    let db = ctx.db();
    let stream = match db.get(&key) {
        Some(RedisObject::Stream(s)) => s,
        Some(_) => return RespValue::wrong_type(),
        None => return RespValue::error("ERR no such key"),
    };

    let group = match stream.groups.get(&group_name) {
        Some(g) => g,
        None => {
            return RespValue::error("NOGROUP No such consumer group for key name")
        }
    };

    if is_summary {
        let total = group.pel.len() as i64;
        if total == 0 {
            return RespValue::array(vec![
                RespValue::integer(0),
                RespValue::Null,
                RespValue::Null,
                RespValue::Null,
            ]);
        }

        let min_id = group.pel.keys().next().unwrap().to_string();
        let max_id = group.pel.keys().next_back().unwrap().to_string();

        let mut consumer_counts: HashMap<Bytes, i64> = HashMap::new();
        for pe in group.pel.values() {
            *consumer_counts.entry(pe.consumer.clone()).or_insert(0) += 1;
        }

        let consumer_array: Vec<RespValue> = consumer_counts
            .iter()
            .map(|(name, count)| {
                RespValue::array(vec![
                    RespValue::bulk_string(name.clone()),
                    RespValue::bulk_string(Bytes::from(count.to_string())),
                ])
            })
            .collect();

        RespValue::array(vec![
            RespValue::integer(total),
            RespValue::bulk_string(Bytes::from(min_id)),
            RespValue::bulk_string(Bytes::from(max_id)),
            RespValue::array(consumer_array),
        ])
    } else {
        let start_str = start_str.unwrap();
        let end_str = end_str.unwrap();
        let count = detail_count.unwrap();

        let start = match parse_stream_id_for_range_start(&start_str) {
            Ok(id) => id,
            Err(e) => return e,
        };
        let end = match parse_stream_id_for_range_end(&end_str) {
            Ok(id) => id,
            Err(e) => return e,
        };

        let now = current_time_ms();
        let mut results: Vec<RespValue> = Vec::new();

        for (id, pe) in group.pel.range(&start..=&end) {
            if let Some(ref cf) = consumer_filter {
                if &pe.consumer != cf {
                    continue;
                }
            }

            if let Some(min_idle_val) = min_idle {
                let idle = now.saturating_sub(pe.delivery_time);
                if idle < min_idle_val {
                    continue;
                }
            }

            results.push(RespValue::array(vec![
                RespValue::bulk_string(Bytes::from(id.to_string())),
                RespValue::bulk_string(pe.consumer.clone()),
                RespValue::integer(now.saturating_sub(pe.delivery_time) as i64),
                RespValue::integer(pe.delivery_count as i64),
            ]));

            if results.len() >= count {
                break;
            }
        }

        RespValue::array(results)
    }
}

// === XCLAIM ===
pub fn cmd_xclaim(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 5 {
        return RespValue::wrong_arity("xclaim");
    }

    let key = ctx.args[1].clone();
    let group_name = ctx.args[2].clone();
    let consumer_name = ctx.args[3].clone();
    let min_idle_time: u64 = match String::from_utf8_lossy(&ctx.args[4]).parse() {
        Ok(v) => v,
        Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
    };

    let mut ids: Vec<StreamId> = Vec::new();
    for i in 5..ctx.args.len() {
        let arg_str = String::from_utf8_lossy(&ctx.args[i]).to_uppercase();
        // Stop at option flags
        if arg_str == "IDLE" || arg_str == "TIME" || arg_str == "RETRYCOUNT"
            || arg_str == "FORCE" || arg_str == "JUSTID"
        {
            break;
        }
        let id_str = String::from_utf8_lossy(&ctx.args[i]).to_string();
        match parse_stream_id(&id_str) {
            Ok(id) => ids.push(id),
            Err(e) => return e,
        }
    }

    let now = current_time_ms();
    let db = ctx.db();
    let stream = match db.get_mut(&key) {
        Some(RedisObject::Stream(s)) => s,
        Some(_) => return RespValue::wrong_type(),
        None => return RespValue::array(vec![]),
    };

    let group = match stream.groups.get_mut(&group_name) {
        Some(g) => g,
        None => {
            return RespValue::error("NOGROUP No such consumer group for key name")
        }
    };

    // Ensure consumer exists
    if !group.consumers.contains_key(&consumer_name) {
        group.consumers.insert(
            consumer_name.clone(),
            ConsumerData {
                pel: HashSet::new(),
                seen_time: now,
            },
        );
    }

    let mut results: Vec<RespValue> = Vec::new();

    for id in &ids {
        let should_claim = if let Some(pe) = group.pel.get(id) {
            let idle = now.saturating_sub(pe.delivery_time);
            idle >= min_idle_time
        } else {
            false
        };

        if should_claim {
            // Transfer ownership
            if let Some(pe) = group.pel.get_mut(id) {
                let old_consumer = pe.consumer.clone();
                pe.consumer = consumer_name.clone();
                pe.delivery_time = now;
                pe.delivery_count += 1;

                // Update consumer PELs
                if let Some(old_c) = group.consumers.get_mut(&old_consumer) {
                    old_c.pel.remove(id);
                }
                if let Some(new_c) = group.consumers.get_mut(&consumer_name) {
                    new_c.pel.insert(id.clone());
                    new_c.seen_time = now;
                }
            }

            // Return entry if it still exists
            if let Some(fields) = stream.entries.get(id) {
                results.push(format_entry(id, fields));
            }
        }
    }

    RespValue::array(results)
}

// === XAUTOCLAIM ===
pub fn cmd_xautoclaim(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 6 {
        return RespValue::wrong_arity("xautoclaim");
    }

    let key = ctx.args[1].clone();
    let group_name = ctx.args[2].clone();
    let consumer_name = ctx.args[3].clone();
    let min_idle_time: u64 = match String::from_utf8_lossy(&ctx.args[4]).parse() {
        Ok(v) => v,
        Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
    };
    let start_str = String::from_utf8_lossy(&ctx.args[5]).to_string();
    let start = match parse_stream_id_for_range_start(&start_str) {
        Ok(id) => id,
        Err(e) => return e,
    };

    let mut count: usize = 100; // default
    if ctx.args.len() >= 8 {
        let opt = String::from_utf8_lossy(&ctx.args[6]).to_uppercase();
        if opt == "COUNT" {
            count = match String::from_utf8_lossy(&ctx.args[7]).parse() {
                Ok(v) => v,
                Err(_) => {
                    return RespValue::error("ERR value is not an integer or out of range")
                }
            };
        }
    }

    let now = current_time_ms();
    let db = ctx.db();
    let stream = match db.get_mut(&key) {
        Some(RedisObject::Stream(s)) => s,
        Some(_) => return RespValue::wrong_type(),
        None => {
            return RespValue::array(vec![
                RespValue::bulk_string(Bytes::from("0-0")),
                RespValue::array(vec![]),
                RespValue::array(vec![]),
            ])
        }
    };

    let group = match stream.groups.get_mut(&group_name) {
        Some(g) => g,
        None => {
            return RespValue::error("NOGROUP No such consumer group for key name")
        }
    };

    // Ensure consumer exists
    if !group.consumers.contains_key(&consumer_name) {
        group.consumers.insert(
            consumer_name.clone(),
            ConsumerData {
                pel: HashSet::new(),
                seen_time: now,
            },
        );
    }

    let mut results: Vec<RespValue> = Vec::new();
    let mut deleted_ids: Vec<RespValue> = Vec::new();
    let mut next_start = StreamId::zero();
    let mut claimed = 0;

    // Collect IDs to claim first
    let claimable: Vec<(StreamId, Bytes)> = group
        .pel
        .range(&start..)
        .filter(|(_, pe)| {
            let idle = now.saturating_sub(pe.delivery_time);
            idle >= min_idle_time
        })
        .take(count)
        .map(|(id, pe)| (id.clone(), pe.consumer.clone()))
        .collect();

    for (id, old_consumer) in &claimable {
        // Check if the entry still exists
        if stream.entries.contains_key(id) {
            if let Some(pe) = group.pel.get_mut(id) {
                pe.consumer = consumer_name.clone();
                pe.delivery_time = now;
                pe.delivery_count += 1;
            }

            if let Some(old_c) = group.consumers.get_mut(old_consumer) {
                old_c.pel.remove(id);
            }
            if let Some(new_c) = group.consumers.get_mut(&consumer_name) {
                new_c.pel.insert(id.clone());
                new_c.seen_time = now;
            }

            if let Some(fields) = stream.entries.get(id) {
                results.push(format_entry(id, fields));
            }
        } else {
            // Entry was deleted, remove from PEL
            if let Some(pe) = group.pel.remove(id) {
                if let Some(c) = group.consumers.get_mut(&pe.consumer) {
                    c.pel.remove(id);
                }
            }
            deleted_ids.push(RespValue::bulk_string(Bytes::from(id.to_string())));
        }

        next_start = StreamId {
            ms: id.ms,
            seq: id.seq.saturating_add(1),
        };
        claimed += 1;
    }

    // If we claimed fewer than count, cursor is 0-0 (scan complete)
    let cursor = if claimed < count {
        "0-0".to_string()
    } else {
        next_start.to_string()
    };

    RespValue::array(vec![
        RespValue::bulk_string(Bytes::from(cursor)),
        RespValue::array(results),
        RespValue::array(deleted_ids),
    ])
}
