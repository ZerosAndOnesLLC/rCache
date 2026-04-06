use bytes::Bytes;
use crate::protocol::RespValue;
use crate::storage::RedisObject;
use crate::storage::db::glob_match;
use super::registry::CommandContext;

pub fn cmd_scan(ctx: &mut CommandContext) -> RespValue {
    let cursor: usize = match String::from_utf8_lossy(&ctx.args[1]).parse() {
        Ok(v) => v,
        Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
    };

    let mut pattern: Option<String> = None;
    let mut count = 10usize;
    let mut type_filter: Option<String> = None;

    let mut i = 2;
    while i < ctx.args.len() {
        let opt = String::from_utf8_lossy(&ctx.args[i]).to_uppercase();
        match opt.as_str() {
            "MATCH" => {
                i += 1;
                if i < ctx.args.len() {
                    pattern = Some(String::from_utf8_lossy(&ctx.args[i]).to_string());
                }
            }
            "COUNT" => {
                i += 1;
                if i < ctx.args.len() {
                    count = String::from_utf8_lossy(&ctx.args[i]).parse().unwrap_or(10);
                }
            }
            "TYPE" => {
                i += 1;
                if i < ctx.args.len() {
                    type_filter = Some(String::from_utf8_lossy(&ctx.args[i]).to_string());
                }
            }
            _ => {}
        }
        i += 1;
    }

    let (new_cursor, keys) = ctx.db().scan(
        cursor,
        pattern.as_deref(),
        count,
        type_filter.as_deref(),
    );

    RespValue::array(vec![
        RespValue::bulk_string(Bytes::from(new_cursor.to_string())),
        RespValue::array(keys.into_iter().map(RespValue::bulk_string).collect()),
    ])
}

pub fn cmd_sscan(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let cursor: usize = match String::from_utf8_lossy(&ctx.args[2]).parse() {
        Ok(v) => v,
        Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
    };

    let mut pattern: Option<String> = None;
    let mut count = 10usize;

    let mut i = 3;
    while i < ctx.args.len() {
        let opt = String::from_utf8_lossy(&ctx.args[i]).to_uppercase();
        match opt.as_str() {
            "MATCH" => {
                i += 1;
                if i < ctx.args.len() {
                    pattern = Some(String::from_utf8_lossy(&ctx.args[i]).to_string());
                }
            }
            "COUNT" => {
                i += 1;
                if i < ctx.args.len() {
                    count = String::from_utf8_lossy(&ctx.args[i]).parse().unwrap_or(10);
                }
            }
            _ => {}
        }
        i += 1;
    }

    let db = ctx.db();
    match db.get(&key) {
        Some(RedisObject::Set(set)) => {
            let all: Vec<Bytes> = set.iter().cloned().collect();
            let (new_cursor, items) = scan_collection(&all, cursor, count, pattern.as_deref());
            RespValue::array(vec![
                RespValue::bulk_string(Bytes::from(new_cursor.to_string())),
                RespValue::array(items.into_iter().map(|b| RespValue::bulk_string(b.clone())).collect()),
            ])
        }
        Some(_) => RespValue::wrong_type(),
        None => RespValue::array(vec![
            RespValue::bulk_string(Bytes::from("0")),
            RespValue::array(vec![]),
        ]),
    }
}

pub fn cmd_hscan(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let cursor: usize = match String::from_utf8_lossy(&ctx.args[2]).parse() {
        Ok(v) => v,
        Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
    };

    let mut pattern: Option<String> = None;
    let mut count = 10usize;

    let mut i = 3;
    while i < ctx.args.len() {
        let opt = String::from_utf8_lossy(&ctx.args[i]).to_uppercase();
        match opt.as_str() {
            "MATCH" => {
                i += 1;
                if i < ctx.args.len() {
                    pattern = Some(String::from_utf8_lossy(&ctx.args[i]).to_string());
                }
            }
            "COUNT" => {
                i += 1;
                if i < ctx.args.len() {
                    count = String::from_utf8_lossy(&ctx.args[i]).parse().unwrap_or(10);
                }
            }
            _ => {}
        }
        i += 1;
    }

    let db = ctx.db();
    match db.get(&key) {
        Some(RedisObject::Hash(hash)) => {
            let all_keys: Vec<Bytes> = hash.keys().cloned().collect();
            let hash_clone: Vec<(Bytes, Bytes)> = hash.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            let (new_cursor, matched_keys) = scan_collection(&all_keys, cursor, count, pattern.as_deref());
            let mut items = Vec::new();
            for k in matched_keys {
                items.push(RespValue::bulk_string(k.clone()));
                if let Some((_, v)) = hash_clone.iter().find(|(hk, _)| hk == k) {
                    items.push(RespValue::bulk_string(v.clone()));
                }
            }
            RespValue::array(vec![
                RespValue::bulk_string(Bytes::from(new_cursor.to_string())),
                RespValue::array(items),
            ])
        }
        Some(_) => RespValue::wrong_type(),
        None => RespValue::array(vec![
            RespValue::bulk_string(Bytes::from("0")),
            RespValue::array(vec![]),
        ]),
    }
}

pub fn cmd_zscan(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let cursor: usize = match String::from_utf8_lossy(&ctx.args[2]).parse() {
        Ok(v) => v,
        Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
    };

    let mut pattern: Option<String> = None;
    let mut count = 10usize;

    let mut i = 3;
    while i < ctx.args.len() {
        let opt = String::from_utf8_lossy(&ctx.args[i]).to_uppercase();
        match opt.as_str() {
            "MATCH" => {
                i += 1;
                if i < ctx.args.len() {
                    pattern = Some(String::from_utf8_lossy(&ctx.args[i]).to_string());
                }
            }
            "COUNT" => {
                i += 1;
                if i < ctx.args.len() {
                    count = String::from_utf8_lossy(&ctx.args[i]).parse().unwrap_or(10);
                }
            }
            _ => {}
        }
        i += 1;
    }

    let db = ctx.db();
    match db.get(&key) {
        Some(RedisObject::SortedSet(zset)) => {
            let all: Vec<(Bytes, f64)> = zset.range_by_index(0, -1);
            let all_members: Vec<Bytes> = all.iter().map(|(m, _)| m.clone()).collect();
            let (new_cursor, matched) = scan_collection(&all_members, cursor, count, pattern.as_deref());
            let mut items = Vec::new();
            for m in matched {
                if let Some((_, s)) = all.iter().find(|(am, _)| am == m) {
                    items.push(RespValue::bulk_string(m.clone()));
                    items.push(RespValue::bulk_string(Bytes::from(format!("{}", s))));
                }
            }
            RespValue::array(vec![
                RespValue::bulk_string(Bytes::from(new_cursor.to_string())),
                RespValue::array(items),
            ])
        }
        Some(_) => RespValue::wrong_type(),
        None => RespValue::array(vec![
            RespValue::bulk_string(Bytes::from("0")),
            RespValue::array(vec![]),
        ]),
    }
}

fn scan_collection<'a>(items: &'a [Bytes], cursor: usize, count: usize, pattern: Option<&str>) -> (usize, Vec<&'a Bytes>) {
    let total = items.len();
    if total == 0 {
        return (0, vec![]);
    }

    let mut result = Vec::new();
    let start = cursor;
    let mut i = start;
    let mut scanned = 0;

    while scanned < count.max(10) && i < total {
        let item = &items[i];
        i += 1;
        scanned += 1;

        if let Some(pat) = pattern {
            let s = String::from_utf8_lossy(item);
            if !glob_match(pat, &s) {
                continue;
            }
        }

        result.push(item);
    }

    let new_cursor = if i >= total { 0 } else { i };
    (new_cursor, result)
}
