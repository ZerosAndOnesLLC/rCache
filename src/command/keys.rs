use bytes::Bytes;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use crate::protocol::RespValue;
use crate::storage::RedisObject;
use super::registry::CommandContext;

pub fn cmd_del(ctx: &mut CommandContext) -> RespValue {
    let mut count = 0i64;
    for key in ctx.args[1..].to_vec() {
        if ctx.db().remove(&key).is_some() {
            count += 1;
        }
    }
    RespValue::integer(count)
}

pub fn cmd_exists(ctx: &mut CommandContext) -> RespValue {
    let mut count = 0i64;
    let keys: Vec<Bytes> = ctx.args[1..].to_vec();
    for key in &keys {
        if ctx.db().exists(key) {
            count += 1;
        }
    }
    RespValue::integer(count)
}

fn parse_expire_flags(args: &[Bytes], start: usize) -> (bool, bool, bool, bool) {
    let mut nx = false;
    let mut xx = false;
    let mut gt = false;
    let mut lt = false;
    for arg in &args[start..] {
        match String::from_utf8_lossy(arg).to_uppercase().as_str() {
            "NX" => nx = true,
            "XX" => xx = true,
            "GT" => gt = true,
            "LT" => lt = true,
            _ => {}
        }
    }
    (nx, xx, gt, lt)
}

fn apply_expire(ctx: &mut CommandContext, key: &Bytes, new_expire: Instant, nx: bool, xx: bool, gt: bool, lt: bool) -> RespValue {
    let db = ctx.db();
    if !db.exists(key) {
        return RespValue::integer(0);
    }

    let current_expire = db.get_expire(key);

    if nx && current_expire.is_some() {
        return RespValue::integer(0);
    }
    if xx && current_expire.is_none() {
        return RespValue::integer(0);
    }
    if gt {
        if let Some(cur) = current_expire {
            if new_expire <= cur {
                return RespValue::integer(0);
            }
        }
    }
    if lt {
        if let Some(cur) = current_expire {
            if new_expire >= cur {
                return RespValue::integer(0);
            }
        }
    }

    db.set_expire(key, new_expire);
    RespValue::integer(1)
}

pub fn cmd_expire(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let secs: i64 = match String::from_utf8_lossy(&ctx.args[2]).parse() {
        Ok(v) => v,
        Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
    };
    if secs < 0 {
        return RespValue::error("ERR invalid expire time in 'expire' command");
    }
    let (nx, xx, gt, lt) = parse_expire_flags(&ctx.args, 3);
    let when = Instant::now() + Duration::from_secs(secs as u64);
    apply_expire(ctx, &key, when, nx, xx, gt, lt)
}

pub fn cmd_pexpire(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let ms: i64 = match String::from_utf8_lossy(&ctx.args[2]).parse() {
        Ok(v) => v,
        Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
    };
    if ms < 0 {
        return RespValue::error("ERR invalid expire time in 'pexpire' command");
    }
    let (nx, xx, gt, lt) = parse_expire_flags(&ctx.args, 3);
    let when = Instant::now() + Duration::from_millis(ms as u64);
    apply_expire(ctx, &key, when, nx, xx, gt, lt)
}

pub fn cmd_expireat(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let ts: i64 = match String::from_utf8_lossy(&ctx.args[2]).parse() {
        Ok(v) => v,
        Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
    };
    if ts < 0 {
        return RespValue::error("ERR invalid expire time in 'expireat' command");
    }
    let (nx, xx, gt, lt) = parse_expire_flags(&ctx.args, 3);
    let now_secs = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
    let delta = ts - now_secs;
    let when = if delta > 0 {
        Instant::now() + Duration::from_secs(delta as u64)
    } else {
        Instant::now()
    };
    apply_expire(ctx, &key, when, nx, xx, gt, lt)
}

pub fn cmd_pexpireat(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let ts_ms: i64 = match String::from_utf8_lossy(&ctx.args[2]).parse() {
        Ok(v) => v,
        Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
    };
    if ts_ms < 0 {
        return RespValue::error("ERR invalid expire time in 'pexpireat' command");
    }
    let (nx, xx, gt, lt) = parse_expire_flags(&ctx.args, 3);
    let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
    let delta = ts_ms - now_ms;
    let when = if delta > 0 {
        Instant::now() + Duration::from_millis(delta as u64)
    } else {
        Instant::now()
    };
    apply_expire(ctx, &key, when, nx, xx, gt, lt)
}

pub fn cmd_ttl(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    match ctx.db().ttl_ms(&key) {
        Some(-2) => RespValue::integer(-2),
        Some(-1) => RespValue::integer(-1),
        Some(ms) => RespValue::integer(ms / 1000), // floor to seconds (Redis behavior)
        None => RespValue::integer(-2),
    }
}

pub fn cmd_pttl(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    match ctx.db().ttl_ms(&key) {
        Some(v) => RespValue::integer(v),
        None => RespValue::integer(-2),
    }
}

pub fn cmd_persist(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    if ctx.db().persist(&key) {
        RespValue::integer(1)
    } else {
        RespValue::integer(0)
    }
}

pub fn cmd_expiretime(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let db = ctx.db();
    if !db.exists(&key) {
        return RespValue::integer(-2);
    }
    match db.get_expire(&key) {
        Some(expire) => {
            let now = Instant::now();
            let now_unix = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
            if expire > now {
                let delta = (expire - now).as_secs();
                RespValue::integer((now_unix + delta) as i64)
            } else {
                RespValue::integer(-2)
            }
        }
        None => RespValue::integer(-1),
    }
}

pub fn cmd_pexpiretime(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let db = ctx.db();
    if !db.exists(&key) {
        return RespValue::integer(-2);
    }
    match db.get_expire(&key) {
        Some(expire) => {
            let now = Instant::now();
            let now_unix_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
            if expire > now {
                let delta_ms = (expire - now).as_millis() as i64;
                RespValue::integer(now_unix_ms + delta_ms)
            } else {
                RespValue::integer(-2)
            }
        }
        None => RespValue::integer(-1),
    }
}

pub fn cmd_type(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let t = ctx.db().key_type(&key);
    RespValue::simple_string(t)
}

pub fn cmd_rename(ctx: &mut CommandContext) -> RespValue {
    let from = ctx.args[1].clone();
    let to = ctx.args[2].clone();
    if ctx.db().rename(&from, to) {
        RespValue::ok()
    } else {
        RespValue::error("ERR no such key")
    }
}

pub fn cmd_renamenx(ctx: &mut CommandContext) -> RespValue {
    let from = ctx.args[1].clone();
    let to = ctx.args[2].clone();
    let db = ctx.db();
    if !db.exists(&from) {
        return RespValue::error("ERR no such key");
    }
    if db.exists(&to) {
        return RespValue::integer(0);
    }
    db.rename(&from, to);
    RespValue::integer(1)
}

pub fn cmd_randomkey(ctx: &mut CommandContext) -> RespValue {
    match ctx.db().random_key() {
        Some(key) => RespValue::bulk_string(key),
        None => RespValue::Null,
    }
}

pub fn cmd_keys(ctx: &mut CommandContext) -> RespValue {
    let pattern = String::from_utf8_lossy(&ctx.args[1]).to_string();
    let keys = ctx.db().keys(&pattern);
    let items: Vec<RespValue> = keys.into_iter().map(RespValue::bulk_string).collect();
    RespValue::array(items)
}

pub fn cmd_object(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 2 {
        return RespValue::wrong_arity("object");
    }
    let subcmd = String::from_utf8_lossy(&ctx.args[1]).to_uppercase();
    match subcmd.as_str() {
        "HELP" => {
            RespValue::array(vec![
                RespValue::simple_string("OBJECT subcommand [arguments]"),
                RespValue::simple_string("ENCODING <key> - Return the encoding of a key."),
                RespValue::simple_string("REFCOUNT <key> - Return the reference count of a key."),
                RespValue::simple_string("IDLETIME <key> - Return the idle time of a key."),
                RespValue::simple_string("FREQ <key> - Return the access frequency of a key."),
                RespValue::simple_string("HELP - Return this help message."),
            ])
        }
        "ENCODING" => {
            if ctx.args.len() < 3 {
                return RespValue::wrong_arity("object|encoding");
            }
            let key = ctx.args[2].clone();
            let db = ctx.db();
            match db.get(&key) {
                Some(obj) => RespValue::bulk_string(Bytes::from(obj.encoding_name())),
                None => RespValue::Null,
            }
        }
        "REFCOUNT" => {
            if ctx.args.len() < 3 {
                return RespValue::wrong_arity("object|refcount");
            }
            let key = ctx.args[2].clone();
            if ctx.db().exists(&key) {
                RespValue::integer(1)
            } else {
                RespValue::Null
            }
        }
        "IDLETIME" => {
            if ctx.args.len() < 3 {
                return RespValue::wrong_arity("object|idletime");
            }
            let key = ctx.args[2].clone();
            if ctx.db().exists(&key) {
                RespValue::integer(0)
            } else {
                RespValue::Null
            }
        }
        "FREQ" => {
            if ctx.args.len() < 3 {
                return RespValue::wrong_arity("object|freq");
            }
            let key = ctx.args[2].clone();
            let db = ctx.db();
            if !db.exists(&key) {
                return RespValue::Null;
            }
            let counter = db.lfu_of(&key).unwrap_or(0);
            RespValue::integer(counter as i64)
        }
        _ => RespValue::error(format!("ERR unknown subcommand or wrong number of arguments for 'object|{}'", subcmd.to_lowercase())),
    }
}

pub fn cmd_copy(ctx: &mut CommandContext) -> RespValue {
    let from = ctx.args[1].clone();
    let to = ctx.args[2].clone();
    let mut replace = false;

    let mut i = 3;
    while i < ctx.args.len() {
        let opt = String::from_utf8_lossy(&ctx.args[i]).to_uppercase();
        match opt.as_str() {
            "REPLACE" => replace = true,
            "DB" => {
                // Cross-DB copy not implemented yet, just skip
                i += 1;
            }
            _ => return RespValue::error("ERR syntax error"),
        }
        i += 1;
    }

    if ctx.db().copy_key(&from, to, replace) {
        RespValue::integer(1)
    } else {
        RespValue::integer(0)
    }
}

pub fn cmd_touch(ctx: &mut CommandContext) -> RespValue {
    let mut count = 0i64;
    let keys: Vec<Bytes> = ctx.args[1..].to_vec();
    for key in &keys {
        if ctx.db().exists(key) {
            count += 1;
        }
    }
    RespValue::integer(count)
}

pub fn cmd_sort(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let db = ctx.db();

    let items: Vec<Bytes> = match db.get(&key) {
        Some(RedisObject::List(list)) => list.iter().cloned().collect(),
        Some(RedisObject::Set(set)) => set.iter().cloned().collect(),
        Some(RedisObject::SortedSet(zset)) => {
            zset.range_by_index(0, -1).into_iter().map(|(m, _)| m).collect()
        }
        Some(RedisObject::Stream(_)) | Some(RedisObject::Json(_)) => return RespValue::wrong_type(),
        Some(_) => return RespValue::wrong_type(),
        None => return RespValue::array(vec![]),
    };

    let mut alpha = false;
    let mut desc = false;
    let mut limit_offset: Option<usize> = None;
    let mut limit_count: Option<usize> = None;
    let mut store_dest: Option<Bytes> = None;

    let mut i = 2;
    while i < ctx.args.len() {
        let opt = String::from_utf8_lossy(&ctx.args[i]).to_uppercase();
        match opt.as_str() {
            "ALPHA" => alpha = true,
            "ASC" => desc = false,
            "DESC" => desc = true,
            "LIMIT" => {
                if i + 2 >= ctx.args.len() {
                    return RespValue::error("ERR syntax error");
                }
                limit_offset = Some(String::from_utf8_lossy(&ctx.args[i + 1]).parse().unwrap_or(0));
                limit_count = Some(String::from_utf8_lossy(&ctx.args[i + 2]).parse().unwrap_or(0));
                i += 2;
            }
            "STORE" => {
                if i + 1 >= ctx.args.len() {
                    return RespValue::error("ERR syntax error");
                }
                store_dest = Some(ctx.args[i + 1].clone());
                i += 1;
            }
            "BY" | "GET" => {
                // Skip patterns (basic implementation)
                i += 1;
            }
            _ => return RespValue::error("ERR syntax error"),
        }
        i += 1;
    }

    let mut sorted = items;
    if alpha {
        sorted.sort_by(|a, b| {
            let sa = String::from_utf8_lossy(a);
            let sb = String::from_utf8_lossy(b);
            sa.cmp(&sb)
        });
    } else {
        // Validate all values are numeric first (Redis behavior)
        for item in &sorted {
            if String::from_utf8_lossy(item).parse::<f64>().is_err() {
                return RespValue::error("ERR One or more scores can't be converted into double");
            }
        }
        sorted.sort_by(|a, b| {
            let fa: f64 = String::from_utf8_lossy(a).parse().unwrap_or(0.0);
            let fb: f64 = String::from_utf8_lossy(b).parse().unwrap_or(0.0);
            fa.partial_cmp(&fb).unwrap_or(std::cmp::Ordering::Equal)
        });
    }

    if desc {
        sorted.reverse();
    }

    if let (Some(offset), Some(count)) = (limit_offset, limit_count) {
        sorted = sorted.into_iter().skip(offset).take(count).collect();
    }

    if let Some(dest) = store_dest {
        let list: std::collections::VecDeque<Bytes> = sorted.into_iter().collect();
        let len = list.len() as i64;
        ctx.db().set(dest, RedisObject::List(list));
        RespValue::integer(len)
    } else {
        let items: Vec<RespValue> = sorted.into_iter().map(RespValue::bulk_string).collect();
        RespValue::array(items)
    }
}

/// MOVE key db - move a key to another database
pub fn cmd_move(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let target_db: usize = match String::from_utf8_lossy(&ctx.args[2]).parse() {
        Ok(v) => v,
        Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
    };

    if target_db >= ctx.store.db_count() {
        return RespValue::error("ERR invalid DB index");
    }

    if target_db == ctx.db_index {
        return RespValue::error("ERR source and destination objects are the same");
    }

    // Capture the expiry before removing from source
    let src_db = ctx.store.db_mut(ctx.db_index);
    let expiry = src_db.get_expire(&key);
    let obj = match src_db.remove(&key) {
        Some(o) => o,
        None => return RespValue::integer(0),
    };

    let dst_db = ctx.store.db_mut(target_db);
    if dst_db.exists(&key) {
        // Key exists in destination -- restore data AND expiry to source
        let src_db = ctx.store.db_mut(ctx.db_index);
        src_db.set(key.clone(), obj);
        if let Some(exp) = expiry {
            src_db.set_expire(&key, exp);
        }
        return RespValue::integer(0);
    }

    dst_db.set(key.clone(), obj);
    // Transfer the expiry to the destination db
    if let Some(exp) = expiry {
        dst_db.set_expire(&key, exp);
    }
    RespValue::integer(1)
}

/// DUMP key - serialize key value (simplified stub)
pub fn cmd_dump(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let db = ctx.db();
    match db.get(&key) {
        Some(obj) => {
            let data = format!("{:?}", obj);
            RespValue::bulk_string(Bytes::from(data))
        }
        None => RespValue::Null,
    }
}

/// RESTORE key ttl serialized-value [REPLACE]
pub fn cmd_restore(_ctx: &mut CommandContext) -> RespValue {
    RespValue::error("ERR DUMP/RESTORE serialization not supported in rCache")
}
