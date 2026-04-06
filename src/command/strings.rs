use bytes::Bytes;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use crate::protocol::RespValue;
use crate::storage::RedisObject;
use super::registry::CommandContext;

/// Helper to parse a value as i64 from a RedisObject::String.
fn parse_int(obj: &RedisObject) -> Result<i64, RespValue> {
    match obj {
        RedisObject::String(b) => {
            let s = std::str::from_utf8(b).map_err(|_| RespValue::error("ERR value is not an integer or out of range"))?;
            s.parse::<i64>().map_err(|_| RespValue::error("ERR value is not an integer or out of range"))
        }
        _ => Err(RespValue::wrong_type()),
    }
}

fn parse_float(obj: &RedisObject) -> Result<f64, RespValue> {
    match obj {
        RedisObject::String(b) => {
            let s = std::str::from_utf8(b).map_err(|_| RespValue::error("ERR value is not a valid float"))?;
            s.parse::<f64>().map_err(|_| RespValue::error("ERR value is not a valid float"))
        }
        _ => Err(RespValue::wrong_type()),
    }
}

fn get_string<'a>(ctx: &'a mut CommandContext, key: &Bytes) -> Result<Option<&'a Bytes>, RespValue> {
    match ctx.db().get(key) {
        Some(RedisObject::String(b)) => Ok(Some(b)),
        Some(_) => Err(RespValue::wrong_type()),
        None => Ok(None),
    }
}

pub fn cmd_set(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let value = ctx.args[2].clone();

    let mut ex: Option<Duration> = None;
    let mut nx = false;
    let mut xx = false;
    let mut keepttl = false;
    let mut get = false;

    let mut i = 3;
    while i < ctx.args.len() {
        let opt = String::from_utf8_lossy(&ctx.args[i]).to_uppercase();
        match opt.as_str() {
            "EX" => {
                i += 1;
                if i >= ctx.args.len() {
                    return RespValue::error("ERR syntax error");
                }
                let secs: u64 = match String::from_utf8_lossy(&ctx.args[i]).parse() {
                    Ok(v) if v > 0 => v,
                    _ => return RespValue::error("ERR invalid expire time in 'set' command"),
                };
                ex = Some(Duration::from_secs(secs));
            }
            "PX" => {
                i += 1;
                if i >= ctx.args.len() {
                    return RespValue::error("ERR syntax error");
                }
                let ms: u64 = match String::from_utf8_lossy(&ctx.args[i]).parse() {
                    Ok(v) if v > 0 => v,
                    _ => return RespValue::error("ERR invalid expire time in 'set' command"),
                };
                ex = Some(Duration::from_millis(ms));
            }
            "EXAT" => {
                i += 1;
                if i >= ctx.args.len() {
                    return RespValue::error("ERR syntax error");
                }
                let ts: u64 = match String::from_utf8_lossy(&ctx.args[i]).parse() {
                    Ok(v) if v > 0 => v,
                    _ => return RespValue::error("ERR invalid expire time in 'set' command"),
                };
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                if ts <= now {
                    return RespValue::error("ERR invalid expire time in 'set' command");
                }
                ex = Some(Duration::from_secs(ts - now));
            }
            "PXAT" => {
                i += 1;
                if i >= ctx.args.len() {
                    return RespValue::error("ERR syntax error");
                }
                let ts_ms: u64 = match String::from_utf8_lossy(&ctx.args[i]).parse() {
                    Ok(v) if v > 0 => v,
                    _ => return RespValue::error("ERR invalid expire time in 'set' command"),
                };
                let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
                if ts_ms <= now_ms {
                    return RespValue::error("ERR invalid expire time in 'set' command");
                }
                ex = Some(Duration::from_millis(ts_ms - now_ms));
            }
            "NX" => nx = true,
            "XX" => xx = true,
            "KEEPTTL" => keepttl = true,
            "GET" => get = true,
            _ => return RespValue::error("ERR syntax error"),
        }
        i += 1;
    }

    if nx && xx {
        return RespValue::error("ERR XX and NX options at the same time are not compatible");
    }

    let db = ctx.db();
    let old_value = if get {
        match db.get(&key) {
            Some(RedisObject::String(b)) => Some(RespValue::bulk_string(b.clone())),
            Some(_) => return RespValue::wrong_type(),
            None => Some(RespValue::Null),
        }
    } else {
        None
    };

    let db = ctx.db();
    if nx && db.exists(&key) {
        return old_value.unwrap_or(RespValue::Null);
    }
    if xx && !db.exists(&key) {
        return old_value.unwrap_or(RespValue::Null);
    }

    let db = ctx.db();
    if keepttl {
        db.set_keep_ttl(key.clone(), RedisObject::String(value));
    } else {
        db.set(key.clone(), RedisObject::String(value));
    }

    if let Some(dur) = ex {
        let db = ctx.db();
        db.set_expire(&key, Instant::now() + dur);
    }

    old_value.unwrap_or(RespValue::ok())
}

pub fn cmd_get(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    match get_string(ctx, &key) {
        Ok(Some(b)) => RespValue::bulk_string(b.clone()),
        Ok(None) => RespValue::Null,
        Err(e) => e,
    }
}

pub fn cmd_setnx(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let value = ctx.args[2].clone();
    let db = ctx.db();
    if db.exists(&key) {
        RespValue::integer(0)
    } else {
        db.set(key, RedisObject::String(value));
        RespValue::integer(1)
    }
}

pub fn cmd_setex(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let secs: u64 = match String::from_utf8_lossy(&ctx.args[2]).parse() {
        Ok(v) if v > 0 => v,
        _ => return RespValue::error("ERR invalid expire time in 'setex' command"),
    };
    let value = ctx.args[3].clone();
    let db = ctx.db();
    db.set(key.clone(), RedisObject::String(value));
    db.set_expire(&key, Instant::now() + Duration::from_secs(secs));
    RespValue::ok()
}

pub fn cmd_psetex(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let ms: u64 = match String::from_utf8_lossy(&ctx.args[2]).parse() {
        Ok(v) if v > 0 => v,
        _ => return RespValue::error("ERR invalid expire time in 'psetex' command"),
    };
    let value = ctx.args[3].clone();
    let db = ctx.db();
    db.set(key.clone(), RedisObject::String(value));
    db.set_expire(&key, Instant::now() + Duration::from_millis(ms));
    RespValue::ok()
}

pub fn cmd_mget(ctx: &mut CommandContext) -> RespValue {
    let keys: Vec<Bytes> = ctx.args[1..].to_vec();
    let mut results = Vec::with_capacity(keys.len());
    for key in &keys {
        let db = ctx.db();
        match db.get(key) {
            Some(RedisObject::String(b)) => results.push(RespValue::bulk_string(b.clone())),
            _ => results.push(RespValue::Null),
        }
    }
    RespValue::array(results)
}

pub fn cmd_mset(ctx: &mut CommandContext) -> RespValue {
    if (ctx.args.len() - 1) % 2 != 0 {
        return RespValue::wrong_arity("mset");
    }
    let pairs: Vec<(Bytes, Bytes)> = ctx.args[1..].chunks(2)
        .map(|c| (c[0].clone(), c[1].clone()))
        .collect();
    for (key, value) in pairs {
        ctx.db().set(key, RedisObject::String(value));
    }
    RespValue::ok()
}

pub fn cmd_msetnx(ctx: &mut CommandContext) -> RespValue {
    if (ctx.args.len() - 1) % 2 != 0 {
        return RespValue::wrong_arity("msetnx");
    }
    // Check if any key exists first
    let pairs: Vec<(Bytes, Bytes)> = ctx.args[1..].chunks(2)
        .map(|c| (c[0].clone(), c[1].clone()))
        .collect();
    for (key, _) in &pairs {
        if ctx.db().exists(key) {
            return RespValue::integer(0);
        }
    }
    for (key, value) in pairs {
        ctx.db().set(key, RedisObject::String(value));
    }
    RespValue::integer(1)
}

pub fn cmd_incr(ctx: &mut CommandContext) -> RespValue {
    incr_by(ctx, 1)
}

pub fn cmd_decr(ctx: &mut CommandContext) -> RespValue {
    incr_by(ctx, -1)
}

pub fn cmd_incrby(ctx: &mut CommandContext) -> RespValue {
    let delta: i64 = match String::from_utf8_lossy(&ctx.args[2]).parse() {
        Ok(v) => v,
        Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
    };
    incr_by(ctx, delta)
}

pub fn cmd_decrby(ctx: &mut CommandContext) -> RespValue {
    let delta: i64 = match String::from_utf8_lossy(&ctx.args[2]).parse() {
        Ok(v) => v,
        Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
    };
    incr_by(ctx, -delta)
}

fn incr_by(ctx: &mut CommandContext, delta: i64) -> RespValue {
    let key = ctx.args[1].clone();
    let db = ctx.db();

    let current = match db.get(&key) {
        Some(obj) => match parse_int(obj) {
            Ok(v) => v,
            Err(e) => return e,
        },
        None => 0,
    };

    let new_val = match current.checked_add(delta) {
        Some(v) => v,
        None => return RespValue::error("ERR increment or decrement would overflow"),
    };

    let db = ctx.db();
    db.set_keep_ttl(key, RedisObject::String(Bytes::from(new_val.to_string())));
    RespValue::integer(new_val)
}

pub fn cmd_incrbyfloat(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let delta: f64 = match String::from_utf8_lossy(&ctx.args[2]).parse() {
        Ok(v) => v,
        Err(_) => return RespValue::error("ERR value is not a valid float"),
    };

    let db = ctx.db();
    let current = match db.get(&key) {
        Some(obj) => match parse_float(obj) {
            Ok(v) => v,
            Err(e) => return e,
        },
        None => 0.0,
    };

    let new_val = current + delta;
    if new_val.is_infinite() || new_val.is_nan() {
        return RespValue::error("ERR increment would produce NaN or Infinity");
    }

    let s = format_float(new_val);
    let db = ctx.db();
    db.set_keep_ttl(key, RedisObject::String(Bytes::from(s.clone())));
    RespValue::bulk_string(Bytes::from(s))
}

fn format_float(f: f64) -> String {
    if f == f.trunc() && f.abs() < 1e17 {
        // Print as integer-like if it's a whole number
        let s = format!("{:.1}", f);
        // But Redis uses up to 17 significant digits
        s
    } else {
        format!("{}", f)
    }
}

pub fn cmd_append(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let append_value = ctx.args[2].clone();
    let db = ctx.db();

    match db.get(&key) {
        Some(RedisObject::String(existing)) => {
            let mut new_val = existing.to_vec();
            new_val.extend_from_slice(&append_value);
            let len = new_val.len() as i64;
            db.set_keep_ttl(key, RedisObject::String(Bytes::from(new_val)));
            RespValue::integer(len)
        }
        Some(_) => RespValue::wrong_type(),
        None => {
            let len = append_value.len() as i64;
            db.set(key, RedisObject::String(append_value));
            RespValue::integer(len)
        }
    }
}

pub fn cmd_strlen(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let db = ctx.db();
    match db.get(&key) {
        Some(RedisObject::String(b)) => RespValue::integer(b.len() as i64),
        Some(_) => RespValue::wrong_type(),
        None => RespValue::integer(0),
    }
}

pub fn cmd_getrange(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let start: i64 = match String::from_utf8_lossy(&ctx.args[2]).parse() {
        Ok(v) => v,
        Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
    };
    let end: i64 = match String::from_utf8_lossy(&ctx.args[3]).parse() {
        Ok(v) => v,
        Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
    };

    let db = ctx.db();
    match db.get(&key) {
        Some(RedisObject::String(b)) => {
            let len = b.len() as i64;
            let s = if start < 0 { (len + start).max(0) } else { start } as usize;
            let e = if end < 0 { (len + end).max(0) } else { end.min(len - 1) } as usize;
            if s > e || s >= b.len() {
                return RespValue::bulk_string(Bytes::new());
            }
            let slice = &b[s..=e.min(b.len() - 1)];
            RespValue::bulk_string(Bytes::from(slice.to_vec()))
        }
        Some(_) => RespValue::wrong_type(),
        None => RespValue::bulk_string(Bytes::new()),
    }
}

pub fn cmd_setrange(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let offset: usize = match String::from_utf8_lossy(&ctx.args[2]).parse() {
        Ok(v) => v,
        Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
    };
    let value = ctx.args[3].clone();

    let db = ctx.db();
    let mut existing = match db.get(&key) {
        Some(RedisObject::String(b)) => b.to_vec(),
        Some(_) => return RespValue::wrong_type(),
        None => Vec::new(),
    };

    let needed = offset + value.len();
    if needed > existing.len() {
        existing.resize(needed, 0);
    }
    existing[offset..offset + value.len()].copy_from_slice(&value);
    let len = existing.len() as i64;
    let db = ctx.db();
    db.set_keep_ttl(key, RedisObject::String(Bytes::from(existing)));
    RespValue::integer(len)
}

pub fn cmd_getdel(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let db = ctx.db();
    match db.get(&key) {
        Some(RedisObject::String(b)) => {
            let result = RespValue::bulk_string(b.clone());
            db.remove(&key);
            result
        }
        Some(_) => RespValue::wrong_type(),
        None => RespValue::Null,
    }
}

pub fn cmd_getex(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();

    let db = ctx.db();
    let result = match db.get(&key) {
        Some(RedisObject::String(b)) => RespValue::bulk_string(b.clone()),
        Some(_) => return RespValue::wrong_type(),
        None => return RespValue::Null,
    };

    if ctx.args.len() > 2 {
        let opt = String::from_utf8_lossy(&ctx.args[2]).to_uppercase();
        match opt.as_str() {
            "EX" => {
                if ctx.args.len() < 4 { return RespValue::error("ERR syntax error"); }
                let secs: u64 = match String::from_utf8_lossy(&ctx.args[3]).parse() {
                    Ok(v) if v > 0 => v,
                    _ => return RespValue::error("ERR invalid expire time in 'getex' command"),
                };
                ctx.db().set_expire(&key, Instant::now() + Duration::from_secs(secs));
            }
            "PX" => {
                if ctx.args.len() < 4 { return RespValue::error("ERR syntax error"); }
                let ms: u64 = match String::from_utf8_lossy(&ctx.args[3]).parse() {
                    Ok(v) if v > 0 => v,
                    _ => return RespValue::error("ERR invalid expire time in 'getex' command"),
                };
                ctx.db().set_expire(&key, Instant::now() + Duration::from_millis(ms));
            }
            "EXAT" => {
                if ctx.args.len() < 4 { return RespValue::error("ERR syntax error"); }
                let ts: u64 = match String::from_utf8_lossy(&ctx.args[3]).parse() {
                    Ok(v) if v > 0 => v,
                    _ => return RespValue::error("ERR invalid expire time in 'getex' command"),
                };
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                if ts > now {
                    ctx.db().set_expire(&key, Instant::now() + Duration::from_secs(ts - now));
                } else {
                    // Timestamp in the past: delete the key immediately
                    ctx.db().remove(&key);
                }
            }
            "PXAT" => {
                if ctx.args.len() < 4 { return RespValue::error("ERR syntax error"); }
                let ts_ms: u64 = match String::from_utf8_lossy(&ctx.args[3]).parse() {
                    Ok(v) if v > 0 => v,
                    _ => return RespValue::error("ERR invalid expire time in 'getex' command"),
                };
                let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
                if ts_ms > now_ms {
                    ctx.db().set_expire(&key, Instant::now() + Duration::from_millis(ts_ms - now_ms));
                } else {
                    // Timestamp in the past: delete the key immediately
                    ctx.db().remove(&key);
                }
            }
            "PERSIST" => {
                ctx.db().persist(&key);
            }
            _ => return RespValue::error("ERR syntax error"),
        }
    }

    result
}

pub fn cmd_getset(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let new_value = ctx.args[2].clone();
    let db = ctx.db();
    let old = match db.get(&key) {
        Some(RedisObject::String(b)) => RespValue::bulk_string(b.clone()),
        Some(_) => return RespValue::wrong_type(),
        None => RespValue::Null,
    };
    let db = ctx.db();
    db.set(key, RedisObject::String(new_value));
    old
}
