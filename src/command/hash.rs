use bytes::Bytes;
use std::collections::HashMap;
use crate::protocol::RespValue;
use crate::storage::RedisObject;
use super::registry::CommandContext;

fn ensure_hash<'a>(ctx: &'a mut CommandContext, key: &Bytes) -> Result<&'a mut HashMap<Bytes, Bytes>, RespValue> {
    let db = ctx.db();
    if !db.exists(key) {
        db.set(key.clone(), RedisObject::Hash(HashMap::new()));
    }
    match db.get_mut(key) {
        Some(RedisObject::Hash(h)) => Ok(h),
        Some(_) => Err(RespValue::wrong_type()),
        None => unreachable!(),
    }
}

fn cleanup_empty_hash(ctx: &mut CommandContext, key: &Bytes) {
    let db = ctx.db();
    if let Some(RedisObject::Hash(h)) = db.get(key) {
        if h.is_empty() {
            db.remove(key);
        }
    }
}

pub fn cmd_hset(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let pairs: Vec<Bytes> = ctx.args[2..].to_vec();
    if pairs.len() % 2 != 0 {
        return RespValue::wrong_arity("hset");
    }

    let hash = match ensure_hash(ctx, &key) {
        Ok(h) => h,
        Err(e) => return e,
    };

    let mut added = 0i64;
    for chunk in pairs.chunks(2) {
        let field = chunk[0].clone();
        let value = chunk[1].clone();
        if hash.insert(field, value).is_none() {
            added += 1;
        }
    }

    RespValue::integer(added)
}

pub fn cmd_hsetnx(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let field = ctx.args[2].clone();
    let value = ctx.args[3].clone();

    let hash = match ensure_hash(ctx, &key) {
        Ok(h) => h,
        Err(e) => return e,
    };

    if hash.contains_key(&field) {
        RespValue::integer(0)
    } else {
        hash.insert(field, value);
        RespValue::integer(1)
    }
}

pub fn cmd_hget(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let field = ctx.args[2].clone();
    let db = ctx.db();
    match db.get(&key) {
        Some(RedisObject::Hash(h)) => {
            match h.get(&field) {
                Some(v) => RespValue::bulk_string(v.clone()),
                None => RespValue::Null,
            }
        }
        Some(_) => RespValue::wrong_type(),
        None => RespValue::Null,
    }
}

pub fn cmd_hmget(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let fields: Vec<Bytes> = ctx.args[2..].to_vec();
    let db = ctx.db();
    match db.get(&key) {
        Some(RedisObject::Hash(h)) => {
            let results: Vec<RespValue> = fields.iter()
                .map(|f| match h.get(f) {
                    Some(v) => RespValue::bulk_string(v.clone()),
                    None => RespValue::Null,
                })
                .collect();
            RespValue::array(results)
        }
        Some(_) => RespValue::wrong_type(),
        None => RespValue::array(fields.iter().map(|_| RespValue::Null).collect()),
    }
}

pub fn cmd_hdel(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let fields: Vec<Bytes> = ctx.args[2..].to_vec();
    let db = ctx.db();
    match db.get_mut(&key) {
        Some(RedisObject::Hash(h)) => {
            let mut removed = 0i64;
            for f in fields {
                if h.remove(&f).is_some() {
                    removed += 1;
                }
            }
            cleanup_empty_hash(ctx, &key);
            RespValue::integer(removed)
        }
        Some(_) => RespValue::wrong_type(),
        None => RespValue::integer(0),
    }
}

pub fn cmd_hexists(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let field = ctx.args[2].clone();
    let db = ctx.db();
    match db.get(&key) {
        Some(RedisObject::Hash(h)) => RespValue::integer(if h.contains_key(&field) { 1 } else { 0 }),
        Some(_) => RespValue::wrong_type(),
        None => RespValue::integer(0),
    }
}

pub fn cmd_hlen(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let db = ctx.db();
    match db.get(&key) {
        Some(RedisObject::Hash(h)) => RespValue::integer(h.len() as i64),
        Some(_) => RespValue::wrong_type(),
        None => RespValue::integer(0),
    }
}

pub fn cmd_hkeys(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let db = ctx.db();
    match db.get(&key) {
        Some(RedisObject::Hash(h)) => {
            let items: Vec<RespValue> = h.keys().map(|k| RespValue::bulk_string(k.clone())).collect();
            RespValue::array(items)
        }
        Some(_) => RespValue::wrong_type(),
        None => RespValue::array(vec![]),
    }
}

pub fn cmd_hvals(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let db = ctx.db();
    match db.get(&key) {
        Some(RedisObject::Hash(h)) => {
            let items: Vec<RespValue> = h.values().map(|v| RespValue::bulk_string(v.clone())).collect();
            RespValue::array(items)
        }
        Some(_) => RespValue::wrong_type(),
        None => RespValue::array(vec![]),
    }
}

pub fn cmd_hgetall(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let db = ctx.db();
    match db.get(&key) {
        Some(RedisObject::Hash(h)) => {
            let mut items = Vec::with_capacity(h.len() * 2);
            for (k, v) in h {
                items.push(RespValue::bulk_string(k.clone()));
                items.push(RespValue::bulk_string(v.clone()));
            }
            RespValue::array(items)
        }
        Some(_) => RespValue::wrong_type(),
        None => RespValue::array(vec![]),
    }
}

pub fn cmd_hincrby(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let field = ctx.args[2].clone();
    let delta: i64 = match String::from_utf8_lossy(&ctx.args[3]).parse() {
        Ok(v) => v,
        Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
    };

    let hash = match ensure_hash(ctx, &key) {
        Ok(h) => h,
        Err(e) => return e,
    };

    let current: i64 = match hash.get(&field) {
        Some(v) => {
            match std::str::from_utf8(v).ok().and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return RespValue::error("ERR hash value is not an integer"),
            }
        }
        None => 0,
    };

    let new_val = match current.checked_add(delta) {
        Some(v) => v,
        None => return RespValue::error("ERR increment or decrement would overflow"),
    };

    hash.insert(field, Bytes::from(new_val.to_string()));
    RespValue::integer(new_val)
}

pub fn cmd_hincrbyfloat(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let field = ctx.args[2].clone();
    let delta: f64 = match String::from_utf8_lossy(&ctx.args[3]).parse() {
        Ok(v) => v,
        Err(_) => return RespValue::error("ERR value is not a valid float"),
    };

    let hash = match ensure_hash(ctx, &key) {
        Ok(h) => h,
        Err(e) => return e,
    };

    let current: f64 = match hash.get(&field) {
        Some(v) => {
            match std::str::from_utf8(v).ok().and_then(|s| s.parse().ok()) {
                Some(n) => n,
                None => return RespValue::error("ERR hash value is not a valid float"),
            }
        }
        None => 0.0,
    };

    let new_val = current + delta;
    if new_val.is_infinite() || new_val.is_nan() {
        return RespValue::error("ERR increment would produce NaN or Infinity");
    }

    let s = format!("{}", new_val);
    hash.insert(field, Bytes::from(s.clone()));
    RespValue::bulk_string(Bytes::from(s))
}

pub fn cmd_hrandfield(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let count = if ctx.args.len() > 2 {
        match String::from_utf8_lossy(&ctx.args[2]).parse::<i64>() {
            Ok(v) => Some(v),
            Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
        }
    } else {
        None
    };

    let with_values = ctx.args.len() > 3 &&
        String::from_utf8_lossy(&ctx.args[3]).to_uppercase() == "WITHVALUES";

    let db = ctx.db();
    match db.get(&key) {
        Some(RedisObject::Hash(hash)) => {
            if hash.is_empty() {
                return if count.is_some() { RespValue::array(vec![]) } else { RespValue::Null };
            }

            use rand::seq::IteratorRandom;
            let mut rng = rand::thread_rng();

            match count {
                None => {
                    let k = hash.keys().choose(&mut rng).unwrap();
                    RespValue::bulk_string(k.clone())
                }
                Some(n) if n >= 0 => {
                    let n = n as usize;
                    let fields: Vec<(Bytes, Bytes)> = hash.iter()
                        .choose_multiple(&mut rng, n.min(hash.len()))
                        .into_iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                    if with_values {
                        let mut items = Vec::new();
                        for (k, v) in fields {
                            items.push(RespValue::bulk_string(k));
                            items.push(RespValue::bulk_string(v));
                        }
                        RespValue::array(items)
                    } else {
                        RespValue::array(fields.into_iter().map(|(k, _)| RespValue::bulk_string(k)).collect())
                    }
                }
                Some(n) => {
                    let n = (-n) as usize;
                    use rand::seq::SliceRandom;
                    let entries: Vec<(Bytes, Bytes)> = hash.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                    let mut items = Vec::new();
                    for _ in 0..n {
                        let (k, v) = entries.choose(&mut rng).unwrap();
                        if with_values {
                            items.push(RespValue::bulk_string(k.clone()));
                            items.push(RespValue::bulk_string(v.clone()));
                        } else {
                            items.push(RespValue::bulk_string(k.clone()));
                        }
                    }
                    RespValue::array(items)
                }
            }
        }
        Some(_) => RespValue::wrong_type(),
        None => if count.is_some() { RespValue::array(vec![]) } else { RespValue::Null },
    }
}
