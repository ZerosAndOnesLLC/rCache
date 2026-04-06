use bytes::Bytes;
use std::collections::VecDeque;
use crate::protocol::RespValue;
use crate::storage::RedisObject;
use super::registry::CommandContext;

fn get_list<'a>(ctx: &'a mut CommandContext, key: &Bytes) -> Result<Option<&'a mut VecDeque<Bytes>>, RespValue> {
    match ctx.db().get_mut(key) {
        Some(RedisObject::List(list)) => Ok(Some(list)),
        Some(_) => Err(RespValue::wrong_type()),
        None => Ok(None),
    }
}

fn ensure_list<'a>(ctx: &'a mut CommandContext, key: &Bytes) -> Result<&'a mut VecDeque<Bytes>, RespValue> {
    let db = ctx.db();
    if !db.exists(key) {
        db.set(key.clone(), RedisObject::List(VecDeque::new()));
    }
    match db.get_mut(key) {
        Some(RedisObject::List(list)) => Ok(list),
        Some(_) => Err(RespValue::wrong_type()),
        None => unreachable!(),
    }
}

pub fn cmd_lpush(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let values: Vec<Bytes> = ctx.args[2..].to_vec();
    let list = match ensure_list(ctx, &key) {
        Ok(l) => l,
        Err(e) => return e,
    };
    for v in values {
        list.push_front(v);
    }
    RespValue::integer(list.len() as i64)
}

pub fn cmd_rpush(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let values: Vec<Bytes> = ctx.args[2..].to_vec();
    let list = match ensure_list(ctx, &key) {
        Ok(l) => l,
        Err(e) => return e,
    };
    for v in values {
        list.push_back(v);
    }
    RespValue::integer(list.len() as i64)
}

pub fn cmd_lpushx(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let values: Vec<Bytes> = ctx.args[2..].to_vec();
    match get_list(ctx, &key) {
        Ok(Some(list)) => {
            for v in values {
                list.push_front(v);
            }
            RespValue::integer(list.len() as i64)
        }
        Ok(None) => RespValue::integer(0),
        Err(e) => e,
    }
}

pub fn cmd_rpushx(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let values: Vec<Bytes> = ctx.args[2..].to_vec();
    match get_list(ctx, &key) {
        Ok(Some(list)) => {
            for v in values {
                list.push_back(v);
            }
            RespValue::integer(list.len() as i64)
        }
        Ok(None) => RespValue::integer(0),
        Err(e) => e,
    }
}

pub fn cmd_lpop(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let count = if ctx.args.len() > 2 {
        match String::from_utf8_lossy(&ctx.args[2]).parse::<usize>() {
            Ok(v) => Some(v),
            Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
        }
    } else {
        None
    };

    match get_list(ctx, &key) {
        Ok(Some(list)) => {
            if let Some(count) = count {
                let mut results = Vec::new();
                for _ in 0..count {
                    match list.pop_front() {
                        Some(v) => results.push(RespValue::bulk_string(v)),
                        None => break,
                    }
                }
                if results.is_empty() {
                    RespValue::array(vec![])
                } else {
                    cleanup_empty_list(ctx, &key);
                    RespValue::array(results)
                }
            } else {
                match list.pop_front() {
                    Some(v) => {
                        let result = RespValue::bulk_string(v);
                        cleanup_empty_list(ctx, &key);
                        result
                    }
                    None => RespValue::Null,
                }
            }
        }
        Ok(None) => {
            if count.is_some() {
                RespValue::NullArray
            } else {
                RespValue::Null
            }
        }
        Err(e) => e,
    }
}

pub fn cmd_rpop(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let count = if ctx.args.len() > 2 {
        match String::from_utf8_lossy(&ctx.args[2]).parse::<usize>() {
            Ok(v) => Some(v),
            Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
        }
    } else {
        None
    };

    match get_list(ctx, &key) {
        Ok(Some(list)) => {
            if let Some(count) = count {
                let mut results = Vec::new();
                for _ in 0..count {
                    match list.pop_back() {
                        Some(v) => results.push(RespValue::bulk_string(v)),
                        None => break,
                    }
                }
                if results.is_empty() {
                    RespValue::array(vec![])
                } else {
                    cleanup_empty_list(ctx, &key);
                    RespValue::array(results)
                }
            } else {
                match list.pop_back() {
                    Some(v) => {
                        let result = RespValue::bulk_string(v);
                        cleanup_empty_list(ctx, &key);
                        result
                    }
                    None => RespValue::Null,
                }
            }
        }
        Ok(None) => {
            if count.is_some() {
                RespValue::NullArray
            } else {
                RespValue::Null
            }
        }
        Err(e) => e,
    }
}

fn cleanup_empty_list(ctx: &mut CommandContext, key: &Bytes) {
    let db = ctx.db();
    if let Some(RedisObject::List(list)) = db.get(key) {
        if list.is_empty() {
            db.remove(key);
        }
    }
}

pub fn cmd_llen(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let db = ctx.db();
    match db.get(&key) {
        Some(RedisObject::List(list)) => RespValue::integer(list.len() as i64),
        Some(_) => RespValue::wrong_type(),
        None => RespValue::integer(0),
    }
}

pub fn cmd_lindex(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let index: i64 = match String::from_utf8_lossy(&ctx.args[2]).parse() {
        Ok(v) => v,
        Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
    };

    let db = ctx.db();
    match db.get(&key) {
        Some(RedisObject::List(list)) => {
            let len = list.len() as i64;
            let idx = if index < 0 { len + index } else { index };
            if idx < 0 || idx >= len {
                RespValue::Null
            } else {
                RespValue::bulk_string(list[idx as usize].clone())
            }
        }
        Some(_) => RespValue::wrong_type(),
        None => RespValue::Null,
    }
}

pub fn cmd_lrange(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let start: i64 = match String::from_utf8_lossy(&ctx.args[2]).parse() {
        Ok(v) => v,
        Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
    };
    let stop: i64 = match String::from_utf8_lossy(&ctx.args[3]).parse() {
        Ok(v) => v,
        Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
    };

    let db = ctx.db();
    match db.get(&key) {
        Some(RedisObject::List(list)) => {
            let len = list.len() as i64;
            let s = if start < 0 { (len + start).max(0) } else { start.min(len) } as usize;
            let e = if stop < 0 { (len + stop).max(0) } else { stop.min(len - 1) } as usize;
            if s > e {
                return RespValue::array(vec![]);
            }
            let items: Vec<RespValue> = list.iter()
                .skip(s)
                .take(e - s + 1)
                .map(|v| RespValue::bulk_string(v.clone()))
                .collect();
            RespValue::array(items)
        }
        Some(_) => RespValue::wrong_type(),
        None => RespValue::array(vec![]),
    }
}

pub fn cmd_lset(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let index: i64 = match String::from_utf8_lossy(&ctx.args[2]).parse() {
        Ok(v) => v,
        Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
    };
    let value = ctx.args[3].clone();

    match get_list(ctx, &key) {
        Ok(Some(list)) => {
            let len = list.len() as i64;
            let idx = if index < 0 { len + index } else { index };
            if idx < 0 || idx >= len {
                RespValue::error("ERR index out of range")
            } else {
                list[idx as usize] = value;
                RespValue::ok()
            }
        }
        Ok(None) => RespValue::error("ERR no such key"),
        Err(e) => e,
    }
}

pub fn cmd_linsert(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let position = String::from_utf8_lossy(&ctx.args[2]).to_uppercase();
    let pivot = ctx.args[3].clone();
    let value = ctx.args[4].clone();

    let before = match position.as_str() {
        "BEFORE" => true,
        "AFTER" => false,
        _ => return RespValue::error("ERR syntax error"),
    };

    match get_list(ctx, &key) {
        Ok(Some(list)) => {
            if let Some(pos) = list.iter().position(|v| v == &pivot) {
                if before {
                    list.insert(pos, value);
                } else {
                    list.insert(pos + 1, value);
                }
                RespValue::integer(list.len() as i64)
            } else {
                RespValue::integer(-1)
            }
        }
        Ok(None) => RespValue::integer(0),
        Err(e) => e,
    }
}

pub fn cmd_lrem(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let count: i64 = match String::from_utf8_lossy(&ctx.args[2]).parse() {
        Ok(v) => v,
        Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
    };
    let value = ctx.args[3].clone();

    match get_list(ctx, &key) {
        Ok(Some(list)) => {
            let mut removed = 0i64;
            let target = if count > 0 { count } else if count < 0 { -count } else { i64::MAX };

            if count >= 0 {
                // Remove from head
                let mut i = 0;
                while i < list.len() && removed < target {
                    if list[i] == value {
                        list.remove(i);
                        removed += 1;
                    } else {
                        i += 1;
                    }
                }
            } else {
                // Remove from tail
                let mut i = list.len();
                while i > 0 && removed < target {
                    i -= 1;
                    if list[i] == value {
                        list.remove(i);
                        removed += 1;
                    }
                }
            }
            cleanup_empty_list(ctx, &key);
            RespValue::integer(removed)
        }
        Ok(None) => RespValue::integer(0),
        Err(e) => e,
    }
}

pub fn cmd_ltrim(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let start: i64 = match String::from_utf8_lossy(&ctx.args[2]).parse() {
        Ok(v) => v,
        Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
    };
    let stop: i64 = match String::from_utf8_lossy(&ctx.args[3]).parse() {
        Ok(v) => v,
        Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
    };

    match get_list(ctx, &key) {
        Ok(Some(list)) => {
            let len = list.len() as i64;
            let s = if start < 0 { (len + start).max(0) } else { start.min(len) } as usize;
            let e = if stop < 0 { (len + stop).max(0) } else { stop.min(len - 1) } as usize;

            if s > e || s >= list.len() {
                list.clear();
            } else {
                let trimmed: VecDeque<Bytes> = list.drain(s..=e).collect();
                *list = trimmed;
            }
            cleanup_empty_list(ctx, &key);
            RespValue::ok()
        }
        Ok(None) => RespValue::ok(),
        Err(e) => e,
    }
}

pub fn cmd_lpos(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let element = ctx.args[2].clone();

    let mut rank: i64 = 1;
    let mut count: Option<usize> = None;
    let mut maxlen: usize = 0;

    let mut i = 3;
    while i < ctx.args.len() {
        let opt = String::from_utf8_lossy(&ctx.args[i]).to_uppercase();
        match opt.as_str() {
            "RANK" => {
                i += 1;
                if i >= ctx.args.len() { return RespValue::error("ERR syntax error"); }
                rank = match String::from_utf8_lossy(&ctx.args[i]).parse() {
                    Ok(v) => v,
                    Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
                };
                if rank == 0 { return RespValue::error("ERR RANK can't be zero"); }
            }
            "COUNT" => {
                i += 1;
                if i >= ctx.args.len() { return RespValue::error("ERR syntax error"); }
                count = Some(match String::from_utf8_lossy(&ctx.args[i]).parse() {
                    Ok(v) => v,
                    Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
                });
            }
            "MAXLEN" => {
                i += 1;
                if i >= ctx.args.len() { return RespValue::error("ERR syntax error"); }
                maxlen = match String::from_utf8_lossy(&ctx.args[i]).parse() {
                    Ok(v) => v,
                    Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
                };
            }
            _ => return RespValue::error("ERR syntax error"),
        }
        i += 1;
    }

    let db = ctx.db();
    match db.get(&key) {
        Some(RedisObject::List(list)) => {
            let want_count = count.unwrap_or(if rank.abs() > 0 { 1 } else { 0 });
            let scan_max = if maxlen > 0 { maxlen } else { list.len() };
            let mut matches = Vec::new();

            if rank > 0 {
                let mut skip = (rank - 1) as usize;
                for (idx, val) in list.iter().enumerate().take(scan_max) {
                    if val == &element {
                        if skip > 0 {
                            skip -= 1;
                        } else {
                            matches.push(idx as i64);
                            if count.is_none() || matches.len() >= want_count {
                                break;
                            }
                        }
                    }
                }
            } else {
                let mut skip = (-rank - 1) as usize;
                for (idx, val) in list.iter().enumerate().rev().take(scan_max) {
                    if val == &element {
                        if skip > 0 {
                            skip -= 1;
                        } else {
                            matches.push(idx as i64);
                            if count.is_none() || matches.len() >= want_count {
                                break;
                            }
                        }
                    }
                }
            }

            if count.is_some() {
                RespValue::array(matches.into_iter().map(RespValue::integer).collect())
            } else {
                matches.first().map(|&v| RespValue::integer(v)).unwrap_or(RespValue::Null)
            }
        }
        Some(_) => RespValue::wrong_type(),
        None => {
            if count.is_some() {
                RespValue::array(vec![])
            } else {
                RespValue::Null
            }
        }
    }
}

pub fn cmd_lmove(ctx: &mut CommandContext) -> RespValue {
    let src = ctx.args[1].clone();
    let dst = ctx.args[2].clone();
    let wherefrom = String::from_utf8_lossy(&ctx.args[3]).to_uppercase();
    let whereto = String::from_utf8_lossy(&ctx.args[4]).to_uppercase();

    // Pop from source
    let db = ctx.db();
    let value = match db.get_mut(&src) {
        Some(RedisObject::List(list)) => {
            match wherefrom.as_str() {
                "LEFT" => list.pop_front(),
                "RIGHT" => list.pop_back(),
                _ => return RespValue::error("ERR syntax error"),
            }
        }
        Some(_) => return RespValue::wrong_type(),
        None => return RespValue::Null,
    };

    let value = match value {
        Some(v) => v,
        None => return RespValue::Null,
    };

    // Clean up empty source
    cleanup_empty_list(ctx, &src);

    // Push to destination
    let list = match ensure_list(ctx, &dst) {
        Ok(l) => l,
        Err(e) => return e,
    };

    match whereto.as_str() {
        "LEFT" => list.push_front(value.clone()),
        "RIGHT" => list.push_back(value.clone()),
        _ => return RespValue::error("ERR syntax error"),
    }

    RespValue::bulk_string(value)
}

pub fn cmd_lmpop(ctx: &mut CommandContext) -> RespValue {
    let numkeys: usize = match String::from_utf8_lossy(&ctx.args[1]).parse() {
        Ok(v) => v,
        Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
    };

    if ctx.args.len() < 2 + numkeys + 1 {
        return RespValue::wrong_arity("lmpop");
    }

    let keys: Vec<Bytes> = ctx.args[2..2 + numkeys].to_vec();
    let direction = String::from_utf8_lossy(&ctx.args[2 + numkeys]).to_uppercase();

    let mut count = 1usize;
    let mut i = 3 + numkeys;
    while i < ctx.args.len() {
        let opt = String::from_utf8_lossy(&ctx.args[i]).to_uppercase();
        if opt == "COUNT" {
            i += 1;
            if i < ctx.args.len() {
                count = String::from_utf8_lossy(&ctx.args[i]).parse().unwrap_or(1);
            }
        }
        i += 1;
    }

    for key in &keys {
        let db = ctx.db();
        if let Some(RedisObject::List(list)) = db.get_mut(key) {
            if list.is_empty() {
                continue;
            }
            let mut results = Vec::new();
            for _ in 0..count {
                let val = match direction.as_str() {
                    "LEFT" => list.pop_front(),
                    "RIGHT" => list.pop_back(),
                    _ => return RespValue::error("ERR syntax error"),
                };
                match val {
                    Some(v) => results.push(RespValue::bulk_string(v)),
                    None => break,
                }
            }
            cleanup_empty_list(ctx, key);
            return RespValue::array(vec![
                RespValue::bulk_string(key.clone()),
                RespValue::array(results),
            ]);
        }
    }

    RespValue::NullArray
}

// === Blocking list stubs (non-blocking immediate check) ===

pub fn cmd_blpop(ctx: &mut CommandContext) -> RespValue {
    // Non-blocking stub: check keys immediately, ignore timeout
    let keys: Vec<Bytes> = ctx.args[1..ctx.args.len() - 1].to_vec();

    for key in &keys {
        let db = ctx.db();
        if let Some(RedisObject::List(list)) = db.get_mut(key) {
            if let Some(val) = list.pop_front() {
                let key_clone = key.clone();
                cleanup_empty_list(ctx, &key_clone);
                return RespValue::array(vec![
                    RespValue::bulk_string(key_clone),
                    RespValue::bulk_string(val),
                ]);
            }
        }
    }

    RespValue::NullArray
}

pub fn cmd_brpop(ctx: &mut CommandContext) -> RespValue {
    // Non-blocking stub: check keys immediately, ignore timeout
    let keys: Vec<Bytes> = ctx.args[1..ctx.args.len() - 1].to_vec();

    for key in &keys {
        let db = ctx.db();
        if let Some(RedisObject::List(list)) = db.get_mut(key) {
            if let Some(val) = list.pop_back() {
                let key_clone = key.clone();
                cleanup_empty_list(ctx, &key_clone);
                return RespValue::array(vec![
                    RespValue::bulk_string(key_clone),
                    RespValue::bulk_string(val),
                ]);
            }
        }
    }

    RespValue::NullArray
}

pub fn cmd_blmove(ctx: &mut CommandContext) -> RespValue {
    // Non-blocking stub: BLMOVE src dst LEFT|RIGHT LEFT|RIGHT timeout
    // Ignore timeout, try immediately
    let src = ctx.args[1].clone();
    let dst = ctx.args[2].clone();
    let wherefrom = String::from_utf8_lossy(&ctx.args[3]).to_uppercase();
    let whereto = String::from_utf8_lossy(&ctx.args[4]).to_uppercase();

    let db = ctx.db();
    let value = match db.get_mut(&src) {
        Some(RedisObject::List(list)) => {
            match wherefrom.as_str() {
                "LEFT" => list.pop_front(),
                "RIGHT" => list.pop_back(),
                _ => return RespValue::error("ERR syntax error"),
            }
        }
        Some(_) => return RespValue::wrong_type(),
        None => return RespValue::Null,
    };

    let value = match value {
        Some(v) => v,
        None => return RespValue::Null,
    };

    cleanup_empty_list(ctx, &src);

    let list = match ensure_list(ctx, &dst) {
        Ok(l) => l,
        Err(e) => return e,
    };

    match whereto.as_str() {
        "LEFT" => list.push_front(value.clone()),
        "RIGHT" => list.push_back(value.clone()),
        _ => return RespValue::error("ERR syntax error"),
    }

    RespValue::bulk_string(value)
}

pub fn cmd_blmpop(ctx: &mut CommandContext) -> RespValue {
    // Non-blocking stub: BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT count]
    // Skip timeout (first arg after command name), then delegate to lmpop logic
    if ctx.args.len() < 4 {
        return RespValue::wrong_arity("blmpop");
    }

    let numkeys: usize = match String::from_utf8_lossy(&ctx.args[2]).parse() {
        Ok(v) => v,
        Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
    };

    if ctx.args.len() < 3 + numkeys + 1 {
        return RespValue::wrong_arity("blmpop");
    }

    let keys: Vec<Bytes> = ctx.args[3..3 + numkeys].to_vec();
    let direction = String::from_utf8_lossy(&ctx.args[3 + numkeys]).to_uppercase();

    let mut count = 1usize;
    let mut i = 4 + numkeys;
    while i < ctx.args.len() {
        let opt = String::from_utf8_lossy(&ctx.args[i]).to_uppercase();
        if opt == "COUNT" {
            i += 1;
            if i < ctx.args.len() {
                count = String::from_utf8_lossy(&ctx.args[i]).parse().unwrap_or(1);
            }
        }
        i += 1;
    }

    for key in &keys {
        let db = ctx.db();
        if let Some(RedisObject::List(list)) = db.get_mut(key) {
            if list.is_empty() {
                continue;
            }
            let mut results = Vec::new();
            for _ in 0..count {
                let val = match direction.as_str() {
                    "LEFT" => list.pop_front(),
                    "RIGHT" => list.pop_back(),
                    _ => return RespValue::error("ERR syntax error"),
                };
                match val {
                    Some(v) => results.push(RespValue::bulk_string(v)),
                    None => break,
                }
            }
            cleanup_empty_list(ctx, key);
            return RespValue::array(vec![
                RespValue::bulk_string(key.clone()),
                RespValue::array(results),
            ]);
        }
    }

    RespValue::NullArray
}
