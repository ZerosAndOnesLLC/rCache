use bytes::Bytes;
use std::collections::HashSet;
use crate::protocol::RespValue;
use crate::storage::RedisObject;
use super::registry::CommandContext;

fn get_set<'a>(ctx: &'a mut CommandContext, key: &Bytes) -> Result<Option<&'a HashSet<Bytes>>, RespValue> {
    match ctx.db().get(key) {
        Some(RedisObject::Set(s)) => Ok(Some(s)),
        Some(_) => Err(RespValue::wrong_type()),
        None => Ok(None),
    }
}

fn get_set_mut<'a>(ctx: &'a mut CommandContext, key: &Bytes) -> Result<Option<&'a mut HashSet<Bytes>>, RespValue> {
    match ctx.db().get_mut(key) {
        Some(RedisObject::Set(s)) => Ok(Some(s)),
        Some(_) => Err(RespValue::wrong_type()),
        None => Ok(None),
    }
}

fn ensure_set<'a>(ctx: &'a mut CommandContext, key: &Bytes) -> Result<&'a mut HashSet<Bytes>, RespValue> {
    let db = ctx.db();
    if !db.exists(key) {
        db.set(key.clone(), RedisObject::Set(HashSet::new()));
    }
    match db.get_mut(key) {
        Some(RedisObject::Set(s)) => Ok(s),
        Some(_) => Err(RespValue::wrong_type()),
        None => unreachable!(),
    }
}

fn cleanup_empty_set(ctx: &mut CommandContext, key: &Bytes) {
    let db = ctx.db();
    if let Some(RedisObject::Set(s)) = db.get(key) {
        if s.is_empty() {
            db.remove(key);
        }
    }
}

pub fn cmd_sadd(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let members: Vec<Bytes> = ctx.args[2..].to_vec();
    let set = match ensure_set(ctx, &key) {
        Ok(s) => s,
        Err(e) => return e,
    };
    let mut added = 0i64;
    for m in members {
        if set.insert(m) {
            added += 1;
        }
    }
    RespValue::integer(added)
}

pub fn cmd_srem(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let members: Vec<Bytes> = ctx.args[2..].to_vec();
    match get_set_mut(ctx, &key) {
        Ok(Some(set)) => {
            let mut removed = 0i64;
            for m in members {
                if set.remove(&m) {
                    removed += 1;
                }
            }
            cleanup_empty_set(ctx, &key);
            RespValue::integer(removed)
        }
        Ok(None) => RespValue::integer(0),
        Err(e) => e,
    }
}

pub fn cmd_sismember(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let member = ctx.args[2].clone();
    match get_set(ctx, &key) {
        Ok(Some(set)) => RespValue::integer(if set.contains(&member) { 1 } else { 0 }),
        Ok(None) => RespValue::integer(0),
        Err(e) => e,
    }
}

pub fn cmd_smismember(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let members: Vec<Bytes> = ctx.args[2..].to_vec();
    match get_set(ctx, &key) {
        Ok(Some(set)) => {
            let results: Vec<RespValue> = members.iter()
                .map(|m| RespValue::integer(if set.contains(m) { 1 } else { 0 }))
                .collect();
            RespValue::array(results)
        }
        Ok(None) => {
            RespValue::array(members.iter().map(|_| RespValue::integer(0)).collect())
        }
        Err(e) => e,
    }
}

pub fn cmd_smembers(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    match get_set(ctx, &key) {
        Ok(Some(set)) => {
            let items: Vec<RespValue> = set.iter().map(|m| RespValue::bulk_string(m.clone())).collect();
            RespValue::array(items)
        }
        Ok(None) => RespValue::array(vec![]),
        Err(e) => e,
    }
}

pub fn cmd_scard(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    match get_set(ctx, &key) {
        Ok(Some(set)) => RespValue::integer(set.len() as i64),
        Ok(None) => RespValue::integer(0),
        Err(e) => e,
    }
}

pub fn cmd_srandmember(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let count = if ctx.args.len() > 2 {
        match String::from_utf8_lossy(&ctx.args[2]).parse::<i64>() {
            Ok(v) => Some(v),
            Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
        }
    } else {
        None
    };

    match get_set(ctx, &key) {
        Ok(Some(set)) => {
            if set.is_empty() {
                return if count.is_some() { RespValue::array(vec![]) } else { RespValue::Null };
            }

            use rand::seq::IteratorRandom;
            let mut rng = rand::thread_rng();

            match count {
                None => {
                    let member = set.iter().choose(&mut rng).unwrap();
                    RespValue::bulk_string(member.clone())
                }
                Some(n) if n >= 0 => {
                    let n = n as usize;
                    let members: Vec<RespValue> = set.iter()
                        .choose_multiple(&mut rng, n.min(set.len()))
                        .into_iter()
                        .map(|m| RespValue::bulk_string(m.clone()))
                        .collect();
                    RespValue::array(members)
                }
                Some(n) => {
                    // Negative count: allow duplicates
                    let n = (-n) as usize;
                    use rand::seq::SliceRandom;
                    let items: Vec<&Bytes> = set.iter().collect();
                    let members: Vec<RespValue> = (0..n)
                        .map(|_| RespValue::bulk_string((*items.choose(&mut rng).unwrap()).clone()))
                        .collect();
                    RespValue::array(members)
                }
            }
        }
        Ok(None) => if count.is_some() { RespValue::array(vec![]) } else { RespValue::Null },
        Err(e) => e,
    }
}

pub fn cmd_spop(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let count = if ctx.args.len() > 2 {
        match String::from_utf8_lossy(&ctx.args[2]).parse::<usize>() {
            Ok(v) => Some(v),
            Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
        }
    } else {
        None
    };

    match get_set_mut(ctx, &key) {
        Ok(Some(set)) => {
            if set.is_empty() {
                return if count.is_some() { RespValue::array(vec![]) } else { RespValue::Null };
            }

            use rand::seq::IteratorRandom;
            let mut rng = rand::thread_rng();

            match count {
                None => {
                    let member = set.iter().choose(&mut rng).unwrap().clone();
                    set.remove(&member);
                    cleanup_empty_set(ctx, &key);
                    RespValue::bulk_string(member)
                }
                Some(n) => {
                    let to_pop: Vec<Bytes> = set.iter()
                        .choose_multiple(&mut rng, n.min(set.len()))
                        .into_iter()
                        .cloned()
                        .collect();
                    for m in &to_pop {
                        set.remove(m);
                    }
                    cleanup_empty_set(ctx, &key);
                    RespValue::array(to_pop.into_iter().map(RespValue::bulk_string).collect())
                }
            }
        }
        Ok(None) => if count.is_some() { RespValue::array(vec![]) } else { RespValue::Null },
        Err(e) => e,
    }
}

// Helper to collect a set from the db or return an empty set if key doesn't exist.
fn collect_set(ctx: &mut CommandContext, key: &Bytes) -> Result<HashSet<Bytes>, RespValue> {
    match ctx.db().get(key) {
        Some(RedisObject::Set(s)) => Ok(s.clone()),
        Some(_) => Err(RespValue::wrong_type()),
        None => Ok(HashSet::new()),
    }
}

pub fn cmd_sdiff(ctx: &mut CommandContext) -> RespValue {
    let keys: Vec<Bytes> = ctx.args[1..].to_vec();
    let mut result = match collect_set(ctx, &keys[0]) {
        Ok(s) => s,
        Err(e) => return e,
    };
    for key in &keys[1..] {
        let other = match collect_set(ctx, key) {
            Ok(s) => s,
            Err(e) => return e,
        };
        result = result.difference(&other).cloned().collect();
    }
    let items: Vec<RespValue> = result.into_iter().map(RespValue::bulk_string).collect();
    RespValue::array(items)
}

pub fn cmd_sdiffstore(ctx: &mut CommandContext) -> RespValue {
    let dest = ctx.args[1].clone();
    let keys: Vec<Bytes> = ctx.args[2..].to_vec();
    let mut result = match collect_set(ctx, &keys[0]) {
        Ok(s) => s,
        Err(e) => return e,
    };
    for key in &keys[1..] {
        let other = match collect_set(ctx, key) {
            Ok(s) => s,
            Err(e) => return e,
        };
        result = result.difference(&other).cloned().collect();
    }
    let len = result.len() as i64;
    ctx.db().set(dest, RedisObject::Set(result));
    RespValue::integer(len)
}

pub fn cmd_sinter(ctx: &mut CommandContext) -> RespValue {
    let keys: Vec<Bytes> = ctx.args[1..].to_vec();
    let mut result = match collect_set(ctx, &keys[0]) {
        Ok(s) => s,
        Err(e) => return e,
    };
    for key in &keys[1..] {
        let other = match collect_set(ctx, key) {
            Ok(s) => s,
            Err(e) => return e,
        };
        result = result.intersection(&other).cloned().collect();
    }
    let items: Vec<RespValue> = result.into_iter().map(RespValue::bulk_string).collect();
    RespValue::array(items)
}

pub fn cmd_sinterstore(ctx: &mut CommandContext) -> RespValue {
    let dest = ctx.args[1].clone();
    let keys: Vec<Bytes> = ctx.args[2..].to_vec();
    let mut result = match collect_set(ctx, &keys[0]) {
        Ok(s) => s,
        Err(e) => return e,
    };
    for key in &keys[1..] {
        let other = match collect_set(ctx, key) {
            Ok(s) => s,
            Err(e) => return e,
        };
        result = result.intersection(&other).cloned().collect();
    }
    let len = result.len() as i64;
    ctx.db().set(dest, RedisObject::Set(result));
    RespValue::integer(len)
}

pub fn cmd_sintercard(ctx: &mut CommandContext) -> RespValue {
    let numkeys: usize = match String::from_utf8_lossy(&ctx.args[1]).parse() {
        Ok(v) => v,
        Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
    };

    if ctx.args.len() < 2 + numkeys {
        return RespValue::wrong_arity("sintercard");
    }

    let keys: Vec<Bytes> = ctx.args[2..2 + numkeys].to_vec();

    let mut limit = 0usize;
    let mut i = 2 + numkeys;
    while i < ctx.args.len() {
        let opt = String::from_utf8_lossy(&ctx.args[i]).to_uppercase();
        if opt == "LIMIT" {
            i += 1;
            if i < ctx.args.len() {
                limit = String::from_utf8_lossy(&ctx.args[i]).parse().unwrap_or(0);
            }
        }
        i += 1;
    }

    let mut result = match collect_set(ctx, &keys[0]) {
        Ok(s) => s,
        Err(e) => return e,
    };
    for key in &keys[1..] {
        let other = match collect_set(ctx, key) {
            Ok(s) => s,
            Err(e) => return e,
        };
        result = result.intersection(&other).cloned().collect();
    }

    let count = if limit > 0 { result.len().min(limit) } else { result.len() };
    RespValue::integer(count as i64)
}

pub fn cmd_sunion(ctx: &mut CommandContext) -> RespValue {
    let keys: Vec<Bytes> = ctx.args[1..].to_vec();
    let mut result = HashSet::new();
    for key in &keys {
        let other = match collect_set(ctx, key) {
            Ok(s) => s,
            Err(e) => return e,
        };
        result = result.union(&other).cloned().collect();
    }
    let items: Vec<RespValue> = result.into_iter().map(RespValue::bulk_string).collect();
    RespValue::array(items)
}

pub fn cmd_sunionstore(ctx: &mut CommandContext) -> RespValue {
    let dest = ctx.args[1].clone();
    let keys: Vec<Bytes> = ctx.args[2..].to_vec();
    let mut result = HashSet::new();
    for key in &keys {
        let other = match collect_set(ctx, key) {
            Ok(s) => s,
            Err(e) => return e,
        };
        result = result.union(&other).cloned().collect();
    }
    let len = result.len() as i64;
    ctx.db().set(dest, RedisObject::Set(result));
    RespValue::integer(len)
}

pub fn cmd_smove(ctx: &mut CommandContext) -> RespValue {
    let src = ctx.args[1].clone();
    let dst = ctx.args[2].clone();
    let member = ctx.args[3].clone();

    // Remove from source
    let removed = match get_set_mut(ctx, &src) {
        Ok(Some(set)) => set.remove(&member),
        Ok(None) => return RespValue::integer(0),
        Err(e) => return e,
    };

    if !removed {
        return RespValue::integer(0);
    }

    cleanup_empty_set(ctx, &src);

    // Add to destination
    let dest_set = match ensure_set(ctx, &dst) {
        Ok(s) => s,
        Err(e) => return e,
    };
    dest_set.insert(member);

    RespValue::integer(1)
}
