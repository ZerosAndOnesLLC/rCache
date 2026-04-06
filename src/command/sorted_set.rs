use bytes::Bytes;
use crate::protocol::RespValue;
use crate::storage::types::SortedSetData;
use crate::storage::RedisObject;
use super::registry::CommandContext;

fn get_zset<'a>(ctx: &'a mut CommandContext, key: &Bytes) -> Result<Option<&'a SortedSetData>, RespValue> {
    match ctx.db().get(key) {
        Some(RedisObject::SortedSet(z)) => Ok(Some(z)),
        Some(_) => Err(RespValue::wrong_type()),
        None => Ok(None),
    }
}

fn get_zset_mut<'a>(ctx: &'a mut CommandContext, key: &Bytes) -> Result<Option<&'a mut SortedSetData>, RespValue> {
    match ctx.db().get_mut(key) {
        Some(RedisObject::SortedSet(z)) => Ok(Some(z)),
        Some(_) => Err(RespValue::wrong_type()),
        None => Ok(None),
    }
}

fn ensure_zset<'a>(ctx: &'a mut CommandContext, key: &Bytes) -> Result<&'a mut SortedSetData, RespValue> {
    let db = ctx.db();
    if !db.exists(key) {
        db.set(key.clone(), RedisObject::SortedSet(SortedSetData::new()));
    }
    match db.get_mut(key) {
        Some(RedisObject::SortedSet(z)) => Ok(z),
        Some(_) => Err(RespValue::wrong_type()),
        None => unreachable!(),
    }
}

fn cleanup_empty_zset(ctx: &mut CommandContext, key: &Bytes) {
    let db = ctx.db();
    if let Some(RedisObject::SortedSet(z)) = db.get(key) {
        if z.is_empty() {
            db.remove(key);
        }
    }
}

fn collect_zset(ctx: &mut CommandContext, key: &Bytes) -> Result<SortedSetData, RespValue> {
    match ctx.db().get(key) {
        Some(RedisObject::SortedSet(z)) => Ok(z.clone()),
        Some(_) => Err(RespValue::wrong_type()),
        None => Ok(SortedSetData::new()),
    }
}

fn parse_score_bound(s: &str) -> Option<(f64, bool)> {
    if s == "-inf" {
        Some((f64::NEG_INFINITY, true))
    } else if s == "+inf" || s == "inf" {
        Some((f64::INFINITY, true))
    } else if let Some(rest) = s.strip_prefix('(') {
        rest.parse::<f64>().ok().map(|v| (v, false))
    } else {
        s.parse::<f64>().ok().map(|v| (v, true))
    }
}

pub fn cmd_zadd(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();

    let mut nx = false;
    let mut xx = false;
    let mut gt = false;
    let mut lt = false;
    let mut ch = false;

    let mut i = 2;
    while i < ctx.args.len() {
        let opt = String::from_utf8_lossy(&ctx.args[i]).to_uppercase();
        match opt.as_str() {
            "NX" => { nx = true; i += 1; }
            "XX" => { xx = true; i += 1; }
            "GT" => { gt = true; i += 1; }
            "LT" => { lt = true; i += 1; }
            "CH" => { ch = true; i += 1; }
            _ => break,
        }
    }

    if nx && xx {
        return RespValue::error("ERR XX and NX options at the same time are not compatible");
    }

    if (ctx.args.len() - i) % 2 != 0 {
        return RespValue::wrong_arity("zadd");
    }

    let pairs: Vec<(f64, Bytes)> = ctx.args[i..].chunks(2)
        .map(|c| {
            let score: f64 = match String::from_utf8_lossy(&c[0]).parse() {
                Ok(v) => v,
                Err(_) => return Err(RespValue::error("ERR value is not a valid float")),
            };
            Ok((score, c[1].clone()))
        })
        .collect::<Result<Vec<_>, _>>()
        .unwrap_or_else(|_| vec![]);

    if pairs.is_empty() && (ctx.args.len() - i) > 0 {
        return RespValue::error("ERR value is not a valid float");
    }

    let zset = match ensure_zset(ctx, &key) {
        Ok(z) => z,
        Err(e) => return e,
    };

    let mut added = 0i64;
    let mut changed = 0i64;

    for (score, member) in pairs {
        let existing_score = zset.score(&member);

        if nx && existing_score.is_some() {
            continue;
        }
        if xx && existing_score.is_none() {
            continue;
        }

        if let Some(old_score) = existing_score {
            let should_update = if gt && lt {
                false
            } else if gt {
                score > old_score
            } else if lt {
                score < old_score
            } else {
                true
            };

            if should_update && score != old_score {
                zset.insert(member, score);
                changed += 1;
            }
        } else {
            zset.insert(member, score);
            added += 1;
            changed += 1;
        }
    }

    if ch { RespValue::integer(changed) } else { RespValue::integer(added) }
}

pub fn cmd_zrem(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let members: Vec<Bytes> = ctx.args[2..].to_vec();
    match get_zset_mut(ctx, &key) {
        Ok(Some(zset)) => {
            let mut removed = 0i64;
            for m in members {
                if zset.remove(&m) {
                    removed += 1;
                }
            }
            cleanup_empty_zset(ctx, &key);
            RespValue::integer(removed)
        }
        Ok(None) => RespValue::integer(0),
        Err(e) => e,
    }
}

pub fn cmd_zscore(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let member = ctx.args[2].clone();
    match get_zset(ctx, &key) {
        Ok(Some(zset)) => match zset.score(&member) {
            Some(s) => RespValue::bulk_string(Bytes::from(format!("{}", s))),
            None => RespValue::Null,
        },
        Ok(None) => RespValue::Null,
        Err(e) => e,
    }
}

pub fn cmd_zmscore(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let members: Vec<Bytes> = ctx.args[2..].to_vec();
    match get_zset(ctx, &key) {
        Ok(Some(zset)) => {
            let results: Vec<RespValue> = members.iter()
                .map(|m| match zset.score(m) {
                    Some(s) => RespValue::bulk_string(Bytes::from(format!("{}", s))),
                    None => RespValue::Null,
                })
                .collect();
            RespValue::array(results)
        }
        Ok(None) => RespValue::array(members.iter().map(|_| RespValue::Null).collect()),
        Err(e) => e,
    }
}

pub fn cmd_zincrby(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let delta: f64 = match String::from_utf8_lossy(&ctx.args[2]).parse() {
        Ok(v) => v,
        Err(_) => return RespValue::error("ERR value is not a valid float"),
    };
    let member = ctx.args[3].clone();

    let zset = match ensure_zset(ctx, &key) {
        Ok(z) => z,
        Err(e) => return e,
    };

    let new_score = zset.score(&member).unwrap_or(0.0) + delta;
    if new_score.is_nan() || new_score.is_infinite() {
        return RespValue::error("ERR increment would produce NaN or Infinity");
    }
    zset.insert(member, new_score);
    RespValue::bulk_string(Bytes::from(format!("{}", new_score)))
}

pub fn cmd_zcard(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    match get_zset(ctx, &key) {
        Ok(Some(zset)) => RespValue::integer(zset.len() as i64),
        Ok(None) => RespValue::integer(0),
        Err(e) => e,
    }
}

pub fn cmd_zcount(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let min_str = String::from_utf8_lossy(&ctx.args[2]).to_string();
    let max_str = String::from_utf8_lossy(&ctx.args[3]).to_string();

    let (min, min_inclusive) = match parse_score_bound(&min_str) {
        Some(v) => v,
        None => return RespValue::error("ERR min or max is not a float"),
    };
    let (max, max_inclusive) = match parse_score_bound(&max_str) {
        Some(v) => v,
        None => return RespValue::error("ERR min or max is not a float"),
    };

    match get_zset(ctx, &key) {
        Ok(Some(zset)) => {
            let count = zset.range_by_score_bounded(min, min_inclusive, max, max_inclusive).len();
            RespValue::integer(count as i64)
        }
        Ok(None) => RespValue::integer(0),
        Err(e) => e,
    }
}

pub fn cmd_zrange(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let min_arg = String::from_utf8_lossy(&ctx.args[2]).to_string();
    let max_arg = String::from_utf8_lossy(&ctx.args[3]).to_string();

    let mut byscore = false;
    let mut bylex = false;
    let mut rev = false;
    let mut withscores = false;
    let mut limit_offset: Option<usize> = None;
    let mut limit_count: Option<usize> = None;

    let mut i = 4;
    while i < ctx.args.len() {
        let opt = String::from_utf8_lossy(&ctx.args[i]).to_uppercase();
        match opt.as_str() {
            "BYSCORE" => byscore = true,
            "BYLEX" => bylex = true,
            "REV" => rev = true,
            "WITHSCORES" => withscores = true,
            "LIMIT" => {
                i += 1;
                if i + 1 >= ctx.args.len() { return RespValue::error("ERR syntax error"); }
                limit_offset = Some(String::from_utf8_lossy(&ctx.args[i]).parse().unwrap_or(0));
                i += 1;
                limit_count = Some(String::from_utf8_lossy(&ctx.args[i]).parse().unwrap_or(0));
            }
            _ => return RespValue::error("ERR syntax error"),
        }
        i += 1;
    }

    match get_zset(ctx, &key) {
        Ok(Some(zset)) => {
            let mut items: Vec<(Bytes, f64)> = if byscore {
                let (min, min_incl) = match parse_score_bound(&min_arg) {
                    Some(v) => v,
                    None => return RespValue::error("ERR min or max is not a float"),
                };
                let (max, max_incl) = match parse_score_bound(&max_arg) {
                    Some(v) => v,
                    None => return RespValue::error("ERR min or max is not a float"),
                };
                zset.range_by_score_bounded(min, min_incl, max, max_incl)
            } else if bylex {
                // Simplified lex range: return all members
                zset.range_by_index(0, -1)
            } else {
                let start: i64 = min_arg.parse().unwrap_or(0);
                let stop: i64 = max_arg.parse().unwrap_or(-1);
                zset.range_by_index(start, stop)
            };

            if rev {
                items.reverse();
            }

            if let (Some(offset), Some(count)) = (limit_offset, limit_count) {
                items = items.into_iter().skip(offset).take(count).collect();
            }

            format_zset_response(items, withscores)
        }
        Ok(None) => RespValue::array(vec![]),
        Err(e) => e,
    }
}

pub fn cmd_zrangebyscore(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let min_str = String::from_utf8_lossy(&ctx.args[2]).to_string();
    let max_str = String::from_utf8_lossy(&ctx.args[3]).to_string();

    let (min, min_incl) = match parse_score_bound(&min_str) {
        Some(v) => v,
        None => return RespValue::error("ERR min or max is not a float"),
    };
    let (max, max_incl) = match parse_score_bound(&max_str) {
        Some(v) => v,
        None => return RespValue::error("ERR min or max is not a float"),
    };

    let mut withscores = false;
    let mut limit_offset: Option<usize> = None;
    let mut limit_count: Option<usize> = None;

    let mut i = 4;
    while i < ctx.args.len() {
        let opt = String::from_utf8_lossy(&ctx.args[i]).to_uppercase();
        match opt.as_str() {
            "WITHSCORES" => withscores = true,
            "LIMIT" => {
                i += 1;
                if i + 1 >= ctx.args.len() { return RespValue::error("ERR syntax error"); }
                limit_offset = Some(String::from_utf8_lossy(&ctx.args[i]).parse().unwrap_or(0));
                i += 1;
                limit_count = Some(String::from_utf8_lossy(&ctx.args[i]).parse().unwrap_or(0));
            }
            _ => {}
        }
        i += 1;
    }

    match get_zset(ctx, &key) {
        Ok(Some(zset)) => {
            let mut items = zset.range_by_score_bounded(min, min_incl, max, max_incl);
            if let (Some(offset), Some(count)) = (limit_offset, limit_count) {
                items = items.into_iter().skip(offset).take(count).collect();
            }
            format_zset_response(items, withscores)
        }
        Ok(None) => RespValue::array(vec![]),
        Err(e) => e,
    }
}

pub fn cmd_zrevrange(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let start: i64 = match String::from_utf8_lossy(&ctx.args[2]).parse() {
        Ok(v) => v,
        Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
    };
    let stop: i64 = match String::from_utf8_lossy(&ctx.args[3]).parse() {
        Ok(v) => v,
        Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
    };

    let withscores = ctx.args.len() > 4 && String::from_utf8_lossy(&ctx.args[4]).to_uppercase() == "WITHSCORES";

    match get_zset(ctx, &key) {
        Ok(Some(zset)) => {
            // ZREVRANGE uses reverse indices: 0 = highest score, 1 = second highest, etc.
            let len = zset.len() as i64;
            let fwd_start = if start < 0 { (len + start).max(0) } else { start.min(len) };
            let fwd_stop = if stop < 0 { (len + stop).max(0) } else { stop.min(len - 1) };
            // Convert reverse indices to forward: rev_idx 0 -> fwd_idx (len-1)
            let real_start = len - 1 - fwd_stop;
            let real_stop = len - 1 - fwd_start;
            let mut items = zset.range_by_index(real_start, real_stop);
            items.reverse();
            format_zset_response(items, withscores)
        }
        Ok(None) => RespValue::array(vec![]),
        Err(e) => e,
    }
}

pub fn cmd_zrevrangebyscore(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let max_str = String::from_utf8_lossy(&ctx.args[2]).to_string();
    let min_str = String::from_utf8_lossy(&ctx.args[3]).to_string();

    let (min, min_incl) = match parse_score_bound(&min_str) {
        Some(v) => v,
        None => return RespValue::error("ERR min or max is not a float"),
    };
    let (max, max_incl) = match parse_score_bound(&max_str) {
        Some(v) => v,
        None => return RespValue::error("ERR min or max is not a float"),
    };

    let mut withscores = false;
    let mut limit_offset: Option<usize> = None;
    let mut limit_count: Option<usize> = None;

    let mut i = 4;
    while i < ctx.args.len() {
        let opt = String::from_utf8_lossy(&ctx.args[i]).to_uppercase();
        match opt.as_str() {
            "WITHSCORES" => withscores = true,
            "LIMIT" => {
                i += 1;
                if i + 1 >= ctx.args.len() { return RespValue::error("ERR syntax error"); }
                limit_offset = Some(String::from_utf8_lossy(&ctx.args[i]).parse().unwrap_or(0));
                i += 1;
                limit_count = Some(String::from_utf8_lossy(&ctx.args[i]).parse().unwrap_or(0));
            }
            _ => {}
        }
        i += 1;
    }

    match get_zset(ctx, &key) {
        Ok(Some(zset)) => {
            let mut items = zset.range_by_score_bounded(min, min_incl, max, max_incl);
            items.reverse();
            if let (Some(offset), Some(count)) = (limit_offset, limit_count) {
                items = items.into_iter().skip(offset).take(count).collect();
            }
            format_zset_response(items, withscores)
        }
        Ok(None) => RespValue::array(vec![]),
        Err(e) => e,
    }
}

pub fn cmd_zrank(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let member = ctx.args[2].clone();
    match get_zset(ctx, &key) {
        Ok(Some(zset)) => match zset.rank(&member) {
            Some(r) => RespValue::integer(r as i64),
            None => RespValue::Null,
        },
        Ok(None) => RespValue::Null,
        Err(e) => e,
    }
}

pub fn cmd_zrevrank(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let member = ctx.args[2].clone();
    match get_zset(ctx, &key) {
        Ok(Some(zset)) => match zset.rev_rank(&member) {
            Some(r) => RespValue::integer(r as i64),
            None => RespValue::Null,
        },
        Ok(None) => RespValue::Null,
        Err(e) => e,
    }
}

pub fn cmd_zpopmin(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let count: usize = if ctx.args.len() > 2 {
        match String::from_utf8_lossy(&ctx.args[2]).parse() {
            Ok(v) => v,
            Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
        }
    } else {
        1
    };

    match get_zset_mut(ctx, &key) {
        Ok(Some(zset)) => {
            let mut items = Vec::new();
            for _ in 0..count {
                match zset.pop_min() {
                    Some((member, score)) => {
                        items.push(RespValue::bulk_string(member));
                        items.push(RespValue::bulk_string(Bytes::from(format!("{}", score))));
                    }
                    None => break,
                }
            }
            cleanup_empty_zset(ctx, &key);
            RespValue::array(items)
        }
        Ok(None) => RespValue::array(vec![]),
        Err(e) => e,
    }
}

pub fn cmd_zpopmax(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let count: usize = if ctx.args.len() > 2 {
        match String::from_utf8_lossy(&ctx.args[2]).parse() {
            Ok(v) => v,
            Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
        }
    } else {
        1
    };

    match get_zset_mut(ctx, &key) {
        Ok(Some(zset)) => {
            let mut items = Vec::new();
            for _ in 0..count {
                match zset.pop_max() {
                    Some((member, score)) => {
                        items.push(RespValue::bulk_string(member));
                        items.push(RespValue::bulk_string(Bytes::from(format!("{}", score))));
                    }
                    None => break,
                }
            }
            cleanup_empty_zset(ctx, &key);
            RespValue::array(items)
        }
        Ok(None) => RespValue::array(vec![]),
        Err(e) => e,
    }
}

pub fn cmd_zrandmember(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let count = if ctx.args.len() > 2 {
        match String::from_utf8_lossy(&ctx.args[2]).parse::<i64>() {
            Ok(v) => Some(v),
            Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
        }
    } else {
        None
    };

    let withscores = ctx.args.len() > 3 &&
        String::from_utf8_lossy(&ctx.args[3]).to_uppercase() == "WITHSCORES";

    let db = ctx.db();
    match db.get(&key) {
        Some(RedisObject::SortedSet(zset)) => {
            if zset.is_empty() {
                return if count.is_some() { RespValue::array(vec![]) } else { RespValue::Null };
            }

            use rand::seq::IteratorRandom;
            let mut rng = rand::thread_rng();

            match count {
                None => {
                    let (member, _) = zset.random_member().unwrap();
                    RespValue::bulk_string(member.clone())
                }
                Some(n) if n >= 0 => {
                    let n = n as usize;
                    let all: Vec<(Bytes, f64)> = zset.range_by_index(0, -1);
                    let selected: Vec<(Bytes, f64)> = all.iter()
                        .choose_multiple(&mut rng, n.min(all.len()))
                        .into_iter()
                        .cloned()
                        .collect();
                    if withscores {
                        let mut items = Vec::new();
                        for (m, s) in &selected {
                            items.push(RespValue::bulk_string(m.clone()));
                            items.push(RespValue::bulk_string(Bytes::from(format!("{}", s))));
                        }
                        RespValue::array(items)
                    } else {
                        RespValue::array(selected.into_iter().map(|(m, _)| RespValue::bulk_string(m)).collect())
                    }
                }
                Some(n) => {
                    let n = (-n) as usize;
                    use rand::seq::SliceRandom;
                    let all: Vec<(Bytes, f64)> = zset.range_by_index(0, -1);
                    let mut items = Vec::new();
                    for _ in 0..n {
                        let (m, s) = all.choose(&mut rng).unwrap();
                        if withscores {
                            items.push(RespValue::bulk_string(m.clone()));
                            items.push(RespValue::bulk_string(Bytes::from(format!("{}", s))));
                        } else {
                            items.push(RespValue::bulk_string(m.clone()));
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

pub fn cmd_zunionstore(ctx: &mut CommandContext) -> RespValue {
    zstore_op(ctx, SetOp::Union)
}

pub fn cmd_zinterstore(ctx: &mut CommandContext) -> RespValue {
    zstore_op(ctx, SetOp::Inter)
}

pub fn cmd_zdiffstore(ctx: &mut CommandContext) -> RespValue {
    zstore_op(ctx, SetOp::Diff)
}

enum SetOp { Union, Inter, Diff }

fn zstore_op(ctx: &mut CommandContext, op: SetOp) -> RespValue {
    let dest = ctx.args[1].clone();
    let numkeys: usize = match String::from_utf8_lossy(&ctx.args[2]).parse() {
        Ok(v) => v,
        Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
    };

    if ctx.args.len() < 3 + numkeys {
        return RespValue::wrong_arity("zstore");
    }

    let keys: Vec<Bytes> = ctx.args[3..3 + numkeys].to_vec();

    // Parse WEIGHTS and AGGREGATE
    let mut weights: Vec<f64> = vec![1.0; numkeys];
    let mut aggregate = Aggregate::Sum;

    let mut i = 3 + numkeys;
    while i < ctx.args.len() {
        let opt = String::from_utf8_lossy(&ctx.args[i]).to_uppercase();
        match opt.as_str() {
            "WEIGHTS" => {
                for j in 0..numkeys {
                    i += 1;
                    if i < ctx.args.len() {
                        weights[j] = String::from_utf8_lossy(&ctx.args[i]).parse().unwrap_or(1.0);
                    }
                }
            }
            "AGGREGATE" => {
                i += 1;
                if i < ctx.args.len() {
                    let agg = String::from_utf8_lossy(&ctx.args[i]).to_uppercase();
                    aggregate = match agg.as_str() {
                        "MIN" => Aggregate::Min,
                        "MAX" => Aggregate::Max,
                        _ => Aggregate::Sum,
                    };
                }
            }
            _ => {}
        }
        i += 1;
    }

    let mut sets: Vec<SortedSetData> = Vec::new();
    for key in &keys {
        sets.push(match collect_zset(ctx, key) {
            Ok(z) => z,
            Err(e) => return e,
        });
    }

    let mut result = SortedSetData::new();

    match op {
        SetOp::Union => {
            for (idx, set) in sets.iter().enumerate() {
                for (member, &score) in &set.members {
                    let weighted = score * weights[idx];
                    let existing = result.score(member);
                    let new_score = match existing {
                        Some(e) => aggregate.apply(e, weighted),
                        None => weighted,
                    };
                    result.insert(member.clone(), new_score);
                }
            }
        }
        SetOp::Inter => {
            if sets.is_empty() {
                // empty result
            } else {
                let first = &sets[0];
                for (member, &score) in &first.members {
                    let mut combined = score * weights[0];
                    let mut in_all = true;
                    for (idx, set) in sets.iter().enumerate().skip(1) {
                        match set.score(member) {
                            Some(s) => combined = aggregate.apply(combined, s * weights[idx]),
                            None => { in_all = false; break; }
                        }
                    }
                    if in_all {
                        result.insert(member.clone(), combined);
                    }
                }
            }
        }
        SetOp::Diff => {
            if let Some(first) = sets.first() {
                for (member, &score) in &first.members {
                    let in_others = sets[1..].iter().any(|s| s.score(member).is_some());
                    if !in_others {
                        result.insert(member.clone(), score * weights[0]);
                    }
                }
            }
        }
    }

    let len = result.len() as i64;
    ctx.db().set(dest, RedisObject::SortedSet(result));
    RespValue::integer(len)
}

enum Aggregate { Sum, Min, Max }

impl Aggregate {
    fn apply(&self, a: f64, b: f64) -> f64 {
        match self {
            Aggregate::Sum => a + b,
            Aggregate::Min => a.min(b),
            Aggregate::Max => a.max(b),
        }
    }
}

pub fn cmd_zlexcount(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let min_str = String::from_utf8_lossy(&ctx.args[2]).to_string();
    let max_str = String::from_utf8_lossy(&ctx.args[3]).to_string();

    match get_zset(ctx, &key) {
        Ok(Some(zset)) => {
            let all = zset.range_by_index(0, -1);
            let count = all.iter().filter(|(m, _)| {
                lex_in_range(m, &min_str, &max_str)
            }).count();
            RespValue::integer(count as i64)
        }
        Ok(None) => RespValue::integer(0),
        Err(e) => e,
    }
}

fn lex_in_range(member: &Bytes, min: &str, max: &str) -> bool {
    let m = String::from_utf8_lossy(member);

    let above_min = if min == "-" {
        true
    } else if let Some(rest) = min.strip_prefix('(') {
        m.as_ref() > rest
    } else if let Some(rest) = min.strip_prefix('[') {
        m.as_ref() >= rest
    } else {
        true
    };

    let below_max = if max == "+" {
        true
    } else if let Some(rest) = max.strip_prefix('(') {
        m.as_ref() < rest
    } else if let Some(rest) = max.strip_prefix('[') {
        m.as_ref() <= rest
    } else {
        true
    };

    above_min && below_max
}

fn format_zset_response(items: Vec<(Bytes, f64)>, withscores: bool) -> RespValue {
    if withscores {
        let mut result = Vec::with_capacity(items.len() * 2);
        for (member, score) in items {
            result.push(RespValue::bulk_string(member));
            result.push(RespValue::bulk_string(Bytes::from(format!("{}", score))));
        }
        RespValue::array(result)
    } else {
        RespValue::array(items.into_iter().map(|(m, _)| RespValue::bulk_string(m)).collect())
    }
}
