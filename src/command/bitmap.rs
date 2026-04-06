use bytes::Bytes;
use crate::protocol::RespValue;
use crate::storage::RedisObject;
use super::registry::CommandContext;

fn get_or_create_string(ctx: &mut CommandContext, key: &Bytes) -> Vec<u8> {
    let db = ctx.db();
    match db.get(key) {
        Some(RedisObject::String(b)) => b.to_vec(),
        Some(_) => vec![], // will be caught by type check
        None => vec![],
    }
}

fn check_type(ctx: &mut CommandContext, key: &Bytes) -> Result<(), RespValue> {
    let db = ctx.db();
    match db.get(key) {
        Some(RedisObject::String(_)) | None => Ok(()),
        Some(_) => Err(RespValue::wrong_type()),
    }
}

pub fn cmd_setbit(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let offset: usize = match String::from_utf8_lossy(&ctx.args[2]).parse() {
        Ok(v) => v,
        Err(_) => return RespValue::error("ERR bit offset is not an integer or out of range"),
    };
    let value: u8 = match String::from_utf8_lossy(&ctx.args[3]).parse::<u8>() {
        Ok(v) if v <= 1 => v,
        _ => return RespValue::error("ERR bit is not an integer or out of range"),
    };

    if let Err(e) = check_type(ctx, &key) {
        return e;
    }

    let mut data = get_or_create_string(ctx, &key);

    let byte_idx = offset / 8;
    let bit_idx = 7 - (offset % 8);

    // Extend if needed
    if byte_idx >= data.len() {
        data.resize(byte_idx + 1, 0);
    }

    let old_bit = (data[byte_idx] >> bit_idx) & 1;

    if value == 1 {
        data[byte_idx] |= 1 << bit_idx;
    } else {
        data[byte_idx] &= !(1 << bit_idx);
    }

    ctx.db().set_keep_ttl(key, RedisObject::String(Bytes::from(data)));
    RespValue::integer(old_bit as i64)
}

pub fn cmd_getbit(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let offset: usize = match String::from_utf8_lossy(&ctx.args[2]).parse() {
        Ok(v) => v,
        Err(_) => return RespValue::error("ERR bit offset is not an integer or out of range"),
    };

    if let Err(e) = check_type(ctx, &key) {
        return e;
    }

    let db = ctx.db();
    match db.get(&key) {
        Some(RedisObject::String(b)) => {
            let byte_idx = offset / 8;
            let bit_idx = 7 - (offset % 8);
            if byte_idx >= b.len() {
                RespValue::integer(0)
            } else {
                RespValue::integer(((b[byte_idx] >> bit_idx) & 1) as i64)
            }
        }
        Some(_) => RespValue::wrong_type(),
        None => RespValue::integer(0),
    }
}

pub fn cmd_bitcount(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();

    let db = ctx.db();
    let data = match db.get(&key) {
        Some(RedisObject::String(b)) => b.clone(),
        Some(_) => return RespValue::wrong_type(),
        None => return RespValue::integer(0),
    };

    if ctx.args.len() <= 2 {
        // Count all bits
        let count: u32 = data.iter().map(|b| b.count_ones()).sum();
        return RespValue::integer(count as i64);
    }

    let start: i64 = match String::from_utf8_lossy(&ctx.args[2]).parse() {
        Ok(v) => v,
        Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
    };
    let end: i64 = match String::from_utf8_lossy(&ctx.args[3]).parse() {
        Ok(v) => v,
        Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
    };

    let use_bit = ctx.args.len() > 4 &&
        String::from_utf8_lossy(&ctx.args[4]).to_uppercase() == "BIT";

    if use_bit {
        // BIT mode: start and end are bit offsets
        let total_bits = data.len() * 8;
        let s = if start < 0 { (total_bits as i64 + start).max(0) as usize } else { (start as usize).min(total_bits) };
        let e = if end < 0 { (total_bits as i64 + end).max(0) as usize } else { (end as usize).min(total_bits - 1) };

        if s > e {
            return RespValue::integer(0);
        }

        let mut count = 0i64;
        for bit_pos in s..=e {
            let byte_idx = bit_pos / 8;
            let bit_idx = 7 - (bit_pos % 8);
            if byte_idx < data.len() && (data[byte_idx] >> bit_idx) & 1 == 1 {
                count += 1;
            }
        }
        RespValue::integer(count)
    } else {
        // BYTE mode (default)
        let len = data.len() as i64;
        let s = if start < 0 { (len + start).max(0) as usize } else { (start as usize).min(data.len()) };
        let e = if end < 0 { (len + end).max(0) as usize } else { (end as usize).min(data.len() - 1) };

        if s > e {
            return RespValue::integer(0);
        }

        let count: u32 = data[s..=e].iter().map(|b| b.count_ones()).sum();
        RespValue::integer(count as i64)
    }
}

pub fn cmd_bitpos(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let target: u8 = match String::from_utf8_lossy(&ctx.args[2]).parse::<u8>() {
        Ok(v) if v <= 1 => v,
        _ => return RespValue::error("ERR bit is not an integer or out of range"),
    };

    let db = ctx.db();
    let data = match db.get(&key) {
        Some(RedisObject::String(b)) => b.clone(),
        Some(_) => return RespValue::wrong_type(),
        None => {
            return if target == 0 { RespValue::integer(0) } else { RespValue::integer(-1) };
        }
    };

    if data.is_empty() {
        return if target == 0 { RespValue::integer(0) } else { RespValue::integer(-1) };
    }

    let use_bit = ctx.args.len() > 5 &&
        String::from_utf8_lossy(&ctx.args[5]).to_uppercase() == "BIT";
    let has_end = ctx.args.len() > 4;

    let (start_byte, end_byte) = if ctx.args.len() > 3 {
        let start: i64 = match String::from_utf8_lossy(&ctx.args[3]).parse() {
            Ok(v) => v,
            Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
        };
        let end: i64 = if ctx.args.len() > 4 {
            match String::from_utf8_lossy(&ctx.args[4]).parse() {
                Ok(v) => v,
                Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
            }
        } else {
            -1
        };

        if use_bit {
            // BIT mode
            let total_bits = data.len() * 8;
            let s = if start < 0 { (total_bits as i64 + start).max(0) as usize } else { (start as usize).min(total_bits) };
            let e = if end < 0 { (total_bits as i64 + end).max(0) as usize } else { (end as usize).min(total_bits - 1) };

            if s > e {
                return RespValue::integer(-1);
            }

            for bit_pos in s..=e {
                let byte_idx = bit_pos / 8;
                let bit_idx = 7 - (bit_pos % 8);
                if byte_idx < data.len() {
                    let bit = (data[byte_idx] >> bit_idx) & 1;
                    if bit == target {
                        return RespValue::integer(bit_pos as i64);
                    }
                }
            }
            return RespValue::integer(-1);
        }

        let len = data.len() as i64;
        let s = if start < 0 { (len + start).max(0) as usize } else { (start as usize).min(data.len()) };
        let e = if end < 0 { (len + end).max(0) as usize } else { (end as usize).min(data.len() - 1) };
        (s, e)
    } else {
        (0, data.len() - 1)
    };

    if start_byte > end_byte {
        return RespValue::integer(-1);
    }

    for byte_idx in start_byte..=end_byte {
        for bit_idx in (0..8).rev() {
            let bit = (data[byte_idx] >> bit_idx) & 1;
            if bit == target {
                return RespValue::integer((byte_idx * 8 + (7 - bit_idx)) as i64);
            }
        }
    }

    // If looking for 0 and no end specified, return next bit after data
    if target == 0 && !has_end {
        return RespValue::integer((data.len() * 8) as i64);
    }

    RespValue::integer(-1)
}

pub fn cmd_bitop(ctx: &mut CommandContext) -> RespValue {
    let op = String::from_utf8_lossy(&ctx.args[1]).to_uppercase();
    let destkey = ctx.args[2].clone();

    if op == "NOT" {
        if ctx.args.len() != 4 {
            return RespValue::error("ERR BITOP NOT requires one and only one key");
        }
        let src_key = ctx.args[3].clone();
        let db = ctx.db();
        let src = match db.get(&src_key) {
            Some(RedisObject::String(b)) => b.clone(),
            Some(_) => return RespValue::wrong_type(),
            None => Bytes::new(),
        };
        let result: Vec<u8> = src.iter().map(|b| !b).collect();
        let len = result.len() as i64;
        ctx.db().set(destkey, RedisObject::String(Bytes::from(result)));
        return RespValue::integer(len);
    }

    let keys: Vec<Bytes> = ctx.args[3..].to_vec();
    if keys.is_empty() {
        return RespValue::wrong_arity("bitop");
    }

    let mut sources: Vec<Vec<u8>> = Vec::new();
    let mut max_len = 0;
    for key in &keys {
        let db = ctx.db();
        let data = match db.get(key) {
            Some(RedisObject::String(b)) => b.to_vec(),
            Some(_) => return RespValue::wrong_type(),
            None => vec![],
        };
        max_len = max_len.max(data.len());
        sources.push(data);
    }

    // Pad all to same length
    for src in &mut sources {
        src.resize(max_len, 0);
    }

    let mut result = vec![0u8; max_len];
    if !sources.is_empty() {
        result = sources[0].clone();
        for src in sources.iter().skip(1) {
            for (i, byte) in src.iter().enumerate() {
                match op.as_str() {
                    "AND" => result[i] &= byte,
                    "OR" => result[i] |= byte,
                    "XOR" => result[i] ^= byte,
                    _ => return RespValue::error("ERR syntax error"),
                }
            }
        }
    }

    let len = result.len() as i64;
    ctx.db().set(destkey, RedisObject::String(Bytes::from(result)));
    RespValue::integer(len)
}

pub fn cmd_bitfield(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();

    if let Err(e) = check_type(ctx, &key) {
        return e;
    }

    let mut results = Vec::new();
    let mut overflow = OverflowBehavior::Wrap;
    let mut i = 2;

    while i < ctx.args.len() {
        let subcmd = String::from_utf8_lossy(&ctx.args[i]).to_uppercase();
        match subcmd.as_str() {
            "GET" => {
                if i + 2 >= ctx.args.len() {
                    return RespValue::error("ERR syntax error");
                }
                let (signed, bits) = match parse_encoding(&ctx.args[i + 1]) {
                    Some(v) => v,
                    None => return RespValue::error("ERR Invalid bitfield type"),
                };
                let offset = match parse_bitfield_offset(&ctx.args[i + 2], bits) {
                    Some(v) => v,
                    None => return RespValue::error("ERR bit offset is not an integer or out of range"),
                };
                i += 3;

                let data = get_or_create_string(ctx, &key);
                let val = read_bits(&data, offset, bits, signed);
                results.push(RespValue::integer(val));
            }
            "SET" => {
                if i + 3 >= ctx.args.len() {
                    return RespValue::error("ERR syntax error");
                }
                let (signed, bits) = match parse_encoding(&ctx.args[i + 1]) {
                    Some(v) => v,
                    None => return RespValue::error("ERR Invalid bitfield type"),
                };
                let offset = match parse_bitfield_offset(&ctx.args[i + 2], bits) {
                    Some(v) => v,
                    None => return RespValue::error("ERR bit offset is not an integer or out of range"),
                };
                let value: i64 = match String::from_utf8_lossy(&ctx.args[i + 3]).parse() {
                    Ok(v) => v,
                    Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
                };
                i += 4;

                let mut data = get_or_create_string(ctx, &key);
                let old = read_bits(&data, offset, bits, signed);
                let clamped = clamp_value(value, bits, signed, overflow);
                write_bits(&mut data, offset, bits, clamped);
                ctx.db().set_keep_ttl(key.clone(), RedisObject::String(Bytes::from(data)));
                results.push(RespValue::integer(old));
            }
            "INCRBY" => {
                if i + 3 >= ctx.args.len() {
                    return RespValue::error("ERR syntax error");
                }
                let (signed, bits) = match parse_encoding(&ctx.args[i + 1]) {
                    Some(v) => v,
                    None => return RespValue::error("ERR Invalid bitfield type"),
                };
                let offset = match parse_bitfield_offset(&ctx.args[i + 2], bits) {
                    Some(v) => v,
                    None => return RespValue::error("ERR bit offset is not an integer or out of range"),
                };
                let increment: i64 = match String::from_utf8_lossy(&ctx.args[i + 3]).parse() {
                    Ok(v) => v,
                    Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
                };
                i += 4;

                let mut data = get_or_create_string(ctx, &key);
                let old = read_bits(&data, offset, bits, signed);
                let new_val = old.wrapping_add(increment);

                if overflow == OverflowBehavior::Fail {
                    // Check for overflow
                    let clamped = clamp_value(new_val, bits, signed, OverflowBehavior::Sat);
                    if clamped != new_val {
                        results.push(RespValue::Null);
                        continue;
                    }
                }

                let clamped = clamp_value(new_val, bits, signed, overflow);
                write_bits(&mut data, offset, bits, clamped);
                ctx.db().set_keep_ttl(key.clone(), RedisObject::String(Bytes::from(data)));
                results.push(RespValue::integer(clamped));
            }
            "OVERFLOW" => {
                if i + 1 >= ctx.args.len() {
                    return RespValue::error("ERR syntax error");
                }
                let behavior = String::from_utf8_lossy(&ctx.args[i + 1]).to_uppercase();
                overflow = match behavior.as_str() {
                    "WRAP" => OverflowBehavior::Wrap,
                    "SAT" => OverflowBehavior::Sat,
                    "FAIL" => OverflowBehavior::Fail,
                    _ => return RespValue::error("ERR Invalid OVERFLOW type"),
                };
                i += 2;
            }
            _ => return RespValue::error("ERR syntax error"),
        }
    }

    RespValue::array(results)
}

pub fn cmd_bitfield_ro(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();

    if let Err(e) = check_type(ctx, &key) {
        return e;
    }

    let mut results = Vec::new();
    let mut i = 2;

    while i < ctx.args.len() {
        let subcmd = String::from_utf8_lossy(&ctx.args[i]).to_uppercase();
        match subcmd.as_str() {
            "GET" => {
                if i + 2 >= ctx.args.len() {
                    return RespValue::error("ERR syntax error");
                }
                let (signed, bits) = match parse_encoding(&ctx.args[i + 1]) {
                    Some(v) => v,
                    None => return RespValue::error("ERR Invalid bitfield type"),
                };
                let offset = match parse_bitfield_offset(&ctx.args[i + 2], bits) {
                    Some(v) => v,
                    None => return RespValue::error("ERR bit offset is not an integer or out of range"),
                };
                i += 3;

                let data = get_or_create_string(ctx, &key);
                let val = read_bits(&data, offset, bits, signed);
                results.push(RespValue::integer(val));
            }
            _ => return RespValue::error("ERR BITFIELD_RO only supports GET subcommand"),
        }
    }

    RespValue::array(results)
}

#[derive(Clone, Copy, PartialEq)]
enum OverflowBehavior {
    Wrap,
    Sat,
    Fail,
}

fn parse_encoding(arg: &Bytes) -> Option<(bool, u32)> {
    let s = String::from_utf8_lossy(arg);
    let (signed, rest) = if s.starts_with('i') || s.starts_with('I') {
        (true, &s[1..])
    } else if s.starts_with('u') || s.starts_with('U') {
        (false, &s[1..])
    } else {
        return None;
    };
    let bits: u32 = rest.parse().ok()?;
    if bits == 0 || bits > 64 || (!signed && bits > 63) {
        return None;
    }
    Some((signed, bits))
}

fn parse_bitfield_offset(arg: &Bytes, bits: u32) -> Option<usize> {
    let s = String::from_utf8_lossy(arg);
    if let Some(rest) = s.strip_prefix('#') {
        let idx: usize = rest.parse().ok()?;
        Some(idx * bits as usize)
    } else {
        s.parse().ok()
    }
}

fn read_bits(data: &[u8], offset: usize, bits: u32, signed: bool) -> i64 {
    let mut val: u64 = 0;
    for i in 0..bits as usize {
        let bit_pos = offset + i;
        let byte_idx = bit_pos / 8;
        let bit_idx = 7 - (bit_pos % 8);
        let bit = if byte_idx < data.len() {
            (data[byte_idx] >> bit_idx) & 1
        } else {
            0
        };
        val = (val << 1) | bit as u64;
    }

    if signed && bits < 64 && (val >> (bits - 1)) & 1 == 1 {
        // Sign extend
        val |= !((1u64 << bits) - 1);
    }

    val as i64
}

fn write_bits(data: &mut Vec<u8>, offset: usize, bits: u32, value: i64) {
    let val = value as u64;
    let needed_bytes = (offset + bits as usize + 7) / 8;
    if needed_bytes > data.len() {
        data.resize(needed_bytes, 0);
    }

    for i in 0..bits as usize {
        let bit_pos = offset + i;
        let byte_idx = bit_pos / 8;
        let bit_idx = 7 - (bit_pos % 8);
        let bit = ((val >> (bits as usize - 1 - i)) & 1) as u8;
        if bit == 1 {
            data[byte_idx] |= 1 << bit_idx;
        } else {
            data[byte_idx] &= !(1 << bit_idx);
        }
    }
}

fn clamp_value(value: i64, bits: u32, signed: bool, overflow: OverflowBehavior) -> i64 {
    if bits >= 64 {
        return value;
    }

    if signed {
        let min = -(1i64 << (bits - 1));
        let max = (1i64 << (bits - 1)) - 1;
        match overflow {
            OverflowBehavior::Wrap => {
                let range = 1i64 << bits;
                let mut v = value % range;
                if v > max { v -= range; }
                if v < min { v += range; }
                v
            }
            OverflowBehavior::Sat | OverflowBehavior::Fail => {
                value.max(min).min(max)
            }
        }
    } else {
        let max = (1i64 << bits) - 1;
        match overflow {
            OverflowBehavior::Wrap => {
                let range = 1i64 << bits;
                ((value % range) + range) % range
            }
            OverflowBehavior::Sat | OverflowBehavior::Fail => {
                value.max(0).min(max)
            }
        }
    }
}
