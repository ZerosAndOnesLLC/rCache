use bytes::Bytes;
use crate::protocol::RespValue;
use crate::storage::RedisObject;
use super::registry::CommandContext;

/// HyperLogLog++ dense representation.
/// Uses 16384 (2^14) registers, 6 bits each.
/// Stored as a string value with a magic header.
const HLL_REGISTERS: usize = 16384;
const HLL_P: usize = 14; // log2(HLL_REGISTERS)
const HLL_HEADER: &[u8] = b"HYLL";

fn new_hll() -> Vec<u8> {
    let mut data = Vec::with_capacity(4 + HLL_REGISTERS);
    data.extend_from_slice(HLL_HEADER);
    data.resize(4 + HLL_REGISTERS, 0);
    data
}

fn is_hll(data: &[u8]) -> bool {
    data.len() >= 4 + HLL_REGISTERS && &data[0..4] == HLL_HEADER
}

fn get_register(data: &[u8], index: usize) -> u8 {
    data[4 + index]
}

fn set_register(data: &mut [u8], index: usize, value: u8) {
    data[4 + index] = value;
}

fn hash_element(element: &[u8]) -> u64 {
    // Simple hash using FNV-1a 64-bit
    let mut hash: u64 = 0xcbf29ce484222325;
    for &byte in element {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    // Extra mixing
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51afd7ed558ccd);
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xc4ceb9fe1a85ec53);
    hash ^= hash >> 33;
    hash
}

fn count_leading_zeros(hash: u64, skip_bits: usize) -> u8 {
    // Count leading zeros in the remaining bits after skip_bits
    let remaining = hash << skip_bits;
    let zeros = remaining.leading_zeros() as u8;
    // Cap at (64 - skip_bits)
    zeros.min((64 - skip_bits) as u8) + 1
}

fn hll_add(data: &mut Vec<u8>, element: &[u8]) -> bool {
    let hash = hash_element(element);
    let index = (hash & ((1 << HLL_P) - 1)) as usize;
    let count = count_leading_zeros(hash, HLL_P);

    let current = get_register(data, index);
    if count > current {
        set_register(data, index, count);
        true
    } else {
        false
    }
}

fn hll_count(data: &[u8]) -> i64 {
    // Use the HyperLogLog estimation formula
    let mut sum: f64 = 0.0;
    let mut zeros = 0;

    for i in 0..HLL_REGISTERS {
        let val = get_register(data, i);
        sum += 1.0 / (1u64 << val) as f64;
        if val == 0 {
            zeros += 1;
        }
    }

    let m = HLL_REGISTERS as f64;
    // Alpha constant for m = 16384
    let alpha = 0.7213 / (1.0 + 1.079 / m);
    let mut estimate = alpha * m * m / sum;

    // Small range correction: linear counting
    if estimate <= 2.5 * m && zeros > 0 {
        estimate = m * (m / zeros as f64).ln();
    }

    estimate.round() as i64
}

fn hll_merge(dest: &mut Vec<u8>, source: &[u8]) {
    for i in 0..HLL_REGISTERS {
        let src_val = get_register(source, i);
        let dst_val = get_register(dest, i);
        if src_val > dst_val {
            set_register(dest, i, src_val);
        }
    }
}

fn get_or_create_hll(ctx: &mut CommandContext, key: &Bytes) -> Result<Vec<u8>, RespValue> {
    let db = ctx.db();
    match db.get(key) {
        Some(RedisObject::String(b)) => {
            if is_hll(b) {
                Ok(b.to_vec())
            } else if b.is_empty() {
                Ok(new_hll())
            } else {
                Err(RespValue::error("WRONGTYPE Key is not a valid HyperLogLog string value."))
            }
        }
        Some(_) => Err(RespValue::wrong_type()),
        None => Ok(new_hll()),
    }
}

pub fn cmd_pfadd(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let elements: Vec<Bytes> = ctx.args[2..].to_vec();

    let mut data = match get_or_create_hll(ctx, &key) {
        Ok(d) => d,
        Err(e) => return e,
    };

    let mut modified = false;
    for elem in &elements {
        if hll_add(&mut data, elem) {
            modified = true;
        }
    }

    ctx.db().set_keep_ttl(key, RedisObject::String(Bytes::from(data)));
    RespValue::integer(if modified { 1 } else { 0 })
}

pub fn cmd_pfcount(ctx: &mut CommandContext) -> RespValue {
    let keys: Vec<Bytes> = ctx.args[1..].to_vec();

    if keys.len() == 1 {
        let key = &keys[0];
        let db = ctx.db();
        match db.get(key) {
            Some(RedisObject::String(b)) => {
                if is_hll(b) {
                    RespValue::integer(hll_count(b))
                } else if b.is_empty() {
                    RespValue::integer(0)
                } else {
                    RespValue::error("WRONGTYPE Key is not a valid HyperLogLog string value.")
                }
            }
            Some(_) => RespValue::wrong_type(),
            None => RespValue::integer(0),
        }
    } else {
        // Merge all and count
        let mut merged = new_hll();
        for key in &keys {
            let db = ctx.db();
            match db.get(key) {
                Some(RedisObject::String(b)) => {
                    if is_hll(b) {
                        hll_merge(&mut merged, b);
                    } else if !b.is_empty() {
                        return RespValue::error("WRONGTYPE Key is not a valid HyperLogLog string value.");
                    }
                }
                Some(_) => return RespValue::wrong_type(),
                None => {} // treat as empty
            }
        }
        RespValue::integer(hll_count(&merged))
    }
}

pub fn cmd_pfmerge(ctx: &mut CommandContext) -> RespValue {
    let destkey = ctx.args[1].clone();
    let source_keys: Vec<Bytes> = ctx.args[2..].to_vec();

    let mut merged = match get_or_create_hll(ctx, &destkey) {
        Ok(d) => d,
        Err(e) => return e,
    };

    for key in &source_keys {
        let db = ctx.db();
        match db.get(key) {
            Some(RedisObject::String(b)) => {
                if is_hll(b) {
                    hll_merge(&mut merged, b);
                } else if !b.is_empty() {
                    return RespValue::error("WRONGTYPE Key is not a valid HyperLogLog string value.");
                }
            }
            Some(_) => return RespValue::wrong_type(),
            None => {} // treat as empty
        }
    }

    ctx.db().set_keep_ttl(destkey, RedisObject::String(Bytes::from(merged)));
    RespValue::ok()
}
