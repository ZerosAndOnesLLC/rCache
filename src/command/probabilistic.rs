use bytes::Bytes;
use crate::protocol::RespValue;
use crate::storage::RedisObject;
use super::registry::CommandContext;

// ============================================================================
// Bloom Filter
// ============================================================================

const BLOOM_HEADER: &[u8] = b"BLMF";
const BLOOM_HEADER_LEN: usize = 4;
// Header layout after magic: capacity(u32) + num_items(u32) + num_bits(u32) + k(u32) + error_rate(f64)
const BLOOM_META_LEN: usize = BLOOM_HEADER_LEN + 4 + 4 + 4 + 4 + 8;

const DEFAULT_ERROR_RATE: f64 = 0.01;
const DEFAULT_CAPACITY: u32 = 100;

fn bloom_hash(item: &[u8], seed: u32) -> u64 {
    // FNV-1a with seed mixing
    let mut hash: u64 = 0xcbf29ce484222325_u64.wrapping_add(seed as u64);
    for &byte in item {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    // Extra mixing
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51afd7ed558ccd);
    hash ^= hash >> 33;
    hash
}

fn get_hash_indices(item: &[u8], num_bits: usize, k: usize) -> Vec<usize> {
    let h1 = bloom_hash(item, 0);
    let h2 = bloom_hash(item, 1);
    (0..k)
        .map(|i| ((h1.wrapping_add((i as u64).wrapping_mul(h2))) % num_bits as u64) as usize)
        .collect()
}

fn optimal_k(error_rate: f64) -> u32 {
    (-error_rate.log2()).ceil() as u32
}

fn optimal_bits(capacity: u32, error_rate: f64) -> u32 {
    let ln2 = std::f64::consts::LN_2;
    (-(capacity as f64) * error_rate.ln() / (ln2 * ln2)).ceil() as u32
}

fn new_bloom(error_rate: f64, capacity: u32) -> Vec<u8> {
    let k = optimal_k(error_rate);
    let num_bits = optimal_bits(capacity, error_rate).max(8);
    let byte_count = (num_bits as usize + 7) / 8;

    let mut data = Vec::with_capacity(BLOOM_META_LEN + byte_count);
    data.extend_from_slice(BLOOM_HEADER);
    data.extend_from_slice(&capacity.to_le_bytes());
    data.extend_from_slice(&0u32.to_le_bytes()); // num_items = 0
    data.extend_from_slice(&num_bits.to_le_bytes());
    data.extend_from_slice(&k.to_le_bytes());
    data.extend_from_slice(&error_rate.to_le_bytes());
    data.resize(BLOOM_META_LEN + byte_count, 0);
    data
}

fn is_bloom(data: &[u8]) -> bool {
    data.len() >= BLOOM_META_LEN && &data[0..BLOOM_HEADER_LEN] == BLOOM_HEADER
}

fn bloom_capacity(data: &[u8]) -> u32 {
    u32::from_le_bytes(data[4..8].try_into().unwrap())
}

fn bloom_num_items(data: &[u8]) -> u32 {
    u32::from_le_bytes(data[8..12].try_into().unwrap())
}

fn set_bloom_num_items(data: &mut [u8], count: u32) {
    data[8..12].copy_from_slice(&count.to_le_bytes());
}

fn bloom_num_bits(data: &[u8]) -> u32 {
    u32::from_le_bytes(data[12..16].try_into().unwrap())
}

fn bloom_k(data: &[u8]) -> u32 {
    u32::from_le_bytes(data[16..20].try_into().unwrap())
}

fn bloom_error_rate(data: &[u8]) -> f64 {
    f64::from_le_bytes(data[20..28].try_into().unwrap())
}

fn bloom_get_bit(data: &[u8], index: usize) -> bool {
    let byte_index = BLOOM_META_LEN + index / 8;
    let bit_index = index % 8;
    if byte_index >= data.len() {
        return false;
    }
    (data[byte_index] >> bit_index) & 1 == 1
}

fn bloom_set_bit(data: &mut [u8], index: usize) {
    let byte_index = BLOOM_META_LEN + index / 8;
    let bit_index = index % 8;
    if byte_index < data.len() {
        data[byte_index] |= 1 << bit_index;
    }
}

fn bloom_add(data: &mut Vec<u8>, item: &[u8]) -> bool {
    let num_bits = bloom_num_bits(data) as usize;
    let k = bloom_k(data) as usize;
    let indices = get_hash_indices(item, num_bits, k);

    let mut all_set = true;
    for &idx in &indices {
        if !bloom_get_bit(data, idx) {
            all_set = false;
        }
    }

    for &idx in &indices {
        bloom_set_bit(data, idx);
    }

    if !all_set {
        let count = bloom_num_items(data);
        set_bloom_num_items(data, count + 1);
        true // newly added
    } else {
        false // probably existed
    }
}

fn bloom_exists(data: &[u8], item: &[u8]) -> bool {
    let num_bits = bloom_num_bits(data) as usize;
    let k = bloom_k(data) as usize;
    let indices = get_hash_indices(item, num_bits, k);

    for &idx in &indices {
        if !bloom_get_bit(data, idx) {
            return false;
        }
    }
    true
}

fn get_or_create_bloom(ctx: &mut CommandContext, key: &Bytes) -> Result<Vec<u8>, RespValue> {
    let db = ctx.db();
    match db.get(key) {
        Some(RedisObject::String(b)) => {
            if is_bloom(b) {
                Ok(b.to_vec())
            } else if b.is_empty() {
                Ok(new_bloom(DEFAULT_ERROR_RATE, DEFAULT_CAPACITY))
            } else {
                Err(RespValue::error(
                    "WRONGTYPE Key is not a valid Bloom filter",
                ))
            }
        }
        Some(_) => Err(RespValue::wrong_type()),
        None => Ok(new_bloom(DEFAULT_ERROR_RATE, DEFAULT_CAPACITY)),
    }
}

/// BF.ADD key item
pub fn cmd_bf_add(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let item = ctx.args[2].clone();

    let mut data = match get_or_create_bloom(ctx, &key) {
        Ok(d) => d,
        Err(e) => return e,
    };

    let added = bloom_add(&mut data, &item);
    ctx.db()
        .set_keep_ttl(key, RedisObject::String(Bytes::from(data)));
    RespValue::integer(if added { 1 } else { 0 })
}

/// BF.EXISTS key item
pub fn cmd_bf_exists(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let item = ctx.args[2].clone();

    let db = ctx.db();
    match db.get(&key) {
        Some(RedisObject::String(b)) => {
            if is_bloom(b) {
                RespValue::integer(if bloom_exists(b, &item) { 1 } else { 0 })
            } else if b.is_empty() {
                RespValue::integer(0)
            } else {
                RespValue::error("WRONGTYPE Key is not a valid Bloom filter")
            }
        }
        Some(_) => RespValue::wrong_type(),
        None => RespValue::integer(0),
    }
}

/// BF.MADD key item [item ...]
pub fn cmd_bf_madd(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let items: Vec<Bytes> = ctx.args[2..].to_vec();

    let mut data = match get_or_create_bloom(ctx, &key) {
        Ok(d) => d,
        Err(e) => return e,
    };

    let results: Vec<RespValue> = items
        .iter()
        .map(|item| {
            let added = bloom_add(&mut data, item);
            RespValue::integer(if added { 1 } else { 0 })
        })
        .collect();

    ctx.db()
        .set_keep_ttl(key, RedisObject::String(Bytes::from(data)));
    RespValue::Array(results)
}

/// BF.MEXISTS key item [item ...]
pub fn cmd_bf_mexists(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let items: Vec<Bytes> = ctx.args[2..].to_vec();

    let db = ctx.db();
    let bloom_data = match db.get(&key) {
        Some(RedisObject::String(b)) => {
            if is_bloom(b) {
                Some(b.clone())
            } else if b.is_empty() {
                None
            } else {
                return RespValue::error("WRONGTYPE Key is not a valid Bloom filter");
            }
        }
        Some(_) => return RespValue::wrong_type(),
        None => None,
    };

    let results: Vec<RespValue> = items
        .iter()
        .map(|item| {
            let exists = bloom_data
                .as_ref()
                .is_some_and(|data| bloom_exists(data, item));
            RespValue::integer(if exists { 1 } else { 0 })
        })
        .collect();

    RespValue::Array(results)
}

/// BF.RESERVE key error_rate capacity
pub fn cmd_bf_reserve(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();

    let error_rate: f64 = match std::str::from_utf8(&ctx.args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(v) if v > 0.0 && v < 1.0 => v,
        _ => return RespValue::error("ERR (error) bad error rate"),
    };

    let capacity: u32 = match std::str::from_utf8(&ctx.args[3])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(v) if v > 0 => v,
        _ => return RespValue::error("ERR (error) bad capacity"),
    };

    let db = ctx.db();
    if db.exists(&key) {
        return RespValue::error("ERR item exists");
    }

    let data = new_bloom(error_rate, capacity);
    db.set(key, RedisObject::String(Bytes::from(data)));
    RespValue::ok()
}

/// BF.INFO key
pub fn cmd_bf_info(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();

    let db = ctx.db();
    match db.get(&key) {
        Some(RedisObject::String(b)) => {
            if is_bloom(b) {
                let capacity = bloom_capacity(b);
                let num_bits = bloom_num_bits(b);
                let num_items = bloom_num_items(b);
                let byte_size = b.len();

                RespValue::Array(vec![
                    RespValue::bulk_string("Capacity"),
                    RespValue::integer(capacity as i64),
                    RespValue::bulk_string("Size"),
                    RespValue::integer(byte_size as i64),
                    RespValue::bulk_string("Number of filters"),
                    RespValue::integer(1),
                    RespValue::bulk_string("Number of items inserted"),
                    RespValue::integer(num_items as i64),
                    RespValue::bulk_string("Expansion rate"),
                    RespValue::integer(2),
                    RespValue::bulk_string("Number of bits"),
                    RespValue::integer(num_bits as i64),
                ])
            } else {
                RespValue::error("WRONGTYPE Key is not a valid Bloom filter")
            }
        }
        Some(_) => RespValue::wrong_type(),
        None => RespValue::error("ERR not found"),
    }
}

// ============================================================================
// Count-Min Sketch
// ============================================================================

const CMS_HEADER: &[u8] = b"CMSK";
const CMS_HEADER_LEN: usize = 4;
// Header layout: magic(4) + width(u32) + depth(u32) + total_count(u64)
const CMS_META_LEN: usize = CMS_HEADER_LEN + 4 + 4 + 8;

fn new_cms(width: u32, depth: u32) -> Vec<u8> {
    let counter_bytes = (width as usize) * (depth as usize) * 8; // u64 per counter
    let mut data = Vec::with_capacity(CMS_META_LEN + counter_bytes);
    data.extend_from_slice(CMS_HEADER);
    data.extend_from_slice(&width.to_le_bytes());
    data.extend_from_slice(&depth.to_le_bytes());
    data.extend_from_slice(&0u64.to_le_bytes()); // total_count
    data.resize(CMS_META_LEN + counter_bytes, 0);
    data
}

fn is_cms(data: &[u8]) -> bool {
    data.len() >= CMS_META_LEN && &data[0..CMS_HEADER_LEN] == CMS_HEADER
}

fn cms_width(data: &[u8]) -> u32 {
    u32::from_le_bytes(data[4..8].try_into().unwrap())
}

fn cms_depth(data: &[u8]) -> u32 {
    u32::from_le_bytes(data[8..12].try_into().unwrap())
}

fn cms_total_count(data: &[u8]) -> u64 {
    u64::from_le_bytes(data[12..20].try_into().unwrap())
}

fn set_cms_total_count(data: &mut [u8], count: u64) {
    data[12..20].copy_from_slice(&count.to_le_bytes());
}

fn cms_get_counter(data: &[u8], row: usize, col: usize, width: usize) -> u64 {
    let offset = CMS_META_LEN + (row * width + col) * 8;
    u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap())
}

fn cms_set_counter(data: &mut [u8], row: usize, col: usize, width: usize, value: u64) {
    let offset = CMS_META_LEN + (row * width + col) * 8;
    data[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn cms_hash(item: &[u8], seed: u32) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325_u64.wrapping_add(seed as u64);
    for &byte in item {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51afd7ed558ccd);
    hash ^= hash >> 33;
    hash
}

fn cms_increment(data: &mut Vec<u8>, item: &[u8], increment: u64) {
    let width = cms_width(data) as usize;
    let depth = cms_depth(data) as usize;

    for row in 0..depth {
        let h = cms_hash(item, row as u32);
        let col = (h % width as u64) as usize;
        let current = cms_get_counter(data, row, col, width);
        cms_set_counter(data, row, col, width, current.saturating_add(increment));
    }

    let total = cms_total_count(data);
    set_cms_total_count(data, total.saturating_add(increment));
}

fn cms_query(data: &[u8], item: &[u8]) -> u64 {
    let width = cms_width(data) as usize;
    let depth = cms_depth(data) as usize;

    let mut min_count = u64::MAX;
    for row in 0..depth {
        let h = cms_hash(item, row as u32);
        let col = (h % width as u64) as usize;
        let count = cms_get_counter(data, row, col, width);
        min_count = min_count.min(count);
    }

    if min_count == u64::MAX {
        0
    } else {
        min_count
    }
}

fn get_or_error_cms(ctx: &mut CommandContext, key: &Bytes) -> Result<Vec<u8>, RespValue> {
    let db = ctx.db();
    match db.get(key) {
        Some(RedisObject::String(b)) => {
            if is_cms(b) {
                Ok(b.to_vec())
            } else {
                Err(RespValue::error(
                    "WRONGTYPE Key is not a valid Count-Min Sketch",
                ))
            }
        }
        Some(_) => Err(RespValue::wrong_type()),
        None => Err(RespValue::error("ERR CMS: key does not exist")),
    }
}

/// CMS.INITBYDIM key width depth
pub fn cmd_cms_initbydim(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();

    let width: u32 = match std::str::from_utf8(&ctx.args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(v) if v > 0 => v,
        _ => return RespValue::error("ERR invalid width"),
    };

    let depth: u32 = match std::str::from_utf8(&ctx.args[3])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(v) if v > 0 => v,
        _ => return RespValue::error("ERR invalid depth"),
    };

    let db = ctx.db();
    if db.exists(&key) {
        return RespValue::error("ERR item exists");
    }

    let data = new_cms(width, depth);
    db.set(key, RedisObject::String(Bytes::from(data)));
    RespValue::ok()
}

/// CMS.INITBYPROB key error probability
pub fn cmd_cms_initbyprob(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();

    let error: f64 = match std::str::from_utf8(&ctx.args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(v) if v > 0.0 && v < 1.0 => v,
        _ => return RespValue::error("ERR invalid error rate"),
    };

    let probability: f64 = match std::str::from_utf8(&ctx.args[3])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(v) if v > 0.0 && v < 1.0 => v,
        _ => return RespValue::error("ERR invalid probability"),
    };

    let db = ctx.db();
    if db.exists(&key) {
        return RespValue::error("ERR item exists");
    }

    // width = ceil(e / error), depth = ceil(ln(1/probability))
    let width = (std::f64::consts::E / error).ceil() as u32;
    let depth = (1.0_f64 / probability).ln().ceil() as u32;

    let data = new_cms(width.max(1), depth.max(1));
    db.set(key, RedisObject::String(Bytes::from(data)));
    RespValue::ok()
}

/// CMS.INCRBY key item increment [item increment ...]
pub fn cmd_cms_incrby(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();

    // Must have pairs of item+increment after key
    let pairs: Vec<Bytes> = ctx.args[2..].to_vec();
    if pairs.len() % 2 != 0 {
        return RespValue::error("ERR wrong number of arguments for 'cms.incrby' command");
    }

    let mut data = match get_or_error_cms(ctx, &key) {
        Ok(d) => d,
        Err(e) => return e,
    };

    let mut results = Vec::new();
    let mut i = 0;
    while i < pairs.len() {
        let item = &pairs[i];
        let increment: u64 = match std::str::from_utf8(&pairs[i + 1])
            .ok()
            .and_then(|s| s.parse().ok())
        {
            Some(v) => v,
            None => return RespValue::error("ERR invalid increment value"),
        };

        cms_increment(&mut data, item, increment);
        let count = cms_query(&data, item);
        results.push(RespValue::integer(count as i64));
        i += 2;
    }

    ctx.db()
        .set_keep_ttl(key, RedisObject::String(Bytes::from(data)));
    RespValue::Array(results)
}

/// CMS.QUERY key item [item ...]
pub fn cmd_cms_query(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let items: Vec<Bytes> = ctx.args[2..].to_vec();

    let db = ctx.db();
    let data = match db.get(&key) {
        Some(RedisObject::String(b)) => {
            if is_cms(b) {
                b.clone()
            } else {
                return RespValue::error("WRONGTYPE Key is not a valid Count-Min Sketch");
            }
        }
        Some(_) => return RespValue::wrong_type(),
        None => return RespValue::error("ERR CMS: key does not exist"),
    };

    let results: Vec<RespValue> = items
        .iter()
        .map(|item| RespValue::integer(cms_query(&data, item) as i64))
        .collect();

    RespValue::Array(results)
}

/// CMS.MERGE destkey numkeys src [src ...] [WEIGHTS weight ...]
pub fn cmd_cms_merge(ctx: &mut CommandContext) -> RespValue {
    let destkey = ctx.args[1].clone();

    let numkeys: usize = match std::str::from_utf8(&ctx.args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(v) if v > 0 => v,
        _ => return RespValue::error("ERR invalid numkeys"),
    };

    if ctx.args.len() < 3 + numkeys {
        return RespValue::error("ERR wrong number of arguments for 'cms.merge' command");
    }

    let src_keys: Vec<Bytes> = ctx.args[3..3 + numkeys].to_vec();

    // Parse optional WEIGHTS
    let mut weights: Vec<u64> = vec![1; numkeys];
    let remaining = &ctx.args[3 + numkeys..];
    if !remaining.is_empty() {
        let first = String::from_utf8_lossy(&remaining[0]).to_uppercase();
        if first == "WEIGHTS" {
            if remaining.len() != 1 + numkeys {
                return RespValue::error(
                    "ERR wrong number of weights for 'cms.merge' command",
                );
            }
            for (i, w) in remaining[1..].iter().enumerate() {
                match std::str::from_utf8(w).ok().and_then(|s| s.parse().ok()) {
                    Some(v) => weights[i] = v,
                    None => return RespValue::error("ERR invalid weight value"),
                }
            }
        }
    }

    // Read all source sketches, collect their data
    let mut source_data: Vec<Vec<u8>> = Vec::new();
    let mut target_width: Option<u32> = None;
    let mut target_depth: Option<u32> = None;

    for src_key in &src_keys {
        let db = ctx.db();
        match db.get(src_key) {
            Some(RedisObject::String(b)) => {
                if is_cms(b) {
                    let w = cms_width(b);
                    let d = cms_depth(b);
                    if let Some(tw) = target_width {
                        if tw != w || target_depth.unwrap() != d {
                            return RespValue::error(
                                "ERR CMS: all sketches must have the same dimensions",
                            );
                        }
                    } else {
                        target_width = Some(w);
                        target_depth = Some(d);
                    }
                    source_data.push(b.to_vec());
                } else {
                    return RespValue::error(
                        "WRONGTYPE Key is not a valid Count-Min Sketch",
                    );
                }
            }
            Some(_) => return RespValue::wrong_type(),
            None => return RespValue::error("ERR CMS: key does not exist"),
        }
    }

    let width = match target_width {
        Some(w) => w,
        None => return RespValue::error("ERR CMS: no source keys"),
    };
    let depth = target_depth.unwrap();

    let mut dest = new_cms(width, depth);
    let mut total: u64 = 0;

    for (idx, src) in source_data.iter().enumerate() {
        let w = weights[idx];
        let src_total = cms_total_count(src);
        total = total.saturating_add(src_total.saturating_mul(w));

        for row in 0..depth as usize {
            for col in 0..width as usize {
                let src_val = cms_get_counter(src, row, col, width as usize);
                let dst_val = cms_get_counter(&dest, row, col, width as usize);
                cms_set_counter(
                    &mut dest,
                    row,
                    col,
                    width as usize,
                    dst_val.saturating_add(src_val.saturating_mul(w)),
                );
            }
        }
    }

    set_cms_total_count(&mut dest, total);
    ctx.db()
        .set_keep_ttl(destkey, RedisObject::String(Bytes::from(dest)));
    RespValue::ok()
}

/// CMS.INFO key
pub fn cmd_cms_info(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();

    let db = ctx.db();
    match db.get(&key) {
        Some(RedisObject::String(b)) => {
            if is_cms(b) {
                let width = cms_width(b);
                let depth = cms_depth(b);
                let count = cms_total_count(b);
                RespValue::Array(vec![
                    RespValue::bulk_string("width"),
                    RespValue::integer(width as i64),
                    RespValue::bulk_string("depth"),
                    RespValue::integer(depth as i64),
                    RespValue::bulk_string("count"),
                    RespValue::integer(count as i64),
                ])
            } else {
                RespValue::error("WRONGTYPE Key is not a valid Count-Min Sketch")
            }
        }
        Some(_) => RespValue::wrong_type(),
        None => RespValue::error("ERR CMS: key does not exist"),
    }
}

// ============================================================================
// Top-K
// ============================================================================

const TOPK_HEADER: &[u8] = b"TOPK";
const TOPK_HEADER_LEN: usize = 4;
// Header layout: magic(4) + k(u32) + width(u32) + depth(u32) + decay(f64) + num_items(u32)
// Then: CMS counters (width*depth*8 bytes)
// Then: heap entries as length-prefixed strings with u64 counts
const TOPK_META_LEN: usize = TOPK_HEADER_LEN + 4 + 4 + 4 + 8 + 4;

fn new_topk(k: u32, width: u32, depth: u32, decay: f64) -> Vec<u8> {
    let cms_bytes = (width as usize) * (depth as usize) * 8;
    let mut data = Vec::with_capacity(TOPK_META_LEN + cms_bytes);
    data.extend_from_slice(TOPK_HEADER);
    data.extend_from_slice(&k.to_le_bytes());
    data.extend_from_slice(&width.to_le_bytes());
    data.extend_from_slice(&depth.to_le_bytes());
    data.extend_from_slice(&decay.to_le_bytes());
    data.extend_from_slice(&0u32.to_le_bytes()); // num_items in heap
    data.resize(TOPK_META_LEN + cms_bytes, 0);
    // Heap section starts after CMS counters - initially empty
    data
}

fn is_topk(data: &[u8]) -> bool {
    data.len() >= TOPK_META_LEN && &data[0..TOPK_HEADER_LEN] == TOPK_HEADER
}

fn topk_k(data: &[u8]) -> u32 {
    u32::from_le_bytes(data[4..8].try_into().unwrap())
}

fn topk_width(data: &[u8]) -> u32 {
    u32::from_le_bytes(data[8..12].try_into().unwrap())
}

fn topk_depth(data: &[u8]) -> u32 {
    u32::from_le_bytes(data[12..16].try_into().unwrap())
}

fn topk_decay(data: &[u8]) -> f64 {
    f64::from_le_bytes(data[16..24].try_into().unwrap())
}

fn topk_num_items(data: &[u8]) -> u32 {
    u32::from_le_bytes(data[24..28].try_into().unwrap())
}

fn set_topk_num_items(data: &mut [u8], count: u32) {
    data[24..28].copy_from_slice(&count.to_le_bytes());
}

/// Parsed top-k structure for manipulation
struct TopKState {
    k: u32,
    width: u32,
    depth: u32,
    decay: f64,
    // CMS counters as a flat vec (row-major: depth rows x width cols)
    counters: Vec<u64>,
    // Heap: item -> estimated count
    heap: Vec<(String, u64)>,
}

impl TopKState {
    fn from_bytes(data: &[u8]) -> Self {
        let k = topk_k(data);
        let width = topk_width(data);
        let depth = topk_depth(data);
        let decay = topk_decay(data);
        let num_items = topk_num_items(data) as usize;

        let cms_size = (width as usize) * (depth as usize);
        let mut counters = Vec::with_capacity(cms_size);
        for i in 0..cms_size {
            let offset = TOPK_META_LEN + i * 8;
            let val = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
            counters.push(val);
        }

        let mut heap = Vec::with_capacity(num_items);
        let mut pos = TOPK_META_LEN + cms_size * 8;
        for _ in 0..num_items {
            if pos + 4 > data.len() {
                break;
            }
            let name_len =
                u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            if pos + name_len + 8 > data.len() {
                break;
            }
            let name = String::from_utf8_lossy(&data[pos..pos + name_len]).to_string();
            pos += name_len;
            let count = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;
            heap.push((name, count));
        }

        Self {
            k,
            width,
            depth,
            decay,
            counters,
            heap,
        }
    }

    fn to_bytes(&self) -> Vec<u8> {
        let cms_bytes = (self.width as usize) * (self.depth as usize) * 8;
        let mut heap_bytes = 0;
        for (name, _) in &self.heap {
            heap_bytes += 4 + name.len() + 8;
        }
        let mut data = Vec::with_capacity(TOPK_META_LEN + cms_bytes + heap_bytes);
        data.extend_from_slice(TOPK_HEADER);
        data.extend_from_slice(&self.k.to_le_bytes());
        data.extend_from_slice(&self.width.to_le_bytes());
        data.extend_from_slice(&self.depth.to_le_bytes());
        data.extend_from_slice(&self.decay.to_le_bytes());
        data.extend_from_slice(&(self.heap.len() as u32).to_le_bytes());

        for counter in &self.counters {
            data.extend_from_slice(&counter.to_le_bytes());
        }

        for (name, count) in &self.heap {
            data.extend_from_slice(&(name.len() as u32).to_le_bytes());
            data.extend_from_slice(name.as_bytes());
            data.extend_from_slice(&count.to_le_bytes());
        }

        data
    }

    fn cms_hash(&self, item: &[u8], seed: u32) -> u64 {
        cms_hash(item, seed)
    }

    fn cms_increment(&mut self, item: &[u8]) -> u64 {
        let width = self.width as usize;
        let depth = self.depth as usize;
        let mut min_count = u64::MAX;

        for row in 0..depth {
            let h = self.cms_hash(item, row as u32);
            let col = (h % width as u64) as usize;
            let idx = row * width + col;
            self.counters[idx] = self.counters[idx].saturating_add(1);
            min_count = min_count.min(self.counters[idx]);
        }

        min_count
    }

    fn cms_query(&self, item: &[u8]) -> u64 {
        let width = self.width as usize;
        let depth = self.depth as usize;
        let mut min_count = u64::MAX;

        for row in 0..depth {
            let h = self.cms_hash(item, row as u32);
            let col = (h % width as u64) as usize;
            let idx = row * width + col;
            min_count = min_count.min(self.counters[idx]);
        }

        if min_count == u64::MAX {
            0
        } else {
            min_count
        }
    }

    /// Add an item, return the evicted item name if any
    fn add(&mut self, item: &str) -> Option<String> {
        let item_bytes = item.as_bytes();
        let estimated_count = self.cms_increment(item_bytes);

        // Check if item is already in the heap
        if let Some(pos) = self.heap.iter().position(|(name, _)| name == item) {
            self.heap[pos].1 = estimated_count;
            return None;
        }

        // If heap not full, just add
        if (self.heap.len() as u32) < self.k {
            self.heap.push((item.to_string(), estimated_count));
            return None;
        }

        // Find the min item in the heap
        let min_idx = self
            .heap
            .iter()
            .enumerate()
            .min_by_key(|(_, (_, count))| *count)
            .map(|(idx, _)| idx)
            .unwrap();

        let min_count = self.heap[min_idx].1;

        if estimated_count > min_count {
            // Apply decay to the evicted item's fingerprint
            let evicted_name = self.heap[min_idx].0.clone();

            // Decay: probabilistically keep the old item
            if self.decay < 1.0 {
                let keep_prob = self.decay.powf((estimated_count - min_count) as f64);
                // Simple deterministic check: if the difference is large enough, evict
                if keep_prob < 0.5 {
                    self.heap[min_idx] = (item.to_string(), estimated_count);
                    return Some(evicted_name);
                }
            }

            self.heap[min_idx] = (item.to_string(), estimated_count);
            return Some(evicted_name);
        }

        None
    }

    fn query(&self, item: &str) -> bool {
        self.heap.iter().any(|(name, _)| name == item)
    }

    fn count(&self, item: &str) -> u64 {
        self.cms_query(item.as_bytes())
    }

    fn list(&self) -> Vec<(String, u64)> {
        let mut items = self.heap.clone();
        items.sort_by(|a, b| b.1.cmp(&a.1));
        items
    }
}

fn get_or_error_topk(ctx: &mut CommandContext, key: &Bytes) -> Result<TopKState, RespValue> {
    let db = ctx.db();
    match db.get(key) {
        Some(RedisObject::String(b)) => {
            if is_topk(b) {
                Ok(TopKState::from_bytes(b))
            } else {
                Err(RespValue::error("WRONGTYPE Key is not a valid Top-K"))
            }
        }
        Some(_) => Err(RespValue::wrong_type()),
        None => Err(RespValue::error("ERR TOPK: key does not exist")),
    }
}

/// TOPK.RESERVE key topk [width depth decay]
pub fn cmd_topk_reserve(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();

    let k: u32 = match std::str::from_utf8(&ctx.args[2])
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(v) if v > 0 => v,
        _ => return RespValue::error("ERR invalid k"),
    };

    let (width, depth, decay) = if ctx.args.len() >= 6 {
        let w: u32 = match std::str::from_utf8(&ctx.args[3])
            .ok()
            .and_then(|s| s.parse().ok())
        {
            Some(v) if v > 0 => v,
            _ => return RespValue::error("ERR invalid width"),
        };
        let d: u32 = match std::str::from_utf8(&ctx.args[4])
            .ok()
            .and_then(|s| s.parse().ok())
        {
            Some(v) if v > 0 => v,
            _ => return RespValue::error("ERR invalid depth"),
        };
        let decay: f64 = match std::str::from_utf8(&ctx.args[5])
            .ok()
            .and_then(|s| s.parse().ok())
        {
            Some(v) if v > 0.0 && v <= 1.0 => v,
            _ => return RespValue::error("ERR invalid decay"),
        };
        (w, d, decay)
    } else {
        // Defaults: width=8*k, depth=7, decay=0.9
        (8 * k, 7, 0.9)
    };

    let db = ctx.db();
    if db.exists(&key) {
        return RespValue::error("ERR item exists");
    }

    let data = new_topk(k, width, depth, decay);
    db.set(key, RedisObject::String(Bytes::from(data)));
    RespValue::ok()
}

/// TOPK.ADD key item [item ...]
pub fn cmd_topk_add(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let items: Vec<String> = ctx.args[2..]
        .iter()
        .map(|b| String::from_utf8_lossy(b).to_string())
        .collect();

    let mut state = match get_or_error_topk(ctx, &key) {
        Ok(s) => s,
        Err(e) => return e,
    };

    let mut results = Vec::new();
    for item in &items {
        match state.add(item) {
            Some(evicted) => results.push(RespValue::bulk_string(evicted)),
            None => results.push(RespValue::Null),
        }
    }

    let data = state.to_bytes();
    ctx.db()
        .set_keep_ttl(key, RedisObject::String(Bytes::from(data)));
    RespValue::Array(results)
}

/// TOPK.QUERY key item [item ...]
pub fn cmd_topk_query(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let items: Vec<String> = ctx.args[2..]
        .iter()
        .map(|b| String::from_utf8_lossy(b).to_string())
        .collect();

    let state = match get_or_error_topk(ctx, &key) {
        Ok(s) => s,
        Err(e) => return e,
    };

    let results: Vec<RespValue> = items
        .iter()
        .map(|item| RespValue::integer(if state.query(item) { 1 } else { 0 }))
        .collect();

    RespValue::Array(results)
}

/// TOPK.LIST key [WITHCOUNT]
pub fn cmd_topk_list(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();

    let with_count = ctx.args.len() > 2
        && String::from_utf8_lossy(&ctx.args[2]).to_uppercase() == "WITHCOUNT";

    let state = match get_or_error_topk(ctx, &key) {
        Ok(s) => s,
        Err(e) => return e,
    };

    let items = state.list();

    if with_count {
        let mut results = Vec::new();
        for (name, count) in &items {
            results.push(RespValue::bulk_string(Bytes::from(name.clone())));
            results.push(RespValue::integer(*count as i64));
        }
        RespValue::Array(results)
    } else {
        let results: Vec<RespValue> = items
            .iter()
            .map(|(name, _)| RespValue::bulk_string(Bytes::from(name.clone())))
            .collect();
        RespValue::Array(results)
    }
}

/// TOPK.COUNT key item [item ...]
pub fn cmd_topk_count(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let items: Vec<String> = ctx.args[2..]
        .iter()
        .map(|b| String::from_utf8_lossy(b).to_string())
        .collect();

    let state = match get_or_error_topk(ctx, &key) {
        Ok(s) => s,
        Err(e) => return e,
    };

    let results: Vec<RespValue> = items
        .iter()
        .map(|item| RespValue::integer(state.count(item) as i64))
        .collect();

    RespValue::Array(results)
}

/// TOPK.INFO key
pub fn cmd_topk_info(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();

    let db = ctx.db();
    match db.get(&key) {
        Some(RedisObject::String(b)) => {
            if is_topk(b) {
                let k = topk_k(b);
                let width = topk_width(b);
                let depth = topk_depth(b);
                let decay = topk_decay(b);
                RespValue::Array(vec![
                    RespValue::bulk_string("k"),
                    RespValue::integer(k as i64),
                    RespValue::bulk_string("width"),
                    RespValue::integer(width as i64),
                    RespValue::bulk_string("depth"),
                    RespValue::integer(depth as i64),
                    RespValue::bulk_string("decay"),
                    RespValue::bulk_string(format!("{}", decay)),
                ])
            } else {
                RespValue::error("WRONGTYPE Key is not a valid Top-K")
            }
        }
        Some(_) => RespValue::wrong_type(),
        None => RespValue::error("ERR TOPK: key does not exist"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_basic() {
        let mut data = new_bloom(0.01, 100);
        assert!(is_bloom(&data));
        assert_eq!(bloom_num_items(&data), 0);

        // Add item
        let added = bloom_add(&mut data, b"hello");
        assert!(added);
        assert_eq!(bloom_num_items(&data), 1);

        // Check existence
        assert!(bloom_exists(&data, b"hello"));
        assert!(!bloom_exists(&data, b"world_that_does_not_exist_12345"));

        // Add same item again
        let added = bloom_add(&mut data, b"hello");
        assert!(!added); // probably existed
    }

    #[test]
    fn test_bloom_reserve_params() {
        let data = new_bloom(0.001, 1000);
        assert!(is_bloom(&data));
        assert_eq!(bloom_capacity(&data), 1000);
        let k = bloom_k(&data);
        // k = ceil(-log2(0.001)) = ceil(9.97) = 10
        assert_eq!(k, 10);
    }

    #[test]
    fn test_cms_basic() {
        let mut data = new_cms(100, 5);
        assert!(is_cms(&data));
        assert_eq!(cms_width(&data), 100);
        assert_eq!(cms_depth(&data), 5);
        assert_eq!(cms_total_count(&data), 0);

        // Increment
        cms_increment(&mut data, b"hello", 3);
        assert_eq!(cms_query(&data, b"hello"), 3);
        assert_eq!(cms_total_count(&data), 3);

        // Increment again
        cms_increment(&mut data, b"hello", 2);
        assert_eq!(cms_query(&data, b"hello"), 5);

        // Query non-existent
        assert_eq!(cms_query(&data, b"nonexistent"), 0);
    }

    #[test]
    fn test_topk_basic() {
        let data = new_topk(3, 24, 7, 0.9);
        assert!(is_topk(&data));
        assert_eq!(topk_k(&data), 3);
        assert_eq!(topk_width(&data), 24);
        assert_eq!(topk_depth(&data), 7);

        let mut state = TopKState::from_bytes(&data);
        assert_eq!(state.k, 3);

        // Add items
        state.add("apple");
        state.add("banana");
        state.add("cherry");
        assert!(state.query("apple"));
        assert!(state.query("banana"));
        assert!(state.query("cherry"));

        // Serialize and deserialize
        let bytes = state.to_bytes();
        let state2 = TopKState::from_bytes(&bytes);
        assert!(state2.query("apple"));
        assert_eq!(state2.heap.len(), 3);
    }

    #[test]
    fn test_bloom_hash_indices() {
        let indices = get_hash_indices(b"test", 1000, 7);
        assert_eq!(indices.len(), 7);
        for &idx in &indices {
            assert!(idx < 1000);
        }
    }

    #[test]
    fn test_cms_merge_logic() {
        let mut data1 = new_cms(50, 3);
        let mut data2 = new_cms(50, 3);

        cms_increment(&mut data1, b"item1", 5);
        cms_increment(&mut data2, b"item1", 3);

        // After merging, the count for item1 should be >= 8
        // (CMS might overcount but not undercount)
        let width = cms_width(&data1) as usize;
        let depth = cms_depth(&data1) as usize;

        let mut merged = new_cms(50, 3);
        for row in 0..depth {
            for col in 0..width {
                let v1 = cms_get_counter(&data1, row, col, width);
                let v2 = cms_get_counter(&data2, row, col, width);
                cms_set_counter(&mut merged, row, col, width, v1 + v2);
            }
        }

        let count = cms_query(&merged, b"item1");
        assert!(count >= 8);
    }
}
