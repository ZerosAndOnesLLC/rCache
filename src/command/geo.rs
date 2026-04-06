use bytes::Bytes;
use crate::protocol::RespValue;
use crate::storage::RedisObject;
use crate::storage::types::SortedSetData;
use super::registry::CommandContext;

const EARTH_RADIUS_M: f64 = 6372797.560856;
const D_R: f64 = std::f64::consts::PI / 180.0;

// Geohash encoding: interleave longitude and latitude bits into a 52-bit integer,
// then store as the score of a sorted set member.

fn geohash_encode(longitude: f64, latitude: f64) -> f64 {
    // Normalize to [0,1] range
    let lat = (latitude + 90.0) / 180.0;
    let lon = (longitude + 180.0) / 360.0;

    // Interleave 26 bits of longitude and 26 bits of latitude = 52 bits total
    let mut lat_bits = (lat * (1u64 << 26) as f64) as u64;
    let mut lon_bits = (lon * (1u64 << 26) as f64) as u64;

    lat_bits = lat_bits.min((1u64 << 26) - 1);
    lon_bits = lon_bits.min((1u64 << 26) - 1);

    let mut hash: u64 = 0;
    for i in 0..26 {
        // Longitude bits in even positions, latitude in odd
        hash |= ((lon_bits >> (25 - i)) & 1) << (51 - 2 * i);
        hash |= ((lat_bits >> (25 - i)) & 1) << (50 - 2 * i);
    }

    hash as f64
}

fn geohash_decode(hash: f64) -> (f64, f64) {
    let hash = hash as u64;
    let mut lat_bits: u64 = 0;
    let mut lon_bits: u64 = 0;

    for i in 0..26 {
        lon_bits |= ((hash >> (51 - 2 * i)) & 1) << (25 - i);
        lat_bits |= ((hash >> (50 - 2 * i)) & 1) << (25 - i);
    }

    let latitude = (lat_bits as f64 / (1u64 << 26) as f64) * 180.0 - 90.0;
    let longitude = (lon_bits as f64 / (1u64 << 26) as f64) * 360.0 - 180.0;

    (longitude, latitude)
}

fn geohash_string(hash: f64) -> String {
    // Convert to base32 geohash string (11 chars)
    let hash = hash as u64;
    let alphabet = b"0123456789bcdefghjkmnpqrstuvwxyz";
    let mut result = String::with_capacity(11);

    // Take top 55 bits, group into 5-bit chunks
    for i in 0..11 {
        let shift = 52 - (i + 1) * 5;
        let idx = if shift >= 0 {
            ((hash >> shift as u64) & 0x1f) as usize
        } else {
            ((hash << (-shift) as u64) & 0x1f) as usize
        };
        let idx = idx.min(31);
        result.push(alphabet[idx] as char);
    }

    result
}

fn haversine_distance(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let lat1_r = lat1 * D_R;
    let lat2_r = lat2 * D_R;
    let dlat = (lat2 - lat1) * D_R;
    let dlon = (lon2 - lon1) * D_R;

    let a = (dlat / 2.0).sin().powi(2) + lat1_r.cos() * lat2_r.cos() * (dlon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().asin();

    EARTH_RADIUS_M * c
}

fn convert_distance(meters: f64, unit: &str) -> f64 {
    match unit {
        "km" | "KM" => meters / 1000.0,
        "mi" | "MI" => meters / 1609.344,
        "ft" | "FT" => meters / 0.3048,
        _ => meters, // m
    }
}

fn convert_to_meters(distance: f64, unit: &str) -> f64 {
    match unit {
        "km" | "KM" => distance * 1000.0,
        "mi" | "MI" => distance * 1609.344,
        "ft" | "FT" => distance * 0.3048,
        _ => distance, // m
    }
}

fn get_zset<'a>(ctx: &'a mut CommandContext, key: &Bytes) -> Result<Option<&'a SortedSetData>, RespValue> {
    match ctx.db().get(key) {
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

pub fn cmd_geoadd(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();

    let mut nx = false;
    let mut xx = false;
    let mut ch = false;

    let mut i = 2;
    while i < ctx.args.len() {
        let opt = String::from_utf8_lossy(&ctx.args[i]).to_uppercase();
        match opt.as_str() {
            "NX" => { nx = true; i += 1; }
            "XX" => { xx = true; i += 1; }
            "CH" => { ch = true; i += 1; }
            _ => break,
        }
    }

    if (ctx.args.len() - i) % 3 != 0 {
        return RespValue::wrong_arity("geoadd");
    }

    let mut triples = Vec::new();
    while i + 2 < ctx.args.len() {
        let lon: f64 = match String::from_utf8_lossy(&ctx.args[i]).parse() {
            Ok(v) => v,
            Err(_) => return RespValue::error("ERR value is not a valid float"),
        };
        let lat: f64 = match String::from_utf8_lossy(&ctx.args[i + 1]).parse() {
            Ok(v) => v,
            Err(_) => return RespValue::error("ERR value is not a valid float"),
        };

        if lon < -180.0 || lon > 180.0 || lat < -85.05112878 || lat > 85.05112878 {
            return RespValue::error("ERR invalid longitude,latitude pair");
        }

        let member = ctx.args[i + 2].clone();
        triples.push((lon, lat, member));
        i += 3;
    }

    let zset = match ensure_zset(ctx, &key) {
        Ok(z) => z,
        Err(e) => return e,
    };

    let mut added = 0i64;
    let mut changed = 0i64;

    for (lon, lat, member) in triples {
        let score = geohash_encode(lon, lat);
        let exists = zset.score(&member).is_some();

        if nx && exists {
            continue;
        }
        if xx && !exists {
            continue;
        }

        let was_new = zset.insert(member, score);
        if was_new {
            added += 1;
            changed += 1;
        } else {
            changed += 1;
        }
    }

    if ch { RespValue::integer(changed) } else { RespValue::integer(added) }
}

pub fn cmd_geopos(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let members: Vec<Bytes> = ctx.args[2..].to_vec();

    match get_zset(ctx, &key) {
        Ok(Some(zset)) => {
            let results: Vec<RespValue> = members.iter().map(|m| {
                match zset.score(m) {
                    Some(score) => {
                        let (lon, lat) = geohash_decode(score);
                        RespValue::array(vec![
                            RespValue::bulk_string(Bytes::from(format!("{:.6}", lon))),
                            RespValue::bulk_string(Bytes::from(format!("{:.6}", lat))),
                        ])
                    }
                    None => RespValue::Null,
                }
            }).collect();
            RespValue::array(results)
        }
        Ok(None) => {
            RespValue::array(members.iter().map(|_| RespValue::Null).collect())
        }
        Err(e) => e,
    }
}

pub fn cmd_geodist(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let member1 = ctx.args[2].clone();
    let member2 = ctx.args[3].clone();
    let unit = if ctx.args.len() > 4 {
        String::from_utf8_lossy(&ctx.args[4]).to_lowercase()
    } else {
        "m".into()
    };

    match get_zset(ctx, &key) {
        Ok(Some(zset)) => {
            let score1 = match zset.score(&member1) {
                Some(s) => s,
                None => return RespValue::Null,
            };
            let score2 = match zset.score(&member2) {
                Some(s) => s,
                None => return RespValue::Null,
            };

            let (lon1, lat1) = geohash_decode(score1);
            let (lon2, lat2) = geohash_decode(score2);
            let dist = haversine_distance(lon1, lat1, lon2, lat2);
            let converted = convert_distance(dist, &unit);
            RespValue::bulk_string(Bytes::from(format!("{:.4}", converted)))
        }
        Ok(None) => RespValue::Null,
        Err(e) => e,
    }
}

pub fn cmd_geosearch(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();

    let mut from_lon: Option<f64> = None;
    let mut from_lat: Option<f64> = None;
    let mut from_member: Option<Bytes> = None;
    let mut by_radius: Option<(f64, String)> = None;
    let mut by_box: Option<(f64, f64, String)> = None;
    let mut asc = false;
    let mut desc = false;
    let mut count: Option<usize> = None;
    let mut any = false;
    let mut withcoord = false;
    let mut withdist = false;
    let mut withhash = false;

    let mut i = 2;
    while i < ctx.args.len() {
        let opt = String::from_utf8_lossy(&ctx.args[i]).to_uppercase();
        match opt.as_str() {
            "FROMMEMBER" => {
                i += 1;
                if i >= ctx.args.len() { return RespValue::error("ERR syntax error"); }
                from_member = Some(ctx.args[i].clone());
            }
            "FROMLONLAT" => {
                i += 1;
                if i + 1 >= ctx.args.len() { return RespValue::error("ERR syntax error"); }
                from_lon = Some(match String::from_utf8_lossy(&ctx.args[i]).parse() {
                    Ok(v) => v,
                    Err(_) => return RespValue::error("ERR value is not a valid float"),
                });
                i += 1;
                from_lat = Some(match String::from_utf8_lossy(&ctx.args[i]).parse() {
                    Ok(v) => v,
                    Err(_) => return RespValue::error("ERR value is not a valid float"),
                });
            }
            "BYRADIUS" => {
                i += 1;
                if i + 1 >= ctx.args.len() { return RespValue::error("ERR syntax error"); }
                let radius: f64 = match String::from_utf8_lossy(&ctx.args[i]).parse() {
                    Ok(v) => v,
                    Err(_) => return RespValue::error("ERR value is not a valid float"),
                };
                i += 1;
                let unit = String::from_utf8_lossy(&ctx.args[i]).to_lowercase();
                by_radius = Some((radius, unit));
            }
            "BYBOX" => {
                i += 1;
                if i + 2 >= ctx.args.len() { return RespValue::error("ERR syntax error"); }
                let width: f64 = match String::from_utf8_lossy(&ctx.args[i]).parse() {
                    Ok(v) => v,
                    Err(_) => return RespValue::error("ERR value is not a valid float"),
                };
                i += 1;
                let height: f64 = match String::from_utf8_lossy(&ctx.args[i]).parse() {
                    Ok(v) => v,
                    Err(_) => return RespValue::error("ERR value is not a valid float"),
                };
                i += 1;
                let unit = String::from_utf8_lossy(&ctx.args[i]).to_lowercase();
                by_box = Some((width, height, unit));
            }
            "ASC" => asc = true,
            "DESC" => desc = true,
            "COUNT" => {
                i += 1;
                if i >= ctx.args.len() { return RespValue::error("ERR syntax error"); }
                count = Some(match String::from_utf8_lossy(&ctx.args[i]).parse() {
                    Ok(v) => v,
                    Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
                });
                // Check for ANY
                if i + 1 < ctx.args.len() && String::from_utf8_lossy(&ctx.args[i + 1]).to_uppercase() == "ANY" {
                    any = true;
                    i += 1;
                }
            }
            "WITHCOORD" => withcoord = true,
            "WITHDIST" => withdist = true,
            "WITHHASH" => withhash = true,
            _ => return RespValue::error("ERR syntax error"),
        }
        i += 1;
    }

    // Resolve center point
    let (center_lon, center_lat) = if let Some(member) = from_member {
        match get_zset(ctx, &key) {
            Ok(Some(zset)) => match zset.score(&member) {
                Some(score) => geohash_decode(score),
                None => return RespValue::error("ERR could not decode requested zset member"),
            },
            Ok(None) => return RespValue::array(vec![]),
            Err(e) => return e,
        }
    } else if let (Some(lon), Some(lat)) = (from_lon, from_lat) {
        (lon, lat)
    } else {
        return RespValue::error("ERR exactly one of FROMMEMBER or FROMLONLAT must be specified");
    };

    // Get all members and filter by shape
    let all_items = match get_zset(ctx, &key) {
        Ok(Some(zset)) => zset.range_by_index(0, -1),
        Ok(None) => return RespValue::array(vec![]),
        Err(e) => return e,
    };

    let mut results: Vec<(Bytes, f64, f64, f64, f64)> = Vec::new(); // member, score, lon, lat, dist

    for (member, score) in &all_items {
        let (lon, lat) = geohash_decode(*score);
        let dist = haversine_distance(center_lon, center_lat, lon, lat);

        let in_shape = if let Some((radius, ref unit)) = by_radius {
            let radius_m = convert_to_meters(radius, unit);
            dist <= radius_m
        } else if let Some((width, height, ref unit)) = by_box {
            let half_w = convert_to_meters(width / 2.0, unit);
            let half_h = convert_to_meters(height / 2.0, unit);
            // Approximate box check using haversine for lat and lon separately
            let lon_dist = haversine_distance(center_lon, center_lat, lon, center_lat);
            let lat_dist = haversine_distance(center_lon, center_lat, center_lon, lat);
            lon_dist <= half_w && lat_dist <= half_h
        } else {
            return RespValue::error("ERR exactly one of BYRADIUS or BYBOX must be specified");
        };

        if in_shape {
            results.push((member.clone(), *score, lon, lat, dist));
        }
    }

    // Sort
    if asc {
        results.sort_by(|a, b| a.4.partial_cmp(&b.4).unwrap_or(std::cmp::Ordering::Equal));
    } else if desc {
        results.sort_by(|a, b| b.4.partial_cmp(&a.4).unwrap_or(std::cmp::Ordering::Equal));
    }

    // Limit
    if let Some(c) = count {
        results.truncate(c);
    }

    // Format response
    let unit_str = if let Some((_, ref unit)) = by_radius {
        unit.clone()
    } else if let Some((_, _, ref unit)) = by_box {
        unit.clone()
    } else {
        "m".to_string()
    };

    let has_extras = withcoord || withdist || withhash;

    let items: Vec<RespValue> = results.iter().map(|(member, score, lon, lat, dist)| {
        if has_extras {
            let mut item = vec![RespValue::bulk_string(member.clone())];
            if withdist {
                let d = convert_distance(*dist, &unit_str);
                item.push(RespValue::bulk_string(Bytes::from(format!("{:.4}", d))));
            }
            if withhash {
                item.push(RespValue::integer(*score as i64));
            }
            if withcoord {
                item.push(RespValue::array(vec![
                    RespValue::bulk_string(Bytes::from(format!("{:.6}", lon))),
                    RespValue::bulk_string(Bytes::from(format!("{:.6}", lat))),
                ]));
            }
            RespValue::array(item)
        } else {
            RespValue::bulk_string(member.clone())
        }
    }).collect();

    let _ = any; // suppress unused warning - ANY is an optimization hint
    RespValue::array(items)
}

pub fn cmd_geosearchstore(ctx: &mut CommandContext) -> RespValue {
    let dest = ctx.args[1].clone();
    let src = ctx.args[2].clone();

    // Parse the same options as GEOSEARCH but starting from args[3] effectively
    // We'll build a modified args for geosearch logic
    let mut from_lon: Option<f64> = None;
    let mut from_lat: Option<f64> = None;
    let mut from_member: Option<Bytes> = None;
    let mut by_radius: Option<(f64, String)> = None;
    let mut by_box: Option<(f64, f64, String)> = None;
    let mut asc = false;
    let mut desc = false;
    let mut count: Option<usize> = None;
    let mut storedist = false;

    let mut i = 3;
    while i < ctx.args.len() {
        let opt = String::from_utf8_lossy(&ctx.args[i]).to_uppercase();
        match opt.as_str() {
            "FROMMEMBER" => {
                i += 1;
                if i >= ctx.args.len() { return RespValue::error("ERR syntax error"); }
                from_member = Some(ctx.args[i].clone());
            }
            "FROMLONLAT" => {
                i += 1;
                if i + 1 >= ctx.args.len() { return RespValue::error("ERR syntax error"); }
                from_lon = Some(match String::from_utf8_lossy(&ctx.args[i]).parse() {
                    Ok(v) => v,
                    Err(_) => return RespValue::error("ERR value is not a valid float"),
                });
                i += 1;
                from_lat = Some(match String::from_utf8_lossy(&ctx.args[i]).parse() {
                    Ok(v) => v,
                    Err(_) => return RespValue::error("ERR value is not a valid float"),
                });
            }
            "BYRADIUS" => {
                i += 1;
                if i + 1 >= ctx.args.len() { return RespValue::error("ERR syntax error"); }
                let radius: f64 = match String::from_utf8_lossy(&ctx.args[i]).parse() {
                    Ok(v) => v,
                    Err(_) => return RespValue::error("ERR value is not a valid float"),
                };
                i += 1;
                let unit = String::from_utf8_lossy(&ctx.args[i]).to_lowercase();
                by_radius = Some((radius, unit));
            }
            "BYBOX" => {
                i += 1;
                if i + 2 >= ctx.args.len() { return RespValue::error("ERR syntax error"); }
                let width: f64 = match String::from_utf8_lossy(&ctx.args[i]).parse() {
                    Ok(v) => v,
                    Err(_) => return RespValue::error("ERR value is not a valid float"),
                };
                i += 1;
                let height: f64 = match String::from_utf8_lossy(&ctx.args[i]).parse() {
                    Ok(v) => v,
                    Err(_) => return RespValue::error("ERR value is not a valid float"),
                };
                i += 1;
                let unit = String::from_utf8_lossy(&ctx.args[i]).to_lowercase();
                by_box = Some((width, height, unit));
            }
            "ASC" => asc = true,
            "DESC" => desc = true,
            "COUNT" => {
                i += 1;
                if i >= ctx.args.len() { return RespValue::error("ERR syntax error"); }
                count = Some(match String::from_utf8_lossy(&ctx.args[i]).parse() {
                    Ok(v) => v,
                    Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
                });
                if i + 1 < ctx.args.len() && String::from_utf8_lossy(&ctx.args[i + 1]).to_uppercase() == "ANY" {
                    i += 1;
                }
            }
            "STOREDIST" => storedist = true,
            _ => return RespValue::error("ERR syntax error"),
        }
        i += 1;
    }

    let (center_lon, center_lat) = if let Some(member) = from_member {
        match get_zset(ctx, &src) {
            Ok(Some(zset)) => match zset.score(&member) {
                Some(score) => geohash_decode(score),
                None => return RespValue::error("ERR could not decode requested zset member"),
            },
            Ok(None) => {
                ctx.db().remove(&dest);
                return RespValue::integer(0);
            }
            Err(e) => return e,
        }
    } else if let (Some(lon), Some(lat)) = (from_lon, from_lat) {
        (lon, lat)
    } else {
        return RespValue::error("ERR exactly one of FROMMEMBER or FROMLONLAT must be specified");
    };

    let all_items = match get_zset(ctx, &src) {
        Ok(Some(zset)) => zset.range_by_index(0, -1),
        Ok(None) => {
            ctx.db().remove(&dest);
            return RespValue::integer(0);
        }
        Err(e) => return e,
    };

    let unit_str = if let Some((_, ref unit)) = by_radius {
        unit.clone()
    } else if let Some((_, _, ref unit)) = by_box {
        unit.clone()
    } else {
        "m".to_string()
    };

    let mut results: Vec<(Bytes, f64, f64)> = Vec::new(); // member, score, dist

    for (member, score) in &all_items {
        let (lon, lat) = geohash_decode(*score);
        let dist = haversine_distance(center_lon, center_lat, lon, lat);

        let in_shape = if let Some((radius, ref unit)) = by_radius {
            dist <= convert_to_meters(radius, unit)
        } else if let Some((width, height, ref unit)) = by_box {
            let half_w = convert_to_meters(width / 2.0, unit);
            let half_h = convert_to_meters(height / 2.0, unit);
            let lon_dist = haversine_distance(center_lon, center_lat, lon, center_lat);
            let lat_dist = haversine_distance(center_lon, center_lat, center_lon, lat);
            lon_dist <= half_w && lat_dist <= half_h
        } else {
            return RespValue::error("ERR exactly one of BYRADIUS or BYBOX must be specified");
        };

        if in_shape {
            results.push((member.clone(), *score, dist));
        }
    }

    if asc {
        results.sort_by(|a, b| a.2.partial_cmp(&b.2).unwrap_or(std::cmp::Ordering::Equal));
    } else if desc {
        results.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));
    }

    if let Some(c) = count {
        results.truncate(c);
    }

    let mut dest_zset = SortedSetData::new();
    for (member, score, dist) in &results {
        let store_score = if storedist {
            convert_distance(*dist, &unit_str)
        } else {
            *score
        };
        dest_zset.insert(member.clone(), store_score);
    }

    let len = dest_zset.len() as i64;
    if dest_zset.is_empty() {
        ctx.db().remove(&dest);
    } else {
        ctx.db().set(dest, RedisObject::SortedSet(dest_zset));
    }
    RespValue::integer(len)
}

pub fn cmd_geohash(ctx: &mut CommandContext) -> RespValue {
    let key = ctx.args[1].clone();
    let members: Vec<Bytes> = ctx.args[2..].to_vec();

    match get_zset(ctx, &key) {
        Ok(Some(zset)) => {
            let results: Vec<RespValue> = members.iter().map(|m| {
                match zset.score(m) {
                    Some(score) => RespValue::bulk_string(Bytes::from(geohash_string(score))),
                    None => RespValue::Null,
                }
            }).collect();
            RespValue::array(results)
        }
        Ok(None) => RespValue::array(members.iter().map(|_| RespValue::Null).collect()),
        Err(e) => e,
    }
}
