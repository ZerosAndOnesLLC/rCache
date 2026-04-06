use bytes::Bytes;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{self, Write};
use std::path::Path;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::storage::types::{RedisObject, SortedSetData, StreamData, StreamId};
use crate::storage::Store;

/// RDB file magic header.
const RDB_MAGIC: &[u8] = b"REDIS0011";

// RDB opcodes
const RDB_OPCODE_AUX: u8 = 0xFA;
const RDB_OPCODE_SELECTDB: u8 = 0xFE;
const RDB_OPCODE_EXPIRETIME_MS: u8 = 0xFC;
const RDB_OPCODE_EOF: u8 = 0xFF;

// Type bytes
const RDB_TYPE_STRING: u8 = 0;
const RDB_TYPE_LIST: u8 = 1;
const RDB_TYPE_SET: u8 = 2;
const RDB_TYPE_SORTEDSET: u8 = 3;
const RDB_TYPE_HASH: u8 = 4;
const RDB_TYPE_STREAM: u8 = 5;
const RDB_TYPE_JSON: u8 = 6;

/// Save the entire store to an RDB file at the given path.
pub fn save(store: &Store, path: &Path) -> io::Result<()> {
    let temp_path = path.with_extension("rdb.tmp");
    let mut file = std::fs::File::create(&temp_path)?;

    // Magic header
    file.write_all(RDB_MAGIC)?;

    // Auxiliary fields
    write_aux_field(&mut file, "redis-ver", "7.2.0")?;
    write_aux_field(&mut file, "rcache-ver", env!("CARGO_PKG_VERSION"))?;

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    // Per-database sections
    for db_index in 0..store.db_count() {
        let db = store.db(db_index);
        if db.is_empty() {
            continue;
        }

        // DB selector
        file.write_all(&[RDB_OPCODE_SELECTDB])?;
        write_u32_le(&mut file, db_index as u32)?;

        // Collect expiry info
        let expires: HashMap<Bytes, Instant> = db.expires_iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();

        // Write each key-value pair
        for (key, value) in db.iter() {
            // Optional expiry
            if let Some(expire_at) = expires.get(key) {
                let now = Instant::now();
                if *expire_at > now {
                    let remaining = *expire_at - now;
                    let expire_ms = now_ms + remaining.as_millis() as u64;
                    file.write_all(&[RDB_OPCODE_EXPIRETIME_MS])?;
                    file.write_all(&expire_ms.to_le_bytes())?;
                }
            }

            // Type byte + key + value
            match value {
                RedisObject::String(s) => {
                    file.write_all(&[RDB_TYPE_STRING])?;
                    write_bytes(&mut file, key)?;
                    write_bytes(&mut file, s)?;
                }
                RedisObject::List(list) => {
                    file.write_all(&[RDB_TYPE_LIST])?;
                    write_bytes(&mut file, key)?;
                    write_u32_le(&mut file, list.len() as u32)?;
                    for item in list {
                        write_bytes(&mut file, item)?;
                    }
                }
                RedisObject::Set(set) => {
                    file.write_all(&[RDB_TYPE_SET])?;
                    write_bytes(&mut file, key)?;
                    write_u32_le(&mut file, set.len() as u32)?;
                    for member in set {
                        write_bytes(&mut file, member)?;
                    }
                }
                RedisObject::SortedSet(zset) => {
                    file.write_all(&[RDB_TYPE_SORTEDSET])?;
                    write_bytes(&mut file, key)?;
                    write_u32_le(&mut file, zset.members.len() as u32)?;
                    for (member, score) in &zset.members {
                        write_bytes(&mut file, member)?;
                        file.write_all(&score.to_le_bytes())?;
                    }
                }
                RedisObject::Hash(hash) => {
                    file.write_all(&[RDB_TYPE_HASH])?;
                    write_bytes(&mut file, key)?;
                    write_u32_le(&mut file, hash.len() as u32)?;
                    for (field, val) in hash {
                        write_bytes(&mut file, field)?;
                        write_bytes(&mut file, val)?;
                    }
                }
                RedisObject::Stream(stream) => {
                    file.write_all(&[RDB_TYPE_STREAM])?;
                    write_bytes(&mut file, key)?;
                    // Write last_id
                    file.write_all(&stream.last_id.ms.to_le_bytes())?;
                    file.write_all(&stream.last_id.seq.to_le_bytes())?;
                    // Write entries
                    write_u32_le(&mut file, stream.entries.len() as u32)?;
                    for (id, fields) in &stream.entries {
                        file.write_all(&id.ms.to_le_bytes())?;
                        file.write_all(&id.seq.to_le_bytes())?;
                        write_u32_le(&mut file, fields.len() as u32)?;
                        for (k, v) in fields {
                            write_bytes(&mut file, k)?;
                            write_bytes(&mut file, v)?;
                        }
                    }
                    // Write consumer groups
                    write_u32_le(&mut file, stream.groups.len() as u32)?;
                    for (name, group) in &stream.groups {
                        write_bytes(&mut file, name)?;
                        file.write_all(&group.last_delivered.ms.to_le_bytes())?;
                        file.write_all(&group.last_delivered.seq.to_le_bytes())?;
                        // Write PEL count (we skip persisting individual PEL entries for simplicity)
                        write_u32_le(&mut file, 0u32)?;
                        // Write consumers count
                        write_u32_le(&mut file, 0u32)?;
                    }
                }
                RedisObject::Json(value) => {
                    file.write_all(&[RDB_TYPE_JSON])?;
                    write_bytes(&mut file, key)?;
                    let json_str = serde_json::to_string(value)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                    write_bytes(&mut file, json_str.as_bytes())?;
                }
            }
        }
    }

    // EOF marker
    file.write_all(&[RDB_OPCODE_EOF])?;

    // CRC64 placeholder (8 bytes of zeros)
    file.write_all(&[0u8; 8])?;

    file.flush()?;
    drop(file);

    // Atomically rename temp file to target
    std::fs::rename(&temp_path, path)?;

    Ok(())
}

/// Load an RDB file into a new Store.
pub fn load(path: &Path, num_databases: usize) -> io::Result<Store> {
    let data = std::fs::read(path)?;
    if data.len() < RDB_MAGIC.len() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "RDB file too short"));
    }

    if &data[..RDB_MAGIC.len()] != RDB_MAGIC {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid RDB magic header"));
    }

    let mut store = Store::new(num_databases);
    let mut cursor = RDB_MAGIC.len();
    let mut current_db: usize = 0;

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    loop {
        if cursor >= data.len() {
            break;
        }

        let opcode = data[cursor];
        cursor += 1;

        match opcode {
            RDB_OPCODE_AUX => {
                // Read auxiliary key and value, skip them
                let (_, c1) = read_bytes(&data, cursor)?;
                cursor = c1;
                let (_, c2) = read_bytes(&data, cursor)?;
                cursor = c2;
            }
            RDB_OPCODE_SELECTDB => {
                let (db_idx, c) = read_u32_le(&data, cursor)?;
                cursor = c;
                current_db = db_idx as usize;
                if current_db >= num_databases {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("DB index {} out of range", current_db),
                    ));
                }
            }
            RDB_OPCODE_EOF => {
                // Skip CRC64 (8 bytes) if present
                break;
            }
            RDB_OPCODE_EXPIRETIME_MS => {
                // Read expiry timestamp, then the type byte and entry
                if cursor + 8 > data.len() {
                    return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated expiry"));
                }
                let expire_ms = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
                cursor += 8;

                // Now read the actual type + key + value
                if cursor >= data.len() {
                    return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated entry after expiry"));
                }
                let type_byte = data[cursor];
                cursor += 1;

                let (key, value, new_cursor) = read_entry(&data, cursor, type_byte)?;
                cursor = new_cursor;

                let db = store.db_mut(current_db);
                db.set_raw(key.clone(), value);

                // Set expiry if it's still in the future
                if expire_ms > now_ms {
                    let remaining = Duration::from_millis(expire_ms - now_ms);
                    db.set_expire_raw(key, Instant::now() + remaining);
                } else {
                    // Key has expired, remove it
                    db.remove(&key);
                }
            }
            type_byte @ (RDB_TYPE_STRING | RDB_TYPE_LIST | RDB_TYPE_SET | RDB_TYPE_SORTEDSET | RDB_TYPE_HASH | RDB_TYPE_STREAM | RDB_TYPE_JSON) => {
                let (key, value, new_cursor) = read_entry(&data, cursor, type_byte)?;
                cursor = new_cursor;
                store.db_mut(current_db).set_raw(key, value);
            }
            unknown => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Unknown RDB opcode: 0x{:02X}", unknown),
                ));
            }
        }
    }

    Ok(store)
}

/// Read a single key-value entry from the data buffer.
fn read_entry(data: &[u8], cursor: usize, type_byte: u8) -> io::Result<(Bytes, RedisObject, usize)> {
    let (key, mut cursor) = read_bytes(data, cursor)?;

    let value = match type_byte {
        RDB_TYPE_STRING => {
            let (val, c) = read_bytes(data, cursor)?;
            cursor = c;
            RedisObject::String(val)
        }
        RDB_TYPE_LIST => {
            let (count, c) = read_u32_le(data, cursor)?;
            cursor = c;
            let mut list = VecDeque::with_capacity(count as usize);
            for _ in 0..count {
                let (item, c) = read_bytes(data, cursor)?;
                cursor = c;
                list.push_back(item);
            }
            RedisObject::List(list)
        }
        RDB_TYPE_SET => {
            let (count, c) = read_u32_le(data, cursor)?;
            cursor = c;
            let mut set = HashSet::with_capacity(count as usize);
            for _ in 0..count {
                let (member, c) = read_bytes(data, cursor)?;
                cursor = c;
                set.insert(member);
            }
            RedisObject::Set(set)
        }
        RDB_TYPE_SORTEDSET => {
            let (count, c) = read_u32_le(data, cursor)?;
            cursor = c;
            let mut zset = SortedSetData::new();
            for _ in 0..count {
                let (member, c) = read_bytes(data, cursor)?;
                cursor = c;
                if cursor + 8 > data.len() {
                    return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated score"));
                }
                let score = f64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
                cursor += 8;
                zset.insert(member, score);
            }
            RedisObject::SortedSet(zset)
        }
        RDB_TYPE_HASH => {
            let (count, c) = read_u32_le(data, cursor)?;
            cursor = c;
            let mut hash = HashMap::with_capacity(count as usize);
            for _ in 0..count {
                let (field, c) = read_bytes(data, cursor)?;
                cursor = c;
                let (val, c) = read_bytes(data, cursor)?;
                cursor = c;
                hash.insert(field, val);
            }
            RedisObject::Hash(hash)
        }
        RDB_TYPE_STREAM => {
            // Read last_id
            if cursor + 16 > data.len() {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated stream last_id"));
            }
            let last_ms = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
            cursor += 8;
            let last_seq = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
            cursor += 8;

            let mut stream = StreamData::new();
            stream.last_id = StreamId { ms: last_ms, seq: last_seq };

            // Read entries
            let (entry_count, c) = read_u32_le(data, cursor)?;
            cursor = c;
            for _ in 0..entry_count {
                if cursor + 16 > data.len() {
                    return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated stream entry id"));
                }
                let ms = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
                cursor += 8;
                let seq = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
                cursor += 8;
                let (field_count, c) = read_u32_le(data, cursor)?;
                cursor = c;
                let mut fields = Vec::with_capacity(field_count as usize);
                for _ in 0..field_count {
                    let (k, c) = read_bytes(data, cursor)?;
                    cursor = c;
                    let (v, c) = read_bytes(data, cursor)?;
                    cursor = c;
                    fields.push((k, v));
                }
                stream.entries.insert(StreamId { ms, seq }, fields);
            }

            // Read consumer groups
            let (group_count, c) = read_u32_le(data, cursor)?;
            cursor = c;
            for _ in 0..group_count {
                let (name, c) = read_bytes(data, cursor)?;
                cursor = c;
                if cursor + 16 > data.len() {
                    return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated group last_delivered"));
                }
                let ld_ms = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
                cursor += 8;
                let ld_seq = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
                cursor += 8;
                // Skip PEL entries
                let (pel_count, c) = read_u32_le(data, cursor)?;
                cursor = c;
                let _ = pel_count; // PEL entries not persisted, just skip count
                // Skip consumer entries
                let (consumer_count, c) = read_u32_le(data, cursor)?;
                cursor = c;
                let _ = consumer_count;

                let group = crate::storage::types::ConsumerGroup::new(StreamId { ms: ld_ms, seq: ld_seq });
                stream.groups.insert(name, group);
            }

            RedisObject::Stream(stream)
        }
        RDB_TYPE_JSON => {
            let (json_bytes, c) = read_bytes(data, cursor)?;
            cursor = c;
            let json_str = std::str::from_utf8(&json_bytes)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let value: serde_json::Value = serde_json::from_str(json_str)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            RedisObject::Json(value)
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unknown type byte: {}", type_byte),
            ));
        }
    };

    Ok((key, value, cursor))
}

// --- Binary helpers ---

fn write_bytes(w: &mut impl Write, data: &[u8]) -> io::Result<()> {
    write_u32_le(w, data.len() as u32)?;
    w.write_all(data)?;
    Ok(())
}

fn write_u32_le(w: &mut impl Write, val: u32) -> io::Result<()> {
    w.write_all(&val.to_le_bytes())
}

fn write_aux_field(w: &mut impl Write, key: &str, value: &str) -> io::Result<()> {
    w.write_all(&[RDB_OPCODE_AUX])?;
    write_bytes(w, key.as_bytes())?;
    write_bytes(w, value.as_bytes())?;
    Ok(())
}

fn read_bytes(data: &[u8], cursor: usize) -> io::Result<(Bytes, usize)> {
    let (len, cursor) = read_u32_le(data, cursor)?;
    let len = len as usize;
    if cursor + len > data.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated bytes"));
    }
    let bytes = Bytes::copy_from_slice(&data[cursor..cursor + len]);
    Ok((bytes, cursor + len))
}

fn read_u32_le(data: &[u8], cursor: usize) -> io::Result<(u32, usize)> {
    if cursor + 4 > data.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated u32"));
    }
    let val = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap());
    Ok((val, cursor + 4))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rdb_save_load_strings() {
        let dir = std::env::temp_dir().join("rcache_test_rdb_strings");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("test.rdb");

        let mut store = Store::new(16);
        store.db_mut(0).set_raw(
            Bytes::from("hello"),
            RedisObject::String(Bytes::from("world")),
        );
        store.db_mut(0).set_raw(
            Bytes::from("foo"),
            RedisObject::String(Bytes::from("bar")),
        );

        save(&store, &path).unwrap();

        let loaded = load(&path, 16).unwrap();
        assert_eq!(loaded.db(0).len(), 2);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_rdb_save_load_list() {
        let dir = std::env::temp_dir().join("rcache_test_rdb_list");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("test.rdb");

        let mut store = Store::new(16);
        let mut list = VecDeque::new();
        list.push_back(Bytes::from("a"));
        list.push_back(Bytes::from("b"));
        list.push_back(Bytes::from("c"));
        store.db_mut(0).set_raw(Bytes::from("mylist"), RedisObject::List(list));

        save(&store, &path).unwrap();

        let loaded = load(&path, 16).unwrap();
        assert_eq!(loaded.db(0).len(), 1);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_rdb_save_load_set() {
        let dir = std::env::temp_dir().join("rcache_test_rdb_set");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("test.rdb");

        let mut store = Store::new(16);
        let mut set = HashSet::new();
        set.insert(Bytes::from("x"));
        set.insert(Bytes::from("y"));
        store.db_mut(0).set_raw(Bytes::from("myset"), RedisObject::Set(set));

        save(&store, &path).unwrap();

        let loaded = load(&path, 16).unwrap();
        assert_eq!(loaded.db(0).len(), 1);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_rdb_save_load_hash() {
        let dir = std::env::temp_dir().join("rcache_test_rdb_hash");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("test.rdb");

        let mut store = Store::new(16);
        let mut hash = HashMap::new();
        hash.insert(Bytes::from("field1"), Bytes::from("val1"));
        hash.insert(Bytes::from("field2"), Bytes::from("val2"));
        store.db_mut(0).set_raw(Bytes::from("myhash"), RedisObject::Hash(hash));

        save(&store, &path).unwrap();

        let loaded = load(&path, 16).unwrap();
        assert_eq!(loaded.db(0).len(), 1);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_rdb_save_load_sorted_set() {
        let dir = std::env::temp_dir().join("rcache_test_rdb_zset");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("test.rdb");

        let mut store = Store::new(16);
        let mut zset = SortedSetData::new();
        zset.insert(Bytes::from("alice"), 1.0);
        zset.insert(Bytes::from("bob"), 2.5);
        store.db_mut(0).set_raw(Bytes::from("myzset"), RedisObject::SortedSet(zset));

        save(&store, &path).unwrap();

        let loaded = load(&path, 16).unwrap();
        assert_eq!(loaded.db(0).len(), 1);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_rdb_save_load_with_expiry() {
        let dir = std::env::temp_dir().join("rcache_test_rdb_expiry");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("test.rdb");

        let mut store = Store::new(16);
        store.db_mut(0).set_raw(
            Bytes::from("key1"),
            RedisObject::String(Bytes::from("val1")),
        );
        // Set an expiry 1 hour in the future
        store.db_mut(0).set_expire_raw(
            Bytes::from("key1"),
            Instant::now() + Duration::from_secs(3600),
        );
        store.db_mut(0).set_raw(
            Bytes::from("key2"),
            RedisObject::String(Bytes::from("val2")),
        );

        save(&store, &path).unwrap();

        let loaded = load(&path, 16).unwrap();
        assert_eq!(loaded.db(0).len(), 2);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_rdb_save_load_multi_db() {
        let dir = std::env::temp_dir().join("rcache_test_rdb_multidb");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("test.rdb");

        let mut store = Store::new(16);
        store.db_mut(0).set_raw(
            Bytes::from("k0"),
            RedisObject::String(Bytes::from("v0")),
        );
        store.db_mut(3).set_raw(
            Bytes::from("k3"),
            RedisObject::String(Bytes::from("v3")),
        );

        save(&store, &path).unwrap();

        let loaded = load(&path, 16).unwrap();
        assert_eq!(loaded.db(0).len(), 1);
        assert_eq!(loaded.db(1).len(), 0);
        assert_eq!(loaded.db(3).len(), 1);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
