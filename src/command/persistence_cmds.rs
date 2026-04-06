use std::time::{SystemTime, UNIX_EPOCH};
use crate::protocol::RespValue;
use super::registry::CommandContext;

/// SAVE command - blocking foreground RDB save.
pub fn cmd_save(ctx: &mut CommandContext) -> RespValue {
    let path = std::path::Path::new("dump.rdb");
    match crate::persistence::rdb::save(ctx.store, path) {
        Ok(()) => {
            ctx.store.last_save = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            RespValue::ok()
        }
        Err(e) => RespValue::error(format!("ERR saving RDB: {}", e)),
    }
}

/// BGSAVE command - background RDB save.
/// We clone the store data, then serialize in a background tokio task.
/// The actual spawning is done in the connection handler since we need the Arc<SharedState>.
/// Here we just do the foreground clone + save as a simplification since
/// command handlers are synchronous. The connection handler will release the lock
/// after this returns.
pub fn cmd_bgsave(ctx: &mut CommandContext) -> RespValue {
    // We mark that a background save was requested.
    // The actual background save is coordinated by returning a special marker.
    // For simplicity, we do the snapshot (clone data) synchronously here,
    // then the save happens. Since command handlers hold the store lock,
    // we do the save inline (store is already locked).
    let path = std::path::Path::new("dump.rdb");
    match crate::persistence::rdb::save(ctx.store, path) {
        Ok(()) => {
            ctx.store.last_save = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            RespValue::simple_string("Background saving started")
        }
        Err(e) => RespValue::error(format!("ERR background save failed: {}", e)),
    }
}

/// LASTSAVE command - return unix timestamp of last successful save.
pub fn cmd_lastsave(ctx: &mut CommandContext) -> RespValue {
    RespValue::integer(ctx.store.last_save as i64)
}

/// BGREWRITEAOF command - rewrite AOF from current state.
/// Similar to BGSAVE, we do this synchronously since we hold the store lock.
/// The actual AOF rewrite is done via the AOF writer.
pub fn cmd_bgrewriteaof(ctx: &mut CommandContext) -> RespValue {
    // We generate the rewritten AOF content here.
    // The actual file write is coordinated via SharedState's aof_writer.
    // Since we can't access SharedState from the command handler directly,
    // we write a temp AOF from the store and let it be picked up.
    let aof_path = std::path::Path::new("appendonly.aof");
    let temp_path = aof_path.with_extension("aof.tmp");

    match rewrite_aof_from_store(ctx.store, &temp_path) {
        Ok(()) => {
            // Atomically replace
            if let Err(e) = std::fs::rename(&temp_path, aof_path) {
                return RespValue::error(format!("ERR AOF rewrite rename failed: {}", e));
            }
            RespValue::simple_string("Background append only file rewriting started")
        }
        Err(e) => RespValue::error(format!("ERR AOF rewrite failed: {}", e)),
    }
}

/// Helper to write all store data as AOF commands.
fn rewrite_aof_from_store(store: &crate::storage::Store, path: &std::path::Path) -> std::io::Result<()> {
    use bytes::Bytes;
    use std::io::{Write, BufWriter};
    use crate::storage::types::RedisObject;

    let file = std::fs::File::create(path)?;
    let mut writer = BufWriter::new(file);
    let now = std::time::Instant::now();

    for db_index in 0..store.db_count() {
        let db = store.db(db_index);
        if db.is_empty() {
            continue;
        }

        if db_index > 0 {
            write_cmd(&mut writer, &[
                Bytes::from("SELECT"),
                Bytes::from(db_index.to_string()),
            ])?;
        }

        let expires: std::collections::HashMap<Bytes, std::time::Instant> = db.expires_iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();

        for (key, value) in db.iter() {
            match value {
                RedisObject::String(s) => {
                    write_cmd(&mut writer, &[
                        Bytes::from("SET"),
                        key.clone(),
                        s.clone(),
                    ])?;
                }
                RedisObject::List(list) => {
                    if !list.is_empty() {
                        let mut args = vec![Bytes::from("RPUSH"), key.clone()];
                        for item in list {
                            args.push(item.clone());
                        }
                        write_cmd(&mut writer, &args)?;
                    }
                }
                RedisObject::Set(set) => {
                    if !set.is_empty() {
                        let mut args = vec![Bytes::from("SADD"), key.clone()];
                        for member in set {
                            args.push(member.clone());
                        }
                        write_cmd(&mut writer, &args)?;
                    }
                }
                RedisObject::Hash(hash) => {
                    if !hash.is_empty() {
                        let mut args = vec![Bytes::from("HSET"), key.clone()];
                        for (field, val) in hash {
                            args.push(field.clone());
                            args.push(val.clone());
                        }
                        write_cmd(&mut writer, &args)?;
                    }
                }
                RedisObject::SortedSet(zset) => {
                    if !zset.members.is_empty() {
                        let mut args = vec![Bytes::from("ZADD"), key.clone()];
                        for (member, score) in &zset.members {
                            args.push(Bytes::from(score.to_string()));
                            args.push(member.clone());
                        }
                        write_cmd(&mut writer, &args)?;
                    }
                }
                RedisObject::Stream(stream) => {
                    for (id, fields) in &stream.entries {
                        let mut args = vec![
                            Bytes::from("XADD"),
                            key.clone(),
                            Bytes::from(id.to_string()),
                        ];
                        for (k, v) in fields {
                            args.push(k.clone());
                            args.push(v.clone());
                        }
                        write_cmd(&mut writer, &args)?;
                    }
                }
            }

            if let Some(expire_at) = expires.get(key) {
                if *expire_at > now {
                    let remaining = *expire_at - now;
                    let now_ms = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64;
                    let expire_ms = now_ms + remaining.as_millis() as u64;
                    write_cmd(&mut writer, &[
                        Bytes::from("PEXPIREAT"),
                        key.clone(),
                        Bytes::from(expire_ms.to_string()),
                    ])?;
                }
            }
        }
    }

    writer.flush()?;
    writer.get_ref().sync_data()?;
    Ok(())
}

fn write_cmd(w: &mut impl std::io::Write, args: &[bytes::Bytes]) -> std::io::Result<()> {
    write!(w, "*{}\r\n", args.len())?;
    for arg in args {
        write!(w, "${}\r\n", arg.len())?;
        w.write_all(arg)?;
        w.write_all(b"\r\n")?;
    }
    Ok(())
}
