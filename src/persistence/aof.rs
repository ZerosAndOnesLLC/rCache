use bytes::Bytes;
use std::io::{self, Read, Write, BufWriter, BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::fs::{File, OpenOptions};
use std::time::{Instant, Duration};

use crate::storage::Store;
use crate::storage::types::RedisObject;

/// Append-Only File writer.
pub struct AofWriter {
    path: PathBuf,
    writer: BufWriter<File>,
    fsync_mode: FsyncMode,
    last_fsync: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FsyncMode {
    Always,
    Everysec,
    No,
}

impl FsyncMode {
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "always" => FsyncMode::Always,
            "everysec" => FsyncMode::Everysec,
            "no" => FsyncMode::No,
            _ => FsyncMode::Everysec,
        }
    }
}

impl AofWriter {
    /// Open or create the AOF file for appending.
    pub fn open(path: &Path, fsync_mode: FsyncMode) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;

        Ok(Self {
            path: path.to_path_buf(),
            writer: BufWriter::new(file),
            fsync_mode,
            last_fsync: Instant::now(),
        })
    }

    /// Append a command (as RESP array of bulk strings) to the AOF.
    pub fn append(&mut self, args: &[Bytes]) -> io::Result<()> {
        // Write as RESP array
        write!(self.writer, "*{}\r\n", args.len())?;
        for arg in args {
            write!(self.writer, "${}\r\n", arg.len())?;
            self.writer.write_all(arg)?;
            self.writer.write_all(b"\r\n")?;
        }

        match self.fsync_mode {
            FsyncMode::Always => {
                self.writer.flush()?;
                self.writer.get_ref().sync_data()?;
            }
            FsyncMode::Everysec => {
                self.writer.flush()?;
                if self.last_fsync.elapsed() >= Duration::from_secs(1) {
                    self.writer.get_ref().sync_data()?;
                    self.last_fsync = Instant::now();
                }
            }
            FsyncMode::No => {
                // Let the OS handle flushing
                self.writer.flush()?;
            }
        }

        Ok(())
    }

    /// Rewrite the AOF from the current store state.
    /// Creates a new temp file, writes all current data, then replaces the old file.
    pub fn rewrite(&mut self, store: &Store) -> io::Result<()> {
        let temp_path = self.path.with_extension("aof.tmp");
        {
            let file = File::create(&temp_path)?;
            let mut writer = BufWriter::new(file);

            let now = Instant::now();

            for db_index in 0..store.db_count() {
                let db = store.db(db_index);
                if db.is_empty() {
                    continue;
                }

                // SELECT db
                if db_index > 0 {
                    write_resp_command(&mut writer, &[
                        Bytes::from("SELECT"),
                        Bytes::from(db_index.to_string()),
                    ])?;
                }

                // Collect expiry info
                let expires: std::collections::HashMap<Bytes, Instant> = db.expires_iter()
                    .map(|(k, v)| (k.clone(), *v))
                    .collect();

                for (key, value) in db.iter() {
                    match value {
                        RedisObject::String(s) => {
                            write_resp_command(&mut writer, &[
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
                                write_resp_command(&mut writer, &args)?;
                            }
                        }
                        RedisObject::Set(set) => {
                            if !set.is_empty() {
                                let mut args = vec![Bytes::from("SADD"), key.clone()];
                                for member in set {
                                    args.push(member.clone());
                                }
                                write_resp_command(&mut writer, &args)?;
                            }
                        }
                        RedisObject::Hash(hash) => {
                            if !hash.is_empty() {
                                let mut args = vec![Bytes::from("HSET"), key.clone()];
                                for (field, val) in hash {
                                    args.push(field.clone());
                                    args.push(val.clone());
                                }
                                write_resp_command(&mut writer, &args)?;
                            }
                        }
                        RedisObject::SortedSet(zset) => {
                            if !zset.members.is_empty() {
                                let mut args = vec![Bytes::from("ZADD"), key.clone()];
                                for (member, score) in &zset.members {
                                    args.push(Bytes::from(score.to_string()));
                                    args.push(member.clone());
                                }
                                write_resp_command(&mut writer, &args)?;
                            }
                        }
                    }

                    // Write PEXPIREAT if the key has an expiry
                    if let Some(expire_at) = expires.get(key) {
                        if *expire_at > now {
                            let remaining = *expire_at - now;
                            let now_ms = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_millis() as u64;
                            let expire_ms = now_ms + remaining.as_millis() as u64;
                            write_resp_command(&mut writer, &[
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
        }

        // Replace the old AOF with the new one
        std::fs::rename(&temp_path, &self.path)?;

        // Re-open the file for appending
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        self.writer = BufWriter::new(file);

        Ok(())
    }

    /// Get the path of the AOF file.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Write a RESP command (array of bulk strings) to a writer.
fn write_resp_command(w: &mut impl Write, args: &[Bytes]) -> io::Result<()> {
    write!(w, "*{}\r\n", args.len())?;
    for arg in args {
        write!(w, "${}\r\n", arg.len())?;
        w.write_all(arg)?;
        w.write_all(b"\r\n")?;
    }
    Ok(())
}

/// Replay an AOF file into a store using the command registry.
/// This is a simplified replay that directly applies commands to the store.
pub fn replay(path: &Path, store: &mut Store) -> io::Result<usize> {
    if !path.exists() {
        return Ok(0);
    }

    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut commands_replayed = 0;
    let mut current_db: usize = 0;

    loop {
        match read_resp_command(&mut reader)? {
            Some(args) => {
                if args.is_empty() {
                    continue;
                }

                let cmd_name = String::from_utf8_lossy(&args[0]).to_uppercase();

                match cmd_name.as_str() {
                    "SELECT" => {
                        if args.len() >= 2 {
                            if let Ok(idx) = String::from_utf8_lossy(&args[1]).parse::<usize>() {
                                if idx < store.db_count() {
                                    current_db = idx;
                                }
                            }
                        }
                    }
                    "SET" => {
                        if args.len() >= 3 {
                            let key = args[1].clone();
                            let value = args[2].clone();
                            store.db_mut(current_db).set_raw(key, RedisObject::String(value));
                        }
                        // Handle SET with EX/PX/EXAT/PXAT options
                        if args.len() >= 5 {
                            let key = args[1].clone();
                            let opt = String::from_utf8_lossy(&args[3]).to_uppercase();
                            match opt.as_str() {
                                "EX" => {
                                    if let Ok(secs) = String::from_utf8_lossy(&args[4]).parse::<u64>() {
                                        store.db_mut(current_db).set_expire_raw(
                                            key,
                                            Instant::now() + Duration::from_secs(secs),
                                        );
                                    }
                                }
                                "PX" => {
                                    if let Ok(ms) = String::from_utf8_lossy(&args[4]).parse::<u64>() {
                                        store.db_mut(current_db).set_expire_raw(
                                            key,
                                            Instant::now() + Duration::from_millis(ms),
                                        );
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    "DEL" | "UNLINK" => {
                        for arg in &args[1..] {
                            store.db_mut(current_db).remove(arg);
                        }
                    }
                    "RPUSH" | "LPUSH" => {
                        if args.len() >= 3 {
                            let key = args[1].clone();
                            let db = store.db_mut(current_db);
                            let list = match db.get_mut(&key) {
                                Some(RedisObject::List(l)) => l,
                                _ => {
                                    db.set_raw(key.clone(), RedisObject::List(std::collections::VecDeque::new()));
                                    match db.get_mut(&key) {
                                        Some(RedisObject::List(l)) => l,
                                        _ => continue,
                                    }
                                }
                            };
                            for item in &args[2..] {
                                if cmd_name == "RPUSH" {
                                    list.push_back(item.clone());
                                } else {
                                    list.push_front(item.clone());
                                }
                            }
                        }
                    }
                    "SADD" => {
                        if args.len() >= 3 {
                            let key = args[1].clone();
                            let db = store.db_mut(current_db);
                            let set = match db.get_mut(&key) {
                                Some(RedisObject::Set(s)) => s,
                                _ => {
                                    db.set_raw(key.clone(), RedisObject::Set(std::collections::HashSet::new()));
                                    match db.get_mut(&key) {
                                        Some(RedisObject::Set(s)) => s,
                                        _ => continue,
                                    }
                                }
                            };
                            for member in &args[2..] {
                                set.insert(member.clone());
                            }
                        }
                    }
                    "HSET" => {
                        if args.len() >= 4 && (args.len() - 2) % 2 == 0 {
                            let key = args[1].clone();
                            let db = store.db_mut(current_db);
                            let hash = match db.get_mut(&key) {
                                Some(RedisObject::Hash(h)) => h,
                                _ => {
                                    db.set_raw(key.clone(), RedisObject::Hash(std::collections::HashMap::new()));
                                    match db.get_mut(&key) {
                                        Some(RedisObject::Hash(h)) => h,
                                        _ => continue,
                                    }
                                }
                            };
                            for chunk in args[2..].chunks(2) {
                                if chunk.len() == 2 {
                                    hash.insert(chunk[0].clone(), chunk[1].clone());
                                }
                            }
                        }
                    }
                    "ZADD" => {
                        if args.len() >= 4 && (args.len() - 2) % 2 == 0 {
                            let key = args[1].clone();
                            let db = store.db_mut(current_db);
                            let zset = match db.get_mut(&key) {
                                Some(RedisObject::SortedSet(z)) => z,
                                _ => {
                                    db.set_raw(key.clone(), RedisObject::SortedSet(
                                        crate::storage::types::SortedSetData::new()
                                    ));
                                    match db.get_mut(&key) {
                                        Some(RedisObject::SortedSet(z)) => z,
                                        _ => continue,
                                    }
                                }
                            };
                            for chunk in args[2..].chunks(2) {
                                if chunk.len() == 2 {
                                    if let Ok(score) = String::from_utf8_lossy(&chunk[0]).parse::<f64>() {
                                        zset.insert(chunk[1].clone(), score);
                                    }
                                }
                            }
                        }
                    }
                    "EXPIRE" => {
                        if args.len() >= 3 {
                            let key = args[1].clone();
                            if let Ok(secs) = String::from_utf8_lossy(&args[2]).parse::<u64>() {
                                let db = store.db_mut(current_db);
                                if db.exists(&key) {
                                    db.set_expire_raw(key, Instant::now() + Duration::from_secs(secs));
                                }
                            }
                        }
                    }
                    "PEXPIRE" => {
                        if args.len() >= 3 {
                            let key = args[1].clone();
                            if let Ok(ms) = String::from_utf8_lossy(&args[2]).parse::<u64>() {
                                let db = store.db_mut(current_db);
                                if db.exists(&key) {
                                    db.set_expire_raw(key, Instant::now() + Duration::from_millis(ms));
                                }
                            }
                        }
                    }
                    "EXPIREAT" => {
                        if args.len() >= 3 {
                            let key = args[1].clone();
                            if let Ok(ts) = String::from_utf8_lossy(&args[2]).parse::<u64>() {
                                let now_secs = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap()
                                    .as_secs();
                                if ts > now_secs {
                                    let db = store.db_mut(current_db);
                                    if db.exists(&key) {
                                        db.set_expire_raw(key, Instant::now() + Duration::from_secs(ts - now_secs));
                                    }
                                }
                            }
                        }
                    }
                    "PEXPIREAT" => {
                        if args.len() >= 3 {
                            let key = args[1].clone();
                            if let Ok(ts_ms) = String::from_utf8_lossy(&args[2]).parse::<u64>() {
                                let now_ms = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap()
                                    .as_millis() as u64;
                                if ts_ms > now_ms {
                                    let db = store.db_mut(current_db);
                                    if db.exists(&key) {
                                        db.set_expire_raw(key, Instant::now() + Duration::from_millis(ts_ms - now_ms));
                                    }
                                }
                            }
                        }
                    }
                    "PERSIST" => {
                        if args.len() >= 2 {
                            store.db_mut(current_db).persist(&args[1]);
                        }
                    }
                    "FLUSHDB" => {
                        store.db_mut(current_db).flush();
                    }
                    "FLUSHALL" => {
                        store.flush_all();
                    }
                    _ => {
                        // Skip unknown commands during replay
                        tracing::debug!("AOF replay: skipping unknown command '{}'", cmd_name);
                    }
                }

                commands_replayed += 1;
            }
            None => break,
        }
    }

    Ok(commands_replayed)
}

/// Read a single RESP array command from the reader.
/// Returns None at EOF.
fn read_resp_command(reader: &mut BufReader<File>) -> io::Result<Option<Vec<Bytes>>> {
    let mut line = String::new();
    let n = reader.read_line(&mut line)?;
    if n == 0 {
        return Ok(None);
    }

    let line = line.trim_end_matches('\n').trim_end_matches('\r');

    if !line.starts_with('*') {
        // Not a RESP array, skip
        return Ok(Some(vec![]));
    }

    let count: usize = match line[1..].parse() {
        Ok(c) => c,
        Err(_) => return Ok(Some(vec![])),
    };

    let mut args = Vec::with_capacity(count);
    for _ in 0..count {
        // Read $N\r\n
        let mut size_line = String::new();
        reader.read_line(&mut size_line)?;
        let size_line = size_line.trim_end_matches('\n').trim_end_matches('\r');

        if !size_line.starts_with('$') {
            continue;
        }

        let size: usize = match size_line[1..].parse() {
            Ok(s) => s,
            Err(_) => continue,
        };

        // Read exactly `size` bytes + \r\n
        let mut buf = vec![0u8; size + 2];
        reader.read_exact(&mut buf)?;
        buf.truncate(size); // Remove the \r\n
        args.push(Bytes::from(buf));
    }

    Ok(Some(args))
}

/// Set of write commands that should be logged to AOF.
pub fn is_write_command(cmd: &str) -> bool {
    matches!(cmd,
        "SET" | "SETNX" | "SETEX" | "PSETEX" | "MSET" | "MSETNX" |
        "APPEND" | "SETRANGE" | "INCR" | "DECR" | "INCRBY" | "DECRBY" | "INCRBYFLOAT" |
        "GETSET" | "GETDEL" | "GETEX" |
        "DEL" | "UNLINK" |
        "EXPIRE" | "PEXPIRE" | "EXPIREAT" | "PEXPIREAT" | "PERSIST" |
        "RENAME" | "RENAMENX" | "COPY" |
        "LPUSH" | "RPUSH" | "LPUSHX" | "RPUSHX" | "LPOP" | "RPOP" |
        "LSET" | "LINSERT" | "LREM" | "LTRIM" | "LMOVE" | "LMPOP" |
        "SADD" | "SREM" | "SPOP" | "SMOVE" |
        "SDIFFSTORE" | "SINTERSTORE" | "SUNIONSTORE" |
        "HSET" | "HSETNX" | "HDEL" | "HINCRBY" | "HINCRBYFLOAT" |
        "ZADD" | "ZREM" | "ZINCRBY" | "ZPOPMIN" | "ZPOPMAX" |
        "ZUNIONSTORE" | "ZINTERSTORE" | "ZDIFFSTORE" |
        "FLUSHDB" | "FLUSHALL" | "SWAPDB" | "SELECT"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aof_write_and_replay() {
        let dir = std::env::temp_dir().join("rcache_test_aof");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("test.aof");

        // Remove any existing file
        let _ = std::fs::remove_file(&path);

        // Write some commands
        {
            let mut writer = AofWriter::open(&path, FsyncMode::Always).unwrap();
            writer.append(&[
                Bytes::from("SET"),
                Bytes::from("key1"),
                Bytes::from("value1"),
            ]).unwrap();
            writer.append(&[
                Bytes::from("SET"),
                Bytes::from("key2"),
                Bytes::from("value2"),
            ]).unwrap();
            writer.append(&[
                Bytes::from("DEL"),
                Bytes::from("key1"),
            ]).unwrap();
        }

        // Replay
        let mut store = Store::new(16);
        let count = replay(&path, &mut store).unwrap();
        assert_eq!(count, 3);
        assert_eq!(store.db(0).len(), 1); // key1 deleted, key2 remains

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_aof_rewrite() {
        let dir = std::env::temp_dir().join("rcache_test_aof_rewrite");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("test.aof");
        let _ = std::fs::remove_file(&path);

        // Write lots of commands, then rewrite from state
        {
            let mut writer = AofWriter::open(&path, FsyncMode::Always).unwrap();
            // Set and overwrite many times
            for i in 0..100 {
                writer.append(&[
                    Bytes::from("SET"),
                    Bytes::from("counter"),
                    Bytes::from(i.to_string()),
                ]).unwrap();
            }

            // Now rewrite from store state
            let mut store = Store::new(16);
            store.db_mut(0).set_raw(
                Bytes::from("counter"),
                RedisObject::String(Bytes::from("99")),
            );
            writer.rewrite(&store).unwrap();
        }

        // Replay the rewritten AOF should give us just the final state
        let mut store = Store::new(16);
        let count = replay(&path, &mut store).unwrap();
        assert_eq!(count, 1);
        assert_eq!(store.db(0).len(), 1);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_is_write_command() {
        assert!(is_write_command("SET"));
        assert!(is_write_command("DEL"));
        assert!(is_write_command("ZADD"));
        assert!(!is_write_command("GET"));
        assert!(!is_write_command("PING"));
        assert!(!is_write_command("INFO"));
    }
}
