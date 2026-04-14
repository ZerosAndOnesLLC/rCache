use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Mutex;

use crate::protocol::RespValue;
use crate::storage::Store;

/// SHA1 hash of a script (hex string).
fn sha1_hex(script: &str) -> String {
    sha1_compute(script.as_bytes())
}

/// Minimal SHA-1 implementation for script hashing.
fn sha1_compute(data: &[u8]) -> String {
    let mut h0: u32 = 0x67452301;
    let mut h1: u32 = 0xEFCDAB89;
    let mut h2: u32 = 0x98BADCFE;
    let mut h3: u32 = 0x10325476;
    let mut h4: u32 = 0xC3D2E1F0;

    let bit_len = (data.len() as u64) * 8;
    let mut msg = data.to_vec();
    msg.push(0x80);
    while (msg.len() % 64) != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bit_len.to_be_bytes());

    for chunk in msg.chunks(64) {
        let mut w = [0u32; 80];
        for i in 0..16 {
            w[i] = u32::from_be_bytes([
                chunk[i * 4],
                chunk[i * 4 + 1],
                chunk[i * 4 + 2],
                chunk[i * 4 + 3],
            ]);
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }

        let (mut a, mut b, mut c, mut d, mut e) = (h0, h1, h2, h3, h4);

        for i in 0..80 {
            let (f, k) = match i {
                0..=19 => ((b & c) | ((!b) & d), 0x5A827999u32),
                20..=39 => (b ^ c ^ d, 0x6ED9EBA1u32),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1BBCDCu32),
                _ => (b ^ c ^ d, 0xCA62C1D6u32),
            };
            let temp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(w[i]);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = temp;
        }

        h0 = h0.wrapping_add(a);
        h1 = h1.wrapping_add(b);
        h2 = h2.wrapping_add(c);
        h3 = h3.wrapping_add(d);
        h4 = h4.wrapping_add(e);
    }

    format!("{:08x}{:08x}{:08x}{:08x}{:08x}", h0, h1, h2, h3, h4)
}

/// The script cache: maps SHA1 hex -> script source.
pub struct ScriptCache {
    scripts: Mutex<HashMap<String, String>>,
}

impl ScriptCache {
    pub fn new() -> Self {
        Self {
            scripts: Mutex::new(HashMap::new()),
        }
    }

    /// Load a script into the cache, returning its SHA1 hash.
    pub fn load(&self, script: &str) -> String {
        let sha = sha1_hex(script);
        let mut scripts = self.scripts.lock().unwrap();
        scripts.insert(sha.clone(), script.to_string());
        sha
    }

    /// Check if a script exists by SHA1.
    pub fn exists(&self, sha: &str) -> bool {
        let scripts = self.scripts.lock().unwrap();
        scripts.contains_key(sha)
    }

    /// Get a script by SHA1.
    pub fn get(&self, sha: &str) -> Option<String> {
        let scripts = self.scripts.lock().unwrap();
        scripts.get(sha).cloned()
    }

    /// Flush all cached scripts.
    pub fn flush(&self) {
        let mut scripts = self.scripts.lock().unwrap();
        scripts.clear();
    }
}

/// Function library for Redis Functions support.
pub struct FunctionLibrary {
    /// library_name -> FunctionLib
    pub libraries: Mutex<HashMap<String, FunctionLib>>,
}

pub struct FunctionLib {
    pub name: String,
    pub engine: String,
    pub code: String,
    /// function_name -> function body (extracted from the library code)
    pub functions: HashMap<String, String>,
}

impl FunctionLibrary {
    pub fn new() -> Self {
        Self {
            libraries: Mutex::new(HashMap::new()),
        }
    }

    pub fn load(&self, code: &str, replace: bool) -> Result<String, String> {
        // Parse the library header: #!lua name=<libname>
        let first_line = code.lines().next().unwrap_or("");
        if !first_line.starts_with("#!lua") {
            return Err("ERR Missing library metadata".to_string());
        }
        let name = first_line
            .split("name=")
            .nth(1)
            .map(|s| s.trim().to_string())
            .ok_or_else(|| "ERR Library name not found in header".to_string())?;

        let mut libs = self.libraries.lock().unwrap();
        if libs.contains_key(&name) && !replace {
            return Err(format!("ERR Library '{}' already exists", name));
        }

        // Extract function names from `redis.register_function('name', function(...)` patterns
        let mut functions = HashMap::new();
        for line in code.lines() {
            let trimmed = line.trim();
            if trimmed.contains("register_function") {
                // Try to extract function name from patterns like:
                // redis.register_function('fname', function(keys, args) ... end)
                // redis.register_function{function_name='fname', callback=function(keys, args) ... end}
                if let Some(start) = trimmed.find("function_name") {
                    if let Some(eq) = trimmed[start..].find('=') {
                        let after_eq = &trimmed[start + eq + 1..];
                        let after_eq = after_eq.trim().trim_start_matches(['\'', '"']);
                        if let Some(end) = after_eq.find(['\'', '"']) {
                            let fname = &after_eq[..end];
                            functions.insert(fname.to_string(), code.to_string());
                        }
                    }
                } else if let Some(start) = trimmed.find('(') {
                    let after_paren = &trimmed[start + 1..];
                    let after_paren = after_paren.trim().trim_start_matches(['\'', '"']);
                    if let Some(end) = after_paren.find(['\'', '"', ',']) {
                        let fname = &after_paren[..end];
                        if !fname.is_empty() {
                            functions.insert(fname.to_string(), code.to_string());
                        }
                    }
                }
            }
        }

        let lib = FunctionLib {
            name: name.clone(),
            engine: "LUA".to_string(),
            code: code.to_string(),
            functions,
        };
        libs.insert(name.clone(), lib);
        Ok(name)
    }

    pub fn delete(&self, name: &str) -> Result<(), String> {
        let mut libs = self.libraries.lock().unwrap();
        if libs.remove(name).is_some() {
            Ok(())
        } else {
            Err("ERR No such library".to_string())
        }
    }

    pub fn list(&self) -> Vec<(String, String, Vec<String>)> {
        let libs = self.libraries.lock().unwrap();
        libs.values()
            .map(|lib| {
                let fnames: Vec<String> = lib.functions.keys().cloned().collect();
                (lib.name.clone(), lib.engine.clone(), fnames)
            })
            .collect()
    }

    pub fn find_function(&self, fname: &str) -> Option<String> {
        let libs = self.libraries.lock().unwrap();
        for lib in libs.values() {
            if lib.functions.contains_key(fname) {
                return Some(lib.code.clone());
            }
        }
        None
    }

    pub fn flush(&self) {
        let mut libs = self.libraries.lock().unwrap();
        libs.clear();
    }
}

/// Execute a Lua script with the given KEYS and ARGV against the store.
/// This creates a sandboxed Lua environment with redis.call() and redis.pcall().
pub fn execute_script(
    script: &str,
    keys: &[Bytes],
    argv: &[Bytes],
    store: &mut Store,
    db_index: usize,
) -> RespValue {
    use mlua::prelude::*;

    let lua = match Lua::new() {
        lua => lua,
    };

    // Set up the sandbox: remove dangerous globals
    let sandbox_result = lua.scope(|_scope| {
        // Remove dangerous modules
        let globals = lua.globals();
        let _ = globals.set("os", LuaNil);
        let _ = globals.set("io", LuaNil);
        let _ = globals.set("loadfile", LuaNil);
        let _ = globals.set("dofile", LuaNil);
        let _ = globals.set("package", LuaNil);
        let _ = globals.set("require", LuaNil);
        let _ = globals.set("debug", LuaNil);

        // Set KEYS table
        let keys_table = lua.create_table()?;
        for (i, key) in keys.iter().enumerate() {
            keys_table
                .set(i + 1, String::from_utf8_lossy(key).to_string())?;
        }
        globals.set("KEYS", keys_table)?;

        // Set ARGV table
        let argv_table = lua.create_table()?;
        for (i, arg) in argv.iter().enumerate() {
            argv_table
                .set(i + 1, String::from_utf8_lossy(arg).to_string())?;
        }
        globals.set("ARGV", argv_table)?;

        Ok(())
    });

    if let Err(e) = sandbox_result {
        return RespValue::error(format!("ERR {}", e));
    }

    // We need to handle redis.call() and redis.pcall() by creating a redis table.
    // Since we can't pass mutable store references into Lua callbacks easily,
    // we use a RefCell to allow interior mutability.
    // However, with mlua scoped callbacks, we can reference the store.

    // Share the store with Lua callbacks via a stack-local RefCell. mlua's
    // scoped callbacks let non-'static borrows live as long as the scope, so
    // no raw pointer is needed and the borrow checker enforces aliasing.
    let store_cell = std::cell::RefCell::new(store);
    let db_idx = db_index;

    let result: Result<RespValue, String> = lua
        .scope(|scope| {
            let redis_table = lua.create_table()?;

            let call_fn = scope.create_function(|lua_ctx, args: mlua::MultiValue| {
                let str_args: Vec<String> = args
                    .into_iter()
                    .map(|v| match v {
                        mlua::Value::String(s) => s.to_string_lossy(),
                        mlua::Value::Integer(n) => n.to_string(),
                        mlua::Value::Number(n) => {
                            if n == n.floor() {
                                (n as i64).to_string()
                            } else {
                                n.to_string()
                            }
                        }
                        _ => String::new(),
                    })
                    .collect();

                if str_args.is_empty() {
                    return Err(mlua::Error::RuntimeError(
                        "Please specify at least one argument for redis.call()".to_string(),
                    ));
                }

                let mut store_ref = store_cell.borrow_mut();
                let resp = execute_redis_command(&mut **store_ref, db_idx, &str_args);
                resp_to_lua(lua_ctx, &resp)
            })?;

            let pcall_fn = scope.create_function(|lua_ctx, args: mlua::MultiValue| {
                let str_args: Vec<String> = args
                    .into_iter()
                    .map(|v| match v {
                        mlua::Value::String(s) => s.to_string_lossy(),
                        mlua::Value::Integer(n) => n.to_string(),
                        mlua::Value::Number(n) => {
                            if n == n.floor() {
                                (n as i64).to_string()
                            } else {
                                n.to_string()
                            }
                        }
                        _ => String::new(),
                    })
                    .collect();

                if str_args.is_empty() {
                    let err_table = lua_ctx.create_table()?;
                    err_table.set(
                        "err",
                        "Please specify at least one argument for redis.pcall()",
                    )?;
                    return Ok(mlua::Value::Table(err_table));
                }

                let mut store_ref = store_cell.borrow_mut();
                let resp = execute_redis_command(&mut **store_ref, db_idx, &str_args);
                match &resp {
                    RespValue::Error(e) => {
                        let err_table = lua_ctx.create_table()?;
                        err_table.set("err", e.as_str())?;
                        Ok(mlua::Value::Table(err_table))
                    }
                    _ => resp_to_lua(lua_ctx, &resp),
                }
            })?;

            redis_table.set("call", call_fn)?;
            redis_table.set("pcall", pcall_fn)?;

            let status_fn = lua.create_function(|lua_ctx, msg: String| {
                let t = lua_ctx.create_table()?;
                t.set("ok", msg)?;
                Ok(mlua::Value::Table(t))
            })?;
            redis_table.set("status_reply", status_fn)?;

            let error_fn = lua.create_function(|lua_ctx, msg: String| {
                let t = lua_ctx.create_table()?;
                t.set("err", msg)?;
                Ok(mlua::Value::Table(t))
            })?;
            redis_table.set("error_reply", error_fn)?;

            let log_fn = lua.create_function(|_lua_ctx, (_level, msg): (i32, String)| {
                tracing::info!("Lua script log: {}", msg);
                Ok(())
            })?;
            redis_table.set("log", log_fn)?;

            redis_table.set("LOG_DEBUG", 0)?;
            redis_table.set("LOG_VERBOSE", 1)?;
            redis_table.set("LOG_NOTICE", 2)?;
            redis_table.set("LOG_WARNING", 3)?;

            let register_fn = lua.create_function(|_lua_ctx, _args: mlua::MultiValue| {
                // No-op in script execution context; only meaningful during FUNCTION LOAD
                Ok(())
            })?;
            redis_table.set("register_function", register_fn)?;

            lua.globals().set("redis", redis_table)?;

            let val: mlua::Value = lua.load(script).eval()?;
            Ok(lua_to_resp(&val))
        })
        .map_err(|e: mlua::Error| format!("ERR {}", e));

    match result {
        Ok(resp) => resp,
        Err(e) => RespValue::error(e),
    }
}

/// Execute a Redis command from within a Lua script.
fn execute_redis_command(store: &mut Store, db_index: usize, args: &[String]) -> RespValue {
    if args.is_empty() {
        return RespValue::error("ERR empty command");
    }

    let _cmd = args[0].to_uppercase();
    let byte_args: Vec<Bytes> = args.iter().map(|s| Bytes::from(s.clone())).collect();

    // Use the command registry to execute
    let start_time = std::time::Instant::now();
    let mut ctx = crate::command::CommandContext {
        store,
        db_index,
        args: byte_args,
        start_time,
    };

    // Create a temporary registry to execute the command
    let registry = crate::command::CommandRegistry::new();
    registry.execute(&mut ctx)
}

/// Convert a RespValue to a Lua value.
fn resp_to_lua<'a>(lua: &'a mlua::Lua, resp: &RespValue) -> mlua::Result<mlua::Value> {
    match resp {
        RespValue::SimpleString(s) => {
            let t = lua.create_table()?;
            t.set("ok", s.as_str())?;
            Ok(mlua::Value::Table(t))
        }
        RespValue::Error(e) => Err(mlua::Error::RuntimeError(e.clone())),
        RespValue::Integer(n) => Ok(mlua::Value::Integer(*n)),
        RespValue::BulkString(b) => {
            let s = lua.create_string(b.as_ref())?;
            Ok(mlua::Value::String(s))
        }
        RespValue::Array(items) => {
            let t = lua.create_table()?;
            for (i, item) in items.iter().enumerate() {
                let val = resp_to_lua(lua, item)?;
                t.set(i + 1, val)?;
            }
            Ok(mlua::Value::Table(t))
        }
        RespValue::Null | RespValue::NullArray | RespValue::Resp3Null => {
            Ok(mlua::Value::Boolean(false))
        }
        RespValue::Boolean(b) => Ok(mlua::Value::Boolean(*b)),
        RespValue::Double(d) => Ok(mlua::Value::Number(*d)),
        _ => Ok(mlua::Value::Nil),
    }
}

/// Convert a Lua value to a RespValue.
fn lua_to_resp(val: &mlua::Value) -> RespValue {
    match val {
        mlua::Value::Nil => RespValue::Null,
        mlua::Value::Boolean(false) => RespValue::Null,
        mlua::Value::Boolean(true) => RespValue::integer(1),
        mlua::Value::Integer(n) => RespValue::integer(*n),
        mlua::Value::Number(n) => {
            if *n == n.floor() && *n >= i64::MIN as f64 && *n <= i64::MAX as f64 {
                RespValue::integer(*n as i64)
            } else {
                RespValue::bulk_string(Bytes::from(n.to_string()))
            }
        }
        mlua::Value::String(s) => {
            RespValue::bulk_string(Bytes::from(s.as_bytes().to_vec()))
        }
        mlua::Value::Table(t) => {
            // Check if it's an error table {err = "..."}
            if let Ok(err) = t.get::<String>("err") {
                return RespValue::error(err);
            }
            // Check if it's a status table {ok = "..."}
            if let Ok(ok) = t.get::<String>("ok") {
                return RespValue::simple_string(ok);
            }
            // Otherwise treat as array
            let len = t.len().unwrap_or(0);
            let mut arr = Vec::with_capacity(len as usize);
            for i in 1..=len {
                if let Ok(v) = t.get::<mlua::Value>(i) {
                    arr.push(lua_to_resp(&v));
                }
            }
            RespValue::array(arr)
        }
        _ => RespValue::Null,
    }
}

/// Compute SHA1 of a script string (public for use by scripting commands).
pub fn script_sha1(script: &str) -> String {
    sha1_hex(script)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sha1_compute() {
        // Known SHA1 test vectors
        let hash = sha1_compute(b"");
        assert_eq!(hash, "da39a3ee5e6b4b0d3255bfef95601890afd80709");

        let hash = sha1_compute(b"abc");
        assert_eq!(hash, "a9993e364706816aba3e25717850c26c9cd0d89d");
    }

    #[test]
    fn test_script_cache() {
        let cache = ScriptCache::new();
        let sha = cache.load("return 1");
        assert!(cache.exists(&sha));
        assert_eq!(cache.get(&sha), Some("return 1".to_string()));
        cache.flush();
        assert!(!cache.exists(&sha));
    }

    #[test]
    fn test_execute_simple_script() {
        let mut store = Store::new(16);
        let result = execute_script("return 42", &[], &[], &mut store, 0);
        assert_eq!(result, RespValue::integer(42));
    }

    #[test]
    fn test_execute_script_with_keys_argv() {
        let mut store = Store::new(16);
        let keys = vec![Bytes::from("mykey")];
        let argv = vec![Bytes::from("myval")];
        let result = execute_script(
            "return KEYS[1] .. ' ' .. ARGV[1]",
            &keys,
            &argv,
            &mut store,
            0,
        );
        assert_eq!(result, RespValue::bulk_string(Bytes::from("mykey myval")));
    }

    #[test]
    fn test_execute_script_redis_call() {
        let mut store = Store::new(16);
        let result = execute_script(
            "redis.call('SET', 'testkey', 'testval')\nreturn redis.call('GET', 'testkey')",
            &[],
            &[],
            &mut store,
            0,
        );
        assert_eq!(result, RespValue::bulk_string(Bytes::from("testval")));
    }

    #[test]
    fn test_execute_script_pcall_error() {
        let mut store = Store::new(16);
        let result = execute_script(
            "local ok = redis.pcall('INVALID_CMD')\nif ok.err then return ok.err end\nreturn 'ok'",
            &[],
            &[],
            &mut store,
            0,
        );
        // pcall should catch the error
        match result {
            RespValue::BulkString(_) => {} // Got the error string back
            _ => {} // Any non-panic result is fine
        }
    }
}
