use bytes::Bytes;
use crate::protocol::RespValue;
use crate::scripting_engine;
use super::registry::CommandContext;

/// EVAL script numkeys key [key ...] arg [arg ...]
pub fn cmd_eval(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 3 {
        return RespValue::wrong_arity("eval");
    }

    let script = String::from_utf8_lossy(&ctx.args[1]).to_string();
    let numkeys: usize = match String::from_utf8_lossy(&ctx.args[2]).parse() {
        Ok(n) => n,
        Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
    };

    if ctx.args.len() < 3 + numkeys {
        return RespValue::error("ERR Number of keys can't be greater than number of args");
    }

    let keys: Vec<Bytes> = ctx.args[3..3 + numkeys].to_vec();
    let argv: Vec<Bytes> = ctx.args[3 + numkeys..].to_vec();

    scripting_engine::execute_script(&script, &keys, &argv, ctx.store, ctx.db_index)
}

/// EVALSHA sha1 numkeys key [key ...] arg [arg ...]
pub fn cmd_evalsha(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 3 {
        return RespValue::wrong_arity("evalsha");
    }

    let sha = String::from_utf8_lossy(&ctx.args[1]).to_lowercase();
    let numkeys: usize = match String::from_utf8_lossy(&ctx.args[2]).parse() {
        Ok(n) => n,
        Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
    };

    if ctx.args.len() < 3 + numkeys {
        return RespValue::error("ERR Number of keys can't be greater than number of args");
    }

    // Look up script by SHA1 in the global script cache.
    // We use a static-like approach: create a temporary cache to lookup.
    // The actual cache is in SharedState, but we don't have access to it from here.
    // So we store a thread-local reference. Actually, let's use a global.
    // For now, we'll compute SHA1 and use a global script cache.
    let script = match SCRIPT_CACHE.lock() {
        Ok(cache) => cache.get(&sha).cloned(),
        Err(_) => None,
    };

    match script {
        Some(script_source) => {
            let keys: Vec<Bytes> = ctx.args[3..3 + numkeys].to_vec();
            let argv: Vec<Bytes> = ctx.args[3 + numkeys..].to_vec();
            scripting_engine::execute_script(&script_source, &keys, &argv, ctx.store, ctx.db_index)
        }
        None => RespValue::error(format!("NOSCRIPT No matching script. Please use EVAL.")),
    }
}

/// SCRIPT subcommand [args...]
pub fn cmd_script(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 2 {
        return RespValue::error("ERR wrong number of arguments for 'script' command");
    }

    let subcmd = String::from_utf8_lossy(&ctx.args[1]).to_uppercase();
    match subcmd.as_str() {
        "LOAD" => {
            if ctx.args.len() < 3 {
                return RespValue::wrong_arity("script|load");
            }
            let script = String::from_utf8_lossy(&ctx.args[2]).to_string();
            let sha = scripting_engine::script_sha1(&script);

            // Store in global cache
            if let Ok(mut cache) = SCRIPT_CACHE.lock() {
                cache.insert(sha.clone(), script);
            }

            RespValue::bulk_string(Bytes::from(sha))
        }
        "EXISTS" => {
            let shas: Vec<String> = ctx.args[2..]
                .iter()
                .map(|a| String::from_utf8_lossy(a).to_lowercase())
                .collect();
            let results: Vec<RespValue> = shas
                .iter()
                .map(|sha| {
                    let exists = SCRIPT_CACHE
                        .lock()
                        .map(|cache| cache.contains_key(sha))
                        .unwrap_or(false);
                    RespValue::integer(if exists { 1 } else { 0 })
                })
                .collect();
            RespValue::array(results)
        }
        "FLUSH" => {
            if let Ok(mut cache) = SCRIPT_CACHE.lock() {
                cache.clear();
            }
            RespValue::ok()
        }
        _ => RespValue::error(format!(
            "ERR unknown subcommand or wrong number of arguments for 'script|{}' command",
            subcmd.to_lowercase()
        )),
    }
}

/// FUNCTION subcommand [args...]
pub fn cmd_function(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 2 {
        return RespValue::error("ERR wrong number of arguments for 'function' command");
    }

    let subcmd = String::from_utf8_lossy(&ctx.args[1]).to_uppercase();
    match subcmd.as_str() {
        "LOAD" => {
            if ctx.args.len() < 3 {
                return RespValue::wrong_arity("function|load");
            }
            // Check for REPLACE flag
            let mut replace = false;
            let mut code_idx = 2;
            for i in 2..ctx.args.len() {
                let arg = String::from_utf8_lossy(&ctx.args[i]).to_uppercase();
                if arg == "REPLACE" {
                    replace = true;
                    code_idx = i + 1;
                    break;
                }
            }
            if code_idx >= ctx.args.len() {
                code_idx = 2;
            }
            let code = String::from_utf8_lossy(&ctx.args[code_idx]).to_string();

            if let Ok(mut libs) = FUNCTION_LIBS.lock() {
                // Parse library header
                let first_line = code.lines().next().unwrap_or("");
                if !first_line.starts_with("#!lua") {
                    return RespValue::error("ERR Missing library metadata");
                }
                let name = match first_line.split("name=").nth(1) {
                    Some(n) => n.trim().to_string(),
                    None => return RespValue::error("ERR Library name not found in header"),
                };

                if libs.contains_key(&name) && !replace {
                    return RespValue::error(format!("ERR Library '{}' already exists", name));
                }

                let mut functions = std::collections::HashMap::new();
                // Extract function registrations
                for line in code.lines() {
                    let trimmed = line.trim();
                    if trimmed.contains("register_function") {
                        if let Some(start) = trimmed.find("function_name") {
                            if let Some(eq) = trimmed[start..].find('=') {
                                let after = &trimmed[start + eq + 1..];
                                let after = after.trim().trim_start_matches(['\'', '"']);
                                if let Some(end) = after.find(['\'', '"']) {
                                    functions.insert(after[..end].to_string(), code.clone());
                                }
                            }
                        } else if let Some(start) = trimmed.find('(') {
                            let after = &trimmed[start + 1..];
                            let after = after.trim().trim_start_matches(['\'', '"']);
                            if let Some(end) = after.find(['\'', '"', ',']) {
                                let fname = &after[..end];
                                if !fname.is_empty() {
                                    functions.insert(fname.to_string(), code.clone());
                                }
                            }
                        }
                    }
                }

                libs.insert(name.clone(), FunctionLibEntry {
                    name: name.clone(),
                    engine: "LUA".to_string(),
                    code,
                    functions,
                });
                RespValue::bulk_string(Bytes::from(name))
            } else {
                RespValue::error("ERR internal error")
            }
        }
        "LIST" => {
            if let Ok(libs) = FUNCTION_LIBS.lock() {
                let mut result = Vec::new();
                for lib in libs.values() {
                    let fnames: Vec<RespValue> = lib.functions.keys()
                        .map(|f| {
                            RespValue::array(vec![
                                RespValue::bulk_string(Bytes::from("name")),
                                RespValue::bulk_string(Bytes::from(f.clone())),
                            ])
                        })
                        .collect();
                    result.push(RespValue::array(vec![
                        RespValue::bulk_string(Bytes::from("library_name")),
                        RespValue::bulk_string(Bytes::from(lib.name.clone())),
                        RespValue::bulk_string(Bytes::from("engine")),
                        RespValue::bulk_string(Bytes::from(lib.engine.clone())),
                        RespValue::bulk_string(Bytes::from("functions")),
                        RespValue::array(fnames),
                    ]));
                }
                RespValue::array(result)
            } else {
                RespValue::array(vec![])
            }
        }
        "DELETE" => {
            if ctx.args.len() < 3 {
                return RespValue::wrong_arity("function|delete");
            }
            let name = String::from_utf8_lossy(&ctx.args[2]).to_string();
            if let Ok(mut libs) = FUNCTION_LIBS.lock() {
                if libs.remove(&name).is_some() {
                    RespValue::ok()
                } else {
                    RespValue::error("ERR No such library")
                }
            } else {
                RespValue::error("ERR internal error")
            }
        }
        "DUMP" => RespValue::bulk_string(Bytes::new()),
        "RESTORE" => RespValue::error("ERR function restore not supported"),
        "STATS" => RespValue::array(vec![
            RespValue::bulk_string(Bytes::from("running_script")),
            RespValue::integer(0),
            RespValue::bulk_string(Bytes::from("engines")),
            RespValue::array(vec![]),
        ]),
        "FLUSH" => {
            if let Ok(mut libs) = FUNCTION_LIBS.lock() {
                libs.clear();
            }
            RespValue::ok()
        }
        _ => RespValue::error(format!(
            "ERR unknown subcommand or wrong number of arguments for 'function|{}' command",
            subcmd.to_lowercase()
        )),
    }
}

/// FCALL function numkeys key [key ...] arg [arg ...]
pub fn cmd_fcall(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 3 {
        return RespValue::wrong_arity("fcall");
    }

    let fname = String::from_utf8_lossy(&ctx.args[1]).to_string();
    let numkeys: usize = match String::from_utf8_lossy(&ctx.args[2]).parse() {
        Ok(n) => n,
        Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
    };

    if ctx.args.len() < 3 + numkeys {
        return RespValue::error("ERR Number of keys can't be greater than number of args");
    }

    // Look up the function in libraries
    let code = if let Ok(libs) = FUNCTION_LIBS.lock() {
        libs.values()
            .find_map(|lib| {
                if lib.functions.contains_key(&fname) {
                    Some(lib.code.clone())
                } else {
                    None
                }
            })
    } else {
        None
    };

    match code {
        Some(script) => {
            let keys: Vec<Bytes> = ctx.args[3..3 + numkeys].to_vec();
            let argv: Vec<Bytes> = ctx.args[3 + numkeys..].to_vec();
            scripting_engine::execute_script(&script, &keys, &argv, ctx.store, ctx.db_index)
        }
        None => RespValue::error(format!("ERR Function not found")),
    }
}

/// FCALL_RO - same as FCALL but read-only (we don't enforce read-only in this pass)
pub fn cmd_fcall_ro(ctx: &mut CommandContext) -> RespValue {
    cmd_fcall(ctx)
}

// Global script cache and function library (accessible from command handlers).
use std::sync::Mutex;
use std::collections::HashMap;

static SCRIPT_CACHE: std::sync::LazyLock<Mutex<HashMap<String, String>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));

struct FunctionLibEntry {
    name: String,
    engine: String,
    code: String,
    functions: HashMap<String, String>,
}

static FUNCTION_LIBS: std::sync::LazyLock<Mutex<HashMap<String, FunctionLibEntry>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));
