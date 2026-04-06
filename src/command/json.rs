use bytes::Bytes;
use serde_json::Value;

use crate::protocol::RespValue;
use crate::storage::RedisObject;
use super::registry::CommandContext;

/// Resolve a JSONPath-like path into a reference to the target value.
/// Supports: "." or "$" = root, "$.field", ".field", "[N]", nested paths.
fn json_get<'a>(root: &'a Value, path: &str) -> Option<&'a Value> {
    let path = path.trim();
    if path.is_empty() || path == "." || path == "$" {
        return Some(root);
    }

    let normalized = if path.starts_with('$') {
        &path[1..]
    } else if path.starts_with('.') {
        path
    } else {
        return None;
    };

    let mut current = root;
    let mut remaining = normalized;

    while !remaining.is_empty() {
        if remaining.starts_with('.') {
            remaining = &remaining[1..];
            if remaining.is_empty() {
                break;
            }
            // Read field name until next '.' or '['
            let end = remaining
                .find(|c: char| c == '.' || c == '[')
                .unwrap_or(remaining.len());
            let field = &remaining[..end];
            if field.is_empty() {
                return None;
            }
            current = current.get(field)?;
            remaining = &remaining[end..];
        } else if remaining.starts_with('[') {
            let close = remaining.find(']')?;
            let index_str = &remaining[1..close];
            let index: usize = index_str.parse().ok()?;
            current = current.get(index)?;
            remaining = &remaining[close + 1..];
        } else {
            return None;
        }
    }

    Some(current)
}

/// Resolve a JSONPath to a mutable reference. Returns None if path is invalid.
fn json_get_mut<'a>(root: &'a mut Value, path: &str) -> Option<&'a mut Value> {
    let path = path.trim();
    if path.is_empty() || path == "." || path == "$" {
        return Some(root);
    }

    let normalized = if path.starts_with('$') {
        &path[1..]
    } else if path.starts_with('.') {
        path
    } else {
        return None;
    };

    let mut current = root;
    let mut remaining = normalized;

    while !remaining.is_empty() {
        if remaining.starts_with('.') {
            remaining = &remaining[1..];
            if remaining.is_empty() {
                break;
            }
            let end = remaining
                .find(|c: char| c == '.' || c == '[')
                .unwrap_or(remaining.len());
            let field = &remaining[..end];
            if field.is_empty() {
                return None;
            }
            current = current.get_mut(field)?;
            remaining = &remaining[end..];
        } else if remaining.starts_with('[') {
            let close = remaining.find(']')?;
            let index_str = &remaining[1..close];
            let index: usize = index_str.parse().ok()?;
            current = current.get_mut(index)?;
            remaining = &remaining[close + 1..];
        } else {
            return None;
        }
    }

    Some(current)
}

/// Split a path into parent path and the last segment (field name or array index).
/// Returns (parent_path, segment) where segment is either a field name or "[N]".
fn split_parent_path(path: &str) -> Option<(String, String)> {
    let path = path.trim();
    if path.is_empty() || path == "." || path == "$" {
        return None; // root has no parent
    }

    let normalized = if path.starts_with('$') {
        path.to_string()
    } else if path.starts_with('.') {
        format!("${}", path)
    } else {
        return None;
    };

    // Find the last segment
    if let Some(bracket_pos) = normalized.rfind('[') {
        let parent = &normalized[..bracket_pos];
        let segment = &normalized[bracket_pos..];
        let parent = if parent.is_empty() || parent == "$" {
            "$".to_string()
        } else {
            parent.to_string()
        };
        return Some((parent, segment.to_string()));
    }

    if let Some(dot_pos) = normalized.rfind('.') {
        if dot_pos == 0 || (dot_pos == 1 && normalized.starts_with('$')) {
            // e.g. "$.field" -> parent is "$", segment is "field"
            let segment = &normalized[dot_pos + 1..];
            return Some(("$".to_string(), segment.to_string()));
        }
        let parent = &normalized[..dot_pos];
        let segment = &normalized[dot_pos + 1..];
        return Some((parent.to_string(), segment.to_string()));
    }

    None
}

fn json_type_str(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// JSON.SET key path value
pub fn cmd_json_set(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 4 {
        return RespValue::wrong_arity("json.set");
    }

    let key = ctx.args[1].clone();
    let path = String::from_utf8_lossy(&ctx.args[2]).to_string();
    let value_str = String::from_utf8_lossy(&ctx.args[3]).to_string();

    let new_value: Value = match serde_json::from_str(&value_str) {
        Ok(v) => v,
        Err(_) => return RespValue::error("ERR invalid JSON value"),
    };

    // Check for NX/XX flags
    let mut nx = false;
    let mut xx = false;
    let mut i = 4;
    while i < ctx.args.len() {
        let flag = String::from_utf8_lossy(&ctx.args[i]).to_uppercase();
        match flag.as_str() {
            "NX" => nx = true,
            "XX" => xx = true,
            _ => return RespValue::error("ERR syntax error"),
        }
        i += 1;
    }

    let db = ctx.db();

    if path == "." || path == "$" {
        // Setting root
        if nx && db.exists(&key) {
            return RespValue::Null;
        }
        if xx && !db.exists(&key) {
            return RespValue::Null;
        }
        db.set(key, RedisObject::Json(new_value));
        return RespValue::ok();
    }

    // Non-root path: key must exist and be JSON
    let obj = match db.get_mut(&key) {
        Some(RedisObject::Json(v)) => v,
        Some(_) => return RespValue::wrong_type(),
        None => {
            if xx {
                return RespValue::Null;
            }
            // For non-root paths, key must exist
            return RespValue::error("ERR new objects must be created at the root");
        }
    };

    // Check NX/XX on the path
    let path_exists = json_get(obj, &path).is_some();
    if nx && path_exists {
        return RespValue::Null;
    }
    if xx && !path_exists {
        return RespValue::Null;
    }

    if path_exists {
        // Path exists, update in place
        if let Some(target) = json_get_mut(obj, &path) {
            *target = new_value;
            return RespValue::ok();
        }
        return RespValue::error("ERR path error");
    }

    // Path doesn't exist, try to create it by setting on parent
    if let Some((parent_path, segment)) = split_parent_path(&path) {
        if let Some(parent) = json_get_mut(obj, &parent_path) {
            if segment.starts_with('[') {
                return RespValue::error("ERR array index out of range");
            }
            if let Some(map) = parent.as_object_mut() {
                map.insert(segment, new_value);
                return RespValue::ok();
            }
        }
    }

    RespValue::error("ERR path error")
}

/// JSON.GET key [path ...]
pub fn cmd_json_get(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 2 {
        return RespValue::wrong_arity("json.get");
    }

    let key = ctx.args[1].clone();
    let paths: Vec<String> = ctx.args[2..].iter()
        .map(|a| String::from_utf8_lossy(a).to_string())
        .collect();
    let db = ctx.db();

    let obj = match db.get(&key) {
        Some(RedisObject::Json(v)) => v,
        Some(_) => return RespValue::wrong_type(),
        None => return RespValue::Null,
    };

    if paths.is_empty() {
        // No path specified, return entire document
        let s = serde_json::to_string(obj).unwrap_or_else(|_| "null".to_string());
        return RespValue::bulk_string(Bytes::from(s));
    }

    if paths.len() == 1 {
        // Single path
        match json_get(obj, &paths[0]) {
            Some(v) => {
                let s = serde_json::to_string(v).unwrap_or_else(|_| "null".to_string());
                RespValue::bulk_string(Bytes::from(s))
            }
            None => RespValue::Null,
        }
    } else {
        // Multiple paths: return as JSON object mapping path -> value
        let mut result = serde_json::Map::new();
        for path in &paths {
            match json_get(obj, path) {
                Some(v) => {
                    result.insert(path.clone(), v.clone());
                }
                None => {
                    result.insert(path.clone(), Value::Null);
                }
            }
        }
        let s = serde_json::to_string(&Value::Object(result))
            .unwrap_or_else(|_| "{}".to_string());
        RespValue::bulk_string(Bytes::from(s))
    }
}

/// JSON.DEL key [path]
pub fn cmd_json_del(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 2 {
        return RespValue::wrong_arity("json.del");
    }

    let key = ctx.args[1].clone();
    let path = if ctx.args.len() >= 3 {
        String::from_utf8_lossy(&ctx.args[2]).to_string()
    } else {
        "$".to_string()
    };

    let db = ctx.db();

    if path == "." || path == "$" {
        // Delete the whole key
        match db.remove(&key) {
            Some(_) => return RespValue::integer(1),
            None => return RespValue::integer(0),
        }
    }

    let obj = match db.get_mut(&key) {
        Some(RedisObject::Json(v)) => v,
        Some(_) => return RespValue::wrong_type(),
        None => return RespValue::integer(0),
    };

    // Delete at path
    if let Some((parent_path, segment)) = split_parent_path(&path) {
        if let Some(parent) = json_get_mut(obj, &parent_path) {
            if segment.starts_with('[') {
                // Array index removal
                let close = segment.find(']').unwrap_or(segment.len());
                let idx_str = &segment[1..close];
                if let Ok(idx) = idx_str.parse::<usize>() {
                    if let Some(arr) = parent.as_array_mut() {
                        if idx < arr.len() {
                            arr.remove(idx);
                            return RespValue::integer(1);
                        }
                    }
                }
            } else if let Some(map) = parent.as_object_mut() {
                if map.remove(&segment).is_some() {
                    return RespValue::integer(1);
                }
            }
        }
    }

    RespValue::integer(0)
}

/// JSON.NUMINCRBY key path value
pub fn cmd_json_numincrby(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() != 4 {
        return RespValue::wrong_arity("json.numincrby");
    }

    let key = ctx.args[1].clone();
    let path = String::from_utf8_lossy(&ctx.args[2]).to_string();
    let incr_str = String::from_utf8_lossy(&ctx.args[3]).to_string();
    let incr: f64 = match incr_str.parse() {
        Ok(v) => v,
        Err(_) => return RespValue::error("ERR value is not a number"),
    };

    let db = ctx.db();
    let obj = match db.get_mut(&key) {
        Some(RedisObject::Json(v)) => v,
        Some(_) => return RespValue::wrong_type(),
        None => return RespValue::error("ERR no such key"),
    };

    let target = match json_get_mut(obj, &path) {
        Some(v) => v,
        None => return RespValue::error("ERR path does not exist"),
    };

    match target {
        Value::Number(n) => {
            let current = n.as_f64().unwrap_or(0.0);
            let new_val = current + incr;
            // Try to keep as integer if possible
            if new_val.fract() == 0.0 && new_val >= i64::MIN as f64 && new_val <= i64::MAX as f64 {
                *target = Value::Number(serde_json::Number::from(new_val as i64));
            } else {
                match serde_json::Number::from_f64(new_val) {
                    Some(n) => *target = Value::Number(n),
                    None => return RespValue::error("ERR result is not a finite number"),
                }
            }
            let s = serde_json::to_string(target).unwrap_or_else(|_| "null".to_string());
            RespValue::bulk_string(Bytes::from(s))
        }
        _ => RespValue::error("ERR path value is not a number"),
    }
}

/// JSON.NUMMULTBY key path value
pub fn cmd_json_nummultby(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() != 4 {
        return RespValue::wrong_arity("json.nummultby");
    }

    let key = ctx.args[1].clone();
    let path = String::from_utf8_lossy(&ctx.args[2]).to_string();
    let mult_str = String::from_utf8_lossy(&ctx.args[3]).to_string();
    let mult: f64 = match mult_str.parse() {
        Ok(v) => v,
        Err(_) => return RespValue::error("ERR value is not a number"),
    };

    let db = ctx.db();
    let obj = match db.get_mut(&key) {
        Some(RedisObject::Json(v)) => v,
        Some(_) => return RespValue::wrong_type(),
        None => return RespValue::error("ERR no such key"),
    };

    let target = match json_get_mut(obj, &path) {
        Some(v) => v,
        None => return RespValue::error("ERR path does not exist"),
    };

    match target {
        Value::Number(n) => {
            let current = n.as_f64().unwrap_or(0.0);
            let new_val = current * mult;
            if new_val.fract() == 0.0 && new_val >= i64::MIN as f64 && new_val <= i64::MAX as f64 {
                *target = Value::Number(serde_json::Number::from(new_val as i64));
            } else {
                match serde_json::Number::from_f64(new_val) {
                    Some(n) => *target = Value::Number(n),
                    None => return RespValue::error("ERR result is not a finite number"),
                }
            }
            let s = serde_json::to_string(target).unwrap_or_else(|_| "null".to_string());
            RespValue::bulk_string(Bytes::from(s))
        }
        _ => RespValue::error("ERR path value is not a number"),
    }
}

/// JSON.STRAPPEND key path value
pub fn cmd_json_strappend(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() != 4 {
        return RespValue::wrong_arity("json.strappend");
    }

    let key = ctx.args[1].clone();
    let path = String::from_utf8_lossy(&ctx.args[2]).to_string();
    let append_str = String::from_utf8_lossy(&ctx.args[3]).to_string();

    // The value should be a JSON string (quoted)
    let append_val: Value = match serde_json::from_str(&append_str) {
        Ok(Value::String(s)) => Value::String(s),
        _ => return RespValue::error("ERR value is not a valid JSON string"),
    };

    let db = ctx.db();
    let obj = match db.get_mut(&key) {
        Some(RedisObject::Json(v)) => v,
        Some(_) => return RespValue::wrong_type(),
        None => return RespValue::error("ERR no such key"),
    };

    let target = match json_get_mut(obj, &path) {
        Some(v) => v,
        None => return RespValue::error("ERR path does not exist"),
    };

    match target {
        Value::String(s) => {
            if let Value::String(to_append) = append_val {
                s.push_str(&to_append);
                RespValue::integer(s.len() as i64)
            } else {
                RespValue::error("ERR value is not a string")
            }
        }
        _ => RespValue::error("ERR path value is not a string"),
    }
}

/// JSON.ARRAPPEND key path value [value ...]
pub fn cmd_json_arrappend(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 4 {
        return RespValue::wrong_arity("json.arrappend");
    }

    let key = ctx.args[1].clone();
    let path = String::from_utf8_lossy(&ctx.args[2]).to_string();

    let mut values = Vec::new();
    for arg in &ctx.args[3..] {
        let s = String::from_utf8_lossy(arg).to_string();
        match serde_json::from_str::<Value>(&s) {
            Ok(v) => values.push(v),
            Err(_) => return RespValue::error("ERR invalid JSON value"),
        }
    }

    let db = ctx.db();
    let obj = match db.get_mut(&key) {
        Some(RedisObject::Json(v)) => v,
        Some(_) => return RespValue::wrong_type(),
        None => return RespValue::error("ERR no such key"),
    };

    let target = match json_get_mut(obj, &path) {
        Some(v) => v,
        None => return RespValue::error("ERR path does not exist"),
    };

    match target {
        Value::Array(arr) => {
            arr.extend(values);
            RespValue::integer(arr.len() as i64)
        }
        _ => RespValue::error("ERR path value is not an array"),
    }
}

/// JSON.ARRLEN key [path]
pub fn cmd_json_arrlen(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 2 {
        return RespValue::wrong_arity("json.arrlen");
    }

    let key = ctx.args[1].clone();
    let path = if ctx.args.len() >= 3 {
        String::from_utf8_lossy(&ctx.args[2]).to_string()
    } else {
        "$".to_string()
    };

    let db = ctx.db();
    let obj = match db.get(&key) {
        Some(RedisObject::Json(v)) => v,
        Some(_) => return RespValue::wrong_type(),
        None => return RespValue::Null,
    };

    match json_get(obj, &path) {
        Some(Value::Array(arr)) => RespValue::integer(arr.len() as i64),
        Some(_) => RespValue::error("ERR path value is not an array"),
        None => RespValue::Null,
    }
}

/// JSON.ARRPOP key [path [index]]
pub fn cmd_json_arrpop(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 2 {
        return RespValue::wrong_arity("json.arrpop");
    }

    let key = ctx.args[1].clone();
    let path = if ctx.args.len() >= 3 {
        String::from_utf8_lossy(&ctx.args[2]).to_string()
    } else {
        "$".to_string()
    };
    let index: Option<i64> = if ctx.args.len() >= 4 {
        match String::from_utf8_lossy(&ctx.args[3]).parse() {
            Ok(v) => Some(v),
            Err(_) => return RespValue::error("ERR value is not an integer or out of range"),
        }
    } else {
        None // default: pop last element
    };

    let db = ctx.db();
    let obj = match db.get_mut(&key) {
        Some(RedisObject::Json(v)) => v,
        Some(_) => return RespValue::wrong_type(),
        None => return RespValue::Null,
    };

    let target = match json_get_mut(obj, &path) {
        Some(v) => v,
        None => return RespValue::Null,
    };

    match target {
        Value::Array(arr) => {
            if arr.is_empty() {
                return RespValue::Null;
            }
            let idx = match index {
                Some(i) => {
                    let len = arr.len() as i64;
                    let resolved = if i < 0 { (len + i).max(0) as usize } else { i.min(len - 1) as usize };
                    resolved
                }
                None => arr.len() - 1, // pop last
            };
            if idx >= arr.len() {
                return RespValue::Null;
            }
            let removed = arr.remove(idx);
            let s = serde_json::to_string(&removed).unwrap_or_else(|_| "null".to_string());
            RespValue::bulk_string(Bytes::from(s))
        }
        _ => RespValue::error("ERR path value is not an array"),
    }
}

/// JSON.TYPE key [path]
pub fn cmd_json_type(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 2 {
        return RespValue::wrong_arity("json.type");
    }

    let key = ctx.args[1].clone();
    let path = if ctx.args.len() >= 3 {
        String::from_utf8_lossy(&ctx.args[2]).to_string()
    } else {
        "$".to_string()
    };

    let db = ctx.db();
    let obj = match db.get(&key) {
        Some(RedisObject::Json(v)) => v,
        Some(_) => return RespValue::wrong_type(),
        None => return RespValue::Null,
    };

    match json_get(obj, &path) {
        Some(v) => RespValue::bulk_string(Bytes::from(json_type_str(v))),
        None => RespValue::Null,
    }
}

/// JSON.OBJKEYS key [path]
pub fn cmd_json_objkeys(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 2 {
        return RespValue::wrong_arity("json.objkeys");
    }

    let key = ctx.args[1].clone();
    let path = if ctx.args.len() >= 3 {
        String::from_utf8_lossy(&ctx.args[2]).to_string()
    } else {
        "$".to_string()
    };

    let db = ctx.db();
    let obj = match db.get(&key) {
        Some(RedisObject::Json(v)) => v,
        Some(_) => return RespValue::wrong_type(),
        None => return RespValue::Null,
    };

    match json_get(obj, &path) {
        Some(Value::Object(map)) => {
            let keys: Vec<RespValue> = map
                .keys()
                .map(|k| RespValue::bulk_string(Bytes::from(k.clone())))
                .collect();
            RespValue::array(keys)
        }
        Some(_) => RespValue::error("ERR path value is not an object"),
        None => RespValue::Null,
    }
}

/// JSON.OBJLEN key [path]
pub fn cmd_json_objlen(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 2 {
        return RespValue::wrong_arity("json.objlen");
    }

    let key = ctx.args[1].clone();
    let path = if ctx.args.len() >= 3 {
        String::from_utf8_lossy(&ctx.args[2]).to_string()
    } else {
        "$".to_string()
    };

    let db = ctx.db();
    let obj = match db.get(&key) {
        Some(RedisObject::Json(v)) => v,
        Some(_) => return RespValue::wrong_type(),
        None => return RespValue::Null,
    };

    match json_get(obj, &path) {
        Some(Value::Object(map)) => RespValue::integer(map.len() as i64),
        Some(_) => RespValue::error("ERR path value is not an object"),
        None => RespValue::Null,
    }
}

/// JSON.TOGGLE key path
pub fn cmd_json_toggle(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() != 3 {
        return RespValue::wrong_arity("json.toggle");
    }

    let key = ctx.args[1].clone();
    let path = String::from_utf8_lossy(&ctx.args[2]).to_string();

    let db = ctx.db();
    let obj = match db.get_mut(&key) {
        Some(RedisObject::Json(v)) => v,
        Some(_) => return RespValue::wrong_type(),
        None => return RespValue::error("ERR no such key"),
    };

    let target = match json_get_mut(obj, &path) {
        Some(v) => v,
        None => return RespValue::error("ERR path does not exist"),
    };

    match target {
        Value::Bool(b) => {
            *b = !*b;
            let s = serde_json::to_string(target).unwrap_or_else(|_| "null".to_string());
            RespValue::bulk_string(Bytes::from(s))
        }
        _ => RespValue::error("ERR path value is not a boolean"),
    }
}

/// JSON.CLEAR key [path]
pub fn cmd_json_clear(ctx: &mut CommandContext) -> RespValue {
    if ctx.args.len() < 2 {
        return RespValue::wrong_arity("json.clear");
    }

    let key = ctx.args[1].clone();
    let path = if ctx.args.len() >= 3 {
        String::from_utf8_lossy(&ctx.args[2]).to_string()
    } else {
        "$".to_string()
    };

    let db = ctx.db();
    let obj = match db.get_mut(&key) {
        Some(RedisObject::Json(v)) => v,
        Some(_) => return RespValue::wrong_type(),
        None => return RespValue::integer(0),
    };

    let target = match json_get_mut(obj, &path) {
        Some(v) => v,
        None => return RespValue::integer(0),
    };

    match target {
        Value::Array(arr) => {
            arr.clear();
            RespValue::integer(1)
        }
        Value::Object(map) => {
            map.clear();
            RespValue::integer(1)
        }
        Value::Number(_) => {
            *target = Value::Number(serde_json::Number::from(0));
            RespValue::integer(1)
        }
        _ => RespValue::integer(0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_get_root() {
        let v: Value = serde_json::json!({"a": 1, "b": "hello"});
        assert_eq!(json_get(&v, "$"), Some(&v));
        assert_eq!(json_get(&v, "."), Some(&v));
    }

    #[test]
    fn test_json_get_field() {
        let v: Value = serde_json::json!({"a": 1, "b": {"c": 2}});
        assert_eq!(json_get(&v, "$.a"), Some(&serde_json::json!(1)));
        assert_eq!(json_get(&v, "$.b.c"), Some(&serde_json::json!(2)));
    }

    #[test]
    fn test_json_get_array() {
        let v: Value = serde_json::json!({"arr": [10, 20, 30]});
        assert_eq!(json_get(&v, "$.arr[1]"), Some(&serde_json::json!(20)));
    }

    #[test]
    fn test_json_get_nested() {
        let v: Value = serde_json::json!({"a": {"b": [{"c": 42}]}});
        assert_eq!(json_get(&v, "$.a.b[0].c"), Some(&serde_json::json!(42)));
    }

    #[test]
    fn test_json_get_missing() {
        let v: Value = serde_json::json!({"a": 1});
        assert_eq!(json_get(&v, "$.z"), None);
    }

    #[test]
    fn test_split_parent_path() {
        assert_eq!(
            split_parent_path("$.a.b"),
            Some(("$.a".to_string(), "b".to_string()))
        );
        assert_eq!(
            split_parent_path("$.a"),
            Some(("$".to_string(), "a".to_string()))
        );
        assert_eq!(
            split_parent_path("$.a[0]"),
            Some(("$.a".to_string(), "[0]".to_string()))
        );
        assert_eq!(split_parent_path("$"), None);
    }
}
