use bytes::Bytes;
use std::collections::HashMap;
use std::time::Instant;
use super::types::RedisObject;

/// A single Redis database (one of the 16 numbered databases).
pub struct Database {
    data: HashMap<Bytes, RedisObject>,
    expires: HashMap<Bytes, Instant>,
    /// Monotonically increasing counter for LRU tracking (access order).
    pub(crate) lru_clock: u64,
    pub(crate) lru_map: HashMap<Bytes, u64>,
    /// Approximate memory usage in bytes.
    pub(crate) used_memory: usize,
}

impl Database {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
            expires: HashMap::new(),
            lru_clock: 0,
            lru_map: HashMap::new(),
            used_memory: 0,
        }
    }

    /// Get a value, performing lazy expiration check.
    pub fn get(&mut self, key: &Bytes) -> Option<&RedisObject> {
        if self.is_expired(key) {
            self.remove(key);
            return None;
        }
        self.touch_lru(key);
        self.data.get(key)
    }

    /// Get a mutable reference to a value.
    pub fn get_mut(&mut self, key: &Bytes) -> Option<&mut RedisObject> {
        if self.is_expired(key) {
            self.remove(key);
            return None;
        }
        self.touch_lru(key);
        self.data.get_mut(key)
    }

    /// Set a value, removing any existing expiration.
    pub fn set(&mut self, key: Bytes, value: RedisObject) {
        self.expires.remove(&key);
        self.touch_lru(&key);
        // Update memory tracking
        let new_size = key.len() + value.estimate_memory() + 64;
        if let Some(old) = self.data.get(&key) {
            let old_size = key.len() + old.estimate_memory() + 64;
            self.used_memory = self.used_memory.saturating_sub(old_size);
        }
        self.used_memory += new_size;
        self.data.insert(key, value);
    }

    /// Set a value, keeping the existing expiration if any.
    pub fn set_keep_ttl(&mut self, key: Bytes, value: RedisObject) {
        self.touch_lru(&key);
        // Update memory tracking
        let new_size = key.len() + value.estimate_memory() + 64;
        if let Some(old) = self.data.get(&key) {
            let old_size = key.len() + old.estimate_memory() + 64;
            self.used_memory = self.used_memory.saturating_sub(old_size);
        }
        self.used_memory += new_size;
        self.data.insert(key, value);
    }

    /// Check if a key exists (with lazy expiration).
    pub fn exists(&mut self, key: &Bytes) -> bool {
        if self.is_expired(key) {
            self.remove(key);
            return false;
        }
        self.data.contains_key(key)
    }

    /// Remove a key and its expiration.
    pub fn remove(&mut self, key: &Bytes) -> Option<RedisObject> {
        self.expires.remove(key);
        self.lru_map.remove(key);
        if let Some(obj) = self.data.remove(key) {
            let size = key.len() + obj.estimate_memory() + 64;
            self.used_memory = self.used_memory.saturating_sub(size);
            Some(obj)
        } else {
            None
        }
    }

    /// Set expiration on a key.
    pub fn set_expire(&mut self, key: &Bytes, when: Instant) -> bool {
        if self.data.contains_key(key) {
            self.expires.insert(key.clone(), when);
            true
        } else {
            false
        }
    }

    /// Remove expiration from a key.
    pub fn persist(&mut self, key: &Bytes) -> bool {
        self.expires.remove(key).is_some()
    }

    /// Get the expiration time for a key.
    pub fn get_expire(&self, key: &Bytes) -> Option<Instant> {
        self.expires.get(key).copied()
    }

    /// Check if a key has expired.
    fn is_expired(&self, key: &Bytes) -> bool {
        if let Some(expire) = self.expires.get(key) {
            Instant::now() >= *expire
        } else {
            false
        }
    }

    /// Get the TTL remaining in milliseconds, or None if no expiry.
    pub fn ttl_ms(&mut self, key: &Bytes) -> Option<i64> {
        if self.is_expired(key) {
            self.remove(key);
            return Some(-2); // Key doesn't exist
        }
        if !self.data.contains_key(key) {
            return Some(-2);
        }
        match self.expires.get(key) {
            Some(expire) => {
                let now = Instant::now();
                if *expire > now {
                    Some((*expire - now).as_millis() as i64)
                } else {
                    Some(-2)
                }
            }
            None => Some(-1), // No expiry
        }
    }

    /// Number of keys (including not-yet-expired).
    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Number of keys with an expiration set.
    pub fn expires_len(&self) -> usize {
        self.expires.len()
    }

    /// Clear all data.
    pub fn flush(&mut self) {
        self.data.clear();
        self.expires.clear();
        self.lru_map.clear();
        self.used_memory = 0;
    }

    /// Active expiration: sample random keys with TTL and delete expired ones.
    /// Returns the number of keys deleted.
    pub fn expire_cycle(&mut self, sample_size: usize) -> usize {
        use rand::seq::IteratorRandom;
        let mut rng = rand::thread_rng();
        let mut deleted = 0;

        let expired_keys: Vec<Bytes> = self.expires.iter()
            .choose_multiple(&mut rng, sample_size)
            .into_iter()
            .filter(|(_, expire)| Instant::now() >= **expire)
            .map(|(key, _)| key.clone())
            .collect();

        for key in expired_keys {
            if let Some(obj) = self.data.remove(&key) {
                let size = key.len() + obj.estimate_memory() + 64;
                self.used_memory = self.used_memory.saturating_sub(size);
            }
            self.expires.remove(&key);
            self.lru_map.remove(&key);
            deleted += 1;
        }

        deleted
    }

    /// Get all keys matching a glob pattern.
    pub fn keys(&mut self, pattern: &str) -> Vec<Bytes> {
        let keys: Vec<Bytes> = self.data.keys().cloned().collect();
        keys.into_iter()
            .filter(|k| {
                if self.is_expired(k) {
                    return false;
                }
                let key_str = String::from_utf8_lossy(k);
                glob_match(pattern, &key_str)
            })
            .collect()
    }

    /// Get a random key.
    pub fn random_key(&mut self) -> Option<Bytes> {
        use rand::seq::IteratorRandom;
        let mut rng = rand::thread_rng();
        loop {
            let key = self.data.keys().choose(&mut rng)?.clone();
            if self.is_expired(&key) {
                self.remove(&key);
                if self.data.is_empty() {
                    return None;
                }
                continue;
            }
            return Some(key);
        }
    }

    /// Rename a key. Returns false if source doesn't exist.
    pub fn rename(&mut self, from: &Bytes, to: Bytes) -> bool {
        if self.is_expired(from) {
            self.remove(from);
            return false;
        }
        if let Some(value) = self.data.remove(from) {
            let expire = self.expires.remove(from);
            self.data.insert(to.clone(), value);
            if let Some(exp) = expire {
                self.expires.insert(to.clone(), exp);
            }
            // Remove destination's old expiry if it was different
            true
        } else {
            false
        }
    }

    /// Get the type name of a key.
    pub fn key_type(&mut self, key: &Bytes) -> &'static str {
        match self.get(key) {
            Some(obj) => obj.type_name(),
            None => "none",
        }
    }

    /// Swap all data with another database.
    pub fn swap_with(&mut self, other: &mut Database) {
        std::mem::swap(&mut self.data, &mut other.data);
        std::mem::swap(&mut self.expires, &mut other.expires);
        std::mem::swap(&mut self.lru_map, &mut other.lru_map);
    }

    /// Copy a key to a destination. Returns false if source doesn't exist.
    pub fn copy_key(&mut self, from: &Bytes, to: Bytes, replace: bool) -> bool {
        if self.is_expired(from) {
            self.remove(from);
            return false;
        }
        if !replace && self.data.contains_key(&to) {
            return false;
        }
        if let Some(value) = self.data.get(from).cloned() {
            let expire = self.expires.get(from).copied();
            self.data.insert(to.clone(), value);
            if let Some(exp) = expire {
                self.expires.insert(to, exp);
            }
            true
        } else {
            false
        }
    }

    /// Iterate keys for SCAN command. Returns (new_cursor, keys).
    pub fn scan(&mut self, cursor: usize, pattern: Option<&str>, count: usize, type_filter: Option<&str>) -> (usize, Vec<Bytes>) {
        let all_keys: Vec<Bytes> = self.data.keys().cloned().collect();
        let total = all_keys.len();

        if total == 0 {
            return (0, vec![]);
        }

        let mut result = Vec::new();
        let start = cursor;
        let mut i = start;
        let mut scanned = 0;

        while scanned < count.max(10) && i < total {
            let key = &all_keys[i % total];
            i += 1;
            scanned += 1;

            if self.is_expired(key) {
                continue;
            }

            if let Some(pat) = pattern {
                let key_str = String::from_utf8_lossy(key);
                if !glob_match(pat, &key_str) {
                    continue;
                }
            }

            if let Some(tf) = type_filter {
                if let Some(obj) = self.data.get(key) {
                    if obj.type_name() != tf {
                        continue;
                    }
                }
            }

            result.push(key.clone());
        }

        let new_cursor = if i >= total { 0 } else { i };
        (new_cursor, result)
    }

    fn touch_lru(&mut self, key: &Bytes) {
        self.lru_clock += 1;
        self.lru_map.insert(key.clone(), self.lru_clock);
    }

    /// Iterate over all key-value pairs (for persistence).
    pub fn iter(&self) -> impl Iterator<Item = (&Bytes, &RedisObject)> {
        self.data.iter()
    }

    /// Get the expiry map (for persistence).
    pub fn expires_iter(&self) -> impl Iterator<Item = (&Bytes, &Instant)> {
        self.expires.iter()
    }

    /// Insert a key-value pair directly (used by persistence loading).
    /// Does not update LRU.
    pub fn set_raw(&mut self, key: Bytes, value: RedisObject) {
        let new_size = key.len() + value.estimate_memory() + 64;
        if let Some(old) = self.data.get(&key) {
            let old_size = key.len() + old.estimate_memory() + 64;
            self.used_memory = self.used_memory.saturating_sub(old_size);
        }
        self.used_memory += new_size;
        self.data.insert(key, value);
    }

    /// Set expiration directly (used by persistence loading).
    pub fn set_expire_raw(&mut self, key: Bytes, when: Instant) {
        self.expires.insert(key, when);
    }

    /// Get all keys (for eviction sampling).
    pub fn all_keys(&self) -> Vec<Bytes> {
        self.data.keys().cloned().collect()
    }

    /// Get all keys that have an expiry set (for volatile eviction).
    pub fn volatile_keys(&self) -> Vec<Bytes> {
        self.expires.keys().cloned().collect()
    }

    /// Get the LRU clock value for a key.
    pub fn lru_of(&self, key: &Bytes) -> Option<u64> {
        self.lru_map.get(key).copied()
    }

    /// Get the time-to-live remaining for a key, if it has an expiry.
    pub fn time_to_live(&self, key: &Bytes) -> Option<std::time::Duration> {
        self.expires.get(key).and_then(|exp| {
            let now = Instant::now();
            if *exp > now {
                Some(*exp - now)
            } else {
                None
            }
        })
    }
}

/// Simple glob pattern matching supporting *, ?, [abc], [a-z].
pub fn glob_match(pattern: &str, input: &str) -> bool {
    let pat: Vec<char> = pattern.chars().collect();
    let inp: Vec<char> = input.chars().collect();
    glob_match_inner(&pat, &inp)
}

fn glob_match_inner(pat: &[char], inp: &[char]) -> bool {
    let mut pi = 0;
    let mut ii = 0;
    let mut star_pi = None;
    let mut star_ii = None;

    while ii < inp.len() {
        if pi < pat.len() && pat[pi] == '?' {
            pi += 1;
            ii += 1;
        } else if pi < pat.len() && pat[pi] == '*' {
            star_pi = Some(pi);
            star_ii = Some(ii);
            pi += 1;
        } else if pi < pat.len() && pat[pi] == '[' {
            // Character class
            if let Some((matched, end)) = match_char_class(&pat[pi..], inp[ii]) {
                if matched {
                    pi += end;
                    ii += 1;
                } else if let (Some(sp), Some(si)) = (star_pi, star_ii) {
                    pi = sp + 1;
                    ii = si + 1;
                    star_ii = Some(si + 1);
                } else {
                    return false;
                }
            } else {
                return false;
            }
        } else if pi < pat.len() && pat[pi] == '\\' && pi + 1 < pat.len() {
            pi += 1;
            if pat[pi] == inp[ii] {
                pi += 1;
                ii += 1;
            } else if let (Some(sp), Some(si)) = (star_pi, star_ii) {
                pi = sp + 1;
                ii = si + 1;
                star_ii = Some(si + 1);
            } else {
                return false;
            }
        } else if pi < pat.len() && pat[pi] == inp[ii] {
            pi += 1;
            ii += 1;
        } else if let (Some(sp), Some(si)) = (star_pi, star_ii) {
            pi = sp + 1;
            ii = si + 1;
            star_ii = Some(si + 1);
        } else {
            return false;
        }
    }

    while pi < pat.len() && pat[pi] == '*' {
        pi += 1;
    }

    pi == pat.len()
}

fn match_char_class(pat: &[char], ch: char) -> Option<(bool, usize)> {
    if pat.is_empty() || pat[0] != '[' {
        return None;
    }

    let mut i = 1;
    let negate = if i < pat.len() && pat[i] == '^' {
        i += 1;
        true
    } else {
        false
    };

    let mut matched = false;
    while i < pat.len() && pat[i] != ']' {
        if i + 2 < pat.len() && pat[i + 1] == '-' {
            let lo = pat[i];
            let hi = pat[i + 2];
            if ch >= lo && ch <= hi {
                matched = true;
            }
            i += 3;
        } else {
            if pat[i] == ch {
                matched = true;
            }
            i += 1;
        }
    }

    if i < pat.len() && pat[i] == ']' {
        let result = if negate { !matched } else { matched };
        Some((result, i + 1))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_glob_match() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("h?llo", "hello"));
        assert!(glob_match("h?llo", "hallo"));
        assert!(!glob_match("h?llo", "hlo"));
        assert!(glob_match("h*llo", "hello"));
        assert!(glob_match("h*llo", "heeeello"));
        assert!(glob_match("h[ae]llo", "hello"));
        assert!(glob_match("h[ae]llo", "hallo"));
        assert!(!glob_match("h[ae]llo", "hillo"));
        assert!(glob_match("h[a-e]llo", "hcllo"));
        assert!(!glob_match("h[a-e]llo", "hzllo"));
        assert!(glob_match("h[^a-e]llo", "hzllo"));
        assert!(!glob_match("h[^a-e]llo", "hcllo"));
        assert!(glob_match("*", ""));
        assert!(glob_match("user:*", "user:123"));
        assert!(!glob_match("user:*", "admin:123"));
    }
}
