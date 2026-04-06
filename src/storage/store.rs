use bytes::Bytes;
use super::db::Database;

/// Maximum size of the eviction pool for LFU.
const EVICTION_POOL_SIZE: usize = 16;

/// The top-level store containing multiple databases.
pub struct Store {
    databases: Vec<Database>,
    /// Timestamp (unix secs) of last successful RDB save.
    pub last_save: u64,
    /// Eviction pool for LFU: (key, counter, db_index), kept sorted so the
    /// worst candidate (lowest counter) can be efficiently popped.
    lfu_eviction_pool: Vec<(Bytes, u8, usize)>,
}

impl std::fmt::Debug for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Store")
            .field("databases", &self.databases.len())
            .field("last_save", &self.last_save)
            .finish()
    }
}

impl Store {
    pub fn new(num_databases: usize) -> Self {
        let databases = (0..num_databases).map(|_| Database::new()).collect();
        Self {
            databases,
            last_save: 0,
            lfu_eviction_pool: Vec::with_capacity(EVICTION_POOL_SIZE),
        }
    }

    pub fn db(&self, index: usize) -> &Database {
        &self.databases[index]
    }

    pub fn db_mut(&mut self, index: usize) -> &mut Database {
        &mut self.databases[index]
    }

    pub fn db_count(&self) -> usize {
        self.databases.len()
    }

    /// Swap two databases by index.
    pub fn swap_db(&mut self, a: usize, b: usize) {
        if a != b && a < self.databases.len() && b < self.databases.len() {
            let (left, right) = if a < b {
                let (l, r) = self.databases.split_at_mut(b);
                (&mut l[a], &mut r[0])
            } else {
                let (l, r) = self.databases.split_at_mut(a);
                (&mut r[0], &mut l[b])
            };
            left.swap_with(right);
        }
    }

    /// Flush all databases.
    pub fn flush_all(&mut self) {
        for db in &mut self.databases {
            db.flush();
        }
    }

    /// Total approximate memory usage across all databases.
    pub fn total_used_memory(&self) -> usize {
        self.databases.iter().map(|db| db.used_memory).sum()
    }

    /// Check if memory limit is exceeded and try to evict keys.
    /// Returns Ok(()) if under limit or eviction succeeded, Err if OOM and noeviction.
    pub fn check_memory_limit(
        &mut self,
        maxmemory: usize,
        policy: &str,
        samples: usize,
        lfu_log_factor: u64,
        lfu_decay_time: u64,
    ) -> Result<(), ()> {
        if maxmemory == 0 {
            return Ok(());
        }
        let used = self.total_used_memory();
        if used <= maxmemory {
            return Ok(());
        }

        // Try to evict until under limit
        let mut attempts = 0;
        while self.total_used_memory() > maxmemory && attempts < 100 {
            attempts += 1;
            if !self.evict_one(policy, samples, lfu_log_factor, lfu_decay_time) {
                return Err(());
            }
        }

        if self.total_used_memory() > maxmemory {
            Err(())
        } else {
            Ok(())
        }
    }

    /// Evict a single key according to policy. Returns false if no key could be evicted.
    fn evict_one(&mut self, policy: &str, samples: usize, lfu_log_factor: u64, lfu_decay_time: u64) -> bool {
        use rand::seq::SliceRandom;
        let mut rng = rand::thread_rng();

        match policy {
            "noeviction" => false,
            "allkeys-random" => {
                // Pick a random database with keys, remove a random key
                for db in &mut self.databases {
                    if db.is_empty() {
                        continue;
                    }
                    let keys = db.all_keys();
                    if let Some(key) = keys.choose(&mut rng) {
                        db.remove(key);
                        return true;
                    }
                }
                false
            }
            "volatile-random" => {
                for db in &mut self.databases {
                    let vkeys = db.volatile_keys();
                    if vkeys.is_empty() {
                        continue;
                    }
                    if let Some(key) = vkeys.choose(&mut rng) {
                        db.remove(key);
                        return true;
                    }
                }
                false
            }
            "allkeys-lru" => {
                self.evict_lru(samples, false)
            }
            "volatile-lru" => {
                self.evict_lru(samples, true)
            }
            "allkeys-lfu" => {
                self.evict_lfu(samples, false, lfu_log_factor, lfu_decay_time)
            }
            "volatile-lfu" => {
                self.evict_lfu(samples, true, lfu_log_factor, lfu_decay_time)
            }
            "volatile-ttl" => {
                self.evict_volatile_ttl(samples)
            }
            _ => false,
        }
    }

    /// Evict the least recently used key by sampling.
    fn evict_lru(&mut self, samples: usize, volatile_only: bool) -> bool {
        use rand::seq::SliceRandom;
        let mut rng = rand::thread_rng();

        let mut best_key: Option<Bytes> = None;
        let mut best_lru = u64::MAX;
        let mut best_db = 0;

        for (db_idx, db) in self.databases.iter().enumerate() {
            let candidates = if volatile_only {
                db.volatile_keys()
            } else {
                db.all_keys()
            };
            if candidates.is_empty() {
                continue;
            }
            let sampled: Vec<&Bytes> = candidates.choose_multiple(&mut rng, samples).collect();
            for key in sampled {
                let lru = db.lru_of(key).unwrap_or(0);
                if lru < best_lru {
                    best_lru = lru;
                    best_key = Some(key.clone());
                    best_db = db_idx;
                }
            }
        }

        if let Some(key) = best_key {
            self.databases[best_db].remove(&key);
            true
        } else {
            false
        }
    }

    /// Evict the key with the shortest TTL by sampling.
    fn evict_volatile_ttl(&mut self, samples: usize) -> bool {
        use rand::seq::SliceRandom;
        let mut rng = rand::thread_rng();

        let mut best_key: Option<Bytes> = None;
        let mut best_ttl = std::time::Duration::MAX;
        let mut best_db = 0;

        for (db_idx, db) in self.databases.iter().enumerate() {
            let vkeys = db.volatile_keys();
            if vkeys.is_empty() {
                continue;
            }
            let sampled: Vec<&Bytes> = vkeys.choose_multiple(&mut rng, samples).collect();
            for key in sampled {
                if let Some(ttl) = db.time_to_live(key) {
                    if ttl < best_ttl {
                        best_ttl = ttl;
                        best_key = Some(key.clone());
                        best_db = db_idx;
                    }
                } else {
                    // Already expired or no TTL - evict immediately
                    best_key = Some(key.clone());
                    best_ttl = std::time::Duration::ZERO;
                    best_db = db_idx;
                }
            }
        }

        if let Some(key) = best_key {
            self.databases[best_db].remove(&key);
            true
        } else {
            false
        }
    }

    /// Evict the least frequently used key using an eviction pool.
    /// Samples N keys, merges them into the pool (capped at EVICTION_POOL_SIZE),
    /// then evicts the worst candidate (lowest LFU counter) from the pool.
    fn evict_lfu(
        &mut self,
        samples: usize,
        volatile_only: bool,
        lfu_log_factor: u64,
        lfu_decay_time: u64,
    ) -> bool {
        use rand::seq::SliceRandom;
        let mut rng = rand::thread_rng();

        // Sample keys and get their LFU counters (applying decay via touch_lfu_with_params)
        let mut sampled_entries: Vec<(Bytes, u8, usize)> = Vec::new();

        for (db_idx, db) in self.databases.iter_mut().enumerate() {
            let candidates = if volatile_only {
                db.volatile_keys()
            } else {
                db.all_keys()
            };
            if candidates.is_empty() {
                continue;
            }
            let chosen: Vec<Bytes> = candidates
                .choose_multiple(&mut rng, samples)
                .cloned()
                .collect();
            for key in chosen {
                // Touch to apply decay before reading the counter
                db.touch_lfu_with_params(&key, lfu_log_factor, lfu_decay_time);
                let counter = db.lfu_of(&key).unwrap_or(0);
                sampled_entries.push((key, counter, db_idx));
            }
        }

        // Merge sampled entries into the eviction pool
        for entry in sampled_entries {
            if self.lfu_eviction_pool.len() < EVICTION_POOL_SIZE {
                self.lfu_eviction_pool.push(entry);
            } else {
                // Find the best (highest counter) entry in the pool and replace it
                // if this new entry is worse (lower counter)
                let best_idx = self
                    .lfu_eviction_pool
                    .iter()
                    .enumerate()
                    .max_by_key(|(_, (_, c, _))| *c)
                    .map(|(i, _)| i);
                if let Some(idx) = best_idx {
                    if entry.1 < self.lfu_eviction_pool[idx].1 {
                        self.lfu_eviction_pool[idx] = entry;
                    }
                }
            }
        }

        // Evict the worst candidate (lowest counter) from the pool
        if self.lfu_eviction_pool.is_empty() {
            return false;
        }

        let worst_idx = self
            .lfu_eviction_pool
            .iter()
            .enumerate()
            .min_by_key(|(_, (_, c, _))| *c)
            .map(|(i, _)| i)
            .unwrap();

        let (key, _, db_idx) = self.lfu_eviction_pool.remove(worst_idx);

        // Verify the key still exists before removing
        if db_idx < self.databases.len() && self.databases[db_idx].lfu_of(&key).is_some() {
            self.databases[db_idx].remove(&key);
            true
        } else {
            // Key was already removed; stale pool entry
            false
        }
    }
}
