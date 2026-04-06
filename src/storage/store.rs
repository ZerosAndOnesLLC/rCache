use bytes::Bytes;
use super::db::Database;

/// The top-level store containing multiple databases.
pub struct Store {
    databases: Vec<Database>,
    /// Timestamp (unix secs) of last successful RDB save.
    pub last_save: u64,
}

impl Store {
    pub fn new(num_databases: usize) -> Self {
        let databases = (0..num_databases).map(|_| Database::new()).collect();
        Self {
            databases,
            last_save: 0,
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
    pub fn check_memory_limit(&mut self, maxmemory: usize, policy: &str, samples: usize) -> Result<(), ()> {
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
            if !self.evict_one(policy, samples) {
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
    fn evict_one(&mut self, policy: &str, samples: usize) -> bool {
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
}
