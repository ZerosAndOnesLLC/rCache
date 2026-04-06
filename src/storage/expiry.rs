use std::time::{Duration, Instant};
use super::store::Store;

const MAX_CYCLE_DURATION: Duration = Duration::from_millis(25);

/// Manages active key expiration across all databases.
pub struct ExpirationManager {
    sample_size: usize,
}

impl ExpirationManager {
    pub fn new(sample_size: usize) -> Self {
        Self { sample_size }
    }

    /// Run one expiration cycle across all databases.
    /// Repeats sampling if >25% of sampled keys were expired (Redis behavior).
    /// Time-budgeted to avoid starving command processing.
    pub fn run_cycle(&self, store: &mut Store) {
        let start = Instant::now();
        for db_index in 0..store.db_count() {
            let db = store.db_mut(db_index);
            if db.expires_len() == 0 {
                continue;
            }
            loop {
                if start.elapsed() >= MAX_CYCLE_DURATION {
                    return;
                }
                let deleted = db.expire_cycle(self.sample_size);
                // If more than 25% were expired, repeat
                if deleted > 0 && deleted * 4 >= self.sample_size {
                    continue;
                }
                break;
            }
        }
    }
}
