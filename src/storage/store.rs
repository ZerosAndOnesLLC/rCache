use super::db::Database;

/// The top-level store containing multiple databases.
pub struct Store {
    databases: Vec<Database>,
}

impl Store {
    pub fn new(num_databases: usize) -> Self {
        let databases = (0..num_databases).map(|_| Database::new()).collect();
        Self { databases }
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
}
