use bytes::Bytes;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

/// Internal Redis data types.
#[derive(Debug, Clone)]
pub enum RedisObject {
    String(Bytes),
    List(VecDeque<Bytes>),
    Set(HashSet<Bytes>),
    Hash(HashMap<Bytes, Bytes>),
    SortedSet(SortedSetData),
}

impl RedisObject {
    pub fn type_name(&self) -> &'static str {
        match self {
            RedisObject::String(_) => "string",
            RedisObject::List(_) => "list",
            RedisObject::Set(_) => "set",
            RedisObject::Hash(_) => "hash",
            RedisObject::SortedSet(_) => "zset",
        }
    }

    pub fn encoding_name(&self) -> &'static str {
        match self {
            RedisObject::String(b) => {
                if std::str::from_utf8(b).ok().and_then(|s| s.parse::<i64>().ok()).is_some() {
                    "int"
                } else if b.len() <= 44 {
                    "embstr"
                } else {
                    "raw"
                }
            }
            RedisObject::List(l) => {
                if l.len() <= 128 {
                    "listpack"
                } else {
                    "quicklist"
                }
            }
            RedisObject::Set(s) => {
                let all_ints = s.iter().all(|v| std::str::from_utf8(v).ok().and_then(|s| s.parse::<i64>().ok()).is_some());
                if all_ints && s.len() <= 512 {
                    "intset"
                } else if s.len() <= 128 {
                    "listpack"
                } else {
                    "hashtable"
                }
            }
            RedisObject::Hash(h) => {
                if h.len() <= 128 {
                    "listpack"
                } else {
                    "hashtable"
                }
            }
            RedisObject::SortedSet(z) => {
                if z.members.len() <= 128 {
                    "listpack"
                } else {
                    "skiplist"
                }
            }
        }
    }
}

/// Sorted set data: member->score mapping + score-ordered index.
#[derive(Debug, Clone)]
pub struct SortedSetData {
    pub members: HashMap<Bytes, f64>,
    pub scores: BTreeMap<ScoreKey, Bytes>,
}

/// A key for the score-ordered BTreeMap: (score, member) for unique ordering.
#[derive(Debug, Clone)]
pub struct ScoreKey {
    pub score: f64,
    pub member: Bytes,
}

impl PartialEq for ScoreKey {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score && self.member == other.member
    }
}

impl Eq for ScoreKey {}

impl PartialOrd for ScoreKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoreKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score.partial_cmp(&other.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| self.member.cmp(&other.member))
    }
}

impl SortedSetData {
    pub fn new() -> Self {
        Self {
            members: HashMap::new(),
            scores: BTreeMap::new(),
        }
    }

    /// Add or update a member. Returns true if the member was newly inserted.
    pub fn insert(&mut self, member: Bytes, score: f64) -> bool {
        if let Some(old_score) = self.members.insert(member.clone(), score) {
            // Remove old score entry
            self.scores.remove(&ScoreKey { score: old_score, member: member.clone() });
            self.scores.insert(ScoreKey { score, member }, Bytes::new());
            false
        } else {
            self.scores.insert(ScoreKey { score, member }, Bytes::new());
            true
        }
    }

    pub fn remove(&mut self, member: &Bytes) -> bool {
        if let Some(score) = self.members.remove(member) {
            self.scores.remove(&ScoreKey { score, member: member.clone() });
            true
        } else {
            false
        }
    }

    pub fn score(&self, member: &Bytes) -> Option<f64> {
        self.members.get(member).copied()
    }

    pub fn len(&self) -> usize {
        self.members.len()
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// Get the rank (0-based position) of a member.
    pub fn rank(&self, member: &Bytes) -> Option<usize> {
        let score = self.members.get(member)?;
        let key = ScoreKey { score: *score, member: member.clone() };
        Some(self.scores.range(..&key).count())
    }

    /// Get the reverse rank.
    pub fn rev_rank(&self, member: &Bytes) -> Option<usize> {
        let rank = self.rank(member)?;
        Some(self.len() - 1 - rank)
    }

    /// Range by index (inclusive start/end).
    pub fn range_by_index(&self, start: i64, stop: i64) -> Vec<(Bytes, f64)> {
        let len = self.len() as i64;
        let start = if start < 0 { (len + start).max(0) } else { start.min(len) } as usize;
        let stop = if stop < 0 { (len + stop).max(0) } else { stop.min(len - 1) } as usize;

        if start > stop {
            return vec![];
        }

        self.scores.iter()
            .skip(start)
            .take(stop - start + 1)
            .map(|(k, _)| (k.member.clone(), k.score))
            .collect()
    }

    /// Range by score (inclusive min/max).
    pub fn range_by_score(&self, min: f64, max: f64) -> Vec<(Bytes, f64)> {
        use std::ops::Bound;
        let min_key = ScoreKey { score: min, member: Bytes::new() };
        let max_key = ScoreKey { score: max, member: Bytes::from(vec![0xff; 32]) };
        self.scores.range((Bound::Included(&min_key), Bound::Included(&max_key)))
            .map(|(k, _)| (k.member.clone(), k.score))
            .collect()
    }

    /// Count members with scores in [min, max].
    pub fn count_by_score(&self, min: f64, max: f64) -> usize {
        self.range_by_score(min, max).len()
    }

    /// Pop the member with the minimum score.
    pub fn pop_min(&mut self) -> Option<(Bytes, f64)> {
        let key = self.scores.keys().next()?.clone();
        self.scores.remove(&key);
        self.members.remove(&key.member);
        Some((key.member, key.score))
    }

    /// Pop the member with the maximum score.
    pub fn pop_max(&mut self) -> Option<(Bytes, f64)> {
        let key = self.scores.keys().next_back()?.clone();
        self.scores.remove(&key);
        self.members.remove(&key.member);
        Some((key.member, key.score))
    }

    /// Get a random member.
    pub fn random_member(&self) -> Option<(&Bytes, f64)> {
        use rand::seq::IteratorRandom;
        let mut rng = rand::thread_rng();
        self.members.iter().choose(&mut rng).map(|(k, &v)| (k, v))
    }
}
