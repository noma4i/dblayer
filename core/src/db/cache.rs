use std::num::NonZeroUsize;

use lru::LruCache;

use super::row::Row;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
}

pub(crate) struct RowCache {
    entries: LruCache<(String, String), Row>,
    stats: CacheStats,
}

impl RowCache {
    pub(crate) fn new(capacity: usize) -> Self {
        Self { entries: LruCache::new(NonZeroUsize::new(capacity.max(1)).expect("capacity is positive")), stats: CacheStats::default() }
    }

    pub(crate) fn get(&mut self, model: &str, id: &str) -> Option<Row> {
        let found = self.entries.get(&(model.to_string(), id.to_string())).cloned();
        self.count(found.is_some());
        found
    }

    pub(crate) fn fresh(&mut self, model: &str, id: &str, revision: i64) -> Option<Row> {
        let found = self.entries.get(&(model.to_string(), id.to_string())).filter(|row| row.revision == revision).cloned();
        self.count(found.is_some());
        found
    }

    pub(crate) fn put(&mut self, model: &str, row: Row) {
        let key = (model.to_string(), row.id.to_string());
        if let Some((displaced, _)) = self.entries.push(key.clone(), row) {
            if displaced != key {
                self.stats.evictions += 1;
            }
        }
    }

    pub(crate) fn evict(&mut self, model: &str, id: &str) {
        if self.entries.pop(&(model.to_string(), id.to_string())).is_some() {
            self.stats.evictions += 1;
        }
    }

    pub(crate) fn clear(&mut self) {
        self.entries.clear();
    }

    pub(crate) fn evict_models(&mut self, models: &[String]) {
        let doomed: Vec<(String, String)> = self.entries.iter().filter(|(key, _)| models.contains(&key.0)).map(|(key, _)| key.clone()).collect();
        for key in doomed {
            self.entries.pop(&key);
        }
    }

    pub(crate) fn stats(&self) -> CacheStats {
        self.stats
    }

    fn count(&mut self, hit: bool) {
        if hit {
            self.stats.hits += 1;
        } else {
            self.stats.misses += 1;
        }
    }
}
