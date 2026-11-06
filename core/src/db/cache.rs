use std::num::NonZeroUsize;
use std::mem::size_of;

use lru::LruCache;
use serde_json::{Map, Value};

use crate::pacing::Counters;

use super::row::Row;

const MAX_ESTIMATED_BYTES: usize = 16 * 1024 * 1024;

fn object_bytes(data: &Map<String, Value>) -> usize {
    data.iter().map(|(key, value)| size_of::<(String, Value)>() + 4 * size_of::<usize>() + key.capacity() + value_bytes(value)).sum()
}

fn value_bytes(value: &Value) -> usize {
    match value {
        Value::String(text) => text.capacity(),
        Value::Array(items) => items.capacity() * size_of::<Value>() + items.iter().map(value_bytes).sum::<usize>(),
        Value::Object(data) => object_bytes(data),
        _ => 0,
    }
}

pub(crate) fn row_bytes(model: &str, row: &super::Record) -> usize {
    let strings = |values: &Vec<String>| values.capacity() * size_of::<String>() + values.iter().map(String::capacity).sum::<usize>();
    size_of::<super::row::Record>() + size_of::<(String, String)>() + usize::BITS as usize
        + model.len() + 2 * row.id.as_str().len() + object_bytes(&row.data)
        + strings(&row.sync.actions) + strings(&row.sync.dirty_fields)
        + row.sync.error.as_ref().map_or(0, |error| error.message.capacity())
        + row.sync.uploads.iter().map(|(action, upload)| action.capacity() + upload.error.as_ref().map_or(0, |error| error.message.capacity())).sum::<usize>()
}

pub(crate) struct RowCache {
    entries: LruCache<(String, String), (Row, usize)>,
    estimated_bytes: usize,
    pub(crate) committed_revision: i64,
    counters: Counters,
}

impl RowCache {
    pub(crate) fn new(capacity: usize) -> Self {
        Self { entries: LruCache::new(NonZeroUsize::new(capacity.max(1)).expect("capacity is positive")), estimated_bytes: 0, committed_revision: 0, counters: Counters::default() }
    }

    pub(crate) fn get(&mut self, model: &str, id: &str) -> Option<Row> {
        let found = self.entries.get(&(model.to_string(), id.to_string())).map(|(row, _)| row.clone());
        self.count(found.is_some());
        found
    }

    pub(crate) fn fresh(&mut self, model: &str, id: &str, revision: i64) -> Option<Row> {
        let found = self.entries.get(&(model.to_string(), id.to_string())).filter(|(row, _)| row.revision == revision).map(|(row, _)| row.clone());
        self.count(found.is_some());
        found
    }

    pub(crate) fn committed(&mut self, revision: i64) {
        self.committed_revision = self.committed_revision.max(revision);
    }

    #[cfg(test)]
    pub(crate) fn put(&mut self, model: &str, row: Row, snapshot_revision: i64) {
        let bytes = row_bytes(model, &row);
        self.put_sized(model, row, bytes, snapshot_revision);
    }

    pub(crate) fn put_sized(&mut self, model: &str, row: Row, bytes: usize, snapshot_revision: i64) {
        if snapshot_revision < self.committed_revision {
            return;
        }
        let key = (model.to_string(), row.id.to_string());
        if bytes > MAX_ESTIMATED_BYTES {
            self.evict(model, row.id.as_str());
            return;
        }
        self.estimated_bytes += bytes;
        if let Some((displaced, (_, removed))) = self.entries.push(key.clone(), (row, bytes)) {
            self.estimated_bytes -= removed;
            if displaced != key {
                self.counters.cache_evictions += 1;
            }
        }
        while self.estimated_bytes > MAX_ESTIMATED_BYTES {
            if let Some((_, (_, bytes))) = self.entries.pop_lru() {
                self.estimated_bytes -= bytes;
                self.counters.cache_evictions += 1;
            }
        }
    }

    pub(crate) fn evict(&mut self, model: &str, id: &str) {
        if let Some((_, bytes)) = self.entries.pop(&(model.to_string(), id.to_string())) {
            self.estimated_bytes -= bytes;
            self.counters.cache_evictions += 1;
        }
    }

    pub(crate) fn clear(&mut self) {
        self.entries.clear();
        self.estimated_bytes = 0;
    }

    pub(crate) fn evict_models(&mut self, models: &[String]) {
        let doomed: Vec<(String, String)> = self.entries.iter().filter(|(key, _)| models.contains(&key.0)).map(|(key, _)| key.clone()).collect();
        for key in doomed {
            self.evict(&key.0, &key.1);
        }
    }

    pub(crate) fn counters(&self) -> Counters {
        Counters { cache_estimated_bytes: self.estimated_bytes, ..self.counters.clone() }
    }

    fn count(&mut self, hit: bool) {
        if hit {
            self.counters.cache_hits += 1;
        } else {
            self.counters.cache_misses += 1;
        }
    }
}

impl super::open::Db {
    /// The row from the row cache, or `None` when it is not cached (which does not mean that it does not exist).
    pub fn peek_row(&self, model: &str, id: &crate::identity::RowId) -> Option<Row> {
        let found = super::open::locked(&self.shared.cache).get(model, id.as_str());
        let mut counters = super::open::locked(&self.shared.counters);
        if found.is_some() {
            counters.peek_hits += 1;
        } else {
            counters.peek_misses += 1;
        }
        found
    }
}
