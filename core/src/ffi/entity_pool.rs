use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use crate::db::locked;
use super::retention::RetentionPool;
use super::wire::WireRow;

pub(super) struct CachedEntity {
    pub row: Option<WireRow>,
    pub revision: i64,
    pub generation: u64,
    pub epoch: u64,
}

pub(super) struct EntityCache {
    limits: Arc<RetentionPool>,
    rows: Mutex<HashMap<u64, CachedEntity>>,
}

impl EntityCache {
    pub fn new() -> Arc<Self> { Arc::new(Self { limits: RetentionPool::new(), rows: Mutex::new(HashMap::new()) }) }

    pub fn take(&self, model: &str, id: &str, now: u64, generation: u64, epoch: u64) -> Option<CachedEntity> {
        let mut rows = locked(&self.rows);
        let change = self.limits.take(model.into(), id.into(), now);
        for token in change.evicted { rows.remove(&token); }
        change.selected.and_then(|token| rows.remove(&token)).filter(|row| row.generation == generation && row.epoch == epoch)
    }

    pub fn keep(&self, model: &str, id: &str, row: CachedEntity, now: u64) {
        let mut rows = locked(&self.rows);
        let change = self.limits.keep(model.into(), id.into(), row.row.as_ref().map_or(0, |row| row.estimated_bytes), now);
        for token in change.evicted { rows.remove(&token); }
        if let Some(token) = change.selected { rows.insert(token, row); }
    }

    pub fn clear(&self, model: &str) {
        let mut rows = locked(&self.rows);
        for token in self.limits.clear(model.into()).evicted { rows.remove(&token); }
    }

    #[cfg(test)]
    pub fn count(&self, model: &str, now: u64) -> u32 {
        let mut rows = locked(&self.rows);
        let (change, count) = self.limits.count(model, now);
        for token in change.evicted { rows.remove(&token); }
        count
    }
}
