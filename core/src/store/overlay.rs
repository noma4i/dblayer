use std::collections::VecDeque;
use std::sync::Arc;

use arc_swap::ArcSwap;
use im::HashMap;
use serde_json::Value;

const VERSIONS: usize = 64;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub(super) struct RowKey {
    pub model: String,
    pub row_id: String,
}

#[derive(Clone, Default)]
pub(super) struct OverlaySnapshot {
    pub revision: i64,
    pub position: u64,
    pub next_token: u64,
    pub values: HashMap<RowKey, HashMap<String, OverlayValue>>,
}

#[derive(Clone)]
pub(super) struct OverlayValue {
    pub value: Value,
    pub revision: i64,
    pub position: u64,
}

impl OverlaySnapshot {
    pub fn set(&mut self, row: RowKey, field: String, value: Value) -> u64 {
        self.next_token += 1;
        let mut fields = self.values.get(&row).cloned().unwrap_or_default();
        fields.insert(field, OverlayValue { value, revision: self.revision, position: self.next_token });
        self.values.insert(row, fields);
        self.next_token
    }

    pub fn remove_row(&mut self, row: &RowKey) {
        self.values.remove(row);
    }

    pub fn expire(&mut self, row: &RowKey, field: &str, position: u64) -> bool {
        let Some(mut fields) = self.values.get(row).cloned() else { return false };
        if !fields.get(field).is_some_and(|slot| slot.position == position) { return false; }
        fields.remove(field);
        if fields.is_empty() { self.values.remove(row); } else { self.values.insert(row.clone(), fields); }
        true
    }

    /// Drops the values of the rows `present` rejects once more than `limit` rows hold values: a row that never arrives keeps no memory.
    pub fn prune_missing(&mut self, limit: usize, present: impl Fn(&RowKey) -> bool) {
        if self.values.len() <= limit { return; }
        let missing: Vec<RowKey> = self.values.keys().filter(|row| !present(row)).cloned().collect();
        for row in missing { self.values.remove(&row); }
    }

    pub fn clear_models(&mut self, models: Option<&[String]>) {
        self.values.retain(|row, _| models.is_some_and(|models| !models.contains(&row.model)));
    }
}

pub(super) struct Overlays {
    versions: ArcSwap<VecDeque<Arc<OverlaySnapshot>>>,
}

impl Overlays {
    pub fn new(revision: i64) -> Self {
        let snapshot = Arc::new(OverlaySnapshot { revision, ..OverlaySnapshot::default() });
        Self { versions: ArcSwap::from_pointee(VecDeque::from([snapshot])) }
    }

    pub fn reset(&self, revision: i64) {
        let snapshot = Arc::new(OverlaySnapshot { revision, ..OverlaySnapshot::default() });
        self.versions.store(Arc::new(VecDeque::from([snapshot])));
    }

    pub fn get(&self, revision: i64) -> Option<Arc<OverlaySnapshot>> {
        self.versions.load().iter().rev().find(|snapshot| snapshot.revision == revision).cloned()
    }

    pub fn next(&self, revision: i64, writes_database: bool) -> Option<OverlaySnapshot> {
        let current = self.get(revision)?;
        let mut next = current.as_ref().clone();
        if writes_database {
            next.revision = revision.checked_add(1)?;
            next.position = 0;
        } else {
            next.position = next.position.checked_add(1)?;
        }
        Some(next)
    }

    pub fn publish(&self, snapshot: OverlaySnapshot) {
        let mut versions = self.versions.load_full().as_ref().clone();
        versions.push_back(Arc::new(snapshot));
        while versions.len() > VERSIONS { versions.pop_front(); }
        self.versions.store(Arc::new(versions));
    }
}
