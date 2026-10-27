use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

const MAX_BYTES: u64 = 8 * 1024 * 1024;
const MAX_COUNT: usize = 200;
const MAX_AGE: u64 = 120_000;

pub struct RetentionChange {
    pub selected: Option<u64>,
    pub evicted: Vec<u64>,
    #[cfg(test)]
    pub count: u32,
}

struct RetainedRow {
    model: String,
    id: String,
    bytes: u64,
    expires: u64,
}

#[derive(Default)]
struct RetainedRows {
    rows: BTreeMap<u64, RetainedRow>,
    keys: HashMap<(String, String), u64>,
    models: HashMap<String, BTreeSet<u64>>,
    bytes: u64,
    next: u64,
}

impl RetainedRows {
    fn remove(&mut self, token: u64, evicted: &mut Vec<u64>) {
        let Some(row) = self.rows.remove(&token) else { return };
        self.bytes -= row.bytes;
        self.keys.remove(&(row.model.clone(), row.id));
        if let Some(tokens) = self.models.get_mut(&row.model) {
            tokens.remove(&token);
            if tokens.is_empty() { self.models.remove(&row.model); }
        }
        evicted.push(token);
    }

    fn expire(&mut self, now: u64, evicted: &mut Vec<u64>) {
        while let Some((&token, row)) = self.rows.first_key_value() {
            if row.expires > now { break; }
            self.remove(token, evicted);
        }
    }

    fn change(&self, model: &str, selected: Option<u64>, evicted: Vec<u64>) -> RetentionChange {
        RetentionChange { selected, evicted, #[cfg(test)] count: self.models.get(model).map_or(0, |rows| rows.len() as u32) }
    }
}

pub struct RetentionPool {
    rows: Mutex<RetainedRows>,
}

impl RetentionPool {
    pub fn new() -> Arc<Self> {
        Arc::new(Self { rows: Mutex::new(RetainedRows::default()) })
    }

    pub fn keep(&self, model: String, id: String, bytes: u64, now: u64) -> RetentionChange {
        let bytes = bytes.saturating_add((std::mem::size_of::<RetainedRow>() + 8 * std::mem::size_of::<usize>() + 2 * (model.len() + id.len())) as u64);
        let mut rows = crate::db::locked(&self.rows);
        let mut evicted = Vec::new();
        rows.expire(now, &mut evicted);
        if let Some(&token) = rows.keys.get(&(model.clone(), id.clone())) {
            rows.remove(token, &mut evicted);
        }
        if bytes > MAX_BYTES { return rows.change(&model, None, evicted); }
        rows.next += 1;
        let token = rows.next;
        rows.bytes += bytes;
        rows.keys.insert((model.clone(), id.clone()), token);
        rows.models.entry(model.clone()).or_default().insert(token);
        rows.rows.insert(token, RetainedRow { model: model.clone(), id, bytes, expires: now.saturating_add(MAX_AGE) });
        while let Some(oldest) = rows.models.get(&model).filter(|tokens| tokens.len() > MAX_COUNT).and_then(|tokens| tokens.first()).copied() {
            rows.remove(oldest, &mut evicted);
        }
        while let Some((&oldest, _)) = rows.rows.first_key_value() {
            if rows.bytes <= MAX_BYTES { break; }
            rows.remove(oldest, &mut evicted);
        }
        rows.change(&model, Some(token), evicted)
    }

    pub fn take(&self, model: String, id: String, now: u64) -> RetentionChange {
        let mut rows = crate::db::locked(&self.rows);
        let mut evicted = Vec::new();
        rows.expire(now, &mut evicted);
        let selected = rows.keys.get(&(model.clone(), id)).copied();
        if let Some(token) = selected {
            rows.remove(token, &mut Vec::new());
        }
        rows.change(&model, selected, evicted)
    }

    #[cfg(test)]
    pub fn count(&self, model: String, now: u64) -> RetentionChange {
        let mut rows = crate::db::locked(&self.rows);
        let mut evicted = Vec::new();
        rows.expire(now, &mut evicted);
        rows.change(&model, None, evicted)
    }

    pub fn clear(&self, model: String) -> RetentionChange {
        let mut rows = crate::db::locked(&self.rows);
        let tokens: Vec<_> = rows.models.get(&model).into_iter().flatten().copied().collect();
        let mut evicted = Vec::new();
        for token in tokens { rows.remove(token, &mut evicted); }
        rows.change(&model, None, evicted)
    }
}
