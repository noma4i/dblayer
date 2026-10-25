use std::collections::HashMap;
use std::sync::{Arc, Weak};

use serde_json::{Map, Value};

use super::open::{locked, Db, Shared};

#[derive(Clone)]
pub(crate) struct ObservedRead {
    pub model: String,
    pub relation: String,
    pub params: Map<String, Value>,
    pub key: String,
}

#[derive(Default)]
pub(crate) struct ObservedReads {
    next: u64,
    entries: HashMap<u64, ObservedRead>,
}

pub(crate) struct ObservedReadLease {
    shared: Weak<Shared>,
    id: u64,
}

impl Drop for ObservedReadLease {
    fn drop(&mut self) {
        if let Some(shared) = self.shared.upgrade() { locked(&shared.observed_reads).entries.remove(&self.id); }
    }
}

impl Db {
    pub(crate) fn register_observed_read(&self, read: ObservedRead) -> ObservedReadLease {
        let mut reads = locked(&self.shared.observed_reads);
        reads.next += 1;
        let id = reads.next;
        reads.entries.insert(id, read);
        ObservedReadLease { shared: Arc::downgrade(&self.shared), id }
    }

    pub(crate) fn observed_reads(&self) -> Vec<ObservedRead> {
        locked(&self.shared.observed_reads).entries.values().cloned().collect()
    }
}
