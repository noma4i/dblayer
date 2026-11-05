use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Mutex, Weak};

use crate::identity::{ModelKey, RowRef};

use super::{Operation, OperationStatus};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QueueOperation {
    pub operation: Operation,
    pub uploading: bool,
}

#[derive(Default)]
pub(crate) struct OperationQueue {
    pub entries: BTreeMap<i64, QueueOperation>,
    pub pending: BTreeSet<i64>,
    pub dirty_uploads: BTreeSet<i64>,
    pub retrying: BTreeSet<i64>,
    blocked: BTreeMap<RowRef, BTreeSet<i64>>,
    held: BTreeMap<RowRef, BTreeSet<i64>>,
    attempted: BTreeSet<i64>,
}

impl OperationQueue {
    pub(crate) fn apply(&mut self, seq: i64, entry: Option<QueueOperation>) {
        if let Some(previous) = self.entries.remove(&seq) {
            if previous.uploading { self.dirty_uploads.insert(seq); }
            for row in row_keys(&previous.operation) {
                remove_sequence(&mut self.blocked, &row, seq);
                remove_sequence(&mut self.held, &row, seq);
            }
        }
        self.pending.remove(&seq);
        self.retrying.remove(&seq);
        let Some(entry) = entry else { return };
        let operation = &entry.operation;
        for row in row_keys(operation) {
            if matches!(operation.status, OperationStatus::Failed | OperationStatus::DeliveryUnknown) { self.blocked.entry(row.clone()).or_default().insert(seq); }
            if self.attempted.contains(&seq) { self.held.entry(row).or_default().insert(seq); }
        }
        if operation.status == OperationStatus::Pending && !operation.transport_started {
            if operation.attempts > 0 { self.retrying.insert(seq); }
            if !entry.uploading { self.pending.insert(seq); }
        }
        if entry.uploading { self.dirty_uploads.insert(seq); }
        self.entries.insert(seq, entry);
    }

    pub(crate) fn reset(&mut self, entries: &[QueueOperation]) {
        let removed: BTreeSet<_> = self.entries.iter().filter_map(|(seq, entry)| entry.uploading.then_some(*seq)).collect();
        *self = Self::default();
        self.dirty_uploads = removed;
        for entry in entries { self.apply(entry.operation.id.0, Some(entry.clone())); }
    }

}

fn remove_sequence(rows: &mut BTreeMap<RowRef, BTreeSet<i64>>, row: &RowRef, seq: i64) {
    if let Some(sequences) = rows.get_mut(row) {
        sequences.remove(&seq);
        if sequences.is_empty() { rows.remove(row); }
    }
}

pub(crate) fn row_keys(operation: &Operation) -> Vec<RowRef> {
    let Some((model, _)) = Operation::split_action_key(&operation.action_key) else { return Vec::new() };
    operation.row_ids.iter().chain(&operation.temp_id).map(|id| RowRef::new(ModelKey::new(&model), id.clone())).collect()
}

#[derive(Default)]
pub(crate) struct OperationQueues {
    subscribers: Mutex<Vec<Weak<Mutex<OperationQueue>>>>,
}

impl OperationQueues {

    pub(crate) fn changed(&self, updates: &BTreeMap<i64, Option<QueueOperation>>) {
        self.tell(|queue| {
            for (seq, entry) in updates { queue.apply(*seq, entry.clone()); }
        });
    }

    pub(crate) fn reset(&self, entries: &[QueueOperation]) {
        self.tell(|queue| queue.reset(entries));
    }

    fn tell(&self, edit: impl Fn(&mut OperationQueue)) {
        self.subscribers.lock().unwrap_or_else(|error| error.into_inner()).retain(|subscriber| {
            let Some(queue) = subscriber.upgrade() else { return false };
            edit(&mut queue.lock().unwrap_or_else(|error| error.into_inner()));
            true
        });
    }
}
