use std::collections::HashSet;

use super::status::{is_open, recovered_status, Operation, OperationStatus};
use crate::identity::{ModelKey, RowId, RowRef};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportVerdict {
    Send,
    SkipUnknownDelivery,
    SkipClosed,
    JoinInFlight,
    BlockedByHead,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutboxEntry {
    pub sequence: i64,
    pub status: OperationStatus,
    pub transport_started: bool,
}

impl OutboxEntry {
    pub fn new(sequence: i64, status: OperationStatus, transport_started: bool) -> Self {
        Self { sequence, status, transport_started }
    }
}

pub fn transport_verdict(entry: Option<&OutboxEntry>, has_in_flight: bool, head_blocked: bool) -> TransportVerdict {
    let Some(entry) = entry else {
        return TransportVerdict::SkipClosed;
    };
    if has_in_flight {
        return TransportVerdict::JoinInFlight;
    }
    if entry.status == OperationStatus::DeliveryUnknown && entry.transport_started {
        return TransportVerdict::SkipUnknownDelivery;
    }
    if head_blocked {
        return TransportVerdict::BlockedByHead;
    }
    TransportVerdict::Send
}

pub fn blocks_followers(entry: &OutboxEntry) -> bool {
    matches!(recovered_status(entry.transport_started, entry.status), OperationStatus::DeliveryUnknown | OperationStatus::Failed)
}

pub fn ready(entries: &[OutboxEntry]) -> Vec<i64> {
    let mut sequences: Vec<i64> = entries
        .iter()
        .filter(|entry| entry.status == OperationStatus::Pending && !entry.transport_started)
        .map(|entry| entry.sequence)
        .collect();
    sequences.sort_unstable();
    sequences
}

#[derive(Debug, Clone)]
pub struct OutboxQueue {
    operations: Vec<Operation>,
}

impl OutboxQueue {
    pub fn new(operations: Vec<Operation>) -> Self {
        Self { operations: operations.into_iter().filter(|operation| is_open(operation.status)).collect() }
    }

    fn of_model<'a, 'm>(&'a self, model: &'m str) -> impl Iterator<Item = &'a Operation> + use<'a, 'm> {
        self.operations
            .iter()
            .filter(move |operation| Operation::split_action_key(&operation.action_key).is_some_and(|(owner, _)| owner == model))
    }

    pub fn open_row_ids(&self, model: &ModelKey) -> HashSet<RowId> {
        let mut ids = HashSet::new();
        for operation in self.of_model(model.as_str()) {
            ids.extend(operation.row_ids.iter().cloned());
            ids.extend(operation.temp_id.iter().cloned());
        }
        ids
    }

    pub fn is_held_before(&self, row: &RowRef, sequence: i64, held: &HashSet<i64>) -> bool {
        self.of_model(row.model.as_str())
            .filter(|operation| operation.row_ids.contains(&row.id) || operation.temp_id.as_ref() == Some(&row.id))
            .any(|operation| operation.id.0 < sequence && held.contains(&operation.id.0))
    }

    pub fn is_blocked_before(&self, row: &RowRef, sequence: i64) -> bool {
        self.of_model(row.model.as_str())
            .filter(|operation| operation.row_ids.contains(&row.id) || operation.temp_id.as_ref() == Some(&row.id))
            .filter(|operation| operation.id.0 < sequence)
            .any(|operation| blocks_followers(&OutboxEntry::new(operation.id.0, operation.status, operation.transport_started)))
    }
}
