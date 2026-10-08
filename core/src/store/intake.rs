use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map, Value};

use crate::schema::ModelSpec;
use crate::value::read_id;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntakeReason {
    MissingId,
    NormalizeRejected,
}

#[derive(Debug, Clone, PartialEq)]
pub enum IntakeOutcome {
    Accepted(Map<String, Value>),
    Quarantined(IntakeReason),
    DroppedByTombstone,
}

pub fn row_intake(payload: &Map<String, Value>, model: &ModelSpec, tombstoned_ids: &BTreeSet<String>) -> IntakeOutcome {
    let Some(id) = payload.get("id").and_then(read_id) else {
        return IntakeOutcome::Quarantined(IntakeReason::MissingId);
    };
    if tombstoned_ids.contains(id.as_str()) {
        return IntakeOutcome::DroppedByTombstone;
    }
    match model.normalize(payload) {
        Ok(normalized) => IntakeOutcome::Accepted(normalized),
        Err(_) => IntakeOutcome::Quarantined(IntakeReason::NormalizeRejected),
    }
}

pub fn accepted_rows(payloads: &[Map<String, Value>], model: &ModelSpec, tombstoned_ids: &BTreeSet<String>) -> Vec<Map<String, Value>> {
    payloads
        .iter()
        .filter_map(|payload| match row_intake(payload, model, tombstoned_ids) {
            IntakeOutcome::Accepted(row) => Some(row),
            _ => None,
        })
        .collect()
}

pub fn tombstone_drop_count(payloads: &[Map<String, Value>], model: &ModelSpec, tombstoned_ids: &BTreeSet<String>) -> usize {
    payloads
        .iter()
        .filter(|payload| matches!(row_intake(payload, model, tombstoned_ids), IntakeOutcome::DroppedByTombstone))
        .count()
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct RowPatch {
    pub values: BTreeMap<String, Value>,
}

impl RowPatch {
    pub fn new(values: BTreeMap<String, Value>) -> Self {
        Self { values }
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn paths(&self) -> Vec<String> {
        self.values.keys().cloned().collect()
    }

    pub fn merging(&self, other: &RowPatch) -> RowPatch {
        let mut values = self.values.clone();
        values.extend(other.values.clone());
        RowPatch { values }
    }

    pub fn merge(patches: &[RowPatch]) -> RowPatch {
        patches.iter().fold(RowPatch::default(), |merged, patch| merged.merging(patch))
    }

    pub fn filtered(&self, allowed: &BTreeSet<String>) -> RowPatch {
        RowPatch { values: self.values.iter().filter(|(path, _)| allowed.contains(*path)).map(|(path, value)| (path.clone(), value.clone())).collect() }
    }

    pub fn changed(&self, current: &BTreeMap<String, Value>) -> RowPatch {
        RowPatch {
            values: self
                .values
                .iter()
                .filter(|(path, value)| current.get(*path) != Some(*value))
                .map(|(path, value)| (path.clone(), value.clone()))
                .collect(),
        }
    }

    pub fn rollback(patched: &RowPatch, previous: &BTreeMap<String, Value>, current: &BTreeMap<String, Value>) -> RowPatch {
        let values = patched
            .values
            .iter()
            .filter(|(path, patched_value)| current.get(*path) == Some(*patched_value))
            .filter_map(|(path, _)| previous.get(path).map(|before| (path.clone(), before.clone())))
            .collect();
        RowPatch { values }
    }
}
