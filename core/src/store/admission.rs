use serde_json::{Map, Value};

use crate::identity::RowId;

use super::StoredRow;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RevisionVerdict {
    Accept,
    DropServerFields,
}

impl RevisionVerdict {
    pub fn of(current_revision: i64, base_revision: i64) -> Self {
        if current_revision <= base_revision {
            RevisionVerdict::Accept
        } else {
            RevisionVerdict::DropServerFields
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowRevision {
    pub row: RowId,
    pub revision: i64,
}

pub fn next_revision(current: i64) -> i64 {
    current + 1
}

pub fn admitted(rows: &[RowRevision], base_revision: i64) -> Vec<RowId> {
    rows.iter()
        .filter(|row| RevisionVerdict::of(row.revision, base_revision) == RevisionVerdict::Accept)
        .map(|row| row.row.clone())
        .collect()
}

pub fn dropped(rows: &[RowRevision], base_revision: i64) -> Vec<RowId> {
    rows.iter()
        .filter(|row| RevisionVerdict::of(row.revision, base_revision) == RevisionVerdict::DropServerFields)
        .map(|row| row.row.clone())
        .collect()
}

pub fn drop_count(rows: &[RowRevision], base_revision: i64) -> usize {
    dropped(rows, base_revision).len()
}

#[derive(Debug, Clone, PartialEq)]
pub enum UpsertAdmission {
    Row { row: Map<String, Value>, dropped_fields: Vec<String> },
    DropExistence,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PatchAdmission {
    Apply { patch: Map<String, Value>, dropped_fields: Vec<String> },
    Skip,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DestroyAdmission {
    Apply,
    Drop,
}

fn newer_than(row: &StoredRow, field: &str, base: i64) -> bool {
    row.field_rev.get(field).is_some_and(|revision| *revision > base)
}

pub fn admit_upsert(
    base: Option<i64>,
    existing: Option<&StoredRow>,
    tombstone_seq: Option<i64>,
    incoming: Map<String, Value>,
) -> UpsertAdmission {
    let Some(base) = base else {
        return UpsertAdmission::Row { row: incoming, dropped_fields: Vec::new() };
    };
    let Some(existing) = existing else {
        if tombstone_seq.is_some_and(|seq| seq > base) {
            return UpsertAdmission::DropExistence;
        }
        return UpsertAdmission::Row { row: incoming, dropped_fields: Vec::new() };
    };
    if existing.born_rev > base {
        return UpsertAdmission::DropExistence;
    }
    let mut row = incoming;
    let mut dropped_fields = Vec::new();
    let keys: Vec<String> = row.keys().cloned().collect();
    for key in keys {
        if !newer_than(existing, &key, base) {
            continue;
        }
        dropped_fields.push(key.clone());
        match existing.row.as_ref().and_then(|stored| stored.get(&key)) {
            Some(value) => {
                row.insert(key, value.clone());
            }
            None => {
                row.remove(&key);
            }
        }
    }
    UpsertAdmission::Row { row, dropped_fields }
}

pub fn admit_patch(base: Option<i64>, existing: Option<&StoredRow>, patch: Map<String, Value>) -> PatchAdmission {
    let Some(existing) = existing else { return PatchAdmission::Skip };
    let Some(base) = base else { return PatchAdmission::Apply { patch, dropped_fields: Vec::new() } };
    let (kept, lost): (Vec<_>, Vec<_>) = patch.into_iter().partition(|(key, _)| !newer_than(existing, key, base));
    if kept.is_empty() {
        return PatchAdmission::Skip;
    }
    PatchAdmission::Apply { patch: kept.into_iter().collect(), dropped_fields: lost.into_iter().map(|(key, _)| key).collect() }
}

pub fn admit_destroy(base: Option<i64>, existing: Option<&StoredRow>) -> DestroyAdmission {
    match (base, existing) {
        (Some(base), Some(row)) if row.revision > base || row.born_rev > base => DestroyAdmission::Drop,
        _ => DestroyAdmission::Apply,
    }
}
