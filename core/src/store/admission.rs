use serde_json::{Map, Value};

use super::StoredRow;

#[derive(Debug, Clone, PartialEq)]
pub enum UpsertAdmission {
    Row { row: Map<String, Value>, dropped_fields: Vec<String> },
    DropExistence,
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
    if existing.is_none() && tombstone_seq.is_some_and(|seq| base.is_none_or(|base| seq > base)) {
        return UpsertAdmission::DropExistence;
    }
    let Some(base) = base else {
        return UpsertAdmission::Row { row: incoming, dropped_fields: Vec::new() };
    };
    let Some(existing) = existing else {
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
        match existing.row.get(&key) {
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
