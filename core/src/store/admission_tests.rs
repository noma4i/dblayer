use super::*;
use crate::identity::RowId;
use crate::store::rows::HydrationVerdict;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

fn row_id(text: &str) -> RowId {
    RowId::new(text).unwrap()
}

fn map(value: Value) -> Map<String, Value> {
    value.as_object().unwrap().clone()
}

fn stored(born: i64, revision: i64, fields: &[(&str, i64)], row: Value) -> StoredRow {
    StoredRow {
        id: row_id("m1"),
        revision,
        born_rev: born,
        field_rev: fields.iter().map(|(field, rev)| (field.to_string(), *rev)).collect::<BTreeMap<_, _>>(),
        verdict: HydrationVerdict::Accepted,
        row: Some(map(row)),
    }
}

#[test]
fn local_writes_without_base_pass_unchanged() {
    let existing = stored(1, 9, &[("body", 9)], json!({"id": "m1", "body": "local"}));
    let incoming = map(json!({"id": "m1", "body": "server"}));
    assert_eq!(
        admit_upsert(None, Some(&existing), None, incoming.clone()),
        UpsertAdmission::Row { row: incoming, dropped_fields: vec![] }
    );
}

#[test]
fn insert_or_delete_after_base_drops_the_whole_row() {
    let incoming = map(json!({"id": "m1"}));
    assert_eq!(admit_upsert(Some(5), None, Some(6), incoming.clone()), UpsertAdmission::DropExistence);
    assert_eq!(admit_upsert(Some(5), None, Some(5), incoming.clone()), UpsertAdmission::Row { row: incoming.clone(), dropped_fields: vec![] });
    let born_later = stored(6, 6, &[], json!({"id": "m1"}));
    assert_eq!(admit_upsert(Some(5), Some(&born_later), None, incoming), UpsertAdmission::DropExistence);
}

#[test]
fn field_level_admission_keeps_untouched_server_fields() {
    let existing = stored(1, 7, &[("id", 1), ("a", 1), ("b", 7)], json!({"id": "m1", "a": "old", "b": "local"}));
    let incoming = map(json!({"id": "m1", "a": "server", "b": "server"}));
    let UpsertAdmission::Row { row, dropped_fields } = admit_upsert(Some(5), Some(&existing), None, incoming) else {
        panic!("row expected")
    };
    assert_eq!(row["a"], json!("server"));
    assert_eq!(row["b"], json!("local"));
    assert_eq!(dropped_fields, vec!["b"]);
}

#[test]
fn dropped_field_absent_before_is_removed_from_incoming() {
    let existing = stored(1, 7, &[("id", 1), ("b", 7)], json!({"id": "m1"}));
    let UpsertAdmission::Row { row, .. } = admit_upsert(Some(5), Some(&existing), None, map(json!({"id": "m1", "b": "server"}))) else {
        panic!("row expected")
    };
    assert!(row.get("b").is_none());
}
