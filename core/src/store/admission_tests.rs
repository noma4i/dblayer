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
fn verdict_accepts_unless_current_is_newer_than_base() {
    assert_eq!(RevisionVerdict::of(5, 5), RevisionVerdict::Accept);
    assert_eq!(RevisionVerdict::of(4, 5), RevisionVerdict::Accept);
    assert_eq!(RevisionVerdict::of(6, 5), RevisionVerdict::DropServerFields);
    assert_eq!(next_revision(0), 1);
    assert_eq!(next_revision(41), 42);
    assert_eq!(RevisionVerdict::of(next_revision(5), 5), RevisionVerdict::DropServerFields);
}

#[test]
fn admitted_and_dropped_split_by_revision() {
    let rows: Vec<RowRevision> = [("1", 4), ("2", 5), ("3", 6)].iter().map(|(id, revision)| RowRevision { row: row_id(id), revision: *revision }).collect();
    assert_eq!(admitted(&rows, 5).len(), 2);
    assert_eq!(dropped(&rows, 5), vec![row_id("3")]);
    assert_eq!(drop_count(&rows, 5), 1);
    assert!(admitted(&[], 5).is_empty());
    assert_eq!(drop_count(&[], 5), 0);
    let below = vec![RowRevision { row: row_id("1"), revision: 1 }];
    assert!(dropped(&below, 5).is_empty());
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

#[test]
fn patch_admission_drops_newer_fields_and_skips_when_nothing_is_left() {
    let existing = stored(1, 7, &[("a", 1), ("b", 7)], json!({"id": "m1", "a": 1, "b": 2}));
    assert_eq!(admit_patch(Some(5), None, map(json!({"a": 1}))), PatchAdmission::Skip);
    assert_eq!(
        admit_patch(Some(5), Some(&existing), map(json!({"a": 2, "b": 3}))),
        PatchAdmission::Apply { patch: map(json!({"a": 2})), dropped_fields: vec!["b".into()] }
    );
    assert_eq!(admit_patch(Some(5), Some(&existing), map(json!({"b": 3}))), PatchAdmission::Skip);
    assert_eq!(
        admit_patch(None, Some(&existing), map(json!({"b": 3}))),
        PatchAdmission::Apply { patch: map(json!({"b": 3})), dropped_fields: vec![] }
    );
}

#[test]
fn destroy_admission_needs_row_not_touched_after_base() {
    let touched = stored(1, 7, &[], json!({"id": "m1"}));
    let untouched = stored(1, 5, &[], json!({"id": "m1"}));
    let born_later = stored(6, 6, &[], json!({"id": "m1"}));
    assert_eq!(admit_destroy(Some(5), Some(&touched)), DestroyAdmission::Drop);
    assert_eq!(admit_destroy(Some(5), Some(&untouched)), DestroyAdmission::Apply);
    assert_eq!(admit_destroy(Some(5), Some(&born_later)), DestroyAdmission::Drop);
    assert_eq!(admit_destroy(Some(5), None), DestroyAdmission::Apply);
    assert_eq!(admit_destroy(None, Some(&touched)), DestroyAdmission::Apply);
}
