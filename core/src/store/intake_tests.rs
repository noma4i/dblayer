use super::support::*;
use super::*;
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};

use crate::schema::{FieldSpec, ModelSpec};
use crate::value::ScalarKind;

fn spec() -> ModelSpec {
    ModelSpec::new(
        "messages",
        vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("body", ScalarKind::Str).optional()],
    )
}

fn strict_spec() -> ModelSpec {
    ModelSpec::new(
        "messages",
        vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("body", ScalarKind::Str)],
    )
}

fn none() -> BTreeSet<String> {
    BTreeSet::new()
}

#[test]
fn intake_accepts_quarantines_and_drops_by_tombstone() {
    let payload = row(json!({"id": "m1", "body": "hi"}));
    assert_eq!(row_intake(&payload, &spec(), &none()), IntakeOutcome::Accepted(payload.clone()));
    assert_eq!(
        row_intake(&row(json!({"body": "hi"})), &spec(), &none()),
        IntakeOutcome::Quarantined(IntakeReason::MissingId)
    );
    assert_eq!(
        row_intake(&row(json!({"id": ""})), &spec(), &none()),
        IntakeOutcome::Quarantined(IntakeReason::MissingId)
    );
    assert_eq!(
        row_intake(&row(json!({"id": "m1"})), &spec(), &["m1".to_string()].into()),
        IntakeOutcome::DroppedByTombstone
    );
    assert_eq!(
        row_intake(&row(json!({"id": "m2"})), &strict_spec(), &none()),
        IntakeOutcome::Quarantined(IntakeReason::NormalizeRejected)
    );
}

#[test]
fn accepted_rows_and_drop_count_classify_payloads() {
    let payloads = vec![row(json!({"id": "m1", "body": "hi"})), row(json!({"id": "m2"})), row(json!({"id": "m3"}))];
    let tombstoned: BTreeSet<String> = ["m2".to_string()].into();
    let accepted = accepted_rows(&payloads, &spec(), &tombstoned);
    assert_eq!(accepted.len(), 2);
    assert_eq!(accepted[0]["id"], json!("m1"));
    assert_eq!(tombstone_drop_count(&payloads, &spec(), &tombstoned), 1);
}

fn patch(pairs: &[(&str, &str)]) -> RowPatch {
    RowPatch::new(pairs.iter().map(|(path, value)| (path.to_string(), json!(value))).collect())
}

fn values(pairs: &[(&str, &str)]) -> BTreeMap<String, serde_json::Value> {
    pairs.iter().map(|(path, value)| (path.to_string(), json!(value))).collect()
}

#[test]
fn patch_paths_sort_and_filter_and_changed_keep_only_differences() {
    assert!(RowPatch::default().is_empty());
    assert!(RowPatch::default().paths().is_empty());
    assert_eq!(patch(&[("zeta", "z"), ("alpha", "a"), ("middle", "m")]).paths(), vec!["alpha", "middle", "zeta"]);
    let allowed: BTreeSet<String> = ["allowed".to_string(), "missing".to_string()].into();
    assert_eq!(patch(&[("allowed", "allowed"), ("forbidden", "forbidden")]).filtered(&allowed).values, values(&[("allowed", "allowed")]));
    let current = values(&[("allowed", "same"), ("forbidden", "old")]);
    assert_eq!(
        patch(&[("allowed", "same"), ("forbidden", "new"), ("missing", "new")]).changed(&current).values,
        values(&[("forbidden", "new"), ("missing", "new")])
    );
}

#[test]
fn merging_prefers_later_value_and_folds_left_to_right() {
    let first = patch(&[("title", "first"), ("body", "body")]);
    let second = patch(&[("title", "second"), ("count", "count")]);
    let third = patch(&[("title", "third")]);
    assert_eq!(first.merging(&second).values, values(&[("title", "second"), ("body", "body"), ("count", "count")]));
    assert_eq!(RowPatch::merge(&[first, second, third]).values["title"], json!("third"));
    assert!(RowPatch::merge(&[]).is_empty());
}

#[test]
fn rollback_skips_foreign_changes() {
    let patched = patch(&[("restored", "patched"), ("foreign", "patched"), ("missing", "patched")]);
    let previous = values(&[("restored", "before"), ("foreign", "before")]);
    let current = values(&[("restored", "patched"), ("foreign", "foreign"), ("missing", "patched")]);
    assert_eq!(RowPatch::rollback(&patched, &previous, &current).values, values(&[("restored", "before")]));
}
