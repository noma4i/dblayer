use super::error::SyncError;
use super::normalize::*;
use serde_json::{Map, Value};
use crate::sync::support::{rig, Rig};
use serde_json::json;

fn nodes(value: Value) -> Vec<Map<String, Value>> {
    value.as_array().unwrap().iter().map(|node| node.as_object().unwrap().clone()).collect()
}

#[test]
fn nested_author_becomes_user_row_and_sets_foreign_key() {
    let Rig { db, .. } = rig();
    let input = nodes(json!([{"id": "m1", "text": "hi", "author": {"id": "u1", "name": "Ann", "ignored": 1}}]));
    let normalized = normalize_nodes(&db, "moments", &input).unwrap();
    assert_eq!(normalized.roots.len(), 1);
    assert_eq!(normalized.roots[0]["userId"], json!("u1"));
    assert_eq!(normalized.related.len(), 1);
    assert_eq!(normalized.related[0].0.as_str(), "users");
    assert_eq!(normalized.related[0].1["name"], json!("Ann"));
    assert!(!normalized.roots[0].contains_key("author"));
}

#[test]
fn shared_author_is_reported_once() {
    let Rig { db, .. } = rig();
    let input = nodes(json!([
        {"id": "m1", "author": {"id": "u1", "name": "Ann"}},
        {"id": "m2", "author": {"id": "u1", "name": "Ann B"}}
    ]));
    let normalized = normalize_nodes(&db, "moments", &input).unwrap();
    assert_eq!(normalized.roots.len(), 2);
    assert_eq!(normalized.related.len(), 1);
    assert_eq!(normalized.related[0].1["name"], json!("Ann B"));
}

#[test]
fn row_without_id_is_dropped_and_counted() {
    let Rig { db, .. } = rig();
    let input = nodes(json!([{"text": "no id"}, {"id": "m2", "text": "ok"}]));
    let normalized = normalize_nodes(&db, "moments", &input).unwrap();
    assert_eq!(normalized.roots.len(), 1);
    assert_eq!(normalized.dropped, 1);
}

#[test]
fn invalid_author_is_skipped_but_moment_stays() {
    let Rig { db, .. } = rig();
    let input = nodes(json!([{"id": "m1", "userId": "u9", "author": {"name": "no id"}}]));
    let normalized = normalize_nodes(&db, "moments", &input).unwrap();
    assert_eq!(normalized.roots[0]["userId"], json!("u9"));
    assert!(normalized.related.is_empty());
}

#[test]
fn unknown_model_is_an_error() {
    let Rig { db, .. } = rig();
    assert_eq!(normalize_nodes(&db, "ghosts", &[]), Err(SyncError::UnknownModel("ghosts".to_string())));
}
