use super::fixtures::*;
use super::*;
use rusqlite::types::Value as SqlValue;
use serde_json::json;

use super::rows::column_value;
use crate::ddl::ColumnType;

fn seed(store: &Store, ids: &[(&str, &str)]) {
    let ops = ids.iter().map(|(row_id, chat)| upsert("messages", message(row_id, chat))).collect();
    store.commit(envelope(store, ops, vec![])).unwrap();
}

#[test]
fn column_value_maps_scalars_and_absent_cases() {
    assert_eq!(column_value(ColumnType::Text, Some(&json!("a"))), SqlValue::Text("a".into()));
    assert_eq!(column_value(ColumnType::Integer, Some(&json!(true))), SqlValue::Integer(1));
    assert_eq!(column_value(ColumnType::Integer, Some(&json!(false))), SqlValue::Integer(0));
    assert_eq!(column_value(ColumnType::Integer, Some(&json!(5))), SqlValue::Integer(5));
    assert_eq!(column_value(ColumnType::Integer, Some(&json!(5.0))), SqlValue::Integer(5));
    assert_eq!(column_value(ColumnType::Integer, Some(&json!(1.5))), SqlValue::Null);
    assert_eq!(column_value(ColumnType::Real, Some(&json!(1.5))), SqlValue::Real(1.5));
    assert_eq!(column_value(ColumnType::Text, Some(&json!(null))), SqlValue::Null);
    assert_eq!(column_value(ColumnType::Text, Some(&json!([]))), SqlValue::Null);
    assert_eq!(column_value(ColumnType::Text, None), SqlValue::Null);
    assert_eq!(column_value(ColumnType::Integer, Some(&json!("x"))), SqlValue::Null);
}

#[test]
fn point_returns_the_row_or_none() {
    let (store, _) = booted_store();
    seed(&store, &[("m1", "c1")]);
    assert_eq!(store.point("messages", "m1").unwrap().unwrap()["chatId"], json!("c1"));
    assert!(store.point("messages", "m3").unwrap().is_none());
}

#[test]
fn rows_by_ids_restores_requested_order_and_skips_missing() {
    let (store, _) = booted_store();
    seed(&store, &[("v1", "c"), ("v2", "c")]);
    let ordered = store.rows_by_ids("messages", &[id("v2"), id("missing"), id("v1")]).unwrap();
    let ids: Vec<&str> = ordered.iter().map(|row| row["id"].as_str().unwrap()).collect();
    assert_eq!(ids, vec!["v2", "v1"]);
    assert!(store.rows_by_ids("messages", &[]).unwrap().is_empty());
}

#[test]
fn scope_rows_follow_membership_limit_and_count() {
    let (store, _) = booted_store();
    let thread = scope("messages", "thread");
    let other = scope("messages", "other");
    store
        .commit(envelope(
            &store,
            vec![upsert("messages", message("m1", "c1")), upsert("messages", message("m2", "c2"))],
            vec![append(&thread, "m1", "0001"), append(&thread, "m2", "0002")],
        ))
        .unwrap();
    let all = store.scope_rows("messages", thread.key.as_str(), None).unwrap();
    let limited = store.scope_rows("messages", thread.key.as_str(), Some(1)).unwrap();
    assert_eq!(all.rows.len(), 2);
    assert_eq!(limited.rows.len(), 1);
    assert_eq!(store.count_members("messages", thread.key.as_str()).unwrap(), 2);
    assert_eq!(store.count_members("messages", other.key.as_str()).unwrap(), 0);
    let members = store.scope_members("messages", thread.key.as_str()).unwrap();
    assert_eq!(members, vec![("m1".to_string(), "0001".to_string()), ("m2".to_string(), "0002".to_string())]);
}

#[test]
fn dangling_membership_is_skipped_and_counted() {
    let (store, _) = booted_store();
    let thread = scope("messages", "thread");
    store
        .commit(envelope(&store, vec![upsert("messages", message("m1", "c1"))], vec![append(&thread, "m1", "a"), append(&thread, "ghost", "b")]))
        .unwrap();
    let result = store.scope_rows("messages", thread.key.as_str(), None).unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.dangling_members, 1);
    assert_eq!(store.counters().membership_missing_entity, 1);
}

#[test]
fn tombstones_read_returns_only_marked_ids() {
    let (store, _) = booted_store();
    store.mark_tombstone("messages", "1", 1, 100).unwrap();
    store.mark_tombstone("messages", "2", 2, 200).unwrap();
    let marked = store.tombstoned_ids("messages", &["1".into(), "2".into(), "3".into()]).unwrap();
    assert_eq!(marked.into_iter().collect::<Vec<_>>(), vec!["1", "2"]);
    assert!(store.tombstoned_ids("messages", &[]).unwrap().is_empty());
}

#[test]
fn read_unknown_model_fails_with_unknown_model() {
    let (store, _) = booted_store();
    assert!(matches!(store.point("nope", "x"), Err(StoreError::UnknownModel(_))));
}
