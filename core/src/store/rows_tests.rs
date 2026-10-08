use super::support::*;
use super::*;
use rusqlite::types::Value as SqlValue;
use serde_json::json;

use super::rows::{column_value, encode_row, hydrate};
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
fn encode_rejects_missing_non_string_and_blank_ids() {
    assert!(encode_row(&row(json!({"chatId": "c1"}))).unwrap().is_none());
    assert!(encode_row(&row(json!({"id": 1}))).unwrap().is_none());
    assert!(encode_row(&row(json!({"id": ""}))).unwrap().is_none());
    let (row_id, payload) = encode_row(&row(json!({"id": "m1", "chatId": "c1"}))).unwrap().unwrap();
    assert_eq!(row_id, "m1");
    let (verdict, decoded) = hydrate(1, &payload, "m1");
    assert_eq!(verdict, HydrationVerdict::Accepted);
    assert_eq!(decoded.unwrap().len(), 2);
    assert_eq!(hydrate(1, &payload, "m2").0, HydrationVerdict::IdentityMismatch);
}

#[test]
fn hydration_verdict_follows_version_identity_and_readability() {
    let payload = br#"{"id":"m1"}"#;
    assert_eq!(hydrate(1, payload, "m1").0, HydrationVerdict::Accepted);
    assert_eq!(hydrate(0, payload, "m1").0, HydrationVerdict::StaleVersion);
    assert_eq!(hydrate(1, payload, "m2").0, HydrationVerdict::IdentityMismatch);
    assert_eq!(hydrate(1, b"not json", "m1").0, HydrationVerdict::Unreadable);
    assert!(hydrate(0, payload, "m1").1.is_none());
}

#[test]
fn silent_drop_and_counter_name_follow_verdict() {
    assert!(HydrationVerdict::StaleVersion.is_silent_drop());
    assert!(!HydrationVerdict::IdentityMismatch.is_silent_drop());
    assert!(!HydrationVerdict::Unreadable.is_silent_drop());
    assert!(!HydrationVerdict::Accepted.is_silent_drop());
    assert_eq!(HydrationVerdict::StaleVersion.counter_name(), Some("stale_version_drops"));
    assert_eq!(HydrationVerdict::Unreadable.counter_name(), Some("rejected_records"));
    assert_eq!(HydrationVerdict::Accepted.counter_name(), None);
}

#[test]
fn point_returns_payload_or_none() {
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
fn all_rows_returns_every_row_in_insert_order() {
    let (store, _) = booted_store();
    assert!(store.read(|reader| reader.all_rows("items")).unwrap().is_empty());
    seed(&store, &[("v1", "c"), ("v2", "c")]);
    let rows = store.read(|reader| reader.all_rows("messages")).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["id"], json!("v1"));
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
fn stale_version_row_is_dropped_silently_with_its_membership() {
    let (store, _) = booted_store();
    let thread = scope("messages", "thread");
    store
        .commit(envelope(
            &store,
            vec![upsert("messages", message("m1", "c1")), upsert("messages", message("m2", "c1"))],
            vec![append(&thread, "m1", "a"), append(&thread, "m2", "b")],
        ))
        .unwrap();
    exec(&store, "UPDATE row_messages SET record_version = 0 WHERE id = 'm2'");
    let result = store.scope_rows("messages", thread.key.as_str(), None).unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.stale_version_drops, 1);
    assert_eq!(result.rejected_records, 0);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM row_messages"), 1);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM scope_member"), 1);
    assert_eq!(store.counters().stale_version_drops, 1);
    assert_eq!(store.counters().rejected_records, 0);
}

#[test]
fn corrupt_row_is_removed_alone_and_neighbours_survive() {
    let (store, _) = booted_store();
    seed(&store, &[("m1", "c1"), ("m2", "c1")]);
    exec(&store, "UPDATE row_messages SET payload = x'7b' WHERE id = 'm2'");
    assert!(store.point("messages", "m2").unwrap().is_none());
    assert_eq!(count(&store, "SELECT COUNT(*) FROM row_messages"), 1);
    assert!(store.point("messages", "m1").unwrap().is_some());
    assert_eq!(store.counters().rejected_records, 1);
}

#[test]
fn payload_identity_must_equal_row_key() {
    let (store, _) = booted_store();
    seed(&store, &[("m1", "c1")]);
    exec(&store, "UPDATE row_messages SET payload = CAST('{\"id\":\"other\"}' AS BLOB) WHERE id = 'm1'");
    assert!(store.point("messages", "m1").unwrap().is_none());
    assert_eq!(count(&store, "SELECT COUNT(*) FROM row_messages"), 0);
}

#[test]
fn row_count_reports_table_size() {
    let (store, _) = booted_store();
    seed(&store, &[("m1", "c1"), ("m2", "c1")]);
    assert_eq!(store.row_count("messages").unwrap(), 2);
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
