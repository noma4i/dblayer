use super::fixtures::*;
use super::*;
use rusqlite::types::Value as SqlValue;
use serde_json::json;

use super::rows::column_value;
use crate::ddl::ColumnType;

#[test]
fn revision_reads_and_resets_distinguish_missing_corrupt_and_unreadable_values() {
    for value in [None, Some("17"), Some("bad"), Some("9223372036854775808")] {
        let (store, _) = booted_store();
        {
            let inner = store.lock();
            inner.connection.execute("DELETE FROM manifest WHERE key = ?1", [crate::ddl::REVISION_SEQ_KEY]).unwrap();
            if let Some(value) = value {
                crate::ddl::manifest_write(&inner.connection, crate::ddl::REVISION_SEQ_KEY, value).unwrap();
            }
        }
        match value {
            None | Some("17") => {
                let expected = if value.is_none() { 0 } else { 17 };
                assert_eq!(store.revision_seq().unwrap(), expected);
                store.reset_cache_only(&["messages"]).unwrap();
                assert_eq!(store.revision_seq().unwrap(), expected + 1);
            }
            _ => {
                assert!(store.revision_seq().is_err());
                assert!(store.reset_cache_only(&["messages"]).is_err());
            }
        }
        store.lock().connection.execute_batch("DROP TABLE manifest").unwrap();
        assert!(store.revision_seq().is_err());
    }
}

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
fn keyword_columns_preserve_values_across_insert_update_and_read() {
    let spec = crate::schema::ModelSpec::new("keywords", vec![
        crate::schema::FieldSpec::scalar("id", crate::value::ScalarKind::Id),
        crate::schema::FieldSpec::scalar("default", crate::value::ScalarKind::Str).indexed(),
        crate::schema::FieldSpec::scalar("select", crate::value::ScalarKind::Str),
    ]);
    let store = Store::open_in_memory().unwrap();
    store.boot(&[spec], None).unwrap();
    store.mark_ready();
    for value in ["first", "changed"] {
        store.commit(envelope(&store, vec![upsert("keywords", json!({"id":"k1", "default":value, "select":"chosen"}))], vec![])).unwrap();
        let row = store.point("keywords", "k1").unwrap().unwrap();
        assert_eq!((row["default"].as_str(), row["select"].as_str()), (Some(value), Some("chosen")));
    }
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
    assert_eq!(store.scope_members("messages", thread.key.as_str()).unwrap().len(), 2);
    assert_eq!(store.scope_members("messages", other.key.as_str()).unwrap().len(), 0);
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
