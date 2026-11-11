use super::fixtures::*;
use super::*;

const RETENTION: TombstoneRetention = TombstoneRetention::DEFAULT;

#[test]
fn mark_checks_presence_for_the_same_model_and_id() {
    let (store, _) = booted_store();
    store.mark_tombstone("messages", "1", 1, 100).unwrap();
    assert!(store.is_tombstoned("messages", "1").unwrap());
    assert!(!store.is_tombstoned("messages", "2").unwrap());
    assert!(!store.is_tombstoned("items", "1").unwrap());
}

#[test]
fn repeated_mark_leaves_one_tombstone_row_with_latest_values() {
    let (store, _) = booted_store();
    store.mark_tombstone("messages", "1", 1, 100).unwrap();
    store.mark_tombstone("messages", "1", 2, 200).unwrap();
    assert_eq!(store.tombstone_count().unwrap(), 1);
    assert_eq!(store.read(&[], |reader| reader.tombstone_seq("messages", "1")).unwrap(), Some(2));
}

#[test]
fn caps_decide_eviction() {
    assert_eq!(RETENTION.eviction_count(RETENTION.cap - 1), 0);
    assert_eq!(RETENTION.eviction_count(RETENTION.cap), 0);
    assert_eq!(RETENTION.eviction_count(RETENTION.cap + 3), 3);
    assert!(!RETENTION.ignores_minimum_age(RETENTION.overflow_cap));
    assert!(RETENTION.ignores_minimum_age(RETENTION.overflow_cap + 1));
}

#[test]
fn expired_tombstones_are_deleted() {
    let (store, clock) = booted_store();
    let now = clock.now().as_millis() as i64;
    for name in ["one", "two", "three"] {
        store.mark_tombstone("messages", name, 1, now - RETENTION.ttl_ms - 1).unwrap();
    }
    store.mark_tombstone("messages", "fresh", 1, now - RETENTION.ttl_ms).unwrap();
    assert_eq!(store.evict_tombstones(&RETENTION).unwrap(), 3);
    assert_eq!(store.tombstone_count().unwrap(), 1);
}

#[test]
fn over_cap_evicts_oldest_beyond_minimum_age_only() {
    let (store, clock) = booted_store();
    let now = clock.now().as_millis() as i64;
    let small = TombstoneRetention { ttl_ms: 10_000_000, min_age_ms: 1_000, cap: 3, overflow_cap: 100 };
    for index in 0..5 {
        store.mark_tombstone("messages", &format!("old{index}"), 1, now - 5_000 + index).unwrap();
    }
    store.mark_tombstone("messages", "young", 1, now - 10).unwrap();
    let removed = store.evict_tombstones(&small).unwrap();
    assert_eq!(removed, 3);
    assert!(store.is_tombstoned("messages", "young").unwrap());
    assert!(!store.is_tombstoned("messages", "old0").unwrap());
    assert!(store.is_tombstoned("messages", "old4").unwrap());
}

#[test]
fn over_overflow_cap_ignores_minimum_age() {
    let (store, clock) = booted_store();
    let now = clock.now().as_millis() as i64;
    let small = TombstoneRetention { ttl_ms: 10_000_000, min_age_ms: 1_000_000, cap: 2, overflow_cap: 4 };
    for index in 0..6 {
        store.mark_tombstone("messages", &format!("t{index}"), 1, now - 100 + index).unwrap();
    }
    assert_eq!(store.evict_tombstones(&small).unwrap(), 4);
    assert_eq!(store.tombstone_count().unwrap(), 2);
}

#[test]
fn cap_equal_to_count_evicts_nothing() {
    let (store, clock) = booted_store();
    let now = clock.now().as_millis() as i64;
    let small = TombstoneRetention { ttl_ms: 10_000_000, min_age_ms: 1, cap: 3, overflow_cap: 100 };
    for index in 0..3 {
        store.mark_tombstone("messages", &format!("t{index}"), 1, now - 5_000 + index).unwrap();
    }
    assert_eq!(store.evict_tombstones(&small).unwrap(), 0);
}

#[test]
fn store_landing_keeps_tombstone() {
    use crate::owners::store::{RowValue, StoreOp};
    use serde_json::json;

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    let answer = || vec![StoreOp::Rows { model: "messages".into(), rows: vec![RowValue { row_id: "m1".into(), values: json!({"id": "m1", "chatId": "c1", "body": "back"}).to_string() }] }];
    let point = |store: &Store| store.read(&[], |reader| reader.point("messages", "m1")).unwrap();

    let store = Store::open_models(Some(path.as_path()), vec![messages_spec(), items_spec()], None, std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
    put(&store, "messages", json!({"id": "m1", "chatId": "c1", "body": "hi"}));
    assert!(point(&store).is_some(), "the server row is stored");

    store.land_events_with_pin(&[("messages".into(), json!({"op": "delete", "id": "m1"}).to_string())], None).unwrap();
    assert!(store.is_tombstoned("messages", "m1").unwrap(), "the server removal writes a tombstone");
    assert!(point(&store).is_none(), "the removal clears the row");
    let tombstone = store.read(&[], |reader| reader.tombstone_seq("messages", "m1")).unwrap().unwrap();

    store.write_queue_ops(&answer()).unwrap();
    assert!(point(&store).is_none(), "an operation-answer landing without a base revision keeps the tombstone");

    drop(store);
    let store = Store::open_models(Some(path.as_path()), vec![messages_spec(), items_spec()], None, std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
    assert!(point(&store).is_none(), "the row stays absent after close and open");

    let patch = row(json!({"id": "m1", "chatId": "c1", "body": "back"}));
    assert_eq!(admit_upsert(Some(tombstone - 1), None, Some(tombstone), patch.clone()), UpsertAdmission::DropExistence);
    assert_eq!(admit_upsert(Some(tombstone), None, Some(tombstone), patch.clone()), UpsertAdmission::Row { row: patch, dropped_fields: vec![] });
}
