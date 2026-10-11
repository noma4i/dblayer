use super::fixtures::*;
use super::*;

use crate::identity::ScopeKey;
use crate::schema::PageDirection;

fn window(key: &str) -> WindowRecord {
    WindowRecord { key: key.into(), cursor: None, has_next: false, total: None, ids: vec![], issued: 0, applied: 0, generation: 0 }
}

fn key(model_name: &str, relation: &str, param: &str) -> String {
    ScopeKey::compose(&[model_name, relation, param]).as_str().to_string()
}

#[test]
fn save_and_load_return_equal_record() {
    let (store, _) = booted_store();
    let record = WindowRecord {
        key: "messages:thread:1".into(),
        cursor: Some("c1".into()),
        has_next: true,
        total: Some(5),
        ids: vec!["m1".into(), "m2".into()],
        issued: 1,
        applied: 2,
        generation: 3,
    };
    store.save_window(&record).unwrap();
    assert_eq!(store.window(&record.key).unwrap(), Some(record));
}

#[test]
fn load_unknown_key_returns_none() {
    let (store, _) = booted_store();
    assert_eq!(store.window("unknown").unwrap(), None);
}

#[test]
fn repeated_save_replaces_record_and_keeps_one_row() {
    let (store, _) = booted_store();
    let first = WindowRecord { cursor: Some("c1".into()), has_next: true, ids: vec!["m1".into()], issued: 1, ..window("k") };
    let second = WindowRecord { cursor: Some("c2".into()), ids: vec!["m3".into()], issued: 4, applied: 5, generation: 6, ..window("k") };
    store.save_window(&first).unwrap();
    store.save_window(&second).unwrap();
    assert_eq!(store.window("k").unwrap(), Some(second));
    assert_eq!(count(&store, "SELECT COUNT(*) FROM query_window"), 1);
    assert_eq!(count(&store, "SELECT count FROM query_window"), 1);
}

#[test]
fn save_keeps_landed_at_and_invalidation_of_existing_window() {
    let (store, clock) = booted_store();
    let landed = clock.now().as_millis() as i64;
    store.save_window(&window("k")).unwrap();
    store.invalidate_window("k").unwrap();
    clock.advance(std::time::Duration::from_millis(5_000));
    store.save_window(&WindowRecord { ids: vec!["m2".into()], issued: 2, ..window("k") }).unwrap();
    assert_eq!(store.window_freshness("k").unwrap(), Some(WindowFreshness { landed_at: landed, invalidated: true }));
    assert!(store.stale_window_keys().unwrap().contains(&"k".to_string()));
    assert_eq!(store.window("k").unwrap().unwrap().issued, 2);
}

#[test]
fn freshness_needs_landing_inside_window_and_no_invalidation() {
    let fresh = WindowFreshness { landed_at: 100, invalidated: false };
    assert!(fresh.is_fresh(110, 30));
    assert!(!WindowFreshness { invalidated: true, ..fresh }.is_fresh(110, 30));
    assert!(!fresh.is_fresh(140, 30));
}

#[test]
fn mark_exact_marks_only_requested_key() {
    let (store, _) = booted_store();
    let thread = key("messages", "thread", "1");
    let media = key("messages", "media", "1");
    store.save_window(&window(&thread)).unwrap();
    store.save_window(&window(&media)).unwrap();
    store.invalidate_window(&thread).unwrap();
    assert_eq!(store.stale_window_keys().unwrap(), vec![thread]);
}

#[test]
fn mark_family_marks_only_thread_keys() {
    let (store, _) = booted_store();
    let thread = key("messages", "thread", "1");
    let media = key("messages", "media", "1");
    store.save_window(&window(&thread)).unwrap();
    store.save_window(&window(&media)).unwrap();
    assert_eq!(store.invalidate_family("messages", "thread").unwrap(), 1);
    assert_eq!(store.stale_window_keys().unwrap(), vec![thread]);
}

#[test]
fn stale_keys_are_empty_on_clean_database() {
    let (store, _) = booted_store();
    store.save_window(&window(&key("messages", "thread", "1"))).unwrap();
    assert!(store.stale_window_keys().unwrap().is_empty());
}

#[test]
fn window_key_carries_direction() {
    assert_eq!(window_key("inbox", PageDirection::Forward), "inbox:forward");
    assert_eq!(window_key("inbox", PageDirection::Backward), "inbox:backward");
}

#[test]
fn window_ids_decode_filters_non_strings() {
    let (store, _) = booted_store();
    store.save_window(&window("k")).unwrap();
    exec(&store, "UPDATE query_window SET ids = CAST('[\"a\",1]' AS BLOB) WHERE key = 'k'");
    assert_eq!(store.window("k").unwrap().unwrap().ids, vec!["a"]);
}
