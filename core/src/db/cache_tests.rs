use super::cache::RowCache;
use super::row::Record;
use super::*;
use crate::identity::RowId;
use serde_json::{json, Map};
use std::sync::Arc;
use crate::test_wait::join_within;

fn record(id: &str, revision: i64) -> Row {
    let mut data = Map::new();
    data.insert("id".into(), json!(id));
    Arc::new(Record { id: RowId::new(id).unwrap(), revision, data, sync: Default::default(), ephemeral: 0 })
}

#[test]
fn get_counts_hits_and_misses() {
    let mut cache = RowCache::new(4);
    assert!(cache.get("users", "u1").is_none());
    cache.put("users", record("u1", 1), 1);
    assert!(cache.get("users", "u1").is_some());
    let counters = cache.counters();
    assert_eq!((counters.cache_hits, counters.cache_misses, counters.cache_evictions), (1, 1, 0));
}

#[test]
fn fresh_requires_the_same_revision() {
    let mut cache = RowCache::new(4);
    cache.put("users", record("u1", 3), 3);
    assert!(cache.fresh("users", "u1", 3).is_some());
    assert!(cache.fresh("users", "u1", 4).is_none());
    let counters = cache.counters();
    assert_eq!((counters.cache_hits, counters.cache_misses, counters.cache_evictions), (1, 1, 0));
}

#[test]
fn capacity_evicts_the_least_recently_used_row() {
    let mut cache = RowCache::new(2);
    cache.put("users", record("u1", 1), 1);
    cache.put("users", record("u2", 1), 1);
    cache.get("users", "u1");
    cache.put("users", record("u3", 1), 1);
    assert!(cache.get("users", "u2").is_none());
    assert!(cache.get("users", "u1").is_some());
    assert_eq!(cache.counters().cache_evictions, 1);
}

#[test]
fn replacing_a_row_is_not_an_eviction() {
    let mut cache = RowCache::new(2);
    cache.put("users", record("u1", 1), 1);
    cache.put("users", record("u1", 2), 2);
    assert_eq!(cache.counters().cache_evictions, 0);
    assert_eq!(cache.get("users", "u1").unwrap().revision, 2);
}

#[test]
fn large_records_obey_the_byte_budget_and_oversized_records_are_not_retained() {
    let mut cache = RowCache::new(4096);
    let large = |id: &str, bytes: usize| {
        let mut row = record(id, 1);
        Arc::make_mut(&mut row).data.insert("body".into(), json!("x".repeat(bytes)));
        row
    };
    cache.put("notes", large("first", 9 * 1024 * 1024), 1);
    cache.put("notes", large("second", 9 * 1024 * 1024), 1);
    assert!(cache.get("notes", "first").is_none());
    assert!(cache.get("notes", "second").is_some());
    cache.put("notes", large("oversized", 17 * 1024 * 1024), 1);
    assert!(cache.get("notes", "oversized").is_none());
    assert!(cache.get("notes", "second").is_some());
}

#[test]
fn evict_removes_one_row_and_counts_it() {
    let mut cache = RowCache::new(4);
    cache.put("users", record("u1", 1), 1);
    cache.put("moments", record("u1", 1), 1);
    cache.evict("users", "u1");
    cache.evict("users", "missing");
    assert!(cache.get("users", "u1").is_none());
    assert!(cache.get("moments", "u1").is_some());
    assert_eq!(cache.counters().cache_evictions, 1);
}

#[test]
fn a_row_being_prepared_does_not_hold_the_cache_against_another_reader() {
    use crate::db::fixtures::*;
    let db = booted();
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    put(&db, "users", json!({"id": "u2", "name": "Bob"}));
    assert!(db.find("users", &RowId::new("u1").unwrap()).unwrap().is_some());
    locked(&db.shared.cache).evict("users", "u2");
    let (entered, hold) = (std::sync::mpsc::channel::<()>(), std::sync::mpsc::channel::<()>());
    let release = std::sync::Mutex::new(hold.1);
    *locked(&db.shared.row_build_hook) = Some(Arc::new(move |id: &str| {
        if id == "u2" {
            let _ = entered.0.send(());
            let _ = release.lock().unwrap().recv_timeout(std::time::Duration::from_secs(5));
        }
    }));
    let loader = {
        let db = db.clone();
        std::thread::spawn(move || db.find("users", &RowId::new("u2").unwrap()).unwrap())
    };
    entered.1.recv_timeout(std::time::Duration::from_secs(5)).expect("the loader reached the row build");
    let (done, finished) = std::sync::mpsc::channel();
    let reader = {
        let db = db.clone();
        std::thread::spawn(move || {
            let hit = db.peek_row("users", &RowId::new("u1").unwrap()).is_some();
            let _ = done.send(hit);
        })
    };
    let answered = finished.recv_timeout(std::time::Duration::from_secs(2));
    let _ = hold.0.send(());
    join_within(reader);
    assert!(join_within(loader).is_some());
    assert_eq!(answered, Ok(true), "a ready row was unreachable while another row was being prepared");
}
