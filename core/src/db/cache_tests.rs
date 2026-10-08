use super::cache::RowCache;
use super::row::Record;
use super::*;
use crate::pacing::Counters;
use crate::identity::RowId;
use serde_json::{json, Map};
use std::sync::Arc;

fn record(id: &str, revision: i64) -> Row {
    let mut data = Map::new();
    data.insert("id".into(), json!(id));
    Arc::new(Record { id: RowId::new(id).unwrap(), revision, data, sync: Default::default(), ephemeral: 0 })
}

#[test]
fn get_counts_hits_and_misses() {
    let mut cache = RowCache::new(4);
    assert!(cache.get("users", "u1").is_none());
    cache.put("users", record("u1", 1));
    assert!(cache.get("users", "u1").is_some());
    assert_eq!(cache.counters(), Counters { cache_hits: 1, cache_misses: 1, cache_evictions: 0, ..Counters::default() });
}

#[test]
fn fresh_requires_the_same_revision() {
    let mut cache = RowCache::new(4);
    cache.put("users", record("u1", 3));
    assert!(cache.fresh("users", "u1", 3).is_some());
    assert!(cache.fresh("users", "u1", 4).is_none());
    assert_eq!(cache.counters(), Counters { cache_hits: 1, cache_misses: 1, cache_evictions: 0, ..Counters::default() });
}

#[test]
fn capacity_evicts_the_least_recently_used_row() {
    let mut cache = RowCache::new(2);
    cache.put("users", record("u1", 1));
    cache.put("users", record("u2", 1));
    cache.get("users", "u1");
    cache.put("users", record("u3", 1));
    assert!(cache.get("users", "u2").is_none());
    assert!(cache.get("users", "u1").is_some());
    assert_eq!(cache.counters().cache_evictions, 1);
}

#[test]
fn replacing_a_row_is_not_an_eviction() {
    let mut cache = RowCache::new(2);
    cache.put("users", record("u1", 1));
    cache.put("users", record("u1", 2));
    assert_eq!(cache.counters().cache_evictions, 0);
    assert_eq!(cache.get("users", "u1").unwrap().revision, 2);
}

#[test]
fn evict_removes_one_row_and_counts_it() {
    let mut cache = RowCache::new(4);
    cache.put("users", record("u1", 1));
    cache.put("moments", record("u1", 1));
    cache.evict("users", "u1");
    cache.evict("users", "missing");
    assert!(cache.get("users", "u1").is_none());
    assert!(cache.get("moments", "u1").is_some());
    assert_eq!(cache.counters().cache_evictions, 1);
}
