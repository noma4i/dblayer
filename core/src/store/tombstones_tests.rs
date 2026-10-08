use super::fixtures::*;
use super::*;
use std::collections::BTreeSet;

use crate::identity::RowRef;

const RETENTION: TombstoneRetention = TombstoneRetention::DEFAULT;

fn reference(model_name: &str, row_id: &str) -> RowRef {
    RowRef::new(model(model_name), id(row_id))
}

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
    assert_eq!(store.read(|reader| reader.tombstone_seq("messages", "1")).unwrap(), Some(2));
}

#[test]
fn expiry_boundary_is_strict_and_caps_decide_eviction() {
    let now = 100_000_000;
    assert!(!RETENTION.is_expired(now - RETENTION.ttl_ms + 1, now));
    assert!(!RETENTION.is_expired(now - RETENTION.ttl_ms, now));
    assert!(RETENTION.is_expired(now - RETENTION.ttl_ms - 1, now));
    assert_eq!(RETENTION.eviction_count(RETENTION.cap - 1), 0);
    assert_eq!(RETENTION.eviction_count(RETENTION.cap), 0);
    assert_eq!(RETENTION.eviction_count(RETENTION.cap + 3), 3);
    assert!(!RETENTION.ignores_minimum_age(RETENTION.overflow_cap));
    assert!(RETENTION.ignores_minimum_age(RETENTION.overflow_cap + 1));
}

#[test]
fn evictable_takes_oldest_first_and_keeps_young_below_overflow() {
    let now = 100_000_000;
    let oldest = reference("message", "row-oldest");
    let old = reference("message", "row-old");
    let middle = reference("message", "row-middle");
    let recent = reference("message", "row-recent");
    let newest = reference("message", "row-newest");
    let entries = vec![
        (recent, now - 100_000),
        (oldest.clone(), now - 86_401_000),
        (newest, now - 1_000),
        (middle.clone(), now - 500_000),
        (old.clone(), now - 601_000),
    ];
    let young = vec![(middle.clone(), now - 500_000), (reference("message", "n"), now - 100_000), (old.clone(), now - 601_000)];
    assert_eq!(RETENTION.evictable(&entries, now, RETENTION.cap + 2), vec![oldest, old.clone()]);
    assert_eq!(RETENTION.evictable(&young, now, RETENTION.cap + 2), vec![old]);
}

#[test]
fn temp_row_staleness_needs_ttl_and_no_open_operation() {
    let now = 100_000_000;
    let row_id = id("temp-row");
    let open: BTreeSet<_> = [row_id.clone()].into();
    let none = BTreeSet::new();
    assert!(!TombstoneRetention::temp_row_is_stale(now - 601_000, now, None, &none, &row_id));
    assert!(!TombstoneRetention::temp_row_is_stale(now - 601_000, now, Some(600_000), &open, &row_id));
    assert!(!TombstoneRetention::temp_row_is_stale(now - 500_000, now, Some(600_000), &none, &row_id));
    assert!(TombstoneRetention::temp_row_is_stale(now - 601_000, now, Some(600_000), &none, &row_id));
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
