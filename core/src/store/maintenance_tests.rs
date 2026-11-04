use super::fixtures::*;
use super::*;
use std::collections::BTreeSet;

fn policy(drop_after_ms: i64, protected: &[&str]) -> TempRowPolicy {
    TempRowPolicy {
        model: model("items"),
        drop_after_ms,
        protected: protected.iter().map(|text| id(text)).collect::<BTreeSet<_>>(),
    }
}

fn item(store: &Store, row_id: &str, created_at: Option<&str>) {
    let payload = match created_at {
        Some(value) => serde_json::json!({"id": row_id, "createdAt": value}),
        None => serde_json::json!({"id": row_id}),
    };
    store.commit(envelope(store, vec![upsert("items", payload)], vec![])).unwrap();
}

#[test]
fn expired_temp_row_is_deleted() {
    let (store, _) = booted_store();
    item(&store, "temp-1", Some("1970-01-01T00:00:10.000Z"));
    let report = store.maintain(&store.lifecycle.admit(crate::lifecycle::Origin::Explicit, "test maintenance").unwrap(), &[policy(60_000, &[])]).unwrap();
    assert_eq!(report.temp_rows_dropped, 1);
    assert_eq!(store.row_count("items").unwrap(), 0);
}

#[test]
fn protected_temp_row_is_kept() {
    let (store, _) = booted_store();
    item(&store, "temp-1", Some("1970-01-01T00:00:10.000Z"));
    let report = store.maintain(&store.lifecycle.admit(crate::lifecycle::Origin::Explicit, "test maintenance").unwrap(), &[policy(60_000, &["temp-1"])]).unwrap();
    assert_eq!(report.temp_rows_dropped, 0);
    assert_eq!(store.row_count("items").unwrap(), 1);
}

#[test]
fn regular_and_young_temp_rows_are_kept() {
    let (store, _) = booted_store();
    item(&store, "v1", Some("1970-01-01T00:00:10.000Z"));
    item(&store, "temp-young", Some("1970-01-01T00:16:35.000Z"));
    let report = store.maintain(&store.lifecycle.admit(crate::lifecycle::Origin::Explicit, "test maintenance").unwrap(), &[policy(60_000, &[])]).unwrap();
    assert_eq!(report.temp_rows_dropped, 0);
    assert_eq!(store.row_count("items").unwrap(), 2);
}

#[test]
fn temp_row_without_created_at_counts_as_oldest() {
    let (store, _) = booted_store();
    item(&store, "temp-null", None);
    assert_eq!(store.maintain(&store.lifecycle.admit(crate::lifecycle::Origin::Explicit, "test maintenance").unwrap(), &[policy(60_000, &[])]).unwrap().temp_rows_dropped, 1);
}

#[test]
fn expiry_boundary_is_strict() {
    let (store, _) = booted_store();
    item(&store, "temp-edge", Some("1970-01-01T00:15:40.000Z"));
    assert_eq!(store.maintain(&store.lifecycle.admit(crate::lifecycle::Origin::Explicit, "test maintenance").unwrap(), &[policy(60_000, &[])]).unwrap().temp_rows_dropped, 0);
}

#[test]
fn expired_temp_row_is_destroyed_without_tombstone_and_with_membership_removed() {
    let (store, _) = booted_store();
    let list = scope("items", "list");
    item(&store, "temp-1", Some("1970-01-01T00:00:10.000Z"));
    store.commit(envelope(&store, vec![], vec![append(&list, "temp-1", "a")])).unwrap();
    let report = store.maintain(&store.lifecycle.admit(crate::lifecycle::Origin::Explicit, "test maintenance").unwrap(), &[policy(60_000, &[])]).unwrap();
    let commit = report.commit.unwrap();
    assert!(commit.scopes.contains(&list));
    assert!(!store.is_tombstoned("items", "temp-1").unwrap());
    assert!(store.scope_members("items", list.key.as_str()).unwrap().is_empty());
}

#[test]
fn model_without_created_at_column_is_rejected() {
    let (store, _) = booted_store();
    let bad = TempRowPolicy { model: model("messages"), drop_after_ms: 1, protected: BTreeSet::new() };
    assert!(matches!(store.maintain(&store.lifecycle.admit(crate::lifecycle::Origin::Explicit, "test maintenance").unwrap(), &[bad]), Err(StoreError::Constraint(_))));
}

#[test]
fn maintain_without_policies_only_prunes_tombstones() {
    let (store, clock) = booted_store();
    store.mark_tombstone("items", "old", 1, 0).unwrap();
    clock.advance(std::time::Duration::from_millis(200_000_000));
    let report = store.maintain(&store.lifecycle.admit(crate::lifecycle::Origin::Explicit, "test maintenance").unwrap(), &[]).unwrap();
    assert_eq!(report.tombstone_drops, 1);
    assert!(report.commit.is_none());
}
