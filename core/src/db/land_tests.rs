use super::support::*;
use super::*;
use crate::commit::{CommitEnvelope, EnvelopeFault, EntityOp, WriteOrigin};
use crate::identity::ModelKey;
use serde_json::json;

fn upsert_envelope(db: &Db, origin: WriteOrigin, id: &str, name: &str) -> CommitEnvelope {
    let mut envelope = CommitEnvelope::new(origin, db.generation());
    envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("users"), id: rid(id), row: obj(json!({"id": id, "name": name})) });
    envelope
}

#[test]
fn land_writes_server_rows_for_each_landable_origin() {
    let db = booted();
    for (index, origin) in [WriteOrigin::Snapshot, WriteOrigin::Event, WriteOrigin::Response].into_iter().enumerate() {
        let id = format!("u{index}");
        let result = db.land(upsert_envelope(&db, origin, &id, "n")).unwrap();
        assert!(!result.is_noop());
        assert!(db.find("users", &rid(&id)).unwrap().is_some());
    }
}

#[test]
fn land_drops_the_cached_copy_of_every_changed_row() {
    let db = booted();
    db.land(upsert_envelope(&db, WriteOrigin::Snapshot, "u1", "old")).unwrap();
    assert_eq!(db.find("users", &rid("u1")).unwrap().unwrap().text("name"), Some("old"));
    db.land(upsert_envelope(&db, WriteOrigin::Event, "u1", "new")).unwrap();
    assert_eq!(db.find("users", &rid("u1")).unwrap().unwrap().text("name"), Some("new"));
}

#[test]
fn land_rejects_optimistic_envelopes() {
    let db = booted();
    let envelope = upsert_envelope(&db, WriteOrigin::Optimistic, "u1", "x");
    assert!(matches!(db.land(envelope), Err(DbError::OriginNotLandable(WriteOrigin::Optimistic))));
    assert!(db.find("users", &rid("u1")).unwrap().is_none());
}

#[test]
fn land_rejects_a_stale_generation() {
    let db = booted();
    let mut envelope = upsert_envelope(&db, WriteOrigin::Event, "u1", "x");
    envelope.generation = envelope.generation.next();
    assert!(matches!(db.land(envelope), Err(DbError::Store(crate::store::StoreError::Envelope(EnvelopeFault::StaleGeneration)))));
}

#[test]
fn land_of_an_identical_row_is_a_noop_that_keeps_the_cache() {
    let db = booted();
    db.land(upsert_envelope(&db, WriteOrigin::Snapshot, "u1", "same")).unwrap();
    let first = db.find("users", &rid("u1")).unwrap().unwrap();
    let result = db.land(upsert_envelope(&db, WriteOrigin::Snapshot, "u1", "same")).unwrap();
    assert!(result.is_noop());
    assert!(std::sync::Arc::ptr_eq(&first, &db.find("users", &rid("u1")).unwrap().unwrap()));
}
