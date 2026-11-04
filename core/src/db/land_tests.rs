use super::fixtures::*;
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
        let result = db.land(&db.admit().unwrap(), upsert_envelope(&db, origin, &id, "n")).unwrap();
        assert!(!result.is_noop());
        assert!(db.find("users", &rid(&id)).unwrap().is_some());
    }
}

#[test]
fn land_rejects_optimistic_envelopes() {
    let db = booted();
    let envelope = upsert_envelope(&db, WriteOrigin::Optimistic, "u1", "x");
    assert!(matches!(db.land(&db.admit().unwrap(), envelope), Err(DbError::OriginNotLandable(WriteOrigin::Optimistic))));
    assert!(db.find("users", &rid("u1")).unwrap().is_none());
}

#[test]
fn land_rejects_a_stale_generation() {
    let db = booted();
    let mut envelope = upsert_envelope(&db, WriteOrigin::Event, "u1", "x");
    envelope.generation = envelope.generation.next();
    assert!(matches!(db.land(&db.admit().unwrap(), envelope), Err(DbError::Store(crate::store::StoreError::Envelope(EnvelopeFault::StaleGeneration)))));
}

#[tokio::test(flavor = "multi_thread")]
async fn an_old_subscription_admission_cannot_land_an_event_in_the_next_session() {
    let db = booted();
    let life = db.shared.store.lifecycle.clone();
    assert!(life.session_from(Some("A".into()), life.admit_passive(crate::lifecycle::Origin::Explicit, "test session", Some(life.snapshot().session_epoch)).unwrap()).is_ok());
    let admitted = life.admit(crate::lifecycle::Origin::Session, "subscription event").unwrap();
    db.end_session::<crate::transport::TransportError>(crate::lifecycle::End::Local, None, false, |_| Ok(())).await.unwrap();
    assert!(life.session_from(Some("B".into()), life.admit_passive(crate::lifecycle::Origin::Explicit, "test session", Some(life.snapshot().session_epoch)).unwrap()).is_ok());
    let event = upsert_envelope(&db, WriteOrigin::Event, "u1", "A");

    let result = db.land(&admitted, event);

    assert!(matches!(result, Err(DbError::NoSession { .. })));
    assert!(db.find("users", &rid("u1")).unwrap().is_none());
}

#[test]
fn land_of_an_identical_row_is_a_noop_that_keeps_the_cache() {
    let db = booted();
    db.land(&db.admit().unwrap(), upsert_envelope(&db, WriteOrigin::Snapshot, "u1", "same")).unwrap();
    let first = db.find("users", &rid("u1")).unwrap().unwrap();
    let result = db.land(&db.admit().unwrap(), upsert_envelope(&db, WriteOrigin::Snapshot, "u1", "same")).unwrap();
    assert!(result.is_noop());
    assert!(std::sync::Arc::ptr_eq(&first, &db.find("users", &rid("u1")).unwrap().unwrap()));
}

const OLD: &str = "2026-01-01T00:00:00.000Z";
const MID: &str = "2026-01-02T00:00:00.000Z";
const NEW: &str = "2026-01-03T00:00:00.000Z";

fn message_envelope(db: &Db, origin: WriteOrigin, id: &str, created_at: &str) -> CommitEnvelope {
    let mut envelope = CommitEnvelope::new(origin, db.generation());
    envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("messages"), id: rid(id), row: obj(json!({"id": id, "chatId": "c1", "createdAt": created_at})) });
    envelope
}

fn chat_pointer(db: &Db) -> (Option<String>, Option<String>) {
    let chat = db.find("chats", &rid("c1")).unwrap().unwrap();
    (chat.text("lastMessageId").map(str::to_string), chat.text("lastMessageAt").map(str::to_string))
}

#[test]
fn landed_newer_message_moves_the_chat_pointer() {
    let db = booted();
    put(&db, "chats", json!({"id": "c1", "title": "t", "lastMessageId": "m1", "lastMessageAt": OLD}));
    for origin in [WriteOrigin::Event, WriteOrigin::Snapshot, WriteOrigin::Response] {
        db.land(&db.admit().unwrap(), message_envelope(&db, origin, "m2", NEW)).unwrap();
        assert_eq!(chat_pointer(&db), (Some("m2".to_string()), Some(NEW.to_string())));
        put(&db, "chats", json!({"id": "c1", "title": "t", "lastMessageId": "m1", "lastMessageAt": OLD}));
    }
}

#[test]
fn landed_older_message_keeps_the_chat_pointer() {
    let db = booted();
    put(&db, "chats", json!({"id": "c1", "title": "t", "lastMessageId": "m9", "lastMessageAt": NEW}));
    db.land(&db.admit().unwrap(), message_envelope(&db, WriteOrigin::Event, "m2", OLD)).unwrap();
    assert_eq!(chat_pointer(&db), (Some("m9".to_string()), Some(NEW.to_string())));
}

#[test]
fn a_message_later_in_the_same_second_moves_a_pointer_that_the_server_wrote_in_whole_seconds() {
    let db = booted();
    let server_row = |model: &str, row: serde_json::Value| db.spec(model).unwrap().patch(&obj(row));
    put(&db, "chats", serde_json::Value::Object(server_row("chats", json!({"id": "c1", "title": "t", "lastMessageId": "m1", "lastMessageAt": "2026-01-03T00:00:05Z"}))));
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    let row = server_row("messages", json!({"id": "m2", "chatId": "c1", "createdAt": "2026-01-03T00:00:05.900Z"}));
    envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("messages"), id: rid("m2"), row });
    db.land(&db.admit().unwrap(), envelope).unwrap();
    assert_eq!(chat_pointer(&db).0.as_deref(), Some("m2"));
}

fn destroy_envelope(db: &Db, id: &str) -> CommitEnvelope {
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    envelope.entity_ops.push(EntityOp::Destroy { model: ModelKey::new("messages"), id: rid(id), reason: crate::commit::DestroyReason::Server });
    envelope
}

#[test]
fn destroying_the_head_message_falls_back_to_the_previous_one() {
    let db = booted();
    put(&db, "messages", json!({"id": "m1", "chatId": "c1", "createdAt": OLD}));
    put(&db, "messages", json!({"id": "m2", "chatId": "c1", "createdAt": MID}));
    put(&db, "messages", json!({"id": "m3", "chatId": "c1", "createdAt": NEW}));
    put(&db, "chats", json!({"id": "c1", "title": "t", "lastMessageId": "m3", "lastMessageAt": NEW}));
    db.land(&db.admit().unwrap(), destroy_envelope(&db, "m3")).unwrap();
    assert_eq!(chat_pointer(&db), (Some("m2".to_string()), Some(MID.to_string())));
}

#[test]
fn destroying_a_non_head_message_keeps_the_chat_pointer() {
    let db = booted();
    put(&db, "messages", json!({"id": "m1", "chatId": "c1", "createdAt": OLD}));
    put(&db, "messages", json!({"id": "m3", "chatId": "c1", "createdAt": NEW}));
    put(&db, "chats", json!({"id": "c1", "title": "t", "lastMessageId": "m3", "lastMessageAt": NEW}));
    db.land(&db.admit().unwrap(), destroy_envelope(&db, "m1")).unwrap();
    assert_eq!(chat_pointer(&db), (Some("m3".to_string()), Some(NEW.to_string())));
}

#[test]
fn server_seconds_and_local_milliseconds_sort_compare_and_filter_by_time() {
    use crate::query::{Order, Query, Where};
    use crate::value::SortDirection;
    let db = booted();
    let at = |id: &str, created_at: &str| {
        let row = db.spec("messages").unwrap().patch(&obj(json!({"id": id, "chatId": "c1", "createdAt": created_at})));
        put(&db, "messages", serde_json::Value::Object(row));
    };
    at("a", "2026-01-03T00:00:05.900Z");
    at("b", "2026-01-03T00:00:05Z");
    at("c", "2026-01-03T00:00:06Z");
    let ids = |query: Query| db.query(&query).unwrap().into_iter().map(|row| row.id.to_string()).collect::<Vec<_>>();
    assert_eq!(ids(Query::new("messages").ordered(vec![Order::new("createdAt", SortDirection::Asc)])), ["b", "a", "c"]);
    let after = Where::compare("createdAt", crate::query::CompareOp::Greater, json!("2026-01-03T00:00:05.500Z"));
    assert_eq!(ids(Query::new("messages").filtered(after).ordered(vec![Order::new("createdAt", SortDirection::Asc)])), ["a", "c"]);
}

#[test]
fn landing_on_a_disk_database_plans_off_the_writer() {
    let directory = tempfile::tempdir().unwrap();
    let db = Db::open(&directory.path().join("db.sqlite"), all_specs(), None).unwrap();
    db.land(&db.admit().unwrap(), upsert_envelope(&db, WriteOrigin::Event, "u1", "n")).unwrap();
    let counters = db.shared.store.counters();
    assert_eq!((counters.plans_off_writer, counters.plans_under_writer), (1, 0));
}
