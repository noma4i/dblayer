use serde_json::json;

use super::fixtures::*;
use super::*;
use crate::commit::{CommitEnvelope, OperationTransition, WriteOrigin};
use crate::operation::OperationStatus;
use crate::query::{Order, Query, Where};
use crate::schema::{ActionDeclaration, FieldSpec, ModelSpec, RootShape};
use crate::syncstate::{FailureKind, OperationFailure, SyncState};
use crate::value::{ScalarKind, SortDirection};

fn apply(db: &Db, transition: OperationTransition) {
    let mut envelope = CommitEnvelope::new(WriteOrigin::Response, db.generation());
    envelope.operation_transitions.push(transition);
    db.shared.store.commit(envelope).unwrap();
}

fn only_seq(db: &Db) -> i64 {
    db.outbox().unwrap()[0].operation.id.0
}

fn rejected(db: &Db, seq: i64) {
    apply(db, OperationTransition::Fail { seq, status: OperationStatus::Failed, failure: OperationFailure::new(FailureKind::Rejected, "no") });
}

fn state_of(db: &Db, model: &str, id: &str) -> SyncState {
    db.sync_info(model, &rid(id)).unwrap().state
}


#[test]
fn created_record_is_pending_local_and_names_its_action() {
    let db = booted();
    let row = db.create(&db.admit().unwrap(), "moments", obj(json!({"userId": "u1", "text": "hi"}))).unwrap();
    assert!(row.is_local());
    assert_eq!((row.sync.state, row.sync.actions.clone(), row.sync.attempts, row.sync.since), (SyncState::Pending, vec!["create".to_string()], 0, 1_000_000));
    assert_eq!(db.sync_info("moments", &row.id).unwrap(), row.sync);
}

#[test]
fn updated_record_lists_the_fields_not_yet_confirmed() {
    let db = booted();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    assert!(db.find("moments", &rid("m1")).unwrap().unwrap().sync.is_synced());
    db.update(&db.admit().unwrap(), "moments", &rid("m1"), obj(json!({"text": "two"}))).unwrap();
    let row = db.find("moments", &rid("m1")).unwrap().unwrap();
    assert!(!row.is_local());
    assert_eq!((row.sync.state, row.sync.actions.clone(), row.sync.dirty_fields.clone()), (SyncState::Pending, vec!["update".to_string()], vec!["text".to_string()]));
}

#[test]
fn state_walks_pending_sending_failed_pending_with_attempts_and_error() {
    let db = booted();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    db.update(&db.admit().unwrap(), "moments", &rid("m1"), obj(json!({"text": "two"}))).unwrap();
    let seq = only_seq(&db);
    apply(&db, OperationTransition::Start { seq });
    let sending = db.sync_info("moments", &rid("m1")).unwrap();
    assert_eq!((sending.state, sending.attempts), (SyncState::Sending, 1));
    rejected(&db, seq);
    let failed = db.sync_info("moments", &rid("m1")).unwrap();
    assert_eq!((failed.state, failed.attempts, failed.error), (SyncState::Failed, 1, Some(OperationFailure::new(FailureKind::Rejected, "no"))));
    assert_eq!(db.retry(&db.admit().unwrap(), "moments", &rid("m1"), None).unwrap(), 1);
    let again = db.sync_info("moments", &rid("m1")).unwrap();
    assert_eq!((again.state, again.attempts, again.error.clone()), (SyncState::Pending, 0, None));
    assert_eq!(db.find("moments", &rid("m1")).unwrap().unwrap().sync, again);
}



#[test]
fn inv_sync_02_queries_filter_by_sync_state_and_count_matches_rows() {
    let db = booted();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    let created = db.create(&db.admit().unwrap(), "moments", obj(json!({"userId": "u1", "text": "new"}))).unwrap();
    db.update(&db.admit().unwrap(), "moments", &rid("m1"), obj(json!({"text": "uno"}))).unwrap();
    let outbox = db.outbox().unwrap();
    rejected(&db, outbox.iter().find(|entry| entry.operation.action_key == "moments:update").unwrap().operation.id.0);
    let ids = |query: Query| db.query(&query).unwrap().iter().map(|row| row.id.to_string()).collect::<Vec<_>>();
    assert_eq!(ids(Query::failed("moments")), ["m1"]);
    assert_eq!(ids(Query::pending("moments")), [created.id.to_string()]);
    assert_eq!(ids(Query::unsynced("moments").ordered(vec![Order::new("id", SortDirection::Asc)])), ["m1".to_string(), created.id.to_string()]);
    let synced = Query::new("moments").filtered(Where::equal("sync", json!("synced")));
    assert_eq!(db.count(&synced).unwrap(), 0);
    assert_eq!(db.count(&Query::unsynced("moments")).unwrap(), 2);
    let ordered = Query::new("moments").ordered(vec![Order::new("sync", SortDirection::Asc), Order::new("id", SortDirection::Asc)]);
    assert_eq!(ids(ordered)[0], "m1");
}

#[test]
fn discard_rolls_back_a_failed_create_and_its_queued_followers() {
    let db = booted();
    let row = db.create(&db.admit().unwrap(), "moments", obj(json!({"userId": "u1", "text": "draft"}))).unwrap();
    db.update(&db.admit().unwrap(), "moments", &row.id, obj(json!({"text": "edited"}))).unwrap();
    rejected(&db, db.outbox().unwrap()[0].operation.id.0);
    assert_eq!(state_of(&db, "moments", row.id.as_str()), SyncState::Failed);
    assert_eq!(db.discard(&db.admit().unwrap(), "moments", &row.id, None).unwrap(), 2);
    assert!(db.find("moments", &row.id).unwrap().is_none());
    assert!(db.outbox().unwrap().is_empty());
    assert_eq!(state_of(&db, "moments", row.id.as_str()), SyncState::Synced);
}

#[test]
fn discard_restores_the_values_of_a_failed_update() {
    let db = booted();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    db.update(&db.admit().unwrap(), "moments", &rid("m1"), obj(json!({"text": "two"}))).unwrap();
    rejected(&db, only_seq(&db));
    assert_eq!(db.discard(&db.admit().unwrap(), "moments", &rid("m1"), None).unwrap(), 1);
    let row = db.find("moments", &rid("m1")).unwrap().unwrap();
    assert_eq!((row.text("text"), row.sync.state), (Some("one"), SyncState::Synced));
}

#[test]
fn retry_does_nothing_for_a_record_without_stuck_operations() {
    let db = booted();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    db.update(&db.admit().unwrap(), "moments", &rid("m1"), obj(json!({"text": "two"}))).unwrap();
    assert_eq!(db.retry(&db.admit().unwrap(), "moments", &rid("m1"), None).unwrap(), 0);
    assert_eq!(db.outbox().unwrap().len(), 1);
    assert!(matches!(db.retry(&db.admit().unwrap(), "nope", &rid("m1"), None), Err(DbError::UnknownModel(_))));
}

#[test]
fn a_field_named_sync_and_an_action_named_like_a_record_gesture_are_rejected() {
    let field = ModelSpec::new("notes", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("sync", ScalarKind::Str).optional()]);
    assert!(matches!(Db::open_in_memory(vec![field], None), Err(DbError::Invalid(_))));
    for name in ["sync", "retry", "discard"] {
        let mut spec = ModelSpec::new("notes", vec![FieldSpec::scalar("id", ScalarKind::Id)]);
        spec.actions.push(ActionDeclaration::new(name, RootShape::Upsert));
        assert!(matches!(Db::open_in_memory(vec![spec], None), Err(DbError::Invalid(_))), "{name}");
    }
}



