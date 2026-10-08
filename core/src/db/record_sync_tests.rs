use serde_json::json;

use super::fixtures::*;
use super::*;
use crate::commit::{CommitEnvelope, OperationStatus, OperationTransition, WriteOrigin};
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
    let row = db.create("moments", obj(json!({"userId": "u1", "text": "hi"}))).unwrap();
    assert!(row.is_local());
    assert_eq!((row.sync.state, row.sync.actions.clone(), row.sync.attempts, row.sync.since), (SyncState::Pending, vec!["create".to_string()], 0, 1_000_000));
    assert_eq!(db.sync_info("moments", &row.id).unwrap(), row.sync);
}

#[test]
fn updated_record_lists_the_fields_not_yet_confirmed() {
    let db = booted();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    assert!(db.find("moments", &rid("m1")).unwrap().unwrap().sync.is_synced());
    db.update("moments", &rid("m1"), obj(json!({"text": "two"}))).unwrap();
    let row = db.find("moments", &rid("m1")).unwrap().unwrap();
    assert!(!row.is_local());
    assert_eq!((row.sync.state, row.sync.actions.clone(), row.sync.dirty_fields.clone()), (SyncState::Pending, vec!["update".to_string()], vec!["text".to_string()]));
}

#[test]
fn state_walks_pending_sending_failed_pending_with_attempts_and_error() {
    let db = booted();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    db.update("moments", &rid("m1"), obj(json!({"text": "two"}))).unwrap();
    let seq = only_seq(&db);
    apply(&db, OperationTransition::Start { seq });
    let sending = db.sync_info("moments", &rid("m1")).unwrap();
    assert_eq!((sending.state, sending.attempts), (SyncState::Sending, 1));
    rejected(&db, seq);
    let failed = db.sync_info("moments", &rid("m1")).unwrap();
    assert_eq!((failed.state, failed.attempts, failed.error), (SyncState::Failed, 1, Some(OperationFailure::new(FailureKind::Rejected, "no"))));
    assert_eq!(db.retry("moments", &rid("m1")).unwrap(), 1);
    let again = db.sync_info("moments", &rid("m1")).unwrap();
    assert_eq!((again.state, again.attempts, again.error.clone()), (SyncState::Pending, 0, None));
    assert_eq!(db.find("moments", &rid("m1")).unwrap().unwrap().sync, again);
}

#[test]
fn state_change_without_a_data_change_reaches_a_row_observer_as_updated() {
    let db = booted();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    db.update("moments", &rid("m1"), obj(json!({"text": "two"}))).unwrap();
    let (log, callback) = recorder();
    let _subscription = db.observe_row("moments", &rid("m1"), callback).unwrap();
    db.settle();
    apply(&db, OperationTransition::Start { seq: only_seq(&db) });
    db.settle();
    let seen = changes(&log);
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0].rows[0].revision, seen[1].rows[0].revision);
    assert_eq!((seen[1].rows[0].sync.state, seen[1].diff.updated.clone()), (SyncState::Sending, vec![rid("m1")]));
}

#[test]
fn state_change_reaches_a_list_observer_and_a_scope_free_query() {
    let db = booted();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    put(&db, "moments", json!({"id": "m2", "userId": "u1", "text": "two"}));
    db.update("moments", &rid("m2"), obj(json!({"text": "deux"}))).unwrap();
    let (log, callback) = recorder();
    let _subscription = db.observe(Query::new("moments").ordered(vec![Order::new("id", SortDirection::Asc)]), callback).unwrap();
    db.settle();
    rejected(&db, only_seq(&db));
    db.settle();
    let last = changes(&log).pop().unwrap();
    assert_eq!(last.diff.updated, vec![rid("m2")]);
    assert_eq!(last.rows.iter().map(|row| row.sync.state).collect::<Vec<_>>(), [SyncState::Synced, SyncState::Failed]);
}

#[test]
fn queries_filter_by_sync_state_and_count_matches_rows() {
    let db = booted();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    let created = db.create("moments", obj(json!({"userId": "u1", "text": "new"}))).unwrap();
    db.update("moments", &rid("m1"), obj(json!({"text": "uno"}))).unwrap();
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
    let row = db.create("moments", obj(json!({"userId": "u1", "text": "draft"}))).unwrap();
    db.update("moments", &row.id, obj(json!({"text": "edited"}))).unwrap();
    rejected(&db, db.outbox().unwrap()[0].operation.id.0);
    assert_eq!(state_of(&db, "moments", row.id.as_str()), SyncState::Failed);
    assert_eq!(db.discard("moments", &row.id).unwrap(), 1);
    assert!(db.find("moments", &row.id).unwrap().is_none());
    assert!(db.outbox().unwrap().is_empty());
    assert_eq!(state_of(&db, "moments", row.id.as_str()), SyncState::Synced);
}

#[test]
fn discard_restores_the_values_of_a_failed_update() {
    let db = booted();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    db.update("moments", &rid("m1"), obj(json!({"text": "two"}))).unwrap();
    rejected(&db, only_seq(&db));
    assert_eq!(db.discard("moments", &rid("m1")).unwrap(), 1);
    let row = db.find("moments", &rid("m1")).unwrap().unwrap();
    assert_eq!((row.text("text"), row.sync.state), (Some("one"), SyncState::Synced));
}

#[test]
fn retry_does_nothing_for_a_record_without_stuck_operations() {
    let db = booted();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    db.update("moments", &rid("m1"), obj(json!({"text": "two"}))).unwrap();
    assert_eq!(db.retry("moments", &rid("m1")).unwrap(), 0);
    assert_eq!(db.outbox().unwrap().len(), 1);
    assert!(matches!(db.retry("nope", &rid("m1")), Err(DbError::UnknownModel(_))));
}

#[test]
fn restart_turns_an_interrupted_send_into_delivery_unknown_and_repairs_row_sync() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    {
        let db = Db::open(&path, all_specs(), None).unwrap();
        put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
        db.update("moments", &rid("m1"), obj(json!({"text": "two"}))).unwrap();
        apply(&db, OperationTransition::Start { seq: only_seq(&db) });
        assert_eq!(state_of(&db, "moments", "m1"), SyncState::Sending);
        db.execute_for_test("UPDATE row_sync SET state = 'pending', attempts = 7; INSERT INTO row_sync (model, row_id, state, actions, dirty_fields, attempts, since) VALUES ('moments', 'ghost', 'failed', x'5b5d', x'5b5d', 0, 1);");
    }
    let db = Db::open(&path, all_specs(), None).unwrap();
    let info = db.sync_info("moments", &rid("m1")).unwrap();
    assert_eq!((info.state, info.attempts), (SyncState::DeliveryUnknown, 1));
    assert_eq!(state_of(&db, "moments", "ghost"), SyncState::Synced);
    assert_eq!(db.outbox().unwrap()[0].operation.status, OperationStatus::DeliveryUnknown);
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

#[test]
fn a_new_action_on_a_pending_row_reaches_its_observer() {
    let db = booted();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    db.update("moments", &rid("m1"), obj(json!({"text": "two"}))).unwrap();
    let (log, callback) = recorder();
    let _subscription = db.observe_row("moments", &rid("m1"), callback).unwrap();
    db.settle();
    db.act("moments", "like", &rid("m1"), serde_json::Map::new(), serde_json::Map::new()).unwrap();
    db.settle();
    let last = changes(&log).pop().unwrap();
    assert_eq!(last.rows[0].sync.actions, vec!["update".to_string(), "like".to_string()]);
}

#[test]
fn a_state_change_of_an_included_row_reaches_the_parent_observer() {
    let db = booted();
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    db.update("users", &rid("u1"), obj(json!({"name": "Anna"}))).unwrap();
    let (log, callback) = recorder();
    let _subscription = db.observe(Query::new("moments").including(&["author"]), callback).unwrap();
    db.settle();
    apply(&db, OperationTransition::Start { seq: only_seq(&db) });
    db.settle();
    let last = changes(&log).pop().unwrap();
    assert_eq!(last.diff.updated, vec![rid("m1")]);
}
