use super::fixtures::*;
use super::*;
use crate::commit::{
    CommitEnvelope, DestroyReason, EntityOp, EnvelopeFault, OperationRecord, OperationStatus, OperationTransition,
    RowChangeKind, ScopeOp, WriteOrigin,
};
use serde_json::json;

fn commit(store: &Store, entity_ops: Vec<EntityOp>, scope_ops: Vec<ScopeOp>) -> crate::commit::CommitResult {
    store.commit(envelope(store, entity_ops, scope_ops)).unwrap()
}

#[test]
fn commit_inserts_row_with_promoted_column_and_payload() {
    let (store, _) = booted_store();
    let result = commit(&store, vec![upsert("messages", message("m1", "c1"))], vec![]);
    assert_eq!(result.revision, Some(1));
    assert_eq!(count(&store, "SELECT COUNT(*) FROM row_messages"), 1);
    assert_eq!(text(&store, "SELECT chat_id FROM row_messages WHERE id = 'm1'"), Some("c1".into()));
    let payload = store.point("messages", "m1").unwrap().unwrap();
    assert_eq!(payload["chatId"], json!("c1"));
    assert!(payload.get("chat_id").is_none());
}

#[test]
fn booleans_promote_to_integer_columns() {
    let (store, _) = booted_store();
    commit(&store, vec![upsert("messages", json!({"id": "m1", "chatId": "c1", "seen": true}))], vec![]);
    assert_eq!(count(&store, "SELECT seen FROM row_messages WHERE id = 'm1'"), 1);
}

#[test]
fn identical_upsert_changes_nothing_and_keeps_revision() {
    let (store, _) = booted_store();
    commit(&store, vec![upsert("messages", message("m1", "c1"))], vec![]);
    let repeat = commit(&store, vec![upsert("messages", message("m1", "c1"))], vec![]);
    assert!(repeat.is_noop());
    assert!(repeat.rows.is_empty());
    assert_eq!(store.revision_seq().unwrap(), 1);
}

#[test]
fn changed_upsert_advances_revision_and_field_revisions() {
    let (store, _) = booted_store();
    commit(&store, vec![upsert("messages", json!({"id": "m1", "chatId": "c1", "body": "a"}))], vec![]);
    let second = commit(&store, vec![upsert("messages", json!({"id": "m1", "chatId": "c1", "body": "b"}))], vec![]);
    assert_eq!(second.revision, Some(2));
    assert_eq!(second.rows.len(), 1);
    assert_eq!(second.rows[0].kind, RowChangeKind::Updated);
    assert_eq!(second.rows[0].fields.iter().cloned().collect::<Vec<_>>(), vec!["body".to_string()]);
    let stored = store.read(|reader| reader.stored_row("messages", "m1")).unwrap().unwrap();
    assert_eq!(stored.revision, 2);
    assert_eq!(stored.born_rev, 1);
    assert_eq!(stored.field_rev["body"], 2);
    assert_eq!(stored.field_rev["chatId"], 1);
}

#[test]
fn removed_field_leaves_field_revisions() {
    let (store, _) = booted_store();
    commit(&store, vec![upsert("messages", json!({"id": "m1", "chatId": "c1", "body": "a"}))], vec![]);
    commit(&store, vec![upsert("messages", message("m1", "c1"))], vec![]);
    let stored = store.read(|reader| reader.stored_row("messages", "m1")).unwrap().unwrap();
    assert!(!stored.field_rev.contains_key("body"));
    assert!(stored.row.get("body").is_none());
}

#[test]
fn touch_updates_revision_of_existing_row_only() {
    let (store, _) = booted_store();
    commit(&store, vec![upsert("messages", message("m1", "c1"))], vec![]);
    let touched = commit(
        &store,
        vec![
            EntityOp::Touch { model: model("messages"), id: id("m1") },
            EntityOp::Touch { model: model("messages"), id: id("missing") },
        ],
        vec![],
    );
    assert_eq!(touched.rows.len(), 1);
    assert_eq!(touched.rows[0].kind, RowChangeKind::Touched);
    assert_eq!(count(&store, "SELECT revision FROM row_messages WHERE id = 'm1'"), 2);
}

#[test]
fn destroy_removes_row_and_all_memberships_and_reports_scopes() {
    let (store, _) = booted_store();
    let first = scope("messages", "thread");
    let second = scope("messages", "pinned");
    commit(
        &store,
        vec![upsert("messages", message("m1", "c1"))],
        vec![append(&first, "m1", "a"), append(&second, "m1", "a")],
    );
    let result = commit(&store, vec![destroy("messages", "m1", DestroyReason::Server)], vec![]);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM row_messages"), 0);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM scope_member"), 0);
    assert!(result.scopes.contains(&first) && result.scopes.contains(&second));
    assert_eq!(result.rows[0].kind, RowChangeKind::Destroyed);
}

#[test]
fn destroy_with_server_or_local_reason_writes_tombstone_with_revision() {
    let (store, clock) = booted_store();
    commit(&store, vec![upsert("messages", message("m1", "c1")), upsert("messages", message("m2", "c1"))], vec![]);
    let result = commit(
        &store,
        vec![destroy("messages", "m1", DestroyReason::Server), destroy("messages", "m2", DestroyReason::Expired)],
        vec![],
    );
    assert_eq!(store.read(|reader| reader.tombstone_seq("messages", "m1")).unwrap(), result.revision);
    assert!(!store.is_tombstoned("messages", "m2").unwrap());
    assert_eq!(count(&store, "SELECT deleted_at FROM tombstone WHERE id = 'm1'"), clock.now().as_millis() as i64);
}

#[test]
fn destroying_unknown_row_still_tombstones_it() {
    let (store, _) = booted_store();
    let result = commit(&store, vec![destroy("messages", "ghost", DestroyReason::Local)], vec![]);
    assert!(store.is_tombstoned("messages", "ghost").unwrap());
    assert_eq!(result.revision, Some(1));
}

#[test]
fn destroy_replaced_moves_membership_to_successor_without_tombstone() {
    let (store, _) = booted_store();
    let thread = scope("messages", "thread");
    commit(&store, vec![upsert("messages", message("temp-1", "c1"))], vec![append(&thread, "temp-1", "k1")]);
    let result = commit(
        &store,
        vec![
            destroy("messages", "temp-1", DestroyReason::Replaced { by: id("m1") }),
            upsert("messages", message("m1", "c1")),
        ],
        vec![],
    );
    assert_eq!(
        store.scope_members("messages", thread.key.as_str()).unwrap(),
        vec![("m1".to_string(), "k1".to_string())]
    );
    assert!(!store.is_tombstoned("messages", "temp-1").unwrap());
    assert!(result.scopes.contains(&thread));
}

#[test]
fn scope_append_writes_order_key_even_without_entity() {
    let (store, _) = booted_store();
    let thread = scope("messages", "thread");
    let result = commit(&store, vec![], vec![append(&thread, "m1", "k1")]);
    assert_eq!(result.revision, Some(1));
    assert_eq!(text(&store, "SELECT order_key FROM scope_member"), Some("k1".into()));
    assert!(result.scopes.contains(&thread));
}

#[test]
fn members_come_back_ordered_by_order_key() {
    let (store, _) = booted_store();
    let thread = scope("messages", "thread");
    commit(&store, vec![], vec![append(&thread, "m2", "b"), append(&thread, "m1", "a")]);
    let members: Vec<String> = store.scope_members("messages", thread.key.as_str()).unwrap().into_iter().map(|m| m.0).collect();
    assert_eq!(members, vec!["m1", "m2"]);
}

#[test]
fn move_changes_member_order() {
    let (store, _) = booted_store();
    let thread = scope("messages", "thread");
    commit(&store, vec![], vec![append(&thread, "m1", "a"), append(&thread, "m2", "b")]);
    commit(&store, vec![], vec![ScopeOp::Move { scope: thread.clone(), row: id("m1"), order_key: "z".into() }]);
    let members: Vec<String> = store.scope_members("messages", thread.key.as_str()).unwrap().into_iter().map(|m| m.0).collect();
    assert_eq!(members, vec!["m2", "m1"]);
}

#[test]
fn moves_may_swap_order_keys_in_one_envelope() {
    let (store, _) = booted_store();
    let thread = scope("messages", "thread");
    commit(&store, vec![], vec![append(&thread, "m1", "a"), append(&thread, "m2", "b")]);
    commit(
        &store,
        vec![],
        vec![
            ScopeOp::Move { scope: thread.clone(), row: id("m1"), order_key: "b".into() },
            ScopeOp::Move { scope: thread.clone(), row: id("m2"), order_key: "a".into() },
        ],
    );
    let members: Vec<String> = store.scope_members("messages", thread.key.as_str()).unwrap().into_iter().map(|m| m.0).collect();
    assert_eq!(members, vec!["m2", "m1"]);
}

#[test]
fn detach_removes_only_that_member() {
    let (store, _) = booted_store();
    let thread = scope("messages", "thread");
    commit(&store, vec![], vec![append(&thread, "m1", "a"), append(&thread, "m2", "b")]);
    commit(&store, vec![], vec![ScopeOp::Detach { scope: thread.clone(), row: id("m1") }]);
    assert_eq!(store.count_members("messages", thread.key.as_str()).unwrap(), 1);
}

#[test]
fn order_key_collision_rolls_back_whole_envelope() {
    let (store, _) = booted_store();
    let thread = scope("messages", "thread");
    commit(&store, vec![], vec![append(&thread, "m1", "a")]);
    let failing = envelope(
        &store,
        vec![upsert("messages", message("m9", "c1"))],
        vec![append(&thread, "m2", "a")],
    );
    let error = store.commit(failing).unwrap_err();
    assert!(matches!(error, StoreError::Constraint(_)));
    assert_eq!(count(&store, "SELECT COUNT(*) FROM row_messages"), 0);
    assert_eq!(store.revision_seq().unwrap(), 1);
    assert!(store.apply_failure().is_none());
    commit(&store, vec![upsert("messages", message("m3", "c1"))], vec![]);
}

#[test]
fn unchanged_append_is_not_a_write() {
    let (store, _) = booted_store();
    let thread = scope("messages", "thread");
    commit(&store, vec![], vec![append(&thread, "m1", "a")]);
    let again = commit(&store, vec![], vec![append(&thread, "m1", "a")]);
    assert!(again.is_noop());
    assert!(again.scopes.is_empty());
}

#[test]
fn an_update_reports_the_scopes_holding_the_row_even_past_one_statement_chunk() {
    let (store, _) = booted_store();
    let thread = scope("messages", "thread");
    let ops: Vec<EntityOp> = (0..1200).map(|index| upsert("messages", message(&format!("m{index}"), "c1"))).collect();
    let appends: Vec<ScopeOp> = (0..1200).map(|index| append(&thread, &format!("m{index}"), &format!("k{index:04}"))).collect();
    commit(&store, ops, appends);
    let touches: Vec<EntityOp> = (0..1200).map(|index| EntityOp::Touch { model: model("messages"), id: id(&format!("m{index}")) }).collect();
    let result = commit(&store, touches, vec![]);
    assert_eq!(result.rows.len(), 1200);
    assert!(result.scopes.contains(&thread));
    let update = commit(&store, vec![upsert("messages", json!({"id": "m0", "chatId": "c2"}))], vec![]);
    assert!(update.scopes.contains(&thread));
}

#[test]
fn unknown_model_is_a_plan_error_without_poison() {
    let (store, _) = booted_store();
    let error = store.commit(envelope(&store, vec![upsert("nope", message("m1", "c1"))], vec![])).unwrap_err();
    assert!(matches!(error, StoreError::UnknownModel(_)));
    assert!(store.apply_failure().is_none());
}

#[test]
fn stale_generation_envelope_is_rejected_before_any_write() {
    let (store, _) = booted_store();
    let stale = envelope(&store, vec![upsert("messages", message("m1", "c1"))], vec![]);
    store.reset_incompatible().unwrap();
    let error = store.commit(stale).unwrap_err();
    assert!(matches!(error, StoreError::Envelope(EnvelopeFault::StaleGeneration)));
    assert_eq!(count(&store, "SELECT COUNT(*) FROM row_messages"), 0);
}

#[test]
fn empty_envelope_opens_no_transaction_and_keeps_revision() {
    let (store, _) = booted_store();
    let result = commit(&store, vec![], vec![]);
    assert!(result.is_noop());
    assert_eq!(store.revision_seq().unwrap(), 0);
}

#[test]
fn second_apply_failure_poisons_reads_until_reset() {
    let (store, _) = booted_store();
    exec(&store, "DROP TABLE row_messages");
    let error = store.commit(envelope(&store, vec![upsert("messages", message("m1", "c1"))], vec![])).unwrap_err();
    assert!(matches!(error, StoreError::Sqlite(_)));
    assert!(store.apply_failure().is_some());
    assert!(matches!(store.point("messages", "m1"), Err(StoreError::Poisoned(_))));
    assert!(matches!(
        store.commit(envelope(&store, vec![upsert("items", json!({"id": "i1"}))], vec![])),
        Err(StoreError::Poisoned(_))
    ));
    store.reset_incompatible().unwrap();
    assert!(store.apply_failure().is_none());
}

#[test]
fn commit_with_plans_against_the_snapshot_of_the_same_transaction() {
    let (store, _) = booted_store();
    commit(&store, vec![upsert("messages", json!({"id": "m1", "chatId": "c1", "body": "a"}))], vec![]);
    let result = store
        .commit_with(store.generation(), |reader| {
            let seen = reader.stored_row("messages", "m1").unwrap().unwrap();
            let mut envelope = CommitEnvelope::new(WriteOrigin::Patch, store_generation());
            envelope.entity_ops = vec![upsert("messages", json!({"id": "m1", "chatId": "c1", "body": format!("{}!", seen.row["body"].as_str().unwrap())}))];
            Ok::<_, ()>(Some(envelope))
        })
        .unwrap();
    assert_eq!(result.revision, Some(2));
    assert_eq!(store.point("messages", "m1").unwrap().unwrap()["body"], json!("a!"));
}

fn store_generation() -> crate::identity::Generation {
    crate::identity::Generation::INITIAL
}

#[test]
fn commit_with_plan_error_writes_nothing_and_none_is_a_noop() {
    let (store, _) = booted_store();
    let failed = store.commit_with(store.generation(), |_| Err::<Option<CommitEnvelope>, _>("bad plan"));
    assert!(matches!(failed, Err(PlanCommitError::Plan("bad plan"))));
    let noop = store.commit_with(store.generation(), |_| Ok::<Option<CommitEnvelope>, ()>(None)).unwrap();
    assert!(noop.is_noop());
    assert_eq!(store.revision_seq().unwrap(), 0);
}

#[test]
fn commit_with_rejects_stale_generation_before_planning() {
    let (store, _) = booted_store();
    let old = store.generation();
    store.reset_incompatible().unwrap();
    let mut planned = false;
    let outcome = store.commit_with(old, |_| {
        planned = true;
        Ok::<Option<CommitEnvelope>, ()>(None)
    });
    assert!(matches!(outcome, Err(PlanCommitError::Store(StoreError::Envelope(EnvelopeFault::StaleGeneration)))));
    assert!(!planned);
}

fn record(seq: Option<i64>, status: OperationStatus, dedupe: Option<&str>) -> OperationRecord {
    OperationRecord {
        seq,
        action_key: "messages:send".into(),
        status,
        row_ids: vec![id("temp-1")],
        temp_id: Some(id("temp-1")),
        input: json!({"body": "hi"}),
        transport_started: false,
        base_revision: None,
        created_at: 5,
        dedupe_key: dedupe.map(str::to_string),
        rollback: Some(json!({"v": 1, "kind": "patch"})),
        upload: None,
    }
}

fn transitions(store: &Store, items: Vec<OperationTransition>) -> crate::commit::CommitResult {
    let mut envelope = envelope(store, vec![], vec![]);
    envelope.operation_transitions = items;
    store.commit(envelope).unwrap()
}

#[test]
fn begin_writes_operation_row_with_assigned_seq() {
    let (store, _) = booted_store();
    let result = transitions(&store, vec![OperationTransition::Begin(record(None, OperationStatus::Pending, Some("k")))]);
    assert_eq!(result.operations.len(), 1);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op WHERE status = 'pending' AND dedupe_key = 'k'"), 1);
    assert_eq!(result.revision, Some(1));
}

#[test]
fn begin_with_existing_seq_replaces_the_row() {
    let (store, _) = booted_store();
    let first = transitions(&store, vec![OperationTransition::Begin(record(None, OperationStatus::Pending, None))]);
    let seq = *first.operations.iter().next().unwrap();
    transitions(&store, vec![OperationTransition::Begin(record(Some(seq), OperationStatus::Failed, Some("k")))]);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op"), 1);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op WHERE dedupe_key IS NULL AND status = 'failed'"), 1);
}

#[test]
fn closing_an_operation_follows_status_and_once_flag() {
    let cases = [
        ("committed with once", OperationStatus::Committed, true, "SELECT COUNT(*) FROM op", 0, 1),
        ("committed without once", OperationStatus::Committed, false, "SELECT COUNT(*) FROM op", 0, 0),
        ("failed", OperationStatus::Failed, false, "SELECT COUNT(*) FROM op WHERE status = 'failed' AND dedupe_key IS NULL", 1, 0),
        ("delivery unknown", OperationStatus::DeliveryUnknown, false, "SELECT COUNT(*) FROM op WHERE status = 'delivery_unknown' AND dedupe_key = 'k'", 1, 0),
    ];
    for (case, status, once, operations_sql, operations, once_keys) in cases {
        let (store, _) = booted_store();
        let started = transitions(&store, vec![OperationTransition::Begin(record(None, OperationStatus::Pending, Some("k")))]);
        let seq = *started.operations.iter().next().unwrap();
        transitions(&store, vec![OperationTransition::Close { seq, status, once }]);
        assert_eq!(count(&store, operations_sql), operations, "{case}: operations");
        assert_eq!(count(&store, "SELECT COUNT(*) FROM op_once WHERE namespace = 'messages:send' AND dedupe_key = 'k'"), once_keys, "{case}: once keys");
    }
}

#[test]
fn closing_unknown_operation_counts_ack_and_still_applies_the_envelope() {
    let (store, _) = booted_store();
    let mut mixed = envelope(&store, vec![upsert("messages", message("m1", "c1"))], vec![]);
    mixed.operation_transitions = vec![OperationTransition::Close { seq: 99, status: OperationStatus::Committed, once: false }];
    let result = store.commit(mixed).unwrap();
    assert_eq!(result.unknown_operation_acks, 1);
    assert_eq!(store.counters().unknown_operation_acks, 1);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM row_messages"), 1);
}

#[test]
fn remove_respects_expected_status() {
    let (store, _) = booted_store();
    let started = transitions(&store, vec![OperationTransition::Begin(record(None, OperationStatus::Pending, None))]);
    let seq = *started.operations.iter().next().unwrap();
    let wrong = transitions(&store, vec![OperationTransition::Remove { seq, expected: Some(OperationStatus::Failed) }]);
    assert_eq!(wrong.unknown_operation_acks, 1);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op"), 1);
    transitions(&store, vec![OperationTransition::Remove { seq, expected: Some(OperationStatus::Pending) }]);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op"), 0);
}

#[test]
fn open_dedupe_key_is_unique_and_failed_frees_it() {
    let (store, _) = booted_store();
    let first = transitions(&store, vec![OperationTransition::Begin(record(None, OperationStatus::Pending, Some("k")))]);
    let mut duplicate = envelope(&store, vec![], vec![]);
    duplicate.operation_transitions = vec![OperationTransition::Begin(record(None, OperationStatus::Pending, Some("k")))];
    assert!(matches!(store.commit(duplicate), Err(StoreError::Constraint(_))));
    let seq = *first.operations.iter().next().unwrap();
    transitions(&store, vec![OperationTransition::Close { seq, status: OperationStatus::Failed, once: false }]);
    transitions(&store, vec![OperationTransition::Begin(record(None, OperationStatus::Pending, Some("k")))]);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op"), 2);
}

#[test]
fn rows_and_operation_transition_commit_together() {
    let (store, _) = booted_store();
    let mut both = envelope(&store, vec![upsert("messages", message("temp-1", "c1"))], vec![]);
    both.operation_transitions = vec![OperationTransition::Begin(record(None, OperationStatus::Pending, None))];
    let result = store.commit(both).unwrap();
    assert_eq!(result.revision, Some(1));
    assert_eq!(count(&store, "SELECT COUNT(*) FROM row_messages"), 1);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op"), 1);
}

#[test]
fn destroy_prunes_tombstones_over_ttl_in_the_same_commit() {
    let (store, clock) = booted_store();
    store.mark_tombstone("messages", "old", 1, 0).unwrap();
    clock.advance(std::time::Duration::from_millis(100_000_000));
    commit(&store, vec![destroy("messages", "fresh", DestroyReason::Server)], vec![]);
    assert!(!store.is_tombstoned("messages", "old").unwrap());
    assert!(store.is_tombstoned("messages", "fresh").unwrap());
}

#[test]
fn a_replaced_destroy_is_reported_as_a_pair() {
    let (store, _) = booted_store();
    commit(&store, vec![upsert("messages", message("temp-1", "c1"))], vec![]);
    let result = commit(
        &store,
        vec![destroy("messages", "temp-1", DestroyReason::Replaced { by: id("m1") }), upsert("messages", message("m1", "c1"))],
        vec![],
    );
    assert_eq!(result.replaced, vec![(crate::identity::RowRef::new(model("messages"), id("temp-1")), id("m1"))]);
}

#[test]
fn the_commit_hook_runs_inside_the_commit_for_every_non_empty_result() {
    use crate::store::StoreEvent;
    use std::sync::{Arc, Mutex};
    let (store, _) = booted_store();
    let seen: Arc<Mutex<Vec<i64>>> = Arc::default();
    let sink = seen.clone();
    store.set_hook(Box::new(move |event| {
        if let StoreEvent::Committed(result) = event {
            sink.lock().unwrap().push(result.revision.unwrap());
        }
    }));
    commit(&store, vec![upsert("messages", message("m1", "c1"))], vec![]);
    commit(&store, vec![], vec![]);
    commit(&store, vec![upsert("messages", message("m1", "c1"))], vec![]);
    commit(&store, vec![upsert("messages", message("m2", "c1"))], vec![]);
    assert_eq!(*seen.lock().unwrap(), vec![1, 2]);
}

fn sync_row(store: &Store, id: &str) -> Option<String> {
    text(store, &format!("SELECT state || ':' || attempts FROM row_sync WHERE model = 'messages' AND row_id = '{id}'"))
}

fn synced_rows(result: &crate::commit::CommitResult) -> Vec<(String, RowChangeKind)> {
    result.rows.iter().map(|change| (change.row.id.to_string(), change.kind)).collect()
}

#[test]
fn begin_writes_row_sync_in_the_same_commit_and_reports_the_row_as_synced_when_it_has_no_entity_change() {
    let (store, _) = booted_store();
    let result = transitions(&store, vec![OperationTransition::Begin(record(None, OperationStatus::Pending, None))]);
    assert_eq!(sync_row(&store, "temp-1"), Some("pending:0".into()));
    assert_eq!(synced_rows(&result), [("temp-1".to_string(), RowChangeKind::Synced)]);
    assert!(result.scopes.is_empty());
}

#[test]
fn begin_with_an_entity_change_reports_the_row_once_with_its_own_kind() {
    let (store, _) = booted_store();
    let mut mixed = envelope(&store, vec![upsert("messages", message("temp-1", "c1"))], vec![]);
    mixed.operation_transitions = vec![OperationTransition::Begin(record(None, OperationStatus::Pending, None))];
    let result = store.commit(mixed).unwrap();
    assert_eq!(synced_rows(&result), [("temp-1".to_string(), RowChangeKind::Inserted)]);
    assert_eq!(sync_row(&store, "temp-1"), Some("pending:0".into()));
}

#[test]
fn start_fail_and_retry_rewrite_row_sync_and_each_commit_reports_the_row() {
    use crate::syncstate::{FailureKind, OperationFailure};
    let (store, _) = booted_store();
    let begun = transitions(&store, vec![OperationTransition::Begin(record(None, OperationStatus::Pending, None))]);
    let seq = *begun.operations.iter().next().unwrap();
    let started = transitions(&store, vec![OperationTransition::Start { seq }]);
    assert_eq!((sync_row(&store, "temp-1"), synced_rows(&started)), (Some("sending:1".into()), vec![("temp-1".to_string(), RowChangeKind::Synced)]));
    let failure = OperationFailure::new(FailureKind::Server, "http 503");
    let failed = transitions(&store, vec![OperationTransition::Fail { seq, status: OperationStatus::DeliveryUnknown, failure: failure.clone() }]);
    assert_eq!((sync_row(&store, "temp-1"), synced_rows(&failed).len()), (Some("delivery_unknown:1".into()), 1));
    assert_eq!(text(&store, "SELECT error_kind || ':' || error_message FROM row_sync WHERE row_id = 'temp-1'"), Some("server:http 503".to_string()));
    transitions(&store, vec![OperationTransition::Retry { seq }]);
    assert_eq!(sync_row(&store, "temp-1"), Some("pending:0".into()));
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op WHERE error_kind IS NULL AND transport_started = 0 AND status = 'pending'"), 1);
}

#[test]
fn start_only_applies_to_a_pending_operation_and_retry_only_to_a_stuck_one() {
    let (store, _) = booted_store();
    let begun = transitions(&store, vec![OperationTransition::Begin(record(None, OperationStatus::Pending, None))]);
    let seq = *begun.operations.iter().next().unwrap();
    let retried = transitions(&store, vec![OperationTransition::Retry { seq }]);
    assert!(retried.is_noop());
    transitions(&store, vec![OperationTransition::Start { seq }]);
    let twice = transitions(&store, vec![OperationTransition::Start { seq }]);
    assert!(twice.is_noop());
    assert_eq!(sync_row(&store, "temp-1"), Some("sending:1".into()));
    assert_eq!(store.counters().unknown_operation_acks, 2);
}

#[test]
fn closing_the_operation_removes_row_sync_and_reports_the_row() {
    let (store, _) = booted_store();
    let begun = transitions(&store, vec![OperationTransition::Begin(record(None, OperationStatus::Pending, None))]);
    let seq = *begun.operations.iter().next().unwrap();
    let closed = transitions(&store, vec![OperationTransition::Close { seq, status: OperationStatus::Committed, once: false }]);
    assert_eq!(sync_row(&store, "temp-1"), None);
    assert_eq!(synced_rows(&closed), [("temp-1".to_string(), RowChangeKind::Synced)]);
}

#[test]
fn retargeting_an_operation_moves_row_sync_to_the_new_id() {
    let (store, _) = booted_store();
    let begun = transitions(&store, vec![OperationTransition::Begin(record(None, OperationStatus::Pending, None))]);
    let seq = *begun.operations.iter().next().unwrap();
    let mut moved = record(Some(seq), OperationStatus::Pending, None);
    moved.row_ids = vec![id("m7")];
    moved.temp_id = None;
    let result = transitions(&store, vec![OperationTransition::Begin(moved)]);
    assert_eq!((sync_row(&store, "temp-1"), sync_row(&store, "m7")), (None, Some("pending:0".into())));
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn row_sync_change_reaches_the_scopes_that_hold_the_row() {
    let (store, _) = booted_store();
    commit(&store, vec![upsert("messages", message("m1", "c1"))], vec![append(&scope("messages", "thread"), "m1", "a")]);
    let mut moved = record(None, OperationStatus::Pending, None);
    moved.row_ids = vec![id("m1")];
    moved.temp_id = None;
    let result = transitions(&store, vec![OperationTransition::Begin(moved)]);
    assert_eq!(result.scopes, [scope("messages", "thread")].into());
}

#[test]
fn every_scalar_field_is_a_column_and_no_blob_holds_the_row() {
    let (store, _) = booted_store();
    let names: Vec<String> = {
        let inner = store.lock();
        let mut statement = inner.connection.prepare("SELECT name FROM pragma_table_info('row_messages')").unwrap();
        statement.query_map([], |row| row.get(0)).unwrap().map(Result::unwrap).collect()
    };
    for wanted in ["id", "revision", "born_rev", "chat_id", "body", "seen", "rev__chat_id", "rev__body", "rev__seen"] {
        assert!(names.iter().any(|name| name == wanted), "missing column {wanted} in {names:?}");
    }
    for gone in ["payload", "record_version", "field_rev"] {
        assert!(!names.iter().any(|name| name == gone), "column {gone} must not exist");
    }
}

#[test]
fn an_update_writes_only_the_changed_field_and_its_revision() {
    let (store, _) = booted_store();
    commit(&store, vec![upsert("messages", json!({"id": "m1", "chatId": "c1", "body": "b1"}))], vec![]);
    commit(&store, vec![upsert("messages", json!({"id": "m1", "chatId": "c1", "body": "b2"}))], vec![]);
    assert_eq!(text(&store, "SELECT body FROM row_messages WHERE id = 'm1'"), Some("b2".into()));
    assert_eq!(count(&store, "SELECT rev__body FROM row_messages WHERE id = 'm1'"), 2);
    assert_eq!(count(&store, "SELECT rev__chat_id FROM row_messages WHERE id = 'm1'"), 1);
    assert_eq!(count(&store, "SELECT revision FROM row_messages WHERE id = 'm1'"), 2);
}

#[test]
fn an_update_does_not_touch_the_columns_of_unchanged_fields() {
    let (store, _) = booted_store();
    commit(&store, vec![upsert("messages", json!({"id": "m1", "chatId": "c1", "body": "b1"}))], vec![]);
    exec(
        &store,
        "CREATE TEMP TABLE touched (column_name TEXT);
         CREATE TEMP TRIGGER touch_chat AFTER UPDATE OF chat_id ON row_messages BEGIN INSERT INTO touched VALUES ('chat_id'); END;
         CREATE TEMP TRIGGER touch_body AFTER UPDATE OF body ON row_messages BEGIN INSERT INTO touched VALUES ('body'); END;",
    );
    commit(&store, vec![upsert("messages", json!({"id": "m1", "chatId": "c1", "body": "b2"}))], vec![]);
    assert_eq!(text(&store, "SELECT group_concat(column_name) FROM touched"), Some("body".into()));
}
