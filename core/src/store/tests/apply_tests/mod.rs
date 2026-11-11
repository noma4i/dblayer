use super::fixtures::*;
use super::*;
use crate::store::{
    DestroyReason, EntityOp, PlanFault, OperationRecord, OperationTransition, RowChangeKind, ScopeOp, ScopeRef,
};
use crate::owners::write::operation_status::OperationStatus;
use crate::value::sync::{FailureKind, OperationFailure};
use serde_json::json;
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};

fn commit(store: &Store, entity_ops: Vec<EntityOp>, scope_ops: Vec<ScopeOp>) -> crate::store::CommitResult {
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
fn nullable_relation_membership_moves_to_null_and_back() {
    use crate::schema::{Coverage, FieldSpec, ModelSpec, RelationSpec, RemoteShape};
    use crate::value::ScalarKind;
    let mut spec = ModelSpec::new("notes", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("folder", ScalarKind::Str).nullable().with_default(json!("a")).indexed()]);
    let mut relation = RelationSpec::new("folder", &["folder"]);
    relation.remote = Some(crate::schema_fixtures::remote("Notes", RemoteShape::List { coverage: Coverage::Complete }, "1.minute"));
    spec.relations.push(relation);
    let target = |value| ScopeRef::new(model("notes"), spec.selection_key("folder", &row(json!({"folder": value}))).unwrap());
    let (named, null) = (target(json!("a")), target(json!(null)));
    let clock = std::sync::Arc::new(crate::runtime::ManualClock::new(std::time::Duration::from_millis(1_000_000)));
    let store = Store::open_in_memory(clock).unwrap();
    store.boot(&[spec], None).unwrap();
    store.mark_ready();
    commit(&store, vec![upsert("notes", json!({"id":"n1", "folder":"a"}))], vec![append(&named, "n1", "a")]);
    for (value, expected_named, expected_null) in [(json!(null), 0, 1), (json!("a"), 1, 0)] {
        commit(&store, vec![upsert("notes", json!({"id":"n1", "folder":value}))], vec![]);
        assert_eq!(store.scope_members("notes", named.key.as_str()).unwrap().len(), expected_named);
        assert_eq!(store.scope_members("notes", null.key.as_str()).unwrap().len(), expected_null);
    }
}

#[test]
fn booleans_promote_to_integer_columns() {
    let (store, _) = booted_store();
    commit(&store, vec![upsert("messages", json!({"id": "m1", "chatId": "c1", "seen": true}))], vec![]);
    assert_eq!(count(&store, "SELECT seen FROM row_messages WHERE id = 'm1'"), 1);
}

#[test]
fn inv_row_03_identical_upsert_changes_nothing_and_keeps_revision() {
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
    let stored = store.read(&[], |reader| reader.stored_row("messages", "m1")).unwrap().unwrap();
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
    let stored = store.read(&[], |reader| reader.stored_row("messages", "m1")).unwrap().unwrap();
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
    assert_eq!(store.read(&[], |reader| reader.tombstone_seq("messages", "m1")).unwrap(), result.revision);
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
fn append_reuses_detached_order_key_in_either_operation_order() {
    for detach_first in [false, true] {
        let (store, _) = booted_store();
        let thread = scope("messages", "thread");
        commit(&store, vec![], vec![append(&thread, "old", "a"), append(&thread, "kept", "b")]);
        let mut operations = vec![
            append(&thread, "new", "a"),
            ScopeOp::Detach { scope: thread.clone(), row: id("old") },
        ];
        if detach_first { operations.reverse(); }
        let result = commit(&store, vec![], operations);
        assert_eq!(store.scope_members("messages", thread.key.as_str()).unwrap(), vec![("new".into(), "a".into()), ("kept".into(), "b".into())]);
        assert!(result.scopes.contains(&thread));
        assert!(result.members.contains(&crate::identity::RowRef::new(thread.model.clone(), id("old"))));
        assert!(result.members.contains(&crate::identity::RowRef::new(thread.model.clone(), id("new"))));
    }
}

#[test]
fn detach_removes_only_that_member() {
    let (store, _) = booted_store();
    let thread = scope("messages", "thread");
    commit(&store, vec![], vec![append(&thread, "m1", "a"), append(&thread, "m2", "b")]);
    commit(&store, vec![], vec![ScopeOp::Detach { scope: thread.clone(), row: id("m1") }]);
    assert_eq!(store.scope_members("messages", thread.key.as_str()).unwrap().len(), 1);
}

#[test]
fn inv_order_01_order_key_collision_rolls_back_whole_envelope() {
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
    store.erase_account(store.generation().next(), false).unwrap();
    let error = store.commit(stale).unwrap_err();
    assert!(matches!(error, StoreError::Plan(PlanFault::StaleGeneration)));
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
    let directory = tempfile::tempdir().unwrap();
    let clock = std::sync::Arc::new(crate::runtime::ManualClock::new(std::time::Duration::from_millis(1_000_000)));
    for store in [Store::open_in_memory(clock.clone()).unwrap(), Store::open(&directory.path().join("failure.sqlite"), clock.clone()).unwrap()] {
        store.boot(&[messages_spec(), items_spec()], None).unwrap();
        exec(&store, "DROP TABLE row_messages");
        let error = store.commit(envelope(&store, vec![upsert("messages", message("m1", "c1"))], vec![])).unwrap_err();
        assert!(matches!(error, StoreError::Sqlite(_)));
        assert!(store.apply_failure().is_some());
        for _ in 0..3 {
            assert!(matches!(store.point("messages", "m1"), Err(StoreError::Poisoned(_))));
        }
        assert!(matches!(
            store.commit(envelope(&store, vec![upsert("items", json!({"id": "i1"}))], vec![])),
            Err(StoreError::Poisoned(_))
        ));
        store.erase_account(store.generation().next(), false).unwrap();
        assert!(store.apply_failure().is_none());
        assert!(store.point("items", "i1").unwrap().is_none());
    }
}

mod apply_operations_tests;
