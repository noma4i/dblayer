use super::*;
use serde_json::json;

fn row(id: &str) -> RowId {
    RowId::new(id).unwrap()
}

fn model() -> ModelKey {
    ModelKey::new("messages")
}

fn scope() -> ScopeRef {
    ScopeRef::new(model(), ScopeKey::compose(&["messages", "thread"]))
}

fn payload(id: &str) -> Map<String, Value> {
    json!({"id": id}).as_object().unwrap().clone()
}

fn upsert(id: &str) -> EntityOp {
    EntityOp::Upsert { model: model(), id: row(id), row: payload(id) }
}

fn touch(id: &str) -> EntityOp {
    EntityOp::Touch { model: model(), id: row(id) }
}

fn envelope() -> CommitEnvelope {
    CommitEnvelope::new(WriteOrigin::Event, Generation::INITIAL)
}

#[test]
fn envelope_with_any_op_is_not_empty() {
    let mut scoped = envelope();
    scoped.scope_ops.push(ScopeOp::Append { scope: scope(), row: row("m1"), order_key: "a".into() });
    let mut transitioned = envelope();
    transitioned.operation_transitions.push(OperationTransition::Remove { seq: 1, expected: None });
    assert!(!scoped.is_empty());
    assert!(!transitioned.is_empty());
}

#[test]
fn generation_fence_rejects_other_generation() {
    let valid = envelope();
    assert_eq!(valid.validate(Generation::INITIAL), Ok(()));
    assert_eq!(valid.validate(Generation::INITIAL.next()), Err(EnvelopeFault::StaleGeneration));
}

#[test]
fn schema_version_mismatch_is_reported_first() {
    let mut stale = envelope();
    stale.schema_version = ENVELOPE_VERSION + 1;
    assert_eq!(stale.validate(Generation::INITIAL.next()), Err(EnvelopeFault::SchemaVersionMismatch));
}

#[test]
fn several_entity_ops_for_one_row_conflict() {
    let mut conflicting = envelope();
    conflicting.entity_ops = vec![upsert("m1"), touch("m1")];
    let mut distinct = envelope();
    distinct.entity_ops = vec![upsert("m1"), touch("m2")];
    assert_eq!(conflicting.validate(Generation::INITIAL), Err(EnvelopeFault::ConflictingRowOps));
    assert_eq!(distinct.validate(Generation::INITIAL), Ok(()));
}

#[test]
fn same_id_in_other_model_is_not_a_conflict() {
    let mut other = envelope();
    other.entity_ops = vec![upsert("m1"), EntityOp::Touch { model: ModelKey::new("chats"), id: row("m1") }];
    assert_eq!(other.validate(Generation::INITIAL), Ok(()));
}

#[test]
fn several_member_scope_ops_for_one_row_are_duplicates() {
    let mut duplicate = envelope();
    duplicate.scope_ops = vec![
        ScopeOp::Append { scope: scope(), row: row("m1"), order_key: "a".into() },
        ScopeOp::Move { scope: scope(), row: row("m1"), order_key: "b".into() },
    ];
    let mut distinct = envelope();
    distinct.scope_ops = vec![
        ScopeOp::Append { scope: scope(), row: row("m1"), order_key: "a".into() },
        ScopeOp::Move { scope: scope(), row: row("m2"), order_key: "b".into() },
    ];
    assert_eq!(duplicate.validate(Generation::INITIAL), Err(EnvelopeFault::DuplicateScopeOps));
    assert_eq!(distinct.validate(Generation::INITIAL), Ok(()));
}

#[test]
fn replace_all_may_precede_member_op_on_same_row() {
    let mut mixed = envelope();
    mixed.scope_ops = vec![
        ScopeOp::ReplaceAll { scope: scope(), members: vec![(row("m1"), "a".into())] },
        ScopeOp::Detach { scope: scope(), row: row("m1") },
    ];
    assert_eq!(mixed.validate(Generation::INITIAL), Ok(()));
}

#[test]
fn upsert_payload_id_must_match_row_id() {
    let mut mismatch = envelope();
    mismatch.entity_ops = vec![EntityOp::Upsert { model: model(), id: row("m1"), row: payload("m2") }];
    assert_eq!(mismatch.validate(Generation::INITIAL), Err(EnvelopeFault::RowIdMismatch));
}

#[test]
fn touched_rows_and_scopes_keep_first_occurrence() {
    let mut touched = envelope();
    touched.entity_ops = vec![upsert("m1"), touch("m1"), touch("m2")];
    touched.scope_ops = vec![
        ScopeOp::Append { scope: scope(), row: row("m1"), order_key: "a".into() },
        ScopeOp::Move { scope: scope(), row: row("m2"), order_key: "b".into() },
    ];
    let rows: Vec<String> = touched.touched_rows().iter().map(|row| row.id.to_string()).collect();
    assert_eq!(rows, vec!["m1", "m2"]);
    assert_eq!(touched.touched_scopes(), vec![scope()]);
}

#[test]
fn operation_status_round_trips_through_sql_text() {
    for status in [
        OperationStatus::Pending,
        OperationStatus::Failed,
        OperationStatus::DeliveryUnknown,
        OperationStatus::Committed,
        OperationStatus::RolledBack,
    ] {
        assert_eq!(OperationStatus::from_sql(status.as_sql()), Some(status));
    }
    assert_eq!(OperationStatus::from_sql("nope"), None);
    assert!(OperationStatus::Committed.is_terminal());
    assert!(!OperationStatus::Failed.is_terminal());
}
