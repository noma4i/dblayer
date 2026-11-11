use super::*;
use crate::identity::{Generation, ListKey, ModelKey, RowId};
use serde_json::{Map, Value};
use serde_json::json;

fn row(id: &str) -> RowId {
    RowId::new(id).unwrap()
}

fn model() -> ModelKey {
    ModelKey::new("messages")
}

fn scope() -> ScopeRef {
    ScopeRef::new(model(), ListKey::compose(&["messages", "thread"]))
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

fn envelope() -> BatchPlan {
    BatchPlan::from_source(Generation::INITIAL, crate::schema::RowSource::Event)
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
    assert_eq!(valid.validate(Generation::INITIAL.next()), Err(PlanFault::StaleGeneration));
}

#[test]
fn entity_ops_conflict_only_on_the_same_model_and_id() {
    let other_model = EntityOp::Touch { model: ModelKey::new("chats"), id: row("m1") };
    let cases = [
        ("same model and id", touch("m1"), Err(PlanFault::ConflictingRowOps)),
        ("other id", touch("m2"), Ok(())),
        ("same id in other model", other_model, Ok(())),
    ];
    for (name, second, expected) in cases {
        let mut candidate = envelope();
        candidate.entity_ops = vec![upsert("m1"), second];
        assert_eq!(candidate.validate(Generation::INITIAL), expected, "{name}");
    }
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
    assert_eq!(duplicate.validate(Generation::INITIAL), Err(PlanFault::DuplicateScopeOps));
    assert_eq!(distinct.validate(Generation::INITIAL), Ok(()));
}

#[test]
fn upsert_payload_id_must_match_row_id() {
    let mut mismatch = envelope();
    mismatch.entity_ops = vec![EntityOp::Upsert { model: model(), id: row("m1"), row: payload("m2") }];
    assert_eq!(mismatch.validate(Generation::INITIAL), Err(PlanFault::RowIdMismatch));
}
