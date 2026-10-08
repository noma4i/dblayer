use super::*;
use crate::commit::{CommitEnvelope, DestroyReason, EntityOp, ScopeOp, ScopeRef, WriteOrigin};
use crate::identity::{Generation, ModelKey, RowId, ScopeKey};
use serde_json::{json, Map, Value};
use std::collections::HashSet;

fn model(name: &str) -> ModelKey {
    ModelKey::new(name)
}

fn id(value: &str) -> RowId {
    RowId::new(value).unwrap()
}

fn object(value: Value) -> Map<String, Value> {
    value.as_object().unwrap().clone()
}

#[test]
fn new_plan_and_empty_mutations_add_no_intents() {
    let mut plan = WritePlan::new();
    assert!(plan.is_empty());
    assert!(plan.models().is_empty());
    plan.upsert(model("messages"), vec![]);
    plan.destroy(model("messages"), vec![]);
    plan.update(model("messages"), id("1"), Map::new());
    assert!(plan.is_empty());
}

#[test]
fn intents_collect_models_without_duplicates_in_order_of_appearance() {
    let mut plan = WritePlan::new();
    plan.upsert(model("messages"), vec![object(json!({"id": "1"}))]);
    plan.update(model("messages"), id("1"), object(json!({"read": true})));
    plan.destroy(model("chats"), vec![id("1")]);
    plan.invalidate(model("messages"), "thread");
    assert!(!plan.is_empty());
    assert_eq!(plan.intents().len(), 4);
    assert_eq!(plan.intents().last(), Some(&WriteIntent::Invalidate(RelationInvalidation { model: model("messages"), relation_name: "thread".into() })));
    assert_eq!(plan.models(), [model("messages"), model("chats")]);
}

#[test]
fn upsert_and_update_produce_entity_ops_and_rows_without_id_are_skipped() {
    let mut plan = WritePlan::new();
    plan.upsert(model("messages"), vec![object(json!({"body": "missing"})), object(json!({"id": "1"})), object(json!({"id": 2}))]);
    plan.update(model("messages"), id("7"), object(json!({"read": true})));
    let ops = entity_ops(&plan);
    assert_eq!(
        ops,
        [
            EntityOp::Upsert { model: model("messages"), id: id("1"), row: object(json!({"id": "1"})) },
            EntityOp::Upsert { model: model("messages"), id: id("2"), row: object(json!({"id": "2"})) },
            EntityOp::Upsert { model: model("messages"), id: id("7"), row: object(json!({"id": "7", "read": true})) },
        ]
    );
}

#[test]
fn destroy_produces_local_entity_ops_and_empty_plan_produces_none() {
    let mut plan = WritePlan::new();
    plan.destroy(model("messages"), vec![id("1"), id("2")]);
    assert_eq!(
        entity_ops(&plan),
        [
            EntityOp::Destroy { model: model("messages"), id: id("1"), reason: DestroyReason::Local },
            EntityOp::Destroy { model: model("messages"), id: id("2"), reason: DestroyReason::Local },
        ]
    );
    assert!(entity_ops(&WritePlan::new()).is_empty());
}

#[test]
fn envelope_merges_ops_per_row_and_validates() {
    let mut plan = WritePlan::new();
    plan.upsert(model("messages"), vec![object(json!({"id": "1", "body": "a"}))]);
    plan.update(model("messages"), id("1"), object(json!({"body": "b", "read": true})));
    plan.upsert(model("chats"), vec![object(json!({"id": "9"}))]);
    plan.destroy(model("chats"), vec![id("9")]);
    plan.invalidate(model("messages"), "thread");
    let envelope = plan.envelope(WriteOrigin::Event, Generation::INITIAL);
    assert_eq!(envelope.entity_ops.len(), 2);
    assert_eq!(
        envelope.entity_ops[0],
        EntityOp::Upsert { model: model("messages"), id: id("1"), row: object(json!({"id": "1", "body": "b", "read": true})) }
    );
    assert!(matches!(envelope.entity_ops[1], EntityOp::Destroy { .. }));
    assert!(envelope.validate(Generation::INITIAL).is_ok());
    assert_eq!(plan.invalidations(), [RelationInvalidation { model: model("messages"), relation_name: "thread".into() }]);
}

#[test]
fn rollback_restores_only_unchanged_fields_and_nulls_missing_ones() {
    let patched = object(json!({"body": "new", "kind": "text"}));
    let changed = object(json!({"body": "other", "kind": "text"}));
    let fields = vec!["body".to_string(), "kind".to_string()];
    assert_eq!(restorable_fields(&fields, &patched, &patched), fields);
    assert_eq!(restorable_fields(&fields, &changed, &patched), ["kind"]);
    let restored = restore_patch(&["body".to_string(), "gone".to_string()], &object(json!({"body": "old"})));
    assert_eq!(restored["body"], json!("old"));
    assert_eq!(restored["gone"], Value::Null);
}

#[test]
fn selections_expose_values_and_empty_ones_are_no_ops() {
    let none: RootSelection<&str> = RootSelection::None;
    let one = RootSelection::One("a");
    let many = RootSelection::Many(vec!["a", "b"]);
    let empty: RootSelection<&str> = RootSelection::Many(vec![]);
    assert!(none.values().is_empty() && none.is_noop());
    assert_eq!(none.planned_count(), 0);
    assert_eq!(one.values(), ["a"]);
    assert!(!one.is_noop());
    assert_eq!(many.planned_count(), 2);
    assert!(empty.is_noop());
    assert_eq!(empty.planned_count(), 0);
}

#[test]
fn same_owner_intent_is_rejected() {
    let owner = model("messages");
    assert_eq!(rejects_same_owner(&owner, &[model("chats"), model("messages")]), Some(PlanError::SameOwnerIntent(model("messages"))));
    assert_eq!(rejects_same_owner(&owner, &[model("chats")]), None);
    assert_eq!(rejects_same_owner(&owner, &[]), None);
}

#[test]
fn missing_row_outcome_follows_staleness_residency_and_relation() {
    let invalidating = MissingRowInput { row_exists: false, authoritative_relation: Some("thread".into()), is_stale_generation: false };
    let outcome = missing_row_outcome(&invalidating);
    assert_eq!(outcome, MissingRowOutcome::InvalidateAuthoritative);
    assert!(publishes_event(outcome));
    assert_eq!(invalidated_relations(&invalidating), ["thread"]);
    for relation in [None, Some(String::new())] {
        let input = MissingRowInput { row_exists: false, authoritative_relation: relation, is_stale_generation: false };
        let outcome = missing_row_outcome(&input);
        assert_eq!(outcome, MissingRowOutcome::NoOp);
        assert!(!publishes_event(outcome));
        assert!(invalidated_relations(&input).is_empty());
    }
    let stale = MissingRowInput { row_exists: true, authoritative_relation: None, is_stale_generation: true };
    assert_eq!(missing_row_outcome(&stale), MissingRowOutcome::StaleGeneration);
    assert!(!publishes_event(MissingRowOutcome::StaleGeneration));
    let present = MissingRowInput { row_exists: true, authoritative_relation: None, is_stale_generation: false };
    assert_eq!(missing_row_outcome(&present), MissingRowOutcome::Applied);
    assert!(publishes_event(MissingRowOutcome::Applied));
}

fn row_envelope(generation: Generation) -> CommitEnvelope {
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, generation);
    envelope.entity_ops.push(EntityOp::Upsert { model: model("message"), id: id("m1"), row: object(json!({"id": "m1"})) });
    envelope
}

#[test]
fn publishes_matching_envelope_unless_apply_failed_empty_or_stale() {
    let generation = Generation::INITIAL;
    let matching = row_envelope(generation);
    let empty = CommitEnvelope::new(WriteOrigin::Event, generation);
    assert!(publishes(&matching, generation, false));
    assert!(!publishes(&matching, generation, true));
    assert!(!publishes(&empty, generation, false));
    assert!(!publishes(&matching, generation.next(), false));
}

#[test]
fn affected_keys_filter_subscriptions_and_tick_count_is_one_per_envelope() {
    let generation = Generation::INITIAL;
    let rows = row_envelope(generation);
    let scope = ScopeRef::new(model("messages"), ScopeKey::compose(&["messages", "thread"]));
    let mut scoped = CommitEnvelope::new(WriteOrigin::Event, generation);
    scoped.scope_ops.push(ScopeOp::Append { scope: scope.clone(), row: id("m1"), order_key: "a".into() });
    let empty = CommitEnvelope::new(WriteOrigin::Event, generation);
    let subscribed: HashSet<String> = ["m1".to_string()].into();
    let none = HashSet::new();
    assert_eq!(affected_row_keys(&rows, &subscribed), ["m1"]);
    assert!(affected_row_keys(&rows, &none).is_empty());
    let scope_key = scope.key.as_str().to_string();
    assert_eq!(affected_scope_keys(&scoped, &[scope_key.clone()].into()), [scope_key.clone()]);
    assert!(affected_scope_keys(&scoped, &["other".to_string()].into()).is_empty());
    assert_eq!(tick_count(&rows, generation, &subscribed, &none), 1);
    assert_eq!(tick_count(&rows, generation, &none, &none), 0);
    assert_eq!(tick_count(&empty, generation, &subscribed, &none), 0);
    assert_eq!(tick_count(&scoped, generation, &none, &[scope_key].into()), 1);
}
