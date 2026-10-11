use super::*;
use crate::commit::{EntityOp, WriteOrigin};
use crate::identity::{Generation, ModelKey, RowId};
use serde_json::{json, Map, Value};

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
fn empty_mutations_add_no_intents() {
    let mut plan = WritePlan::new();
    plan.upsert(model("messages"), vec![]);
    plan.update(model("messages"), id("1"), Map::new());
    assert!(entity_ops(&plan).is_empty());
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
fn envelope_merges_ops_per_row_and_validates() {
    let mut plan = WritePlan::new();
    plan.upsert(model("messages"), vec![object(json!({"id": "1", "body": "a"}))]);
    plan.update(model("messages"), id("1"), object(json!({"body": "b", "read": true})));
    plan.upsert(model("chats"), vec![object(json!({"id": "9"}))]);
    let envelope = plan.envelope(WriteOrigin::Event, Generation::INITIAL);
    assert_eq!(envelope.entity_ops.len(), 2);
    assert_eq!(
        envelope.entity_ops[0],
        EntityOp::Upsert { model: model("messages"), id: id("1"), row: object(json!({"id": "1", "body": "b", "read": true})) }
    );
    assert!(matches!(envelope.entity_ops[1], EntityOp::Upsert { .. }));
    assert!(envelope.validate(Generation::INITIAL).is_ok());
}

#[test]
fn restore_patch_nulls_missing_fields() {
    let restored = restore_patch(&["body".to_string(), "gone".to_string()], &object(json!({"body": "old"})));
    assert_eq!(restored["body"], json!("old"));
    assert_eq!(restored["gone"], Value::Null);
}

