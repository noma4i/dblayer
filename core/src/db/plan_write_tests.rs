use serde_json::json;

use super::fixtures::*;
use crate::owners::store::StoreOp;

/// out_plan_update: an `update` plans only the fields that change, an update with the values the row has plans nothing.
#[test]
fn an_update_plans_the_changed_fields_only_and_an_equal_update_plans_nothing() {
    let db = booted();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    let planned = db.plan_update("moments", &rid("m1"), obj(json!({"text": "two", "userId": "u1"})), Default::default()).unwrap();
    assert!(!planned.noop);
    let values: serde_json::Value = serde_json::from_str(&planned.effects.rows[0].values).unwrap();
    assert_eq!(values["text"], "two");
    assert!(values.get("userId").is_none(), "an unchanged field is not an effect: {values}");
    let same = db.plan_update("moments", &rid("m1"), obj(json!({"text": "one"})), Default::default()).unwrap();
    assert!(same.noop && same.effects.rows.is_empty());
}

/// out_plan_destroy: a `destroy` plans the row and the rows that depend on it, the effects of the plan roll back as one operation.
#[test]
fn a_destroy_plans_the_dependents_and_its_effects_roll_back() {
    let db = booted();
    put(&db, "users", json!({"id": "u1", "name": "a"}));
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    let planned = db.plan_destroy("users", &rid("u1")).unwrap();
    let doomed: Vec<_> = planned.effects.tombstones.iter().map(|tombstone| (tombstone.model.as_str(), tombstone.row_id.as_str())).collect();
    assert_eq!(doomed, vec![("moments", "m1"), ("users", "u1")]);
    db.write_queue_ops(&[StoreOp::Intent { op_id: 1, model: "users".into(), row_id: "u1".into(), action: "destroy".into(), payload: planned.input.to_string(), effects: planned.effects }]).unwrap();
    assert!(db.find("users", &rid("u1")).unwrap().is_none() && db.find("moments", &rid("m1")).unwrap().is_none());
    db.write_queue_ops(&[StoreOp::RollBack { op_id: 1 }, StoreOp::RemoveIntent { op_id: 1 }]).unwrap();
    assert!(db.find("users", &rid("u1")).unwrap().is_some() && db.find("moments", &rid("m1")).unwrap().is_some(), "the rollback gives both rows back");
}

/// out_plan_create: a `create` of a server model plans a temporary row, a second create with the same id is refused.
#[test]
fn a_create_plans_a_temporary_row() {
    let db = booted();
    let planned = db.plan_create("moments", obj(json!({"userId": "u1", "text": "new"})), Default::default()).unwrap();
    assert!(planned.row_id.is_temp());
    assert_eq!(planned.effects.rows.len(), 1);
    db.write_queue_ops(&[StoreOp::Intent { op_id: 1, model: "moments".into(), row_id: planned.row_id.to_string(), action: "create".into(), payload: planned.input.to_string(), effects: planned.effects }]).unwrap();
    assert_eq!(db.find("moments", &planned.row_id).unwrap().unwrap().text("text"), Some("new"));
    let again = db.plan_create("moments", obj(json!({"id": planned.row_id.to_string(), "userId": "u1", "text": "x"})), Default::default());
    assert!(matches!(again, Err(crate::db::DbError::AlreadyExists { .. })));
}
