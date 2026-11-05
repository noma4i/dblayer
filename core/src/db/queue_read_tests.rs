use serde_json::json;

use super::fixtures::*;
use super::Db;
use crate::owners::store::{EffectRow, IntentEffects, StoreOp};
use crate::owners::write::outbox::Origin;
use crate::schema::{ActionDeclaration, OnFailure, RootShape};

fn intent(op_id: u64, action: &str) -> StoreOp {
    let effects = IntentEffects { rows: vec![EffectRow { model: "moments".into(), row_id: "m1".into(), values: json!({"id": "m1", "text": "x"}).to_string() }], ..Default::default() };
    StoreOp::Intent { op_id, model: "moments".into(), row_id: "m1".into(), action: action.into(), payload: json!({"text": "x"}).to_string(), effects }
}

/// out_open_restores_pending, out_open_restores_failed (D-25): the path of a restored intent is the declared mutation of `(model, action)`.
#[test]
fn a_restored_intent_takes_its_path_from_the_declared_operation_of_its_action() {
    let mut moments = moments_spec();
    let mut publish = ActionDeclaration::new("publish", RootShape::Upsert);
    publish.mutation = Some("publishMoment".into());
    publish.on_failure = OnFailure::Keep;
    moments.actions.push(publish);
    let db = Db::open_in_memory(vec![users_spec(), moments, vibes_spec(), chats_spec(), messages_spec()], None).unwrap();
    put(&db, "moments", json!({"id": "m1", "text": "t0"}));
    let admitted = db.admit().unwrap();
    db.write_queue_ops(&admitted, &[intent(1, "publish")]).unwrap();
    db.write_queue_ops(&admitted, &[intent(2, "update")]).unwrap();
    let stored = db.read_intents().unwrap();
    assert_eq!(stored.len(), 2);
    let declared = &stored[0];
    assert_eq!((declared.op_id, declared.op.path.as_str(), declared.op.on_failure, declared.error_mark), (1, "/publishMoment", OnFailure::Keep, false));
    assert_eq!((declared.op.model.as_str(), declared.op.row_id.as_str(), declared.op.action.as_str(), declared.op.origin.clone()), ("moments", "m1", "publish", Origin::Api));
    assert_eq!(declared.op.body, json!({"text": "x"}).to_string());
    let unregistered = &stored[1];
    assert!(unregistered.error_mark && unregistered.op.path.is_empty(), "an action without a declared mutation comes back failed, not with a guessed path");
}
