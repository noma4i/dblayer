use serde_json::json;

use super::fixtures::*;
use super::Db;
use crate::owners::store::{EffectRow, IntentEffects, StoreOp};
use crate::owners::write::outbox::OpSource;
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
    db.write_queue_ops(&[intent(1, "publish")]).unwrap();
    let stored = db.read_intents().unwrap();
    assert_eq!(stored.len(), 1);
    let declared = &stored[0];
    assert_eq!((declared.op_id, declared.op.path.as_str(), declared.op.on_failure, declared.error_mark), (1, "", OnFailure::Keep, false));
    assert_eq!((declared.op.model.as_str(), declared.op.row_id.as_str(), declared.op.action.as_str(), declared.op.origin.clone()), ("moments", "m1", "publish", OpSource::Api));
    assert_eq!(declared.op.body, json!({"text": "x"}).to_string());
    db.write_queue_ops(&[intent(2, "update")]).unwrap();
    let refused = db.read_intents().unwrap_err().to_string();
    assert!(refused.contains("moments.update") && refused.contains("declares no mutation"), "an action without a declared mutation is an error that names it: {refused}");
}

/// out_open_restores_dropped, out_open_orphan_answer: a dropped operation comes back as `Failed{RestoreFailed}` with its element, an answer without an operation is listed to be removed.
#[test]
fn a_dropped_operation_and_an_answer_without_an_operation_are_read_for_the_restore() {
    let db = Db::open_in_memory(vec![users_spec(), moments_spec(), vibes_spec(), chats_spec(), messages_spec()], None).unwrap();
    db.execute_for_test("INSERT INTO restore_failed VALUES ('op', 'k1', 'constraint', '{\"seq\":9,\"action_key\":\"moments:update\",\"temp_id\":\"t1\"}')");
    db.execute_for_test("INSERT INTO encrypted_answer (request_id, outcome, detail, operation_id) VALUES (41, 'ok', '{}', NULL), (42, 'ok', '{}', 77)");
    let restore = db.read_restore().unwrap();
    let ops = restore.ops.unwrap();
    assert_eq!(ops.len(), 1);
    assert_eq!((ops[0].op_id, ops[0].op.model.as_str(), ops[0].op.action.as_str(), ops[0].op.row_id.as_str()), (9, "moments", "update", "t1"));
    assert_eq!((ops[0].restore_failed.as_deref(), ops[0].restore_element.clone()), (Some("constraint"), Some(("op".to_string(), "k1".to_string()))));
    assert_eq!(restore.orphan_answers, vec![41, 42]);
}
