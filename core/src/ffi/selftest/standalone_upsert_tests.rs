//! D-485 M84: a standalone operation with `upsert Model, from:` lands the rows of its answer before the call answers.

use serde_json::{json, Value};

use super::rig_tests::{bound, Rig, World};
use crate::schema::{FieldSpec, ModelSpec};
use crate::value::ScalarKind;

fn viewer_model() -> ModelSpec {
    ModelSpec::new("Viewer", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("registered", ScalarKind::Bool).nullable().optional()])
}

fn viewer_load(model_key: &str) -> Value {
    let mut viewer = FieldSpec::object("viewer");
    viewer.nullable = true;
    viewer.app_type = Some("ViewerLoadResultViewer".into());
    viewer.fields = vec![FieldSpec::scalar("id", ScalarKind::Id).nullable(), FieldSpec::scalar("registered", ScalarKind::Bool).nullable()];
    let mut returns = FieldSpec::object("viewer_load");
    returns.app_type = Some("ViewerLoadResult".into());
    returns.fields = vec![viewer];
    json!([{"name": "ViewerLoad", "returns": returns, "lands": [{"model": model_key, "root": "viewerLoad.viewer"}]}])
}

/// D-485 M84: the call answers after the landing batch: the row of the answer is in the database the moment the call returns, and the call still gives the typed answer.
#[tokio::test(flavor = "multi_thread")]
async fn a_standalone_operation_lands_its_answer_rows() {
    let key = viewer_model().key.to_string();
    let rig = Rig::signed_with_operations(World::new("server", "A", true), false, |models| models.as_array_mut().unwrap().push(serde_json::to_value(viewer_model()).unwrap()), viewer_load(&key)).await;
    let mut operations: Vec<Value> = serde_json::from_str(&super::rig_models::plain_operations()).unwrap();
    operations.push(json!({"name": "ViewerLoad", "document": "query ViewerLoad { viewerLoad { viewer { id registered } } }"}));
    bound(rig.database.register_operations(serde_json::to_string(&operations).unwrap())).await.unwrap();
    rig.world.answer_with("ViewerLoad", json!({"data": {"viewerLoad": {"viewer": {"__typename": "Viewer", "id": "v1", "registered": true}}}}));
    let epoch = rig.database.session_epoch();
    let answer = bound(rig.database.call(epoch, "ViewerLoad".into(), "{}".into())).await.unwrap();
    let row = rig.database.peek_row_now(epoch, "Viewer".into(), "v1".into()).unwrap();
    let registered = row.as_ref().map(|row| super::flow_fixtures::cell(&rig, "Viewer", row, "registered"));
    assert_eq!(registered, Some(crate::ffi::wire::Cell::Bool { value: true }), "the answer row is landed before the call returns");
    let answer: Value = serde_json::from_str(&answer).unwrap();
    assert_eq!(answer["viewer"]["registered"], json!(true), "the call still gives the typed answer: {answer}");
    rig.database.close_now();
}
