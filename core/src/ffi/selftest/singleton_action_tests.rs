//! D-480: an action of a singleton model is called on the type, with or without a stored row; the answer writes the row.

use serde_json::{json, Value};

use super::rig_tests::{bound, Rig, World};
use crate::schema::{ActionDeclaration, FieldSpec, ModelSpec, RelationSpec, RemoteShape, RootShape};

/// A singleton `Viewer` read by `ViewerMe`, with the action `complete` whose answer is the viewer.
fn viewer(models: &mut Value) {
    let mut viewer = ModelSpec::new("Viewer", vec![FieldSpec::scalar("id", crate::value::ScalarKind::Id), FieldSpec::scalar("registered", crate::value::ScalarKind::Bool).nullable().optional()]);
    let mut me = RelationSpec::global("me");
    me.remote = Some(crate::schema_fixtures::remote("ViewerMe", RemoteShape::Single, "5.minutes"));
    viewer.relations.push(me);
    let mut complete = ActionDeclaration::new("complete", RootShape::Upsert);
    complete.mutation = Some("ViewerComplete".into());
    complete.root = Some("viewerComplete.viewer".into());
    viewer.actions.push(complete);
    models.as_array_mut().unwrap().push(serde_json::to_value(viewer).unwrap());
}

async fn rig() -> Rig {
    let rig = Rig::signed_as(World::new("server", "A", true), false, viewer).await;
    let mut operations: Vec<Value> = serde_json::from_str(&super::rig_models::plain_operations()).unwrap();
    operations.push(json!({"name": "ViewerMe", "document": "query ViewerMe { viewer { id registered } }"}));
    operations.push(json!({"name": "ViewerComplete", "document": "mutation ViewerComplete { viewerComplete { viewer { id registered } } }"}));
    bound(rig.database.register_operations(serde_json::to_string(&operations).unwrap())).await.unwrap();
    rig.world.answer_with("ViewerComplete", json!({"data": {"viewerComplete": {"viewer": {"__typename": "Viewer", "id": "v1", "registered": true}}}}));
    rig
}

/// D-480: `act` on the singleton id sends the mutation once and the answer writes the row, whether the row was stored before or not.
#[tokio::test(flavor = "multi_thread")]
async fn a_singleton_action_runs_on_the_type_and_its_answer_writes_the_row() {
    let mut missed = Vec::new();
    for stored_before in [false, true] {
        let rig = rig().await;
        let epoch = rig.database.session_epoch();
        if stored_before {
            rig.world.answer_with("ViewerMe", json!({"data": {"viewer": {"__typename": "Viewer", "id": "v1", "registered": false}}}));
            bound(rig.database.refresh(epoch, "Viewer".into(), Some("me".into()), "{}".into())).await.unwrap();
        }
        let receipt = bound(rig.database.act(epoch, "Viewer".into(), crate::schema::SINGLETON.into(), "complete".into(), "{}".into(), "{}".into())).await;
        let Ok(receipt) = receipt else { missed.push(format!("stored before {stored_before}: the call failed: {receipt:?}")); rig.database.close_now(); continue };
        if let Err(error) = bound(rig.database.confirmed(receipt, false)).await { missed.push(format!("stored before {stored_before}: the receipt ended with {error:?}")); }
        rig.settle().await;
        let row = rig.database.peek_row_now(epoch, "Viewer".into(), "v1".into()).unwrap();
        let registered = row.as_ref().map(|row| super::flow_fixtures::cell(&rig, "Viewer", row, "registered"));
        if registered != Some(crate::ffi::wire::Cell::Bool { value: true }) { missed.push(format!("stored before {stored_before}: the row after the answer is {registered:?}")); }
        if rig.world.count("ViewerComplete") != 1 { missed.push(format!("stored before {stored_before}: {} mutations sent", rig.world.count("ViewerComplete"))); }
        rig.database.close_now();
    }
    assert!(missed.is_empty(), "{missed:#?}");
}
