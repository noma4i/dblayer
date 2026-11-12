use serde_json::{json, Value};

use super::flow_fixtures::cell;
use super::rig_tests::{bound, Rig, World};
use crate::ffi::wire::Cell;

pub(super) fn wrapper_models(models: &mut Value) { *models = serde_json::from_str(include_str!("wrapper_models.inc")).unwrap(); }

const SCREEN_NODE: &str = "query ScreenNode($id: ID!) { node(id: $id) { ... on CustomScreen { id slug version requires { ios android } spec { root state { path value } elements { id type slot children padding margin background radius expand align accessibilityLabel props visible press change } } } } }";

fn element(value: Value) -> Value {
    let mut fields = value.as_object().unwrap().clone();
    for key in ["props", "visible", "press", "change"] {
        if let Some(inner) = fields.get(key) {
            let text = inner.to_string();
            fields.insert(key.into(), Value::String(text));
        }
    }
    Value::Object(fields)
}

fn vectors() -> [Value; 2] {
    let props = json!({"nested":{"values":[1,false,null,{"deep":["x",2.5]}]},"empty":{},"array":[]});
    [
        json!({"root":"root","state":[],"elements":[{"id":"root","type":"unlisted-future-kind","children":["child"],"props":props,"press":[],"change":[]}]}),
        json!({"root":"root","state":[{"path":"value","value":"\"Ada\""},{"path":"count","value":"3.0"}],"elements":[{"id":"root","type":"k","children":["child"],"slot":"content","expand":true,"visible":{"$and":[true,{"$state":"count","gt":2,"not":false}]},"press":[{"action":"navigate","when":{"$or":[false,{"$state":"enabled","not":true}]},"params":{"a":[1,{"b":false}]}},{"action":"close","when":false}]}]}),
    ]
}

fn answer(extra: Value) -> Value {
    let mut row = json!({"__typename":"CustomScreen","id":"sample","slug":"sample","version":1,"requires":{"ios":2,"android":null}});
    row.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
    json!({"data":{"node":row}})
}

async fn stored(index: usize) {
    let rig = Rig::signed_as(World::new("server", "A", true), false, wrapper_models).await;
    let mut spec = vectors()[index].clone();
    let elements: Vec<Value> = spec["elements"].as_array().unwrap().iter().cloned().map(element).collect();
    spec["elements"] = Value::Array(elements);
    rig.database.register_operations(super::rig_models::with_own_operations(&json!([{"name":"ScreenNode","document":SCREEN_NODE}]).to_string())).await.unwrap();
    rig.world.answer_with("ScreenNode", answer(json!({"spec": spec})));
    let epoch = rig.database.session_epoch();
    let row = bound(rig.database.node(epoch, "CustomScreen".into(), "node".into(), "sample".into(), false)).await.unwrap().expect("the screen was not stored");
    let saved = cell(&rig, "CustomScreen", &row, "spec");
    assert_ne!(saved, Cell::Null, "the spec of the screen was not stored");
    rig.world.answer_with("ScreenNode", answer(json!({"slug":"updated"})));
    let kept = bound(rig.database.node(epoch, "CustomScreen".into(), "node".into(), "sample".into(), true)).await.unwrap().unwrap();
    assert_eq!(cell(&rig, "CustomScreen", &kept, "spec"), saved);
    rig.database.close_now();
}

fn without_unknown_keys(value: Value) -> Value {
    match value {
        Value::Object(fields) => Value::Object(fields.into_iter().filter(|(key, _)| key != "unknown").map(|(key, inner)| (key, without_unknown_keys(inner))).collect()),
        Value::Array(items) => Value::Array(items.into_iter().map(without_unknown_keys).collect()),
        other => other,
    }
}

async fn stored_value(rig: &Rig, epoch: u64, value: Value) -> Cell {
    let values = json!({ "value": value }).to_string();
    let row = rig.database.create(epoch, "NestedSample".into(), values, "{}".into()).await.unwrap().optimistic_value.unwrap();
    cell(rig, "NestedSample", &row, "value")
}

#[tokio::test(flavor = "multi_thread")]
async fn nested_shared_cases_round_trip() {
    let rig = Rig::signed_as(World::new("server", "A", true), false, wrapper_models).await;
    let epoch = rig.database.session_epoch();
    let cases: Vec<Value> = serde_json::from_str(include_str!("../../../../fixtures/nested.json")).unwrap();
    for case in &cases {
        let fields = case.as_object().unwrap();
        if fields.get("error") == Some(&Value::Bool(true)) { continue; }
        let (Some(input), Some(output)) = (fields.get("input"), fields.get("output")) else { continue };
        let label = fields["name"].as_str().unwrap();
        let stored = stored_value(&rig, epoch, without_unknown_keys(input.clone())).await;
        let expected = stored_value(&rig, epoch, output.clone()).await;
        assert_eq!(stored, expected, "{label}");
    }
    rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_screen_spec_with_unknown_element_types_props_and_ordered_actions_is_stored_and_kept() {
    stored(0).await;
    stored(1).await;
}
