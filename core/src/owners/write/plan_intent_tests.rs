use super::plan_intent::*;
use serde_json::{json, Map, Value};

fn object(value: Value) -> Map<String, Value> {
    value.as_object().unwrap().clone()
}

#[test]
fn restore_patch_nulls_missing_fields() {
    let restored = restore_patch(&["body".to_string(), "gone".to_string()], &object(json!({"body": "old"})));
    assert_eq!(restored["body"], json!("old"));
    assert_eq!(restored["gone"], Value::Null);
}

