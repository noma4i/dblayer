use serde_json::{Map, Value};

pub fn restore_patch(fields: &[String], previous: &Map<String, Value>) -> Map<String, Value> {
    fields.iter().map(|field| (field.clone(), previous.get(field).cloned().unwrap_or(Value::Null))).collect()
}
