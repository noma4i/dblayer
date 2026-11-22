use crate::schema::{FieldKind, FieldSpec, ModelSpec};
use serde_json::{Map, Value};
use std::collections::BTreeMap;

pub(super) fn project(spec: &ModelSpec, row: &Map<String, Value>) -> BTreeMap<Vec<String>, Value> {
    let mut result = BTreeMap::new();
    for field in &spec.fields {
        if field.name == "id" || field.derived || matches!(field.kind, FieldKind::File) || spec.ephemeral_field(&field.name).is_some() { continue; }
        if let Some(value) = row.get(&field.name) { visit(field, value, vec![field.name.clone()], &mut result); }
    }
    result
}

fn visit(field: &FieldSpec, value: &Value, path: Vec<String>, result: &mut BTreeMap<Vec<String>, Value>) {
    if field.derived || matches!(field.kind, FieldKind::File) { return; }
    if matches!(field.kind, FieldKind::Object) && !field.fields.is_empty() {
        if let Some(object) = value.as_object().filter(|object| !object.is_empty()) {
            for child in &field.fields {
                let key = child.from.as_ref().unwrap_or(&child.name);
                if let Some(value) = object.get(key) {
                    let mut path = path.clone();
                    path.push(key.clone());
                    visit(child, value, path, result);
                }
            }
            return;
        }
    }
    result.insert(path, value.clone());
}
