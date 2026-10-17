use serde_json::{Map, Value};
use super::FieldSpec;

const MARK: &str = "$keyed";

pub fn is_blank(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::String(text) => text.is_empty(),
        Value::Array(items) => items.is_empty(),
        Value::Object(fields) => fields.is_empty(),
        _ => false,
    }
}

pub fn take(patch: &mut Map<String, Value>) -> Vec<(String, Map<String, Value>)> {
    let keyed: Vec<String> = patch.iter().filter(|(_, value)| value.get(MARK).is_some()).map(|(field, _)| field.clone()).collect();
    keyed
        .into_iter()
        .filter_map(|field| match patch.remove(&field)?.get(MARK)? {
            Value::Object(op) => Some((field, op.clone())),
            _ => None,
        })
        .collect()
}

fn resolved(value: &Value, args: &Map<String, Value>) -> Value {
    match value.get("$arg").and_then(Value::as_str) {
        Some(name) => args.get(name).cloned().unwrap_or(Value::Null),
        None => value.clone(),
    }
}

pub fn apply(field: &FieldSpec, current: Option<&Value>, op: &Map<String, Value>, args: &Map<String, Value>) -> Option<Value> {
    let stored_name = |name: &str| field.shared.iter().chain(&field.fields).find(|field| field.name == name).and_then(|field| field.from.clone()).unwrap_or_else(|| name.to_string());
    let by = stored_name(op.get("by")?.as_str()?);
    let element: Map<String, Value> = op.get("element")?.as_object()?.iter().map(|(name, value)| (stored_name(name), resolved(value, args))).collect();
    let key = element.get(&by)?;
    let mut items: Vec<Value> = current.and_then(Value::as_array).cloned().unwrap_or_default();
    let at = items.iter().position(|item| item.get(&by) == Some(key));
    let removes = element.iter().filter(|(name, _)| **name != by).all(|(_, value)| is_blank(value));
    match (at, removes) {
        (Some(index), true) => {
            items.remove(index);
        }
        (Some(index), false) => items[index] = Value::Object(element),
        (None, false) => items.push(Value::Object(element)),
        (None, true) => {}
    }
    Some(Value::Array(items))
}
