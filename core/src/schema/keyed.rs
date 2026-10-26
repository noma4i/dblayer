use serde_json::{Map, Value};
use super::{FieldKind, FieldSpec};

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

pub fn apply<E>(
    field: &FieldSpec,
    current: Option<&Value>,
    op: &Map<String, Value>,
    args: &Map<String, Value>,
    target_row: impl Fn(&str, &str) -> Result<Option<Map<String, Value>>, E>,
) -> Result<Option<Value>, E> {
    let declared = || field.shared.iter().chain(&field.fields);
    let stored_name = |name: &str| declared().find(|field| field.name == name).and_then(|field| field.from.clone()).unwrap_or_else(|| name.to_string());
    let Some(by) = op.get("by").and_then(Value::as_str).map(stored_name) else { return Ok(None) };
    let Some(spec) = op.get("element").and_then(Value::as_object) else { return Ok(None) };
    let mut element: Map<String, Value> = spec.iter().map(|(name, value)| (stored_name(name), resolved(value, args))).collect();
    let Some(key) = element.get(&by).cloned() else { return Ok(None) };
    let mut items: Vec<Value> = current.and_then(Value::as_array).cloned().unwrap_or_default();
    let at = items.iter().position(|item| item.get(&by) == Some(&key));
    let removes = element.iter().filter(|(name, _)| **name != by).all(|(_, value)| is_blank(value));
    if !removes {
        let missing: Vec<&FieldSpec> = declared().filter(|nested| nested.kind == FieldKind::Object && !element.contains_key(&stored_name(&nested.name))).collect();
        for nested in missing {
            let Some(id) = element.get(&stored_name(&format!("{}Id", nested.name))).and_then(Value::as_str).filter(|id| !id.is_empty()).map(str::to_string) else { continue };
            if let Some(row) = target_row(&nested.name, &id)? {
                element.insert(stored_name(&nested.name), Value::Object(snapshot(nested, &id, &row)));
            }
        }
    }
    match (at, removes) {
        (Some(index), true) => {
            items.remove(index);
        }
        (Some(index), false) => items[index] = Value::Object(element),
        (None, false) => items.push(Value::Object(element)),
        (None, true) => {}
    }
    Ok(Some(Value::Array(items)))
}

fn snapshot(nested: &FieldSpec, id: &str, row: &Map<String, Value>) -> Map<String, Value> {
    nested
        .fields
        .iter()
        .filter_map(|child| {
            let value = if child.name == "id" { Value::String(id.to_string()) } else { row.get(&child.name).filter(|value| !value.is_null())?.clone() };
            Some((child.from.clone().unwrap_or_else(|| child.name.clone()), value))
        })
        .collect()
}
