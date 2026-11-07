use serde_json::{Map, Value};

use crate::schema::{FieldKind, FieldSpec};
use crate::value::{read_enum, read_scalar};

const TYPENAME: &str = "__typename";

pub(super) fn normalize(root: &str, declaration: &str) -> Result<String, String> {
    let declared: FieldSpec = serde_json::from_str(declaration).map_err(|error| format!("the returns declaration is invalid: {error}"))?;
    if matches!(declared.kind, FieldKind::Raw) { return Ok(root.to_string()); }
    let body: Value = serde_json::from_str(root).map_err(|error| format!("the answer is not JSON: {error}"))?;
    let name = declared.name.as_str();
    let value = match &body {
        Value::Array(items) => Value::Array(items.iter().map(|item| fields(item, declared.fields.iter(), name)).collect::<Result<_, _>>()?),
        other => fields(other, declared.fields.iter(), name)?,
    };
    Ok(value.to_string())
}

fn fields<'a>(value: &Value, declared: impl Iterator<Item = &'a FieldSpec>, path: &str) -> Result<Value, String> {
    let Value::Object(source) = value else { return Err(format!("{path}: an object is expected")) };
    let read_field = |field: &FieldSpec| Ok((field.name.clone(), read(source.get(&field.name).unwrap_or(&Value::Null), field, &format!("{path}.{}", field.name))?));
    declared.map(read_field).collect::<Result<Map<_, _>, String>>().map(Value::Object)
}

fn read(value: &Value, field: &FieldSpec, path: &str) -> Result<Value, String> {
    if value.is_null() { return Ok(Value::Null); }
    let mismatch = || format!("{path}: the value does not match the declaration");
    match &field.kind {
        FieldKind::Scalar { kind } => read_scalar(value, *kind).ok_or_else(mismatch),
        FieldKind::Enum { .. } => read_enum(value).map(Value::String).ok_or_else(mismatch),
        FieldKind::Raw => Ok(value.clone()),
        FieldKind::Object => fields(value, field.fields.iter(), path),
        FieldKind::Array => items(value, path, |item, at| fields(item, field.fields.iter(), at)),
        FieldKind::List { element } => items(value, path, |item, at| read_scalar(item, *element).ok_or_else(|| format!("{at}: the value does not match the declaration"))),
        FieldKind::Union => union(value, field, path),
        FieldKind::UnionArray => items(value, path, |item, at| union(item, field, at)),
        FieldKind::File => Err(mismatch()),
    }
}

fn items(value: &Value, path: &str, read_item: impl Fn(&Value, &str) -> Result<Value, String>) -> Result<Value, String> {
    let Value::Array(list) = value else { return Err(format!("{path}: a list is expected")) };
    list.iter().enumerate().map(|(at, item)| read_item(item, &format!("{path}[{at}]"))).collect::<Result<_, _>>().map(Value::Array)
}

fn union(value: &Value, field: &FieldSpec, path: &str) -> Result<Value, String> {
    let Some(typename) = value.get(TYPENAME).and_then(Value::as_str) else { return Err(format!("{path}: the union value has no {TYPENAME}")) };
    let mut tagged = Map::from_iter([(TYPENAME.to_string(), Value::String(typename.to_string()))]);
    if let Some(variant) = field.fields.iter().find(|variant| variant.app_type.as_deref() == Some(typename)) {
        if let Value::Object(members) = fields(value, field.shared.iter().chain(&variant.fields), path)? { tagged.extend(members); }
    }
    Ok(Value::Object(tagged))
}
