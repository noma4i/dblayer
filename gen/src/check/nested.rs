use super::*;
use dblayer::schema::{FieldKind, FieldSpec};
use dblayer::value::ScalarKind;
use serde_json::Value as Json;

fn fits(field: &FieldSpec, value: &Json, types: &[&FieldSpec]) -> bool {
    if value.is_null() { return field.nullable || field.optional; }
    match &field.kind {
        FieldKind::Scalar { kind } => match kind {
            ScalarKind::Bool => value.is_boolean(),
            ScalarKind::Int => value.as_i64().is_some(),
            ScalarKind::Num => value.as_f64().is_some(),
            ScalarKind::Id => value.is_string() || value.as_i64().is_some(),
            _ => value.is_string(),
        },
        FieldKind::Enum { values } => value.as_str().is_some_and(|text| values.iter().any(|candidate| candidate == text)),
        FieldKind::List { element } => value.as_array().is_some_and(|items| items.iter().all(|item| fits(&FieldSpec::scalar("", *element), item, types))),
        FieldKind::Object => value.as_object().is_some_and(|values| {
            if field.app_type.is_none() { return true; }
            let Some(target) = types.iter().find(|candidate| candidate.app_type == field.app_type) else { return false };
            target.fields.iter().all(|child| values.get(child.from.as_deref().unwrap_or(&child.name)).map_or(child.nullable || child.optional || child.default.is_some(), |value| fits(child, value, types)))
        }),
        FieldKind::Array => value.as_array().is_some_and(|items| field.app_type.is_none() || items.iter().all(|item| fits(&FieldSpec { kind: FieldKind::Object, ..field.clone() }, item, types))),
        FieldKind::Raw => true,
        FieldKind::Union | FieldKind::File => false,
    }
}

fn object_cycle(name: &str, types: &[&FieldSpec], seen: &mut Vec<String>) -> bool {
    if seen.iter().any(|prior| prior == name) { return true; }
    let Some(target) = types.iter().find(|field| field.app_type.as_deref() == Some(name)) else { return false };
    seen.push(name.to_string());
    let cycle = target.fields.iter().filter(|field| matches!(field.kind, FieldKind::Object | FieldKind::Union)).filter_map(|field| field.app_type.as_deref()).any(|child| object_cycle(child, types, seen));
    seen.pop();
    cycle
}

fn walk(calls: &[Call], fields: &[FieldSpec], file: usize, types: &[&FieldSpec], errors: &mut Vec<Found>, in_type: bool) {
    for call in calls {
        if let (true, Some(Node { item: Value::Const(typename), .. }), Some(block)) = (call.word == "variant", call.args.first(), &call.block) {
            if let Some(variant) = fields.iter().find(|field| field.app_type.as_deref() == Some(typename.as_str())) {
                walk(block, &variant.fields, file, types, errors, true);
            }
            continue;
        }
        let Some(name) = symbol_name(call.args.first()) else { continue };
        let Some(field) = fields.iter().find(|field| field.name == name) else { continue };
        if in_type {
            if let Some(pair) = call.options.iter().find(|pair| pair.key == "default") {
                if field.default.as_ref().is_some_and(|value| !fits(field, value, types)) {
                    errors.push(at(file, pair.value.line, pair.value.column, format!("nested field `{name}` default must match its type and required fields")));
                }
            }
        }
        if let Some(block) = &call.block {
            if object_cycle(field.app_type.as_deref().unwrap_or_default(), types, &mut Vec::new()) {
                errors.push(at(file, call.line, call.column, "recursive objects need an array between repeated types".to_string()));
            }
            walk(block, &field.fields, file, types, errors, true);
        }
    }
}

pub(super) fn check_defaults(nodes: &[ModelNode], models: &[ModelSpec], errors: &mut Vec<Found>) {
    let types = crate::emit::declared_types(models);
    for node in nodes {
        let Some(model) = models.iter().find(|model| model.key.as_str() == node.name) else { continue };
        for schema in node.body.iter().filter(|call| call.word == "schema") {
            walk(schema.block.as_deref().unwrap_or_default(), &model.fields, node.file, &types, errors, false);
        }
    }
}
