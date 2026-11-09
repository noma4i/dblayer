use dblayer::schema::{FieldKind, FieldSpec, ModelSpec};
use dblayer::value::ScalarKind;
use serde_json::Value as Json;

use super::{at, Found};
use crate::decl::{AttributeBlock, AttributeDecl, ModelDecl};

fn fits(field: &FieldSpec, value: &Json, types: &[&FieldSpec]) -> bool {
    if value.is_null() { return crate::emit::is_nullable(field); }
    match &field.kind {
        FieldKind::Scalar { kind } => match kind {
            ScalarKind::Bool => value.is_boolean(),
            ScalarKind::Int => value.as_i64().is_some(),
            ScalarKind::Num => value.as_f64().is_some(),
            ScalarKind::Id => value.is_string() || value.as_i64().is_some(),
            _ => value.is_string(),
        },
        FieldKind::Enum { values } => value.as_str().is_some_and(|text| values.iter().any(|candidate| candidate == text)),
        FieldKind::List { .. } if !field.values.is_empty() => value.as_array().is_some_and(|items| items.iter().all(|item| item.as_str().is_some_and(|text| field.values.iter().any(|candidate| candidate == text)))),
        FieldKind::List { element } => value.as_array().is_some_and(|items| items.iter().all(|item| fits(&FieldSpec::scalar("", *element), item, types))),
        FieldKind::Object => value.as_object().is_some_and(|values| {
            if field.app_type.is_none() { return true; }
            let Some(target) = types.iter().find(|candidate| candidate.app_type == field.app_type) else { return false };
            target.fields.iter().all(|child| match values.get(child.from.as_deref().unwrap_or(&child.name)) {
                Some(value) => fits(child, value, types),
                None => crate::emit::is_nullable(child) || child.default.is_some(),
            })
        }),
        FieldKind::Array => value.as_array().is_some_and(|items| field.app_type.is_none() || items.iter().all(|item| fits(&FieldSpec { kind: FieldKind::Object, ..field.clone() }, item, types))),
        FieldKind::Raw => true,
        FieldKind::Union | FieldKind::UnionArray | FieldKind::File => false,
    }
}

fn object_cycle(name: &str, types: &[&FieldSpec], seen: &mut Vec<String>) -> bool {
    if seen.iter().any(|prior| prior == name) { return true; }
    let Some(target) = types.iter().find(|field| field.app_type.as_deref() == Some(name)) else { return false };
    seen.push(name.to_string());
    let cycle = target.fields.iter().filter(|field| matches!(field.kind, FieldKind::Object | FieldKind::Union)).flat_map(|field| field.app_type.as_deref()).any(|child| object_cycle(child, types, seen));
    seen.pop();
    cycle
}

fn walk(attributes: &[AttributeDecl], fields: &[FieldSpec], file: usize, types: &[&FieldSpec], errors: &mut Vec<Found>, in_type: bool) {
    for attribute in attributes {
        let Some(field) = fields.iter().find(|field| field.name == attribute.name) else { continue };
        let enumerated = matches!(field.kind, FieldKind::Enum { .. }) || !field.values.is_empty();
        if let Some(default) = attribute.default.as_ref().filter(|_| in_type || enumerated) {
            if field.default.as_ref().is_some_and(|value| !fits(field, value, types)) {
                let owner = if in_type { "nested attribute" } else { "attribute" };
                errors.push(at(file, default.pos, format!("{owner} `{}` default must match its type and required fields", attribute.name)));
            }
        }
        let recursive = || field.app_type.as_deref().is_some_and(|name| object_cycle(name, types, &mut Vec::new()));
        match &attribute.block {
            AttributeBlock::None => {}
            AttributeBlock::Fields(inner) => {
                if recursive() {
                    errors.push(at(file, attribute.pos, "recursive objects need an array between repeated types"));
                }
                walk(inner, &field.fields, file, types, errors, true);
            }
            AttributeBlock::Union { shared, variants } => {
                if recursive() {
                    errors.push(at(file, attribute.pos, "recursive objects need an array between repeated types"));
                }
                for variant in variants {
                    let Some(inner) = &variant.fields else { continue };
                    if let Some(spec) = field.fields.iter().find(|candidate| candidate.app_type.as_deref() == Some(variant.type_name.as_str())) {
                        walk(inner, &spec.fields, file, types, errors, true);
                    }
                }
                if let Some(shared) = shared {
                    walk(&shared.item, &field.shared, file, types, errors, true);
                }
            }
        }
    }
}

pub(super) fn check_defaults(nodes: &[ModelDecl], models: &[ModelSpec], errors: &mut Vec<Found>) {
    let types = crate::emit::declared_types(models);
    for node in nodes {
        let Some(model) = models.iter().find(|model| model.key.as_str() == node.name) else { continue };
        walk(&node.attributes, &model.fields, node.file, &types, errors, false);
    }
}
