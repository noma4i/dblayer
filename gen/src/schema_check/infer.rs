use std::collections::HashMap;

use super::graphql::{Operation, Operations, Schema};
use crate::decl::{CallDecl, ModelDecl, OperationDecl, OperationKind, Pos, VariableBinding, VariableValue};
use crate::naming::lower_first;

pub(super) type Failure = (usize, Pos, String);

/// The one document variable that carries the body: an input object the root field takes.
fn wrapped(schema: &Schema, operation: &Operation) -> Option<String> {
    let root = operation.root.as_ref()?;
    let mut variables = operation.variables.iter();
    let (name, ty) = variables.next().filter(|_| variables.next().is_none())?;
    (!ty.is_list() && schema.input_fields(ty.name()).is_some() && root.takes.contains(name)).then(|| name.clone())
}

/// The fields the body of the operation sends: the fields of the wrapping input object, or the document variables.
pub(super) fn input_of(schema: &Schema, operation: &Operation, wrap: Option<&str>) -> HashMap<String, super::Ty> {
    match wrap {
        Some(key) => schema.input_fields(operation.variables[key].name()).expect("a wrapped operation names an input object variable"),
        None => operation.variables.clone(),
    }
}

/// M64 and M80: `wrap` and the binding of every input field come from the operation document.
pub(super) fn infer(schema: &Schema, documents: &Operations, models: &mut [ModelDecl], calls: &mut [CallDecl]) -> Vec<Failure> {
    let mut failures = Vec::new();
    for node in models.iter_mut().filter(|node| node.storage != dblayer::schema::Storage::Local) {
        let sources: Vec<String> = node.attributes.iter().filter(|attribute| !attribute.local && !attribute.derived).map(|attribute| attribute.name.clone()).collect();
        let owner_id = format!("{}Id", lower_first(&node.name));
        for operation in &mut node.operations {
            let Some(name) = operation.mutation.as_ref().map(|found| found.item.clone()) else { continue };
            let Some(document) = documents.named.get(&name).filter(|document| document.kind == "mutation") else {
                failures.push((node.file, operation.pos, format!("operation `{name}` is not in the operation documents")));
                continue;
            };
            operation.wrap = wrapped(schema, document);
            if operation.upload.is_none() {
                bind(schema, document, operation, (&sources, &owner_id), &mut |pos, message| failures.push((node.file, pos, message)));
            }
        }
    }
    for call in calls.iter_mut() {
        let Some(document) = call.kind.as_ref().and_then(|(_, name, _)| documents.named.get(name)) else { continue };
        call.wrap = wrapped(schema, document);
    }
    failures
}

fn bind(schema: &Schema, document: &Operation, operation: &mut OperationDecl, (attributes, owner_id): (&[String], &str), fail: &mut dyn FnMut(Pos, String)) {
    let wrap = operation.wrap.as_deref();
    let fields = input_of(schema, document, wrap);
    let owner = match wrap {
        None => "the variables of the document".to_string(),
        Some(key) => format!("`{}`", document.variables[key].name()),
    };
    let mut bound: Vec<VariableBinding> = Vec::new();
    for binding in operation.variables.take().into_iter().flatten() {
        if fields.contains_key(&binding.name) { bound.push(binding) } else { fail(binding.pos, format!("`{}` is not an input of {owner}", binding.name)) }
    }
    let mut names: Vec<&String> = fields.keys().filter(|field| !bound.iter().any(|binding| &binding.name == *field)).collect();
    names.sort();
    for field in names {
        let local = operation.local.iter().flatten().any(|name| name == field);
        let source = if operation.parameters.iter().any(|parameter| &parameter.name == field) {
            Some(field.as_str())
        } else if matches!(operation.kind, OperationKind::Create | OperationKind::Update) && !local && attributes.contains(field) {
            Some(field.as_str())
        } else if operation.kind != OperationKind::Create && (field == "id" || field == owner_id) {
            Some("id")
        } else {
            None
        };
        match source {
            Some(source) => bound.push(VariableBinding { pos: operation.pos, name: field.clone(), value: VariableValue::Parameter(source.to_string()) }),
            None if !fields[field].optional() => fail(operation.pos, format!("operation `{}`: input `{field}` of {owner} has no source: declare `parameter :{field}`, an attribute of that name, or rename one with `variables {field}: :source`", operation.name)),
            None => {}
        }
    }
    operation.variables = Some(bound);
}
