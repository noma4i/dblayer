use std::collections::HashMap;

use super::graphql::{Operation, Operations, Schema};
use crate::decl::{AttributeDecl, CallDecl, ModelDecl, OperationDecl, OperationKind, ParameterDecl, Pos, VariableBinding, VariableValue};
use crate::naming::lower_first;

pub(super) type Failure = (usize, Pos, String);

/// What an operation offers to the fields of its input, in the order of M80: an explicit rename, a parameter, an attribute (`create`, `update`), the row id. A standalone operation has parameters only.
struct Sources<'a> {
    parameters: &'a [ParameterDecl],
    explicit: &'a [VariableBinding],
    kind: Option<OperationKind>,
    local: &'a [String],
    attributes: &'a [String],
    owner_id: &'a str,
}

impl Sources<'_> {
    fn of(&self, field: &str) -> Option<String> {
        let by_name = |name: &str| Some(name.to_string());
        if let Some(binding) = self.explicit.iter().find(|binding| binding.name == field) {
            return match &binding.value {
                VariableValue::Parameter(source) => by_name(source),
                VariableValue::Constant(_) => None,
            };
        }
        if self.parameters.iter().any(|parameter| parameter.name == field) {
            return by_name(field);
        }
        let kind = self.kind?;
        if matches!(kind, OperationKind::Create | OperationKind::Update) && !self.local.iter().any(|name| name == field) && self.attributes.iter().any(|name| name == field) {
            return by_name(field);
        }
        (kind != OperationKind::Create && (field == "id" || field == self.owner_id)).then(|| "id".to_string())
    }
}

/// M64: the one document variable that carries the body: an input object the root field takes and nothing of the operation feeds by its own name.
fn wrapped(schema: &Schema, operation: &Operation, sources: &Sources) -> Option<String> {
    let root = operation.root.as_ref()?;
    let mut variables = operation.variables.iter();
    let (name, ty) = variables.next().filter(|_| variables.next().is_none())?;
    (!ty.is_list() && schema.input_fields(ty.name()).is_some() && root.takes.contains(name) && sources.of(name).is_none()).then(|| name.clone())
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
        let attributes: Vec<String> = node.attributes.iter().filter(|attribute| !attribute.local && !attribute.derived).map(|attribute| attribute.name.clone()).collect();
        let owner_id = format!("{}Id", lower_first(&node.name));
        for operation in &mut node.operations {
            let Some(name) = operation.mutation.as_ref().map(|found| found.item.clone()) else { continue };
            let Some(document) = documents.named.get(&name).filter(|document| document.kind == "mutation") else {
                failures.push((node.file, operation.pos, format!("operation `{name}` is not in the operation documents")));
                continue;
            };
            let declared = operation.variables.take();
            let explicit: Vec<VariableBinding> = declared.iter().flatten().cloned().collect();
            let local: Vec<String> = operation.local.iter().flatten().cloned().collect();
            let sources = Sources { parameters: &operation.parameters, explicit: &explicit, kind: Some(operation.kind), local: &local, attributes: &attributes, owner_id: &owner_id };
            let wrap = wrapped(schema, document, &sources);
            let bound = if operation.upload.is_none() { Some(bind(schema, document, (&*operation, wrap.as_deref()), (&sources, &node.attributes), &mut |pos, message| failures.push((node.file, pos, message)))) } else { None };
            operation.wrap = wrap;
            operation.variables = if bound.is_some() { bound } else { declared };
        }
    }
    for call in calls.iter_mut() {
        let Some(document) = call.kind.as_ref().and_then(|(_, name, _)| documents.named.get(name)) else { continue };
        let sources = Sources { parameters: &call.parameters, explicit: &[], kind: None, local: &[], attributes: &[], owner_id: "" };
        call.wrap = wrapped(schema, document, &sources);
    }
    failures
}

fn bind(schema: &Schema, document: &Operation, (operation, wrap): (&OperationDecl, Option<&str>), (sources, attribute_decls): (&Sources, &[AttributeDecl]), fail: &mut dyn FnMut(Pos, String)) -> Vec<VariableBinding> {
    let fields = input_of(schema, document, wrap);
    let owner = match wrap {
        None => "the variables of the document".to_string(),
        Some(key) => format!("`{}`", document.variables[key].name()),
    };
    let mut bound: Vec<VariableBinding> = Vec::new();
    for binding in sources.explicit {
        if fields.contains_key(&binding.name) { bound.push(binding.clone()) } else { fail(binding.pos, format!("`{}` is not an input of {owner}", binding.name)) }
    }
    let mut names: Vec<&String> = fields.keys().filter(|field| !bound.iter().any(|binding| &binding.name == *field)).collect();
    names.sort();
    for field in names {
        match sources.of(field) {
            Some(source) => bound.push(VariableBinding { pos: operation.pos, name: field.clone(), value: VariableValue::Parameter(source) }),
            None if !fields[field].optional() => fail(operation.pos, format!("operation `{}`: input `{field}` of {owner} has no source: declare `parameter :{field}`, an attribute of that name, or rename one with `variables {field}: :source`", operation.name)),
            None => {}
        }
    }
    let written = crate::build::written_by_default(attribute_decls, operation);
    for parameter in &operation.parameters {
        let named = |binding: &VariableBinding| matches!(&binding.value, VariableValue::Parameter(source) if *source == parameter.name);
        if !(bound.iter().any(named) || crate::check::optimistic_reads(operation, &parameter.name) || written.contains(&parameter.name)) {
            fail(parameter.pos, format!("parameter `{}` is used by neither `variables` nor `optimistic`", parameter.name));
        }
    }
    bound
}
