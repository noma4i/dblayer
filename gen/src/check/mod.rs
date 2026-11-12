use std::collections::HashSet;

use dblayer::schema::{validate_models, ModelSpec, NodeRead, Storage};
use dblayer::value::ScalarKind;

use crate::build::build;
use crate::decl::{
    Effect, ModelDecl, OperationDecl, OperationKind, OptimisticDecl, OptimisticElement, OptimisticValue, ParamBase, ParameterDecl, Pos,
    PreloadChoice, PreloadDecl, ResponseDecl, ResponseShape, ScopeDecl, Spanned, SubscribeDecl, TypeRef, VariableValue,
};
use crate::ParseError;

mod scope;
mod attributes;
mod changes;
mod derived;
mod removed_by;
mod effects;
mod enums;
mod nested;
#[cfg(test)]
mod tests;

use scope::*;

type Found = (usize, ParseError);

#[cfg(test)]
pub(crate) fn check_source(source: &str) -> Result<Vec<ModelSpec>, Vec<ParseError>> {
    crate::parse_tree(&[source])
        .map_err(|errors| errors.into_iter().map(|(_, error)| error).collect::<Vec<_>>())
        .and_then(|parsed| crate::decl::lower_models(&parsed.models, None).map_err(|errors| errors.into_iter().map(|(_, error)| error).collect::<Vec<_>>()))
        .and_then(|models| check(&models).map_err(|errors| errors.into_iter().map(|(_, error)| error).collect()))
}

fn at(file: usize, pos: Pos, message: impl Into<String>) -> Found {
    (file, ParseError::new(pos.line, pos.column, message))
}

pub fn check(nodes: &[ModelDecl]) -> Result<Vec<ModelSpec>, Vec<Found>> {
    let mut errors = Vec::new();
    let mut seen_models = HashSet::new();
    for node in nodes {
        if !seen_models.insert(node.name.as_str()) {
            errors.push(at(node.file, node.pos, format!("model `{}` is declared twice", node.name)));
        }
        check_model(node, nodes, &mut errors);
    }
    attributes::check_types(nodes, &mut errors);
    derived::check_derived(nodes, &mut errors);
    if !errors.is_empty() {
        return Err(errors);
    }
    let models = build(nodes);
    changes::check_changes(nodes, &models, &mut errors);
    removed_by::check_removed_by(nodes, &models, &mut errors);
    enums::check_enums(nodes, &models, &mut errors);
    if !errors.is_empty() {
        return Err(errors);
    }
    nested::check_defaults(nodes, &models, &mut errors);
    if !errors.is_empty() {
        return Err(errors);
    }
    let mut resolved = models.clone();
    dblayer::schema::resolve_types(&mut resolved);
    let faults = validate_models(&resolved, &dblayer::schema::reserved_member_names());
    if faults.is_empty() {
        return Ok(models);
    }
    Err(fault_errors(nodes, &faults))
}

fn fault_errors(nodes: &[ModelDecl], faults: &[dblayer::schema::ModelFault]) -> Vec<Found> {
    faults
        .iter()
        .map(|fault| {
            let node = nodes.iter().find(|node| node.name == fault.model.as_str()).or_else(|| nodes.first()).expect("a fault belongs to a project with a model");
            let (file, pos) = (node.file, node.pos);
            let hint = if matches!(fault.error, dblayer::schema::DeclarationError::TooManyColumns { .. }) { ". A union stores its variants in the columns of one table: declare fewer variants or fields, or split the model" } else { "" };
            at(file, pos, format!("model {}: {}{hint}", fault.model.as_str(), fault.error))
        })
        .collect()
}

fn check_model(node: &ModelDecl, all: &[ModelDecl], errors: &mut Vec<Found>) {
    let file = node.file;
    let mut report = |pos: Pos, message: String| errors.push(at(file, pos, message));
    if node.storage == Storage::Local {
        for scope in node.scopes.iter().filter(|scope| scope.is_remote()) {
            report(scope.pos, "local model has no server: a scope with `query` is not allowed with `storage: :local`".into());
        }
        for subscription in node.subscriptions.iter().chain(node.scopes.iter().flat_map(|scope| &scope.subscriptions)) {
            report(subscription.pos, "local model has no server: `subscribe` is not allowed with `storage: :local`".into());
        }
        for operation in &node.operations {
            report(operation.pos, "local model has no server: `operation` is not allowed with `storage: :local`".into());
        }
    }
    let mut subscriptions = HashSet::new();
    for update in node.subscriptions.iter().chain(node.scopes.iter().flat_map(|scope| &scope.subscriptions)) {
        if !subscriptions.insert(update.name.as_str()) {
            report(update.pos, format!("duplicate subscription `{}`", update.name));
        }
        check_subscription(update, &mut report);
    }
    for scope in &node.scopes {
        let Some(bindings) = scope.variables.as_ref() else { continue };
        for id in scope.subscriptions.iter().flat_map(|subscription| &subscription.id) {
            if !bindings.iter().any(|binding| binding.name == id.item) {
                report(id.pos, format!("scope `{}`: subscription `id: \"{}\"` is no variable of `variables`", scope.name, id.item));
            }
        }
    }
    let mut member_names = HashSet::new();
    let members = node.scopes.iter().map(|scope| (scope.pos, scope.name.as_str()))
        .chain(node.associations.iter().map(|association| (association.pos, association.name.as_str())))
        .chain(node.operations.iter().map(|operation| (operation.pos, operation.name.as_str())));
    for (pos, name) in members {
        if !member_names.insert(name) {
            report(pos, format!("name `{name}` is declared twice"));
        }
    }
    for association in &node.associations {
        let target = crate::build::association_target(association);
        if !all.iter().any(|candidate| candidate.name == target) {
            report(association.pos, format!("association `{}` targets unknown model `{target}`", association.name));
        }
    }
    if node.changes.len() > 1 {
        report(node.changes[1].pos, "duplicate changes subscription".into());
    }
    for (index, index_decl) in node.indexes.iter().enumerate() {
        if node.indexes[..index].iter().any(|earlier| earlier.columns == index_decl.columns) {
            report(index_decl.pos, format!("index on {} is declared twice", index_decl.columns.join(", ")));
        }
    }
    for scope in &node.scopes {
        check_scope(node, all, scope, &mut report);
    }
    let attributes = attributes::AttributeCheck { node, report: &mut report };
    attributes.run();
    for operation in &node.operations {
        check_operation(node, all, operation, &mut report);
    }
}

fn check_subscription(update: &SubscribeDecl, report: &mut impl FnMut(Pos, String)) {
    let apply = &update.apply;
    let name = &update.name;
    let misplaced = |key: &str, allowed: &str| format!("subscription `{name}`: `{key}` applies only to effect {allowed}");
    match apply.effect {
        Effect::Ephemeral if apply.fields.is_empty() => report(apply.pos, "ephemeral apply requires nonempty fields and paths".into()),
        Effect::Reread if apply.via.is_none() => report(apply.pos, "reread apply requires via".into()),
        _ => {}
    }
    if apply.effect != Effect::Ephemeral && !apply.fields.is_empty() {
        report(apply.pos, misplaced("fields", "`:ephemeral`"));
    }
    if apply.effect != Effect::Reread && apply.via.is_some() {
        report(apply.pos, misplaced("via", "`:reread`"));
    }
    if apply.effect != Effect::Upsert && !apply.values.is_empty() {
        report(apply.pos, misplaced("values", "`:upsert`"));
    }
    if apply.effect != Effect::Upsert && apply.delete_if.is_some() {
        report(apply.pos, misplaced("delete_if", "`:upsert`"));
    }
    if apply.effect != Effect::Upsert && apply.delete_unless.is_some() {
        report(apply.pos, misplaced("delete_unless", "`:upsert`"));
    }
    if apply.delete_if.is_some() && apply.delete_unless.is_some() {
        report(apply.pos, format!("subscription `{name}`: `delete_if` and `delete_unless` cannot be combined"));
    }
}

fn check_operation(node: &ModelDecl, all: &[ModelDecl], operation: &OperationDecl, report: &mut impl FnMut(Pos, String)) {
    let name = &operation.name;
    let attributes: HashSet<&str> = node.attributes.iter().chain(node.ephemeral.iter().flat_map(|group| &group.attributes)).map(|attribute| attribute.name.as_str()).collect();
    let standard = operation.kind != OperationKind::Custom;
    if operation.mutation.is_none() {
        report(operation.pos, format!("operation `{name}` needs a `mutation` line"));
    }
    check_parameters(&operation.parameters, &attributes, standard, report);
    if let Some(variables) = &operation.variables {
        for binding in variables {
            let VariableValue::Parameter(source) = &binding.value else { continue };
            if source == "id" && operation.kind == OperationKind::Create {
                report(binding.pos, format!("variable `{}` of `create` names `id`: the row has a temporary id until the server answers", binding.name));
                continue;
            }
            let known = source == "id" || attributes.contains(source.as_str()) || operation.parameters.iter().any(|parameter| &parameter.name == source);
            if !known {
                report(binding.pos, format!("variable `{}` names unknown attribute or parameter `{source}`", binding.name));
            }
        }
    }
    for parameter in &operation.parameters {
        let in_variables = operation.variables.iter().flatten().any(|binding| matches!(&binding.value, VariableValue::Parameter(source) if *source == parameter.name));
        let in_optimistic = matches!(&operation.optimistic, Some((OptimisticDecl::Fields(fields), _)) if fields.iter().any(|(_, value, _)| match value {
            OptimisticValue::Parameter(name) => *name == parameter.name,
            OptimisticValue::Keyed { element, .. } => element.iter().any(|(_, part)| matches!(part, OptimisticElement::Parameter(name) if *name == parameter.name)),
            OptimisticValue::Mapped { from, .. } => *from == parameter.name,
            OptimisticValue::Include { when, .. } | OptimisticValue::AtLeast { when, .. } => *when == parameter.name,
            OptimisticValue::Literal(_) => false,
        }));
        let implicit = operation.upload.is_some() && operation.variables.is_none();
        if !(in_variables || in_optimistic || implicit) {
            report(parameter.pos, format!("parameter `{}` is used by neither `variables` nor `optimistic`", parameter.name));
        }
    }
    match (&operation.optimistic, operation.kind) {
        (None, OperationKind::Custom) => report(operation.pos, format!("operation `{name}` must declare `optimistic` fields or `optimistic false`")),
        (Some((OptimisticDecl::Fields(fields), _)), _) => {
            for (field, value, pos) in fields {
                if !attributes.contains(field.as_str()) {
                    report(*pos, format!("`optimistic` names unknown attribute `{field}`"));
                }
                check_optimistic_value(node, all, operation, (field.as_str(), value), *pos, report);
            }
        }
        _ => {}
    }
    for refetch in &operation.refetch {
        let target = match refetch.model.as_deref() {
            Some(model) => all.iter().find(|candidate| candidate.name == model).ok_or(model),
            None => Ok(node),
        };
        match target {
            Err(model) => report(refetch.pos, format!("`refetch` names unknown model `{model}`")),
            Ok(target) if !target.scopes.iter().any(|scope| scope.name == refetch.scope && scope.is_remote()) => {
                report(refetch.pos, format!("`refetch` names `{}`, which is not a scope with `query` of `{}`", refetch.scope, target.name));
            }
            Ok(_) => {}
        }
    }
    let mut refetched = HashSet::new();
    for refetch in &operation.refetch {
        if !refetched.insert((refetch.scope.as_str(), refetch.model.as_deref())) {
            report(refetch.pos, format!("`refetch :{}` is declared twice", refetch.scope));
        }
    }
    for upsert in &operation.upserts {
        if !all.iter().any(|candidate| candidate.name == upsert.model) {
            report(upsert.pos, format!("`upsert` names unknown model `{}`", upsert.model));
        }
    }
    if let Some(local) = &operation.local {
        for name in local.iter().filter(|name| !attributes.contains(name.as_str())) {
            report(operation.pos, format!("`local` names unknown attribute `{name}`"));
        }
    }
    effects::check_local_from(node, all, operation, report);
    effects::check_closed_effects(node, all, operation, report);
    let ignored = matches!(operation.response, Some(ResponseDecl::Ignore));
    if ignored && (!operation.upserts.is_empty() || operation.match_by.is_some()) {
        report(operation.pos, format!("operation `{name}` ignores its response: `upsert` and `match_by` read it"));
    }
    check_batch(node, operation, report);
    check_processing(node, operation, report);
}

fn check_optimistic_value(node: &ModelDecl, all: &[ModelDecl], operation: &OperationDecl, (field, value): (&str, &OptimisticValue), pos: Pos, report: &mut impl FnMut(Pos, String)) {
    let declared = |name: &str| operation.parameters.iter().any(|parameter| parameter.name == name);
    let flag = |when: &str, report: &mut dyn FnMut(Pos, String)| match operation.parameters.iter().find(|parameter| parameter.name == when) {
        None => report(pos, format!("`optimistic` reads `:{when}`, which is not declared with `parameter`")),
        Some(parameter) if parameter.ty.base != ParamBase::Bool || parameter.ty.list => report(pos, format!("`optimistic` reads `:{when}`, which must be a bool parameter")),
        Some(_) => {}
    };
    let attribute = node.attributes.iter().find(|attribute| attribute.name == field);
    match value {
        OptimisticValue::Include { source, when } => {
            flag(when, report);
            effects::check_optimistic_source(node, all, operation, source, pos, report);
            if attribute.is_some_and(|attribute| !matches!(attribute.ty, TypeRef::List(_))) {
                report(pos, format!("`optimistic` `include` needs `{field}` to be a list attribute"));
            }
        }
        OptimisticValue::AtLeast { when, .. } => {
            flag(when, report);
            if attribute.is_some_and(|attribute| !matches!(attribute.ty, TypeRef::Scalar(ScalarKind::Int | ScalarKind::Num))) {
                report(pos, format!("`optimistic` `at_least` needs `{field}` to be a number attribute"));
            }
        }
        OptimisticValue::Parameter(name) if !declared(name) => report(pos, format!("`optimistic` value `:{name}` is not declared with `parameter`")),
        OptimisticValue::Mapped { from, .. } => match operation.parameters.iter().find(|parameter| parameter.name == *from) {
            None => report(pos, format!("`optimistic` map reads `:{from}`, which is not declared with `parameter`")),
            Some(parameter) if !matches!((&parameter.ty.base, parameter.ty.list), (ParamBase::Named(_), false)) => report(pos, format!("`optimistic` map reads `:{from}`, which must be a schema enum parameter")),
            Some(_) => {}
        },
        OptimisticValue::Keyed { element, .. } => {
            for (_, part) in element {
                if let OptimisticElement::Parameter(name) = part {
                    if !declared(name) {
                        report(pos, format!("`optimistic` element value `:{name}` is not declared with `parameter`"));
                    }
                }
            }
        }
        _ => {}
    }
}

fn check_processing(node: &ModelDecl, operation: &OperationDecl, report: &mut impl FnMut(Pos, String)) {
    let Some(processing) = &operation.processing else { return };
    let name = &operation.name;
    if operation.upload.is_none() {
        report(processing.pos, format!("operation `{name}`: `processing` belongs to an operation with `upload: true`"));
    }
    for path in std::iter::once(&processing.field).chain(&processing.progress) {
        let first = path.split('.').next().unwrap_or_default();
        if !node.attributes.iter().any(|attribute| attribute.name == first) {
            report(processing.pos, format!("operation `{name}`: `processing` path `{path}` names no attribute of `{}`", node.name));
        }
    }
    let first = processing.field.split('.').next().unwrap_or_default();
    if let Some(values) = node.attributes.iter().find(|attribute| attribute.name == first && !processing.field.contains('.')).and_then(|attribute| attribute.values.as_ref()) {
        if let Some(missing) = processing.done.iter().find(|done| !values.contains(done)) {
            report(processing.pos, format!("operation `{name}`: `done` value `{missing}` is not a value of `{}`", processing.field));
        }
    }
}

fn check_batch(node: &ModelDecl, operation: &OperationDecl, report: &mut impl FnMut(Pos, String)) {
    let name = &operation.name;
    match (&operation.batch, &operation.length) {
        (Some(batch), Some(length)) => {
            if operation.upload.is_none() {
                report(batch.pos, format!("operation `{name}`: `batch` belongs to an operation with `upload: true`"));
            }
            if length.attribute != batch.items {
                report(length.pos, format!("operation `{name}`: `validates :{}` must name the batch items `{}`", length.attribute, batch.items));
            }
            let single = node.operations.iter().find(|candidate| candidate.name == batch.single);
            if single.is_none_or(|single| single.upload.is_none() || single.batch.is_some()) {
                report(batch.pos, format!("operation `{name}`: batch `single:` must name an operation with `upload: true` and no batch"));
            }
        }
        (Some(batch), None) => report(batch.pos, format!("operation `{name}`: `batch` needs `validates :{}, length: {{ minimum:, maximum: }}`", batch.items)),
        (None, Some(length)) => report(length.pos, format!("operation `{name}`: `validates` needs a `batch`")),
        (None, None) => {}
    }
    if let (Some(batch), Some(length)) = (&operation.batch, &operation.length) {
        if length.minimum < 2 || length.maximum > 10 || length.minimum > length.maximum {
            report(length.pos, format!("operation `{name}`: batch length needs 2 <= minimum <= maximum <= 10"));
        }
        if let Some(renumber) = &batch.renumber {
            if renumber.position.item == renumber.size.item {
                report(renumber.position.pos, "renumber `position` and `size` name the same field".to_string());
            }
        }
    }
}
