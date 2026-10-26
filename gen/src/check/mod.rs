use std::collections::HashSet;

use dblayer::schema::{validate_models, ModelSpec, NodeRead, Storage};
use dblayer::value::ScalarKind;

use crate::build::build;
use crate::decl::{
    Effect, ModelDecl, OperationDecl, OperationKind, OptimisticDecl, OptimisticElement, OptimisticValue, ParamBase, ParameterDecl, Pos,
    PreloadDecl, ResponseDecl, ResponseShape, ScopeDecl, SubscribeDecl, TypeRef, VariableValue,
};
use crate::ParseError;

mod attributes;
mod changes;
mod cloud;
pub(crate) use cloud::selected as cloud_names;
mod effects;
mod enums;
mod nested;
#[cfg(test)]
mod changes_tests;
#[cfg(test)]
mod effects_tests;
#[cfg(test)]
mod attribute_tests;
#[cfg(test)]
mod operation_tests;
#[cfg(test)]
mod scope_tests;

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
    if !errors.is_empty() {
        return Err(errors);
    }
    let models = build(nodes);
    changes::check_changes(nodes, &models, &mut errors);
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
    let faults = validate_models(&resolved, &[]);
    if faults.is_empty() {
        return Ok(models);
    }
    let found = faults
        .iter()
        .filter_map(|fault| {
            let node = nodes.iter().find(|node| node.name == fault.model.as_str())?;
            let hint = if matches!(fault.error, dblayer::schema::DeclarationError::TooManyColumns { .. }) { ". A union stores its variants in the columns of one table: declare fewer variants or fields, or split the model" } else { "" };
            Some(at(node.file, node.pos, format!("model {}: {}{hint}", node.name, fault.error)))
        })
        .collect();
    Err(found)
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
        check_scope(node, scope, &mut report);
    }
    cloud::check_cloud(node, &mut report);
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
    if matches!(apply.effect, Effect::Upsert | Effect::Delete) && !apply.matches.is_empty() {
        report(apply.pos, misplaced("matches", "`:ephemeral`, `:reread` and `:ignore`"));
    }
}

fn check_scope(node: &ModelDecl, scope: &ScopeDecl, report: &mut impl FnMut(Pos, String)) {
    let name = &scope.name;
    let mut names = HashSet::new();
    for parameter in &scope.parameters {
        if !names.insert(parameter.name.as_str()) {
            report(parameter.pos, format!("scope `{name}`: parameter `{}` is declared twice", parameter.name));
        }
    }
    let linked: Vec<&str> = scope.where_.iter().flat_map(|clause| clause.pairs.iter().map(|(_, parameter)| parameter.item.as_str())).collect();
    if let Some(clause) = &scope.where_ {
        let mut columns = HashSet::new();
        for (column, parameter) in &clause.pairs {
            if !scope.parameters.iter().any(|candidate| candidate.name == parameter.item) {
                report(parameter.pos, format!("scope `{name}`: where value `:{}` is not declared with `parameter`", parameter.item));
            }
            if !columns.insert(column.item.as_str()) {
                report(column.pos, format!("scope `{name}`: where column `{}` is given twice", column.item));
            }
        }
    }
    for parameter in &scope.parameters {
        let is_linked = linked.contains(&parameter.name.as_str());
        if !scope.is_remote() && !is_linked {
            report(parameter.pos, format!("scope `{name}`: parameter `:{}` is not used in `where`", parameter.name));
        }
        if is_linked && (parameter.null || parameter.default.is_some()) {
            report(parameter.pos, format!("scope `{name}`: parameter `:{}` filters a column: `null` and `default` apply to server-only parameters", parameter.name));
        }
        if scope.node.is_some() {
            continue;
        }
        check_scope_parameter(scope, parameter, is_linked, node, report);
    }
    if let Some(order) = &scope.order {
        let mut seen = HashSet::new();
        for (column, _, pos) in &order.fields {
            if !seen.insert(column.as_str()) {
                report(*pos, format!("scope `{name}`: order column `{column}` is given twice"));
            }
        }
    }
    if let Some(variables) = &scope.variables {
        for binding in variables {
            if let VariableValue::Parameter(parameter) = &binding.value {
                if !scope.parameters.iter().any(|candidate| &candidate.name == parameter) {
                    report(binding.pos, format!("scope `{name}`: variable `{}` names `:{parameter}`, which is not declared with `parameter`", binding.name));
                }
            }
        }
    }
    if let Some(preload) = &scope.preload {
        check_preload(scope, preload, report);
    }
    if !scope.is_remote() {
        return;
    }
    check_remote_scope(scope, report);
}

fn check_preload(scope: &ScopeDecl, preload: &PreloadDecl, report: &mut impl FnMut(Pos, String)) {
    let name = &scope.name;
    if scope.node.is_some() {
        report(preload.pos, format!("scope `{name}`: `preload` does not apply to a `node` scope"));
        return;
    }
    let mut keys = HashSet::new();
    for (key, values) in &preload.values {
        if !keys.insert(key.item.as_str()) {
            report(key.pos, format!("scope `{name}`: `preload` lists `{}` twice", key.item));
        }
        let Some(parameter) = scope.parameters.iter().find(|parameter| parameter.name == key.item) else {
            report(key.pos, format!("scope `{name}`: `preload` names `{}`, which is not declared with `parameter`", key.item));
            continue;
        };
        if values.is_empty() {
            report(key.pos, format!("scope `{name}`: `preload {}:` needs at least 1 value", key.item));
        }
        let mut seen = Vec::new();
        for value in values {
            let fits = match (&parameter.ty.base, parameter.ty.list, &value.item) {
                (_, true, _) => false,
                (ParamBase::Int | ParamBase::Long, _, serde_json::Value::Number(_)) | (ParamBase::Bool, _, serde_json::Value::Bool(_)) => true,
                (ParamBase::String | ParamBase::Id | ParamBase::Named(_), _, serde_json::Value::String(_)) => true,
                _ => false,
            };
            if !fits {
                report(value.pos, format!("scope `{name}`: `preload {}:` value does not match the type {} of the parameter", key.item, parameter.ty.describe()));
            } else if seen.contains(&&value.item) {
                report(value.pos, format!("scope `{name}`: `preload {}:` lists a value twice", key.item));
            }
            seen.push(&value.item);
        }
    }
    for parameter in &scope.parameters {
        if !keys.contains(parameter.name.as_str()) {
            report(preload.pos, format!("scope `{name}`: `preload` needs a list of values for parameter `{}`", parameter.name));
        }
    }
}

fn check_scope_parameter(scope: &ScopeDecl, parameter: &ParameterDecl, linked: bool, node: &ModelDecl, report: &mut impl FnMut(Pos, String)) {
    let name = &scope.name;
    let text_like = matches!((&parameter.ty.base, parameter.ty.list), (ParamBase::String | ParamBase::Id | ParamBase::Named(_), false));
    let typed = matches!((&parameter.ty.base, parameter.ty.list), (ParamBase::Int | ParamBase::Bool, false));
    if linked {
        let Some((column, _)) = scope.where_.iter().flat_map(|clause| &clause.pairs).find(|(_, linked)| linked.item == parameter.name) else { return };
        let Some(attribute) = node.attributes.iter().find(|attribute| attribute.name == column.item) else { return };
        let fits = match (&attribute.ty, &parameter.ty.base, parameter.ty.list) {
            (TypeRef::Scalar(ScalarKind::Id | ScalarKind::Str), ParamBase::Id | ParamBase::String, false) => true,
            (TypeRef::Scalar(ScalarKind::Int), ParamBase::Int, false) => true,
            (TypeRef::Scalar(ScalarKind::Num), ParamBase::Num, false) => true,
            (TypeRef::Scalar(ScalarKind::Bool), ParamBase::Bool, false) => true,
            (TypeRef::Scalar(ScalarKind::Date), ParamBase::String, false) => true,
            (TypeRef::Enum, ParamBase::String | ParamBase::Named(_), false) => true,
            (TypeRef::Scalar(_), _, _) => false,
            _ => true,
        };
        if !fits {
            report(parameter.pos, format!("scope `{name}`: parameter `:{}` is {} but column `{}` is a different type", parameter.name, parameter.ty.describe(), column.item));
        }
        return;
    }
    if !(text_like || typed) {
        report(parameter.pos, format!("scope `{name}`: a server-only parameter is :string, :id, :int, :bool or an enum name"));
        return;
    }
    let Some(default) = &parameter.default else { return };
    let fits = matches!((&parameter.ty.base, parameter.ty.list, &default.item), (ParamBase::Named(_), false, serde_json::Value::String(_)));
    if !fits {
        report(default.pos, format!("scope `{name}`: default of `:{}` does not match its type {}: a default belongs to an enum parameter and is one of its values", parameter.name, parameter.ty.describe()));
    }
}

fn check_remote_scope(scope: &ScopeDecl, report: &mut impl FnMut(Pos, String)) {
    let name = &scope.name;
    let paginated = scope.paginate.is_some();
    let listed = scope.response.as_ref().is_some_and(|read| read.shape == Some(ResponseShape::List));
    if paginated && listed {
        report(scope.paginate.as_ref().map_or(scope.pos, |paginate| paginate.pos), format!("scope `{name}`: `paginate` and `response shape: :list` cannot be used together"));
    }
    if let Some(total) = &scope.total {
        if !paginated {
            report(total.pos, "`total` needs `paginate`".to_string());
        }
    }
    let linked = scope.where_.is_some();
    let server_only = scope.parameters.iter().any(|parameter| !scope.where_.iter().flat_map(|clause| &clause.pairs).any(|(_, linked)| linked.item == parameter.name));
    if let Some(insert) = &scope.insert_on {
        let mut sorted: Vec<_> = insert.item.iter().map(|source| *source as u8).collect();
        sorted.sort_unstable();
        let create = crate::decl::InsertOn::Create as u8;
        let events = [crate::decl::InsertOn::Event as u8, crate::decl::InsertOn::Response as u8];
        if sorted != [create] && sorted != events {
            report(insert.pos, format!("scope `{name}`: `insert_on` is `[:create]` or `[:event, :response]`"));
        } else if sorted == events && !linked {
            report(insert.pos, format!("scope `{name}`: `insert_on: [:event, :response]` joins rows by their `where` columns and needs `where`"));
        } else if sorted == events && server_only {
            report(insert.pos, format!("scope `{name}`: `insert_on: [:event, :response]` joins rows by their `where` columns and a scope with server-only parameters has none, remove `insert_on:` and the server answer decides the members"));
        }
    }
    if let Some(pos) = scope.stable_order {
        if scope.order.is_none() || scope.where_.is_none() || server_only {
            report(pos, format!("scope `{name}`: `stable_order: true` keeps the shown order of the rows that `where` selects and needs `order` and `where`, without server-only parameters"));
        }
    }
    if !paginated {
        for edge in &scope.edges {
            report(edge.pos, format!("edge `{}` needs `paginate` in its scope", edge.name));
        }
    }
    attributes::check_edges(&scope.edges, report);
    match scope.node {
        Some(NodeRead::One) => check_node_one(scope, report),
        Some(NodeRead::Many) => check_node_many(scope, report),
        None => {}
    }
}

fn check_node_one(scope: &ScopeDecl, report: &mut impl FnMut(Pos, String)) {
    let name = &scope.name;
    if scope.parameters.len() != 1 || scope.parameters[0].ty.base != ParamBase::Id || scope.parameters[0].ty.list || scope.parameters[0].null || scope.parameters[0].default.is_some() {
        report(scope.pos, format!("scope `{name}`: a `node: :one` scope has exactly one parameter, declared as `parameter :id, :id`"));
    }
    let bound = scope.variables.as_ref().is_some_and(|variables| variables.len() == 1 && matches!(&variables[0].value, VariableValue::Parameter(parameter) if scope.parameters.first().is_some_and(|declared| &declared.name == parameter)));
    if !bound {
        report(scope.pos, format!("scope `{name}`: a `node: :one` scope binds its parameter with `variables id: :id`"));
    }
    for (present, word) in [(scope.where_.is_some(), "where"), (scope.order.is_some(), "order"), (scope.paginate.is_some(), "paginate"), (scope.total.is_some(), "total"), (!scope.edges.is_empty(), "edge"), (scope.response.is_some(), "response"), (scope.insert_on.is_some(), "insert_on")] {
        if present {
            report(scope.pos, format!("scope `{name}`: `{word}` does not apply to a `node: :one` scope"));
        }
    }
}

fn check_node_many(scope: &ScopeDecl, report: &mut impl FnMut(Pos, String)) {
    let name = &scope.name;
    for (present, word) in [(!scope.parameters.is_empty(), "parameter"), (scope.where_.is_some(), "where"), (scope.order.is_some(), "order"), (scope.paginate.is_some(), "paginate"), (scope.total.is_some(), "total"), (!scope.edges.is_empty(), "edge"), (scope.variables.is_some(), "variables"), (scope.insert_on.is_some(), "insert_on")] {
        if present {
            report(scope.pos, format!("scope `{name}`: `{word}` does not apply to a `node: :many` scope"));
        }
    }
    if scope.response.as_ref().is_some_and(|read| read.shape == Some(ResponseShape::One)) {
        report(scope.pos, format!("scope `{name}`: a `node: :many` scope reads a list"));
    }
}

fn check_parameters(parameters: &[ParameterDecl], attributes: &HashSet<&str>, standard: bool, report: &mut impl FnMut(Pos, String)) {
    let mut names = HashSet::new();
    for parameter in parameters {
        if !names.insert(parameter.name.as_str()) {
            report(parameter.pos, format!("parameter `{}` is declared twice", parameter.name));
        }
        if standard && attributes.contains(parameter.name.as_str()) {
            report(parameter.pos, format!("parameter `{}` has the name of an attribute", parameter.name));
        }
        if parameter.default.is_some() {
            report(parameter.pos, format!("parameter `{}`: an operation parameter has no `default`", parameter.name));
        }
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
            OptimisticValue::Literal(_) => false,
        }));
        let implicit = operation.upload.is_some() && operation.variables.is_none();
        if !(in_variables || in_optimistic || implicit || effects::used_parameter(operation, &parameter.name)) {
            report(parameter.pos, format!("parameter `{}` is used by neither `variables` nor `optimistic`", parameter.name));
        }
    }
    match (&operation.optimistic, operation.kind) {
        (None, OperationKind::Custom) if operation.effects.is_empty() => report(operation.pos, format!("operation `{name}` must declare `optimistic` fields or `optimistic false`")),
        (Some((OptimisticDecl::Fields(fields), _)), _) => {
            for (field, value, pos) in fields {
                if !attributes.contains(field.as_str()) {
                    report(*pos, format!("`optimistic` names unknown attribute `{field}`"));
                }
                check_optimistic_value(operation, value, *pos, report);
            }
        }
        _ => {}
    }
    for refetch in &operation.refetch {
        let target = refetch.model.as_deref().map_or(Some(node), |model| all.iter().find(|candidate| candidate.name == model));
        match target {
            None => report(refetch.pos, format!("`refetch` names unknown model `{}`", refetch.model.as_deref().unwrap_or_default())),
            Some(target) if !target.scopes.iter().any(|scope| scope.name == refetch.scope && scope.is_remote()) => {
                report(refetch.pos, format!("`refetch` names `{}`, which is not a scope with `query` of `{}`", refetch.scope, target.name));
            }
            Some(_) => {}
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
    let ignored = matches!(operation.response, Some(ResponseDecl::Ignore));
    if ignored && (!operation.upserts.is_empty() || operation.match_by.is_some()) {
        report(operation.pos, format!("operation `{name}` ignores its response: `upsert` and `match_by` read it"));
    }
    effects::check(node, all, operation, report);
    check_batch(node, operation, report);
}

fn check_optimistic_value(operation: &OperationDecl, value: &OptimisticValue, pos: Pos, report: &mut impl FnMut(Pos, String)) {
    let declared = |name: &str| operation.parameters.iter().any(|parameter| parameter.name == name);
    match value {
        OptimisticValue::Parameter(name) if !declared(name) => report(pos, format!("`optimistic` value `:{name}` is not declared with `parameter`")),
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
