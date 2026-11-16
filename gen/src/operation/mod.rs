use std::collections::HashSet;

use dblayer::schema::{FieldSpec, ModelSpec};

use crate::decl::{CallDecl, ResponseShape};
use crate::ParseError;

type Found = (usize, ParseError);

/// A server operation that no model owns: a typed call that sends it and reads the declared fields of its answer.
#[derive(Debug, Clone, PartialEq)]
pub struct OperationCall {
    pub name: String,
    pub operation: String,
    pub wrap: Option<String>,
    pub args: Vec<(String, String)>,
    pub result: Option<FieldSpec>,
    pub list: bool,
    pub lands: Vec<dblayer::schema::ActionLanding>,
    /// The `Model.scope` windows that turn stale after a successful answer (`refetch :scope, model: Model`).
    pub refetch: Vec<String>,
}

pub(crate) fn element_name(kind: &str) -> &str {
    kind.trim_start_matches('[').trim_end_matches('?').trim_end_matches(']').trim_end_matches('?')
}

pub(crate) fn is_list_kind(kind: &str) -> bool {
    kind.starts_with('[')
}

pub(crate) fn is_input_object(kind: &str) -> bool {
    element_name(kind).chars().next().is_some_and(char::is_uppercase)
}

pub(crate) fn display_kind(kind: &str) -> String {
    let base = kind.trim_end_matches('?');
    if is_list_kind(base) {
        let inner = base.trim_start_matches('[').trim_end_matches(']');
        if is_input_object(inner) { format!("[{inner}]") } else { format!("[:{inner}]") }
    } else if is_input_object(base) {
        base.to_string()
    } else {
        format!(":{base}")
    }
}

pub(crate) fn schema_type_names(models: &[ModelSpec], operations: &[OperationCall]) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    let model_args = models.iter().flat_map(|model| &model.actions).flat_map(|action| action.args.iter());
    let operation_args = operations.iter().flat_map(|operation| operation.args.iter().map(|(name, kind)| (name, kind)));
    for (_, kind) in model_args.chain(operation_args).filter(|(_, kind)| is_input_object(kind)) {
        let name = element_name(kind).to_string();
        if !names.contains(&name) {
            names.push(name);
        }
    }
    for relation in models.iter().flat_map(|model| &model.relations) {
        for (parameter, kind) in &relation.text_types {
            if !relation.membership.contains_key(parameter) && !names.contains(kind) {
                names.push(kind.clone());
            }
        }
    }
    names
}

pub(crate) fn build(nodes: &[CallDecl], errors: &mut Vec<Found>) -> Vec<OperationCall> {
    let mut seen = HashSet::new();
    let mut built = Vec::new();
    for node in nodes {
        let fail = |pos: crate::decl::Pos, message: String| (node.file, ParseError::new(pos.line, pos.column, message));
        if !seen.insert(node.name.as_str()) {
            errors.push(fail(node.pos, format!("operation `{}` is declared twice", node.name)));
        }
        let mut call = OperationCall { name: node.name.clone(), operation: String::new(), wrap: node.wrap.clone(), args: Vec::new(), result: None, list: false, lands: node.upserts.iter().map(|upsert| dblayer::schema::ActionLanding { model: upsert.model.clone(), root: upsert.from.item.clone() }).collect(), refetch: node.refetch.iter().flat_map(|refetch| refetch.model.as_ref().map(|model| format!("{model}.{}", refetch.scope))).collect() };
        if let Some((_, name, _)) = &node.kind {
            call.operation = name.clone();
        }
        let mut names = HashSet::new();
        for parameter in &node.parameters {
            if !names.insert(parameter.name.as_str()) {
                errors.push(fail(parameter.pos, format!("parameter `{}` is declared twice", parameter.name)));
                continue;
            }
            if parameter.default.is_some() {
                errors.push(fail(parameter.pos, format!("parameter `{}`: an operation parameter has no `default`", parameter.name)));
            }
            call.args.push((parameter.name.clone(), parameter.ty.kind_text(parameter.null)));
        }
        call.result = node.answer.clone();
        if let Some((ResponseShape::List, pos)) = &node.shape {
            call.list = true;
            if node.answer.is_none() {
                errors.push(fail(*pos, "`response shape: :list` needs the document to select the fields of one element".to_string()));
            }
        }
        built.push(call);
    }
    built
}
