use std::collections::HashSet;

use dblayer::schema::{FieldKind, FieldSpec, ModelSpec};
use dblayer::value::ScalarKind;

use crate::build::camel;
use crate::decl::{AttributeBlock, AttributeDecl, CallDecl, ListElement, ResponseShape, TypeRef};
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

pub(crate) fn resolve_result_enums(operations: &mut [OperationCall], schema: Option<&crate::schema_check::Schema>) -> Result<(), String> {
    fn walk(field: &mut FieldSpec, schema: Option<&crate::schema_check::Schema>) -> Result<(), String> {
        if let (true, Some(name)) = (matches!(field.kind, FieldKind::Enum { .. } | FieldKind::List { .. }), field.app_type.clone()) {
            let Some(schema) = schema else { return Err(format!("result field `{}` names the schema enum `{name}`: pass --schema <schema.graphql> so the generator can read it", field.name)) };
            let Some(crate::schema_check::TypeKind::Enum(known)) = schema.types.get(&name) else { return Err(format!("result field `{}` names `{name}`, which is not an enum of the schema", field.name)) };
            match &mut field.kind {
                FieldKind::Enum { values } => *values = known.clone(),
                _ => field.values = known.clone(),
            }
        }
        field.fields.iter_mut().try_for_each(|inner| walk(inner, schema))
    }
    operations.iter_mut().filter_map(|operation| operation.result.as_mut()).try_for_each(|root| walk(root, schema))
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

fn result_fields(attributes: &[AttributeDecl], file: usize, errors: &mut Vec<Found>) -> Vec<FieldSpec> {
    let mut fields: Vec<FieldSpec> = Vec::new();
    for attribute in attributes {
        let fail = |message: &str| (file, ParseError::new(attribute.pos.line, attribute.pos.column, message));
        if fields.iter().any(|field| field.name == attribute.name) {
            errors.push(fail(&format!("returns attribute `{}` is declared twice", attribute.name)));
            continue;
        }
        let name = attribute.name.as_str();
        let nested = |errors: &mut Vec<Found>, mut field: FieldSpec, type_name: &str| {
            field.app_type = Some(type_name.to_string());
            if let AttributeBlock::Fields(inner) = &attribute.block {
                field.fields = result_fields(inner, file, errors);
            }
            field
        };
        let field: Result<FieldSpec, &str> = match (&attribute.ty, &attribute.block) {
            (TypeRef::Json, AttributeBlock::None) => Ok(FieldSpec::raw(name)),
            (TypeRef::Scalar(kind), AttributeBlock::None) if *kind != ScalarKind::Date => Ok(FieldSpec::scalar(name, *kind)),
            (TypeRef::List(ListElement::Scalar(kind)), AttributeBlock::None) if *kind != ScalarKind::Date => Ok(FieldSpec::list(name, *kind)),
            (TypeRef::Named(type_name), AttributeBlock::None) => {
                let mut field = FieldSpec::enumeration(name, &[]);
                field.app_type = Some(type_name.clone());
                Ok(field)
            }
            (TypeRef::NamedList(type_name), AttributeBlock::None) => {
                let mut field = FieldSpec::list(name, ScalarKind::Str);
                field.app_type = Some(type_name.clone());
                Ok(field)
            }
            (TypeRef::Named(type_name), AttributeBlock::Fields(_)) => Ok(nested(errors, FieldSpec::object(name), type_name)),
            (TypeRef::NamedList(type_name), AttributeBlock::Fields(_)) => Ok(nested(errors, FieldSpec::array(name), type_name)),
            _ => Err(RESULT_TYPES),
        };
        match field {
            Ok(field) => fields.push(field.nullable()),
            Err(message) => errors.push(fail(message)),
        }
    }
    fields
}

const RESULT_TYPES: &str = "a returned attribute is :bool, :int, :num, :string, :id, :json, [:id] or another scalar list, the name of a schema enum (alone or in a list), or a named object with a `do ... end` block (alone or in a list)";

pub(crate) fn build(nodes: &[CallDecl], errors: &mut Vec<Found>) -> Vec<OperationCall> {
    let mut seen = HashSet::new();
    let mut built = Vec::new();
    for node in nodes {
        let fail = |pos: crate::decl::Pos, message: String| (node.file, ParseError::new(pos.line, pos.column, message));
        if !seen.insert(node.name.as_str()) {
            errors.push(fail(node.pos, format!("operation `{}` is declared twice", node.name)));
        }
        let mut call = OperationCall { name: node.name.clone(), operation: String::new(), wrap: node.wrap.clone(), args: Vec::new(), result: None, list: false };
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
        if let Some(attributes) = &node.returns {
            let type_name = format!("{}Result", camel(&call.name));
            let mut root = FieldSpec::object(&call.name);
            root.app_type = Some(type_name);
            root.fields = result_fields(attributes, node.file, errors);
            call.result = Some(root);
        }
        if let Some((ResponseShape::List, pos)) = &node.shape {
            call.list = true;
            if node.returns.is_none() {
                errors.push(fail(*pos, "`response shape: :list` needs a `returns` block that describes one element".to_string()));
            }
        }
        built.push(call);
    }
    built
}
