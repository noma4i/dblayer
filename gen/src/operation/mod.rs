use std::collections::HashSet;

use dblayer::schema::{FieldKind, FieldSpec, ModelSpec};
use dblayer::value::ScalarKind;

use crate::build::camel;
use crate::parser::{Call, Node, OperationNode, Pair, Value};
use crate::ParseError;

type Found = (usize, ParseError);

const ARG_KINDS: [&str; 7] = ["bool", "int", "long", "num", "string", "id", "ids"];
const WORDS: [&str; 6] = ["mutation", "query", "wrap", "args", "result", "list"];

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

fn symbol(value: Option<&Node<Value>>) -> Option<&str> {
    match &value?.item {
        Value::Symbol(text) | Value::Const(text) | Value::Str(text) => Some(text),
        _ => None,
    }
}

pub(crate) fn is_input_object(kind: &str) -> bool {
    kind.trim_end_matches('?').chars().next().is_some_and(char::is_uppercase)
}

pub(crate) const ARG_KIND_MESSAGE: &str = "arg type must be :bool, :int, :long, :num, :string, :id, :ids or the name of a schema input object or enum like :QrCodeInput, each optionally followed by ?";

pub(crate) fn is_arg_kind(value: &Value) -> bool {
    matches!(value, Value::Symbol(kind) if ARG_KINDS.contains(&kind.trim_end_matches('?')) || is_input_object(kind))
}

pub(crate) fn resolve_result_enums(operations: &mut [OperationCall], schema: Option<&crate::schema_check::Schema>) -> Result<(), String> {
    fn walk(field: &mut FieldSpec, schema: Option<&crate::schema_check::Schema>) -> Result<(), String> {
        if let (FieldKind::Enum { values }, Some(name)) = (&mut field.kind, field.app_type.clone()) {
            let Some(schema) = schema else { return Err(format!("result field `{}` names the schema enum `{name}`: pass --schema <schema.graphql> so the generator can read it", field.name)) };
            match schema.types.get(&name) {
                Some(crate::schema_check::TypeKind::Enum(known)) => *values = known.clone(),
                _ => return Err(format!("result field `{}` names `{name}`, which is not an enum of the schema", field.name)),
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
        let name = kind.trim_end_matches('?').to_string();
        if !names.contains(&name) {
            names.push(name);
        }
    }
    names
}

fn kinds(call: &Call, file: usize, errors: &mut Vec<Found>) -> Vec<(String, String)> {
    let mut found = Vec::new();
    let mut names = HashSet::new();
    for pair in &call.options {
        if !names.insert(&pair.key) {
            errors.push((file, ParseError::new(pair.line, pair.column, format!("arg `{}` is declared twice", pair.key))));
            continue;
        }
        match &pair.value.item {
            Value::Symbol(kind) if is_arg_kind(&pair.value.item) => found.push((pair.key.clone(), kind.clone())),
            _ => errors.push((file, ParseError::new(pair.line, pair.column, ARG_KIND_MESSAGE))),
        }
    }
    found
}

const RESULT_TYPES: &str = "type must be :bool, :int, :num, :string, :id, :ids, :json or the name of a schema enum, a { ... } object, or a [ ... ] list of one type";

fn scalar_of(kind: &str) -> Option<ScalarKind> {
    match kind {
        "bool" => Some(ScalarKind::Bool),
        "int" => Some(ScalarKind::Int),
        "num" => Some(ScalarKind::Num),
        "string" => Some(ScalarKind::Str),
        "id" => Some(ScalarKind::Id),
        _ => None,
    }
}

fn result_fields(pairs: &[Pair], type_name: &str, file: usize, errors: &mut Vec<Found>) -> Vec<FieldSpec> {
    let mut fields: Vec<FieldSpec> = Vec::new();
    for pair in pairs {
        let fail = |message: &str| (file, ParseError::new(pair.value.line, pair.value.column, message));
        if fields.iter().any(|field| field.name == pair.key) {
            errors.push(fail(&format!("result field `{}` is declared twice", pair.key)));
            continue;
        }
        let nested = format!("{type_name}{}", camel(&pair.key));
        let field: Result<FieldSpec, &str> = match &pair.value.item {
            Value::Symbol(kind) if kind == "json" => Ok(FieldSpec::raw(&pair.key)),
            Value::Symbol(kind) if kind == "ids" => Ok(FieldSpec::list(&pair.key, ScalarKind::Id)),
            Value::Symbol(kind) if is_input_object(kind) => {
                let mut field = FieldSpec::enumeration(&pair.key, &[]);
                field.app_type = Some(kind.clone());
                Ok(field)
            }
            Value::Symbol(kind) => scalar_of(kind).map(|scalar| FieldSpec::scalar(&pair.key, scalar)).ok_or(RESULT_TYPES),
            Value::Hash(inner) if inner.is_empty() => Err("an object needs at least one field"),
            Value::Hash(inner) => Ok(object_of(FieldSpec::object(&pair.key), inner, &nested, file, errors)),
            Value::Array(items) => match items.as_slice() {
                [Node { item: Value::Symbol(kind), .. }] => scalar_of(kind).map(|scalar| FieldSpec::list(&pair.key, scalar)).ok_or(RESULT_TYPES),
                [Node { item: Value::Hash(inner), .. }] if !inner.is_empty() => Ok(object_of(FieldSpec::array(&pair.key), inner, &nested, file, errors)),
                _ => Err("a list holds one type: [:string] or [{ ... }] with at least one field"),
            },
            _ => Err(RESULT_TYPES),
        };
        match field {
            Ok(field) => fields.push(field.nullable()),
            Err(message) => errors.push(fail(message)),
        }
    }
    fields
}

fn object_of(mut field: FieldSpec, inner: &[Pair], type_name: &str, file: usize, errors: &mut Vec<Found>) -> FieldSpec {
    field.app_type = Some(type_name.to_string());
    field.fields = result_fields(inner, type_name, file, errors);
    field
}

fn result_root(line: &Call, operation: &str, file: usize, errors: &mut Vec<Found>) -> Option<FieldSpec> {
    let type_name = format!("{}Result", camel(operation));
    let root = object_of(FieldSpec::object(operation), &line.options, &type_name, file, errors);
    if root.fields.is_empty() { None } else { Some(root) }
}

pub(crate) fn build(nodes: &[OperationNode], errors: &mut Vec<Found>) -> Vec<OperationCall> {
    let mut seen = HashSet::new();
    let mut built = Vec::new();
    for node in nodes {
        let fail = |line: usize, column: usize, message: String| (node.file, ParseError::new(line, column, message));
        if !seen.insert(node.name.as_str()) {
            errors.push(fail(node.line, node.column, format!("operation `{}` is declared twice", node.name)));
        }
        let mut call = OperationCall { name: node.name.clone(), operation: String::new(), wrap: None, args: Vec::new(), result: None, list: false };
        let mut named = 0;
        let mut list_line = None;
        let mut lines = HashSet::new();
        for line in &node.body {
            if !lines.insert(&line.word) {
                errors.push(fail(line.line, line.column, format!("`{}` is declared twice", line.word)));
                continue;
            }
            match line.word.as_str() {
                "mutation" | "query" => {
                    named += 1;
                    match symbol(line.args.first()) {
                        Some(name) => call.operation = name.to_string(),
                        None => errors.push(fail(line.line, line.column, format!("`{}` needs the operation name, for example `{} QrCodeCreate`", line.word, line.word))),
                    }
                }
                "wrap" => match symbol(line.args.first()) {
                    Some(name) => call.wrap = Some(name.to_string()),
                    None => errors.push(fail(line.line, line.column, "`wrap` needs a variable name like `:input`".to_string())),
                },
                "args" => call.args = kinds(line, node.file, errors),
                "result" => call.result = result_root(line, &call.name, node.file, errors),
                "list" => {
                    call.list = true;
                    list_line = Some((line.line, line.column));
                }
                other => errors.push(fail(line.line, line.column, format!("unknown line `{other}` in an operation, expected one of {}", WORDS.join(", ")))),
            }
        }
        if named != 1 {
            errors.push(fail(node.line, node.column, format!("operation `{}` needs exactly one `mutation` or `query` line", node.name)));
        }
        if let (true, None, Some((line, column))) = (call.list, &call.result, list_line) {
            errors.push(fail(line, column, "`list` needs a `result` line that describes one element".to_string()));
        }
        built.push(call);
    }
    built
}
