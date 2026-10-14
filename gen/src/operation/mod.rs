use std::collections::HashSet;

use dblayer::schema::FieldSpec;
use dblayer::value::ScalarKind;

use crate::build::camel;
use crate::parser::{Call, Node, OperationNode, Pair, Value};
use crate::ParseError;

type Found = (usize, ParseError);

const ARG_KINDS: [&str; 6] = ["bool", "int", "num", "string", "id", "ids"];
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

pub(crate) fn input_object_names(operations: &[OperationCall]) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    for (_, kind) in operations.iter().flat_map(|operation| &operation.args).filter(|(_, kind)| is_input_object(kind)) {
        let name = kind.trim_end_matches('?').to_string();
        if !names.contains(&name) {
            names.push(name);
        }
    }
    names
}

fn kinds(call: &Call, file: usize, errors: &mut Vec<Found>) -> Vec<(String, String)> {
    let mut found = Vec::new();
    for pair in &call.options {
        match &pair.value.item {
            Value::Symbol(kind) if ARG_KINDS.contains(&kind.trim_end_matches('?')) || is_input_object(kind) => found.push((pair.key.clone(), kind.clone())),
            _ => errors.push((file, ParseError::new(pair.line, pair.column, "type must be :bool, :int, :num, :string, :id, :ids or the name of a schema input object like :QrCodeInput, each optionally followed by ?"))),
        }
    }
    found
}

const RESULT_TYPES: &str = "type must be :bool, :int, :num, :string, :id, :ids or :json, a { ... } object, or a [ ... ] list of one type";

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
        for line in &node.body {
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
