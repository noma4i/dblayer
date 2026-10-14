use std::collections::HashSet;

use crate::parser::{Call, Node, OperationNode, Value};
use crate::ParseError;

type Found = (usize, ParseError);

const ARG_KINDS: [&str; 6] = ["bool", "int", "num", "string", "id", "ids"];
const WORDS: [&str; 5] = ["mutation", "query", "wrap", "args", "result"];

/// A server operation that no model owns: a typed call that sends it and reads the declared fields of its answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationCall {
    pub name: String,
    pub operation: String,
    pub wrap: Option<String>,
    pub args: Vec<(String, String)>,
    pub result: Vec<(String, String)>,
}

fn symbol(value: Option<&Node<Value>>) -> Option<&str> {
    match &value?.item {
        Value::Symbol(text) | Value::Const(text) | Value::Str(text) => Some(text),
        _ => None,
    }
}

fn kinds(call: &Call, file: usize, errors: &mut Vec<Found>) -> Vec<(String, String)> {
    let mut found = Vec::new();
    for pair in &call.options {
        match &pair.value.item {
            Value::Symbol(kind) if ARG_KINDS.contains(&kind.trim_end_matches('?')) => found.push((pair.key.clone(), kind.clone())),
            _ => errors.push((file, ParseError::new(pair.line, pair.column, "type must be :bool, :int, :num, :string, :id or :ids, each optionally followed by ?"))),
        }
    }
    found
}

pub(crate) fn build(nodes: &[OperationNode], errors: &mut Vec<Found>) -> Vec<OperationCall> {
    let mut seen = HashSet::new();
    let mut built = Vec::new();
    for node in nodes {
        let fail = |line: usize, column: usize, message: String| (node.file, ParseError::new(line, column, message));
        if !seen.insert(node.name.as_str()) {
            errors.push(fail(node.line, node.column, format!("operation `{}` is declared twice", node.name)));
        }
        let mut call = OperationCall { name: node.name.clone(), operation: String::new(), wrap: None, args: Vec::new(), result: Vec::new() };
        let mut named = 0;
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
                "result" => call.result = kinds(line, node.file, errors),
                other => errors.push(fail(line.line, line.column, format!("unknown line `{other}` in an operation, expected one of {}", WORDS.join(", ")))),
            }
        }
        if named != 1 {
            errors.push(fail(node.line, node.column, format!("operation `{}` needs exactly one `mutation` or `query` line", node.name)));
        }
        built.push(call);
    }
    built
}
