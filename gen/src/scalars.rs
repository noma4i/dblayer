use std::collections::BTreeMap;

use crate::parser::ScalarNode;
use crate::ParseError;

const KINDS: [&str; 7] = ["string", "date", "int", "long", "num", "bool", "json"];

pub type Scalars = BTreeMap<String, String>;

pub(crate) fn build(nodes: &[ScalarNode]) -> Result<Scalars, Vec<(usize, ParseError)>> {
    let mut found = Scalars::new();
    let mut errors = Vec::new();
    for node in nodes {
        if !KINDS.contains(&node.kind.as_str()) {
            errors.push((node.file, ParseError::new(node.line, node.column, format!("scalar kind must be one of {}", KINDS.map(|kind| format!(":{kind}")).join(", ")))));
        } else if found.insert(node.name.clone(), node.kind.clone()).is_some() {
            errors.push((node.file, ParseError::new(node.line, node.column, format!("scalar `{}` is declared twice", node.name))));
        }
    }
    if errors.is_empty() { Ok(found) } else { Err(errors) }
}

pub(crate) fn unmapped(name: &str) -> String {
    format!("custom scalar `{name}` has no mapping: add `scalar :{name}, as: :string` (or :date, :int, :long, :num, :bool, :json) to a .dbl file")
}

/// Whether a scalar mapped to `mapped` fits a model field word or an operation arg kind.
pub(crate) fn fits(mapped: &str, kind: &str) -> bool {
    match mapped {
        "string" => matches!(kind, "string" | "id"),
        "date" => matches!(kind, "string" | "date"),
        "int" => matches!(kind, "int" | "id"),
        "long" => matches!(kind, "int" | "long" | "id"),
        "num" => kind == "num",
        "bool" => kind == "bool",
        "json" => matches!(kind, "object" | "array" | "raw"),
        _ => false,
    }
}
