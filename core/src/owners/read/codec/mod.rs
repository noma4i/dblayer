//! The wire form of rows and requests shared by the readers and the writers: how an answer of the server becomes normalized rows, how a request gets its variables.
//! The functions have no state and know no owner.

mod extract;
mod normalize;
mod variables;

pub(crate) use extract::{extract, root_value};
pub(crate) use normalize::{normalize_nodes, Normalized};
pub(crate) use variables::load_variables;

use serde_json::{Map, Value};

#[cfg(test)]
mod extract_tests;
#[cfg(test)]
mod normalize_tests;
#[cfg(test)]
mod variables_tests;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CodecError {
    #[error("model {0} is not registered")]
    UnknownModel(String),
    #[error("malformed response: {0}")]
    Malformed(String),
    #[error("store: {0}")]
    Store(String),
}

impl From<crate::store::StoreError> for CodecError {
    fn from(error: crate::store::StoreError) -> Self { CodecError::Store(error.to_string()) }
}

/// Writes the values of the scope into every node that does not carry them.
pub(crate) fn stamp_scope_values(mut nodes: Vec<Map<String, Value>>, values: &Map<String, Value>) -> Vec<Map<String, Value>> {
    for node in &mut nodes {
        for (column, value) in values {
            node.entry(column.clone()).or_insert_with(|| value.clone());
        }
    }
    nodes
}

/// Every combination of the choices of the parameters, at most `limit` of them, in the order of the choices.
pub(crate) fn limited_combinations(parameters: &[(&String, Vec<Value>)], at: usize, params: &mut Map<String, Value>, limit: usize, out: &mut Vec<Map<String, Value>>) {
    if out.len() >= limit { return; }
    let Some((name, choices)) = parameters.get(at) else {
        if !out.contains(params) { out.push(params.clone()); }
        return;
    };
    for choice in choices {
        params.insert((*name).clone(), choice.clone());
        limited_combinations(parameters, at + 1, params, limit, out);
        if out.len() >= limit { break; }
    }
    params.remove(name.as_str());
}
