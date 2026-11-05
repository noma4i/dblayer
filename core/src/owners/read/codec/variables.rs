use serde_json::{json, Map, Value};

use std::collections::BTreeMap;

use crate::schema::{PageDirection, RemoteShape, VariableSource};

pub(crate) fn mapped_variables(params: &Map<String, Value>, mapping: &BTreeMap<String, VariableSource>) -> Map<String, Value> {
    let mut variables = params.clone();
    for source in mapping.values() {
        if let VariableSource::Param { name } = source {
            variables.remove(name);
        }
    }
    variables.extend(declared_variables(params, mapping));
    variables
}

pub(crate) fn declared_variables(params: &Map<String, Value>, mapping: &BTreeMap<String, VariableSource>) -> Map<String, Value> {
    mapping.iter().filter_map(|(variable, source)| match source {
        VariableSource::Param { name } => params.get(name).map(|value| (variable.clone(), value.clone())),
        VariableSource::Constant { value } => Some((variable.clone(), value.clone())),
    }).collect()
}

pub(crate) fn load_variables(params: &Map<String, Value>, shape: &RemoteShape, page_size: u32, cursor: Option<&str>, mapping: &BTreeMap<String, VariableSource>) -> Value {
    let mut variables = mapped_variables(params, mapping);
    if let RemoteShape::Connection { cursor: Some(spec), .. } = shape {
        if let Some(cursor) = cursor {
            let value = if spec.numeric { cursor.parse::<i64>().map_or_else(|_| json!(cursor), |number| json!(number)) } else { json!(cursor) };
            variables.insert(spec.variable.clone(), value);
        }
    } else if let RemoteShape::Connection { direction, .. } = shape {
        let (size_key, cursor_key) = match direction {
            PageDirection::Forward => ("first", "after"),
            PageDirection::Backward => ("last", "before"),
        };
        variables.insert(size_key.to_string(), json!(page_size));
        if let Some(cursor) = cursor {
            variables.insert(cursor_key.to_string(), json!(cursor));
        }
    }
    Value::Object(variables)
}
