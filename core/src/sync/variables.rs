use serde_json::{json, Map, Value};

use std::collections::BTreeMap;

use crate::schema::{PageDirection, RemoteShape, VariableSource};

pub(crate) fn load_variables(params: &Map<String, Value>, shape: &RemoteShape, page_size: u32, cursor: Option<&str>, mapping: &BTreeMap<String, VariableSource>) -> Value {
    let mut variables = params.clone();
    for (name, source) in mapping {
        match source {
            VariableSource::Param { name: param } => {
                if let Some(value) = variables.remove(param) {
                    variables.insert(name.clone(), value);
                }
            }
            VariableSource::Constant { value } => {
                variables.insert(name.clone(), value.clone());
            }
        }
    }
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
