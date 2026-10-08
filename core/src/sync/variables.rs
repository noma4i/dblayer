use serde_json::{json, Map, Value};

use crate::schema::{PageDirection, RemoteShape};

pub(crate) fn load_variables(params: &Map<String, Value>, shape: &RemoteShape, page_size: u32, cursor: Option<&str>) -> Value {
    let mut variables = params.clone();
    if let RemoteShape::Connection { direction, .. } = shape {
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
