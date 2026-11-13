use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::ffi::upload_wire::WireSource;

#[derive(Debug, Clone, PartialEq)]
pub struct UploadItem {
    pub values: Map<String, Value>,
    pub source: WireSource,
    pub variables: Map<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadBatchInput {
    pub operation: String,
    pub variables: Map<String, Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UploadFile {
    pub source: WireSource,
    pub variables: Map<String, Value>,
}

impl UploadFile {
    pub fn new(path: &str) -> Self {
        Self { source: WireSource::File { path: path.to_string(), name: None, mime: None, prepare: None, max_bytes: None }, variables: Map::new() }
    }

    pub fn variable(mut self, name: &str, value: Value) -> Self {
        self.variables.insert(name.to_string(), value);
        self
    }

}
