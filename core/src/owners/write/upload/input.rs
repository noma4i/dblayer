use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadItem {
    pub values: Map<String, Value>,
    pub path: String,
    pub name: Option<String>,
    pub mime: Option<String>,
    pub variables: Map<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadBatchInput {
    pub operation: String,
    pub variables: Map<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SingleSteps {
    pub create: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultipartSteps {
    pub init: String,
    pub presign: String,
    pub complete: String,
    pub abort: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadSteps {
    pub single: Option<SingleSteps>,
    pub multipart: Option<MultipartSteps>,
    pub mutation: String,
    pub signed_id_variable: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadInput {
    pub path: String,
    pub name: Option<String>,
    pub mime: Option<String>,
    pub variables: Map<String, Value>,
    pub steps: UploadSteps,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadFile {
    pub path: String,
    pub name: Option<String>,
    pub mime: Option<String>,
    pub variables: Map<String, Value>,
}

impl UploadFile {
    pub fn new(path: &str) -> Self {
        Self { path: path.to_string(), name: None, mime: None, variables: Map::new() }
    }

    pub fn variable(mut self, name: &str, value: Value) -> Self {
        self.variables.insert(name.to_string(), value);
        self
    }

}
