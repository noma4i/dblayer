use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

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
    pub blob_id_variable: Option<String>,
}

impl UploadSteps {
    pub fn new(mutation: &str) -> Self {
        Self {
            single: None,
            multipart: None,
            mutation: mutation.to_string(),
            signed_id_variable: "signedId".to_string(),
            blob_id_variable: Some("blobId".to_string()),
        }
    }

    pub fn single(mut self, create: &str) -> Self {
        self.single = Some(SingleSteps { create: create.to_string() });
        self
    }

    pub fn multipart(mut self, init: &str, presign: &str, complete: &str, abort: &str) -> Self {
        self.multipart = Some(MultipartSteps {
            init: init.to_string(),
            presign: presign.to_string(),
            complete: complete.to_string(),
            abort: abort.to_string(),
        });
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadInput {
    pub path: String,
    pub name: Option<String>,
    pub mime: Option<String>,
    pub variables: Map<String, Value>,
    pub steps: UploadSteps,
}

impl UploadInput {
    pub fn new(path: &str, steps: UploadSteps) -> Self {
        Self { path: path.to_string(), name: None, mime: None, variables: Map::new(), steps }
    }

    pub fn variable(mut self, name: &str, value: Value) -> Self {
        self.variables.insert(name.to_string(), value);
        self
    }
}
