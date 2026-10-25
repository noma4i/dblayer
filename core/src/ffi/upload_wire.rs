use serde_json::{Map, Value};

use super::DbError;
use crate::upload::{MultipartOperations, UploadBatchInput, UploadFile, UploadItem, UploadOperation, UploadOperations};

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct WireUploadOperation {
    pub name: String,
    pub root_path: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct WireMultipartOperations {
    pub initiate: WireUploadOperation,
    pub presign: WireUploadOperation,
    pub complete: WireUploadOperation,
    pub abort: WireUploadOperation,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct WireUploadOperations {
    pub create_blob: Option<WireUploadOperation>,
    pub multipart: Option<WireMultipartOperations>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct WireUploadRequest {
    pub path: String,
    pub name: Option<String>,
    pub mime: Option<String>,
    pub variables_json: String,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct WireUploadItem {
    pub values_json: String,
    pub path: String,
    pub name: Option<String>,
    pub mime: Option<String>,
    pub variables_json: String,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct WireUploadBatch {
    pub operation: String,
    pub variables_json: String,
}

fn object(json: &str) -> Result<Map<String, Value>, DbError> {
    match serde_json::from_str(json).map_err(DbError::from)? {
        Value::Object(map) => Ok(map),
        _ => Err(DbError::Invalid { detail: "upload values must be a JSON object".into() }),
    }
}

impl From<WireUploadOperation> for UploadOperation {
    fn from(wire: WireUploadOperation) -> Self {
        Self { name: wire.name, root_path: wire.root_path }
    }
}

impl From<WireUploadOperations> for UploadOperations {
    fn from(wire: WireUploadOperations) -> Self {
        Self {
            create_blob: wire.create_blob.map(Into::into),
            multipart: wire.multipart.map(|steps| MultipartOperations { initiate: steps.initiate.into(), presign: steps.presign.into(), complete: steps.complete.into(), abort: steps.abort.into() }),
        }
    }
}

#[cfg(test)]
impl WireUploadRequest {
    pub fn of(file: &UploadFile) -> Self {
        Self { path: file.path.clone(), name: file.name.clone(), mime: file.mime.clone(), variables_json: Value::Object(file.variables.clone()).to_string() }
    }
}

impl WireUploadRequest {
    pub fn into_file(self) -> Result<UploadFile, DbError> {
        Ok(UploadFile { path: self.path, name: self.name, mime: self.mime, variables: object(&self.variables_json)? })
    }
}

impl WireUploadItem {
    pub fn into_item(self) -> Result<UploadItem, DbError> {
        Ok(UploadItem { values: object(&self.values_json)?, path: self.path, name: self.name, mime: self.mime, variables: object(&self.variables_json)? })
    }
}

impl WireUploadBatch {
    pub fn into_input(self) -> Result<UploadBatchInput, DbError> {
        Ok(UploadBatchInput { operation: self.operation, variables: object(&self.variables_json)? })
    }
}
