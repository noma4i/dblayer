use serde_json::{Map, Value};

use super::DbError;
use crate::upload::{UploadBatchInput, UploadFile, UploadItem};

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
