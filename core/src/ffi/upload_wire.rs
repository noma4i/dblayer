use serde_json::{Map, Value};

use super::DbError;
use crate::owners::write::upload::{UploadBatchInput, UploadFile, UploadItem};

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct WireUploadRequest {
    pub source: WireSource,
    pub variables_json: String,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct WireUploadItem {
    pub values_json: String,
    pub source: WireSource,
    pub variables_json: String,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum WireSource {
    File { path: String, name: Option<String>, mime: Option<String> },
    Pending,
}

impl WireSource {
    fn parts(self) -> (Option<String>, Option<String>, Option<String>) {
        match self {
            Self::File { path, name, mime } => (Some(path), name, mime),
            Self::Pending => (None, None, None),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct WireUploadSource {
    pub path: String,
    pub name: Option<String>,
    pub mime: Option<String>,
    pub values_json: String,
    pub variables_json: String,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct WireUploadBatch {
    pub operation: String,
    pub variables_json: String,
}

pub(super) fn object(json: &str) -> Result<Map<String, Value>, DbError> {
    match serde_json::from_str(json).map_err(DbError::from)? {
        Value::Object(map) => Ok(map),
        _ => Err(DbError::Invalid { detail: "upload values must be a JSON object".into() }),
    }
}

#[cfg(test)]
impl WireUploadRequest {
    pub fn of(file: &UploadFile) -> Self {
        let source = file.path.clone().map(|path| WireSource::File { path, name: file.name.clone(), mime: file.mime.clone() }).unwrap_or(WireSource::Pending);
        Self { source, variables_json: Value::Object(file.variables.clone()).to_string() }
    }
}

impl WireUploadRequest {
    pub fn into_file(self) -> Result<UploadFile, DbError> {
        let (path, name, mime) = self.source.parts();
        Ok(UploadFile { path, name, mime, variables: object(&self.variables_json)? })
    }
}

impl WireUploadItem {
    pub fn into_item(self) -> Result<UploadItem, DbError> {
        let (path, name, mime) = self.source.parts();
        Ok(UploadItem { values: object(&self.values_json)?, path, name, mime, variables: object(&self.variables_json)? })
    }
}

impl WireUploadBatch {
    pub fn into_input(self) -> Result<UploadBatchInput, DbError> {
        Ok(UploadBatchInput { operation: self.operation, variables: object(&self.variables_json)? })
    }
}
