use serde_json::{Map, Value};

use super::DbError;
use crate::upload::{MultipartSteps, SingleSteps, UploadBatchInput, UploadInput, UploadItem, UploadSteps};

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct WireMultipart {
    pub initiate: String,
    pub presign: String,
    pub complete: String,
    pub abort: String,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct WireUploadRequest {
    pub path: String,
    pub name: Option<String>,
    pub mime: Option<String>,
    pub variables_json: String,
    pub create_blob: Option<String>,
    pub multipart: Option<WireMultipart>,
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
    pub create_blob: Option<String>,
    pub multipart: Option<WireMultipart>,
}

fn object(json: &str) -> Result<Map<String, Value>, DbError> {
    match serde_json::from_str(json).map_err(DbError::from)? {
        Value::Object(map) => Ok(map),
        _ => Err(DbError::Invalid { detail: "upload values must be a JSON object".into() }),
    }
}

fn steps(create_blob: Option<String>, multipart: Option<WireMultipart>) -> UploadSteps {
    UploadSteps {
        single: create_blob.map(|create| SingleSteps { create }),
        multipart: multipart.map(|steps| MultipartSteps { init: steps.initiate, presign: steps.presign, complete: steps.complete, abort: steps.abort }),
        mutation: String::new(),
        signed_id_variable: "signedId".to_string(),
        blob_id_variable: None,
    }
}

#[cfg(test)]
impl WireUploadRequest {
    pub fn of(input: &UploadInput) -> Self {
        Self {
            path: input.path.clone(),
            name: input.name.clone(),
            mime: input.mime.clone(),
            variables_json: Value::Object(input.variables.clone()).to_string(),
            create_blob: input.steps.single.as_ref().map(|single| single.create.clone()),
            multipart: input.steps.multipart.as_ref().map(|steps| WireMultipart { initiate: steps.init.clone(), presign: steps.presign.clone(), complete: steps.complete.clone(), abort: steps.abort.clone() }),
        }
    }
}

impl WireUploadRequest {
    pub fn into_input(self) -> Result<UploadInput, DbError> {
        Ok(UploadInput { path: self.path, name: self.name, mime: self.mime, variables: object(&self.variables_json)?, steps: steps(self.create_blob, self.multipart) })
    }
}

impl WireUploadItem {
    pub fn into_item(self) -> Result<UploadItem, DbError> {
        Ok(UploadItem { values: object(&self.values_json)?, path: self.path, name: self.name, mime: self.mime, variables: object(&self.variables_json)? })
    }
}

impl WireUploadBatch {
    pub fn into_input(self) -> Result<UploadBatchInput, DbError> {
        Ok(UploadBatchInput { operation: self.operation, steps: steps(self.create_blob, self.multipart), variables: object(&self.variables_json)? })
    }
}
