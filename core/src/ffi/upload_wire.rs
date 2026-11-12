use serde_json::{Map, Value};

use super::DbError;
use crate::owners::write::upload::{UploadBatchInput, UploadFile, UploadItem};
use crate::prepare::{ImageFormat, PhotoEdit, VideoLimits};

#[derive(Clone, Debug, PartialEq, uniffi::Record)]
pub struct WireUploadRequest {
    pub source: WireSource,
    pub variables_json: String,
}

#[derive(Clone, Debug, PartialEq, uniffi::Record)]
pub struct WireUploadItem {
    pub values_json: String,
    pub source: WireSource,
    pub variables_json: String,
}

#[derive(Clone, Debug, PartialEq, uniffi::Enum)]
pub enum WireSource {
    File { path: String, name: Option<String>, mime: Option<String>, prepare: Option<WirePrepare> },
    Pending,
}

/// What the library makes of the source file before it uploads: `limits = None` takes `default_video_limits`, `max_bytes` is the largest file that may go to the network.
#[derive(Clone, Debug, PartialEq, uniffi::Enum)]
pub enum WirePrepare {
    Video { limits: Option<VideoLimits>, mirror: bool, on_cannot_decode: CannotDecode, max_bytes: Option<u64> },
    Image { edit: PhotoEdit, quality: Option<f64>, format: ImageFormat, max_bytes: Option<u64> },
}

impl WirePrepare {
    pub(crate) fn kind(&self) -> super::PrepareKind {
        match self {
            Self::Video { .. } => super::PrepareKind::Video,
            Self::Image { .. } => super::PrepareKind::Image,
        }
    }

    pub(crate) fn max_bytes(&self) -> Option<u64> {
        match self {
            Self::Video { max_bytes, .. } | Self::Image { max_bytes, .. } => *max_bytes,
        }
    }
}

/// What happens to a source the device cannot decode: the upload fails with a typed reason, or the source goes as it is.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, uniffi::Enum)]
pub enum CannotDecode {
    #[default]
    Fail,
    UploadSource,
}

impl WireSource {
    pub(crate) fn path(&self) -> Option<&str> {
        match self {
            Self::File { path, .. } => Some(path),
            Self::Pending => None,
        }
    }

    pub(crate) fn name(&self) -> Option<&str> {
        match self {
            Self::File { name, .. } => name.as_deref(),
            Self::Pending => None,
        }
    }

    pub(crate) fn prepare(&self) -> Option<&WirePrepare> {
        match self {
            Self::File { prepare, .. } => prepare.as_ref(),
            Self::Pending => None,
        }
    }

    pub(crate) fn mime(&self) -> Option<&str> {
        match self {
            Self::File { mime, .. } => mime.as_deref(),
            Self::Pending => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, uniffi::Record)]
pub struct WireUploadSource {
    pub path: String,
    pub name: Option<String>,
    pub mime: Option<String>,
    pub values_json: String,
    pub variables_json: String,
    #[uniffi(default = None)]
    pub prepare: Option<WirePrepare>,
}

/// A file accepted before its row: `row_id` is the id of the row `commit_staged` creates.
#[derive(Clone, Debug, uniffi::Record)]
pub struct WireStagedUpload {
    pub row_id: String,
    pub receipt: super::receipt::WireOperationReceipt,
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
        Self { source: file.source.clone(), variables_json: Value::Object(file.variables.clone()).to_string() }
    }
}

impl WireUploadRequest {
    pub fn into_file(self) -> Result<UploadFile, DbError> {
        Ok(UploadFile { source: self.source, variables: object(&self.variables_json)? })
    }
}

impl WireUploadItem {
    pub fn into_item(self) -> Result<UploadItem, DbError> {
        Ok(UploadItem { values: object(&self.values_json)?, source: self.source, variables: object(&self.variables_json)? })
    }
}

impl WireUploadBatch {
    pub fn into_input(self) -> Result<UploadBatchInput, DbError> {
        Ok(UploadBatchInput { operation: self.operation, variables: object(&self.variables_json)? })
    }
}
