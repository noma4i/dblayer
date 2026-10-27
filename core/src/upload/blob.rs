use serde::{Deserialize, Serialize};

use super::UploadState;

pub(crate) const BLOB_LIFETIME_MS: i64 = 7 * 24 * 60 * 60 * 1000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UploadOwner {
    Action { model: String },
    Blob { id: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobUploadDeclaration {
    pub name: String,
    pub create: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum BlobState { Uploading, Uploaded, Failed, Expired, Discarded, Consumed }

#[derive(Debug, Clone, PartialEq)]
pub struct BlobSnapshot {
    pub id: String,
    pub state: BlobState,
    pub upload: UploadState,
    pub signed_id: Option<String>,
    pub consumer: Option<i64>,
}
