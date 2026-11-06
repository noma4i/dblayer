use super::UploadState;

pub(crate) const BLOB_LIFETIME_MS: i64 = 7 * 24 * 60 * 60 * 1000;

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
