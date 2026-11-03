use std::sync::Arc;
use crate::upload::{BlobSnapshot, BlobState};
use super::{Database, DbError};

#[derive(Clone, Debug, PartialEq, uniffi::Record)]
pub struct WireBlobSnapshot {
    pub id: String,
    pub state: BlobState,
    pub progress: f64,
    pub sent_bytes: u64,
    pub total_bytes: u64,
    pub error: Option<String>,
    pub signed_id: Option<String>,
}

impl From<BlobSnapshot> for WireBlobSnapshot {
    fn from(blob: BlobSnapshot) -> Self {
        let upload = blob.upload;
        Self { id: blob.id, state: blob.state, progress: if upload.total_bytes == 0 { 0.0 } else { (upload.sent_bytes as f64 / upload.total_bytes as f64).min(1.0) }, sent_bytes: upload.sent_bytes, total_bytes: upload.total_bytes, error: upload.error.map(|failure| failure.message).or(upload.background_refused), signed_id: blob.signed_id }
    }
}

#[uniffi::export]
impl Database {
    pub async fn upload_blob(self: Arc<Self>, name: String, create: String, request: super::upload_wire::WireUploadRequest) -> Result<WireBlobSnapshot, DbError> {
        let sync = self.sync()?.clone();
        super::database::on_executor(async move {
            let id = sync.upload_blob(&name, &create, request.into_file()?).await.map_err(DbError::from)?;
            Ok::<_, DbError>(sync.blob_snapshot(&id)?.into())
        }).await
    }
    pub async fn blob(self: Arc<Self>, id: String) -> Result<WireBlobSnapshot, DbError> {
        super::database::blocking(move || Ok(self.sync()?.blob_snapshot(&id)?.into())).await
    }
    pub async fn wait_blob(self: Arc<Self>, id: String) -> Result<String, DbError> {
        let sync = self.sync()?.clone();
        super::database::on_executor(async move { sync.wait_blob(&id).await }).await
    }
    pub async fn retry_blob(self: Arc<Self>, id: String) -> Result<(), DbError> {
        let sync = self.sync()?.clone();
        super::database::on_executor(async move { sync.retry_blob(&id).await }).await
    }
    pub async fn discard_blob(self: Arc<Self>, id: String) -> Result<(), DbError> {
        let sync = self.sync()?.clone();
        super::database::on_executor(async move { sync.discard_blob(&id).await }).await
    }
}
