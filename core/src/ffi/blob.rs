use std::sync::{Arc, Mutex};
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

#[derive(uniffi::Object)]
pub struct BlobChanges {
    registration: std::sync::OnceLock<super::open_streams::Registration>,
    sync: crate::sync::Sync,
    id: String,
    changes: tokio::sync::Mutex<tokio::sync::watch::Receiver<u64>>,
    last: Mutex<Option<WireBlobSnapshot>>,
    cancel: crate::transport::CancelToken,
}

#[uniffi::export]
impl BlobChanges {
    pub async fn next(self: Arc<Self>) -> Result<Option<WireBlobSnapshot>, DbError> {
        super::database::on_executor(async move {
            let mut changes = self.changes.lock().await;
            loop {
                if self.cancel.is_cancelled() { return Ok::<_, DbError>(None); }
                changes.borrow_and_update();
                let next: WireBlobSnapshot = self.sync.blob_snapshot(&self.id).map_err(DbError::from)?.into();
                let changed = {
                    let mut last = self.last.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                    if last.as_ref() == Some(&next) { false } else { *last = Some(next.clone()); true }
                };
                if changed { return Ok(Some(next)); }
                tokio::select! { _ = self.cancel.cancelled() => return Ok(None), changed = changes.changed() => if changed.is_err() { return Ok(None); } }
            }
        }).await
    }
    pub fn cancel(&self) { self.cancel.cancel(); }
}

impl super::open_streams::Closable for BlobChanges {
    fn close(&self) { self.cancel.cancel(); }
    fn registration(&self) -> &std::sync::OnceLock<super::open_streams::Registration> { &self.registration }
}

impl Drop for BlobChanges { fn drop(&mut self) { self.cancel.cancel(); } }

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
    pub async fn blob_changes(self: Arc<Self>, id: String) -> Result<Arc<BlobChanges>, DbError> {
        let sync = self.sync()?.clone();
        let changes = self.db.outbox_changes();
        let changes = super::database::blocking(move || {
            sync.blob_snapshot(&id)?;
            Ok(Arc::new(BlobChanges { sync, id, changes: tokio::sync::Mutex::new(changes), last: Mutex::new(None), cancel: crate::transport::CancelToken::new(), registration: Default::default() }))
        }).await?;
        self.streams.enroll(&changes);
        Ok(changes)
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
