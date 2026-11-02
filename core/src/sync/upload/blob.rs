use std::sync::Arc;
use rusqlite::OptionalExtension;
use crate::commit::{CommitEnvelope, OperationRecord, OperationTransition, WriteOrigin};
use crate::operation::OperationStatus;
use crate::upload::{BlobSnapshot, BlobState, UploadFile, UploadState, UploadOperations, UploadOperation};
use super::super::{locked, Inner, Sync, SyncError};

impl Inner {
    pub(crate) async fn expire_blobs(self: &Arc<Self>) -> Result<(), SyncError> {
        let failed_due = self.now_ms().saturating_sub(self.config.upload.failed_ttl.as_millis().try_into().unwrap_or(i64::MAX));
        let inner = self.clone();
        tokio::task::spawn_blocking(move || {
            inner.db.expire_blob_cache(failed_due)?;
            Ok::<_, SyncError>(())
        }).await.map_err(|error| SyncError::Store(error.to_string()))?
    }

    pub(super) fn blob_id(&self, seq: i64) -> Result<Option<String>, SyncError> {
        Ok(self.db.shared.read(&[], |reader| Ok(reader.connection().query_row("SELECT id FROM blob_upload WHERE seq = ?1", [seq], |row| row.get(0)).optional()?))?)
    }
}

impl Sync {
    pub async fn upload_blob(&self, name: &str, create: &str, file: UploadFile) -> Result<String, SyncError> {
        if !locked(&self.inner.operations).contains_key(create) { return Err(SyncError::UnknownOperation(create.into())); }
        let inner = self.inner.clone();
        let generation = inner.db.generation();
        let (name, create) = (name.to_owned(), create.to_owned());
        tokio::task::spawn_blocking(move || {
            let uploads = UploadOperations { create_blob: Some(UploadOperation::new(&create)), multipart: inner.config.uploads.as_ref().and_then(|uploads| uploads.multipart.clone()) };
            uploads.check(|name| locked(&inner.operations).contains_key(name)).map_err(SyncError::Params)?;
            let input = file.with_steps(uploads.steps(""));
            let id = uuid::Uuid::new_v4().to_string();
            let seq = inner.db.prepare_blob(generation, |files| {
                let item = files.prepare("", crate::identity::RowId::new(&id).map_err(|error| SyncError::Params(error.to_string()))?, id.clone(), input.clone(), UploadState::queued())?;
                let record = OperationRecord { seq: None, action_key: format!("@blob/{name}"), status: OperationStatus::Pending, row_ids: Vec::new(), temp_id: None, input: serde_json::Value::Null, transport_started: false, base_revision: Some(inner.db.revision_seq()?), created_at: inner.now_ms(), dedupe_key: None, rollback: None, upload: None };
                let result = inner.commit(generation, |_| {
                    let mut envelope = CommitEnvelope::new(WriteOrigin::Optimistic, generation);
                    envelope.operation_transitions.push(OperationTransition::BeginBlob { record: Box::new(record.clone()), item: Box::new(item.clone()), id: id.clone(), name: name.clone() });
                    Ok(Some(envelope))
                })?;
                result.and_then(|result| result.operations.into_iter().next()).ok_or(SyncError::Cancelled)
            })?;
            inner.start_uploads()?;
            inner.db.report_upload_accepted("", vec![crate::db::AcceptedUpload { seq, position: 0 }]);
            Ok(id)
        }).await.map_err(|error| SyncError::Store(error.to_string()))?
    }

    pub fn blob_snapshot(&self, id: &str) -> Result<BlobSnapshot, SyncError> { Ok(self.inner.db.blob_snapshot(id)?) }

    pub async fn wait_blob(&self, id: &str) -> Result<String, SyncError> {
        let mut changes = self.inner.db.outbox_changes();
        loop {
            changes.borrow_and_update();
            let blob = self.blob_snapshot(id)?;
            match blob.state {
                BlobState::Uploaded | BlobState::Consumed => return blob.signed_id.ok_or_else(|| SyncError::Malformed("uploaded Blob has no signedId".into())),
                BlobState::Uploading => {},
                _ => return Err(SyncError::Params(blob.upload.error.map_or_else(|| format!("Blob is {:?}", blob.state), |error| error.message))),
            }
            changes.changed().await.map_err(|_| SyncError::Cancelled)?;
        }
    }

    pub async fn retry_blob(&self, id: &str) -> Result<(), SyncError> {
        self.inner.expire_blobs().await?;
        let blob = self.blob_snapshot(id)?;
        if blob.state != BlobState::Failed { return Err(SyncError::Params("only a failed Blob can retry".into())); }
        let generation = self.inner.db.generation();
        self.inner.commit(generation, |reader| {
            let seq = reader.connection().query_row("SELECT seq FROM blob_upload WHERE id = ?1", [id], |row| row.get::<_, i64>(0)).map_err(crate::store::StoreError::from)?;
            let mut envelope = CommitEnvelope::new(WriteOrigin::Optimistic, generation);
            let mut state = blob.upload.clone();
            state.background_refused = None;
            if state.phase == crate::upload::UploadStatus::Failed {
                state.apply(crate::upload::UploadEvent::Retried).map_err(|error| SyncError::Malformed(error.to_string()))?;
            }
            envelope.operation_transitions.push(OperationTransition::Progress { seq, item_position: 0, state: Box::new(state) });
            envelope.operation_transitions.push(OperationTransition::Retry { seq });
            Ok(Some(envelope))
        })?;
        self.inner.start_uploads()
    }

    pub async fn discard_blob(&self, id: &str) -> Result<(), SyncError> {
        let blob = self.blob_snapshot(id)?;
        if blob.consumer.is_some() { return Err(SyncError::Params("Blob is pinned to an operation".into())); }
        let files: Vec<String> = self.inner.db.shared.read(&[], |reader| Ok(reader.connection().prepare("SELECT i.file_id FROM blob_upload b JOIN op_item i ON i.seq = b.seq WHERE b.id = ?1 AND i.file_id IS NOT NULL")?.query_map([id], |row| row.get(0))?.collect::<Result<_, _>>()?))?;
        let seq = self.inner.db.shared.read(&[], |reader| Ok(reader.connection().query_row("SELECT seq FROM blob_upload WHERE id = ?1", [id], |row| row.get::<_, Option<i64>>(0))?))?;
        if let Some(seq) = seq {
            let running = self.inner.running_uploads(seq, None);
            for (token, _) in &running { token.cancel(); }
            let waiting = running.into_iter().map(|(_, done)| done);
            for mut done in waiting { let _ = done.wait_for(|done| *done).await; }
        }
        let generation = self.inner.db.generation();
        self.inner.commit(generation, |_| {
            let mut envelope = CommitEnvelope::new(WriteOrigin::Optimistic, generation);
            envelope.operation_transitions.push(OperationTransition::RetireBlob { id: id.into(), expired: false });
            Ok(Some(envelope))
        })?;
        self.inner.spawn_upload_housekeeping(false);
        self.inner.discard_files(files).await?;
        Ok(())
    }
}
