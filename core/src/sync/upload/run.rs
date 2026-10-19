use std::path::Path;
use std::sync::{Arc, Mutex};

use serde_json::json;
use tokio::sync::Semaphore;

use crate::commit::{CommitEnvelope, OperationStatus, OperationTransition, WriteOrigin};
use crate::operation::Operation;
use crate::syncstate::{FailureKind, OperationFailure};
use crate::transport::{CancelToken, Response, TransportError};
use crate::upload::{
    file_len, md5_base64, mime_of, scenario_for, TransferProgress, UploadInput, UploadScenario, UploadStage, UploadState, UploadStatus,
};

use super::super::send::Sending;
use super::super::{locked, Inner};
use super::steps::{text, StepError, Stop, EXPIRED_MESSAGE};

#[derive(Clone, Copy)]
pub(super) enum Mode {
    Transfer,
    Orphan,
    Cancel,
}

pub(super) struct Runner {
    pub(super) inner: Arc<Inner>,
    pub(super) seq: i64,
    pub(super) position: i64,
    pub(super) run: u64,
    pub(super) token: CancelToken,
    pub(super) input: UploadInput,
    source_error: Option<String>,
    file_id: Option<String>,
    model: String,
    state: Mutex<UploadState>,
    progress: Mutex<TransferProgress>,
    pub(super) resumed: Mutex<Option<Result<Response, TransportError>>>,
}

impl Drop for Runner {
    fn drop(&mut self) {
        if let Some(id) = &self.file_id {
            let missing = self.inner.db.shared.read(&[], |reader| {
                Ok(!reader.connection().query_row("SELECT EXISTS (SELECT 1 FROM file WHERE id = ?1)", [id], |row| row.get::<_, bool>(0))?)
            });
            if missing.is_ok_and(|missing| missing) { self.inner.db.shared.files.discard(std::slice::from_ref(id)); }
        }
    }
}

pub(super) fn stop_of(error: StepError) -> Stop {
    match error {
        StepError::Retry(failure) | StepError::Fatal(failure) => Stop::Failed(failure),
        StepError::Expired => Stop::Failed(OperationFailure::new(FailureKind::Rejected, EXPIRED_MESSAGE)),
        StepError::Cancelled => Stop::Cancelled,
    }
}

pub(super) async fn drive(inner: Arc<Inner>, seq: i64, position: i64, run: u64, token: CancelToken, permits: Arc<Semaphore>, mode: Mode, resumed: Option<Result<Response, TransportError>>) {
    let generation = inner.db.generation();
    let runner = match Runner::load(inner.clone(), seq, position, run, token, matches!(mode, Mode::Transfer)) {
        Ok(Some(runner)) => runner,
        Ok(None) => return,
        Err(error) => {
            let _ = inner.commit(generation, |_| {
                if !inner.is_current(seq, position, run) { return Ok(None); }
                let mut envelope = CommitEnvelope::new(WriteOrigin::Response, generation);
                envelope.operation_transitions.push(OperationTransition::Fail { seq, status: OperationStatus::Failed, failure: OperationFailure::new(FailureKind::Unrecognized, error.to_string()) });
                Ok(Some(envelope))
            });
            return;
        }
    };
    let runner = Arc::new(runner);
    *locked(&runner.resumed) = resumed;
    match mode {
        Mode::Transfer => runner.transfer(permits).await,
        Mode::Orphan => runner.abort_remote().await,
        Mode::Cancel => runner.cancelled().await,
    }
}

impl Runner {
    fn load(inner: Arc<Inner>, seq: i64, position: i64, run: u64, token: CancelToken, transfer: bool) -> Result<Option<Self>, super::super::SyncError> {
        let Some(entry) = inner.db.outbox()?.into_iter().find(|entry| entry.operation.id.0 == seq) else { return Ok(None) };
        let (model, _) = Operation::split_action_key(&entry.operation.action_key).ok_or_else(|| super::super::SyncError::Malformed("the upload action key is invalid".into()))?;
        let Some(state) = inner.upload_items()?.remove(&(seq, position)) else { return Ok(None) };
        let needs_file = transfer && !state.is_transferred();
        let (mut input, file, source_error, file_id) = inner.db.shared.read(&[], |reader| {
            let connection = reader.connection();
            let value = crate::operation::read_item_value(connection, seq, position, "input")?.unwrap_or(serde_json::Value::Null);
            let input: UploadInput = serde_json::from_value(value).map_err(|error| crate::store::StoreError::from(std::io::Error::other(error)))?;
            let (id, error): (Option<String>, Option<String>) = connection.query_row("SELECT file_id, source_error FROM op_item WHERE seq = ?1 AND position = ?2", rusqlite::params![seq, position], |row| Ok((row.get(0)?, row.get(1)?)))?;
            let file = if needs_file {
                id.as_ref().map(|id| connection.query_row("SELECT name, data FROM file WHERE id = ?1", [id], |row| Ok((id.clone(), row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?)))).transpose()?
            } else { None };
            Ok((input, file, error, id))
        })?;
        if let Some((id, name, bytes)) = file {
            input.path = inner.db.shared.files.materialize(&id, &name, &bytes).map_err(|error| super::super::SyncError::Store(error.to_string()))?.to_string_lossy().into_owned();
        }
        let progress = TransferProgress::new(state.total_bytes, inner.config.upload.tick);
        Ok(Some(Self { inner, seq, position, run, token, input, source_error, file_id, model, state: Mutex::new(state), progress: Mutex::new(progress), resumed: Mutex::new(None) }))
    }

    pub(super) fn snapshot(&self) -> UploadState {
        locked(&self.state).clone()
    }

    pub(super) fn commit(&self, build: impl FnOnce(&mut CommitEnvelope)) -> Result<(), Stop> {
        let generation = self.inner.db.generation();
        let committed = self.inner.commit(generation, |_| {
            if !self.inner.is_current(self.seq, self.position, self.run) {
                return Ok(None);
            }
            let mut envelope = CommitEnvelope::new(WriteOrigin::Response, generation);
            build(&mut envelope);
            Ok(Some(envelope))
        });
        match committed {
            Ok(Some(result)) if !result.is_noop() => Ok(()),
            Ok(_) => Err(Stop::Superseded),
            Err(error) => Err(Stop::Failed(OperationFailure::new(FailureKind::Unrecognized, error.to_string()))),
        }
    }

    pub(super) fn save(&self, edit: impl FnOnce(&mut UploadState)) -> Result<(), Stop> {
        let mut state = locked(&self.state);
        edit(&mut state);
        let snapshot = state.clone();
        self.commit(|envelope| envelope.operation_transitions.push(OperationTransition::Progress { seq: self.seq, item_position: self.position, state: snapshot }))
    }

    pub(super) fn report(&self, part: u32, bytes: u64) {
        let due = {
            let mut progress = locked(&self.progress);
            progress.report(part, bytes);
            progress.due(self.inner.clock.now())
        };
        if let Some(sent) = due {
            let _ = self.save(|state| state.sent_bytes = state.sent_bytes.max(sent));
        }
    }

    pub(super) fn progress(&self) -> std::sync::MutexGuard<'_, TransferProgress> {
        locked(&self.progress)
    }

    async fn transfer(self: &Arc<Self>, permits: Arc<Semaphore>) {
        let transferred = self.transfer_file(permits).await;
        let result = match transferred {
            Ok(()) => self.mutation().await,
            Err(stop) => Err(stop),
        };
        match result {
            Ok(()) | Err(Stop::Superseded) => {}
            Err(Stop::Cancelled) => self.cancelled().await,
            Err(Stop::Failed(failure)) => self.failed(failure).await,
            Err(Stop::Expired) => self.failed(OperationFailure::new(FailureKind::Rejected, EXPIRED_MESSAGE)).await,
        }
    }

    async fn transfer_file(self: &Arc<Self>, permits: Arc<Semaphore>) -> Result<(), Stop> {
        if self.snapshot().is_transferred() {
            return Ok(());
        }
        if let Some(error) = &self.source_error {
            return Err(Stop::Failed(OperationFailure::new(FailureKind::Rejected, format!("source file is unreadable: {error}"))));
        }
        if let Some(deadline) = self.snapshot().retry_at {
            let delay = std::time::Duration::from_millis(deadline).saturating_sub(self.inner.clock.now());
            tokio::select! {
                () = tokio::time::sleep(delay) => {}
                () = self.token.cancelled() => return Err(Stop::Cancelled),
            }
        }
        let permit = tokio::select! {
            permit = permits.acquire_owned() => permit.ok(),
            () = self.token.cancelled() => None,
        };
        let Some(_permit) = permit else { return Err(Stop::Cancelled) };
        self.save(|state| {
            state.phase = UploadStatus::Preparing;
            state.stage = UploadStage::Prepare;
            state.retries = 0;
            state.retry_at = None;
            state.error = None;
        })?;
        let total = file_len(Path::new(&self.input.path))
            .map_err(|error| Stop::Failed(OperationFailure::new(FailureKind::Rejected, format!("source file is unreadable: {error}"))))?;
        let stored = self.snapshot();
        if stored.upload_id.is_some() && stored.total_bytes != 0 && stored.total_bytes != total {
            self.abort_remote().await;
        }
        *locked(&self.progress) = TransferProgress::new(total, self.inner.config.upload.tick);
        let scenario = match self.snapshot() {
            state if state.upload_id.is_some() => UploadScenario::Multipart,
            _ => scenario_for(total, self.inner.config.upload.multipart_threshold),
        };
        self.save(|state| {
            state.total_bytes = total;
            state.phase = UploadStatus::Uploading;
            state.scenario = Some(scenario);
        })?;
        if !self.snapshot().is_transferred() {
            match scenario {
                UploadScenario::Single => self.single(total).await?,
                UploadScenario::Multipart => self.multipart(total).await?,
            }
        }
        Ok(())
    }

    pub(super) fn file_name(&self) -> String {
        self.input.name.clone().unwrap_or_else(|| {
            Path::new(&self.input.path).file_name().map_or_else(|| self.input.path.clone(), |name| name.to_string_lossy().into_owned())
        })
    }

    pub(super) fn content_type(&self) -> String {
        self.input.mime.clone().unwrap_or_else(|| mime_of(&self.file_name()).to_string())
    }

    async fn single(self: &Arc<Self>, total: u64) -> Result<(), Stop> {
        let steps = self
            .input
            .steps
            .single
            .clone()
            .ok_or_else(|| Stop::Failed(OperationFailure::new(FailureKind::Rejected, "the upload has no single-upload steps")))?;
        let checksum = md5_base64(Path::new(&self.input.path))
            .map_err(|error| Stop::Failed(OperationFailure::new(FailureKind::Rejected, format!("source file is unreadable: {error}"))))?;
        let mut renewals = 0;
        loop {
            if self.snapshot().put_url.is_none() {
                self.save(|state| state.stage = UploadStage::Create)?;
                let variables = json!({
                    "filename": self.file_name(),
                    "contentType": self.content_type(),
                    "byteSize": total,
                    "checksum": checksum,
                });
                let data = self.retrying(|| self.graphql(&steps.create, variables.clone(), true)).await?;
                let url = text(&data, "url").map_err(stop_of)?;
                let headers = crate::upload::parse_headers(data.get("headers"));
                let (signed, blob) = (text(&data, "signedId").map_err(stop_of)?, text(&data, "blobId").map_err(stop_of)?);
                self.save(|state| {
                    state.signed_id = Some(signed);
                    state.blob_id = Some(blob);
                    state.put_url = Some(url);
                    state.headers = headers;
                    state.checksum = Some(checksum.clone());
                    state.url_issued_at = Some(self.inner.clock.now().as_millis() as u64);
                })?;
            }
            let grant = self.snapshot();
            let url = grant.put_url.ok_or(Stop::Superseded)?;
            let headers = grant.headers;
            self.save(|state| state.stage = UploadStage::Put)?;
            self.progress().begin_phase([]);
            match self.retrying(|| self.put_once(&url, &headers, 0, total, 1)).await {
                Ok(_) => {
                    self.progress().finish(1, total);
                    self.save(|state| {
                        state.sent_bytes = total;
                        state.confirmed_bytes = total;
                        state.transferred = true;
                    })?;
                    return Ok(());
                }
                Err(Stop::Expired) => {
                    self.pause_retry(renewals, OperationFailure::new(FailureKind::Rejected, EXPIRED_MESSAGE)).await?;
                    renewals += 1;
                    self.save(|state| { state.put_url = None; state.headers.clear(); })?;
                }
                Err(other) => return Err(other),
            }
        }
    }

    async fn mutation(self: &Arc<Self>) -> Result<(), Stop> {
        let items = self.inner.upload_items().map_err(|error| Stop::Failed(OperationFailure::new(FailureKind::Unrecognized, error.to_string())))?;
        if items.iter().any(|(key, state)| key.0 == self.seq && !state.is_transferred()) {
            return Ok(());
        }
        let state = self.snapshot();
        let (Some(_), Some(_)) = (&state.signed_id, &state.blob_id) else {
            return Err(Stop::Failed(OperationFailure::new(FailureKind::Unrecognized, "the upload has no stored blob")));
        };
        let Some(operation) = locked(&self.inner.operations).get(&self.input.steps.mutation).cloned() else {
            return Err(Stop::Failed(OperationFailure::new(FailureKind::Rejected, format!("operation {} is not registered", self.input.steps.mutation))));
        };
        if self.token.is_cancelled() {
            return Err(Stop::Cancelled);
        }
        self.await_online().await?;
        let Some(entry) = self.inner.db.outbox().ok().and_then(|entries| entries.into_iter().find(|entry| entry.operation.id.0 == self.seq)) else {
            return Err(Stop::Superseded);
        };
        let spec = self.inner.db.spec(&self.model).map_err(|error| Stop::Failed(OperationFailure::new(FailureKind::Unrecognized, error.to_string())))?;
        let declaration = spec.actions.iter().find(|action| action.mutation.as_deref() == Some(&operation.name));
        let sending = Sending {
            entry: &entry,
            model: self.model.clone(),
            action: declaration.map_or_else(|| operation.name.clone(), |action| action.name.clone()),
            generation: self.inner.db.generation(),
            base_revision: self.inner.db.revision_seq().unwrap_or_default(),
        };
        let resumed = locked(&self.resumed).take();
        if let Err(error) = self.inner.send_owned(&sending, &operation, resumed, true).await {
            let failure = OperationFailure::new(FailureKind::Unrecognized, error.to_string());
            let _ = self.commit(|envelope| {
                envelope.operation_transitions.push(OperationTransition::Fail { seq: self.seq, status: OperationStatus::DeliveryUnknown, failure })
            });
        }
        Ok(())
    }

    pub(super) async fn abort_remote(&self) {
        let state = self.snapshot();
        let (Some(key), Some(upload_id)) = (state.key, state.upload_id) else { return };
        if let Some(steps) = &self.input.steps.multipart {
            self.inner.connectivity.online().await;
            if self.graphql(&steps.abort, json!({"key": key, "uploadId": upload_id}), false).await.is_err() {
                return;
            }
        }
        let _ = self.save(|state| {
            state.key = None;
            state.upload_id = None;
            state.parts.clear();
            state.sent_bytes = 0;
            state.request = None;
        });
    }

    async fn failed(&self, failure: OperationFailure) {
        let stage = self.snapshot().stage;
        if self.snapshot().upload_id.is_some() && !self.has_pending_request() {
            let _ = self.save(|state| state.stage = UploadStage::Abort);
            self.abort_remote().await;
        }
        let pending = self.has_pending_request();
        let state = {
            let mut state = locked(&self.state);
            if !pending {
                state.stage = stage;
            }
            state.error = Some(failure.clone());
            state.clone()
        };
        let _ = self.commit(|envelope| {
            envelope.operation_transitions.push(OperationTransition::Progress { seq: self.seq, item_position: self.position, state });
            envelope.operation_transitions.push(OperationTransition::Fail { seq: self.seq, status: OperationStatus::Failed, failure });
        });
    }

    pub(super) async fn cancelled(&self) {
        let _ = self.save(|state| state.phase = UploadStatus::Cancelled);
        if self.snapshot().upload_id.is_some() {
            let _ = self.save(|state| state.stage = UploadStage::Abort);
            self.abort_remote().await;
        }
        if self.snapshot().upload_id.is_some() {
            let failure = OperationFailure::new(FailureKind::Network, "the upload was cancelled but its remote parts are not aborted yet");
            let _ = self.commit(|envelope| envelope.operation_transitions.push(OperationTransition::Fail { seq: self.seq, status: OperationStatus::Failed, failure }));
            return;
        }
        let _ = self.commit(|envelope| {
            envelope.operation_transitions.push(OperationTransition::Close { seq: self.seq, status: OperationStatus::RolledBack, once: false })
        });
    }
}
