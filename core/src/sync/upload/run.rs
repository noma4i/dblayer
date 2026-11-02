use std::path::Path;
use std::sync::{Arc, Mutex};

use serde_json::json;

use crate::commit::{CommitEnvelope, OperationTransition, WriteOrigin};
use crate::operation::OperationStatus;
use crate::operation::Operation;
use crate::syncstate::{FailureKind, OperationFailure};
use crate::transport::{CancelToken, Response, TransportError};
use crate::upload::{
    file_len, md5_base64, mime_of, scenario_for, RunMode, TransferProgress, UploadEvent, UploadInput, UploadScenario, UploadStage, UploadState,
};

use super::super::send::Sending;
use super::super::{locked, Inner};
use super::steps::{text, StepError, Stop, EXPIRED_MESSAGE};

pub(super) struct Runner {
    pub(super) inner: Arc<Inner>,
    pub(super) seq: i64,
    pub(super) position: i64,
    pub(super) run: u64,
    pub(super) token: CancelToken,
    runtime: tokio::runtime::Handle,
    pub(super) input: UploadInput,
    source_error: Option<String>,
    file_id: Option<String>,
    owner: crate::upload::UploadOwner,
    inline_file: Mutex<Option<(String, String, Vec<u8>)>>,
    model: String,
    batch: bool,
    state: Mutex<UploadState>,
    progress: Mutex<TransferProgress>,
    pub(super) resumed: Mutex<Option<Result<Response, TransportError>>>,
}

impl Drop for Runner {
    fn drop(&mut self) {
        self.token.cancel();
        let Some(id) = self.file_id.take() else { return };
        let shared = self.inner.db.shared.clone();
        let cleanup = move || {
            let missing = shared.read(&[], |reader| {
                Ok(!reader.connection().query_row("SELECT EXISTS (SELECT 1 FROM file WHERE id = ?1)", [&id], |row| row.get::<_, bool>(0))?)
            });
            if missing.is_ok_and(|missing| missing) { let _ = shared.files.discard(std::slice::from_ref(&id)); }
        };
        self.inner.spawn_file_task(&self.runtime, cleanup);
    }
}

pub(super) fn stop_of(error: StepError) -> Stop {
    match error {
        StepError::Retry(failure) | StepError::Fatal(failure) => Stop::failed(*failure),
        StepError::Expired => Stop::failed(OperationFailure::new(FailureKind::Rejected, EXPIRED_MESSAGE)),
        StepError::Cancelled => Stop::Cancelled,
    }
}

pub(super) async fn drive(inner: Arc<Inner>, seq: i64, position: i64, run: u64, token: CancelToken, mode: RunMode, resumed: Option<Result<Response, TransportError>>) {
    let runner = match crate::blocking::blocking_unit(|| Runner::load(inner.clone(), seq, position, run, token, matches!(mode, RunMode::Transfer))) {
        Ok(Some(runner)) => runner,
        Ok(None) => return,
        Err(error) => {
            if inner.is_current(seq, position, run) { inner.fail_upload(seq, error); }
            return;
        }
    };
    let runner = Arc::new(runner);
    *locked(&runner.resumed) = resumed;
    let result = match mode {
        RunMode::Transfer | RunMode::Mutation => runner.transfer().await,
        RunMode::Orphan => runner.abort_remote().await,
        RunMode::Cancel => runner.cancelled().await,
    };
    if let Err(Stop::Failed(failure)) = result {
        inner.fail_upload(seq, super::super::SyncError::Store(failure.message));
    }
}

impl Runner {
    pub(super) fn load(inner: Arc<Inner>, seq: i64, position: i64, run: u64, token: CancelToken, transfer: bool) -> Result<Option<Self>, super::super::SyncError> {
        let Some(entry) = inner.outbox_operation(seq)? else { return Ok(None) };
        let owner = match inner.blob_id(seq)? {
            Some(id) => crate::upload::UploadOwner::Blob { id },
            None => crate::upload::UploadOwner::Action { model: Operation::split_action_key(&entry.operation.action_key).ok_or_else(|| super::super::SyncError::Malformed("the upload action key is invalid".into()))?.0 },
        };
        let model = match &owner { crate::upload::UploadOwner::Action { model } => model.clone(), crate::upload::UploadOwner::Blob { .. } => String::new() };
        let Some(state) = inner.operation_uploads(seq)?.remove(&(seq, position)) else { return Ok(None) };
        let needs_file = transfer && !state.is_transferred();
        let (mut input, file, source_error, file_id) = inner.db.shared.read(&[], |reader| {
            let connection = reader.connection();
            let value = crate::operation::read_item_value(connection, seq, position, "input")?.unwrap_or(serde_json::Value::Null);
            let input: UploadInput = serde_json::from_value(value).map_err(|error| crate::store::StoreError::from(std::io::Error::other(error)))?;
            let (id, error): (Option<String>, Option<String>) = connection.query_row("SELECT file_id, source_error FROM op_item WHERE seq = ?1 AND position = ?2", rusqlite::params![seq, position], |row| Ok((row.get(0)?, row.get(1)?)))?;
            let file = if needs_file {
                id.as_ref().map(|id| {
                    let (name, location): (String, String) = connection.query_row("SELECT name, location FROM file WHERE id = ?1", [id], |row| Ok((row.get(0)?, row.get(1)?)))?;
                    let bytes = match location.as_str() {
                        "disk" => None,
                        "inline" => Some(connection.query_row("SELECT data FROM file WHERE id = ?1", [id], |row| row.get::<_, Vec<u8>>(0))?),
                        _ => return Err(crate::store::StoreError::Constraint("invalid upload file location".into())),
                    };
                    Ok((id.clone(), name, bytes))
                }).transpose()?
            } else { None };
            Ok((input, file, error, id))
        })?;
        let mut inline_file = None;
        if let Some((id, name, bytes)) = file {
            let path = match bytes {
                Some(bytes) => {
                    let path = inner.db.shared.files.cache_target(&id, &name);
                    inline_file = Some((id, name, bytes));
                    path
                }
                None => inner.db.shared.files.path(&id).ok_or_else(|| super::super::SyncError::Store("upload file directory is missing".into()))?,
            };
            input.path = path.to_string_lossy().into_owned();
        }
        let batch = match &owner { crate::upload::UploadOwner::Action { model } => inner.db.spec(model)?.actions.iter().any(|action| action.batch.is_some() && action.mutation.as_deref() == Some(&input.steps.mutation)), crate::upload::UploadOwner::Blob { .. } => false };
        let progress = TransferProgress::new(state.total_bytes, inner.config.upload.tick);
        Ok(Some(Self { inner, seq, position, run, token, batch, owner, runtime: tokio::runtime::Handle::current(), input, source_error, file_id, inline_file: Mutex::new(inline_file), model, state: Mutex::new(state), progress: Mutex::new(progress), resumed: Mutex::new(None) }))
    }

    pub(super) fn snapshot(&self) -> UploadState {
        locked(&self.state).clone()
    }

    fn commit_deferred(&self, mut build: impl FnMut(&mut CommitEnvelope)) -> Result<crate::store::Committed<'_>, Stop> {
        let generation = crate::blocking::blocking_unit(|| self.inner.db.generation());
        let committed = self.inner.commit_deferred(generation, |reader| {
            if !self.inner.is_current(self.seq, self.position, self.run) {
                return Ok(None);
            }
            if !reader.connection().query_row("SELECT EXISTS (SELECT 1 FROM op_item WHERE seq = ?1 AND position = ?2)", rusqlite::params![self.seq, self.position], |row| row.get::<_, bool>(0)).map_err(crate::store::StoreError::from)? {
                return Ok(None);
            }
            let mut envelope = CommitEnvelope::new(WriteOrigin::Response, generation);
            build(&mut envelope);
            Ok(Some(envelope))
        });
        match committed {
            Ok(Some(committed)) => Ok(committed),
            Ok(None) => Err(Stop::Superseded),
            Err(error) => Err(Stop::failed(OperationFailure::new(FailureKind::Unrecognized, error.to_string()))),
        }
    }

    fn settle(committed: crate::store::Committed<'_>) -> Result<(), Stop> {
        let noop = committed.result().is_noop();
        committed.publish();
        if noop { Err(Stop::Superseded) } else { Ok(()) }
    }

    pub(super) fn commit(&self, build: impl FnMut(&mut CommitEnvelope)) -> Result<(), Stop> {
        Self::settle(self.commit_deferred(build)?)
    }

    pub(super) fn save(&self, event: UploadEvent) -> Result<(), Stop> {
        let committed = {
            let mut state = locked(&self.state);
            let mut snapshot = state.clone();
            snapshot.apply(event).map_err(|error| Stop::failed(OperationFailure::new(FailureKind::Unrecognized, error.to_string())))?;
            let committed = self.commit_deferred(|envelope| envelope.operation_transitions.push(OperationTransition::Progress { seq: self.seq, item_position: self.position, state: Box::new(snapshot.clone()) }))?;
            if !committed.result().is_noop() {
                *state = snapshot;
            }
            committed
        };
        Self::settle(committed)
    }

    pub(super) fn report(&self, part: u32, bytes: u64) {
        let due = {
            let mut progress = locked(&self.progress);
            progress.report(part, bytes);
            progress.due(self.inner.clock.now())
        };
        if let Some(sent) = due {
            if let Err(Stop::Failed(failure)) = self.save(UploadEvent::Progress { sent }) {
                self.inner.fail_upload(self.seq, super::super::SyncError::Store(failure.message));
                self.token.cancel();
            }
        }
    }

    pub(super) fn progress(&self) -> std::sync::MutexGuard<'_, TransferProgress> {
        locked(&self.progress)
    }

    async fn transfer(self: &Arc<Self>) -> Result<(), Stop> {
        let transferred = tokio::select! {
            biased;
            () = self.token.cancelled() => Err(Stop::Cancelled),
            result = self.transfer_file() => result,
        };
        self.inner.release_file_slot(self.seq, self.position, self.run);
        let result = match transferred {
            Ok(()) if matches!(self.owner, crate::upload::UploadOwner::Blob { .. }) => {
                match self.save(UploadEvent::BlobUploaded) {
                    Ok(()) => self.inner.flush().await.map(|_| ()).map_err(|error| Stop::failed(OperationFailure::new(FailureKind::Unrecognized, error.to_string()))),
                    Err(error) => Err(error),
                }
            }
            Ok(()) => self.mutation().await,
            Err(stop) => Err(stop),
        };
        match result {
            Ok(()) | Err(Stop::Superseded) => Ok(()),
            Err(Stop::Cancelled) => self.cancelled().await,
            Err(Stop::Failed(failure)) => self.failed(*failure).await,
            Err(Stop::Expired) => self.failed(OperationFailure::new(FailureKind::Rejected, EXPIRED_MESSAGE)).await,
        }
    }

    pub(super) async fn materialize(self: &Arc<Self>) -> Result<(), Stop> {
        let Some((id, name, bytes)) = locked(&self.inline_file).take() else { return Ok(()) };
        let (sent, received) = tokio::sync::oneshot::channel();
        let runner = self.clone();
        self.inner.spawn_file_task(&self.runtime, move || { let _ = sent.send(runner.inner.db.shared.files.materialize(&id, &name, &bytes)); }); // lock-ok: file guard ended at statement
        match received.await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(error)) => Err(Stop::failed(OperationFailure::new(FailureKind::Unrecognized, error.to_string()))),
            Err(_) => Err(Stop::Cancelled),
        }
    }

    async fn transfer_file(self: &Arc<Self>) -> Result<(), Stop> {
        if self.snapshot().is_transferred() {
            return Ok(());
        }
        if let Some(error) = &self.source_error {
            return Err(Stop::failed(OperationFailure::new(FailureKind::Rejected, format!("source file is unreadable: {error}"))));
        }
        self.save(UploadEvent::Start)?;
        self.materialize().await?;
        let total = file_len(Path::new(&self.input.path), &self.token).await
            .map_err(|error| Stop::failed(OperationFailure::new(FailureKind::Rejected, format!("source file is unreadable: {error}"))))?;
        let stored = self.snapshot();
        if stored.upload_id.is_some() && stored.total_bytes != 0 && stored.total_bytes != total {
            self.abort_remote().await?;
        }
        *locked(&self.progress) = TransferProgress::new(total, self.inner.config.upload.tick);
        let scenario = match self.snapshot() {
            state if state.upload_id.is_some() => UploadScenario::Multipart,
            _ => scenario_for(total, self.inner.config.upload.multipart_threshold),
        };
        self.save(UploadEvent::Located { total, scenario })?;
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
            .ok_or_else(|| Stop::failed(OperationFailure::new(FailureKind::Rejected, "the upload has no single-upload steps")))?;
        let checksum = md5_base64(Path::new(&self.input.path), &self.token).await
            .map_err(|error| Stop::failed(OperationFailure::new(FailureKind::Rejected, format!("source file is unreadable: {error}"))))?;
        loop {
            if self.snapshot().put_url.is_none() {
                self.save(UploadEvent::CreateStarted)?;
                let variables = json!({
                    "filename": self.file_name(),
                    "contentType": self.content_type(),
                    "byteSize": total,
                    "checksum": checksum,
                });
                let data = self.retrying(UploadStage::Create, 0, || self.graphql(&steps.create, variables.clone(), true)).await?;
                let url = text(&data, "url").map_err(stop_of)?;
                let headers = crate::upload::parse_headers(data.get("headers")).map_err(|error| Stop::failed(OperationFailure::new(FailureKind::Unrecognized, error)))?;
                let (signed, blob) = (text(&data, "signedId").map_err(stop_of)?, text(&data, "blobId").map_err(stop_of)?);
                self.save(UploadEvent::Granted { signed_id: signed, blob_id: blob, url, headers, checksum: checksum.clone(), issued_at: self.inner.clock.now().as_millis() as u64 })?;
            }
            let grant = self.snapshot();
            let url = grant.put_url.ok_or(Stop::Superseded)?;
            let headers = grant.headers;
            self.save(UploadEvent::PutStarted)?;
            self.progress().begin_phase([]);
            match self.retrying(UploadStage::Put, 0, || self.put_once(&url, &headers, 0, total, 1)).await {
                Ok(_) => {
                    self.progress().finish(1, total);
                    self.save(UploadEvent::PutDone { total })?;
                    return Ok(());
                }
                Err(Stop::Expired) => {
                    self.pause_retry(UploadStage::Put, 0, OperationFailure::new(FailureKind::Rejected, EXPIRED_MESSAGE)).await?;
                    self.save(UploadEvent::GrantDropped)?;
                }
                Err(other) => return Err(other),
            }
        }
    }

    async fn mutation(self: &Arc<Self>) -> Result<(), Stop> {
        self.await_online().await?;
        let items = self.inner.operation_uploads(self.seq).map_err(|error| Stop::failed(OperationFailure::new(FailureKind::Unrecognized, error.to_string())))?;
        if items.iter().any(|(key, state)| key.0 == self.seq && !state.is_transferred()) {
            return Ok(());
        }
        let state = self.snapshot();
        let (Some(_), Some(_)) = (&state.signed_id, &state.blob_id) else {
            return Err(Stop::failed(OperationFailure::new(FailureKind::Unrecognized, "the upload has no stored blob")));
        };
        let Some(operation) = locked(&self.inner.operations).get(&self.input.steps.mutation).cloned() else {
            return Err(Stop::failed(OperationFailure::new(FailureKind::Rejected, format!("operation {} is not registered", self.input.steps.mutation))));
        };
        if self.token.is_cancelled() {
            return Err(Stop::Cancelled);
        }
        let Some(entry) = self.inner.outbox_operation(self.seq).map_err(|error| Stop::failed(OperationFailure::new(FailureKind::Unrecognized, error.to_string())))? else {
            return Err(Stop::Superseded);
        };
        let action = crate::operation::Operation::upload_action(&entry.operation.action_key).ok_or_else(|| Stop::failed(OperationFailure::new(FailureKind::Unrecognized, "the upload has no declared action")))?.1;
        let sending = Sending {
            entry: &entry,
            model: self.model.clone(),
            action,
            generation: self.inner.db.generation(),
            base_revision: self.inner.db.revision_seq().map_err(|error| Stop::failed(OperationFailure::new(FailureKind::Unrecognized, error.to_string())))?,
        };
        let resumed = locked(&self.resumed).take();
        if let Err(error) = self.inner.send_owned(&sending, &operation, resumed, true).await {
            let failure = OperationFailure::new(FailureKind::Unrecognized, error.to_string());
            self.commit(|envelope| {
                envelope.operation_transitions.push(OperationTransition::Fail { seq: self.seq, status: OperationStatus::DeliveryUnknown, failure: failure.clone() })
            })?;
        }
        self.inner.start_uploads().map_err(|error| Stop::failed(OperationFailure::new(FailureKind::Unrecognized, error.to_string())))?;
        Ok(())
    }

    pub(super) async fn abort_remote(&self) -> Result<(), Stop> {
        let state = self.snapshot();
        let (Some(key), Some(upload_id)) = (state.key, state.upload_id) else { return Ok(()) };
        if let Some(steps) = &self.input.steps.multipart {
            if !self.inner.connectivity.is_online() {
                return Ok(());
            }
            match self.graphql(&steps.abort, json!({"key": key, "uploadId": upload_id}), false).await {
                Ok(_) => {}
                Err(StepError::Fatal(failure)) => return Err(Stop::failed(*failure)),
                Err(_) => return Ok(()),
            }
        }
        self.save(UploadEvent::Aborted)
    }

    async fn failed(&self, failure: OperationFailure) -> Result<(), Stop> {
        let batch = self.batch;
        let stage = self.snapshot().stage;
        let snapshot = self.snapshot();
        if snapshot.upload_id.is_some() && !snapshot.all_parts_confirmed() && !self.has_pending_request()? {
            self.save(UploadEvent::AbortStarted)?;
            self.abort_remote().await?;
        }
        let pending = self.has_pending_request()?;
        let state = {
            let mut state = self.snapshot();
            state.apply(UploadEvent::Failed { failure: failure.clone(), at: self.inner.clock.now().as_millis() as u64, batch, resume_at: (!pending).then_some(stage) }).map_err(|error| Stop::failed(OperationFailure::new(FailureKind::Unrecognized, error.to_string())))?;
            state
        };
        self.commit(|envelope| {
            envelope.operation_transitions.push(OperationTransition::Progress { seq: self.seq, item_position: self.position, state: Box::new(state.clone()) });
            if !batch { envelope.operation_transitions.push(OperationTransition::Fail { seq: self.seq, status: OperationStatus::Failed, failure: failure.clone() }); }
        })?;
        *locked(&self.state) = state;
        Ok(())
    }

    pub(super) async fn cancelled(&self) -> Result<(), Stop> {
        let batch = self.batch;
        self.has_pending_request()?;
        self.save(UploadEvent::Cancel)?;
        if self.snapshot().upload_id.is_some() {
            self.save(UploadEvent::AbortStarted)?;
            self.abort_remote().await?;
        }
        if self.snapshot().upload_id.is_some() {
            let failure = OperationFailure::new(FailureKind::Network, "the upload was cancelled but its remote parts are not aborted yet");
            if batch { return self.save(UploadEvent::ErrorNoted { failure }); }
            return self.commit(|envelope| envelope.operation_transitions.push(OperationTransition::Fail { seq: self.seq, status: OperationStatus::Failed, failure: failure.clone() }));
        }
        if batch {
            let row = self.inner.db.shared.read(&[], |reader| Ok(reader.connection().query_row("SELECT row_id FROM op_item WHERE seq = ?1 AND position = ?2", rusqlite::params![self.seq, self.position], |row| row.get::<_, String>(0))?))
                .map_err(|error| Stop::failed(OperationFailure::new(FailureKind::Unrecognized, error.to_string())))?;
            let id = crate::identity::RowId::new(&row).map_err(|error| Stop::failed(OperationFailure::new(FailureKind::Unrecognized, error.to_string())))?;
            self.inner.db.remove_upload_item(&self.model, &id, self.seq, false).map_err(|error| Stop::failed(OperationFailure::new(FailureKind::Unrecognized, error.to_string())))?;
            return self.inner.start_uploads().map_err(|error| Stop::failed(OperationFailure::new(FailureKind::Unrecognized, error.to_string())));
        }
        self.commit(|envelope| {
            envelope.operation_transitions.push(OperationTransition::Close { seq: self.seq, status: OperationStatus::RolledBack })
        })
    }
}
