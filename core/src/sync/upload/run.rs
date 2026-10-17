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

use super::super::send::{Delivery, Sending};
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
    pub(super) run: u64,
    pub(super) token: CancelToken,
    pub(super) input: UploadInput,
    model: String,
    state: Mutex<UploadState>,
    progress: Mutex<TransferProgress>,
    pub(super) resumed: Mutex<Option<Result<Response, TransportError>>>,
}

pub(super) fn stop_of(error: StepError) -> Stop {
    match error {
        StepError::Retry(failure) | StepError::Fatal(failure) => Stop::Failed(failure),
        StepError::Expired => Stop::Failed(OperationFailure::new(FailureKind::Rejected, EXPIRED_MESSAGE)),
        StepError::Cancelled => Stop::Cancelled,
    }
}

pub(super) async fn drive(inner: Arc<Inner>, seq: i64, run: u64, token: CancelToken, permits: Arc<Semaphore>, mode: Mode, resumed: Option<Result<Response, TransportError>>) {
    let Some(runner) = Runner::load(inner, seq, run, token) else { return };
    let runner = Arc::new(runner);
    *locked(&runner.resumed) = resumed;
    match mode {
        Mode::Transfer => runner.transfer(permits).await,
        Mode::Orphan => runner.abort_remote().await,
        Mode::Cancel => runner.cancelled().await,
    }
}

impl Runner {
    fn load(inner: Arc<Inner>, seq: i64, run: u64, token: CancelToken) -> Option<Self> {
        let entry = inner.db.outbox().ok()?.into_iter().find(|entry| entry.operation.id.0 == seq)?;
        let (model, _) = Operation::split_action_key(&entry.operation.action_key)?;
        let input: UploadInput = serde_json::from_value(entry.input.clone()).ok()?;
        let state = entry.operation.upload.clone()?;
        let progress = TransferProgress::new(state.total_bytes, inner.config.upload.tick);
        Some(Self { inner, seq, run, token, input, model, state: Mutex::new(state), progress: Mutex::new(progress), resumed: Mutex::new(None) })
    }

    pub(super) fn snapshot(&self) -> UploadState {
        locked(&self.state).clone()
    }

    pub(super) fn commit(&self, build: impl FnOnce(&mut CommitEnvelope)) -> Result<(), Stop> {
        let generation = self.inner.db.generation();
        let committed = self.inner.commit(generation, |_| {
            if !self.inner.is_current(self.seq, self.run) {
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
        self.commit(|envelope| envelope.operation_transitions.push(OperationTransition::Progress { seq: self.seq, state: snapshot }))
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
        let permit = tokio::select! {
            permit = permits.acquire_owned() => permit.ok(),
            () = self.token.cancelled() => None,
        };
        let Some(_permit) = permit else { return self.cancelled().await };
        match self.transfer_and_send().await {
            Ok(()) | Err(Stop::Superseded) => {}
            Err(Stop::Cancelled) => self.cancelled().await,
            Err(Stop::Failed(failure)) => self.failed(failure).await,
            Err(Stop::Expired) => self.failed(OperationFailure::new(FailureKind::Rejected, EXPIRED_MESSAGE)).await,
        }
    }

    async fn transfer_and_send(self: &Arc<Self>) -> Result<(), Stop> {
        if self.snapshot().is_transferred() {
            return self.mutation().await;
        }
        self.save(|state| {
            state.phase = UploadStatus::Preparing;
            state.stage = UploadStage::Prepare;
            state.retries = 0;
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
        self.mutation().await
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
            self.save(|state| state.stage = UploadStage::Put)?;
            self.progress().begin_phase([]);
            match self.retrying(|| self.put_once(&url, &headers, 0, total, 1)).await {
                Ok(_) => {
                    self.progress().finish(1, total);
                    self.save(|state| {
                        state.sent_bytes = total;
                        state.signed_id = Some(signed);
                        state.blob_id = Some(blob);
                    })?;
                    return Ok(());
                }
                Err(Stop::Expired) if self.inner.config.upload.backoff.allows(renewals) => renewals += 1,
                Err(other) => return Err(other),
            }
        }
    }

    async fn mutation(self: &Arc<Self>) -> Result<(), Stop> {
        let mut state = self.snapshot();
        let (Some(signed), Some(blob)) = (state.signed_id.clone(), state.blob_id.clone()) else {
            return Err(Stop::Failed(OperationFailure::new(FailureKind::Unrecognized, "the upload has no stored blob")));
        };
        let Some(operation) = locked(&self.inner.operations).get(&self.input.steps.mutation).cloned() else {
            return Err(Stop::Failed(OperationFailure::new(FailureKind::Rejected, format!("operation {} is not registered", self.input.steps.mutation))));
        };
        if self.token.is_cancelled() {
            return Err(Stop::Cancelled);
        }
        self.await_online().await?;
        state.phase = UploadStatus::Sending;
        state.stage = UploadStage::Mutation;
        *locked(&self.state) = state.clone();
        self.commit(|envelope| {
            envelope.operation_transitions.push(OperationTransition::Progress { seq: self.seq, state });
            envelope.operation_transitions.push(OperationTransition::Start { seq: self.seq });
        })?;
        let mut variables = self.input.variables.clone();
        variables.insert(self.input.steps.signed_id_variable.clone(), json!(signed));
        if let Some(name) = &self.input.steps.blob_id_variable {
            variables.insert(name.clone(), json!(blob));
        }
        let Some(entry) = self.inner.db.outbox().ok().and_then(|entries| entries.into_iter().find(|entry| entry.operation.id.0 == self.seq)) else {
            return Err(Stop::Superseded);
        };
        let spec = self.inner.db.spec(&self.model).map_err(|error| Stop::Failed(OperationFailure::new(FailureKind::Unrecognized, error.to_string())))?;
        let declaration = spec.actions.iter().find(|action| action.mutation.as_deref() == Some(&operation.name));
        let variables = serde_json::Value::Object(variables);
        let variables = match declaration {
            Some(declaration) => super::super::send::upload_variables(declaration, variables, &entry.operation.client_mutation_id),
            None => variables,
        };
        let request = crate::transport::GraphQLRequest::new(&operation.document, &operation.name, variables);
        let sending = Sending {
            entry: &entry,
            model: self.model.clone(),
            action: declaration.map_or_else(|| operation.name.clone(), |action| action.name.clone()),
            generation: self.inner.db.generation(),
            base_revision: self.inner.db.revision_seq().unwrap_or_default(),
        };
        self.remember(&request).map_err(stop_of)?;
        let resumed = locked(&self.resumed).take();
        let delivery = match tokio::time::timeout(self.inner.config.upload.step_timeout, self.inner.deliver_or_resume(request, resumed)).await {
            Ok(delivery) => delivery,
            Err(_) => Delivery::Unknown(OperationFailure::new(FailureKind::Network, "the mutation timed out")),
        };
        if let Err(error) = self.inner.settle_delivery(&sending, &operation, delivery, true).await {
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
            state.clone()
        };
        let _ = self.commit(|envelope| {
            envelope.operation_transitions.push(OperationTransition::Progress { seq: self.seq, state });
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
