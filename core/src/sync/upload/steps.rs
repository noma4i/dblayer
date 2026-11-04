use std::future::Future;
use std::sync::Arc;

use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::syncstate::{FailureKind, OperationFailure};
use crate::transport::{GraphQLRequest, PutFile, PutRequest, PutResponse, ResponseOutcome, RetryClass, TransportError};
use crate::upload::{copy_range, UploadEvent, UploadStage};

use super::super::extract::root_value;
use super::super::locked;
use super::run::Runner;

impl super::super::Inner {
    pub(super) fn transfer_request(&self, name: &str, variables: Value, transfer: &crate::upload::Transfer, stage: UploadStage) -> Result<(crate::sync::OperationDef, GraphQLRequest), super::super::SyncError> {
        let operation = locked(&self.operations).get(name).cloned().ok_or_else(|| super::super::SyncError::Malformed(format!("operation {name} is not registered")))?;
        let request = transfer.request(&operation.document, &operation.name, variables, stage);
        Ok((operation, request))
    }

    pub(super) async fn send_transfer(&self, admitted: &crate::lifecycle::Admitted, request: GraphQLRequest) -> Result<crate::transport::Response, TransportError> {
        let name = request.operation_name.clone();
        locked(&self.counters).record_transport_call();
        crate::db::lock::assert_no_library_lock("Transport::mutate");
        crate::pacing::timeout(self.clock.as_ref(), self.config.upload.step_timeout, self.mutate_scoped(admitted, request)).await
            .map_err(|_| TransportError::Network(format!("{name} timed out")))?
    }
}

pub(super) enum StepError {
    Retry(Box<OperationFailure>),
    Expired,
    Fatal(Box<OperationFailure>),
    Cancelled,
}

pub(super) enum Stop {
    Cancelled,
    Failed(Box<OperationFailure>),
    Expired,
    Superseded,
}

impl Stop {
    pub(super) fn failed(failure: OperationFailure) -> Self {
        Self::Failed(Box::new(failure))
    }
}

pub(super) fn fatal(kind: FailureKind, message: impl Into<String>) -> StepError {
    StepError::Fatal(Box::new(OperationFailure::new(kind, message)))
}

pub(super) fn transfer_response(name: &str, operation: &crate::sync::OperationDef, response: crate::transport::Response) -> Result<Value, StepError> {
    if response.shape().outcome() != ResponseOutcome::Commit {
        let codes: Vec<&str> = response.errors.iter().filter_map(|error| error.code()).collect();
        let message = response.errors.first().map_or("empty response".to_string(), |error| error.message.clone());
        let retryable = response.errors.iter().any(|error| error.extensions.as_ref().and_then(|fields| fields.get("retryable")).and_then(Value::as_bool) == Some(true));
        return Err(if retryable || RetryClass::classify(false, None, &codes) == RetryClass::Retriable {
            StepError::Retry(Box::new(OperationFailure { kind: FailureKind::Server, message, extensions: response.extensions() }))
        } else {
            StepError::Fatal(Box::new(OperationFailure { kind: FailureKind::Rejected, message, extensions: response.extensions() }))
        });
    }
    response.data.and_then(|data| root_value(&data, &operation.root_path).cloned()).filter(|root| !root.is_null())
        .ok_or_else(|| fatal(FailureKind::Unrecognized, format!("{name} answered without {}", operation.root_path)))
}

pub(crate) fn transfer_abort_settled(name: &str, operation: &crate::sync::OperationDef, response: crate::transport::Response) -> bool {
    match transfer_response(name, operation, response) {
        Ok(_) => true,
        Err(StepError::Fatal(failure)) => failure.kind == FailureKind::Rejected,
        _ => false,
    }
}

fn step_error(error: TransportError, cancelled: bool) -> StepError {
    let message = error.to_string();
    let retryable = RetryClass::of_error(&error) == RetryClass::Retriable;
    match error {
        TransportError::Graphql { extensions, .. } if retryable => StepError::Retry(Box::new(OperationFailure { kind: FailureKind::Server, message, extensions })),
        TransportError::Security { .. } | TransportError::Config { .. } => fatal(FailureKind::Unrecognized, message),
        TransportError::Interrupted(_) if cancelled => StepError::Cancelled,
        TransportError::Network(_) | TransportError::Interrupted(_) => StepError::Retry(Box::new(OperationFailure::new(FailureKind::Network, message))),
        TransportError::Http { status: 403, .. } => StepError::Expired,
        TransportError::Http { status, .. } if status >= 500 || status == 408 || status == 429 => {
            StepError::Retry(Box::new(OperationFailure::new(FailureKind::Server, message)))
        }
        TransportError::Session { code, detail } => fatal(FailureKind::Session(code), detail),
        TransportError::Graphql { messages, extensions } => StepError::Fatal(Box::new(OperationFailure { kind: FailureKind::Rejected, message: messages.join("\n"), extensions })),
        TransportError::RequestTooLarge { .. } | TransportError::Http { .. } | TransportError::Unscripted(_) => fatal(FailureKind::Rejected, message),
        TransportError::Decode(_) => fatal(FailureKind::Unrecognized, message),
    }
}

impl Runner {
    pub(super) fn remember(&self, request: &GraphQLRequest) -> Result<(), StepError> {
        self.save(UploadEvent::RequestRemembered { request: crate::upload::UploadRequest {
            document: request.document.clone(), name: request.operation_name.clone(), variables: request.variables.clone(),
        } }).map_err(|stop| match stop {
            Stop::Failed(failure) => StepError::Fatal(Box::new(*failure)),
            _ => StepError::Cancelled,
        })
    }

    pub(super) fn has_pending_request(&self) -> Result<bool, Stop> {
        let stage = self.snapshot().stage;
        let owner = if stage == UploadStage::Mutation { crate::transport::RequestOwner::Operation { seq: self.seq } } else { self.transfer.owner(stage) };
        crate::transport::encrypted::pending_matches(&self.inner.db, &owner)
            .map_err(|error| Stop::failed(OperationFailure::new(FailureKind::Unrecognized, error.to_string())))
    }

    pub(super) async fn graphql(&self, name: &str, variables: Value, interruptible: bool) -> Result<Value, StepError> {
        let (operation, request) = self.inner.transfer_request(name, variables, &self.transfer, self.snapshot().stage).map_err(|error| fatal(FailureKind::Rejected, error.to_string()))?;
        self.remember(&request)?;
        let resumed = locked(&self.resumed).take();
        let call = async {
            match resumed {
                Some(result) => result,
                None => self.inner.send_transfer(&self.admitted, request).await,
            }
        };
        let outcome = if interruptible {
            tokio::select! {
                result = call => result,
                () = self.token.cancelled() => return Err(StepError::Cancelled),
            }
        } else {
            call.await
        };
        let response = match outcome {
            Err(error) => {
                return Err(match step_error(error, false) {
                    StepError::Expired => fatal(FailureKind::Rejected, format!("{name} was forbidden")),
                    other => other,
                })
            }
            Ok(response) => response,
        };
        transfer_response(name, &operation, response)
    }

    pub(super) async fn put_once(
        self: &Arc<Self>,
        url: &str,
        headers: &[(String, String)],
        offset: u64,
        length: u64,
        part: u32,
        step: crate::transport::TransferStep,
    ) -> Result<Option<String>, StepError> {
        let identity = self.transfer.put(step);
        let response = self.send_put(url, headers, offset, length, part, identity).await;
        match response {
            Err(error) => Err(error),
            Ok(response) if !(200..300).contains(&response.status) => {
                Err(step_error(TransportError::Http { status: response.status, body: String::new() }, self.token.is_cancelled()))
            }
            Ok(response) => Ok(response.etag),
        }
    }

    async fn send_put(self: &Arc<Self>, url: &str, headers: &[(String, String)], offset: u64, length: u64, part: u32, identity: crate::transport::TransferIdentity) -> Result<PutResponse, StepError> {
        let whole = offset == 0 && length == self.snapshot().total_bytes;
        let file = if whole {
            PutFile { path: self.input.path.clone(), offset, length }
        } else {
            let item_digest: String = Sha256::digest(identity.item_id.as_bytes()).iter().map(|byte| format!("{byte:02x}")).collect();
            let target = self.inner.db.shared.files.range_path(&format!("{}.{}.{}", identity.operation_id, item_digest, part));
            copy_range(std::path::Path::new(&self.input.path), offset, length, &target, &self.token).await
                .map_err(|error| fatal(FailureKind::Rejected, format!("source file is unreadable: {error}")))?;
            PutFile { path: target.to_string_lossy().into_owned(), offset: 0, length }
        };
        locked(&self.inner.counters).record_transport_call();
        let runner = self.clone();
        let request = PutRequest {
            url: url.to_string(),
            headers: headers.to_vec(),
            file: file.clone(),
            identity,
            cancel: self.token.clone(),
            sent: Arc::new(crate::task::bind(crate::task::Carry::here(), move |bytes| runner.report(part, bytes))),
        };
        crate::db::lock::assert_no_library_lock("Transport::put");
        let sent = crate::pacing::timeout(self.inner.clock.as_ref(), self.inner.config.upload.put_timeout, self.inner.transport.put(&self.admitted, request)).await;
        if !whole {
            let path = file.path.clone();
            let _ = crate::task::spawn_blocking(crate::task::Carry::Detached, move || std::fs::remove_file(path)).await;
        }
        match sent {
            Err(_) => Err(StepError::Retry(Box::new(OperationFailure::new(FailureKind::Network, "put timed out")))),
            Ok(outcome) => outcome.map_err(|error| step_error(error, self.token.is_cancelled())),
        }
    }

    pub(super) async fn retrying<T, F, Fut>(&self, stage: UploadStage, part_number: u32, mut call: F) -> Result<T, Stop>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, StepError>>,
    {
        loop {
            self.wait_retry(stage, part_number).await?;
            self.await_online().await?;
            match call().await {
                Ok(value) => {
                    self.save(UploadEvent::RetryCleared { stage, part_number })?;
                    return Ok(value);
                }
                Err(StepError::Cancelled) => return Err(Stop::Cancelled),
                Err(StepError::Expired) => return Err(Stop::Expired),
                Err(StepError::Fatal(failure)) => return Err(Stop::failed(*failure)),
                Err(StepError::Retry(failure)) => {
                    if !self.inner.connectivity.is_online() {
                        continue;
                    }
                    self.pause_retry(stage, part_number, *failure).await?;
                }
            }
        }
    }

    pub(super) async fn pause_retry(&self, stage: UploadStage, part_number: u32, failure: OperationFailure) -> Result<(), Stop> {
        self.await_online().await?;
        let attempt = self.snapshot().retries.iter().find(|retry| retry.stage == stage && retry.part_number == part_number).map_or(0, |retry| retry.attempts);
        let backoff = self.inner.config.upload.backoff;
        if !backoff.allows(attempt) {
            return Err(Stop::failed(failure));
        }
        let delay = backoff.delay(attempt);
        self.save(UploadEvent::RetryScheduled { stage, part_number, attempts: attempt + 1, retry_at: (self.inner.clock.now() + delay).as_millis() as u64, failure })?;
        self.wait_retry(stage, part_number).await
    }

    async fn wait_retry(&self, stage: UploadStage, part_number: u32) -> Result<(), Stop> {
        let deadline = self.snapshot().retries.iter().find(|retry| retry.stage == stage && retry.part_number == part_number).and_then(|retry| retry.retry_at);
        let Some(deadline) = deadline else { return Ok(()) };
        let delay = std::time::Duration::from_millis(deadline).saturating_sub(self.inner.clock.now());
        tokio::select! {
            () = self.inner.connectivity.wait(self.inner.clock.as_ref(), delay) => {}
            () = self.token.cancelled() => return Err(Stop::Cancelled),
        }
        self.save(UploadEvent::RetryElapsed { stage, part_number })
    }

    pub(super) async fn await_online(&self) -> Result<(), Stop> {
        if self.inner.connectivity.is_online() {
            return Ok(());
        }
        self.save(UploadEvent::WentOffline)?;
        let arrived = tokio::select! {
            () = self.inner.connectivity.online() => Ok(()),
            () = self.token.cancelled() => Err(Stop::Cancelled),
        };
        let cleared = self.save(UploadEvent::CameOnline);
        arrived.and(cleared)
    }
}

pub(super) const EXPIRED_MESSAGE: &str = "the upload url expired too many times";

pub(super) fn text(value: &Value, field: &str) -> Result<String, StepError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| fatal(FailureKind::Unrecognized, format!("the answer has no {field}")))
}
