use std::future::Future;
use std::sync::Arc;

use serde_json::Value;

use crate::syncstate::{FailureKind, OperationFailure};
use crate::transport::{GraphQLRequest, PutFile, PutRequest, PutResponse, PutTaskState, ResponseOutcome, RetryClass, TransportError};
use crate::upload::{copy_range, UploadEvent, UploadStage};

use super::super::extract::root_value;
use super::super::locked;
use super::run::Runner;

pub(super) enum StepError {
    Retry(OperationFailure),
    Expired,
    Fatal(OperationFailure),
    Cancelled,
}

pub(super) enum Stop {
    Cancelled,
    Failed(OperationFailure),
    Expired,
    Superseded,
}

pub(super) fn fatal(kind: FailureKind, message: impl Into<String>) -> StepError {
    StepError::Fatal(OperationFailure::new(kind, message))
}

fn step_error(error: TransportError, cancelled: bool) -> StepError {
    let message = error.to_string();
    let retryable = RetryClass::of_error(&error) == RetryClass::Retriable;
    match error {
        TransportError::Graphql { extensions, .. } if retryable => StepError::Retry(OperationFailure { kind: FailureKind::Server, message, extensions }),
        TransportError::Security { .. } | TransportError::Config { .. } => fatal(FailureKind::Unrecognized, message),
        TransportError::Interrupted(_) if cancelled => StepError::Cancelled,
        TransportError::Network(_) | TransportError::Interrupted(_) => StepError::Retry(OperationFailure::new(FailureKind::Network, message)),
        TransportError::Http { status: 403, .. } => StepError::Expired,
        TransportError::Http { status, .. } if status >= 500 || status == 408 || status == 429 => {
            StepError::Retry(OperationFailure::new(FailureKind::Server, message))
        }
        TransportError::Session { code, detail } => fatal(FailureKind::Session(code), detail),
        TransportError::Graphql { messages, extensions } => StepError::Fatal(OperationFailure { kind: FailureKind::Rejected, message: messages.join("\n"), extensions }),
        TransportError::RequestTooLarge { .. } | TransportError::Http { .. } | TransportError::Unscripted(_) => fatal(FailureKind::Rejected, message),
        TransportError::Decode(_) => fatal(FailureKind::Unrecognized, message),
    }
}

impl Runner {
    pub(super) fn remember(&self, request: &GraphQLRequest) -> Result<(), StepError> {
        self.save(UploadEvent::RequestRemembered { request: crate::upload::UploadRequest {
            document: request.document.clone(), name: request.operation_name.clone(), variables: request.variables.clone(),
        } }).map_err(|stop| match stop {
            Stop::Failed(failure) => StepError::Fatal(failure),
            _ => StepError::Cancelled,
        })
    }

    pub(super) fn has_pending_request(&self) -> bool {
        crate::transport::encrypted::pending_matches(&self.inner.db, self.seq, self.position, &crate::operation::enum_text(&self.snapshot().stage).unwrap_or_default())
    }

    pub(super) async fn graphql(&self, name: &str, variables: Value, interruptible: bool) -> Result<Value, StepError> {
        let Some(operation) = locked(&self.inner.operations).get(name).cloned() else {
            return Err(fatal(FailureKind::Rejected, format!("operation {name} is not registered")));
        };
        locked(&self.inner.counters).record_transport_call();
        let mut request = GraphQLRequest::new(&operation.document, &operation.name, variables);
        request.upload = true;
        request.owner = Some(crate::transport::RequestOwner { seq: self.seq, item_position: self.position, stage: crate::operation::enum_text(&self.snapshot().stage).unwrap_or_default() });
        self.remember(&request)?;
        let resumed = locked(&self.resumed).take();
        let call = crate::pacing::timeout(self.inner.clock.as_ref(), self.inner.config.upload.step_timeout, async {
            match resumed {
                Some(result) => result,
                None => self.inner.transport.mutate(request).await,
            }
        });
        let outcome = if interruptible {
            tokio::select! {
                result = call => result,
                () = self.token.cancelled() => return Err(StepError::Cancelled),
            }
        } else {
            call.await
        };
        let response = match outcome {
            Err(_) => return Err(StepError::Retry(OperationFailure::new(FailureKind::Network, format!("{name} timed out")))),
            Ok(Err(error)) => {
                return Err(match step_error(error, false) {
                    StepError::Expired => fatal(FailureKind::Rejected, format!("{name} was forbidden")),
                    other => other,
                })
            }
            Ok(Ok(response)) => response,
        };
        if response.shape().outcome() != ResponseOutcome::Commit {
            let codes: Vec<&str> = response.errors.iter().filter_map(|error| error.code()).collect();
            let message = response.errors.first().map_or("empty response".to_string(), |error| error.message.clone());
            let retryable = response.errors.iter().any(|error| error.extensions.as_ref().and_then(|fields| fields.get("retryable")).and_then(Value::as_bool) == Some(true));
            return Err(if retryable || RetryClass::classify(false, None, &codes) == RetryClass::Retriable {
                StepError::Retry(OperationFailure { kind: FailureKind::Server, message, extensions: response.extensions() })
            } else {
                StepError::Fatal(OperationFailure { kind: FailureKind::Rejected, message, extensions: response.extensions() })
            });
        }
        response
            .data
            .and_then(|data| root_value(&data, &operation.root_path).cloned())
            .filter(|root| !root.is_null())
            .ok_or_else(|| fatal(FailureKind::Unrecognized, format!("{name} answered without {}", operation.root_path)))
    }

    pub(super) async fn put_once(
        self: &Arc<Self>,
        url: &str,
        headers: &[(String, String)],
        offset: u64,
        length: u64,
        part: u32,
    ) -> Result<Option<String>, StepError> {
        let tag = format!("{}.{}.{}", self.seq, self.position, part);
        let response = match self.adopted_put(&tag).await {
            Some(adopted) => adopted,
            None => self.send_put(url, headers, offset, length, part, tag).await,
        };
        match response {
            Err(error) => Err(error),
            Ok(response) if !(200..300).contains(&response.status) => {
                Err(step_error(TransportError::Http { status: response.status, body: String::new() }, self.token.is_cancelled()))
            }
            Ok(response) => Ok(response.etag),
        }
    }

    async fn send_put(self: &Arc<Self>, url: &str, headers: &[(String, String)], offset: u64, length: u64, part: u32, tag: String) -> Result<PutResponse, StepError> {
        let whole = offset == 0 && length == self.snapshot().total_bytes;
        let file = if whole {
            PutFile { path: self.input.path.clone(), offset, length }
        } else {
            let target = self.inner.db.shared.files.range_path(&tag);
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
            tag,
            cancel: self.token.clone(),
            sent: Arc::new(move |bytes| runner.report(part, bytes)),
        };
        let sent = crate::pacing::timeout(self.inner.clock.as_ref(), self.inner.config.upload.put_timeout, self.inner.transport.put(request)).await;
        if !whole {
            let _ = std::fs::remove_file(&file.path);
        }
        match sent {
            Err(_) => Err(StepError::Retry(OperationFailure::new(FailureKind::Network, "put timed out"))),
            Ok(outcome) => outcome.map_err(|error| step_error(error, self.token.is_cancelled())),
        }
    }

    async fn adopted_put(&self, tag: &str) -> Option<Result<PutResponse, StepError>> {
        let mut states = self.inner.adopted_put(tag)?;
        loop {
            let state = states.borrow_and_update().clone();
            match state {
                PutTaskState::Finished { status, etag } => {
                    self.inner.forget_put(tag);
                    return Some(Ok(PutResponse { status, etag }));
                }
                PutTaskState::Failed { detail } => {
                    self.inner.forget_put(tag);
                    return Some(Err(StepError::Retry(OperationFailure::new(FailureKind::Network, detail))));
                }
                PutTaskState::Running => tokio::select! {
                    changed = states.changed() => if changed.is_err() { return None },
                    () = self.token.cancelled() => return Some(Err(StepError::Cancelled)),
                },
            }
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
                Err(StepError::Fatal(failure)) => return Err(Stop::Failed(failure)),
                Err(StepError::Retry(failure)) => {
                    if !self.inner.connectivity.is_online() {
                        continue;
                    }
                    self.pause_retry(stage, part_number, failure).await?;
                }
            }
        }
    }

    pub(super) async fn pause_retry(&self, stage: UploadStage, part_number: u32, failure: OperationFailure) -> Result<(), Stop> {
        self.await_online().await?;
        let attempt = self.snapshot().retries.iter().find(|retry| retry.stage == stage && retry.part_number == part_number).map_or(0, |retry| retry.attempts);
        let backoff = self.inner.config.upload.backoff;
        if !backoff.allows(attempt) {
            return Err(Stop::Failed(failure));
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
