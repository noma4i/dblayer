use std::future::Future;
use std::sync::Arc;

use serde_json::Value;

use crate::syncstate::{FailureKind, OperationFailure};
use crate::transport::{GraphQLRequest, PutRequest, ResponseOutcome, RetryClass, TransportError};
use crate::upload::read_range;

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
    match error {
        TransportError::Interrupted(_) if cancelled => StepError::Cancelled,
        TransportError::Network(_) | TransportError::Interrupted(_) => StepError::Retry(OperationFailure::new(FailureKind::Network, message)),
        TransportError::Http { status: 403, .. } => StepError::Expired,
        TransportError::Http { status, .. } if status >= 500 || status == 408 || status == 429 => {
            StepError::Retry(OperationFailure::new(FailureKind::Server, message))
        }
        TransportError::Http { .. } | TransportError::Unscripted(_) => fatal(FailureKind::Rejected, message),
        TransportError::Decode(_) => fatal(FailureKind::Unrecognized, message),
    }
}

impl Runner {
    pub(super) async fn graphql(&self, name: &str, variables: Value, interruptible: bool) -> Result<Value, StepError> {
        let Some(operation) = locked(&self.inner.operations).get(name).cloned() else {
            return Err(fatal(FailureKind::Rejected, format!("operation {name} is not registered")));
        };
        locked(&self.inner.counters).record_transport_call();
        let request = GraphQLRequest::new(&operation.document, &operation.name, variables);
        let call = tokio::time::timeout(self.inner.config.upload.step_timeout, self.inner.transport.mutate(request));
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
            return Err(if RetryClass::classify(false, None, &codes) == RetryClass::Retriable {
                StepError::Retry(OperationFailure::new(FailureKind::Server, message))
            } else {
                fatal(FailureKind::Rejected, message)
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
        let body = read_range(std::path::Path::new(&self.input.path), offset, length)
            .map_err(|error| fatal(FailureKind::Rejected, format!("source file is unreadable: {error}")))?;
        locked(&self.inner.counters).record_transport_call();
        let runner = self.clone();
        let request = PutRequest {
            url: url.to_string(),
            headers: headers.to_vec(),
            body,
            cancel: self.token.clone(),
            sent: Arc::new(move |bytes| runner.report(part, bytes)),
        };
        match tokio::time::timeout(self.inner.config.upload.put_timeout, self.inner.transport.put(request)).await {
            Err(_) => Err(StepError::Retry(OperationFailure::new(FailureKind::Network, "put timed out"))),
            Ok(Err(error)) => Err(step_error(error, self.token.is_cancelled())),
            Ok(Ok(response)) => Ok(response.etag),
        }
    }

    pub(super) async fn retrying<T, F, Fut>(&self, mut call: F) -> Result<T, Stop>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, StepError>>,
    {
        let backoff = self.inner.config.upload.backoff;
        let mut attempt = 0;
        loop {
            self.await_online().await?;
            match call().await {
                Ok(value) => return Ok(value),
                Err(StepError::Cancelled) => return Err(Stop::Cancelled),
                Err(StepError::Expired) => return Err(Stop::Expired),
                Err(StepError::Fatal(failure)) => return Err(Stop::Failed(failure)),
                Err(StepError::Retry(failure)) => {
                    if !self.inner.connectivity.is_online() {
                        continue;
                    }
                    if !backoff.allows(attempt) {
                        return Err(Stop::Failed(failure));
                    }
                    self.save(|state| state.retries += 1)?;
                    tokio::select! {
                        () = tokio::time::sleep(backoff.delay(attempt)) => {}
                        () = self.token.cancelled() => return Err(Stop::Cancelled),
                    }
                    attempt += 1;
                }
            }
        }
    }

    pub(super) async fn await_online(&self) -> Result<(), Stop> {
        if self.inner.connectivity.is_online() {
            return Ok(());
        }
        tokio::select! {
            () = self.inner.connectivity.online() => Ok(()),
            () = self.token.cancelled() => Err(Stop::Cancelled),
        }
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
