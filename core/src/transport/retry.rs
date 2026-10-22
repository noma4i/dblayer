use std::sync::{Arc, Mutex};

use super::error::TransportError;
use crate::pacing::{Backoff, Clock, RetryPolicy};
use super::{Connectivity, GraphQLRequest, Response, Transport};

const RETRIABLE_CODES: [&str; 4] = ["INTERNAL_SERVER_ERROR", "TIMEOUT", "SERVICE_UNAVAILABLE", "DATABASE_ERROR"];
// GraphQL list cursor errors require restarting pagination from the first page.
pub(crate) const INVALID_CURSOR: &str = "INVALID_CURSOR";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryClass {
    Network,
    Server,
    Retriable,
    Fatal,
}

impl RetryClass {
    pub fn classify(is_network: bool, http_status: Option<u16>, graphql_codes: &[&str]) -> RetryClass {
        if is_network {
            return RetryClass::Network;
        }
        if http_status.is_some_and(|status| (500..=599).contains(&status)) {
            return RetryClass::Server;
        }
        if matches!(http_status, Some(408 | 429)) {
            return RetryClass::Retriable;
        }
        if graphql_codes.iter().any(|code| RETRIABLE_CODES.contains(code)) {
            return RetryClass::Retriable;
        }
        RetryClass::Fatal
    }

    pub fn of_error(error: &TransportError) -> RetryClass {
        match error {
            TransportError::Security { .. } | TransportError::Config { .. } => RetryClass::Fatal,
            TransportError::Network(_) => RetryClass::Network,
            TransportError::Http { status, .. } => RetryClass::classify(false, Some(*status), &[]),
            TransportError::Graphql { messages, extensions } => {
                let codes: Vec<_> = extensions.iter().filter_map(|value| value.code.as_deref()).chain(messages.iter().map(String::as_str)).collect();
                RetryClass::classify(false, Some(200), &codes)
            }
            TransportError::RequestTooLarge { .. } | TransportError::Session { .. } | TransportError::Interrupted(_) | TransportError::Decode(_) | TransportError::Unscripted(_) => RetryClass::Fatal,
        }
    }

}

#[uniffi::export(rust, foreign)]
pub trait RetryListener: Send + Sync {
    fn retrying(&self, attempt: u64, delay_ms: u64, reason: String);
}

#[derive(Default)]
pub(crate) struct RetryEvents(Mutex<Option<Arc<dyn RetryListener>>>);

impl RetryEvents {
    pub(crate) fn set(&self, listener: Option<Arc<dyn RetryListener>>) { *crate::db::locked(&self.0) = listener; }
}

pub(crate) struct Retrying<'a> {
    clock: &'a dyn crate::pacing::Clock,
    events: &'a RetryEvents,
    backoff: Backoff,
    attempt: u64,
    online: &'a Connectivity,
}

impl<'a> Retrying<'a> {
    pub(crate) fn new(clock: &'a dyn Clock, events: &'a RetryEvents, online: &'a Connectivity, policy: RetryPolicy) -> Self {
        Self { clock, events, online, backoff: policy.backoff().randomized(), attempt: 0 }
    }

    pub(crate) async fn failed(&mut self, error: TransportError) -> Result<(), TransportError> {
        if RetryClass::of_error(&error) == RetryClass::Fatal || !self.backoff.allows(self.attempt.min(u32::MAX as u64) as u32) { return Err(error); }
        self.online.online().await;
        let delay = self.backoff.delay(self.attempt.min(u32::MAX as u64) as u32);
        self.attempt = self.attempt.saturating_add(1);
        let listener = crate::db::locked(&self.events.0).clone();
        if let Some(listener) = listener {
            let reason = match &error {
                TransportError::Network(_) => "network".to_string(),
                TransportError::Http { status, .. } => format!("HTTP {status}"),
                TransportError::Graphql { messages, extensions } => extensions.as_ref().and_then(|value| value.code.clone()).or_else(|| messages.first().cloned()).unwrap_or_default(),
                _ => return Err(error),
            };
            listener.retrying(self.attempt, delay.as_millis() as u64, reason);
        }
        self.online.wait(self.clock, delay).await;
        Ok(())
    }
}

pub(crate) async fn query(transport: &dyn Transport, request: GraphQLRequest, clock: &dyn Clock, online: &Connectivity, called: impl Fn()) -> Result<(Response, u32), TransportError> {
    if transport.retries_queries() {
        called();
        return transport.query(request).await.map(|response| (response, 1));
    }
    let events = RetryEvents::default();
    let mut retry = Retrying::new(clock, &events, online, RetryPolicy::QueryNetwork);
    loop {
        online.online().await;
        called();
        let (error, response) = match transport.query(request.clone()).await {
            Ok(response) => {
                let codes: Vec<_> = response.errors.iter().filter_map(|error| error.code()).collect();
                if RetryClass::classify(false, None, &codes) == RetryClass::Fatal { return Ok((response, retry.attempt as u32 + 1)); }
                (TransportError::Graphql { messages: codes.into_iter().map(str::to_string).collect(), extensions: response.extensions() }, Some(response))
            }
            Err(error) => (error, None),
        };
        retry.backoff.max_attempts = match RetryClass::of_error(&error) {
            RetryClass::Network => RetryPolicy::QueryNetwork.backoff().max_attempts,
            _ => RetryPolicy::QueryServer.backoff().max_attempts,
        };
        if let Err(error) = retry.failed(error).await {
            return response.map(|response| (response, retry.attempt as u32 + 1)).ok_or(error);
        }
    }
}
