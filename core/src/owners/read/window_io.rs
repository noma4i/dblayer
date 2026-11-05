//! Runner of the effects of `Windows`: the request of a page goes through the `Transport` of the core, the answer comes back as
//! `WindowsInput::Transport`. A cancelled request gives no answer.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use tokio::runtime::Handle;

use super::ports::{Admit, Rest};
use crate::owners::contract::{ErrorClass, RequestError, RequestId, TransportOutcome};
use crate::owners::read::windows::{AnswerPage, WindowEffect, WindowKey, Windows, WindowsEffect, WindowsInput};
use crate::runtime::{Back, EffectRunner, EpochStamp};
use crate::task::{spawn_on, Carry};
use crate::transport::{CancelToken, GraphQLRequest, Response, RetryClass, Transport, TransportError};

/// The wire form of a window: how a page is asked and how its answer reads.
pub trait WindowCodec: Send + Sync + 'static {
    fn request(&self, key: &WindowKey, page: u32, cursor: Option<&str>) -> Result<GraphQLRequest, String>;
    fn answer(&self, key: &WindowKey, page: u32, response: Response) -> Result<AnswerPage, String>;

    fn refusal(&self, response: &Response) -> Option<RequestError> {
        if response.errors.is_empty() { return None; }
        let class = if response.errors.iter().any(|error| error.code() == Some(crate::transport::retry::INVALID_CURSOR)) { ErrorClass::InvalidCursor } else { ErrorClass::Permanent };
        let source = TransportError::Graphql { messages: response.errors.iter().map(|error| error.message.clone()).collect(), extensions: response.extensions() };
        Some(RequestError { message: source.to_string(), class, source: Some(source) })
    }
}

type Open = Arc<Mutex<HashMap<(RequestId, u32), CancelToken>>>;

pub struct WindowIo {
    transport: Option<Arc<dyn Transport>>,
    codec: Arc<dyn WindowCodec>,
    admit: Admit,
    handle: Handle,
    counted: Option<crate::runtime::Handle>,
    open: Open,
    rest: Rest<WindowsEffect>,
}

impl WindowIo {
    pub fn new(transport: Option<Arc<dyn Transport>>, codec: Arc<dyn WindowCodec>, admit: Admit, handle: Handle, rest: Rest<WindowsEffect>) -> Self {
        Self { transport, codec, admit, handle, counted: None, open: Open::default(), rest }
    }

    /// The barrier of the owners waits for the requests this executor starts.
    pub fn counted_by(mut self, owners: crate::runtime::Handle) -> Self {
        self.counted = Some(owners);
        self
    }

    fn request(&self, stamp: EpochStamp, key: WindowKey, (request, attempt): (RequestId, u32), page: u32, cursor: Option<String>, back: &Back<Windows>) {
        let token = CancelToken::new();
        self.open.lock().unwrap_or_else(PoisonError::into_inner).insert((request, attempt), token.clone());
        let (transport, codec, admit, open, back) = (self.transport.clone(), self.codec.clone(), self.admit.clone(), self.open.clone(), back.clone());
        let launch = self.counted.as_ref().map(crate::runtime::Handle::launch);
        spawn_on(Carry::Detached, &self.handle, crate::runtime::counted(launch, async move {
            let outcome = tokio::select! {
                () = token.cancelled() => None,
                outcome = ask(transport.as_deref(), codec.as_ref(), &admit, &key, page, cursor.as_deref()) => Some(outcome),
            };
            open.lock().unwrap_or_else(PoisonError::into_inner).remove(&(request, attempt));
            if let Some(outcome) = outcome.filter(|_| !token.is_cancelled()) {
                back.reply(stamp, match outcome {
                    TransportOutcome::Response(page) => WindowsInput::Response { request, attempt, page },
                    TransportOutcome::Error(error) => WindowsInput::Failure { request, attempt, error },
                    TransportOutcome::Dropped => WindowsInput::Dropped { request, attempt },
                });
            }
        }));
    }

    fn cancel(&self, request: RequestId, attempt: u32) {
        let token = self.open.lock().unwrap_or_else(PoisonError::into_inner).remove(&(request, attempt));
        if let Some(token) = token {
            token.cancel();
        }
    }
}

impl EffectRunner<Windows> for WindowIo {
    fn run(&mut self, stamp: EpochStamp, effect: WindowsEffect, back: &Back<Windows>) {
        match effect {
            WindowsEffect::Window { key, effect: WindowEffect::Request { request, attempt, page, cursor } } => self.request(stamp, key, (request, attempt), page, cursor, back),
            WindowsEffect::Window { effect: WindowEffect::Cancel { request, attempt }, .. } => self.cancel(request, attempt),
            other => (self.rest)(stamp, other),
        }
    }
}

async fn ask(transport: Option<&dyn Transport>, codec: &dyn WindowCodec, admit: &Admit, key: &WindowKey, page: u32, cursor: Option<&str>) -> TransportOutcome<AnswerPage> {
    let admitted = match admit() {
        Ok(admitted) => admitted,
        Err(refused) => return TransportOutcome::Error(RequestError { message: refused.0, class: ErrorClass::Permanent, source: None }),
    };
    let request = match codec.request(key, page, cursor) {
        Ok(request) => request,
        Err(message) => return TransportOutcome::Error(RequestError { message, class: ErrorClass::Permanent, source: None }),
    };
    let Some(transport) = transport else {
        return TransportOutcome::Error(RequestError { message: "the database has no transport".into(), class: ErrorClass::Network, source: None });
    };
    match transport.query(&admitted, request).await {
        Ok(response) => match codec.refusal(&response) {
            Some(error) => TransportOutcome::Error(error),
            None => match codec.answer(key, page, response) {
                Ok(answer) => TransportOutcome::Response(answer),
                Err(message) => TransportOutcome::Error(RequestError { message, class: ErrorClass::Permanent, source: None }),
            },
        },
        Err(error) => failure(&error),
    }
}

/// `Interrupted` = the request left and no answer came; every other error has the class of its retry policy.
pub(super) fn failure<R>(error: &TransportError) -> TransportOutcome<R> {
    if matches!(error, TransportError::Graphql { extensions: Some(extensions), .. } if extensions.code.as_deref() == Some(crate::transport::retry::INVALID_CURSOR)) {
        return TransportOutcome::Error(RequestError { message: error.to_string(), class: ErrorClass::InvalidCursor, source: Some(error.clone()) });
    }
    let class = match RetryClass::of_error(error) {
        _ if matches!(error, TransportError::Interrupted(_)) => return TransportOutcome::Dropped,
        RetryClass::Network => ErrorClass::Network,
        RetryClass::Server | RetryClass::Retriable => ErrorClass::Server,
        RetryClass::Fatal => ErrorClass::Permanent,
    };
    TransportOutcome::Error(RequestError { message: error.to_string(), class, source: Some(error.clone()) })
}

impl Drop for WindowIo {
    fn drop(&mut self) { crate::db::lock::assert_no_library_lock("WindowIo drop"); }
}
