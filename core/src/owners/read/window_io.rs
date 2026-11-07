//! Runner of the effects of `Windows`: the request of a page goes through the `Upstream` of the core, the answer comes back as
//! `WindowsInput::Upstream`. A cancelled request gives no answer.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::runtime::Handle;

use super::ports::{Admit, Rest};
use crate::owners::contract::{ErrorClass, RequestError, RequestId, TransportOutcome};
use crate::owners::read::windows::{request_task, read_task, AnswerPage, RequestKind, SavedWindow, WindowEffect, WindowKey, Windows, WindowsEffect, WindowsInput};
use crate::runtime::{Back, EffectRunner, Epoch, EpochStamp};
use crate::task::{spawn_on, Carry};
use crate::transport::{CancelToken, GraphQLRequest, Response, RetryClass, Upstream, TransportError};

/// The wire form of a window: how a page is asked and how its answer reads.
pub trait WindowCodec: Send + Sync + 'static {
    fn request(&self, key: &WindowKey, page: u32, cursor: Option<&str>, kind: RequestKind) -> Result<GraphQLRequest, String>;
    fn answer(&self, key: &WindowKey, page: u32, kind: RequestKind, response: Response) -> Result<AnswerPage, String>;

    fn refusal(&self, response: &Response) -> Option<RequestError> {
        if response.errors.is_empty() { return None; }
        let class = if response.errors.iter().any(|error| error.code() == Some(crate::transport::retry::INVALID_CURSOR)) { ErrorClass::InvalidCursor } else { ErrorClass::Permanent };
        let source = TransportError::Graphql { messages: response.errors.iter().map(|error| error.message.clone()).collect(), extensions: response.extensions() };
        Some(RequestError { message: source.to_string(), class, source: Some(source) })
    }
}

type Open = HashMap<(RequestId, u32), CancelToken>;

pub type WindowReader = Arc<dyn Fn(&WindowKey) -> Result<Option<SavedWindow>, String> + Send + Sync>;

pub struct WindowIo {
    transport: Option<Arc<dyn Upstream>>,
    codec: Arc<dyn WindowCodec>,
    admit: Admit,
    gate: Arc<crate::runtime::EpochGate>,
    handle: Handle,
    now: Arc<dyn Fn() -> i64 + Send + Sync>,
    counted: Option<crate::runtime::Handle>,
    open: Open,
    reader: Option<WindowReader>,
    rest: Rest<WindowsEffect>,
    #[cfg(test)]
    flights: Option<super::ports::RequestsOnTheWay>,
}

impl WindowIo {
    pub fn new(transport: Option<Arc<dyn Upstream>>, codec: Arc<dyn WindowCodec>, (admit, gate): (Admit, Arc<crate::runtime::EpochGate>), (handle, now): (Handle, Arc<dyn Fn() -> i64 + Send + Sync>), rest: Rest<WindowsEffect>) -> Self {
        Self { transport, codec, admit, gate, handle, now, counted: None, open: Open::default(), reader: None, rest, #[cfg(test)] flights: None }
    }

    #[cfg(test)]
    pub fn flights_in(mut self, flights: super::ports::RequestsOnTheWay) -> Self {
        self.flights = Some(flights);
        self
    }

    pub fn reading(mut self, reader: WindowReader) -> Self {
        self.reader = Some(reader);
        self
    }

    /// The barrier of the owners waits for the requests this executor starts.
    pub fn counted_by(mut self, owners: crate::runtime::Handle) -> Self {
        self.counted = Some(owners);
        self
    }

    fn request(&mut self, stamp: EpochStamp, key: WindowKey, (request, attempt): (RequestId, u32), (page, epoch, kind): (u32, Epoch, RequestKind), cursor: Option<String>, back: &Back<Windows>) {
        if let Err(refused) = (self.admit)(epoch) {
            return back.reply(stamp, WindowsInput::Failure { request, attempt, error: RequestError { message: refused.0, class: ErrorClass::Permanent, source: None } });
        }
        let token = CancelToken::new();
        self.open.retain(|_, flight| !flight.is_cancelled());
        self.open.insert((request, attempt), token.clone());
        let (transport, codec, back, admit, gate, now) = (self.transport.clone(), self.codec.clone(), back.clone(), self.admit.clone(), self.gate.clone(), self.now.clone());
        let launch = self.counted.as_ref().map(crate::runtime::Handle::launch);
        #[cfg(test)]
        let flight = self.flights.as_ref().map(super::ports::InFlight::start);
        spawn_on(Carry::Detached, &self.handle, crate::runtime::counted(launch, async move {
            let _watch = back.watch(request_task(request, attempt));
            #[cfg(test)]
            let _flight = flight;
            let outcome = tokio::select! {
                biased;
                () = token.cancelled() => None,
                outcome = async {
                    if let Err(refused) = admit(epoch) { return TransportOutcome::Error(RequestError { message: refused.0, class: ErrorClass::Permanent, source: None }); }
                    ask(transport.as_deref(), (&gate, epoch), codec.as_ref(), (&key, page, kind), (cursor.as_deref(), crate::transport::WireIds::new(crate::transport::Author::Window, request, u64::from(attempt)))).await
                } => Some(outcome),
            };
            let delivered = outcome.filter(|_| !token.is_cancelled());
            token.cancel();
            if let Some(outcome) = delivered {
                back.reply(stamp, match outcome {
                    TransportOutcome::Response(page) => WindowsInput::Response { request, attempt, page, now: now() },
                    TransportOutcome::Error(error) => WindowsInput::Failure { request, attempt, error },
                    TransportOutcome::Dropped => WindowsInput::Dropped { request, attempt },
                });
            }
        }));
    }

    fn read(&self, stamp: EpochStamp, (key, read_id): (WindowKey, u64), back: &Back<Windows>) -> Option<(WindowKey, u64)> {
        let Some(reader) = self.reader.clone() else { return Some((key, read_id)) };
        let back = back.clone();
        let launch = self.counted.as_ref().map(crate::runtime::Handle::launch);
        crate::task::spawn_blocking_on(Carry::Detached, &self.handle, move || {
            let _launch = launch;
            let _watch = back.watch(read_task(read_id));
            let result = reader(&key);
            back.reply(stamp, WindowsInput::WindowRead { key, read_id, result });
        });
        None
    }

    fn cancel(&mut self, request: RequestId, attempt: u32) {
        if let Some(token) = self.open.remove(&(request, attempt)) {
            token.cancel();
        }
    }
}

impl EffectRunner<Windows> for WindowIo {
    fn run(&mut self, stamp: EpochStamp, effect: WindowsEffect, back: &Back<Windows>) {
        match effect {
            WindowsEffect::Window { key, effect: WindowEffect::Request { request, attempt, page, cursor, epoch, kind } } => self.request(stamp, key, (request, attempt), (page, epoch, kind), cursor, back),
            WindowsEffect::Window { effect: WindowEffect::Cancel { request, attempt }, .. } => self.cancel(request, attempt),
            WindowsEffect::ReadWindow { key, read_id } => {
                if let Some((key, read_id)) = self.read(stamp, (key, read_id), back) { (self.rest)(stamp, WindowsEffect::ReadWindow { key, read_id }); }
            }
            other => (self.rest)(stamp, other),
        }
    }
}

async fn ask(transport: Option<&dyn Upstream>, (gate, epoch): (&crate::runtime::EpochGate, Epoch), codec: &dyn WindowCodec, (key, page, kind): (&WindowKey, u32, RequestKind), (cursor, wire): (Option<&str>, crate::transport::WireIds)) -> TransportOutcome<AnswerPage> {
    let request = match codec.request(key, page, cursor, kind) {
        Ok(request) => request.by(wire),
        Err(message) => return TransportOutcome::Error(RequestError { message, class: ErrorClass::Permanent, source: None }),
    };
    let Some(transport) = transport else {
        return TransportOutcome::Error(RequestError { message: "the database has no transport".into(), class: ErrorClass::Network, source: None });
    };
    match crate::runtime::send_in_epoch(gate, Some(epoch), || transport.query(request)).await {
        Ok(response) => match codec.refusal(&response) {
            Some(error) => TransportOutcome::Error(error),
            None => match codec.answer(key, page, kind, response) {
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
    fn drop(&mut self) { crate::lock::assert_no_library_lock("WindowIo drop"); }
}
