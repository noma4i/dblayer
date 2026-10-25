use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::mpsc::UnboundedReceiver;

use super::request::Response;

#[derive(Clone)]
pub struct ServerSubscriptionHandle {
    inner: Arc<HandleInner>,
}

struct HandleInner {
    cancelled: AtomicBool,
    on_cancel: Box<dyn Fn() + Send + Sync>,
}

impl ServerSubscriptionHandle {
    pub fn new(on_cancel: impl Fn() + Send + Sync + 'static) -> Self {
        Self { inner: Arc::new(HandleInner { cancelled: AtomicBool::new(false), on_cancel: Box::new(on_cancel) }) }
    }

    pub fn cancel(&self) {
        if !self.inner.cancelled.swap(true, Ordering::SeqCst) {
            (self.inner.on_cancel)();
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubscriptionFailureKind {
    Transport,
    Graphql,
    Internal,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SubscriptionFailure {
    pub kind: SubscriptionFailureKind,
    pub error: super::TransportError,
    pub close: Option<SubscriptionClose>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionClose {
    pub code: u16,
    pub session: Option<super::SessionCode>,
    pub terminal: bool,
}

impl SubscriptionFailure {
    pub fn session_refused(&self) -> bool {
        matches!(self.error, super::TransportError::Session { .. }) || self.close.as_ref().is_some_and(|close| close.terminal)
    }

    pub fn new(kind: SubscriptionFailureKind, message: impl Into<String>) -> Self {
        let message = message.into();
        let error = match kind {
            SubscriptionFailureKind::Transport => super::TransportError::Network(message),
            SubscriptionFailureKind::Graphql => super::TransportError::Graphql { messages: vec![message], extensions: None },
            SubscriptionFailureKind::Internal => super::TransportError::Decode(message),
        };
        Self { kind, error, close: None }
    }

    pub fn from_error(error: super::TransportError) -> Self {
        let kind = match &error {
            super::TransportError::Graphql { .. } => SubscriptionFailureKind::Graphql,
            error if super::RetryClass::of_error(error) != super::RetryClass::Fatal => SubscriptionFailureKind::Transport,
            _ => SubscriptionFailureKind::Internal,
        };
        Self { kind, error, close: None }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SubscriptionEvent {
    Ready,
    Next(Response),
    Failed(SubscriptionFailure),
}

impl SubscriptionEvent {
    pub fn response(response: Response) -> Self {
        if response.data.is_none() && !response.errors.is_empty() {
            let error = super::TransportError::Graphql { extensions: response.extensions(), messages: response.errors.into_iter().map(|error| error.message).collect() };
            return Self::Failed(SubscriptionFailure::from_error(error));
        }
        Self::Next(response)
    }
}

pub struct ServerSubscription {
    events: UnboundedReceiver<SubscriptionEvent>,
    handle: ServerSubscriptionHandle,
    #[cfg(any(test, feature = "stress"))]
    on_next: Option<Box<dyn Fn(ConsumerSignal) + Send + Sync>>,
}

#[cfg(any(test, feature = "stress"))]
#[derive(Clone, Copy)]
pub(crate) enum ConsumerSignal {
    Waiting,
    Received,
    Left,
}

#[cfg(any(test, feature = "stress"))]
struct Waiting<'a>(&'a (dyn Fn(ConsumerSignal) + Send + Sync));

#[cfg(any(test, feature = "stress"))]
impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        (self.0)(ConsumerSignal::Left);
    }
}

impl ServerSubscription {
    pub fn new(events: UnboundedReceiver<SubscriptionEvent>, handle: ServerSubscriptionHandle) -> Self {
        Self {
            events,
            handle,
            #[cfg(any(test, feature = "stress"))]
            on_next: None,
        }
    }

    #[cfg(any(test, feature = "stress"))]
    pub(crate) fn on_next(mut self, hook: impl Fn(ConsumerSignal) + Send + Sync + 'static) -> Self {
        self.on_next = Some(Box::new(hook));
        self
    }

    pub async fn next(&mut self) -> Option<SubscriptionEvent> {
        #[cfg(any(test, feature = "stress"))]
        let _waiting = self.on_next.as_deref().map(|hook| {
            hook(ConsumerSignal::Waiting);
            Waiting(hook)
        });
        let event = self.events.recv().await;
        #[cfg(any(test, feature = "stress"))]
        if let (Some(hook), Some(_)) = (&self.on_next, &event) {
            hook(ConsumerSignal::Received);
        }
        event
    }

    #[cfg(test)]
    pub fn cancel(&self) {
        self.handle.cancel();
    }
}

impl Drop for ServerSubscription {
    fn drop(&mut self) {
        self.handle.cancel();
    }
}
