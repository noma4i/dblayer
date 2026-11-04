use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::collections::VecDeque;
use tokio::sync::Notify;

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
            crate::db::lock::assert_no_library_lock("ServerSubscription cancel");
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
    pub fn is_terminal(&self) -> bool {
        self.kind != SubscriptionFailureKind::Transport || matches!(self.error, super::TransportError::Session { .. })
    }

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
            super::TransportError::Interrupted(_) => SubscriptionFailureKind::Transport,
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
    Lost { first: u64, last: u64 },
    Failed(SubscriptionFailure),
}

const CAPACITY: usize = 256;

struct Queue {
    events: VecDeque<SubscriptionEvent>,
    ordinal: u64,
    lost: Option<(u64, u64)>,
    after_ready: Option<(u64, u64)>,
    ready: bool,
    failure: Option<SubscriptionFailure>,
    senders: usize,
    receiving: bool,
}

struct Channel { queue: Mutex<Queue>, wake: Notify }

pub struct SubscriptionSender(Arc<Channel>);
pub struct SubscriptionReceiver(Arc<Channel>);

pub fn subscription_channel() -> (SubscriptionSender, SubscriptionReceiver) {
    let channel = Arc::new(Channel { queue: Mutex::new(Queue { events: VecDeque::new(), ordinal: 0, lost: None, after_ready: None, ready: false, failure: None, senders: 1, receiving: true }), wake: Notify::new() });
    (SubscriptionSender(channel.clone()), SubscriptionReceiver(channel))
}

impl Clone for SubscriptionSender {
    fn clone(&self) -> Self {
        self.0.queue.lock().expect("subscription queue").senders += 1;
        Self(self.0.clone())
    }
}

impl Drop for SubscriptionSender {
    fn drop(&mut self) {
        let ended = {
            let mut queue = self.0.queue.lock().expect("subscription queue");
            queue.senders -= 1;
            queue.senders == 0
        };
        if ended { self.0.wake.notify_one(); }
    }
}

impl SubscriptionSender {
    pub fn send(&self, event: SubscriptionEvent) -> Result<(), ()> {
        let mut queue = self.0.queue.lock().expect("subscription queue");
        if !queue.receiving { return Err(()); }
        if queue.failure.is_some() { return Ok(()); }
        if matches!(event, SubscriptionEvent::Next(_)) { queue.ordinal += 1; }
        if queue.events.len() < CAPACITY && queue.lost.is_none() && queue.after_ready.is_none() && !queue.ready {
            queue.events.push_back(event);
        } else {
            match event {
                SubscriptionEvent::Next(_) => {
                    let ordinal = queue.ordinal;
                    let lost = if queue.ready { &mut queue.after_ready } else { &mut queue.lost };
                    if let Some((_, last)) = lost { *last = ordinal; } else { *lost = Some((ordinal, ordinal)); }
                }
                SubscriptionEvent::Ready => queue.ready = true,
                SubscriptionEvent::Failed(failure) => { queue.failure.get_or_insert(failure); }
                SubscriptionEvent::Lost { first, last } => {
                    let lost = if queue.ready { &mut queue.after_ready } else { &mut queue.lost };
                    if let Some((_, end)) = lost { *end = last; } else { *lost = Some((first, last)); }
                }
            }
        }
        drop(queue);
        self.0.wake.notify_one();
        Ok(())
    }
}

impl SubscriptionReceiver {
    pub(crate) fn try_recv(&mut self) -> Result<SubscriptionEvent, ()> {
        let mut queue = self.0.queue.lock().expect("subscription queue");
        if let Some(event) = queue.events.pop_front() { return Ok(event); }
        if let Some((first, last)) = queue.lost.take() { return Ok(SubscriptionEvent::Lost { first, last }); }
        if std::mem::take(&mut queue.ready) {
            queue.lost = queue.after_ready.take();
            return Ok(SubscriptionEvent::Ready);
        }
        if let Some(failure) = queue.failure.take() { return Ok(SubscriptionEvent::Failed(failure)); }
        Err(())
    }

    pub(crate) async fn recv(&mut self) -> Option<SubscriptionEvent> {
        loop {
            let channel = self.0.clone();
            let notified = channel.wake.notified();
            if let Ok(event) = self.try_recv() { return Some(event); }
            if self.0.queue.lock().expect("subscription queue").senders == 0 { return None; }
            notified.await;
        }
    }
}

impl Drop for SubscriptionReceiver {
    fn drop(&mut self) { self.0.queue.lock().expect("subscription queue").receiving = false; }
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
    events: SubscriptionReceiver,
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
    pub fn new(events: SubscriptionReceiver, handle: ServerSubscriptionHandle) -> Self {
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

    pub(crate) fn take_queued_responses(&mut self) -> bool {
        let mut taken = false;
        while let Ok(event) = self.events.try_recv() {
            taken |= matches!(event, SubscriptionEvent::Next(_) | SubscriptionEvent::Lost { .. });
        }
        taken
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
