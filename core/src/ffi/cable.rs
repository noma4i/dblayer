use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use async_trait::async_trait;
use tokio::sync::Notify;
use crate::transport::{RawFailure, SocketEvent, TransportError};

pub(crate) const MAILBOX_FRAMES: usize = 256;
const GOING_AWAY: u16 = 1001;
const NORMAL: u16 = 1000;
const UNSUPPORTED_DATA: u16 = 1003;
const TRY_AGAIN_LATER: u16 = 1013;
const ABNORMAL: u16 = 1006;

/// One open platform socket. The core sends text and closes it, the platform does nothing else with it.
#[uniffi::export(rust, foreign)]
pub trait PlatformSocket: Send + Sync {
    fn send(&self, text: String) -> bool;
    /// Sends a close frame with `code`. The core calls `cancel` when the drain of the closed connection ends.
    fn close(&self, code: u16, reason: String);
    /// Ends the connection without a close handshake.
    fn cancel(&self);
}

/// What the platform reports about one socket, in the order it happens on that socket.
#[uniffi::export(rust, foreign)]
pub trait CableEvents: Send + Sync {
    fn frame(&self, text: String);
    fn binary(&self, bytes: Vec<u8>);
    /// The socket ended: `close_code` and `reason` are the peer's close frame when it sent one, `failure` the raw error when the connection broke.
    fn ended(&self, close_code: Option<u16>, reason: Option<String>, failure: Option<RawFailure>);
}

#[derive(uniffi::Enum)]
pub enum CableOutcome {
    Open { connection: Arc<dyn PlatformSocket> },
    /// `handshake_status` is the HTTP status the server answered the upgrade with, when it answered.
    Failed { failure: RawFailure, handshake_status: Option<u16> },
}

#[derive(Default)]
struct Queue {
    events: VecDeque<SocketEvent>,
    ended: bool,
}

/// The bounded queue between the platform socket and the reader. It never blocks the platform: a full queue ends the connection.
pub(super) struct Mailbox {
    queue: Mutex<Queue>,
    ready: Notify,
    connection: Mutex<Option<Arc<dyn PlatformSocket>>>,
}

impl Drop for Mailbox {
    fn drop(&mut self) { crate::lock::assert_no_library_lock("Mailbox::drop"); }
}

enum Push {
    Queued,
    Ended,
    Overflow,
}

impl Mailbox {
    pub(super) fn new() -> Self {
        Self { queue: Mutex::default(), ready: Notify::new(), connection: Mutex::default() }
    }

    fn push(&self, event: SocketEvent) -> Push {
        let pushed = {
            let mut queue = crate::lock::acquire(&self.queue);
            if queue.ended {
                Push::Ended
            } else if matches!(event, SocketEvent::Closed { .. }) {
                queue.ended = true;
                queue.events.push_back(event);
                Push::Queued
            } else if queue.events.len() >= MAILBOX_FRAMES {
                queue.ended = true;
                queue.events.push_back(SocketEvent::Closed { code: TRY_AGAIN_LATER, reason: "the cable queue is full".into() });
                Push::Overflow
            } else {
                queue.events.push_back(event);
                Push::Queued
            }
        };
        crate::lock::effect(|| self.ready.notify_one());
        pushed
    }

    fn end_with(&self, code: u16, reason: &str) -> bool {
        let ended = matches!(self.push(SocketEvent::Closed { code, reason: reason.into() }), Push::Queued);
        if ended { self.close_connection(code, reason); }
        ended
    }

    fn close_connection(&self, code: u16, reason: &str) {
        let connection = crate::lock::acquire(&self.connection).clone();
        if let Some(connection) = connection { crate::lock::effect(|| connection.close(code, reason.into())); }
    }

    pub(super) fn is_ended(&self) -> bool { crate::lock::acquire(&self.queue).ended }

    fn shut(&self) {
        {
            let mut queue = crate::lock::acquire(&self.queue);
            queue.ended = true;
            queue.events.clear();
        }
        let released = crate::lock::acquire(&self.connection).take();
        drop(released);
        crate::lock::effect(|| self.ready.notify_one());
    }

    async fn next(&self) -> Option<SocketEvent> {
        loop {
            {
                let mut queue = crate::lock::acquire(&self.queue);
                if let Some(event) = queue.events.pop_front() { return Some(event); }
                if queue.ended { return None; }
            }
            self.ready.notified().await;
        }
    }
}

impl CableEvents for Mailbox {
    fn frame(&self, text: String) {
        if matches!(self.push(SocketEvent::Text(text)), Push::Overflow) { self.close_connection(TRY_AGAIN_LATER, "the cable queue is full"); }
    }

    fn binary(&self, _: Vec<u8>) { self.end_with(UNSUPPORTED_DATA, "binary cable frames are not supported"); }

    fn ended(&self, close_code: Option<u16>, reason: Option<String>, failure: Option<RawFailure>) {
        let event = match (close_code, failure) {
            (Some(code), _) => SocketEvent::Closed { code, reason: reason.unwrap_or_default() },
            (None, Some(failure)) => SocketEvent::Closed { code: ABNORMAL, reason: failure.message },
            (None, None) => SocketEvent::Closed { code: ABNORMAL, reason: reason.unwrap_or_default() },
        };
        self.push(event);
    }
}

pub(super) struct ForeignSocket {
    connection: Arc<dyn PlatformSocket>,
    mailbox: Arc<Mailbox>,
    closed: AtomicBool,
}

impl ForeignSocket {
    pub(super) fn new(connection: Arc<dyn PlatformSocket>, mailbox: Arc<Mailbox>) -> Self {
        *crate::lock::acquire(&mailbox.connection) = Some(connection.clone());
        let socket = Self { connection, mailbox, closed: AtomicBool::new(false) };
        if socket.mailbox.is_ended() { socket.shut(NORMAL); }
        socket
    }

    fn shut(&self, code: u16) {
        if self.closed.swap(true, Ordering::SeqCst) { return; }
        crate::lock::assert_no_library_lock("PlatformSocket::close");
        self.connection.close(code, String::new());
    }
}

impl Drop for ForeignSocket {
    fn drop(&mut self) {
        crate::lock::assert_no_library_lock("ForeignSocket::drop");
        self.shut(GOING_AWAY);
    }
}

#[async_trait]
impl crate::transport::CableSocket for ForeignSocket {
    async fn send(&self, text: String) -> Result<(), TransportError> {
        crate::lock::assert_no_library_lock("PlatformSocket::send");
        if self.closed.load(Ordering::SeqCst) || self.mailbox.is_ended() { return Err(TransportError::Interrupted("the cable socket is closed".into())); }
        if crate::lock::effect(|| self.connection.send(text)) { Ok(()) } else { Err(TransportError::Network("the cable socket refused the text frame".into())) }
    }

    async fn next(&self) -> Option<SocketEvent> {
        crate::lock::assert_no_library_lock("CableMailbox::next");
        self.mailbox.next().await
    }

    async fn close(&self) {
        self.shut(NORMAL);
        self.mailbox.shut();
    }

    async fn cancel(&self) { crate::lock::effect(|| self.connection.cancel()); }
}
