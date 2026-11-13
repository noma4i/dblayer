//! Runner of the effects of `Feed`: IO of the cable connections without transitions. It opens a connection and runs its handshake (`HandshakeDone`),
//! decrypts every frame of the server and reads it by the codec (`FrameOpened`), encrypts the frames of `Seal` with the key of the connection and the
//! counter the effect names. The counters and the protocol transitions belong to `Feed`. A closed connection gives no more inputs.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[cfg(test)]
pub(crate) static FRAMES_QUEUED: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);

use serde_json::{json, Value};
use tokio::runtime::Handle;
use tokio::sync::mpsc;

use super::feed::{CableClose, CableCloseInitiator, Frame, FramedEvent, Outgoing, ProtocolReason};
use super::ports::{Admit, Rest};
use crate::owners::contract::ConnectionId;
use crate::owners::read::feed::{Feed, FeedEffect, FeedInput};
use crate::runtime::{Back, EffectRunner, EpochStamp};
use crate::task::spawn_on;
use crate::transport::{CableLine, CableLines, CancelToken, GraphQLRequest, Incoming, Response, ResponseShape, TransportError};

/// The one reading of the frames: what the subscription asks and which events a response carries.
pub trait FeedCodec: Send + Sync + 'static {
    fn request(&self, channel: &str, variables: Value) -> GraphQLRequest;
    /// The events of a `next` response of `channel`, none when it carries no model event.
    fn events(&self, channel: &str, response: Response) -> Vec<FramedEvent>;
}

/// How long the queued frames of a closing established connection may still go out.
pub(crate) const CLOSE_DRAIN: Duration = Duration::from_secs(2);

struct Open {
    frames: Option<mpsc::UnboundedSender<(u64, Value)>>,
    cancel: CancelToken,
    /// Set at a draining `Close`: the line closes when the queue is sent and the connection waits for `Cancel`.
    closing: CancelToken,
    reason: Arc<AtomicU8>,
}

type Connections = HashMap<ConnectionId, Open>;

pub struct FeedIo {
    lines: Arc<dyn CableLines>,
    codec: Arc<dyn FeedCodec>,
    handle: Handle,
    counted: Option<crate::runtime::Handle>,
    admit: Option<Admit>,
    open: Connections,
    rest: Rest<FeedEffect>,
    gate: Arc<crate::runtime::EpochGate>,
}

/// The codes of a closing server that no retry changes: the session is over.
fn session_closed(code: u16, reason: &str) -> bool { matches!(code, 4401 | 4403) || matches!(reason, "LOGGED_OUT" | "ACCOUNT_LOCKED") }

impl FeedIo {
    pub fn new(lines: Arc<dyn CableLines>, codec: Arc<dyn FeedCodec>, handle: Handle, rest: Rest<FeedEffect>, gate: Arc<crate::runtime::EpochGate>) -> Self { Self { lines, codec, handle, gate, counted: None, admit: None, open: Connections::default(), rest } }

    /// A connection starts with the admission of the epoch it was decided in, and its end cancels it.
    pub fn admitted_by(mut self, admit: Admit) -> Self {
        self.admit = Some(admit);
        self
    }

    /// The barrier of the owners waits for the connections this executor starts.
    pub fn counted_by(mut self, owners: crate::runtime::Handle) -> Self {
        self.counted = Some(owners);
        self
    }

    fn connect(&mut self, stamp: EpochStamp, connection: ConnectionId, back: &Back<Feed>) {
        if let (Some(admit), EpochStamp::Epoch(epoch)) = (&self.admit, stamp) {
            if admit(epoch).is_err() { return back.reply(stamp, FeedInput::SubscriptionEnded { connection_id: connection }); }
        }
        let cancel = CancelToken::new();
        let closing = CancelToken::new();
        let reason = Arc::new(AtomicU8::new(0));
        let (frames, queued) = mpsc::unbounded_channel::<(u64, Value)>();
        self.open.retain(|_, open| !open.cancel.is_cancelled());
        self.open.insert(connection, Open { frames: Some(frames), cancel: cancel.clone(), closing: closing.clone(), reason: reason.clone() });
        let (lines, codec, back, gate) = (self.lines.clone(), self.codec.clone(), back.clone(), self.gate.clone());
        let epoch = match stamp { EpochStamp::Epoch(epoch) => Some(epoch), EpochStamp::Control => None };
        let token = cancel.clone();
        let report = back.clone();
        let reply = move |input: FeedInput| {
            if !token.is_cancelled() {
                back.reply(stamp, input);
            }
        };
        let launch = self.counted.as_ref().map(crate::runtime::Handle::launch);
        spawn_on(&self.handle, crate::runtime::counted(launch, async move {
            let ended = cancel.clone();
            Self::run((lines, codec), (gate, epoch), connection, (cancel, closing), queued, (reply, move |event| report.reply(stamp, FeedInput::CableClosed(event))), reason).await;
            ended.cancel();
        }));
    }

    async fn run((lines, codec): (Arc<dyn CableLines>, Arc<dyn FeedCodec>), (gate, epoch): (Arc<crate::runtime::EpochGate>, Option<crate::runtime::Epoch>), connection: ConnectionId, (cancel, closing): (CancelToken, CancelToken), mut queued: mpsc::UnboundedReceiver<(u64, Value)>, (reply, report): (impl Fn(FeedInput) + Clone + Send + 'static, impl Fn(CableClose)), reason: Arc<AtomicU8>) {
        {
            let line = match crate::runtime::send_in_epoch(&gate, epoch, || async { Ok(lines.connect(&cancel).await) }).await {
                Ok(Ok(line)) => line,
                Ok(Err(error)) => {
                    if error.socket_open {
                        let (code, detail, failure) = error.closed.unwrap_or_default();
                        let initiator = if code.is_some() { CableCloseInitiator::Server } else if failure.is_some() { CableCloseInitiator::Network } else { CableCloseInitiator::Client };
                        report(CableClose { connection_id: connection, code, reason: detail, failure, initiator, intended: matches!(error.error, TransportError::Interrupted(_)) });
                    }
                    return reply(match error.error {
                        TransportError::Session { .. } => FeedInput::SubscriptionEnded { connection_id: connection },
                        TransportError::Decode(_) => FeedInput::ProtocolError { connection_id: connection, reason: ProtocolReason::Handshake },
                        _ => FeedInput::Dropped { connection_id: connection },
                    });
                }
                Err(_) => return reply(FeedInput::Dropped { connection_id: connection }),
            };
            reply(FeedInput::HandshakeDone { connection_id: connection, key_id: connection });
            let closed = Arc::new(AtomicBool::new(false));
            let writer = {
                let (line, reply, gate, cancel, closing, closed) = (line.clone(), reply.clone(), gate.clone(), cancel.clone(), closing.clone(), closed.clone());
                async move {
                    while let Some((counter, body)) = queued.recv().await {
                        #[cfg(test)]
                        FRAMES_QUEUED.fetch_sub(1, Ordering::SeqCst);
                        if crate::runtime::send_in_epoch(&gate, epoch, || crate::lock::effect(|| line.send(counter, body))).await.is_err() {
                            reply(FeedInput::Dropped { connection_id: connection });
                            return false;
                        }
                    }
                    if closing.is_cancelled() {
                        if !closed.swap(true, Ordering::SeqCst) { line.close().await; }
                        return std::future::pending().await;
                    }
                    cancel.cancel();
                    true
                }
            };
            let reader = Self::read(line.clone(), codec, connection, reply);
            let event = tokio::select! {
                biased;
                () = cancel.cancelled() => None,
                sent = writer => if sent { None } else { Some(CableClose { connection_id: connection, code: None, reason: None, failure: None, initiator: CableCloseInitiator::Client, intended: false }) },
                event = reader => Some(event),
            };
            let event = event.unwrap_or_else(|| CableClose { connection_id: connection, code: None, reason: None, failure: None, initiator: CableCloseInitiator::Client, intended: reason.load(Ordering::SeqCst) != 2 });
            report(event);
            if !closed.swap(true, Ordering::SeqCst) { line.close().await; }
            if closing.is_cancelled() {
                cancel.cancelled().await;
                line.cancel().await;
            }
        }
    }

    async fn read(line: Arc<dyn CableLine>, codec: Arc<dyn FeedCodec>, connection: ConnectionId, reply: impl Fn(FeedInput)) -> CableClose {
        loop {
            match line.next().await {
                Incoming::Frame { counter, body } => {
                    let frame = match Self::frame_of(&codec, &body) {
                        Ok(frame) => frame,
                        Err(reason) => { reply(FeedInput::ProtocolError { connection_id: connection, reason }); return CableClose { connection_id: connection, code: None, reason: None, failure: None, initiator: CableCloseInitiator::Client, intended: false }; },
                    };
                    let last = matches!(&frame, Frame::Ended | Frame::Error { id: None, .. });
                    reply(FeedInput::FrameOpened { connection_id: connection, counter, frame });
                    if last {
                        return CableClose { connection_id: connection, code: None, reason: None, failure: None, initiator: CableCloseInitiator::Server, intended: false };
                    }
                }
                Incoming::Closed { code, reason, failure } => {
                    reply(if session_closed(code.unwrap_or(1006), reason.as_deref().unwrap_or_default()) { FeedInput::SubscriptionEnded { connection_id: connection } } else { FeedInput::Dropped { connection_id: connection } });
                    let initiator = if code.is_some() { CableCloseInitiator::Server } else { CableCloseInitiator::Network };
                    return CableClose { connection_id: connection, code, reason, failure, initiator, intended: false };
                }
                Incoming::Unreadable(_) => { reply(FeedInput::ProtocolError { connection_id: connection, reason: ProtocolReason::Decrypt }); return CableClose { connection_id: connection, code: None, reason: None, failure: None, initiator: CableCloseInitiator::Client, intended: false }; }
            }
        }
    }

    /// The meaning of a frame the cable already validated. A `next` that is no response is a broken frame.
    fn frame_of(codec: &Arc<dyn FeedCodec>, body: &Value) -> Result<Frame, ProtocolReason> {
        let id = body["id"].as_str().map(str::to_string);
        Ok(match body["t"].as_str() {
            Some("ping") => Frame::Ping { ts: body["ts"].as_number().cloned().ok_or(ProtocolReason::Frame)? },
            Some("pong") => Frame::Pong,
            Some("closing") => Frame::Ended,
            Some("error") => Frame::Error { id, retryable: body["retryable"].as_bool().ok_or(ProtocolReason::Frame)? },
            Some("complete") => Frame::Complete { id: id.unwrap_or_default() },
            Some("subscribed") => Frame::Confirmed { id: id.unwrap_or_default() },
            Some("next") => {
                let response = serde_json::from_value::<Response>(body["result"].clone()).map_err(|_| ProtocolReason::Frame)?;
                match response.shape() {
                    ResponseShape::Empty => return Err(ProtocolReason::Frame),
                    ResponseShape::ErrorsOnly | ResponseShape::DataWithErrors => return Ok(Frame::Error { id, retryable: body["retryable"].as_bool().unwrap_or(false) }),
                    _ => {}
                }
                let id = id.unwrap_or_default();
                let events = codec.events(&id, response);
                Frame::Events { id, events }
            }
            _ => Frame::Quiet,
        })
    }

    fn body_of(&self, frame: Outgoing) -> Value {
        match frame {
            Outgoing::Subscribe { channel, variables } => {
                let request = self.codec.request(&channel, variables.map_or_else(|| json!({}), Value::Object));
                json!({"t":"subscribe", "id":channel, "query":request.document, "variables":request.variables, "operationName":request.operation_name})
            }
            Outgoing::Unsubscribe { channel } => json!({"t":"unsubscribe", "id":channel}),
            Outgoing::Pong { ts } => json!({"t":"pong", "ts":ts}),
            Outgoing::Ping { ts } => json!({"t":"ping", "ts":ts}),
        }
    }

    fn seal(&mut self, connection: ConnectionId, counter: u64, frame: Outgoing) {
        let body = self.body_of(frame);
        let frames = self.open.get(&connection).and_then(|open| open.frames.clone());
        if let Some(frames) = frames {
            #[cfg(test)]
            FRAMES_QUEUED.fetch_add(1, Ordering::SeqCst);
            let _ = crate::lock::effect(|| frames.send((counter, body)));
        }
    }

    /// The frames sealed before the close still go out: the writer ends when its queue is empty and the sender is gone, then the line closes.
    fn close(&mut self, connection: ConnectionId, goodbye: bool, drain: bool, intended: bool) {
        if let Some(open) = self.open.get(&connection) { open.reason.store(if intended { 1 } else { 2 }, Ordering::SeqCst); }
        if drain {
            if let Some(open) = self.open.get_mut(&connection) {
                open.closing.cancel();
                open.frames = None;
            }
            return;
        }
        let Some(open) = self.open.remove(&connection) else { return };
        if !goodbye {
            open.cancel.cancel();
        }
    }

    fn cancel(&mut self, connection: ConnectionId) {
        if let Some(open) = self.open.remove(&connection) {
            open.cancel.cancel();
        }
    }
}

impl EffectRunner<Feed> for FeedIo {
    fn run(&mut self, stamp: EpochStamp, effect: FeedEffect, back: &Back<Feed>) {
        match effect {
            FeedEffect::Connect { connection } => self.connect(stamp, connection, back),
            FeedEffect::Seal { connection, counter, frame, .. } => self.seal(connection, counter, frame),
            FeedEffect::Close { connection, goodbye, drain, intended } => self.close(connection, goodbye, drain, intended),
            FeedEffect::Cancel { connection } => self.cancel(connection),
            other => (self.rest)(stamp, other),
        }
    }
}

impl Drop for FeedIo {
    fn drop(&mut self) {
        crate::lock::assert_no_library_lock("FeedIo drop");
        self.open.values().for_each(|open| open.cancel.cancel());
    }
}
