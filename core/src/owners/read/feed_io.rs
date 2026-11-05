//! Runner of the effects of `Feed`: IO of the cable connections without transitions. It opens a connection and runs its handshake (`HandshakeDone`),
//! decrypts every frame of the server and reads it by the codec (`FrameOpened`), encrypts the frames of `Seal` with the key of the connection and the
//! counter the effect names. The counters and the protocol transitions belong to `Feed`. A closed connection gives no more inputs.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::runtime::Handle;
use tokio::sync::mpsc;

use super::feed::{Frame, FramedEvent, Outgoing, ProtocolReason, SUBSCRIPTION};
use super::ports::Rest;
use crate::owners::contract::ConnectionId;
use crate::owners::read::feed::{Feed, FeedEffect, FeedInput};
use crate::runtime::{Back, EffectRunner, EpochStamp};
use crate::task::{spawn_on, Carry};
use crate::transport::{CableLine, CableLines, CancelToken, GraphQLRequest, Incoming, Response, ResponseShape, TransportError};

/// The one reading of the frames: what the subscription asks and which events a response carries.
pub trait FeedCodec: Send + Sync + 'static {
    fn request(&self, models: &[String]) -> GraphQLRequest;
    /// The events of a `next` response, none when it carries no model event.
    fn events(&self, response: Response) -> Vec<FramedEvent>;
}

/// How long the queued frames of a closing established connection may still go out.
const CLOSE_DRAIN: Duration = Duration::from_secs(2);

struct Open {
    frames: mpsc::UnboundedSender<(u64, Value)>,
    cancel: CancelToken,
    /// Set at `Close`: the writer gets `CLOSE_DRAIN` to send the queue, then the line closes.
    closing: CancelToken,
    established: Arc<AtomicBool>,
    /// Which attempt of the connection id owns the entry: a finished task removes only its own.
    attempt: u64,
}

type Connections = Arc<Mutex<HashMap<ConnectionId, Open>>>;

pub struct FeedIo {
    lines: Arc<dyn CableLines>,
    codec: Arc<dyn FeedCodec>,
    handle: Handle,
    open: Connections,
    attempts: AtomicU64,
    rest: Rest<FeedEffect>,
}

/// The codes of a closing server that no retry changes: the session is over.
fn session_closed(code: u16, reason: &str) -> bool { matches!(code, 4401 | 4403) || matches!(reason, "LOGGED_OUT" | "ACCOUNT_LOCKED") }

impl FeedIo {
    pub fn new(lines: Arc<dyn CableLines>, codec: Arc<dyn FeedCodec>, handle: Handle, rest: Rest<FeedEffect>) -> Self { Self { lines, codec, handle, open: Connections::default(), attempts: AtomicU64::new(0), rest } }

    fn connect(&self, stamp: EpochStamp, connection: ConnectionId, back: &Back<Feed>) {
        let cancel = CancelToken::new();
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
        let open = self.open.clone();
        let (closing, established) = (CancelToken::new(), Arc::new(AtomicBool::new(false)));
        let (frames, queued) = mpsc::unbounded_channel::<(u64, Value)>();
        self.open.lock().unwrap_or_else(PoisonError::into_inner).insert(connection, Open { frames, cancel: cancel.clone(), closing: closing.clone(), established: established.clone(), attempt });
        let (lines, codec, back) = (self.lines.clone(), self.codec.clone(), back.clone());
        let token = cancel.clone();
        let reply = move |input: FeedInput| {
            if !token.is_cancelled() {
                back.reply(stamp, input);
            }
        };
        spawn_on(Carry::Detached, &self.handle, async move {
            Self::run(lines, codec, connection, (cancel, closing, established), queued, reply).await;
            let mut open = open.lock().unwrap_or_else(PoisonError::into_inner);
            if open.get(&connection).is_some_and(|entry| entry.attempt == attempt) {
                open.remove(&connection);
            }
        });
    }

    async fn run(lines: Arc<dyn CableLines>, codec: Arc<dyn FeedCodec>, connection: ConnectionId, (cancel, closing, established): (CancelToken, CancelToken, Arc<AtomicBool>), mut queued: mpsc::UnboundedReceiver<(u64, Value)>, reply: impl Fn(FeedInput) + Clone + Send + 'static) {
        {
            let line = tokio::select! {
                () = cancel.cancelled() => return,
                line = lines.connect() => line,
            };
            let line = match line {
                Ok(line) => line,
                Err(TransportError::Session { .. }) => return reply(FeedInput::SubscriptionEnded { connection_id: connection }),
                Err(TransportError::Decode(_)) => return reply(FeedInput::ProtocolError { connection_id: connection, reason: ProtocolReason::Handshake }),
                Err(_) => return reply(FeedInput::Dropped { connection_id: connection }),
            };
            established.store(true, Ordering::SeqCst);
            reply(FeedInput::HandshakeDone { connection_id: connection, key_id: connection });
            let writer = {
                let (line, reply) = (line.clone(), reply.clone());
                async move {
                    while let Some((counter, body)) = queued.recv().await {
                        if crate::db::lock::effect(|| line.send(counter, body)).await.is_err() {
                            return reply(FeedInput::Dropped { connection_id: connection });
                        }
                    }
                }
            };
            let reader = Self::read(line.clone(), codec, connection, reply);
            tokio::select! {
                () = cancel.cancelled() => {}
                () = async { closing.cancelled().await; tokio::time::sleep(CLOSE_DRAIN).await } => {}
                () = writer => cancel.cancel(),
                () = reader => {}
            }
            line.close().await;
        }
    }

    async fn read(line: Arc<dyn CableLine>, codec: Arc<dyn FeedCodec>, connection: ConnectionId, reply: impl Fn(FeedInput)) {
        loop {
            match line.next().await {
                Incoming::Frame { counter, body } => {
                    let frame = match Self::frame_of(&codec, &body) {
                        Ok(frame) => frame,
                        Err(reason) => return reply(FeedInput::ProtocolError { connection_id: connection, reason }),
                    };
                    let last = matches!(&frame, Frame::Ended) || matches!(&frame, Frame::Complete { id } if id == SUBSCRIPTION) || matches!(&frame, Frame::Error { id } if id.as_deref().is_none_or(|id| id == SUBSCRIPTION));
                    reply(FeedInput::FrameOpened { connection_id: connection, counter, frame });
                    if last {
                        return;
                    }
                }
                Incoming::Closed { code, reason } => {
                    return reply(if session_closed(code, &reason) { FeedInput::SubscriptionEnded { connection_id: connection } } else { FeedInput::Dropped { connection_id: connection } });
                }
                Incoming::Unreadable(_) => return reply(FeedInput::ProtocolError { connection_id: connection, reason: ProtocolReason::Decrypt }),
            }
        }
    }

    /// The meaning of a frame the cable already validated. A `next` that is no response is a broken frame.
    fn frame_of(codec: &Arc<dyn FeedCodec>, body: &Value) -> Result<Frame, ProtocolReason> {
        let id = body["id"].as_str().map(str::to_string);
        let foreign = id.as_deref().is_some_and(|id| id != SUBSCRIPTION);
        Ok(match body["t"].as_str() {
            Some("ping") => Frame::Ping { ts: body["ts"].as_number().cloned().ok_or(ProtocolReason::Frame)? },
            Some("closing") => Frame::Ended,
            Some("error") => Frame::Error { id },
            Some("complete") => Frame::Complete { id: id.unwrap_or_default() },
            Some("next") if foreign => Frame::Events { id: id.unwrap_or_default(), events: Vec::new() },
            Some("next") => {
                let response = serde_json::from_value::<Response>(body["result"].clone()).map_err(|_| ProtocolReason::Frame)?;
                match response.shape() {
                    ResponseShape::Empty => return Err(ProtocolReason::Frame),
                    ResponseShape::ErrorsOnly | ResponseShape::DataWithErrors => return Err(ProtocolReason::Errors),
                    _ => {}
                }
                Frame::Events { id: id.unwrap_or_default(), events: codec.events(response) }
            }
            _ => Frame::Quiet,
        })
    }

    fn body_of(&self, frame: Outgoing) -> Value {
        match frame {
            Outgoing::Subscribe { models } => {
                let request = self.codec.request(&models);
                json!({"t":"subscribe", "id":SUBSCRIPTION, "query":request.document, "variables":request.variables, "operationName":request.operation_name})
            }
            Outgoing::Unsubscribe => json!({"t":"unsubscribe", "id":SUBSCRIPTION}),
            Outgoing::Pong { ts } => json!({"t":"pong", "ts":ts}),
        }
    }

    fn seal(&self, connection: ConnectionId, counter: u64, frame: Outgoing) {
        let body = self.body_of(frame);
        let frames = self.open.lock().unwrap_or_else(PoisonError::into_inner).get(&connection).map(|open| open.frames.clone());
        if let Some(frames) = frames {
            let _ = crate::db::lock::effect(|| frames.send((counter, body)));
        }
    }

    /// The frames sealed before the close still go out: the writer ends when its queue is empty and the sender is gone, then the line closes.
    fn close(&self, connection: ConnectionId) {
        let Some(open) = self.open.lock().unwrap_or_else(PoisonError::into_inner).remove(&connection) else { return };
        if open.established.load(Ordering::SeqCst) {
            open.closing.cancel();
        } else {
            open.cancel.cancel();
        }
    }
}

impl EffectRunner<Feed> for FeedIo {
    fn run(&mut self, stamp: EpochStamp, effect: FeedEffect, back: &Back<Feed>) {
        match effect {
            FeedEffect::Connect { connection, .. } => self.connect(stamp, connection, back),
            FeedEffect::Seal { connection, counter, frame, .. } => self.seal(connection, counter, frame),
            FeedEffect::Close { connection } => self.close(connection),
            other => (self.rest)(stamp, other),
        }
    }
}

impl Drop for FeedIo {
    fn drop(&mut self) {
        crate::db::lock::assert_no_library_lock("FeedIo drop");
        let open: Vec<Open> = self.open.lock().unwrap_or_else(PoisonError::into_inner).drain().map(|(_, open)| open).collect();
        open.iter().for_each(|open| open.cancel.cancel());
    }
}
