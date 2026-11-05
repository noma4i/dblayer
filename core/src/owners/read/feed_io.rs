//! Runner of the effects of `Feed`: IO of the cable connections without transitions. It opens a connection and runs its handshake (`HandshakeDone`),
//! decrypts every frame of the server and reads it by the codec (`FrameOpened`), encrypts the frames of `Seal` with the key of the connection and the
//! counter the effect names. The counters and the protocol transitions belong to `Feed`. A closed connection gives no more inputs.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use serde_json::{json, Value};
use tokio::runtime::Handle;
use tokio::sync::mpsc;

use super::feed::{FramedEvent, Outgoing, ProtocolReason};
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

const SUBSCRIPTION: &str = "1";

struct Open {
    frames: mpsc::UnboundedSender<(u64, Value)>,
    cancel: CancelToken,
}

type Connections = Arc<Mutex<HashMap<ConnectionId, Open>>>;

pub struct FeedIo {
    lines: Arc<dyn CableLines>,
    codec: Arc<dyn FeedCodec>,
    handle: Handle,
    open: Connections,
    rest: Rest<FeedEffect>,
}

/// The codes of a closing server that no retry changes: the session is over.
fn session_closed(code: u16, reason: &str) -> bool { matches!(code, 4401 | 4403) || matches!(reason, "LOGGED_OUT" | "ACCOUNT_LOCKED") }

impl FeedIo {
    pub fn new(lines: Arc<dyn CableLines>, codec: Arc<dyn FeedCodec>, handle: Handle, rest: Rest<FeedEffect>) -> Self { Self { lines, codec, handle, open: Connections::default(), rest } }

    fn connect(&self, stamp: EpochStamp, connection: ConnectionId, back: &Back<Feed>) {
        let cancel = CancelToken::new();
        let (frames, mut queued) = mpsc::unbounded_channel::<(u64, Value)>();
        self.open.lock().unwrap_or_else(PoisonError::into_inner).insert(connection, Open { frames, cancel: cancel.clone() });
        let (lines, codec, back) = (self.lines.clone(), self.codec.clone(), back.clone());
        let token = cancel.clone();
        let reply = move |input: FeedInput| {
            if !token.is_cancelled() {
                back.reply(stamp, input);
            }
        };
        spawn_on(Carry::Detached, &self.handle, async move {
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
                () = writer => cancel.cancel(),
                () = reader => {}
            }
            line.close().await;
        });
    }

    async fn read(line: Arc<dyn CableLine>, codec: Arc<dyn FeedCodec>, connection: ConnectionId, reply: impl Fn(FeedInput)) {
        loop {
            match line.next().await {
                Incoming::Frame { counter, body } => match body["t"].as_str() {
                    Some("closing") => {
                        let (code, reason) = (body["code"].as_u64().unwrap_or(0) as u16, body["reason"].as_str().unwrap_or(""));
                        return reply(if session_closed(code, reason) { FeedInput::SubscriptionEnded { connection_id: connection } } else { FeedInput::Dropped { connection_id: connection } });
                    }
                    Some("complete") => return reply(FeedInput::SubscriptionEnded { connection_id: connection }),
                    Some("error") => {
                        let retryable = body["retryable"] == true;
                        return reply(if retryable { FeedInput::Dropped { connection_id: connection } } else { FeedInput::SubscriptionEnded { connection_id: connection } });
                    }
                    Some("next") => {
                        let events = serde_json::from_value::<Response>(body["result"].clone()).ok().filter(|response| response.shape() != ResponseShape::Empty).map(|response| codec.events(response)).unwrap_or_default();
                        reply(FeedInput::FrameOpened { connection_id: connection, counter, events });
                    }
                    _ => reply(FeedInput::FrameOpened { connection_id: connection, counter, events: Vec::new() }),
                },
                Incoming::Closed { code, reason } => {
                    return reply(if session_closed(code, &reason) { FeedInput::SubscriptionEnded { connection_id: connection } } else { FeedInput::Dropped { connection_id: connection } });
                }
                Incoming::Unreadable(_) => return reply(FeedInput::ProtocolError { connection_id: connection, reason: ProtocolReason::Decrypt }),
            }
        }
    }

    fn body_of(&self, frame: Outgoing) -> Value {
        match frame {
            Outgoing::Subscribe { models } => {
                let request = self.codec.request(&models);
                json!({"t":"subscribe", "id":SUBSCRIPTION, "query":request.document, "variables":request.variables, "operationName":request.operation_name})
            }
            Outgoing::Unsubscribe => json!({"t":"unsubscribe", "id":SUBSCRIPTION}),
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
    fn close(&self, connection: ConnectionId) { self.open.lock().unwrap_or_else(PoisonError::into_inner).remove(&connection); }
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
