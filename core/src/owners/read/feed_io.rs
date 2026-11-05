//! Runner of the effects of `Feed`: the subscription of the core transport is IO without transitions. Every frame becomes
//! `FeedInput::Frame` of its connection; a closed connection gives no more frames.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use tokio::runtime::Handle;

use super::ports::{Admit, Rest};
use crate::owners::contract::{ConnectionId, Frame, SubscriptionFrame};
use crate::owners::read::feed::{Feed, FeedEffect, FeedInput};
use crate::runtime::{Back, EffectRunner, EpochStamp};
use crate::task::{spawn_on, Carry};
use crate::transport::{CancelToken, GraphQLRequest, Response, SubscriptionEvent, Transport};

/// The wire form of the subscription: what it asks and what an event of a model reads as.
pub trait FeedCodec: Send + Sync + 'static {
    fn request(&self, models: &[String]) -> GraphQLRequest;
    /// `(model, payload)` of a response, `None` = the response is no model event.
    fn event(&self, response: Response) -> Option<(String, String)>;
}

type Open = Arc<Mutex<HashMap<ConnectionId, CancelToken>>>;

pub struct FeedIo {
    transport: Arc<dyn Transport>,
    codec: Arc<dyn FeedCodec>,
    admit: Admit,
    handle: Handle,
    open: Open,
    rest: Rest<FeedEffect>,
}

impl FeedIo {
    pub fn new(transport: Arc<dyn Transport>, codec: Arc<dyn FeedCodec>, admit: Admit, handle: Handle, rest: Rest<FeedEffect>) -> Self {
        Self { transport, codec, admit, handle, open: Open::default(), rest }
    }

    fn subscribe(&self, stamp: EpochStamp, connection: ConnectionId, models: Vec<String>, back: &Back<Feed>) {
        let cancel = CancelToken::new();
        self.open.lock().unwrap_or_else(PoisonError::into_inner).insert(connection, cancel.clone());
        let token = cancel.clone();
        let (transport, codec, admit, back) = (self.transport.clone(), self.codec.clone(), self.admit.clone(), back.clone());
        let send = move |frame: Frame| {
            if !token.is_cancelled() {
                back.reply(stamp, FeedInput::Frame(SubscriptionFrame { connection_id: connection, frame }));
            }
        };
        spawn_on(Carry::Detached, &self.handle, async move {
            let Some(admitted) = admit() else { return send(Frame::Dropped) };
            let mut subscription = match transport.subscribe(&admitted, codec.request(&models)).await {
                Ok(subscription) => subscription,
                Err(_) => return send(Frame::Dropped),
            };
            loop {
                let event = tokio::select! {
                    () = cancel.cancelled() => return,
                    event = subscription.next() => event,
                };
                match event {
                    Some(SubscriptionEvent::Ready) => send(Frame::Ready),
                    Some(SubscriptionEvent::Next(response)) => {
                        if let Some((model, payload)) = codec.event(response) {
                            send(Frame::Event { model, payload });
                        }
                    }
                    Some(SubscriptionEvent::Lost { first, last }) => send(Frame::ChannelLost { dropped: last - first + 1, models: models.clone() }),
                    Some(SubscriptionEvent::Failed(failure)) => {
                        let ended = failure.session_refused() || failure.close.as_ref().is_some_and(|close| close.terminal);
                        return send(if ended { Frame::Ended } else { Frame::Dropped });
                    }
                    None => return send(Frame::Dropped),
                }
            }
        });
    }

    fn close(&self, connection: ConnectionId) {
        let token = self.open.lock().unwrap_or_else(PoisonError::into_inner).remove(&connection);
        if let Some(token) = token {
            token.cancel();
        }
    }

    fn close_all(&self) {
        let tokens: Vec<CancelToken> = self.open.lock().unwrap_or_else(PoisonError::into_inner).drain().map(|(_, token)| token).collect();
        tokens.iter().for_each(CancelToken::cancel);
    }
}

impl EffectRunner<Feed> for FeedIo {
    fn run(&mut self, stamp: EpochStamp, effect: FeedEffect, back: &Back<Feed>) {
        match effect {
            FeedEffect::Subscribe { connection, models } => self.subscribe(stamp, connection, models, back),
            FeedEffect::Unsubscribe => self.close_all(),
            FeedEffect::CloseConnection { connection } => self.close(connection),
            other => (self.rest)(stamp, other),
        }
    }
}

impl Drop for FeedIo {
    fn drop(&mut self) { crate::db::lock::assert_no_library_lock("FeedIo drop"); }
}
