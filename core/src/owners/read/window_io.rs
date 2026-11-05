//! Runner of the effects of `Windows`: the request of a page goes through the `Transport` of the core, the answer comes back as
//! `WindowsInput::Transport`; the answer batch goes to the owner `StoreWriter`. A cancelled request gives no answer.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, PoisonError};

use tokio::runtime::Handle;

use super::ports::{epoch_of, Admit, Rest, StorePort};
use crate::owners::contract::{ErrorClass, RequestError, RequestId, Transport as Answer, TransportOutcome};
use crate::owners::store::StoreBatch;
use crate::owners::windows::{AnswerPage, WindowEffect, WindowKey, Windows, WindowsEffect, WindowsInput};
use crate::runtime::{Back, EffectRunner, EpochStamp};
use crate::task::{spawn_on, Carry};
use crate::transport::{CancelToken, GraphQLRequest, Response, RetryClass, Transport, TransportError};

/// The wire form of a window: how a page is asked and how its answer reads.
pub trait WindowCodec: Send + Sync + 'static {
    fn request(&self, key: &WindowKey, page: u32, cursor: Option<&str>) -> GraphQLRequest;
    fn answer(&self, key: &WindowKey, page: u32, response: Response) -> Result<AnswerPage, String>;
}

type Open = Arc<Mutex<HashMap<RequestId, CancelToken>>>;

pub struct WindowIo {
    transport: Arc<dyn Transport>,
    codec: Arc<dyn WindowCodec>,
    admit: Admit,
    writer: Arc<dyn StorePort>,
    handle: Handle,
    open: Open,
    rest: Rest<WindowsEffect>,
}

impl WindowIo {
    pub fn new(transport: Arc<dyn Transport>, codec: Arc<dyn WindowCodec>, admit: Admit, writer: Arc<dyn StorePort>, handle: Handle, rest: Rest<WindowsEffect>) -> Self {
        Self { transport, codec, admit, writer, handle, open: Open::default(), rest }
    }

    fn request(&self, stamp: EpochStamp, key: WindowKey, request: RequestId, page: u32, cursor: Option<String>, back: &Back<Windows>) {
        let token = CancelToken::new();
        self.open.lock().unwrap_or_else(PoisonError::into_inner).insert(request, token.clone());
        let (transport, codec, admit, open, back) = (self.transport.clone(), self.codec.clone(), self.admit.clone(), self.open.clone(), back.clone());
        spawn_on(Carry::Detached, &self.handle, async move {
            let outcome = tokio::select! {
                () = token.cancelled() => None,
                outcome = ask(transport.as_ref(), codec.as_ref(), &admit, &key, page, cursor.as_deref()) => Some(outcome),
            };
            open.lock().unwrap_or_else(PoisonError::into_inner).remove(&request);
            if let Some(outcome) = outcome.filter(|_| !token.is_cancelled()) {
                back.reply(stamp, WindowsInput::Transport(Answer { req_id: request, outcome }));
            }
        });
    }

    fn cancel(&self, request: RequestId) {
        let token = self.open.lock().unwrap_or_else(PoisonError::into_inner).remove(&request);
        if let Some(token) = token {
            token.cancel();
        }
    }

    fn commit(&self, stamp: EpochStamp, key: &WindowKey, request: RequestId, basis: u64, page: AnswerPage) {
        let rows = page.rows.into_iter().map(|row| (key.model.clone(), row)).collect();
        let batch = StoreBatch { id: request, epoch: epoch_of(stamp), basis: BTreeMap::from([(key.model.clone(), basis)]), rows, intents: 0, credential: None };
        self.writer.commit(stamp, batch);
    }
}

impl EffectRunner<Windows> for WindowIo {
    fn run(&mut self, stamp: EpochStamp, effect: WindowsEffect, back: &Back<Windows>) {
        match effect {
            WindowsEffect::Window { key, effect: WindowEffect::Request { request, page, cursor, .. } } => self.request(stamp, key, request, page, cursor, back),
            WindowsEffect::Window { effect: WindowEffect::Cancel { request }, .. } => self.cancel(request),
            WindowsEffect::Window { key, effect: WindowEffect::Commit { request, basis, page } } => self.commit(stamp, &key, request, basis, page),
            other => (self.rest)(stamp, other),
        }
    }
}

async fn ask(transport: &dyn Transport, codec: &dyn WindowCodec, admit: &Admit, key: &WindowKey, page: u32, cursor: Option<&str>) -> TransportOutcome<AnswerPage> {
    let Some(admitted) = admit() else { return TransportOutcome::Dropped };
    match transport.query(&admitted, codec.request(key, page, cursor)).await {
        Ok(response) => match codec.answer(key, page, response) {
            Ok(answer) => TransportOutcome::Response(answer),
            Err(message) => TransportOutcome::Error(RequestError { message, class: ErrorClass::Permanent }),
        },
        Err(error) => failure(&error),
    }
}

/// `Interrupted` = the request left and no answer came; every other error has the class of its retry policy.
pub(super) fn failure<R>(error: &TransportError) -> TransportOutcome<R> {
    let class = match RetryClass::of_error(error) {
        _ if matches!(error, TransportError::Interrupted(_)) => return TransportOutcome::Dropped,
        RetryClass::Network => ErrorClass::Network,
        RetryClass::Server | RetryClass::Retriable => ErrorClass::Server,
        RetryClass::Fatal => ErrorClass::Permanent,
    };
    TransportOutcome::Error(RequestError { message: error.to_string(), class })
}

impl Drop for WindowIo {
    fn drop(&mut self) { crate::db::lock::assert_no_library_lock("WindowIo drop"); }
}
