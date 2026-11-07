use std::collections::HashMap;

use tokio::sync::oneshot;

use super::owner::{Cause, Epoch};
use super::sync::{lock, Arc, Mutex, MutexGuard};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct OutcomeId(u64);

impl OutcomeId {
    pub fn raw(self) -> u64 { self.0 }

    pub fn from_raw(raw: u64) -> Self { Self(raw) }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome<T> {
    Value(T),
    EpochEnded(Cause),
    Closing,
    Internal,
}

struct Inner<T> {
    next: u64,
    closed: bool,
    last_ended: Option<(Epoch, Cause)>,
    open: HashMap<u64, (Epoch, oneshot::Sender<Outcome<T>>)>,
}

pub struct Outcomes<T> {
    inner: Arc<Mutex<Inner<T>>>,
}

impl<T> Clone for Outcomes<T> {
    fn clone(&self) -> Self { Self { inner: self.inner.clone() } }
}

impl<T> Default for Outcomes<T> {
    fn default() -> Self { Self::new() }
}

pub struct Waiter<T> {
    id: OutcomeId,
    receiver: oneshot::Receiver<Outcome<T>>,
    taken: bool,
}

impl<T> Outcomes<T> {
    pub fn new() -> Self { Self { inner: Arc::new(Mutex::new(Inner { next: 0, closed: false, last_ended: None, open: HashMap::new() })) } }

    /// The calls that wait for an answer now.
    #[cfg(test)]
    pub fn waiting(&self) -> usize { self.lock().open.len() }

    fn lock(&self) -> MutexGuard<'_, Inner<T>> { lock(&self.inner) }

    pub fn register(&self, epoch: Epoch) -> Waiter<T> { self.register_with(None, epoch) }

    /// A waiter under the id of the caller: the id names the request the answer belongs to.
    pub fn register_as(&self, id: u64, epoch: Epoch) -> Waiter<T> { self.register_with(Some(id), epoch) }

    pub fn forget(&self, id: OutcomeId) {
        let removed = self.lock().open.remove(&id.0);
        crate::lock::effect(|| drop(removed));
    }

    fn register_with(&self, id: Option<u64>, epoch: Epoch) -> Waiter<T> {
        let mut inner = self.lock();
        let id = id.unwrap_or_else(|| {
            inner.next += 1;
            inner.next - 1
        });
        let (sender, receiver) = oneshot::channel();
        let terminal = if inner.closed {
            Some(Outcome::Closing)
        } else {
            inner.last_ended.filter(|(ended, _)| epoch <= *ended).map(|(_, cause)| Outcome::EpochEnded(cause))
        };
        match terminal {
            Some(outcome) => {
                drop(inner);
                let _ = crate::lock::effect(|| sender.send(outcome));
            }
            None => {
                inner.open.insert(id, (epoch, sender));
            }
        }
        Waiter { id: OutcomeId(id), receiver, taken: false }
    }

    pub fn settle(&self, id: OutcomeId, value: T) -> bool {
        let Some((_, sender)) = self.lock().open.remove(&id.0) else { return false };
        crate::lock::effect(|| sender.send(Outcome::Value(value)).is_ok())
    }

    pub fn end_epoch(&self, epoch: Epoch, cause: Cause) -> usize {
        let mut inner = self.lock();
        if inner.last_ended.is_none_or(|(ended, _)| ended <= epoch) {
            inner.last_ended = Some((epoch, cause));
        }
        let ids: Vec<u64> = inner.open.iter().filter(|(_, (open_epoch, _))| *open_epoch <= epoch).map(|(id, _)| *id).collect();
        let senders: Vec<_> = ids.iter().filter_map(|id| inner.open.remove(id)).map(|(_, sender)| sender).collect();
        drop(inner);
        for sender in senders {
            let _ = crate::lock::effect(|| sender.send(Outcome::EpochEnded(cause)));
        }
        ids.len()
    }

    pub fn close(&self) -> usize {
        let mut inner = self.lock();
        inner.closed = true;
        let open: Vec<_> = inner.open.drain().collect();
        drop(inner);
        let count = open.len();
        for (_, (_, sender)) in open {
            let _ = crate::lock::effect(|| sender.send(Outcome::Closing));
        }
        count
    }

    pub fn open_count(&self) -> usize { self.lock().open.len() }
}

impl<T> Waiter<T> {
    pub fn id(&self) -> OutcomeId { self.id }

    pub fn try_take(&mut self) -> Option<Outcome<T>> {
        if self.taken {
            return None;
        }
        let outcome = match self.receiver.try_recv() {
            Ok(outcome) => outcome,
            Err(oneshot::error::TryRecvError::Empty) => return None,
            Err(oneshot::error::TryRecvError::Closed) => Outcome::Internal,
        };
        self.taken = true;
        Some(outcome)
    }

    pub async fn wait(self) -> Outcome<T> { self.receiver.await.unwrap_or(Outcome::Internal) }
}
