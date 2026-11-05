use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use tokio::sync::oneshot;

use super::owner::{Cause, Epoch};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct OutcomeId(u64);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome<T> {
    Value(T),
    EpochEnded(Cause),
    Closing,
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

    fn lock(&self) -> MutexGuard<'_, Inner<T>> { self.inner.lock().unwrap_or_else(PoisonError::into_inner) }

    pub fn register(&self, epoch: Epoch) -> Waiter<T> {
        let mut inner = self.lock();
        let id = inner.next;
        inner.next += 1;
        let (sender, receiver) = oneshot::channel();
        let terminal = if inner.closed {
            Some(Outcome::Closing)
        } else {
            inner.last_ended.filter(|(ended, _)| epoch <= *ended).map(|(_, cause)| Outcome::EpochEnded(cause))
        };
        match terminal {
            Some(outcome) => {
                let _ = sender.send(outcome);
            }
            None => {
                inner.open.insert(id, (epoch, sender));
            }
        }
        Waiter { id: OutcomeId(id), receiver, taken: false }
    }

    pub fn settle(&self, id: OutcomeId, value: T) -> bool {
        let Some((_, sender)) = self.lock().open.remove(&id.0) else { return false };
        sender.send(Outcome::Value(value)).is_ok()
    }

    pub fn end_epoch(&self, epoch: Epoch, cause: Cause) -> usize {
        let mut inner = self.lock();
        if inner.last_ended.is_none_or(|(ended, _)| ended <= epoch) {
            inner.last_ended = Some((epoch, cause));
        }
        let ids: Vec<u64> = inner.open.iter().filter(|(_, (open_epoch, _))| *open_epoch <= epoch).map(|(id, _)| *id).collect();
        for id in &ids {
            if let Some((_, sender)) = inner.open.remove(id) {
                let _ = sender.send(Outcome::EpochEnded(cause));
            }
        }
        ids.len()
    }

    pub fn close(&self) -> usize {
        let mut inner = self.lock();
        inner.closed = true;
        let open: Vec<_> = inner.open.drain().collect();
        let count = open.len();
        for (_, (_, sender)) in open {
            let _ = sender.send(Outcome::Closing);
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
            Err(oneshot::error::TryRecvError::Closed) => Outcome::Closing,
        };
        self.taken = true;
        Some(outcome)
    }

    pub async fn wait(self) -> Outcome<T> { self.receiver.await.unwrap_or(Outcome::Closing) }
}
