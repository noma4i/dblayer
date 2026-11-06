use std::collections::{BTreeSet, VecDeque};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ReadEvent {
    Closed(Vec<u64>),
    Finished(u64),
}

type Sink = Box<dyn Fn(ReadEvent) + Send + Sync>;

#[derive(Default)]
struct State {
    next: u64,
    open: BTreeSet<u64>,
    closed: bool,
    named: BTreeSet<u64>,
    queue: VecDeque<ReadEvent>,
    delivering: bool,
}

#[derive(Default)]
pub(crate) struct Reads {
    state: Mutex<State>,
    sink: OnceLock<Sink>,
}

pub(crate) struct ReadGuard {
    reads: Arc<Reads>,
    id: u64,
}

impl Reads {
    pub fn new() -> Arc<Self> { Arc::new(Self::default()) }

    pub fn attach(&self, sink: impl Fn(ReadEvent) + Send + Sync + 'static) { let _ = self.sink.set(Box::new(sink)); }

    pub fn admit(self: &Arc<Self>) -> Option<ReadGuard> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.closed {
            return None;
        }
        state.next += 1;
        let id = state.next;
        state.open.insert(id);
        Some(ReadGuard { reads: self.clone(), id })
    }

    pub fn close(&self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.closed {
            return;
        }
        state.closed = true;
        state.named = state.open.clone();
        let ids = state.open.iter().copied().collect();
        state.queue.push_back(ReadEvent::Closed(ids));
        drop(state);
        self.deliver();
    }

    fn finish(&self, id: u64) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.open.remove(&id);
        if state.named.remove(&id) {
            state.queue.push_back(ReadEvent::Finished(id));
        }
        drop(state);
        self.deliver();
    }

    /// One caller at a time takes the events out of the queue and hands them over outside the lock, so the order of the queue is the order of delivery.
    fn deliver(&self) {
        let Some(sink) = self.sink.get() else { return };
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.delivering {
            return;
        }
        state.delivering = true;
        loop {
            let Some(event) = state.queue.pop_front() else {
                state.delivering = false;
                return;
            };
            drop(state);
            crate::lock::effect(|| sink(event));
            state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        }
    }
}

impl Drop for ReadGuard {
    fn drop(&mut self) { self.reads.finish(self.id); }
}
