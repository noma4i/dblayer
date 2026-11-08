//! Helpers of the runner tests: a Tokio runtime, a Store peer that keeps batches, a pump for the scheduler.
//! `DetScheduler` takes the replies of other threads at its next step, so the pump waits for the signal of the next reply.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use super::Admit;
use crate::owners::store::{StoreBatch, StoreInput};
use crate::spec::support::NoEffect;
use crate::runtime::{Cause, DetScheduler, Effects, Epoch, StateOwner, Time};

pub const LIMIT: Duration = Duration::from_secs(10);

pub fn runtime() -> tokio::runtime::Runtime { tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().expect("a runtime") }

pub fn admit() -> Admit { Arc::new(|_| Ok(())) }

pub fn open_gate(epoch: u64) -> Arc<crate::runtime::EpochGate> {
    let gate = Arc::new(crate::runtime::EpochGate::default());
    gate.open(epoch);
    gate
}

pub fn open_gate_over(epoch: u64, transport: Arc<dyn crate::transport::Upstream>) -> Arc<crate::runtime::EpochGate> {
    let gate = open_gate(epoch);
    gate.attach(transport);
    gate
}

/// A Store peer: it keeps the batches of `Commit`.
#[derive(Clone, Default)]
pub struct Batches(pub Arc<Mutex<Vec<StoreBatch>>>);

impl Batches {
    pub fn all(&self) -> Vec<StoreBatch> { self.0.lock().unwrap_or_else(PoisonError::into_inner).clone() }
}

pub(crate) struct BatchPeer;

impl StateOwner for BatchPeer {
    const EPOCHED: bool = false;

    type State = Batches;
    type Input = StoreInput;
    type Effect = NoEffect;

    fn step(state: &mut Batches, input: StoreInput) -> Effects<Self> {
        if let StoreInput::Commit(batch) = input {
            state.0.lock().unwrap_or_else(PoisonError::into_inner).push(batch);
        }
        Vec::new()
    }

    fn tick(_now: Time) -> StoreInput { StoreInput::Idle }

    fn epoch_ended(_epoch: Epoch, _cause: Cause) -> StoreInput { StoreInput::Idle }

    fn closing() -> StoreInput { StoreInput::Idle }
}

pub fn collector<E: Send + 'static>() -> (Arc<Mutex<Vec<E>>>, super::Rest<E>) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = seen.clone();
    (seen, Box::new(move |_, effect| { sink.lock().unwrap_or_else(PoisonError::into_inner).push(effect); crate::runtime::wake_pumps(); }))
}

/// Steps the scheduler until `done` holds, then waits for the next reply of another thread. Fails after `LIMIT`.
pub fn pump(det: &mut DetScheduler, mut done: impl FnMut(&DetScheduler) -> bool) {
    let deadline = Instant::now() + LIMIT;
    loop {
        let seen = det.arrivals();
        det.run_until_quiet(10_000).expect("the scheduler goes quiet");
        if done(det) {
            return;
        }
        let left = deadline.saturating_duration_since(Instant::now());
        assert!(det.wait_arrival(seen, left), "the condition did not hold in time, trace: {:?}", det.trace());
    }
}

pub fn saw_input(det: &DetScheduler, prefix: &str) -> usize { det.trace().iter().filter(|entry| entry.input.starts_with(prefix)).count() }
