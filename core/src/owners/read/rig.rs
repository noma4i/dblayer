//! Helpers of the runner tests: a Tokio runtime, a Store peer that keeps batches, a pump for the scheduler.
//! `DetScheduler` takes the inputs that other threads reply only after a step, so the pump posts a `Tick` between checks.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use super::Admit;
use crate::owners::store::{StoreBatch, StoreInput};
use crate::owners::support::NoEffect;
use crate::runtime::{Address, Cause, DetScheduler, Effects, Epoch, EpochStamp, StateOwner, Time};

pub const LIMIT: Duration = Duration::from_secs(10);

pub fn runtime() -> tokio::runtime::Runtime { tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().expect("a runtime") }

pub fn admit() -> Admit { Arc::new(|| crate::transport::test_admission().map_err(|error| super::Refused(format!("{error:?}")))) }

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
    (seen, Box::new(move |_, effect| sink.lock().unwrap_or_else(PoisonError::into_inner).push(effect)))
}

/// Steps the scheduler until `done` holds. Fails after `LIMIT`.
pub fn pump<O: StateOwner>(det: &mut DetScheduler, owner: Address<O>, mut done: impl FnMut(&DetScheduler) -> bool) {
    let deadline = Instant::now() + LIMIT;
    loop {
        det.run_until_quiet(10_000).expect("the scheduler goes quiet");
        if done(det) {
            return;
        }
        assert!(Instant::now() < deadline, "the condition did not hold in time, trace: {:?}", det.trace());
        det.post(owner, EpochStamp::Control, O::tick(Time(0)));
        std::thread::sleep(Duration::from_millis(2));
    }
}

/// Steps the scheduler for `span`: the proof that something does not happen.
pub fn pump_for<O: StateOwner>(det: &mut DetScheduler, owner: Address<O>, span: Duration) {
    let end = Instant::now() + span;
    pump(det, owner, |_| Instant::now() >= end);
}

pub fn saw_input(det: &DetScheduler, prefix: &str) -> usize { det.trace().iter().filter(|entry| entry.input.starts_with(prefix)).count() }
