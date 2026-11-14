//! Stubs for the owner-level tests: an effect log and a scripted source of inputs.

use std::any::Any;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use crate::owners::contract::{BatchId, Committed, RejectReason};
use crate::owners::store::{StoreBatch, StoreInput};
use crate::runtime::{Address, Back, Cause, Control, Effect, EffectRunner, Effects, Epoch, EpochStamp, Outbound, ScheduleId, StateOwner};

pub(crate) type Log<E> = Arc<Mutex<Vec<E>>>;

pub(crate) fn new_log<E>() -> Log<E> { Arc::new(Mutex::new(Vec::new())) }

pub(crate) fn take<E>(log: &Log<E>) -> Vec<E> { std::mem::take(&mut *log.lock().unwrap_or_else(PoisonError::into_inner)) }

pub(crate) fn snapshot<E: Clone>(log: &Log<E>) -> Vec<E> { log.lock().unwrap_or_else(PoisonError::into_inner).clone() }

/// The reply of the Store to the author of `batch`: no rows, no models, nothing shown or refused.
pub(crate) fn committed_at(batch: BatchId, revision: i64, tables: &[&str]) -> Committed {
    Committed { batch, revision, tables: tables.iter().map(|table| (*table).to_string()).collect(), rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0, refused: Vec::new() }
}

/// The typed batches behind the `Commit` sends of `emitted`: the last ones the captured Store received, in the order of the sends.
pub(crate) fn sent_batches<E>(emitted: &[Emitted<E>], stored: &Log<StoreBatch>) -> Vec<StoreBatch> {
    let sent = emitted.iter().filter(|entry| matches!(entry, Emitted::Send { input, .. } if input.starts_with("Commit("))).count();
    let all = snapshot(stored);
    all[all.len() - sent..].to_vec()
}

/// Runs no effect: it keeps them in the log.
pub(crate) struct Recorder<O: StateOwner> {
    pub log: Log<O::Effect>,
}

impl<O: StateOwner> Recorder<O> {
    pub fn new(log: &Log<O::Effect>) -> Self { Self { log: log.clone() } }
}

impl<O: StateOwner> EffectRunner<O> for Recorder<O> {
    fn run(&mut self, _stamp: EpochStamp, effect: O::Effect, _back: &Back<O>) { self.log.lock().unwrap_or_else(PoisonError::into_inner).push(effect); }
}

/// A source of inputs: on `Go` it sends its script to `to`, in order.
pub(crate) struct Source<O: StateOwner>(PhantomData<fn() -> O>);

#[derive(Clone, Debug)]
pub(crate) enum SourceInput {
    Go,
    Idle,
}

pub(crate) struct SourceState<O: StateOwner> {
    pub to: Address<O>,
    pub script: Vec<O::Input>,
}

#[derive(Debug)]
pub(crate) enum NoEffect {}

impl<O: StateOwner> StateOwner for Source<O>
where
    O::Input: Clone,
{
    const EPOCHED: bool = false;

    type State = SourceState<O>;
    type Input = SourceInput;
    type Effect = NoEffect;

    fn step(state: &mut SourceState<O>, input: SourceInput, _now: crate::runtime::Time) -> Effects<Self> {
        match input {
            SourceInput::Go => state.script.iter().map(|item| Effect::Send(Outbound::new(state.to, item.clone()))).collect(),
            SourceInput::Idle => Vec::new(),
        }
    }

    fn epoch_ended(_epoch: Epoch, _cause: Cause) -> SourceInput { SourceInput::Idle }

    fn closing() -> SourceInput { SourceInput::Idle }
}

pub(crate) struct Silent;

impl<O: StateOwner<Effect = NoEffect>> EffectRunner<O> for Silent {
    fn run(&mut self, _stamp: EpochStamp, effect: NoEffect, _back: &Back<O>) { match effect {} }
}

/// Lifecycle stub that ends the epoch: on `Go` it broadcasts `EpochEnded(epoch, cause)`.
pub(crate) struct Ender;

pub(crate) struct EnderState {
    pub epoch: Epoch,
    pub cause: Cause,
}

impl StateOwner for Ender {
    const EPOCHED: bool = false;

    type State = EnderState;
    type Input = SourceInput;
    type Effect = NoEffect;

    fn step(state: &mut EnderState, input: SourceInput, _now: crate::runtime::Time) -> Effects<Self> {
        match input {
            SourceInput::Go => vec![Effect::Broadcast(crate::runtime::Control::EpochEnded(state.epoch, state.cause))],
            SourceInput::Idle => Vec::new(),
        }
    }

    fn epoch_ended(_epoch: Epoch, _cause: Cause) -> SourceInput { SourceInput::Idle }

    fn closing() -> SourceInput { SourceInput::Idle }
}

/// What an owner gave out in a step, in the order of the effects. `Send` and `Schedule` keep the `Debug` form of their input.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Emitted<E> {
    Run(E),
    Send { to: usize, input: String },
    Schedule { id: ScheduleId, after: Duration, input: String },
    Cancel(ScheduleId),
    Broadcast(Control),
}

/// The owner `O` with a log of everything its steps give out. The inputs and the effects are the ones of `O`, so the address of a spy
/// takes the inputs of `O` and `Address::new(spy.index())` is the address of `O` for the owners that send to it.
pub(crate) struct Spy<O: StateOwner>(PhantomData<fn() -> O>);

pub(crate) struct SpyState<O: StateOwner> {
    pub inner: O::State,
    pub seen: Log<Emitted<O::Effect>>,
    pub store_batches: Log<StoreBatch>,
}

impl<O: StateOwner> StateOwner for Spy<O>
where
    O::Effect: Clone,
{
    const EPOCHED: bool = O::EPOCHED;

    type State = SpyState<O>;
    type Input = O::Input;
    type Effect = O::Effect;

    fn step(state: &mut SpyState<O>, input: O::Input, now: crate::runtime::Time) -> Effects<Self> {
        let effects = O::step(&mut state.inner, input, now);
        let mut seen = state.seen.lock().unwrap_or_else(PoisonError::into_inner);
        effects
            .into_iter()
            .map(|effect| match effect {
                Effect::Run(run) => {
                    seen.push(Emitted::Run(run.clone()));
                    Effect::Run(run)
                }
                Effect::Send(outbound) => {
                    if let Some(StoreInput::Commit(batch)) = (&*outbound.payload as &dyn Any).downcast_ref::<StoreInput>() {
                        state.store_batches.lock().unwrap_or_else(PoisonError::into_inner).push(batch.clone());
                    }
                    seen.push(Emitted::Send { to: outbound.to, input: format!("{:?}", outbound.payload) });
                    Effect::Send(outbound)
                }
                Effect::Schedule { id, after, input } => {
                    seen.push(Emitted::Schedule { id, after, input: format!("{input:?}") });
                    Effect::Schedule { id, after, input }
                }
                Effect::ScheduleAt { id, at, input } => {
                    seen.push(Emitted::Schedule { id, after: std::time::Duration::from_millis(at.0), input: format!("{input:?}") });
                    Effect::ScheduleAt { id, at, input }
                }
                Effect::Backoff { id, policy, attempt, input } => {
                    seen.push(Emitted::Schedule { id, after: policy.delay_seeded(attempt, 0), input: format!("{input:?}") });
                    Effect::Backoff { id, policy, attempt, input }
                }
                Effect::Cancel(id) => {
                    seen.push(Emitted::Cancel(id));
                    Effect::Cancel(id)
                }
                Effect::Broadcast(control) => {
                    seen.push(Emitted::Broadcast(control));
                    Effect::Broadcast(control)
                }
            })
            .collect()
    }


    fn epoch_started(epoch: Epoch) -> Option<O::Input> { O::epoch_started(epoch) }

    fn epoch_ended(epoch: Epoch, cause: Cause) -> O::Input { O::epoch_ended(epoch, cause) }

    fn closing() -> O::Input { O::closing() }
}

impl<O: StateOwner> SpyState<O> {
    pub fn new(inner: O::State, seen: &Log<Emitted<O::Effect>>) -> Self { Self { inner, seen: seen.clone(), store_batches: new_log() } }
}

/// The address of `O` for the slot of a `Spy<O>`.
pub(crate) fn as_owner<O: StateOwner>(spy: Address<Spy<O>>) -> Address<O>
where
    O::Effect: Clone,
{
    Address::new(spy.index())
}

/// Runs no effect and drops it: the owner is watched through its `Spy` log.
pub(crate) struct Discard;

impl<O: StateOwner> EffectRunner<O> for Discard {
    fn run(&mut self, _stamp: EpochStamp, _effect: O::Effect, _back: &Back<O>) {}
}

/// A peer that takes the inputs of `O` and keeps their `Debug` form; it sends nothing.
pub(crate) struct Sink<O: StateOwner>(PhantomData<fn() -> O>);

impl<O: StateOwner> StateOwner for Sink<O> {
    const EPOCHED: bool = false;

    type State = Log<String>;
    type Input = O::Input;
    type Effect = NoEffect;

    fn step(state: &mut Log<String>, input: O::Input, _now: crate::runtime::Time) -> Effects<Self> {
        state.lock().unwrap_or_else(PoisonError::into_inner).push(format!("{input:?}"));
        Vec::new()
    }


    fn epoch_ended(epoch: Epoch, cause: Cause) -> O::Input { O::epoch_ended(epoch, cause) }

    fn closing() -> O::Input { O::closing() }
}

/// A peer of the owner tests that keeps the inputs it received as typed values.
pub(crate) struct Captured<O: StateOwner>(PhantomData<fn() -> O>);

impl<O: StateOwner> StateOwner for Captured<O> {
    const EPOCHED: bool = false;

    type State = Log<O::Input>;
    type Input = O::Input;
    type Effect = NoEffect;

    fn step(state: &mut Self::State, input: O::Input, _now: crate::runtime::Time) -> Effects<Self> {
        state.lock().unwrap_or_else(PoisonError::into_inner).push(input);
        Vec::new()
    }

    fn epoch_ended(epoch: Epoch, cause: Cause) -> O::Input { O::epoch_ended(epoch, cause) }

    fn closing() -> O::Input { O::closing() }
}

/// The Store of the owner tests that keeps the batches as typed values.
pub(crate) struct CapturedStore;

impl StateOwner for CapturedStore {
    const EPOCHED: bool = false;

    type State = Log<StoreBatch>;
    type Input = StoreInput;
    type Effect = NoEffect;

    fn step(state: &mut Self::State, input: Self::Input, _now: crate::runtime::Time) -> Effects<Self> {
        if let StoreInput::Commit(batch) = input { state.lock().unwrap_or_else(PoisonError::into_inner).push(batch); }
        Vec::new()
    }

    fn epoch_ended(_: u64, _: Cause) -> Self::Input { StoreInput::Idle }

    fn closing() -> Self::Input { StoreInput::Idle }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Landing {
    pub batch: BatchId,
    pub revision: i64,
    pub rows: usize,
}

/// The Store for the property tests: every batch lands at the next revision.
/// The answer goes to `to` as `Committed`; `note` sees every verdict in the order of the steps.
pub(crate) struct StoreStub<O: StateOwner>(PhantomData<fn() -> O>);

pub(crate) struct StoreStubState<O: StateOwner> {
    pub to: Address<O>,
    pub revision: i64,
    pub committed: fn(Committed) -> O::Input,
    pub note: Box<dyn Fn(Landing) + Send>,
}

impl<O: StateOwner> StateOwner for StoreStub<O> {
    const EPOCHED: bool = false;

    type State = StoreStubState<O>;
    type Input = StoreInput;
    type Effect = NoEffect;

    fn step(state: &mut StoreStubState<O>, input: StoreInput, _now: crate::runtime::Time) -> Effects<Self> {
        let StoreInput::Commit(batch) = input else { return Vec::new() };
        state.revision += 1;
        (state.note)(Landing { batch: batch.id, revision: state.revision, rows: batch.rows.len() });
        let committed = committed_at(batch.id, state.revision, &[]);
        vec![Effect::Send(Outbound::new(state.to, (state.committed)(committed)))]
    }

    fn epoch_ended(_epoch: Epoch, _cause: Cause) -> StoreInput { StoreInput::Idle }

    fn closing() -> StoreInput { StoreInput::Idle }
}

/// The number after `field` in a `Debug` string: `id: 7,` gives 7.
pub(crate) fn number_after(text: &str, field: &str) -> Option<u64> {
    let start = text.find(field)? + field.len();
    let digits: String = text[start..].chars().take_while(char::is_ascii_digit).collect();
    digits.parse().ok()
}

/// The text inside the quotes after `field` in a `Debug` string: `payload: "c1"` gives `c1`.
pub(crate) fn text_after(text: &str, field: &str) -> Option<String> {
    let start = text.find(field)? + field.len();
    let rest = text[start..].strip_prefix('"')?;
    Some(rest[..rest.find('"')?].to_string())
}

pub(crate) fn refusal_of(reason: RejectReason) -> crate::owners::write::outbox_types::Fault {
    use crate::ffi::EndCause;
    use crate::owners::write::outbox_types::Fault;
    match reason {
        RejectReason::NoSession => Fault::Ended(EndCause::SessionEnded),
        RejectReason::Closed => Fault::Ended(EndCause::Closed),
        reason => Fault::Rejected(reason),
    }
}
