//! Stubs for the owner-level tests: an effect log, a scripted source of inputs, a Lifecycle stub that bumps the model generation.

use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use crate::owners::contract::{BatchId, Committed, RejectReason, Rejected};
use crate::owners::store::StoreInput;
use crate::runtime::{Address, Back, Cause, Control, Effect, EffectRunner, Effects, Epoch, EpochStamp, Outbound, ScheduleId, StateOwner, Time};

pub(crate) type Log<E> = Arc<Mutex<Vec<E>>>;

pub(crate) fn new_log<E>() -> Log<E> { Arc::new(Mutex::new(Vec::new())) }

pub(crate) fn take<E>(log: &Log<E>) -> Vec<E> { std::mem::take(&mut *log.lock().unwrap_or_else(PoisonError::into_inner)) }

pub(crate) fn snapshot<E: Clone>(log: &Log<E>) -> Vec<E> { log.lock().unwrap_or_else(PoisonError::into_inner).clone() }

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

    fn step(state: &mut SourceState<O>, input: SourceInput) -> Effects<Self> {
        match input {
            SourceInput::Go => state.script.iter().map(|item| Effect::Send(Outbound::new(state.to, item.clone()))).collect(),
            SourceInput::Idle => Vec::new(),
        }
    }

    fn tick(_now: Time) -> SourceInput { SourceInput::Idle }

    fn epoch_ended(_epoch: Epoch, _cause: Cause) -> SourceInput { SourceInput::Idle }

    fn closing() -> SourceInput { SourceInput::Idle }
}

pub(crate) struct Silent;

impl<O: StateOwner<Effect = NoEffect>> EffectRunner<O> for Silent {
    fn run(&mut self, _stamp: EpochStamp, effect: NoEffect, _back: &Back<O>) { match effect {} }
}

/// Lifecycle stub: on `Go` the Store generation becomes `generation`, then `input` goes to `to`.
pub(crate) struct Reset<O: StateOwner>(PhantomData<fn() -> O>);

pub(crate) struct ResetState<O: StateOwner> {
    pub to: Address<O>,
    pub input: O::Input,
    pub generation: u64,
}

#[derive(Debug)]
pub(crate) struct SetGeneration(pub u64);

impl<O: StateOwner> StateOwner for Reset<O>
where
    O::Input: Clone,
{
    const EPOCHED: bool = false;

    type State = ResetState<O>;
    type Input = SourceInput;
    type Effect = SetGeneration;

    fn step(state: &mut ResetState<O>, input: SourceInput) -> Effects<Self> {
        match input {
            SourceInput::Go => vec![Effect::Run(SetGeneration(state.generation)), Effect::Send(Outbound::new(state.to, state.input.clone()))],
            SourceInput::Idle => Vec::new(),
        }
    }

    fn tick(_now: Time) -> SourceInput { SourceInput::Idle }

    fn epoch_ended(_epoch: Epoch, _cause: Cause) -> SourceInput { SourceInput::Idle }

    fn closing() -> SourceInput { SourceInput::Idle }
}

pub(crate) struct StoreGeneration(pub Arc<AtomicU64>);

impl<O: StateOwner<Effect = SetGeneration>> EffectRunner<O> for StoreGeneration {
    fn run(&mut self, _stamp: EpochStamp, effect: SetGeneration, _back: &Back<O>) { self.0.store(effect.0, Ordering::SeqCst); }
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

    fn step(state: &mut EnderState, input: SourceInput) -> Effects<Self> {
        match input {
            SourceInput::Go => vec![Effect::Broadcast(crate::runtime::Control::EpochEnded(state.epoch, state.cause))],
            SourceInput::Idle => Vec::new(),
        }
    }

    fn tick(_now: Time) -> SourceInput { SourceInput::Idle }

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
}

impl<O: StateOwner> StateOwner for Spy<O>
where
    O::Effect: Clone,
{
    const EPOCHED: bool = O::EPOCHED;

    type State = SpyState<O>;
    type Input = O::Input;
    type Effect = O::Effect;

    fn step(state: &mut SpyState<O>, input: O::Input) -> Effects<Self> {
        let effects = O::step(&mut state.inner, input);
        let mut seen = state.seen.lock().unwrap_or_else(PoisonError::into_inner);
        effects
            .into_iter()
            .map(|effect| match effect {
                Effect::Run(run) => {
                    seen.push(Emitted::Run(run.clone()));
                    Effect::Run(run)
                }
                Effect::Send(outbound) => {
                    seen.push(Emitted::Send { to: outbound.to, input: format!("{:?}", outbound.payload) });
                    Effect::Send(outbound)
                }
                Effect::Schedule { id, after, input } => {
                    seen.push(Emitted::Schedule { id, after, input: format!("{input:?}") });
                    Effect::Schedule { id, after, input }
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

    fn tick(now: Time) -> O::Input { O::tick(now) }

    fn epoch_ended(epoch: Epoch, cause: Cause) -> O::Input { O::epoch_ended(epoch, cause) }

    fn closing() -> O::Input { O::closing() }
}

impl<O: StateOwner> SpyState<O> {
    pub fn new(inner: O::State, seen: &Log<Emitted<O::Effect>>) -> Self { Self { inner, seen: seen.clone() } }
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

    fn step(state: &mut Log<String>, input: O::Input) -> Effects<Self> {
        state.lock().unwrap_or_else(PoisonError::into_inner).push(format!("{input:?}"));
        Vec::new()
    }

    fn tick(now: Time) -> O::Input { O::tick(now) }

    fn epoch_ended(epoch: Epoch, cause: Cause) -> O::Input { O::epoch_ended(epoch, cause) }

    fn closing() -> O::Input { O::closing() }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Landing {
    Landed { batch: BatchId, basis: u64, revision: i64, rows: usize },
    Rejected { batch: BatchId, rows: usize },
}

/// The Store for the property tests: a batch whose basis is older than the generation is rejected, any other lands at the next revision.
/// The answer goes to `to` as `Committed` or `Rejected`; `note` sees every verdict in the order of the steps.
pub(crate) struct StoreStub<O: StateOwner>(PhantomData<fn() -> O>);

pub(crate) struct StoreStubState<O: StateOwner> {
    pub to: Address<O>,
    pub generation: Arc<AtomicU64>,
    pub revision: i64,
    /// Only the generation of this model is compared with the basis of a batch; `None` = every model.
    pub only: Option<&'static str>,
    pub committed: fn(Committed) -> O::Input,
    pub rejected: fn(Rejected) -> O::Input,
    pub note: Box<dyn Fn(Landing) + Send>,
}

impl<O: StateOwner> StateOwner for StoreStub<O> {
    const EPOCHED: bool = false;

    type State = StoreStubState<O>;
    type Input = StoreInput;
    type Effect = NoEffect;

    fn step(state: &mut StoreStubState<O>, input: StoreInput) -> Effects<Self> {
        let StoreInput::Commit(batch) = input else { return Vec::new() };
        let current = state.generation.load(Ordering::SeqCst);
        let stale: Vec<String> = batch.basis.iter().filter(|(model, basis)| **basis < current && state.only.is_none_or(|only| only == model.as_str())).map(|(model, _)| model.clone()).collect();
        if !stale.is_empty() {
            (state.note)(Landing::Rejected { batch: batch.id, rows: batch.rows.len() });
            let rejected = Rejected { batch: batch.id, reason: RejectReason::StaleGeneration { models: stale } };
            return vec![Effect::Send(Outbound::new(state.to, (state.rejected)(rejected)))];
        }
        state.revision += 1;
        (state.note)(Landing::Landed { batch: batch.id, basis: batch.basis.values().copied().min().unwrap_or(0), revision: state.revision, rows: batch.rows.len() });
        let committed = Committed { batch: batch.id, revision: state.revision, tables: Vec::new(), models: Vec::new() };
        vec![Effect::Send(Outbound::new(state.to, (state.committed)(committed)))]
    }

    fn tick(_now: Time) -> StoreInput { StoreInput::Idle }

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
