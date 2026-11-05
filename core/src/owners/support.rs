//! Stubs for the owner-level tests: an effect log, a scripted source of inputs, a Lifecycle stub that bumps the model generation.

use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use crate::runtime::{Address, Back, Cause, Effect, EffectRunner, Effects, Epoch, EpochStamp, Outbound, StateOwner, Time};

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
