use std::any::Any;
use std::fmt::{self, Debug};
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use crate::pacing::Backoff;

pub type Epoch = u64;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Time(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ScheduleId(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cause {
    SessionReset,
    ServerEnded,
    IdentityChanged,
    SessionStarted,
    Close,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EpochStamp {
    Epoch(Epoch),
    Control,
}

pub trait Message: Any + Send + Debug {
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
}

impl<T: Any + Send + Debug> Message for T {
    fn into_any(self: Box<Self>) -> Box<dyn Any> { self }
}

pub type Effects<O> = Vec<Effect<O>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Control {
    EpochStarted(Epoch),
    EpochEnded(Epoch, Cause),
    Closing,
}

pub type EffectId = u64;

pub trait StateOwner: Sized + 'static {
    const EPOCHED: bool;
    type State: Send + 'static;
    type Input: Message;
    type Effect: Debug + Send + 'static;

    fn step(state: &mut Self::State, input: Self::Input) -> Effects<Self>;
    /// Called when the owner of an epoch meets `EpochStarted(epoch)`: the state of the new epoch instance.
    fn epoch_started(_state: &mut Self::State, _epoch: Epoch) {}
    fn tick(now: Time) -> Self::Input;
    fn epoch_ended(epoch: Epoch, cause: Cause) -> Self::Input;
    fn closing() -> Self::Input;
    fn effect_id(_effect: &Self::Effect) -> Option<EffectId> { None }
    fn executor_failed(_executor: &str, _pending: Vec<EffectId>) -> Option<Self::Input> { None }
}

pub enum Effect<O: StateOwner> {
    /// The Runtime adds `after` to its own clock when the effect runs (R5).
    Schedule { id: ScheduleId, after: Duration, input: O::Input },
    Backoff { id: ScheduleId, policy: Backoff, attempt: u32, input: O::Input },
    Cancel(ScheduleId),
    Send(Outbound),
    Broadcast(Control),
    Run(O::Effect),
}

impl<O: StateOwner> Debug for Effect<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Schedule { id, after, input } => f.debug_struct("Schedule").field("id", id).field("after", after).field("input", input).finish(),
            Self::Backoff { id, policy, attempt, input } => f.debug_struct("Backoff").field("id", id).field("base", &policy.base).field("attempt", attempt).field("input", input).finish(),
            Self::Cancel(id) => f.debug_tuple("Cancel").field(id).finish(),
            Self::Send(outbound) => Debug::fmt(outbound, f),
            Self::Broadcast(control) => f.debug_tuple("Broadcast").field(control).finish(),
            Self::Run(effect) => f.debug_tuple("Run").field(effect).finish(),
        }
    }
}

pub struct Address<O: StateOwner> {
    pub(crate) index: usize,
    marker: PhantomData<fn() -> O>,
}

impl<O: StateOwner> Address<O> {
    pub(crate) fn new(index: usize) -> Self { Self { index, marker: PhantomData } }

    pub fn index(&self) -> usize { self.index }
}

impl<O: StateOwner> Clone for Address<O> {
    fn clone(&self) -> Self { *self }
}

impl<O: StateOwner> Copy for Address<O> {}

impl<O: StateOwner> Debug for Address<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.debug_tuple("Address").field(&self.index).finish() }
}

pub struct Outbound {
    pub(crate) to: usize,
    pub(crate) stamp: EpochStamp,
    pub(crate) payload: Box<dyn Message>,
}

impl Outbound {
    pub fn new<P: StateOwner>(to: Address<P>, input: P::Input) -> Self { Self { to: to.index, stamp: EpochStamp::Control, payload: Box::new(input) } }
}

impl Debug for Outbound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.debug_struct("Send").field("to", &self.to).field("input", &self.payload).finish() }
}

pub struct Back<O: StateOwner> {
    deliver: Arc<dyn Fn(EpochStamp, O::Input) + Send + Sync>,
}

impl<O: StateOwner> Back<O> {
    pub(crate) fn new(deliver: impl Fn(EpochStamp, O::Input) + Send + Sync + 'static) -> Self { Self { deliver: Arc::new(deliver) } }

    pub fn reply(&self, stamp: EpochStamp, input: O::Input) { (self.deliver)(stamp, input); }
}

impl<O: StateOwner> Clone for Back<O> {
    fn clone(&self) -> Self { Self { deliver: self.deliver.clone() } }
}
