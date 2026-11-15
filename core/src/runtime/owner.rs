use std::any::Any;
use std::fmt::{self, Debug};
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use crate::runtime::RetryPolicy;

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

    /// A typed copy the tests of the owners read; every input is `Clone` in the test build.
    #[cfg(test)]
    fn typed(&self) -> Typed;
}

#[cfg(not(test))]
impl<T: Any + Send + Debug> Message for T {
    fn into_any(self: Box<Self>) -> Box<dyn Any> { self }
}

#[cfg(test)]
impl<T: Any + Send + Debug + Clone> Message for T {
    fn into_any(self: Box<Self>) -> Box<dyn Any> { self }

    fn typed(&self) -> Typed { Typed(Arc::new(std::sync::Mutex::new(Box::new(self.clone())))) }
}

/// The input of a step or of a `Send` as a typed value: the test names the input type of the owner and matches on it.
#[cfg(test)]
#[derive(Clone)]
pub struct Typed(Arc<std::sync::Mutex<Box<dyn Any + Send>>>);

#[cfg(test)]
impl Debug for Typed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str("Typed") }
}

#[cfg(test)]
impl PartialEq for Typed {
    fn eq(&self, other: &Self) -> bool { Arc::ptr_eq(&self.0, &other.0) }
}

#[cfg(test)]
impl Eq for Typed {}

#[cfg(test)]
impl Typed {
    /// Whether the input is a `T` for which `check` holds.
    pub fn is<T: 'static>(&self, check: impl FnOnce(&T) -> bool) -> bool { self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner).downcast_ref::<T>().is_some_and(check) }

    /// A clone of the input when it is a `T`.
    pub fn typed<T: 'static + Clone>(&self) -> Option<T> { self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner).downcast_ref::<T>().cloned() }
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

    fn step(state: &mut Self::State, input: Self::Input, now: Time) -> Effects<Self>;
    #[cfg(test)]
    fn diagnostic(_state: &Self::State) -> Option<String> { None }
    fn epoch_started(_epoch: Epoch) -> Option<Self::Input> { None }
    fn epoch_ended(epoch: Epoch, cause: Cause) -> Self::Input;
    fn closing() -> Self::Input;
    fn effect_id(_effect: &Self::Effect) -> Option<EffectId> { None }
    fn defer_executor_failure(_pending: &[EffectId]) -> bool { false }
    /// The inputs that apply the failure cell of each pending effect id, and of the executor itself.
    fn executor_failed(_executor: &str, _pending: Vec<EffectId>) -> Vec<Self::Input> { Vec::new() }
}

pub enum Effect<O: StateOwner> {
    /// The Runtime adds `after` to its own clock when the effect runs (R5).
    Schedule { id: ScheduleId, after: Duration, input: O::Input },
    /// A point on the scale of the Clock (R5a): the owner knows the deadline, the moment of the effect does not move it.
    ScheduleAt { id: ScheduleId, at: Time, input: O::Input },
    /// `floor` is the least delay (ZERO = none): the Runtime waits the longer of it and the delay of the policy.
    Backoff { id: ScheduleId, policy: RetryPolicy, attempt: u32, floor: Duration, input: O::Input },
    Cancel(ScheduleId),
    Send(Outbound),
    Broadcast(Control),
    Run(O::Effect),
}

impl<O: StateOwner> Debug for Effect<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Schedule { id, after, input } => f.debug_struct("Schedule").field("id", id).field("after", after).field("input", input).finish(),
            Self::ScheduleAt { id, at, input } => f.debug_struct("ScheduleAt").field("id", id).field("at", at).field("input", input).finish(),
            Self::Backoff { id, policy, attempt, floor, input } => f.debug_struct("Backoff").field("id", id).field("policy", policy).field("attempt", attempt).field("floor", floor).field("input", input).finish(),
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
    fail: Arc<dyn Fn(Vec<EffectId>) + Send + Sync>,
}

impl<O: StateOwner> Back<O> {
    pub(crate) fn new(deliver: impl Fn(EpochStamp, O::Input) + Send + Sync + 'static, fail: impl Fn(Vec<EffectId>) + Send + Sync + 'static) -> Self { Self { deliver: Arc::new(deliver), fail: Arc::new(fail) } }

    pub fn reply(&self, stamp: EpochStamp, input: O::Input) { (self.deliver)(stamp, input); }

    /// An asynchronous task of the executor died: the owner gets the failure of `pending` and Lifecycle closes the database, as for a panic in `run`.
    pub fn fail(&self, pending: Vec<EffectId>) { (self.fail)(pending); }

    /// Held by an asynchronous task for the effect `id`: dropped while the task panics, it reports the failure of `id`.
    pub fn watch(&self, id: EffectId) -> FailureWatch<O> { FailureWatch { back: self.clone(), id } }
}

pub struct FailureWatch<O: StateOwner> {
    back: Back<O>,
    id: EffectId,
}

impl<O: StateOwner> Drop for FailureWatch<O> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.back.fail(vec![self.id]);
        }
    }
}

impl<O: StateOwner> Clone for Back<O> {
    fn clone(&self) -> Self { Self { deliver: self.deliver.clone(), fail: self.fail.clone() } }
}
