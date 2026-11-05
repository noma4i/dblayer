use std::any::Any;
use std::fmt::{self, Debug};
use std::marker::PhantomData;
use std::sync::Arc;

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
    Close,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stamp {
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

pub trait Owner: Sized + 'static {
    const EPOCHED: bool;
    type State: Send + 'static;
    type Input: Message;
    type Effect: Debug + Send + 'static;

    fn step(state: &mut Self::State, input: Self::Input) -> Effects<Self>;
    fn tick(now: Time) -> Self::Input;
    fn epoch_ended(epoch: Epoch, cause: Cause) -> Self::Input;
    fn closing() -> Self::Input;
}

pub enum Effect<O: Owner> {
    Schedule { id: ScheduleId, at: Time, input: O::Input },
    Cancel(ScheduleId),
    Send(Outbound),
    Broadcast(Control),
    Run(O::Effect),
}

impl<O: Owner> Debug for Effect<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Schedule { id, at, input } => f.debug_struct("Schedule").field("id", id).field("at", at).field("input", input).finish(),
            Self::Cancel(id) => f.debug_tuple("Cancel").field(id).finish(),
            Self::Send(outbound) => Debug::fmt(outbound, f),
            Self::Broadcast(control) => f.debug_tuple("Broadcast").field(control).finish(),
            Self::Run(effect) => f.debug_tuple("Run").field(effect).finish(),
        }
    }
}

pub struct Address<O: Owner> {
    pub(crate) index: usize,
    marker: PhantomData<fn() -> O>,
}

impl<O: Owner> Address<O> {
    pub(crate) fn new(index: usize) -> Self { Self { index, marker: PhantomData } }

    pub fn index(&self) -> usize { self.index }
}

impl<O: Owner> Clone for Address<O> {
    fn clone(&self) -> Self { *self }
}

impl<O: Owner> Copy for Address<O> {}

impl<O: Owner> Debug for Address<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.debug_tuple("Address").field(&self.index).finish() }
}

pub struct Outbound {
    pub(crate) to: usize,
    pub(crate) stamp: Stamp,
    pub(crate) payload: Box<dyn Message>,
}

impl Outbound {
    pub fn new<P: Owner>(to: Address<P>, input: P::Input) -> Self { Self { to: to.index, stamp: Stamp::Control, payload: Box::new(input) } }
}

impl Debug for Outbound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.debug_struct("Send").field("to", &self.to).field("input", &self.payload).finish() }
}

pub struct Back<O: Owner> {
    deliver: Arc<dyn Fn(Stamp, O::Input) + Send + Sync>,
}

impl<O: Owner> Back<O> {
    pub(crate) fn new(deliver: impl Fn(Stamp, O::Input) + Send + Sync + 'static) -> Self { Self { deliver: Arc::new(deliver) } }

    pub fn send(&self, stamp: Stamp, input: O::Input) { (self.deliver)(stamp, input); }
}

impl<O: Owner> Clone for Back<O> {
    fn clone(&self) -> Self { Self { deliver: self.deliver.clone() } }
}
