#![allow(unexpected_cfgs)]

mod cell;
mod det;
mod explore;
mod mailbox;
mod outcomes;
mod owner;
mod sync;
mod threaded;

pub use cell::EffectRunner;
pub use det::{Choice, DetScheduler, Lane, TraceEntry};
pub use explore::{explore, Exhaustive, Limits, Manifest, Property, Report, Violation};
pub use outcomes::{Outcome, OutcomeId, Outcomes, Waiter};
pub use owner::{Address, Back, Cause, Control, Effect, Effects, Epoch, Message, Outbound, StateOwner, ScheduleId, EpochStamp, Time, EffectId};
pub use threaded::{Handle, Runtime, RuntimeBuilder};

#[cfg(test)]
mod testing;
#[cfg(test)]
mod cell_tests;
#[cfg(test)]
mod det_tests;
#[cfg(test)]
mod explore_tests;
#[cfg(test)]
mod outcomes_tests;
#[cfg(test)]
mod threaded_tests;
#[cfg(test)]
mod props_tests;
