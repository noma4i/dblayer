#![allow(unexpected_cfgs)]

mod cell;
mod det;
mod explore;
mod flights;
mod mailbox;
mod open_slot;
mod outcomes;
mod owner;
mod published;
mod sync;
mod threaded;

pub use cell::EffectRunner;
pub use det::{Choice, DetScheduler, Lane, TraceEntry};
pub use flights::{Flights, Running};
pub use open_slot::{OpenLease, OpenSlot};
pub use published::PublishedCell;
pub use explore::{explore, Limits, Report, Violation};
#[cfg(test)]
pub use property_tests::{Exhaustive, Manifest, Property};
pub use outcomes::{Outcome, OutcomeId, Outcomes, Waiter};
pub use owner::{Address, Back, Cause, Control, Effect, Effects, Epoch, Message, Outbound, StateOwner, ScheduleId, EpochStamp, Time, EffectId};
pub use threaded::{counted, hold_until_woken, polling, Handle, Holds, Launch, Runtime, RuntimeBuilder};

#[cfg(test)]
mod toys_tests;
#[cfg(test)]
mod property_tests;
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
#[cfg(test)]
mod mailbox_tests;
#[cfg(test)]
mod flights_tests;
#[cfg(test)]
mod open_slot_tests;
#[cfg(test)]
mod published_tests;
#[cfg(test)]
#[path = "../spec/ref_runtime.rs"]
mod ref_runtime;
#[cfg(test)]
#[path = "../spec/ref_runtime_tests.rs"]
mod ref_runtime_tests;
