//! Direction C: owners `Outbox` and `Transfers` with the executors of their effects (`specs/machines/design.md` F3).
//! The executors are not connected to `Database` yet.

pub mod outbox;
pub mod outbox_executor;
pub mod transfer;
// The Transfer executor is wired to `Database` in the next step.
#[allow(dead_code)]
pub(crate) mod transfer_executor;
pub mod wire;

#[cfg(test)]
mod rig;
#[cfg(test)]
mod outbox_executor_tests;
#[cfg(test)]
mod transfer_executor_tests;

use super::lifecycle::Peer;

pub const ABSENT: [Peer; 2] = [Peer::Outbox, Peer::Transfer];
