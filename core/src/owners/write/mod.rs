//! Direction C: owners `Outbox` and `Transfers` with the executors of their effects.

pub mod outbox;
mod outbox_commit;
mod outbox_flow;
mod outbox_local;
mod outbox_restore;
pub mod outbox_types;
pub mod outbox_executor;
pub mod transfer;
pub(crate) mod transfer_executor;
pub mod wire;

#[cfg(test)]
mod rig;
#[cfg(test)]
mod outbox_executor_tests;
#[cfg(test)]
mod transfer_executor_tests;


