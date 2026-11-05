//! Direction C: owners `Outbox` and `Transfers` with the executors of their effects (`specs/machines/design.md` F3).

pub mod outbox;
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


