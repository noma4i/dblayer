//! Direction C: owners `Outbox` and `Transfers` with the executors of their effects.

mod alias_map;
pub mod answer;
pub mod failure;
pub mod outbox;
mod outbox_commit;
mod outbox_flow;
mod outbox_local;
pub mod outbox_plan;
mod outbox_restore;
pub mod outbox_types;
pub mod plan_cascade;
pub mod plan_intent;
pub(crate) mod plan_variables;
pub(crate) mod operation_machine;
pub mod operation_outbox;
pub(crate) mod operation_queue;
pub mod operation_status;
pub mod outbox_executor;
pub mod transfer;
mod transfer_accept;
mod transfer_batch;
mod transfer_commit;
mod transfer_end;
mod transfer_open;
mod transfer_prepare;
mod transfer_steps;
pub mod transfer_types;
pub mod rounds;
pub mod upload;
pub(crate) mod rounds_executor;
pub(crate) mod transfer_executor;
pub mod wire;
pub(crate) mod upload_codec;
pub mod upload_io;

#[cfg(test)]
mod rig;
#[cfg(test)]
mod tests;
