//! Direction C: owners `Outbox` and `Transfers` with the executors of their effects.

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
pub mod operation_status;
pub mod outbox_executor;
pub mod transfer;
mod transfer_accept;
mod transfer_batch;
mod transfer_end;
mod transfer_open;
mod transfer_steps;
pub mod transfer_types;
pub mod rounds;
pub(crate) mod rounds_executor;
pub(crate) mod transfer_executor;
pub mod wire;
pub(crate) mod upload_codec;

#[cfg(test)]
mod rig;
#[cfg(test)]
mod answer_tests;
#[cfg(test)]
mod outbox_executor_tests;
#[cfg(test)]
mod transfer_executor_tests;
#[cfg(test)]
mod failure_tests;
#[cfg(test)]
mod operation_machine_tests;
#[cfg(test)]
mod operation_outbox_tests;
#[cfg(test)]
mod operation_status_tests;
#[cfg(test)]
mod plan_cascade_tests;
#[cfg(test)]
mod plan_intent_tests;
#[cfg(test)]
mod plan_variables_tests;
