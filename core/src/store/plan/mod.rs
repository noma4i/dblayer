//! The plan of a write the platform asks for: what the call changes before the server answers, and what the owner Store needs to take it back.

mod effects;
pub(crate) mod nested_ref;
pub(crate) mod op_effect;
mod outbox_entry;
mod prior;
mod view;
mod write;

pub(crate) use outbox_entry::{plan_fault, read_outbox, OutboxOperation};
pub(crate) use prior::{swap_id, Prior, PriorRow};
pub(crate) use write::{LocalPlan, MutationCall, Planned};
pub(crate) use view::ReaderView;
