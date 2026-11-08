//! Executable references of the machine specs and the tests that compare the core with them: `Lifecycle` and `Store` (direction A), `Delivery`, `Window` and `Feed` (direction B), `Outbox` and `Transfer` (direction C).
//! Every test is named like the `Test` column of the machine tables. Tests of `Delivery`, `Window` and `Feed` in `*_gap_tests.rs` run at the level of the owner (`core/src/owners`) on `DetScheduler`.

pub(crate) mod support;
mod harness_life;
mod lifecycle;
mod ref_lifecycle;
mod ref_store;
mod ref_session;
mod ref_network;
mod ref_calls;
mod store;
mod ref_delivery;
pub(crate) mod ref_delivery_fanout;
mod ref_feed;
mod ref_window;

#[cfg(test)]
mod lifecycle_tests;
#[cfg(test)]
pub(crate) mod lifecycle_owner_tests;
#[cfg(test)]
mod lifecycle_restore_tests;
#[cfg(test)]
mod network_owner_tests;
#[cfg(test)]
mod store_owner_tests;
#[cfg(test)]
mod store_pin_tests;
#[cfg(test)]
mod session_owner_tests;
#[cfg(test)]
mod session_gap_tests;
#[cfg(test)]
mod interrupted_session_answer_tests;
#[cfg(test)]
mod calls_owner_tests;
#[cfg(test)]
mod store_tests;

#[cfg(test)]
mod delivery_gap_tests;
#[cfg(test)]
mod feed_channel_tests;
#[cfg(test)]
mod feed_gap_tests;
#[cfg(test)]
mod window_gap_tests;

mod ref_outbox;
mod rig;
#[cfg(test)]
mod outbox_tests;
#[cfg(test)]
mod outbox_trace_tests;
mod outbox_harness;
#[cfg(test)]
mod outbox_owner_tests;
#[cfg(test)]
mod outbox_discard_tests;
#[cfg(test)]
mod outbox_open_tests;
#[cfg(test)]
mod data_rig_tests;
#[cfg(test)]
mod data_lands_tests;
#[cfg(test)]
mod data_window_tests;
#[cfg(test)]
mod outbox_plan_tests;
#[cfg(test)]
mod outbox_cells_tests;
#[cfg(test)]
mod outbox_queue_tests;
#[cfg(test)]
mod outbox_marks_tests;
#[cfg(test)]
mod outbox_reserve_tests;
#[cfg(test)]
mod outbox_restore_tests;
#[cfg(test)]
mod outbox_release_tests;
#[cfg(test)]
mod outbox_local_tests;
#[cfg(test)]
mod outbox_wait_tests;
mod transfer_harness;
#[cfg(test)]
mod transfer_accept_tests;
#[cfg(test)]
mod transfer_batch_tests;
#[cfg(test)]
mod transfer_batch_intent_tests;
#[cfg(test)]
mod transfer_retry_tests;
#[cfg(test)]
mod transfer_reserve_tests;
#[cfg(test)]
mod transfer_matrix_tests;
#[cfg(test)]
mod transfer_cache_tests;
#[cfg(test)]
mod transfer_steps_tests;
#[cfg(test)]
mod transfer_mutating_tests;
#[cfg(test)]
mod transfer_end_tests;
#[cfg(test)]
mod transfer_orphan_tests;
#[cfg(test)]
mod transfer_batch_restore_tests;
#[cfg(test)]
mod transfer_open_tests;
#[cfg(test)]
mod transfer_release_tests;
#[cfg(test)]
mod transfer_open_batch_tests;
#[cfg(test)]
mod transfer_work_tests;
#[cfg(test)]
mod rounds_tests;
#[cfg(test)]
mod transfer_prop_tests;
#[cfg(test)]
mod transfer_wait_tests;
