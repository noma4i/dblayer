//! Executable references of the machine specs and the tests that compare the core with them: `Lifecycle` and `Store` (direction A), `Delivery`, `Window` and `Feed` (direction B), `Outbox` and `Transfer` (direction C).
//! Every test is named like the `Test` column of the machine tables. Tests of `Delivery`, `Window` and `Feed` in `*_gap_tests.rs` run at the level of the owner (`core/src/owners`) on `DetScheduler`; tests under the feature `spec_api` wait for the owners of their direction.

mod harness_life;
mod lifecycle;
mod ref_lifecycle;
mod ref_store;
mod ref_session;
mod ref_network;
mod ref_calls;
mod store;
mod ref_delivery;
mod ref_feed;
mod ref_window;

mod lifecycle_tests;
mod lifecycle_owner_tests;
mod lifecycle_restore_tests;
mod network_owner_tests;
mod store_owner_tests;
mod store_pin_tests;
mod session_owner_tests;
mod session_gap_tests;
mod interrupted_session_answer_tests;
mod calls_owner_tests;
mod store_tests;

mod delivery_gap_tests;
mod feed_channel_tests;
mod feed_gap_tests;
mod window_gap_tests;

mod ref_outbox;
mod rig;
mod outbox_tests;
mod outbox_trace_tests;
mod outbox_harness;
mod outbox_owner_tests;
mod outbox_discard_tests;
mod outbox_open_tests;
mod outbox_plan_tests;
mod outbox_cells_tests;
mod outbox_reserve_tests;
mod outbox_restore_tests;
mod outbox_local_tests;
mod outbox_wait_tests;
mod transfer_harness;
mod transfer_accept_tests;
mod transfer_batch_tests;
mod transfer_reserve_tests;
mod transfer_matrix_tests;
mod transfer_cache_tests;
mod transfer_steps_tests;
mod transfer_mutating_tests;
mod transfer_end_tests;
mod transfer_orphan_tests;
mod transfer_batch_restore_tests;
mod transfer_open_tests;
mod rounds_tests;
mod transfer_prop_tests;
mod transfer_wait_tests;
