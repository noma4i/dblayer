//! Executable references of the machine specs and the tests that compare the core with them: `Lifecycle` and `Store` (direction A), `Delivery`, `Window` and `Feed` (direction B), `Outbox` and `Transfer` (direction C).
//! Every test is named like the `Test` column of `specs/machines/*.md`. Tests of `Delivery`, `Window` and `Feed` in `*_gap_tests.rs` run at the level of the owner (`core/src/owners`) on `DetScheduler`; tests under the feature `spec_api` wait for the owners of their direction.

mod harness;
mod harness_life;
mod lifecycle;
mod ref_lifecycle;
mod ref_store;
mod store;
mod ref_delivery;
mod ref_feed;
mod ref_window;

mod delivery_tests;
mod feed_tests;
mod lifecycle_tests;
mod store_tests;
mod window_tests;

#[cfg(feature = "spec_api")]
mod owner_sut;
mod delivery_gap_tests;
mod feed_gap_tests;
mod window_gap_tests;

mod ref_outbox;
mod ref_transfer;
mod rig;
mod outbox_tests;
mod transfer_tests;
#[cfg(feature = "spec_api")]
mod outbox_api_tests;
#[cfg(feature = "spec_api")]
mod transfer_api_tests;
