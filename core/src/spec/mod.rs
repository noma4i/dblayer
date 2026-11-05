//! Direction B of the machine specs: the executable reference of `Delivery`, `Window` and `Feed` and the tests that compare the core with it.
//! Every test is named like the `Test` column of `specs/machines/{delivery,window,feed}.md`. Tests that need an input or an observable the public API does not have live in `*_gap_tests.rs` under the feature `spec_api` (see `api_gap.md`).

mod harness;
mod ref_delivery;
mod ref_feed;
mod ref_window;

mod delivery_tests;
mod feed_tests;
mod window_tests;

#[cfg(feature = "spec_api")]
mod delivery_gap_tests;
#[cfg(feature = "spec_api")]
mod feed_gap_tests;
#[cfg(feature = "spec_api")]
mod window_gap_tests;
