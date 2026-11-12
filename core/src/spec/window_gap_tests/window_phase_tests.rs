use super::window_fresh_tests::{opening_twin, saved};
use super::*;

// window.md P-WIN-32: the value a stream gets is a function of the stage, `outcome` and the rows.

fn offline_watch(twin: &mut Twin) {
    twin.step(Input::Online(false));
    twin.watch();
    assert_eq!(twin.stage(), Some(WindowStage::Waiting));
}

/// Waiting for the network shows the phase of `outcome`, not Loading.
#[test]
fn win_phase_waiting_by_outcome() {
    let mut twin = Twin::new();
    twin.watch();
    twin.step(Input::Online(false));
    assert_eq!((twin.stage(), twin.load_state().phase), (Some(WindowStage::Waiting), OwnerPhase::Idle), "no outcome yet");
    let mut landed = Twin::new();
    landed.watch();
    landed.finish(None);
    landed.step(Input::Stale { in_keys: true });
    landed.step(Input::Online(false));
    assert_eq!((landed.stage(), landed.load_state().phase), (Some(WindowStage::Waiting), OwnerPhase::Loaded), "after a landing");
}

/// A landing refused by a new generation while nobody watches leaves the stage Idle, the phase follows `outcome`.
#[test]
fn win_phase_stale_generation_without_interest() {
    let mut twin = Twin::new();
    twin.watch();
    twin.respond(None);
    twin.step(Input::InterestMinus { id: STREAM });
    let request = twin.ref_landing();
    twin.step(Input::Rejected { request, reason: Reject::StaleGeneration });
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
    offline_watch(&mut twin);
    assert_eq!(twin.load_state().phase, OwnerPhase::Idle, "a window that never landed is not loading");
}

/// The last interest takes a refusal with it.
#[test]
fn win_phase_leaving_clears_the_refusal() {
    let mut twin = Twin::new();
    twin.watch();
    twin.fail(Failure::Permanent);
    assert!(twin.load_state().error.is_some());
    twin.step(Input::InterestMinus { id: STREAM });
    twin.step(Input::Online(false));
    twin.watch();
    let state = twin.load_state();
    assert_eq!((state.phase, state.error.is_some(), state.settled), (OwnerPhase::Idle, false, false));
}

/// A load cancelled by the last interest leaves no outcome, the rows of the saved window do not make it Loaded.
#[test]
fn win_phase_cancelled_load_keeps_no_outcome() {
    let mut twin = opening_twin();
    twin.watch();
    twin.step(Input::Read(saved(NOW - 1_000, true)));
    assert_eq!(twin.stage(), Some(WindowStage::Loading));
    twin.step(Input::InterestMinus { id: STREAM });
    assert_eq!(twin.stage(), Some(WindowStage::Idle));
    offline_watch(&mut twin);
    assert_eq!(twin.load_state().phase, OwnerPhase::Idle);
}

/// The outcome belongs to the current interest: after a landing and the last interest gone, an interest back into a not fresh window waits for a new outcome.
#[test]
fn win_phase_landing_does_not_outlive_the_interest() {
    let mut twin = Twin::new();
    twin.watch();
    twin.finish(None);
    twin.step(Input::InterestMinus { id: STREAM });
    twin.step(Input::Stale { in_keys: true });
    offline_watch(&mut twin);
    let state = twin.load_state();
    assert_eq!((state.phase, state.settled), (OwnerPhase::Idle, false));
}

/// Entering a fresh saved window is the outcome Fresh: Loaded and settled with no request.
#[test]
fn win_phase_fresh_entry() {
    let mut twin = opening_twin();
    twin.watch();
    let effects = twin.step(Input::Read(saved(NOW - 1_000, false)));
    assert_eq!(requests(&effects), 0);
    let state = twin.load_state();
    assert_eq!((state.phase, state.settled), (OwnerPhase::Loaded, true));
}

/// A refusal shows Failed with an error with and without rows, both settled: the stream judges the rows (P-DLV-32).
#[test]
fn win_phase_failed_with_and_without_rows() {
    let mut empty = Twin::new();
    empty.watch();
    empty.fail(Failure::Permanent);
    let state = empty.load_state();
    assert_eq!((state.phase, state.error.is_some(), state.settled), (OwnerPhase::Failed, true, true));
    let mut rows = Twin::new();
    rows.watch();
    rows.finish(None);
    rows.step(Input::Stale { in_keys: true });
    rows.fail(Failure::Permanent);
    let state = rows.load_state();
    assert_eq!((state.phase, state.error.is_some(), state.settled), (OwnerPhase::Failed, true, true));
}

/// Only a landing removes the error: it is still there while the next load runs.
#[test]
fn win_phase_error_removed_by_landing_only() {
    let mut twin = Twin::new();
    twin.watch();
    twin.fail(Failure::Permanent);
    twin.call(1, Goal::First);
    assert_eq!(twin.stage(), Some(WindowStage::Loading));
    let state = twin.load_state();
    assert_eq!((state.phase, state.error.is_some()), (OwnerPhase::Loading, true), "the error stays while the load runs");
    twin.finish(None);
    let state = twin.load_state();
    assert_eq!((state.phase, state.error.is_some()), (OwnerPhase::Loaded, false), "the landing removes it");
}
