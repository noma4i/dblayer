use super::window_fresh_tests::{opening_twin, saved};
use super::*;

// window.md P-WIN-32: the value a stream gets is a function of the stage, `outcome` and the rows.

fn same_value(twin: &Twin, line: &str) {
    if twin.stage().is_none() { return; }
    let state = twin.load_state();
    let owner = (twin.phase(), state.error.is_some(), state.settled, state.has_more);
    let want = (twin.reference.phase(), twin.reference.error().is_some(), twin.reference.settled(), twin.reference.has_more());
    assert_eq!(owner, want, "{line}");
}

fn offline_watch(twin: &mut Twin) {
    twin.step(Input::Online(false));
    twin.watch();
    assert_eq!(twin.stage(), Some(WindowStage::Waiting));
}

fn random_program(seed: u64) {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut roll = |bound: usize| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        usize::try_from(state % bound as u64).expect("a small roll")
    };
    let mut twin = Twin::new();
    let (mut watching, mut call) = (false, 1u32);
    for step in 0..40 {
        match twin.stage().unwrap_or(WindowStage::Idle) {
            WindowStage::Loading => match roll(4) {
                0 => { twin.respond(None); }
                1 => { twin.fail(Failure::Permanent); }
                2 => { twin.step(Input::Online(false)); }
                _ => { twin.call(call, Goal::First); call += 1; }
            },
            WindowStage::Landing => if roll(3) == 0 && watching { twin.step(Input::InterestMinus { id: STREAM }); watching = false; } else { twin.land(); },
            WindowStage::Waiting => if roll(2) == 0 && watching { twin.step(Input::InterestMinus { id: STREAM }); watching = false; } else { twin.step(Input::Online(true)); },
            _ => match roll(5) {
                0 if !watching => { twin.watch(); watching = true; }
                1 if watching => { twin.step(Input::InterestMinus { id: STREAM }); watching = false; }
                2 => { twin.call(call, Goal::First); call += 1; }
                3 => { twin.step(Input::Stale { in_keys: true }); }
                _ => { twin.step(Input::Online(roll(2) == 0)); }
            },
        }
        same_value(&twin, &format!("seed {seed} step {step}"));
    }
}

/// Waiting for the network shows the phase of `outcome`, not Loading.
#[test]
fn win_phase_waiting_by_outcome() {
    let mut twin = Twin::new();
    twin.watch();
    twin.step(Input::Online(false));
    assert_eq!((twin.stage(), twin.load_state().phase), (Some(WindowStage::Waiting), OwnerPhase::Idle), "no outcome yet");
    same_value(&twin, "waiting, no outcome");
    let mut landed = Twin::new();
    landed.watch();
    landed.finish(None);
    landed.step(Input::Stale { in_keys: true });
    landed.step(Input::Online(false));
    assert_eq!((landed.stage(), landed.load_state().phase), (Some(WindowStage::Waiting), OwnerPhase::Loaded), "after a landing");
    same_value(&landed, "waiting, landed");
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
    same_value(&twin, "interest back offline");
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
    same_value(&twin, "interest back offline");
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
    same_value(&twin, "cancelled load, interest back offline");
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
    same_value(&twin, "fresh entry");
}

/// A refusal shows Loaded with an error when the window has rows and Failed with an error when it has none; both are settled.
#[test]
fn win_phase_failed_with_and_without_rows() {
    let mut empty = Twin::new();
    empty.watch();
    empty.fail(Failure::Permanent);
    let state = empty.load_state();
    assert_eq!((state.phase, state.error.is_some(), state.settled), (OwnerPhase::Failed, true, true));
    same_value(&empty, "failed without rows");
    let mut rows = Twin::new();
    rows.watch();
    rows.finish(None);
    rows.step(Input::Stale { in_keys: true });
    rows.fail(Failure::Permanent);
    let state = rows.load_state();
    assert_eq!((state.phase, state.error.is_some(), state.settled), (OwnerPhase::Loaded, true, true));
    same_value(&rows, "failed with rows");
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
    same_value(&twin, "new load after a refusal");
    twin.finish(None);
    let state = twin.load_state();
    assert_eq!((state.phase, state.error.is_some()), (OwnerPhase::Loaded, false), "the landing removes it");
    same_value(&twin, "landed after a refusal");
}

#[test]
fn win_prop_phase_is_a_function_of_stage_and_outcome() {
    std::thread::scope(|scope| {
        let runs: Vec<_> = (1..=8u64).map(|seed| scope.spawn(move || random_program(seed))).collect();
        for run in runs {
            if let Err(panic) = run.join() {
                std::panic::resume_unwind(panic);
            }
        }
    });
}
