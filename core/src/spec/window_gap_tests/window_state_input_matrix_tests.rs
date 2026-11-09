//! Matrix S for the owner Windows: every start state of the harness x every input of the alphabet x every second input. The oracle is the reference `ref_window` written from the table: after each step the owner and the reference must give the same ordered effects, the same answers to the callers and the same stage (the assertions of `Twin::step`). A divergence is a red cell with the first line of the assertion.
use std::panic::{catch_unwind, AssertUnwindSafe};

use super::*;
use crate::test_matrix::Matrix;

fn alphabet() -> Vec<(&'static str, Input)> {
    vec![
        ("InterestPlus", Input::InterestPlus { id: 7 }),
        ("InterestMinus", Input::InterestMinus { id: 7 }),
        ("Load first", Input::Load { id: 1, goal: Goal::First, force: false }),
        ("Load next", Input::Load { id: 1, goal: Goal::Next, force: false }),
        ("Load all forced", Input::Load { id: 2, goal: Goal::All, force: true }),
        ("Response with cursor", Input::Response { request: 1, attempt: 1, cursor: Some(2), rows: true }),
        ("Response last page", Input::Response { request: 1, attempt: 1, cursor: None, rows: true }),
        ("Network error", Input::TransportError { request: 1, attempt: 1, failure: Failure::Network }),
        ("Server error", Input::TransportError { request: 1, attempt: 1, failure: Failure::Server }),
        ("Permanent error", Input::TransportError { request: 1, attempt: 1, failure: Failure::Permanent }),
        ("Invalid cursor", Input::TransportError { request: 1, attempt: 1, failure: Failure::InvalidCursor }),
        ("Dropped", Input::Dropped { request: 1, attempt: 1 }),
        ("Stray response", Input::Response { request: 9, attempt: 1, cursor: None, rows: true }),
        ("Retry", Input::Retry { request: 1 }),
        ("Committed", Input::Committed { request: 1, revision: 5 }),
        ("Rejected storage", Input::Rejected { request: 1, reason: Reject::Storage }),
        ("Rejected stale generation", Input::Rejected { request: 1, reason: Reject::StaleGeneration }),
        ("Stale in keys", Input::Stale { in_keys: true }),
        ("StaleGap", Input::StaleGap),
        ("Online false", Input::Online(false)),
        ("Online true", Input::Online(true)),
        ("PowerSaving on", Input::PowerSaving(true)),
        ("OfflineRefused", Input::OfflineRefused),
        ("EpochEnded", Input::EpochEnded),
    ]
}

type Start = (&'static str, fn() -> Twin);

fn starts() -> Vec<Start> {
    vec![("cold", Twin::new), ("fresh", fresh), ("fresh with cursor", fresh_with_cursor), ("loading", loading), ("loading watched", loading_watched), ("landing", landing), ("waiting offline", waiting)]
}

fn first_line(payload: &Box<dyn std::any::Any + Send>) -> String {
    let text = payload.downcast_ref::<String>().cloned().or_else(|| payload.downcast_ref::<&str>().map(|text| text.to_string())).unwrap_or_default();
    text.lines().next().unwrap_or("").chars().take(120).collect()
}

#[test]
fn matrix_owner_state_input_windows() {
    let mut matrix = Matrix::new("S-windows");
    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    for (start_name, start) in starts() {
        for (first_name, first) in alphabet() {
            for (second_name, second) in std::iter::once(("-", None)).chain(alphabet().into_iter().map(|(name, input)| (name, Some(input)))) {
                let sequence = format!("{first_name} then {second_name}");
                let first = first.clone();
                let outcome = catch_unwind(AssertUnwindSafe(|| {
                    let mut twin = start();
                    twin.step(first);
                    if let Some(second) = second { twin.step(second); }
                }));
                let observed = match &outcome { Ok(()) => "owner and reference agree".to_string(), Err(payload) => first_line(payload) };
                matrix.check(start_name, "reference ref_window", &sequence, "owner and reference agree", &observed);
            }
        }
    }
    std::panic::set_hook(hook);
    matrix.finish();
}
