use super::*;
use std::time::Duration;

fn secs(value: u64) -> Duration {
    Duration::from_secs(value)
}

#[test]
fn decide_follows_interval_boundary() {
    let throttle = Throttle::new(secs(8));
    let state = ThrottleState { last_run_at: Some(secs(0)), is_in_flight: false };
    assert_eq!(throttle.decide(&state, Duration::from_millis(7900)), ThrottleDecision::Skip);
    assert_eq!(throttle.decide(&state, secs(8)), ThrottleDecision::Run);
}

#[test]
fn never_run_state_runs() {
    assert_eq!(Throttle::new(secs(8)).decide(&ThrottleState::default(), secs(0)), ThrottleDecision::Run);
}

#[test]
fn in_flight_state_joins_in_flight() {
    let state = ThrottleState { last_run_at: Some(secs(0)), is_in_flight: true };
    assert_eq!(Throttle::new(secs(8)).decide(&state, secs(100)), ThrottleDecision::JoinInFlight);
}

#[test]
fn started_and_finished_transition_state() {
    let throttle = Throttle::new(secs(8));
    let started = throttle.started(secs(5));
    assert_eq!(started.last_run_at, Some(secs(5)));
    assert!(started.is_in_flight);
    let finished = throttle.finished(&started);
    assert_eq!(finished.last_run_at, Some(secs(5)));
    assert!(!finished.is_in_flight);
    assert_eq!(throttle.decide(&finished, secs(5)), ThrottleDecision::Skip);
}

#[test]
fn clock_before_last_run_skips_without_underflow() {
    let state = ThrottleState { last_run_at: Some(secs(10)), is_in_flight: false };
    assert_eq!(Throttle::new(secs(8)).decide(&state, secs(5)), ThrottleDecision::Skip);
}
