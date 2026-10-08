use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThrottleDecision {
    Run,
    JoinInFlight,
    Skip,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ThrottleState {
    pub last_run_at: Option<Duration>,
    pub is_in_flight: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Throttle {
    pub interval: Duration,
}

impl Throttle {
    pub fn new(interval: Duration) -> Self {
        Self { interval }
    }

    pub fn decide(&self, state: &ThrottleState, now: Duration) -> ThrottleDecision {
        if state.is_in_flight {
            return ThrottleDecision::JoinInFlight;
        }
        match state.last_run_at {
            Some(last_run_at) if now.saturating_sub(last_run_at) < self.interval => ThrottleDecision::Skip,
            _ => ThrottleDecision::Run,
        }
    }

    pub fn started(&self, now: Duration) -> ThrottleState {
        ThrottleState { last_run_at: Some(now), is_in_flight: true }
    }

    pub fn finished(&self, state: &ThrottleState) -> ThrottleState {
        ThrottleState { last_run_at: state.last_run_at, is_in_flight: false }
    }
}
