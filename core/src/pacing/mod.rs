mod clock;
mod counters;
mod debounce;
mod freshness;
mod in_flight;
mod resume;
mod throttle;

pub use clock::{Clock, ManualClock, SystemClock, TokioClock};
pub use counters::Counters;
pub use debounce::Debounce;
pub use freshness::{expires_at, is_fresh, selected_policy, FreshnessPolicy, FreshnessTable, UnknownFreshnessPolicy};
pub use in_flight::{FetchCoalescing, FetchVerdict, InFlightKeys};
pub use resume::{
    effective_window, refetches_on_resume, resume_candidates, skips_without_readers, ResumeDrain, ResumeDrainError,
    StaleScopeCandidate,
};
pub use throttle::{Throttle, ThrottleDecision, ThrottleState};

#[cfg(test)]
mod clock_tests;
#[cfg(test)]
mod counters_tests;
#[cfg(test)]
mod debounce_tests;
#[cfg(test)]
mod freshness_tests;
#[cfg(test)]
mod in_flight_tests;
#[cfg(test)]
mod resume_tests;
#[cfg(test)]
mod throttle_tests;
