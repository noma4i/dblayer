mod backoff;
mod clock;
mod counters;
mod debounce;
mod duration;
mod freshness;
mod in_flight;
mod resume;
mod retry;
mod throttle;
#[cfg(any(test, feature = "stress"))]
mod test_clock;

pub use backoff::Backoff;
pub use clock::{Clock, ManualClock, SystemClock};
pub(crate) use clock::timeout;
pub use counters::Counters;
pub use debounce::Debounce;
pub use duration::parse_duration;
pub use freshness::selected_policy;
pub use in_flight::{FetchCoalescing, FetchVerdict};
pub use resume::{ResumeDrain, ResumeDrainError, StaleScopeCandidate};
pub use retry::RetryPolicy;
pub use throttle::{Throttle, ThrottleDecision, ThrottleState};

#[cfg(test)]
mod backoff_tests;
#[cfg(test)]
mod clock_tests;
#[cfg(any(test, feature = "stress"))]
pub(crate) use test_clock::TestClock;
#[cfg(test)]
mod counters_tests;
#[cfg(test)]
mod debounce_tests;
#[cfg(test)]
mod duration_tests;
#[cfg(test)]
mod freshness_tests;
#[cfg(test)]
mod in_flight_tests;
#[cfg(test)]
mod resume_tests;
#[cfg(test)]
mod throttle_tests;
