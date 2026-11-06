mod backoff;
mod clock;
mod counters;
mod duration;
mod freshness;
mod retry;
#[cfg(test)]
mod test_clock;

pub use backoff::Backoff;
pub use clock::{Clock, ManualClock, SystemClock};
pub(crate) use clock::timeout;
pub use counters::Counters;
pub use duration::parse_duration;
pub use freshness::selected_policy;
pub use retry::RetryPolicy;

#[cfg(test)]
mod backoff_tests;
#[cfg(test)]
mod clock_tests;
#[cfg(test)]
pub(crate) use test_clock::TestClock;
#[cfg(test)]
mod counters_tests;
#[cfg(test)]
mod duration_tests;
#[cfg(test)]
mod freshness_tests;
