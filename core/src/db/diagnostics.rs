use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Diagnostics {
    pub observation_reads: u64,
    pub observer_calls: u64,
    pub observed_queries: usize,
    pub subscriptions: usize,
}

#[derive(Default)]
pub(crate) struct Counters {
    observation_reads: AtomicU64,
    observer_calls: AtomicU64,
}

impl Counters {
    pub(crate) fn count_read(&self) {
        self.observation_reads.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn count_call(&self) {
        self.observer_calls.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn observation_reads(&self) -> u64 {
        self.observation_reads.load(Ordering::Relaxed)
    }

    pub(crate) fn observer_calls(&self) -> u64 {
        self.observer_calls.load(Ordering::Relaxed)
    }
}
