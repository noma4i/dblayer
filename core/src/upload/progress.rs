use std::collections::BTreeMap;
use std::time::Duration;

use crate::pacing::{Throttle, ThrottleDecision, ThrottleState};

#[derive(Debug, Clone)]
pub struct TransferProgress {
    total: u64,
    finished: BTreeMap<u32, u64>,
    running: BTreeMap<u32, u64>,
    floor: u64,
    throttle: Throttle,
    gate: ThrottleState,
}

impl TransferProgress {
    pub fn new(total: u64, tick: Duration) -> Self {
        Self { total, finished: BTreeMap::new(), running: BTreeMap::new(), floor: 0, throttle: Throttle::new(tick), gate: ThrottleState::default() }
    }

    pub fn begin_phase(&mut self, finished: impl IntoIterator<Item = (u32, u64)>) {
        self.finished = finished.into_iter().collect();
        self.running.clear();
        self.floor = 0;
    }

    pub fn report(&mut self, part: u32, bytes: u64) {
        if !self.finished.contains_key(&part) {
            self.running.insert(part, bytes);
        }
    }

    pub fn finish(&mut self, part: u32, size: u64) {
        self.running.remove(&part);
        self.finished.insert(part, size);
    }

    pub fn sent(&mut self) -> u64 {
        let now: u64 = self.finished.values().chain(self.running.values()).sum();
        self.floor = self.floor.max(now).min(self.total);
        self.floor
    }

    pub fn due(&mut self, now: Duration) -> Option<u64> {
        if self.throttle.decide(&self.gate, now) != ThrottleDecision::Run {
            return None;
        }
        self.gate = self.throttle.finished(&self.throttle.started(now));
        Some(self.sent())
    }
}
