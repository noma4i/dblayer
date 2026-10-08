#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Counters {
    pub causal_admission_drops: usize,
    pub tombstone_write_drops: usize,
    pub non_resident_touch_drops: usize,
    pub stale_version_drops: usize,
    pub observation_ticks: usize,
    pub transport_calls: usize,
}

impl Counters {
    pub fn record_transport_call(&mut self) {
        self.transport_calls += 1;
    }

    pub fn record_observation_tick(&mut self) {
        self.observation_ticks += 1;
    }

    pub fn record_stale_version_drops(&mut self, count: usize) {
        self.stale_version_drops += count;
    }
}
