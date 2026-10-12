use std::ops::Add;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Counters {
    pub stale_version_drops: usize,
    pub rejected_records: usize,
    pub membership_missing_entity: usize,
    pub unknown_operation_acks: usize,
    pub observation_reads: usize,
    pub observer_calls: usize,
    pub cache_hits: usize,
    pub cache_misses: usize,
    pub cache_evictions: usize,
    pub transport_calls: usize,
    pub lost_outcomes: usize,
    pub settle_parks: usize,
}

impl Counters {
    pub fn record_transport_call(&mut self) {
        self.transport_calls += 1;
    }

    pub fn record_settle_park(&mut self) {
        self.settle_parks += 1;
    }

    pub fn record_stale_version_drops(&mut self, count: usize) {
        self.stale_version_drops += count;
    }
}

impl Add for Counters {
    type Output = Counters;

    fn add(self, other: Counters) -> Counters {
        Counters {
            stale_version_drops: self.stale_version_drops + other.stale_version_drops,
            rejected_records: self.rejected_records + other.rejected_records,
            membership_missing_entity: self.membership_missing_entity + other.membership_missing_entity,
            unknown_operation_acks: self.unknown_operation_acks + other.unknown_operation_acks,
            observation_reads: self.observation_reads + other.observation_reads,
            observer_calls: self.observer_calls + other.observer_calls,
            cache_hits: self.cache_hits + other.cache_hits,
            cache_misses: self.cache_misses + other.cache_misses,
            cache_evictions: self.cache_evictions + other.cache_evictions,
            transport_calls: self.transport_calls + other.transport_calls,
            lost_outcomes: self.lost_outcomes + other.lost_outcomes,
            settle_parks: self.settle_parks + other.settle_parks,
        }
    }
}
