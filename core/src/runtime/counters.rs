use std::ops::Add;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Counters {
    pub membership_missing_entity: usize,
    pub unknown_operation_acks: usize,
    pub observation_reads: usize,
    pub observer_calls: usize,
    pub rows_delivered: usize,
    pub rows_read: usize,
    pub edge_read_steps: usize,
    pub edge_read_scans: usize,
    pub outbox_candidates: usize,
}

impl Add for Counters {
    type Output = Counters;

    fn add(self, other: Counters) -> Counters {
        Counters {
            membership_missing_entity: self.membership_missing_entity + other.membership_missing_entity,
            unknown_operation_acks: self.unknown_operation_acks + other.unknown_operation_acks,
            observation_reads: self.observation_reads + other.observation_reads,
            observer_calls: self.observer_calls + other.observer_calls,
            rows_delivered: self.rows_delivered + other.rows_delivered,
            rows_read: self.rows_read + other.rows_read,
            edge_read_steps: self.edge_read_steps + other.edge_read_steps,
            edge_read_scans: self.edge_read_scans + other.edge_read_scans,
            outbox_candidates: self.outbox_candidates + other.outbox_candidates,
        }
    }
}
