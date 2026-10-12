use super::*;

#[test]
fn new_counters_have_zero_values() {
    assert_eq!(Counters::default(), Counters {
        stale_version_drops: 0,
        rejected_records: 0,
        membership_missing_entity: 0,
        unknown_operation_acks: 0,
        observation_reads: 0,
        observer_calls: 0,
        rows_delivered: 0,
        rows_loaded: 0,
        cache_hits: 0,
        cache_misses: 0,
        cache_evictions: 0,
        transport_calls: 0,
        lost_outcomes: 0,
        settle_parks: 0,
    });
}

#[test]
fn record_methods_increment_matching_counters() {
    let mut counters = Counters::default();
    counters.record_transport_call();
    counters.record_transport_call();
    counters.record_stale_version_drops(3);
    assert_eq!(counters.transport_calls, 2);
    assert_eq!(counters.stale_version_drops, 3);
}

#[test]
fn adding_counters_sums_every_field() {
    let left = Counters { cache_hits: 1, observer_calls: 2, rejected_records: 3, ..Counters::default() };
    let right = Counters { cache_hits: 4, transport_calls: 5, ..Counters::default() };
    assert_eq!(left + right, Counters { cache_hits: 5, observer_calls: 2, rejected_records: 3, transport_calls: 5, ..Counters::default() });
}
