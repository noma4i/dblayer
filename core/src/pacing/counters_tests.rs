use super::*;

#[test]
fn new_counters_have_zero_values() {
    assert_eq!(Counters::default(), Counters {
        causal_admission_drops: 0,
        tombstone_write_drops: 0,
        non_resident_touch_drops: 0,
        counter_op_drops: 0,
        stale_version_drops: 0,
        observation_ticks: 0,
        transport_calls: 0,
    });
}

#[test]
fn record_methods_increment_matching_counters() {
    let mut counters = Counters::default();
    counters.record_transport_call();
    counters.record_transport_call();
    counters.record_observation_tick();
    counters.record_stale_version_drops(3);
    assert_eq!(counters.transport_calls, 2);
    assert_eq!(counters.observation_ticks, 1);
    assert_eq!(counters.stale_version_drops, 3);
    assert_eq!(counters.causal_admission_drops, 0);
}
