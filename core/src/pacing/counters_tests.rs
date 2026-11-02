use super::*;

#[test]
fn record_methods_increment_matching_counters() {
    let mut counters = Counters::default();
    counters.record_transport_call();
    counters.record_transport_call();
    counters.record_stale_version_drops(3);
    assert_eq!(counters.transport_calls, 2);
    assert_eq!(counters.stale_version_drops, 3);
}

macro_rules! sums_each_field {
    ($($field:ident),* $(,)?) => {
        let Counters { $($field: _),* } = Counters::default();
        $({
            let left = Counters { $field: 1, ..Counters::default() };
            let right = Counters { $field: 2, ..Counters::default() };
            assert_eq!(left + right, Counters { $field: 3, ..Counters::default() }, stringify!($field));
        })*
    };
}

#[test]
fn adding_counters_sums_every_field_and_no_other() {
    sums_each_field!(
        stale_version_drops, membership_missing_entity, unknown_operation_acks, observation_reads, observer_calls, rows_delivered,
        rows_loaded, related_visited, entity_facts_told, entity_position_skips, entity_reads_failed, cache_hits, cache_misses, cache_evictions,
        cache_estimated_bytes, peek_hits, peek_misses, transport_calls, lost_outcomes, settle_parks, edge_read_steps,
        edge_read_scans, sql_under_owner, query_reads_failed, owner_panics, outbox_reads, outbox_rows_loaded, outbox_candidates, plans_off_writer, plans_under_writer, reach_notices, listener_spawn_failures,
    );
}
