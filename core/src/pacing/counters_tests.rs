use super::*;

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
        rows_loaded, related_visited, cache_hits, cache_misses, cache_evictions,
        cache_estimated_bytes, peek_hits, peek_misses, transport_calls, edge_read_steps,
        edge_read_scans, sql_under_owner, query_reads_failed, owner_panics, outbox_reads, outbox_rows_loaded, outbox_candidates, reach_notices, listener_spawn_failures,
    );
}
