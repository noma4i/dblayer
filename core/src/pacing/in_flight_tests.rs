use super::*;

#[test]
fn begin_accepts_new_key_and_rejects_duplicate() {
    let mut keys = InFlightKeys::default();
    assert!(keys.begin("request"));
    assert!(!keys.begin("request"));
    assert_eq!(keys.count(), 1);
}

#[test]
fn begin_rejects_empty_key() {
    let mut keys = InFlightKeys::default();
    assert!(!keys.begin(""));
    assert_eq!(keys.count(), 0);
}

#[test]
fn finish_removes_key_and_allows_begin_again() {
    let mut keys = InFlightKeys::default();
    assert!(keys.begin("request"));
    keys.finish("request");
    assert!(!keys.is_in_flight("request"));
    assert!(keys.begin("request"));
}

#[test]
fn different_keys_have_distinct_in_flight_state() {
    let mut keys = InFlightKeys::default();
    assert!(keys.begin("first"));
    assert!(keys.begin("second"));
    assert_eq!(keys.count(), 2);
    assert!(keys.is_in_flight("first"));
    assert!(keys.is_in_flight("second"));
    assert!(!keys.is_in_flight("other"));
}

#[test]
fn second_request_for_same_key_awaits_in_flight_and_finish_allows_new_fetch() {
    let mut coalescing = FetchCoalescing::default();
    assert!(coalescing.in_flight().is_empty());
    assert_eq!(coalescing.transport_calls(), 0);
    assert_eq!(coalescing.request("a", true), FetchVerdict::StartFetch);
    assert_eq!(coalescing.request("a", true), FetchVerdict::AwaitInFlight);
    assert_eq!(coalescing.request("a", false), FetchVerdict::AwaitInFlight);
    assert_eq!(coalescing.transport_calls(), 1);
    coalescing.finish("a");
    assert!(!coalescing.is_busy("a"));
    assert_eq!(coalescing.request("a", true), FetchVerdict::StartFetch);
    assert_eq!(coalescing.transport_calls(), 2);
}

#[test]
fn fresh_keys_skip_and_multiple_keys_track_independently() {
    let mut coalescing = FetchCoalescing::default();
    assert_eq!(coalescing.request("fresh", false), FetchVerdict::SkipFresh);
    assert!(coalescing.in_flight().is_empty());
    assert_eq!(coalescing.transport_calls(), 0);
    for key in ["a", "b", "c"] {
        assert_eq!(coalescing.request(key, true), FetchVerdict::StartFetch);
    }
    assert_eq!(coalescing.transport_calls(), 3);
    assert_eq!(coalescing.in_flight().len(), 3);
    coalescing.finish("unknown");
    assert_eq!(coalescing.in_flight().len(), 3);
}
