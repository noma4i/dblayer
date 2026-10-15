use super::*;

#[test]
fn second_request_for_same_key_awaits_in_flight_and_finish_allows_new_fetch() {
    let mut coalescing = FetchCoalescing::default();
    assert!(coalescing.in_flight().is_empty());
    assert_eq!(coalescing.request("a", true, || false), FetchVerdict::StartFetch);
    assert_eq!(coalescing.request("a", true, || false), FetchVerdict::AwaitInFlight);
    assert_eq!(coalescing.request("a", false, || false), FetchVerdict::AwaitInFlight);
    coalescing.finish("a");
    assert!(coalescing.in_flight().is_empty());
    assert_eq!(coalescing.request("a", true, || false), FetchVerdict::StartFetch);
}

#[test]
fn fresh_keys_skip_and_multiple_keys_track_independently() {
    let mut coalescing = FetchCoalescing::default();
    assert_eq!(coalescing.request("fresh", false, || true), FetchVerdict::SkipFresh);
    assert!(coalescing.in_flight().is_empty());
    for key in ["a", "b", "c"] {
        assert_eq!(coalescing.request(key, true, || false), FetchVerdict::StartFetch);
    }
    assert_eq!(coalescing.in_flight().len(), 3);
    coalescing.finish("unknown");
    assert_eq!(coalescing.in_flight().len(), 3);
}

#[test]
fn a_caller_reading_freshness_after_the_flight_landed_skips_the_second_fetch() {
    let mut coalescing = FetchCoalescing::default();
    assert_eq!(coalescing.request("a", true, || false), FetchVerdict::StartFetch);
    coalescing.finish("a");
    assert_eq!(coalescing.request("a", false, || true), FetchVerdict::SkipFresh);
}
