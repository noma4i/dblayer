use super::*;

#[test]
fn decide_fetches_missing_window_or_nothing_when_fresh_and_valid() {
    assert_eq!(AttachFetch::decide(false, false, true, false), AttachFetch::FetchMissing);
    assert_eq!(AttachFetch::decide(true, false, true, false), AttachFetch::None);
}

#[test]
fn decide_orders_invalidated_before_stale() {
    assert_eq!(AttachFetch::decide(true, true, false, false), AttachFetch::FetchInvalidated);
    assert_eq!(AttachFetch::decide(true, true, true, false), AttachFetch::FetchInvalidated);
    assert_eq!(AttachFetch::decide(true, false, false, false), AttachFetch::FetchStale);
}

#[test]
fn decide_none_when_in_flight_even_without_window_or_invalidated() {
    assert_eq!(AttachFetch::decide(false, true, false, true), AttachFetch::None);
}

#[test]
fn only_none_makes_no_request() {
    assert!(!AttachFetch::None.makes_request());
    assert!(AttachFetch::FetchMissing.makes_request());
    assert!(AttachFetch::FetchStale.makes_request());
    assert!(AttachFetch::FetchInvalidated.makes_request());
}

#[test]
fn refresh_always_fetches_stale_and_invalidated_requests_need_readers() {
    assert_eq!(AttachFetch::REFRESH, AttachFetch::FetchStale);
    assert!(!AttachFetch::requests_without_reader(0, true));
    assert!(AttachFetch::requests_without_reader(1, true));
    assert!(!AttachFetch::requests_without_reader(1, false));
}

#[test]
fn attach_and_detach_results_are_distinct() {
    assert_ne!(AttachResult::StartedObservation, AttachResult::JoinedObservation);
    assert_ne!(DetachResult::KeptObservation, DetachResult::StoppedObservation);
    assert_ne!(DetachResult::StoppedObservation, DetachResult::UnknownObservation);
}
