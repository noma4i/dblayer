use super::*;
use crate::store::InvalidationRequest;

fn at(latitude: f64, longitude: f64) -> Coordinate {
    Coordinate { latitude, longitude }
}

#[test]
fn distance_matches_geography() {
    let first = at(55.75, 37.62);
    let second = at(55.76, 37.62);
    let distance = first.distance_meters(second);
    assert!(first.distance_meters(first).abs() < 0.0001);
    assert!((1000.0..=1200.0).contains(&distance), "{distance}");
}

#[test]
fn missing_baseline_and_large_move_move_the_feed_while_small_move_keeps_it() {
    let baseline = at(55.75, 37.62);
    let small = at(55.7501, 37.62);
    let large = at(55.76, 37.62);
    assert!(moves_feed(None, baseline));
    assert_eq!(feed_next_baseline(None, baseline), baseline);
    assert!(!moves_feed(Some(baseline), small));
    assert_eq!(feed_next_baseline(Some(baseline), small), baseline);
    assert!(moves_feed(Some(baseline), large));
    assert_eq!(feed_next_baseline(Some(baseline), large), large);
}

#[test]
fn threshold_is_inclusive() {
    let baseline = at(0.0, 0.0);
    let exact = at(0.0, 500.0 / 111_194.926_644_558_73);
    assert!(baseline.distance_meters(exact) >= MOVE_THRESHOLD_METERS - 0.001);
    assert!(moves_feed(Some(baseline), at(0.0, 0.01)));
    assert!(!moves_feed(Some(baseline), at(0.0, 0.0005)));
}

#[test]
fn invalidation_names_the_feed_family_only_after_a_far_move() {
    let baseline = at(0.0, 0.0);
    let close = at(0.0, 0.0005);
    let far = at(0.0, 0.01);
    assert!(feed_invalidation(Some(baseline), close).is_empty());
    assert_eq!(feed_invalidation(Some(baseline), far), vec![InvalidationRequest::family(FEED_MODEL, FEED_RELATION)]);
    assert_eq!(feed_invalidation(None, close), vec![InvalidationRequest::family("moments", "feed")]);
}
