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
fn missing_baseline_and_large_move_move_the_feed_while_small_move_keeps_it_and_the_far_move_names_the_feed_family() {
    let baseline = at(55.75, 37.62);
    let small = at(55.7501, 37.62);
    let large = at(55.76, 37.62);
    assert!(moves_feed(None, baseline));
    assert_eq!(feed_next_baseline(None, baseline), baseline);
    assert!(!moves_feed(Some(baseline), small));
    assert_eq!(feed_next_baseline(Some(baseline), small), baseline);
    assert!(moves_feed(Some(baseline), large));
    assert_eq!(feed_next_baseline(Some(baseline), large), large);

    let origin = at(0.0, 0.0);
    let exact = at(0.0, 500.0 / 111_194.926_644_558_73);
    assert!(origin.distance_meters(exact) >= MOVE_THRESHOLD_METERS - 0.001, "threshold distance");
    assert!(moves_feed(Some(origin), at(0.0, 0.01)));
    assert!(!moves_feed(Some(origin), at(0.0, 0.0005)));

    assert!(feed_invalidation(Some(origin), at(0.0, 0.0005)).is_empty(), "close move");
    assert_eq!(feed_invalidation(Some(origin), at(0.0, 0.01)), vec![InvalidationRequest::family(FEED_MODEL, FEED_RELATION)], "far move");
    assert_eq!(feed_invalidation(None, at(0.0, 0.0005)), vec![InvalidationRequest::family("moments", "feed")], "no baseline");
}
