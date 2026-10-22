use std::sync::atomic::Ordering;
use std::time::Duration;

use super::viewer_changes_tests::open_viewer;
use super::TestClock;

#[tokio::test]
async fn a_stale_window_is_requested_once_for_loads_that_overlap_or_follow_each_other() {
    let clock = TestClock::new(1_000_000);
    let (database, transport) = open_viewer(Duration::from_millis(40), Some(clock.clone())).await;
    let load = || database.load("Moment".into(), "feed".into(), "{}".into());
    load().await.unwrap();
    assert_eq!(transport.queries.load(Ordering::SeqCst), 1);

    clock.advance(660_000);
    let (first, second) = tokio::join!(load(), load());
    first.unwrap();
    second.unwrap();
    assert_eq!(transport.queries.load(Ordering::SeqCst), 2, "overlapping loads of one stale window made separate requests");

    load().await.unwrap();
    assert_eq!(transport.queries.load(Ordering::SeqCst), 2, "a load right after the landing requested again");

    database.refresh("Moment".into(), "feed".into(), "{}".into()).await.unwrap();
    assert_eq!(transport.queries.load(Ordering::SeqCst), 3, "refresh must always request");
}
