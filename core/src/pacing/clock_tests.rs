use super::*;
use std::time::Duration;

#[test]
fn manual_clock_advances_and_sets() {
    let clock = ManualClock::new(Duration::from_secs(10));
    assert_eq!(clock.now(), Duration::from_secs(10));
    clock.advance(Duration::from_secs(5));
    assert_eq!(clock.now(), Duration::from_secs(15));
    clock.set(Duration::from_secs(1));
    assert_eq!(clock.now(), Duration::from_secs(1));
}

#[tokio::test(start_paused = true)]
async fn tokio_clock_follows_paused_time() {
    let clock = TokioClock::new();
    assert_eq!(clock.now(), Duration::ZERO);
    tokio::time::advance(Duration::from_secs(42)).await;
    assert_eq!(clock.now(), Duration::from_secs(42));
}

#[test]
fn system_clock_reports_time_since_epoch() {
    assert!(SystemClock.now() > Duration::from_secs(1_700_000_000));
}

#[test]
fn clocks_work_as_dyn_clock() {
    let clock: std::sync::Arc<dyn Clock> = std::sync::Arc::new(ManualClock::new(Duration::from_secs(3)));
    assert_eq!(clock.now(), Duration::from_secs(3));
}
