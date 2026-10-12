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

#[test]
fn system_clock_reports_time_since_epoch() {
    assert!(SystemClock.now() > Duration::from_secs(1_700_000_000));
}

#[test]
fn clocks_work_as_dyn_clock() {
    let clock: std::sync::Arc<dyn Clock> = std::sync::Arc::new(ManualClock::new(Duration::from_secs(3)));
    assert_eq!(clock.now(), Duration::from_secs(3));
}

#[test]
fn inv_panic_01_a_manual_clock_survives_a_poisoned_lock() {
    let clock = ManualClock::new(Duration::from_secs(10));
    crate::db::fixtures::poison(&clock.now);
    clock.advance(Duration::from_secs(5));
    clock.set(Duration::from_secs(20));
    assert_eq!(clock.now(), Duration::from_secs(20));
}
