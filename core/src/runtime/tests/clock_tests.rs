use super::*;
use std::time::Duration;

#[tokio::test]
async fn audit_manual_clock_wakes_sleep_without_tokio_time() {
    use std::{sync::{Arc, atomic::{AtomicBool, Ordering}}, task::{Context, Wake, Waker}};
    struct Awake(AtomicBool);
    impl Wake for Awake {
        fn wake(self: Arc<Self>) { self.0.store(true, Ordering::SeqCst); }
    }
    let awake = Arc::new(Awake(AtomicBool::new(false)));
    let waker = Waker::from(awake.clone());
    let clock = ManualClock::default();
    let mut sleep = clock.sleep_until(Duration::from_secs(10));
    assert!(sleep.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
    clock.advance(Duration::from_secs(10));
    assert!(awake.0.load(Ordering::SeqCst));
    assert!(sleep.as_mut().poll(&mut Context::from_waker(&waker)).is_ready());
}

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
fn inv_panic_01_a_manual_clock_survives_a_poisoned_lock() {
    let clock = ManualClock::new(Duration::from_secs(10));
    crate::store::fixtures::poison(&clock.now);
    clock.advance(Duration::from_secs(5));
    clock.set(Duration::from_secs(20));
    assert_eq!(clock.now(), Duration::from_secs(20));
}
