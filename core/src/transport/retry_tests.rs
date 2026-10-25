use super::*;

#[test]
fn retry_class_follows_code() {
    assert_eq!(RetryClass::classify(true, None, &[]), RetryClass::Network);
    assert_eq!(RetryClass::classify(false, Some(503), &[]), RetryClass::Server);
    assert_eq!(RetryClass::classify(false, Some(400), &["TIMEOUT"]), RetryClass::Retriable);
    assert_eq!(RetryClass::classify(false, Some(400), &[]), RetryClass::Fatal);
    assert_eq!(RetryClass::classify(false, Some(500), &[]), RetryClass::Server);
    assert_eq!(RetryClass::classify(false, Some(599), &[]), RetryClass::Server);
    assert_eq!(RetryClass::classify(false, Some(600), &[]), RetryClass::Fatal);
    assert_eq!(RetryClass::classify(false, Some(499), &[]), RetryClass::Fatal);
    assert_eq!(RetryClass::classify(false, None, &[""]), RetryClass::Fatal);
}

#[test]
fn transport_error_classifies_by_variant() {
    assert_eq!(RetryClass::of_error(&TransportError::Network("x".into())), RetryClass::Network);
    assert_eq!(
        RetryClass::of_error(&TransportError::Http { status: 502, body: String::new() }),
        RetryClass::Server
    );
    assert_eq!(RetryClass::of_error(&TransportError::Decode("x".into())), RetryClass::Fatal);
    assert_eq!(RetryClass::of_error(&TransportError::Graphql { messages: vec!["x".into()], extensions: None }), RetryClass::Fatal);
}

#[test]
fn encrypted_temporary_codes_and_http_throttling_are_retriable() {
    for status in [408, 429] {
        assert_eq!(RetryClass::of_error(&TransportError::Http { status, body: String::new() }), RetryClass::Retriable);
    }
    for code in ["TIMEOUT", "SERVICE_UNAVAILABLE", "INTERNAL_SERVER_ERROR", "DATABASE_ERROR"] {
        assert_eq!(RetryClass::of_error(&TransportError::refusal(code)), RetryClass::Retriable, "{code}");
    }
}

#[tokio::test]
async fn retry_progress_reports_unlimited_attempts_and_capped_jittered_sleeps() {
    use std::{sync::Mutex, time::Duration};
    use super::retry::{RetryCalls, Retrying};
    #[derive(Default)]
    struct Time(Mutex<Vec<Duration>>);
    impl crate::pacing::Clock for Time {
        fn now(&self) -> Duration { Duration::ZERO }
        fn sleep(&self, duration: Duration) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
            self.0.lock().unwrap().push(duration);
            Box::pin(std::future::ready(()))
        }
    }
    let clock = Time::default();
    let progress = RetryCalls::default();
    let online = Connectivity::new(true);
    let mut retry = Retrying::new(&clock, &progress, &online, crate::pacing::RetryPolicy::Http);
    assert!(clock.0.lock().unwrap().is_empty());
    for index in 0..40 {
        retry.failed(TransportError::Network("timeout".into())).await.unwrap();
        let ceiling = [1000, 2000, 4000, 8000, 16000].get(index).copied().unwrap_or(30000);
        let snapshot = progress.current();
        assert_eq!(snapshot.active_calls, 1);
        let RetryAttempt { attempt, delay_ms: delay, reason } = snapshot.last_attempt.unwrap();
        assert_eq!(attempt, index as u64 + 1);
        assert!((ceiling * 8 / 10..=ceiling).contains(&delay), "attempt {attempt}: {delay} outside jittered ceiling {ceiling}");
        assert_eq!(reason, "network");
        assert_eq!(clock.0.lock().unwrap()[index].as_millis() as u64, delay);
    }
    retry.failed(TransportError::Http { status: 429, body: String::new() }).await.unwrap();
    assert_eq!(progress.current().last_attempt.unwrap().reason, "HTTP 429");
    assert_eq!(clock.0.lock().unwrap().len(), 41);
}

#[tokio::test]
async fn dropping_backoff_drops_the_clock_sleep() {
    use std::{future::Future, sync::{Arc, atomic::{AtomicBool, Ordering}}, task::{Context, Waker}, time::Duration};
    use super::retry::{RetryCalls, Retrying};
    struct Sleeping(Arc<AtomicBool>);
    impl Future for Sleeping {
        type Output = ();
        fn poll(self: std::pin::Pin<&mut Self>, _: &mut Context<'_>) -> std::task::Poll<()> { std::task::Poll::Pending }
    }
    impl Drop for Sleeping { fn drop(&mut self) { self.0.store(true, Ordering::SeqCst); } }
    struct Time(Arc<AtomicBool>);
    impl crate::pacing::Clock for Time {
        fn now(&self) -> Duration { Duration::ZERO }
        fn sleep(&self, _: Duration) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send + '_>> { Box::pin(Sleeping(self.0.clone())) }
    }
    let dropped = Arc::new(AtomicBool::new(false));
    let clock = Time(dropped.clone());
    let progress = RetryCalls::default();
    let online = Connectivity::new(true);
    let mut retry = Retrying::new(&clock, &progress, &online, crate::pacing::RetryPolicy::Http);
    let mut wait = Box::pin(retry.failed(TransportError::Network("offline".into())));
    assert!(wait.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
    assert!(!dropped.load(Ordering::SeqCst));
    drop(wait);
    assert!(dropped.load(Ordering::SeqCst));
}

struct Instant;

impl crate::pacing::Clock for Instant {
    fn now(&self) -> std::time::Duration { std::time::Duration::ZERO }
    fn sleep(&self, _: std::time::Duration) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> { Box::pin(std::future::ready(())) }
}

async fn within<T>(wait: impl std::future::Future<Output = T>) -> T {
    crate::test_wait::within_secs(2, wait).await
}

#[tokio::test]
async fn retry_progress_releases_a_call_on_success_terminal_failure_and_cancel_and_keeps_overlapping_calls_active() {
    use super::retry::{RetryCalls, Retrying};
    use std::{future::Future, task::{Context, Poll, Waker}};
    let clock = crate::pacing::ManualClock::new(std::time::Duration::ZERO);
    let online = Connectivity::new(true);
    let progress = RetryCalls::default();
    let waiting = || TransportError::Network("offline".into());
    let poll = |wait: &mut std::pin::Pin<Box<dyn Future<Output = Result<(), TransportError>> + '_>>| wait.as_mut().poll(&mut Context::from_waker(Waker::noop()));

    let mut first = Retrying::new(&clock, &progress, &online, crate::pacing::RetryPolicy::Http);
    let mut second = Retrying::new(&clock, &progress, &online, crate::pacing::RetryPolicy::Http);
    let mut first_wait: std::pin::Pin<Box<dyn Future<Output = _> + '_>> = Box::pin(first.failed(waiting()));
    let mut second_wait: std::pin::Pin<Box<dyn Future<Output = _> + '_>> = Box::pin(second.failed(waiting()));
    assert!(poll(&mut first_wait).is_pending() && poll(&mut second_wait).is_pending());
    assert_eq!(progress.current().active_calls, 2);

    drop(first_wait);
    drop(first);
    assert_eq!(progress.current().active_calls, 1, "a cancelled call leaves the other one active");
    assert!(progress.current().last_attempt.is_some());

    clock.advance(std::time::Duration::from_secs(60));
    assert!(matches!(poll(&mut second_wait), Poll::Ready(Ok(()))));
    drop(second_wait);
    assert!(matches!(within(second.failed(TransportError::Decode("fatal".into()))).await, Err(TransportError::Decode(_))));
    assert_eq!(progress.current(), RetrySnapshot::default(), "a terminal failure of the last call is idle with no attempt");

    let instant = Instant;
    let mut finished = Retrying::new(&instant, &progress, &online, crate::pacing::RetryPolicy::Http);
    within(finished.failed(waiting())).await.unwrap();
    assert_eq!(progress.current().active_calls, 1);
    drop(finished);
    assert_eq!(progress.current(), RetrySnapshot::default(), "a successful call leaves idle");
}

#[tokio::test]
async fn a_late_watcher_reads_the_current_snapshot_and_a_dropped_watcher_leaves_the_others() {
    use super::retry::{RetryCalls, Retrying};
    let clock = Instant;
    let online = Connectivity::new(true);
    let progress = RetryCalls::default();
    let mut retry = Retrying::new(&clock, &progress, &online, crate::pacing::RetryPolicy::Http);
    let early = progress.watch();
    within(retry.failed(TransportError::Network("offline".into()))).await.unwrap();

    let mut late = progress.watch();
    assert_eq!(late.borrow_and_update().active_calls, 1);
    drop(early);
    drop(retry);
    within(late.changed()).await.unwrap();
    assert_eq!(*late.borrow(), RetrySnapshot::default());
}
