use super::*;

#[test]
fn retry_class_follows_code_and_budgets_limit_retries() {
    assert_eq!(RetryClass::classify(true, None, &[]), RetryClass::Network);
    assert_eq!(RetryClass::classify(false, Some(503), &[]), RetryClass::Server);
    assert_eq!(RetryClass::classify(false, Some(400), &["TIMEOUT"]), RetryClass::Retriable);
    assert_eq!(RetryClass::classify(false, Some(400), &[]), RetryClass::Fatal);
    assert_eq!(RetryClass::classify(false, Some(500), &[]), RetryClass::Server);
    assert_eq!(RetryClass::classify(false, Some(599), &[]), RetryClass::Server);
    assert_eq!(RetryClass::classify(false, Some(600), &[]), RetryClass::Fatal);
    assert_eq!(RetryClass::classify(false, Some(499), &[]), RetryClass::Fatal);
    assert_eq!(RetryClass::classify(false, None, &[""]), RetryClass::Fatal);
    assert!(RetryClass::Network.allows_retry(2));
    assert!(!RetryClass::Network.allows_retry(3));
    assert!(!RetryClass::Fatal.allows_retry(0));
    assert_eq!(RetryClass::Network.budget(), 3);
    assert_eq!(RetryClass::Server.budget(), 2);
    assert_eq!(RetryClass::Retriable.budget(), 2);
    assert_eq!(RetryClass::Fatal.budget(), 0);
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
async fn retry_listener_reports_unlimited_attempts_and_capped_jittered_sleeps() {
    use std::{sync::{Arc, Mutex}, time::Duration};
    use super::retry::{RetryEvents, Retrying};
    #[derive(Default)]
    struct Tape(Mutex<Vec<(u64, u64, String)>>);
    impl RetryListener for Tape {
        fn retrying(&self, attempt: u64, delay_ms: u64, reason: String) { self.0.lock().unwrap().push((attempt, delay_ms, reason)); }
    }
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
    let events = RetryEvents::default();
    let tape = Arc::new(Tape::default());
    events.set(Some(tape.clone()));
    let mut retry = Retrying::new(&clock, &events);
    assert!(clock.0.lock().unwrap().is_empty());
    for index in 0..40 {
        retry.failed(TransportError::Network("timeout".into())).await.unwrap();
        let ceiling = [1000, 2000, 4000, 8000, 16000].get(index).copied().unwrap_or(30000);
        let tape = tape.0.lock().unwrap();
        let (attempt, delay, reason) = &tape[index];
        assert_eq!(*attempt, index as u64 + 1);
        assert!((ceiling * 8 / 10..=ceiling).contains(delay), "attempt {attempt}: {delay} outside jittered ceiling {ceiling}");
        assert_eq!(reason, "network");
        assert_eq!(clock.0.lock().unwrap()[index].as_millis() as u64, *delay);
    }
    events.set(None);
    retry.failed(TransportError::Http { status: 429, body: String::new() }).await.unwrap();
    assert_eq!(tape.0.lock().unwrap().len(), 40);
    assert_eq!(clock.0.lock().unwrap().len(), 41);
}

#[tokio::test]
async fn dropping_backoff_drops_the_clock_sleep() {
    use std::{future::Future, sync::{Arc, atomic::{AtomicBool, Ordering}}, task::{Context, Waker}, time::Duration};
    use super::retry::{RetryEvents, Retrying};
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
    let events = RetryEvents::default();
    let mut retry = Retrying::new(&clock, &events);
    let mut wait = Box::pin(retry.failed(TransportError::Network("offline".into())));
    assert!(wait.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
    assert!(!dropped.load(Ordering::SeqCst));
    drop(wait);
    assert!(dropped.load(Ordering::SeqCst));
}
