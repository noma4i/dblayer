use std::time::{Duration, Instant};

const SPIN_LIMIT: Duration = Duration::from_secs(10);

pub(crate) async fn within<T>(work: impl std::future::Future<Output = T>) -> T {
    within_secs(600, work).await
}

pub(crate) async fn within_secs<T>(seconds: u64, work: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(seconds), work).await.expect("bounded wait expired")
}

/// The wait of a mock for its caller: the test bounds the caller, so this wait has no bound of its own.
pub(crate) async fn serving<T>(work: impl std::future::Future<Output = T>) -> T {
    work.await
}

pub(crate) fn spin_until(what: &str, mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + SPIN_LIMIT;
    while !ready() {
        assert!(Instant::now() < deadline, "{what}");
        std::thread::yield_now();
    }
}

pub(crate) async fn yield_until(what: &str, mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + SPIN_LIMIT;
    while !ready() {
        assert!(Instant::now() < deadline, "{what}");
        tokio::task::yield_now().await;
    }
}
