use std::sync::mpsc::RecvTimeoutError;
use std::time::{Duration, Instant};

const SPIN_LIMIT: Duration = Duration::from_secs(10);

/// The limit runs on the wall clock: a paused test clock advances the moment the runtime idles, which would expire it before a real thread answers.
pub(crate) async fn within<T>(work: impl std::future::Future<Output = T>) -> T {
    within_secs(120, work).await
}

pub(crate) async fn within_secs<T>(seconds: u64, work: impl std::future::Future<Output = T>) -> T {
    let (expired, expiry) = tokio::sync::oneshot::channel::<()>();
    let (finished, watching) = std::sync::mpsc::channel::<()>();
    std::thread::spawn(move || {
        if watching.recv_timeout(Duration::from_secs(seconds)) == Err(RecvTimeoutError::Timeout) {
            let _ = expired.send(());
        }
    });
    let _finished = finished;
    tokio::select! {
        output = work => output,
        _ = expiry => panic!("bounded wait expired"),
    }
}

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
