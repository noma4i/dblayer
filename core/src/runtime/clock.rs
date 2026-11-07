use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub trait Clock: Send + Sync {
    fn now(&self) -> Duration;
    fn sleep(&self, duration: Duration) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>>;
    fn on_advance(&self, _wake: Arc<dyn Fn() + Send + Sync>) {}
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Duration {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default()
    }

    fn sleep(&self, duration: Duration) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(tokio::time::sleep(duration))
    }
}

type Waker = Arc<dyn Fn() + Send + Sync>;

#[derive(Clone, Default)]
pub struct ManualClock {
    pub(crate) now: Arc<Mutex<Duration>>,
    changed: Arc<tokio::sync::Notify>,
    wakers: Arc<Mutex<Vec<Waker>>>,
}

impl std::fmt::Debug for ManualClock {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("ManualClock").field("now", &self.now()).finish_non_exhaustive()
    }
}

impl ManualClock {
    pub fn new(start: Duration) -> Self {
        Self { now: Arc::new(Mutex::new(start)), changed: Arc::default(), wakers: Arc::default() }
    }

    pub fn advance(&self, by: Duration) {
        *crate::lock::lock(&self.now) += by;
        crate::lock::effect(|| self.changed.notify_waiters());
        self.wake(); // lock-ok: clock guard ended at statement
    }

    fn wake(&self) {
        let wakers = crate::lock::lock(&self.wakers).clone();
        for wake in wakers { wake(); }
    }

    #[cfg(test)]
    pub fn set(&self, now: Duration) {
        *crate::lock::lock(&self.now) = now;
        crate::lock::effect(|| self.changed.notify_waiters());
        self.wake(); // lock-ok: clock guard ended at statement
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Duration {
        *crate::lock::lock(&self.now)
    }

    fn on_advance(&self, wake: Waker) {
        crate::lock::lock(&self.wakers).push(wake);
    }

    fn sleep(&self, duration: Duration) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        let deadline = self.now().saturating_add(duration);
        Box::pin(async move {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.now() >= deadline { return; }
                changed.await;
            }
        })
    }
}

