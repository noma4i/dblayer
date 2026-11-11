use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub trait Clock: Send + Sync {
    fn now(&self) -> Duration;
    fn sleep_until(&self, deadline: Duration) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>>;
    fn on_advance(&self, _wake: Arc<dyn Fn() + Send + Sync>) {}
    fn armed(&self, _count: u64) {}
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Duration {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default()
    }

    fn sleep_until(&self, deadline: Duration) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(tokio::time::sleep(deadline.saturating_sub(self.now())))
    }
}

type Waker = Arc<dyn Fn() + Send + Sync>;

#[derive(Clone, Default)]
pub struct ManualClock {
    pub(crate) now: Arc<Mutex<Duration>>,
    changed: Arc<tokio::sync::Notify>,
    wakers: Arc<Mutex<Vec<Waker>>>,
    armed_total: Arc<std::sync::atomic::AtomicU64>,
    armed_changed: Arc<tokio::sync::Notify>,
}

impl std::fmt::Debug for ManualClock {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("ManualClock").field("now", &self.now()).finish_non_exhaustive()
    }
}

impl ManualClock {
    pub fn new(start: Duration) -> Self {
        Self { now: Arc::new(Mutex::new(start)), changed: Arc::default(), wakers: Arc::default(), armed_total: Arc::default(), armed_changed: Arc::default() }
    }

    pub fn advance(&self, by: Duration) {
        *crate::lock::lock(&self.now) += by;
        crate::lock::effect(|| self.changed.notify_waiters());
        self.wake(); // lock-ok: clock guard ended at statement
    }

    pub fn armed_total(&self) -> u64 { self.armed_total.load(std::sync::atomic::Ordering::SeqCst) }

    pub async fn wait_armed(&self, after: u64) {
        loop {
            let changed = self.armed_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.armed_total() > after { return; }
            changed.await;
        }
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

    fn armed(&self, count: u64) {
        self.armed_total.fetch_add(count, std::sync::atomic::Ordering::SeqCst);
        crate::lock::effect(|| self.armed_changed.notify_waiters());
    }

    fn sleep_until(&self, deadline: Duration) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
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

