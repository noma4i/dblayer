use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub trait Clock: Send + Sync {
    fn now(&self) -> Duration;
    fn sleep(&self, duration: Duration) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>>;
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

#[derive(Debug, Clone, Default)]
pub struct ManualClock {
    pub(crate) now: Arc<Mutex<Duration>>,
    changed: Arc<tokio::sync::Notify>,
}

impl ManualClock {
    pub fn new(start: Duration) -> Self {
        Self { now: Arc::new(Mutex::new(start)), changed: Arc::default() }
    }

    pub fn advance(&self, by: Duration) {
        *crate::db::locked(&self.now) += by;
        self.changed.notify_waiters();
    }

    #[cfg(test)]
    pub fn set(&self, now: Duration) {
        *crate::db::locked(&self.now) = now;
        self.changed.notify_waiters();
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Duration {
        *crate::db::locked(&self.now)
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

pub(crate) async fn timeout<T>(clock: &dyn Clock, duration: Duration, call: impl std::future::Future<Output = T>) -> Result<T, ()> {
    tokio::select! {
        biased;
        result = call => Ok(result),
        () = clock.sleep(duration) => Err(()),
    }
}
