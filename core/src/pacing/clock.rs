use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub trait Clock: Send + Sync {
    fn now(&self) -> Duration;
    fn sleep(&self, duration: Duration) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(tokio::time::sleep(duration))
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Duration {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default()
    }
}

#[derive(Debug, Clone, Default)]
pub struct ManualClock {
    pub(crate) now: Arc<Mutex<Duration>>,
}

impl ManualClock {
    pub fn new(start: Duration) -> Self {
        Self { now: Arc::new(Mutex::new(start)) }
    }

    pub fn advance(&self, by: Duration) {
        *crate::db::locked(&self.now) += by;
    }

    #[cfg(test)]
    pub fn set(&self, now: Duration) {
        *crate::db::locked(&self.now) = now;
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Duration {
        *crate::db::locked(&self.now)
    }
}
