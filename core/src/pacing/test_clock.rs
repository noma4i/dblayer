use super::Clock;
use std::time::Duration;

#[derive(Clone)]
pub(crate) struct TestClock {
    time: std::sync::Arc<std::sync::Mutex<(Duration, tokio::time::Instant)>>,
    changed: std::sync::Arc<tokio::sync::Notify>,
}

impl TestClock {
    pub(crate) fn new(start: Duration) -> Self {
        Self { time: std::sync::Arc::new(std::sync::Mutex::new((start, tokio::time::Instant::now()))), changed: std::sync::Arc::default() }
    }

    pub(crate) fn advance(&self, by: Duration) {
        crate::db::locked(&self.time).0 += by;
        crate::db::lock::effect(|| self.changed.notify_waiters());
    }

    #[cfg(test)]
    pub(crate) fn set(&self, now: Duration) {
        *crate::db::locked(&self.time) = (now, tokio::time::Instant::now());
        crate::db::lock::effect(|| self.changed.notify_waiters());
    }
}

impl Default for TestClock {
    fn default() -> Self { Self::new(Duration::ZERO) }
}

impl Clock for TestClock {
    fn now(&self) -> Duration {
        let (start, instant) = *crate::db::locked(&self.time);
        start + instant.elapsed()
    }

    fn sleep(&self, duration: Duration) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        let deadline = self.now() + duration;
        Box::pin(async move {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                let now = self.now();
                if now >= deadline { return; }
                tokio::select! {
                    () = tokio::time::sleep(deadline - now) => return,
                    () = changed => {}
                }
            }
        })
    }
}
