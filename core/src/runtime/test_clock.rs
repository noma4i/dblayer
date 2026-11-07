use super::Clock;
use std::time::Duration;

#[derive(Clone)]
pub(crate) struct TestClock {
    started: tokio::time::Instant,
    start: Duration,
}

impl TestClock {
    pub(crate) fn new(start: Duration) -> Self {
        Self { started: tokio::time::Instant::now(), start }
    }
}

impl Default for TestClock {
    fn default() -> Self { Self::new(Duration::ZERO) }
}

impl Clock for TestClock {
    fn now(&self) -> Duration {
        self.start + self.started.elapsed()
    }

    fn sleep(&self, duration: Duration) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(tokio::time::sleep(duration))
    }
}
