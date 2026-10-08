use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub trait Clock: Send + Sync {
    fn now(&self) -> Duration;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Duration {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TokioClock {
    start: tokio::time::Instant,
}

impl TokioClock {
    pub fn new() -> Self {
        Self { start: tokio::time::Instant::now() }
    }
}

impl Default for TokioClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for TokioClock {
    fn now(&self) -> Duration {
        self.start.elapsed()
    }
}

#[derive(Debug, Clone, Default)]
pub struct ManualClock {
    now: Arc<Mutex<Duration>>,
}

impl ManualClock {
    pub fn new(start: Duration) -> Self {
        Self { now: Arc::new(Mutex::new(start)) }
    }

    pub fn advance(&self, by: Duration) {
        *self.now.lock().expect("manual clock") += by;
    }

    pub fn set(&self, now: Duration) {
        *self.now.lock().expect("manual clock") = now;
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Duration {
        *self.now.lock().expect("manual clock")
    }
}
