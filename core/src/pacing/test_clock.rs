use super::Clock;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

struct Time {
    now: Duration,
    started: Option<tokio::time::Instant>,
    sleepers: BTreeMap<Duration, usize>,
}

impl Time {
    fn base(&self) -> Duration {
        self.now + self.started.map_or(Duration::ZERO, |started| started.elapsed())
    }
}

#[derive(Clone)]
pub(crate) struct TestClock {
    time: Arc<Mutex<Time>>,
    shift: Arc<Mutex<Duration>>,
    changed: Arc<tokio::sync::Notify>,
    slept: Arc<tokio::sync::Notify>,
}

struct Sleeper<'a> {
    clock: &'a TestClock,
    deadline: Duration,
}

impl Drop for Sleeper<'_> {
    fn drop(&mut self) {
        let mut time = crate::db::locked(&self.clock.time);
        if let Some(count) = time.sleepers.get_mut(&self.deadline) {
            *count -= 1;
            if *count == 0 { time.sleepers.remove(&self.deadline); }
        }
    }
}

impl TestClock {
    pub(crate) fn new(start: Duration) -> Self {
        Self::with(start, Some(tokio::time::Instant::now()))
    }

    pub(crate) fn stopped(start: Duration) -> Self {
        Self::with(start, None)
    }

    fn with(start: Duration, started: Option<tokio::time::Instant>) -> Self {
        Self {
            time: Arc::new(Mutex::new(Time { now: Duration::ZERO, started, sleepers: BTreeMap::new() })),
            shift: Arc::new(Mutex::new(start)),
            changed: Arc::default(),
            slept: Arc::default(),
        }
    }


    fn base(&self) -> Duration {
        crate::db::locked(&self.time).base()
    }



    #[cfg(test)]
    fn advance_base(&self, by: Duration) {
        crate::db::locked(&self.time).now += by;
        crate::db::lock::effect(|| self.changed.notify_waiters());
    }

    #[cfg(test)]
    fn next_deadline(&self) -> Option<Duration> {
        crate::db::locked(&self.time).sleepers.keys().next().copied()
    }

    #[cfg(test)]
    pub(crate) async fn idle(&self, quiet: &impl Fn() -> Option<u64>, settle: &impl Fn()) {
        loop {
            tokio::time::sleep(Duration::from_millis(1)).await;
            let Some(mark) = quiet() else {
                settle();
                continue;
            };
            tokio::time::sleep(Duration::from_millis(1)).await;
            if quiet() == Some(mark) {
                return;
            }
        }
    }

    /// The only mover of a stopped clock: time jumps to the nearest sleeper only when `quiet` gives the same work mark before and after one whole idle turn of the runtime, so tasks woken by library threads run before the jump.
    #[cfg(test)]
    pub(crate) async fn drive(&self, quiet: impl Fn() -> Option<u64>, settle: impl Fn()) {
        loop {
            self.idle(&quiet, &settle).await;
            let added = self.slept.notified();
            tokio::pin!(added);
            added.as_mut().enable();
            match self.next_deadline() {
                Some(deadline) => self.advance_base(deadline.saturating_sub(self.base())),
                None => added.await,
            }
        }
    }

    fn sleeper(&self, deadline: Duration) -> Option<Sleeper<'_>> {
        let mut time = crate::db::locked(&self.time);
        if time.started.is_some() { return None; }
        *time.sleepers.entry(deadline).or_default() += 1;
        drop(time);
        crate::db::lock::effect(|| self.slept.notify_waiters());
        Some(Sleeper { clock: self, deadline })
    }

    fn stopped_deadline(&self, deadline: Duration) -> Duration {
        deadline.saturating_sub(*crate::db::locked(&self.shift))
    }
}

impl Default for TestClock {
    fn default() -> Self { Self::new(Duration::ZERO) }
}

impl Clock for TestClock {
    fn now(&self) -> Duration {
        self.base() + *crate::db::locked(&self.shift)
    }

    fn sleep(&self, duration: Duration) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        let deadline = self.now() + duration;
        Box::pin(async move {
            let mut stopped = None;
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                let now = self.now();
                if now >= deadline { return; }
                let at = self.stopped_deadline(deadline);
                if stopped.as_ref().is_none_or(|sleeper: &Sleeper<'_>| sleeper.deadline != at) {
                    stopped = self.sleeper(at);
                }
                if stopped.is_some() {
                    changed.await;
                    continue;
                }
                tokio::select! {
                    () = tokio::time::sleep(deadline - now) => return,
                    () = changed => {}
                }
            }
        })
    }
}
