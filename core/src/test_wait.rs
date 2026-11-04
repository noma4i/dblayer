use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::task::Wake;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

const SPIN_LIMIT: Duration = Duration::from_secs(10);
const BLOCK_LIMIT: Duration = Duration::from_secs(30);

#[track_caller]
pub(crate) fn recv_within<T>(receiver: &Receiver<T>) -> T {
    receiver.recv_timeout(BLOCK_LIMIT).unwrap_or_else(|error| panic!("nothing arrived: {error}"))
}

#[cfg(feature = "stress")]
#[track_caller]
pub(crate) fn recv_or_closed<T>(receiver: &Receiver<T>) -> Option<T> {
    match receiver.recv_timeout(BLOCK_LIMIT) {
        Ok(value) => Some(value),
        Err(RecvTimeoutError::Disconnected) => None,
        Err(RecvTimeoutError::Timeout) => panic!("nothing arrived and the sender is still alive"),
    }
}

#[track_caller]
pub(crate) fn join_within<T>(handle: JoinHandle<T>) -> T {
    let deadline = Instant::now() + BLOCK_LIMIT;
    while !handle.is_finished() {
        assert!(Instant::now() < deadline, "the thread did not finish");
        std::thread::sleep(Duration::from_millis(1));
    }
    handle.join().unwrap()
}

pub(crate) struct Rendezvous {
    parties: usize,
    state: Mutex<(usize, usize)>,
    ready: Condvar,
}

impl Rendezvous {
    pub(crate) fn new(parties: usize) -> Self {
        Self { parties, state: Mutex::new((0, 0)), ready: Condvar::new() }
    }

    #[track_caller]
    pub(crate) fn meet(&self) {
        let mut state = self.state.lock().unwrap();
        let round = state.1;
        state.0 += 1;
        if state.0 == self.parties {
            *state = (0, round + 1);
            self.ready.notify_all();
            return;
        }
        let deadline = Instant::now() + BLOCK_LIMIT;
        while state.1 == round {
            let left = deadline.saturating_duration_since(Instant::now());
            assert!(!left.is_zero(), "the other parties never met");
            state = self.ready.wait_timeout(state, left).unwrap().0;
        }
    }
}

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

pub(crate) struct ForeignWaker {
    lock: Arc<Mutex<()>>,
    entered: AtomicBool,
    held_classes: Mutex<Vec<&'static str>>,
}

impl ForeignWaker {
    pub(crate) fn new(lock: Arc<Mutex<()>>) -> Arc<Self> {
        Arc::new(Self { lock, entered: AtomicBool::new(false), held_classes: Mutex::new(Vec::new()) })
    }

    pub(crate) fn entered(&self) -> bool {
        self.entered.load(Ordering::SeqCst)
    }

}

impl Wake for ForeignWaker {
    fn wake(self: Arc<Self>) {
        *self.held_classes.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = crate::db::lock::held_classes();
        self.entered.store(true, Ordering::SeqCst);
        drop(self.lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner));
    }
}

pub(crate) struct ProbeWaker {
    unlocked: Box<dyn Fn() -> bool + Send + Sync>,
    wakes: std::sync::atomic::AtomicUsize,
    violations: std::sync::atomic::AtomicUsize,
}

impl ProbeWaker {
    pub(crate) fn new(unlocked: impl Fn() -> bool + Send + Sync + 'static) -> Arc<Self> {
        Arc::new(Self { unlocked: Box::new(unlocked), wakes: Default::default(), violations: Default::default() })
    }

    pub(crate) fn wakes(&self) -> usize {
        self.wakes.load(Ordering::SeqCst)
    }

    pub(crate) fn violations(&self) -> usize {
        self.violations.load(Ordering::SeqCst)
    }
}

impl Wake for ProbeWaker {
    fn wake(self: Arc<Self>) {
        if !crate::db::lock::is_empty() || !(self.unlocked)() { self.violations.fetch_add(1, Ordering::SeqCst); }
        self.wakes.fetch_add(1, Ordering::SeqCst);
    }
}

pub(crate) fn mutex_free<T>(mutex: &Mutex<T>) -> bool {
    !matches!(mutex.try_lock(), Err(std::sync::TryLockError::WouldBlock))
}

struct World {
    clock: crate::pacing::TestClock,
    dbs: Vec<std::sync::Weak<crate::db::Shared>>,
}

thread_local! { static WORLD: std::cell::RefCell<Option<World>> = const { std::cell::RefCell::new(None) }; }

struct Leave;

impl Drop for Leave {
    fn drop(&mut self) { WORLD.with(|world| world.borrow_mut().take()); }
}

pub(crate) fn paused(test: impl std::future::Future<Output = ()>) {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().expect("test runtime");
    let clock = crate::pacing::TestClock::stopped(Duration::from_millis(crate::sync::rig::START_MS));
    WORLD.with(|world| *world.borrow_mut() = Some(World { clock: clock.clone(), dbs: Vec::new() }));
    let _leave = Leave;
    runtime.block_on(async move {
        let driver = tokio::spawn(async move { clock.drive(|| world_dbs().iter().map(crate::db::Db::quiet_mark).sum::<Option<u64>>(), || for db in world_dbs() { db.await_quiet(); }).await });
        test.await;
        driver.abort();
    });
}

fn world_dbs() -> Vec<crate::db::Db> {
    WORLD.with(|world| world.borrow().as_ref().map(|world| world.dbs.iter().filter_map(std::sync::Weak::upgrade).map(|shared| crate::db::Db { shared }).collect()).unwrap_or_default())
}

pub(crate) fn clock(start: Duration) -> crate::pacing::TestClock {
    world_clock().map_or_else(|| crate::pacing::TestClock::new(start), |world| world.linked(start))
}

pub(crate) fn world_clock() -> Option<crate::pacing::TestClock> {
    WORLD.with(|world| world.borrow().as_ref().map(|world| world.clock.clone()))
}

pub(crate) fn enter_world(db: &crate::db::Db) {
    WORLD.with(|world| if let Some(world) = world.borrow_mut().as_mut() { world.dbs.push(std::sync::Arc::downgrade(&db.shared)); });
}

pub(crate) async fn pass(duration: Duration) {
    match world_clock() {
        Some(clock) => crate::pacing::Clock::sleep(&clock, duration).await,
        None => tokio::time::sleep(duration).await,
    }
}

pub(crate) async fn timeout<T>(duration: Duration, work: impl std::future::Future<Output = T>) -> Result<T, ()> {
    tokio::select! {
        biased;
        output = work => Ok(output),
        () = pass(duration) => Err(()),
    }
}
