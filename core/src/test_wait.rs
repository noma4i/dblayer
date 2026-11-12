use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::{Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

pub(crate) fn package_dir() -> std::path::PathBuf {
    std::env::current_dir().expect("the test has a working directory")
}

const TEST_DEADLINE: u64 = 20;

static WAITING: Mutex<Vec<(u64, String)>> = Mutex::new(Vec::new());
static WAIT_IDS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static SETTLED: Mutex<Vec<(u64, std::sync::Weak<crate::owners::life::Life>, &'static str)>> = Mutex::new(Vec::new());

pub(crate) struct SettledPhase(u64);

impl SettledPhase {
    pub(crate) fn enter(life: &std::sync::Arc<crate::owners::life::Life>, phase: &'static str) -> Self {
        let id = WAIT_IDS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        SETTLED.lock().unwrap_or_else(std::sync::PoisonError::into_inner).push((id, std::sync::Arc::downgrade(life), phase));
        Self(id)
    }
}

impl Drop for SettledPhase {
    fn drop(&mut self) { SETTLED.lock().unwrap_or_else(std::sync::PoisonError::into_inner).retain(|(id, _, _)| *id != self.0); }
}

struct Waiting(u64);

impl Waiting {
    fn enter(what: &str) -> Self {
        let id = WAIT_IDS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        WAITING.lock().unwrap_or_else(std::sync::PoisonError::into_inner).push((id, what.to_string()));
        Self(id)
    }
}

impl Drop for Waiting {
    fn drop(&mut self) { WAITING.lock().unwrap_or_else(std::sync::PoisonError::into_inner).retain(|(id, _)| *id != self.0); }
}

pub(crate) fn hang_report() -> String {
    let waits: Vec<String> = WAITING.lock().unwrap_or_else(std::sync::PoisonError::into_inner).iter().map(|(_, what)| what.clone()).collect();
    let settled: Vec<String> = SETTLED.lock().unwrap_or_else(std::sync::PoisonError::into_inner).iter().filter_map(|(_, life, phase)| life.upgrade().map(|life| format!("{phase}, flights={}", life.requests_in_flight()))).collect();
    format!("waiting for [{}]; settled [{}]; {}", waits.join(" | "), settled.join(" | "), crate::runtime::barrier_report())
}

#[used]
#[cfg_attr(target_os = "macos", link_section = "__DATA,__mod_init_func")]
#[cfg_attr(not(target_os = "macos"), link_section = ".init_array")]
static ARM_TEST_DEADLINE: extern "C" fn() = arm_test_deadline;

extern "C" fn arm_test_deadline() {
    let args: Vec<String> = std::env::args().collect();
    if !args.iter().any(|argument| argument == "--exact") { return; }
    let name = args.iter().find(|argument| argument.contains("::")).cloned().unwrap_or_default();
    std::thread::spawn(move || {
        let (_alive, never) = std::sync::mpsc::channel::<()>();
        let _ = never.recv_timeout(Duration::from_secs(TEST_DEADLINE));
        eprintln!("TIMEOUT {TEST_DEADLINE}s {name}: the test hangs, a blocked thread cannot be caught by a bounded wait; {}", hang_report());
        std::process::abort();
    });
}

const BLOCK_LIMIT: Duration = Duration::from_secs(20);

#[track_caller]
pub(crate) fn recv_within<T>(receiver: &Receiver<T>) -> T {
    receiver.recv_timeout(BLOCK_LIMIT).unwrap_or_else(|error| panic!("nothing arrived: {error}"))
}


pub(crate) trait Joinable<T> {
    fn join_bounded(self) -> T;
}

impl<T> Joinable<T> for JoinHandle<T> {
    #[track_caller]
    fn join_bounded(self) -> T {
        self.join().unwrap()
    }
}

#[cfg(dblayer_loom)]
impl<T> Joinable<T> for std::thread::ScopedJoinHandle<'_, T> {
    #[track_caller]
    fn join_bounded(self) -> T {
        self.join().unwrap()
    }
}

impl<T> Joinable<T> for loom::thread::JoinHandle<T> {
    fn join_bounded(self) -> T {
        self.join().unwrap()
    }
}

#[track_caller]
pub(crate) fn join_within<T>(handle: impl Joinable<T>) -> T {
    handle.join_bounded()
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
    within_secs(8, work).await
}

pub(crate) async fn within_secs<T>(seconds: u64, work: impl std::future::Future<Output = T>) -> T { within_named(seconds, "bounded wait expired", work).await }

pub(crate) async fn within_named<T>(seconds: u64, what: &str, work: impl std::future::Future<Output = T>) -> T {
    let _waiting = Waiting::enter(what);
    let (expired, expiry) = tokio::sync::oneshot::channel::<()>();
    let (finished, watching) = std::sync::mpsc::channel::<()>();
    std::thread::spawn(move || {
        if watching.recv_timeout(Duration::from_secs(seconds)) == Err(RecvTimeoutError::Timeout) {
            let _ = expired.send(());
        }
    });
    let _finished = finished;
    tokio::pin!(work);
    tokio::select! {
        output = &mut work => output,
        _ = expiry => panic!("{what}: the event did not come in {seconds} s; {}", hang_report()),
    }
}

pub(crate) trait Quiet {
    async fn quiet(&self);
}

impl<T: Quiet + ?Sized> Quiet for &T {
    async fn quiet(&self) { (**self).quiet().await }
}

impl Quiet for crate::runtime::Handle {
    async fn quiet(&self) {
        let owners = self.clone();
        within_named(TEST_DEADLINE, "the owners did not get quiet", async move { tokio::task::spawn_blocking(move || owners.barrier()).await.unwrap() }).await;
    }
}

impl Quiet for crate::owners::life::Life {
    async fn quiet(&self) {
        match tokio::runtime::Handle::current().runtime_flavor() {
            tokio::runtime::RuntimeFlavor::MultiThread => tokio::task::block_in_place(|| self.barrier()),
            _ => self.barrier(),
        }
    }
}

impl Quiet for std::sync::Arc<crate::owners::life::Life> {
    async fn quiet(&self) {
        let life = self.clone();
        within_named(TEST_DEADLINE, "the owners did not get quiet", async move { tokio::task::spawn_blocking(move || life.barrier()).await.unwrap() }).await;
    }
}

#[track_caller]
pub(crate) fn settled<'a>(handle: &'a impl Quiet, what: &'a str, ready: impl FnOnce() -> bool + 'a) -> impl std::future::Future<Output = ()> + 'a {
    settled_showing(handle, what, String::new, ready)
}

#[track_caller]
pub(crate) fn settled_showing<'a>(handle: &'a impl Quiet, what: &'a str, state: impl FnOnce() -> String + 'a, ready: impl FnOnce() -> bool + 'a) -> impl std::future::Future<Output = ()> + 'a {
    let place = std::panic::Location::caller();
    async move {
        let _waiting = Waiting::enter(what);
        handle.quiet().await;
        assert!(ready(), "{what}{} (at {place}: the owners are quiet, the state is final)", state());
    }
}

#[derive(Default)]
pub(crate) struct Flag {
    set: Mutex<bool>,
    ready: Condvar,
    signal: tokio::sync::Notify,
}

impl Flag {
    pub(crate) fn set(&self) {
        *self.set.lock().unwrap() = true;
        self.ready.notify_all();
        self.signal.notify_waiters();
    }

    pub(crate) fn is_set(&self) -> bool { *self.set.lock().unwrap() }

    #[track_caller]
    pub(crate) fn wait_blocking(&self, what: &str) {
        let deadline = Instant::now() + BLOCK_LIMIT;
        let mut set = self.set.lock().unwrap();
        while !*set {
            let left = deadline.saturating_duration_since(Instant::now());
            assert!(!left.is_zero(), "{what}");
            set = self.ready.wait_timeout(set, left).unwrap().0;
        }
    }

    pub(crate) async fn wait(&self, what: &str) { signalled(&self.signal, what, || self.is_set()).await }
}

pub(crate) async fn signalled(signal: &tokio::sync::Notify, what: &str, mut ready: impl FnMut() -> bool) {
    within_named(TEST_DEADLINE, what, async {
        loop {
            let notified = signal.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if ready() { return; }
            notified.await;
        }
    }).await;
}

pub(crate) fn mutex_free<T>(mutex: &Mutex<T>) -> bool {
    !matches!(mutex.try_lock(), Err(std::sync::TryLockError::WouldBlock))
}

struct World {
    clock: crate::runtime::TestClock,
}

thread_local! { static WORLD: std::cell::RefCell<Option<World>> = const { std::cell::RefCell::new(None) }; }

struct Leave;

impl Drop for Leave {
    fn drop(&mut self) { WORLD.with(|world| world.borrow_mut().take()); }
}

#[track_caller]
pub(crate) fn limited(limit: Duration, test: impl std::future::Future<Output = ()>) {
    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("test runtime");
    let finished = runtime.block_on(async { tokio::time::timeout(limit, test).await });
    runtime.shutdown_timeout(BLOCK_LIMIT);
    finished.unwrap_or_else(|_| panic!("TIMEOUT {limit:?}: the test did not end"));
}

pub(crate) fn paused(test: impl std::future::Future<Output = ()>) {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().expect("test runtime");
    let _leave = Leave;
    runtime.block_on(async move {
        let clock = crate::runtime::TestClock::new(Duration::from_millis(1_000_000));
        WORLD.with(|world| *world.borrow_mut() = Some(World { clock }));
        test.await;
    });
}

pub(crate) fn world_clock() -> Option<crate::runtime::TestClock> {
    WORLD.with(|world| world.borrow().as_ref().map(|world| world.clock.clone()))
}


pub(crate) async fn pass(duration: Duration) {
    match world_clock() {
        Some(clock) => crate::runtime::Clock::sleep_until(&clock, crate::runtime::Clock::now(&clock).saturating_add(duration)).await,
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

pub(crate) fn permutations<T: Clone>(items: &[T]) -> Vec<Vec<T>> {
    if items.len() <= 1 { return vec![items.to_vec()]; }
    (0..items.len()).flat_map(|at| {
        let mut rest = items.to_vec();
        let first = rest.remove(at);
        permutations(&rest).into_iter().map(move |mut tail| { tail.insert(0, first.clone()); tail })
    }).collect()
}

/// The orders of `events` in which every pair `(first, second)` of `before` keeps its order: the causal filter of a permutation test.
pub(crate) fn orders_of<T: Copy + PartialEq>(events: &[T], before: &[(T, T)]) -> Vec<Vec<T>> {
    let at = |order: &[T], event: T| order.iter().position(|found| *found == event);
    permutations(events).into_iter().filter(|order| before.iter().all(|(first, second)| at(order, *first) < at(order, *second))).collect()
}

/// Runs `body` once per variant in a child process that executes only the test `name` of `module`: a process-wide source keeps its value, so a test of "no value yet" or of "no wake-up" cannot share a process with other tests.
#[track_caller]
pub(crate) fn in_own_process(module: &str, name: &str, variants: &[&str], body: impl Fn(&str)) {
    if let Ok(variant) = std::env::var("DBL_OWN_PROCESS") {
        body(&variant);
        return;
    }
    let path = format!("{}::{name}", module.split_once("::").map_or(module, |(_, rest)| rest));
    for variant in variants {
        let out = std::process::Command::new(std::env::current_exe().expect("the test binary")).args(["--exact", &path, "--test-threads=1"]).env("DBL_OWN_PROCESS", variant).output().expect("the child process starts");
        let (stdout, stderr) = (String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
        assert!(out.status.success() && stdout.contains("1 passed"), "{path} [{variant}] failed in its own process:\n{stdout}\n{stderr}");
    }
}
