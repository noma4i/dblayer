use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::{Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// The directory of this package, read when the test runs: a test binary that embeds the path of the tree is not reusable from the cache. Cargo and corgi start a test in the package directory.
pub(crate) fn package_dir() -> std::path::PathBuf {
    std::env::current_dir().expect("the test has a working directory")
}

/// Seconds a single test may run. The runner starts one process per test (`--exact`): a test that is still running after this long is a hang, the process names it and aborts.
const TEST_DEADLINE: u64 = 20;

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
        eprintln!("TIMEOUT {TEST_DEADLINE}s {name}: the test hangs, a blocked thread cannot be caught by a bounded wait");
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
        _ = expiry => panic!("{what}: the event did not come in {seconds} s"),
    }
}

/// A handle whose owners can be waited for: `quiet` returns once every input and every effect task that can run has ended. Tasks that wait for the outside (a held answer of the mock) do not count.
pub(crate) trait Quiet {
    async fn quiet(&self);
}

impl<T: Quiet + ?Sized> Quiet for &T {
    async fn quiet(&self) { (**self).quiet().await }
}

impl Quiet for crate::runtime::Handle {
    async fn quiet(&self) {
        let owners = self.clone();
        within_secs(TEST_DEADLINE, async move { tokio::task::spawn_blocking(move || owners.barrier()).await.unwrap() }).await;
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
        within_secs(TEST_DEADLINE, async move { tokio::task::spawn_blocking(move || life.barrier()).await.unwrap() }).await;
    }
}

/// Kind A: the state after the inputs the test gave. Waits for the barrier of the owners, then checks once: a state that is not there by then is final and the test is red.
#[track_caller]
pub(crate) fn settled<'a>(handle: &'a impl Quiet, what: &'a str, ready: impl FnOnce() -> bool + 'a) -> impl std::future::Future<Output = ()> + 'a {
    settled_showing(handle, what, String::new, ready)
}

#[track_caller]
pub(crate) fn settled_showing<'a>(handle: &'a impl Quiet, what: &'a str, state: impl FnOnce() -> String + 'a, ready: impl FnOnce() -> bool + 'a) -> impl std::future::Future<Output = ()> + 'a {
    let place = std::panic::Location::caller();
    async move {
        handle.quiet().await;
        assert!(ready(), "{what}{} (at {place}: the owners are quiet, the state is final)", state());
    }
}

/// A one-way event: `set` once, wait on it from a thread (`wait_blocking`) or from a task (`wait`). The wait ends on the event, never by polling.
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

/// Kind B: an event of the outside (a request reached the mock, a held answer came in). Waits for the signal of the party, never polls: the notification is enabled before the condition is read, so a signal between the two is not lost.
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

/// Runs an async test on its own runtime with a limit: a test that does not end is red, and a blocking task that does not end cannot hold the run (`shutdown_timeout`).
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
        Some(clock) => crate::runtime::Clock::sleep(&clock, duration).await,
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
