use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread::JoinHandle;
use std::time::Duration;

use super::cell::{Cell, Emitted, EffectRunner};
use super::det::TraceEntry;
use super::mailbox::Mailbox;
use super::owner::{Address, Back, Cause, Control, EffectId, Epoch, EpochStamp, Message, StateOwner, Time};
use super::sync::{lock, Condvar, Mutex};
use crate::blocking::blocking_unit;
use crate::task::spawn_thread;

enum Command {
    Deliver(EpochStamp, Box<dyn Message>),
    Control(Control),
    Failed(Vec<EffectId>),
    /// The clock moved: the owner reads the time again.
    Wake,
    Stop,
}

type FailureRoute = Box<dyn Fn(&str) -> Vec<(usize, Box<dyn Message>)> + Send + Sync>;

#[derive(Default)]
struct Inflight {
    count: Mutex<usize>,
    idle: Condvar,
}

impl Inflight {
    fn add(&self) { *lock(&self.count) += 1; }

    fn done(&self, handled: usize) {
        let mut count = lock(&self.count);
        *count -= handled;
        if *count == 0 {
            drop(count);
            crate::lock::effect(|| self.idle.notify_all());
        }
    }

    fn wait_idle(&self) {
        let mut count = lock(&self.count);
        while *count > 0 {
            count = blocking_unit(|| self.idle.wait(count).unwrap_or_else(std::sync::PoisonError::into_inner));
        }
    }
}

#[cfg(test)]
static ROUTERS: std::sync::Mutex<Vec<std::sync::Weak<Router>>> = std::sync::Mutex::new(Vec::new());

#[cfg(test)]
pub fn barrier_report() -> String {
    let routers: Vec<_> = ROUTERS.lock().unwrap_or_else(std::sync::PoisonError::into_inner).iter().filter_map(std::sync::Weak::upgrade).collect();
    let busy: Vec<String> = routers.iter().filter_map(|router| {
        let count = *lock(&router.inflight.count);
        if count == 0 { return None; }
        let held: Vec<String> = lock(&router.gates).iter().filter_map(std::sync::Weak::upgrade).filter(|gate| gate.state.load(Ordering::SeqCst) == HELD).map(|gate| gate.place.to_string()).collect();
        Some(format!("in flight {count}, counted tasks held at [{}], the rest are queued commands and launches", held.join(", ")))
    }).collect();
    if busy.is_empty() { "the barrier of every runtime is idle".into() } else { busy.join("; ") }
}

struct Router {
    boxes: Vec<Arc<Mailbox<Command>>>,
    inflight: Inflight,
    failure: Option<FailureRoute>,
    history: Vec<Arc<Mutex<Vec<TraceEntry>>>>,
    record: bool,
    #[cfg(test)]
    gates: Mutex<Vec<std::sync::Weak<Gate>>>,
}

/// Counts the owner threads of one `Runtime` that run now.
struct Alive(Arc<AtomicUsize>);

impl Drop for Alive {
    fn drop(&mut self) { self.0.fetch_sub(1, Ordering::SeqCst); }
}

impl Router {
    fn push(&self, to: usize, command: Command) {
        self.inflight.add();
        self.boxes[to].push(command);
    }

    fn report_failure(&self, executor: &str) {
        for (to, payload) in self.failure.as_ref().map_or_else(Vec::new, |route| route(executor)) {
            self.push(to, Command::Deliver(EpochStamp::Control, payload));
        }
    }

    fn broadcast(&self, control: Control) { (0..self.boxes.len()).for_each(|to| self.push(to, Command::Control(control))); }
}

/// The time of the owners: ms of the clock of the database, the same scale as the saved times.
pub(super) struct Timeline {
    clock: Arc<dyn crate::runtime::Clock>,
}

impl Timeline {
    fn now(&self) -> Time { Time(self.clock.now().as_millis() as u64) }
}

type Body = Box<dyn FnOnce(Arc<Router>, Arc<Timeline>) + Send>;

pub struct RuntimeBuilder {
    boxes: Vec<Arc<Mailbox<Command>>>,
    bodies: Vec<Option<(&'static str, Body)>>,
    types: Vec<std::any::TypeId>,
    history: Vec<Arc<Mutex<Vec<TraceEntry>>>>,
    failure: Option<FailureRoute>,
    record: bool,
    handle: Handle,
    clock: Arc<dyn crate::runtime::Clock>,
}

/// A clonable address book of the running owners. An effect runner takes it before `start` and uses it after.
#[derive(Clone, Default)]
pub struct Handle {
    router: Arc<OnceLock<Arc<Router>>>,
}

impl Handle {
    fn router(&self) -> Option<&Arc<Router>> { self.router.get() }

    pub fn post<O: StateOwner>(&self, to: Address<O>, stamp: EpochStamp, input: O::Input) {
        if let Some(router) = self.router() {
            router.push(to.index, Command::Deliver(stamp, Box::new(input)));
        }
    }

    pub fn start_epoch(&self, epoch: Epoch) {
        if let Some(router) = self.router() { router.broadcast(Control::EpochStarted(epoch)); }
    }

    pub fn end_epoch(&self, epoch: Epoch, cause: Cause) {
        if let Some(router) = self.router() { router.broadcast(Control::EpochEnded(epoch, cause)); }
    }

    pub fn close(&self) {
        if let Some(router) = self.router() { router.broadcast(Control::Closing); }
    }

    /// Returns once nothing is in flight, also after the owner threads ended, or false after `limit`.
    #[cfg(test)]
    pub fn barrier(&self) { if let Some(router) = self.router() { router.inflight.wait_idle(); } }

    #[cfg(test)]
    pub fn pending(&self) -> usize { self.router().map_or(0, |router| *lock(&router.inflight.count)) }

    /// Counts an effect task as in flight from its spawn until its first poll, so the barrier covers the request it is about to make.
    pub fn launch(&self) -> Launch {
        Launch { router: self.router().cloned().inspect(|router| router.inflight.add()) }
    }
}

pub struct Launch {
    router: Option<Arc<Router>>,
}

/// The counts a polled call holds while work started for it runs off its thread. The wake of the call lets them go.
#[derive(Default)]
pub struct Holds(Mutex<Vec<Launch>>);

impl Holds {
    pub fn release(&self) {
        let held = std::mem::take(&mut *lock(&self.0));
        drop(held);
    }
}

thread_local! {
    static RUNNING: std::cell::RefCell<Option<Arc<Router>>> = const { std::cell::RefCell::new(None) };
    static POLLED: std::cell::RefCell<Option<Arc<Holds>>> = const { std::cell::RefCell::new(None) };
}

/// Counts blocking work started by the counted task that polls now, until the work ends. Outside a counted task it is None.
pub fn running_launch() -> Option<Launch> {
    RUNNING.with(|running| running.borrow().clone()).map(|router| {
        router.inflight.add();
        Launch { router: Some(router) }
    })
}

/// Runs one poll of a call whose started work counts in `holds`.
pub fn polling<R>(holds: &Arc<Holds>, poll: impl FnOnce() -> R) -> R {
    let previous = POLLED.replace(Some(holds.clone()));
    let result = poll();
    POLLED.set(previous);
    result
}

/// The awaited work of the polled call ended: the counts it held end with it, also when no wake came.
pub fn release_holds() {
    if let Some(holds) = POLLED.with(|polled| polled.borrow().clone()) { holds.release(); }
}

/// Counts work started by the polled call until the call wakes. Outside a polled call the launch comes back and the caller keeps it.
pub fn hold_until_woken(launch: Launch) -> Option<Launch> {
    match POLLED.with(|polled| polled.borrow().clone()) {
        Some(holds) => {
            lock(&holds.0).push(launch);
            None
        }
        None => Some(launch),
    }
}

const IDLE: u8 = 0;
const HELD: u8 = 1;
const RETIRED: u8 = 2;

/// The count of one effect task in the barrier: held while the task can run (spawned, woken or polled), released while it waits for the outside, never taken again once the task is gone.
struct Gate {
    router: Arc<Router>,
    state: std::sync::atomic::AtomicU8,
    woken: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    place: &'static std::panic::Location<'static>,
}

impl Gate {
    fn arm(&self) {
        if self.state.compare_exchange(IDLE, HELD, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
            self.router.inflight.add();
        }
    }

    fn disarm(&self) {
        if self.state.compare_exchange(HELD, IDLE, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
            self.router.inflight.done(1);
        }
    }

    fn retire(&self) {
        if self.state.swap(RETIRED, Ordering::SeqCst) == HELD {
            self.router.inflight.done(1);
        }
    }
}

struct GateWake {
    gate: Arc<Gate>,
    waker: std::task::Waker,
}

impl std::task::Wake for GateWake {
    fn wake(self: Arc<Self>) { self.wake_by_ref(); }

    fn wake_by_ref(self: &Arc<Self>) {
        self.gate.woken.store(true, Ordering::SeqCst);
        self.gate.arm();
        self.waker.wake_by_ref();
    }
}

/// A task that the barrier of the owners waits for whenever it can run: from its spawn to its first wait and again from each wake to the next wait.
pub struct Counted<F> {
    work: std::pin::Pin<Box<F>>,
    gate: Option<Arc<Gate>>,
    _ended: Retire,
}

/// Ends the count of the task after its future is gone: a wake that its drop or a late answer still sends finds the gate retired.
struct Retire(Option<Arc<Gate>>);

impl Drop for Retire {
    fn drop(&mut self) {
        if let Some(gate) = &self.0 { gate.retire(); }
    }
}

impl<F: std::future::Future> std::future::Future for Counted<F> {
    type Output = F::Output;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<F::Output> {
        let Some(gate) = self.gate.clone() else { return self.work.as_mut().poll(cx) };
        gate.woken.store(false, Ordering::SeqCst);
        gate.arm();
        let waker = std::task::Waker::from(Arc::new(GateWake { gate: gate.clone(), waker: cx.waker().clone() }));
        let previous = RUNNING.replace(Some(gate.router.clone()));
        let polled = self.work.as_mut().poll(&mut std::task::Context::from_waker(&waker));
        RUNNING.set(previous);
        if polled.is_ready() || !gate.woken.load(Ordering::SeqCst) {
            gate.disarm();
            if polled.is_pending() && gate.woken.load(Ordering::SeqCst) { gate.arm(); }
        }
        polled
    }
}

/// Wraps `work` so the barrier counts it whenever it can run. The launch taken before the spawn covers the time until the first poll.
#[track_caller]
pub fn counted<F: std::future::Future>(launch: Option<Launch>, work: F) -> Counted<F> {
    #[cfg(test)]
    let place = std::panic::Location::caller();
    let gate = launch.and_then(|mut launch| launch.router.take()).map(|router| {
        let gate = Arc::new(Gate {
            router,
            state: std::sync::atomic::AtomicU8::new(HELD),
            woken: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            place,
        });
        #[cfg(test)]
        lock(&gate.router.gates).push(Arc::downgrade(&gate));
        gate
    });
    Counted { work: Box::pin(work), _ended: Retire(gate.clone()), gate }
}

impl Drop for Launch {
    fn drop(&mut self) {
        if let Some(router) = self.router.take() {
            router.inflight.done(1);
        }
    }
}

pub struct Runtime {
    router: Arc<Router>,
    joins: Mutex<Vec<JoinHandle<()>>>,
    alive: Arc<AtomicUsize>,
}

impl RuntimeBuilder {
    /// The timers of the owners follow this clock.
    pub fn new(clock: Arc<dyn crate::runtime::Clock>) -> Self {
        Self { boxes: Vec::new(), bodies: Vec::new(), types: Vec::new(), history: Vec::new(), failure: None, record: false, handle: Handle::default(), clock }
    }

    #[cfg(test)]
    pub fn for_tests() -> Self { Self::new(Arc::new(crate::runtime::TestClock::new(Duration::from_secs(1_000_000)))) }

    pub fn reserve<O: StateOwner>(&mut self) -> Address<O> {
        let slot = self.boxes.len();
        self.boxes.push(Arc::new(Mailbox::new()));
        self.history.push(Arc::new(Mutex::new(Vec::new())));
        self.bodies.push(None);
        self.types.push(std::any::TypeId::of::<O>());
        Address::new(slot)
    }

    pub fn install<O: StateOwner>(&mut self, address: Address<O>, name: &'static str, state: O::State, runner: impl EffectRunner<O>) {
        let slot = address.index;
        assert_eq!(self.types.get(slot), Some(&std::any::TypeId::of::<O>()));
        assert!(self.bodies[slot].is_none());
        let own = self.boxes[slot].clone();
        let history = self.history[slot].clone();
        let runner: Box<dyn EffectRunner<O>> = Box::new(runner);
        self.bodies[slot] = Some((name, Box::new(move |router, timeline| run_owner::<O>(OwnerThread { slot, name, cell: Cell::new(state), runner, mailbox: own, history }, router, timeline))));
    }

    pub fn add<O: StateOwner>(&mut self, name: &'static str, state: O::State, runner: impl EffectRunner<O>) -> Address<O> {
        let address = self.reserve::<O>();
        self.install(address, name, state, runner);
        address
    }

    pub fn on_executor_failed<L: StateOwner>(&mut self, lifecycle: Address<L>) {
        self.failure = Some(Box::new(move |executor| L::executor_failed(executor, Vec::new()).into_iter().map(|input| (lifecycle.index, Box::new(input) as Box<dyn Message>)).collect()));
    }

    pub fn record(&mut self) { self.record = true; }

    pub fn handle(&self) -> Handle { self.handle.clone() }

    pub fn start(self) -> std::io::Result<Runtime> { self.start_with(spawn_thread) }

    pub(super) fn start_with(self, mut launch: impl FnMut(String, Box<dyn FnOnce() + Send>) -> std::io::Result<JoinHandle<()>>) -> std::io::Result<Runtime> {
        if self.bodies.iter().any(Option::is_none) {
            return Err(std::io::Error::other("owner reservation is not installed"));
        }
        let router = Arc::new(Router { boxes: self.boxes, inflight: Inflight::default(), failure: self.failure, history: self.history, record: self.record, #[cfg(test)] gates: Mutex::new(Vec::new()) });
        #[cfg(test)]
        ROUTERS.lock().unwrap_or_else(std::sync::PoisonError::into_inner).push(Arc::downgrade(&router));
        self.handle.router.set(router.clone()).map_err(|_| std::io::Error::other("the runtime is already started"))?;
        let clock = self.clock;
        let timeline = Arc::new(Timeline { clock: clock.clone() });
        let weak = Arc::downgrade(&router);
        clock.on_advance(Arc::new(move || {
            if let Some(router) = weak.upgrade() { (0..router.boxes.len()).for_each(|to| router.push(to, Command::Wake)); }
        }));
        let mut joins = Vec::new();
        let alive = Arc::new(AtomicUsize::new(0));
        for (name, body) in self.bodies.into_iter().flatten() {
            let own = router.clone();
            alive.fetch_add(1, Ordering::SeqCst);
            let running = Alive(alive.clone());
            let timeline = timeline.clone();
            match launch(name.to_string(), Box::new(move || { let _running = running; body(own, timeline) })) {
                Ok(join) => joins.push(join),
                Err(error) => {
                    stop(&router, &mut joins);
                    return Err(error);
                }
            }
        }
        Ok(Runtime { router, joins: Mutex::new(joins), alive })
    }
}

fn stop(router: &Router, joins: &mut Vec<JoinHandle<()>>) {
    router.boxes.iter().for_each(|mailbox| mailbox.push(Command::Stop));
    for join in joins.drain(..) {
        if join.thread().id() == std::thread::current().id() { continue; }
        let _ = join.join();
    }
}

impl Runtime {
    pub fn post<O: StateOwner>(&self, to: Address<O>, stamp: EpochStamp, input: O::Input) { self.router.push(to.index, Command::Deliver(stamp, Box::new(input))); }

    pub fn start_epoch(&self, epoch: Epoch) { self.router.broadcast(Control::EpochStarted(epoch)); }

    pub fn end_epoch(&self, epoch: Epoch, cause: Cause) { self.router.broadcast(Control::EpochEnded(epoch, cause)); }

    pub fn close(&self) { self.router.broadcast(Control::Closing); }

    /// Stops the owner threads and joins them. A concurrent call waits for the first to finish.
    pub fn stop(&self) { stop(&self.router, &mut lock(&self.joins)); }

    /// The number of owner threads of this runtime that have not ended.
    pub fn alive(&self) -> Arc<AtomicUsize> { self.alive.clone() }

    pub fn wakeups<O: StateOwner>(&self, address: Address<O>) -> u64 { self.router.boxes[address.index].wakeups() }

    pub fn barrier(&self) { self.router.inflight.wait_idle(); }

    /// Inputs in mailboxes and started effect tasks that did not reply yet.
    #[cfg(test)]
    pub fn pending(&self) -> usize { *lock(&self.router.inflight.count) }

    pub fn history<O: StateOwner>(&self, address: Address<O>) -> Vec<TraceEntry> { lock(&self.router.history[address.index]).clone() }
}

impl Drop for Runtime {
    fn drop(&mut self) { stop(&self.router, self.joins.get_mut().unwrap_or_else(std::sync::PoisonError::into_inner)); }
}

struct OwnerThread<O: StateOwner> {
    slot: usize,
    name: &'static str,
    cell: Cell<O>,
    runner: Box<dyn EffectRunner<O>>,
    mailbox: Arc<Mailbox<Command>>,
    history: Arc<Mutex<Vec<TraceEntry>>>,
}

fn run_owner<O: StateOwner>(mut owner: OwnerThread<O>, router: Arc<Router>, timeline: Arc<Timeline>) {
    owner.cell.set_jitter(|| rand_core::RngCore::next_u64(&mut rand_core::OsRng));
    let back = {
        let own = owner.slot;
        let router = router.clone();
        let failing = router.clone();
        Back::<O>::new(move |stamp, input| router.push(own, Command::Deliver(stamp, Box::new(input))), move |pending| failing.push(own, Command::Failed(pending)))
    };
    let mut armed = 0;
    loop {
        let cell = &owner.cell;
        let commands = owner.mailbox.take(|| cell.next_due().map(|at| Duration::from_millis(at.0.saturating_sub(timeline.now().0))));
        let handled = commands.len();
        for command in commands {
            match command {
                Command::Deliver(stamp, payload) => owner.cell.push_message(stamp, payload),
                Command::Control(control) => owner.cell.push_control(control),
                Command::Failed(pending) => {
                    let deferred = O::defer_executor_failure(&pending);
                    owner.cell.apply_failure(owner.name, pending);
                    if !deferred { router.report_failure(owner.name); }
                }
                Command::Wake => {}
                Command::Stop => return,
            }
        }
        owner.cell.set_now(timeline.now());
        while owner.cell.fire_next_due() {}
        while let Some(ran) = owner.cell.run_one(owner.name, owner.runner.as_mut(), &back, router.record) {
            if router.record {
                lock(&owner.history).push(TraceEntry { slot: owner.slot, owner: owner.name, stamp: ran.stamp, input: ran.input, effects: ran.effects, dropped: ran.dropped });
            }
            if ran.failed {
                router.report_failure(owner.name);
            }
            while owner.cell.emitting() {
                let (emitted, failed) = owner.cell.emit_one(owner.name, owner.runner.as_mut(), &back);
                match emitted {
                    Some(Emitted::Send(outbound)) => router.push(outbound.to, Command::Deliver(outbound.stamp, outbound.payload)),
                    Some(Emitted::Broadcast(control)) => router.broadcast(control),
                    None => {}
                }
                let finished_failed = owner.cell.finish_emission(owner.name, owner.runner.as_mut());
                if failed || finished_failed { router.report_failure(owner.name); }
            }
        }
        let scheduled = owner.cell.scheduled();
        if scheduled != armed {
            timeline.clock.armed(scheduled - armed);
            armed = scheduled;
        }
        router.inflight.done(handled);
    }
}
