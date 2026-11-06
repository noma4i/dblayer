use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use super::cell::{Cell, Emitted, EffectRunner};
use super::det::TraceEntry;
use super::mailbox::Mailbox;
use super::owner::{Address, Back, Cause, Control, EffectId, Epoch, EpochStamp, Message, StateOwner, Time};
use super::sync::{lock, Condvar, Mutex};
use crate::blocking::blocking_unit;
use crate::task::{spawn_thread, Carry};

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
    /// Who holds the barrier outside the mailboxes: the place that took each count.
    #[cfg(test)]
    holders: Mutex<std::collections::HashMap<u64, &'static std::panic::Location<'static>>>,
}

impl Inflight {
    fn add(&self) { *lock(&self.count) += 1; }

    #[cfg(test)]
    fn hold(&self, id: u64, place: &'static std::panic::Location<'static>) { lock(&self.holders).insert(id, place); }

    #[cfg(test)]
    fn release(&self, id: u64) { lock(&self.holders).remove(&id); }

    #[cfg(test)]
    fn report(&self) -> String { lock(&self.holders).values().map(|place| format!("{}:{}", place.file(), place.line())).collect::<Vec<_>>().join(", ") }

    fn done(&self, handled: usize) {
        let mut count = lock(&self.count);
        *count -= handled;
        if *count == 0 {
            drop(count);
            crate::lock::effect(|| self.idle.notify_all());
        }
    }

    fn wait_idle(&self, limit: Duration) -> bool {
        let deadline = Instant::now() + limit;
        let mut count = lock(&self.count);
        while *count > 0 {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return false;
            }
            count = blocking_unit(|| self.idle.wait_timeout(count, left).unwrap_or_else(std::sync::PoisonError::into_inner).0);
        }
        true
    }
}

struct Router {
    boxes: Vec<Arc<Mailbox<Command>>>,
    inflight: Inflight,
    failure: Option<FailureRoute>,
    history: Vec<Arc<Mutex<Vec<TraceEntry>>>>,
    record: bool,
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

/// The time of the owners: ms since the runtime started, from the clock of the database or from the process.
pub(super) struct Timeline {
    clock: Option<Arc<dyn crate::pacing::Clock>>,
    base: Duration,
    origin: Instant,
}

impl Timeline {
    fn now(&self) -> Time {
        match &self.clock {
            Some(clock) => Time(clock.now().saturating_sub(self.base).as_millis() as u64),
            None => Time(self.origin.elapsed().as_millis() as u64),
        }
    }
}

type Body = Box<dyn FnOnce(Arc<Router>, Arc<Timeline>) + Send>;

#[derive(Default)]
pub struct RuntimeBuilder {
    boxes: Vec<Arc<Mailbox<Command>>>,
    bodies: Vec<Option<(&'static str, Body)>>,
    types: Vec<std::any::TypeId>,
    history: Vec<Arc<Mutex<Vec<TraceEntry>>>>,
    failure: Option<FailureRoute>,
    record: bool,
    handle: Handle,
    clock: Option<Arc<dyn crate::pacing::Clock>>,
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
    pub fn barrier(&self, limit: Duration) -> bool { self.router().is_none_or(|router| router.inflight.wait_idle(limit)) }

    #[cfg(test)]
    pub fn pending(&self) -> usize { self.router().map_or(0, |router| *lock(&router.inflight.count)) }

    /// Counts an effect task as in flight from its spawn until its first poll, so the barrier covers the request it is about to make.
    #[track_caller]
    pub fn launch(&self) -> Launch {
        let place = std::panic::Location::caller();
        let id = NEXT_HOLDER.fetch_add(1, Ordering::SeqCst) as u64;
        #[cfg(not(test))]
        let _ = place;
        Launch { router: self.router().cloned().inspect(|router| {
            router.inflight.add();
            #[cfg(test)]
            router.inflight.hold(id, place);
        }), id, place }
    }

    /// Who holds the barrier now, as `file:line` of the places that took the counts.
    #[cfg(test)]
    pub fn holders(&self) -> String { self.router().map_or_else(String::new, |router| router.inflight.report()) }
}

pub struct Launch {
    router: Option<Arc<Router>>,
    id: u64,
    place: &'static std::panic::Location<'static>,
}

static NEXT_HOLDER: AtomicUsize = AtomicUsize::new(1);

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
    static POLLED: std::cell::RefCell<Option<Arc<Holds>>> = const { std::cell::RefCell::new(None) };
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

/// The count of one effect task in the barrier: held while the task can run (spawned, woken or polled), released while it waits for the outside.
struct Gate {
    router: Arc<Router>,
    #[cfg_attr(not(test), allow(dead_code))]
    id: u64,
    #[cfg_attr(not(test), allow(dead_code))]
    place: &'static std::panic::Location<'static>,
    held: std::sync::atomic::AtomicBool,
    woken: std::sync::atomic::AtomicBool,
}

impl Gate {
    fn arm(&self) {
        if !self.held.swap(true, Ordering::SeqCst) {
            self.router.inflight.add();
            #[cfg(test)]
            self.router.inflight.hold(self.id, self.place);
        }
    }

    fn disarm(&self) {
        if self.held.swap(false, Ordering::SeqCst) {
            #[cfg(test)]
            self.router.inflight.release(self.id);
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
}

impl<F: std::future::Future> std::future::Future for Counted<F> {
    type Output = F::Output;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<F::Output> {
        let Some(gate) = self.gate.clone() else { return self.work.as_mut().poll(cx) };
        gate.woken.store(false, Ordering::SeqCst);
        gate.arm();
        let waker = std::task::Waker::from(Arc::new(GateWake { gate: gate.clone(), waker: cx.waker().clone() }));
        let polled = self.work.as_mut().poll(&mut std::task::Context::from_waker(&waker));
        if polled.is_ready() || !gate.woken.load(Ordering::SeqCst) {
            gate.disarm();
            if polled.is_pending() && gate.woken.load(Ordering::SeqCst) { gate.arm(); }
        }
        polled
    }
}

impl<F> Drop for Counted<F> {
    fn drop(&mut self) {
        if let Some(gate) = &self.gate { gate.disarm(); }
    }
}

/// Wraps `work` so the barrier counts it whenever it can run. The launch taken before the spawn covers the time until the first poll.
pub fn counted<F: std::future::Future>(launch: Option<Launch>, work: F) -> Counted<F> {
    let gate = launch.and_then(|mut launch| launch.router.take().map(|router| (router, launch.id, launch.place))).map(|(router, id, place)| {
        Arc::new(Gate { router, id, place, held: std::sync::atomic::AtomicBool::new(true), woken: std::sync::atomic::AtomicBool::new(false) })
    });
    Counted { work: Box::pin(work), gate }
}

impl Drop for Launch {
    fn drop(&mut self) {
        if let Some(router) = self.router.take() {
            #[cfg(test)]
            router.inflight.release(self.id);
            router.inflight.done(1);
        }
    }
}

pub struct Runtime {
    router: Arc<Router>,
    joins: Vec<JoinHandle<()>>,
    alive: Arc<AtomicUsize>,
}

impl RuntimeBuilder {
    pub fn new() -> Self { Self::default() }

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

    /// The timers of the owners follow this clock.
    pub fn with_clock(&mut self, clock: Arc<dyn crate::pacing::Clock>) { self.clock = Some(clock); }

    pub fn handle(&self) -> Handle { self.handle.clone() }

    pub fn start(self) -> std::io::Result<Runtime> { self.start_with(|name, body| spawn_thread(Carry::Internal, name, body)) }

    pub(super) fn start_with(self, mut launch: impl FnMut(String, Box<dyn FnOnce() + Send>) -> std::io::Result<JoinHandle<()>>) -> std::io::Result<Runtime> {
        if self.bodies.iter().any(Option::is_none) {
            return Err(std::io::Error::other("owner reservation is not installed"));
        }
        let router = Arc::new(Router { boxes: self.boxes, inflight: Inflight::default(), failure: self.failure, history: self.history, record: self.record });
        let _ = self.handle.router.set(router.clone());
        let timeline = Arc::new(Timeline { base: self.clock.as_ref().map_or(Duration::ZERO, |clock| clock.now()), clock: self.clock.clone(), origin: Instant::now() });
        if let Some(clock) = &self.clock {
            let weak = Arc::downgrade(&router);
            clock.on_advance(Arc::new(move || {
                if let Some(router) = weak.upgrade() { (0..router.boxes.len()).for_each(|to| router.push(to, Command::Wake)); }
            }));
        }
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
        Ok(Runtime { router, joins, alive })
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

    /// The number of owner threads of this runtime that have not ended.
    pub fn alive(&self) -> Arc<AtomicUsize> { self.alive.clone() }

    pub fn wakeups<O: StateOwner>(&self, address: Address<O>) -> u64 { self.router.boxes[address.index].wakeups() }

    pub fn barrier(&self, limit: Duration) -> bool { self.router.inflight.wait_idle(limit) }

    /// Inputs in mailboxes and started effect tasks that did not reply yet.
    #[cfg(test)]
    pub fn pending(&self) -> usize { *lock(&self.router.inflight.count) }

    pub fn history<O: StateOwner>(&self, address: Address<O>) -> Vec<TraceEntry> { lock(&self.router.history[address.index]).clone() }
}

impl Drop for Runtime {
    fn drop(&mut self) { stop(&self.router, &mut self.joins); }
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
        router.inflight.done(handled);
    }
}
