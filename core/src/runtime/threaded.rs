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
    Wake,
    Stop,
}

struct Queued {
    command: Command,
    #[cfg(test)]
    label: String,
    #[cfg(test)]
    since: std::time::Instant,
}

impl Queued {
    fn new(command: Command) -> Self {
        #[cfg(test)]
        let label = match &command {
            Command::Deliver(_, message) => format!("{message:?}").split([' ', '{', '(']).next().unwrap_or("input").to_string(),
            Command::Control(control) => format!("{control:?}").split([' ', '{', '(']).next().unwrap_or("control").to_string(),
            Command::Failed(_) => "Failed".into(),
            Command::Wake => "Wake".into(),
            Command::Stop => "Stop".into(),
        };
        Self { command, #[cfg(test)] label, #[cfg(test)] since: std::time::Instant::now() }
    }
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
        let held: Vec<String> = lock(&router.gates).iter().filter_map(std::sync::Weak::upgrade).filter(|gate| gate.state.load(Ordering::SeqCst) == HELD).map(|gate| format!("{} {}ms", gate.place, gate.since.elapsed().as_millis())).collect();
        let owners: Vec<String> = router.owners.iter().enumerate().map(|(index, name)| {
            let active = lock(&router.active[index]).iter().map(|(input, since)| format!("{input} {}ms", since.elapsed().as_millis())).collect::<Vec<_>>().join(", ");
            let queued = router.boxes[index].inspect(|items| items.iter().map(|item| format!("{} {}ms", item.label, item.since.elapsed().as_millis())).collect::<Vec<_>>());
            let state = lock(&router.diagnostics[index]).clone();
            if router.stopped[index].load(Ordering::SeqCst) { return format!("{name}: stopped, queued {} [{}], state [{state}]", queued.len(), queued.join(", ")); }
            format!("{name}: queued {} [{}], active [{active}], state [{state}]", queued.len(), queued.join(", "))
        }).collect();
        Some(format!("in flight {count}, counted tasks held at [{}], owners [{}]", held.join(", "), owners.join("; ")))
    }).collect();
    if busy.is_empty() { "the barrier of every runtime is idle".into() } else { busy.join("; ") }
}

struct Router {
    boxes: Vec<Arc<Mailbox<Queued>>>,
    inflight: Inflight,
    failure: Option<FailureRoute>,
    history: Vec<Arc<Mutex<Vec<TraceEntry>>>>,
    record: bool,
    #[cfg(test)]
    gates: Mutex<Vec<std::sync::Weak<Gate>>>,
    #[cfg(test)]
    owners: Vec<&'static str>,
    #[cfg(test)]
    active: Vec<Mutex<Vec<(String, std::time::Instant)>>>,
    #[cfg(test)]
    diagnostics: Vec<Mutex<String>>,
    #[cfg(test)]
    stopped: Vec<std::sync::atomic::AtomicBool>,
}

struct Alive(Arc<AtomicUsize>);

impl Drop for Alive {
    fn drop(&mut self) { self.0.fetch_sub(1, Ordering::SeqCst); }
}

impl Router {
    fn push(&self, to: usize, command: Command) {
        self.inflight.add();
        self.boxes[to].push(Queued::new(command));
    }

    fn report_failure(&self, executor: &str) {
        for (to, payload) in self.failure.as_ref().map_or_else(Vec::new, |route| route(executor)) {
            self.push(to, Command::Deliver(EpochStamp::Control, payload));
        }
    }

    fn broadcast(&self, control: Control) { (0..self.boxes.len()).for_each(|to| self.push(to, Command::Control(control))); }
}

pub(super) struct Timeline {
    clock: Arc<dyn crate::runtime::Clock>,
}

impl Timeline {
    fn now(&self) -> Time { Time(self.clock.now().as_millis() as u64) }
}

type Body = Box<dyn FnOnce(Arc<Router>, Arc<Timeline>) + Send>;

pub struct RuntimeBuilder {
    boxes: Vec<Arc<Mailbox<Queued>>>,
    bodies: Vec<Option<(&'static str, Body)>>,
    types: Vec<std::any::TypeId>,
    history: Vec<Arc<Mutex<Vec<TraceEntry>>>>,
    failure: Option<FailureRoute>,
    record: bool,
    handle: Handle,
    clock: Arc<dyn crate::runtime::Clock>,
}

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

    #[cfg(test)]
    pub fn barrier(&self) { if let Some(router) = self.router() { router.inflight.wait_idle(); } }

    #[cfg(test)]
    pub fn pending(&self) -> usize { self.router().map_or(0, |router| *lock(&router.inflight.count)) }

    pub fn launch(&self) -> Launch {
        Launch { router: self.router().cloned().inspect(|router| router.inflight.add()) }
    }
}

pub struct Launch {
    router: Option<Arc<Router>>,
}

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

pub fn running_launch() -> Option<Launch> {
    RUNNING.with(|running| running.borrow().clone()).map(|router| {
        router.inflight.add();
        Launch { router: Some(router) }
    })
}

pub fn polling<R>(holds: &Arc<Holds>, poll: impl FnOnce() -> R) -> R {
    let previous = POLLED.replace(Some(holds.clone()));
    let result = poll();
    POLLED.set(previous);
    result
}

pub fn release_holds() {
    if let Some(holds) = POLLED.with(|polled| polled.borrow().clone()) { holds.release(); }
}

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
    #[cfg(test)]
    since: std::time::Instant,
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

pub struct Counted<F> {
    work: std::pin::Pin<Box<F>>,
    gate: Option<Arc<Gate>>,
    _ended: Retire,
}

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
            #[cfg(test)]
            since: std::time::Instant::now(),
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
        self.bodies[slot] = Some((name, Box::new(move |router, timeline| { let mut cell = Cell::new(state); cell.set_retry_owner(name); run_owner::<O>(OwnerThread { slot, name, cell, runner, mailbox: own, history }, router, timeline) })));
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
        #[cfg(test)]
        let owners: Vec<_> = self.bodies.iter().filter_map(|body| body.as_ref().map(|(name, _)| *name)).collect();
        let router = Arc::new(Router { boxes: self.boxes, inflight: Inflight::default(), failure: self.failure, history: self.history, record: self.record, #[cfg(test)] gates: Mutex::new(Vec::new()), #[cfg(test)] active: owners.iter().map(|_| Mutex::new(Vec::new())).collect(), #[cfg(test)] diagnostics: owners.iter().map(|_| Mutex::new(String::new())).collect(), #[cfg(test)] stopped: owners.iter().map(|_| std::sync::atomic::AtomicBool::new(false)).collect(), #[cfg(test)] owners });
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
    router.boxes.iter().for_each(|mailbox| mailbox.push(Queued::new(Command::Stop)));
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

    pub fn stop(&self) { stop(&self.router, &mut lock(&self.joins)); }

    pub fn alive(&self) -> Arc<AtomicUsize> { self.alive.clone() }

    pub fn wakeups<O: StateOwner>(&self, address: Address<O>) -> u64 { self.router.boxes[address.index].wakeups() }

    pub fn barrier(&self) { self.router.inflight.wait_idle(); }

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
    mailbox: Arc<Mailbox<Queued>>,
    history: Arc<Mutex<Vec<TraceEntry>>>,
}

fn run_owner<O: StateOwner>(mut owner: OwnerThread<O>, router: Arc<Router>, timeline: Arc<Timeline>) {
    owner.cell.set_jitter(super::process_seed);
    #[cfg(test)]
    if let Some(state) = O::diagnostic(&owner.cell.state) { *lock(&router.diagnostics[owner.slot]) = state; }
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
        #[cfg(test)]
        { *lock(&router.active[owner.slot]) = commands.iter().map(|command| (command.label.clone(), command.since)).collect(); }
        let mut failure_owed = false;
        for queued in commands {
            match queued.command {
                Command::Deliver(stamp, payload) => owner.cell.push_message(stamp, payload),
                Command::Control(control) => owner.cell.push_control(control),
                Command::Failed(pending) => {
                    let deferred = O::defer_executor_failure(&pending);
                    owner.cell.apply_failure(owner.name, pending);
                    failure_owed |= !deferred;
                }
                Command::Wake => {}
                Command::Stop => {
                    #[cfg(test)]
                    { lock(&router.active[owner.slot]).clear(); router.stopped[owner.slot].store(true, Ordering::SeqCst); }
                    return;
                }
            }
        }
        owner.cell.set_now(timeline.now());
        while owner.cell.fire_next_due() {}
        loop {
            owner.cell.set_now(timeline.now());
            let Some(ran) = owner.cell.run_one(owner.name, owner.runner.as_mut(), &back, router.record) else { break };
            #[cfg(test)]
            if let Some(state) = O::diagnostic(&owner.cell.state) { *lock(&router.diagnostics[owner.slot]) = state; }
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
        if failure_owed { router.report_failure(owner.name); }
        let scheduled = owner.cell.scheduled();
        if scheduled != armed {
            timeline.clock.armed(scheduled - armed);
            armed = scheduled;
        }
        #[cfg(test)]
        lock(&router.active[owner.slot]).clear();
        router.inflight.done(handled);
    }
}
