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
}

impl Inflight {
    fn add(&self) { *lock(&self.count) += 1; }

    fn done(&self, handled: usize) {
        let mut count = lock(&self.count);
        *count -= handled;
        if *count == 0 {
            drop(count);
            crate::db::lock::effect(|| self.idle.notify_all());
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
    bodies: Vec<(&'static str, Body)>,
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

    /// Counts an effect task as in flight from its spawn until its first poll, so the barrier covers the request it is about to make.
    pub fn launch(&self) -> Launch { Launch(self.router().cloned().inspect(|router| router.inflight.add())) }
}

pub struct Launch(Option<Arc<Router>>);

impl Drop for Launch {
    fn drop(&mut self) {
        if let Some(router) = self.0.take() { router.inflight.done(1); }
    }
}

pub struct Runtime {
    router: Arc<Router>,
    joins: Vec<JoinHandle<()>>,
    alive: Arc<AtomicUsize>,
}

impl RuntimeBuilder {
    pub fn new() -> Self { Self::default() }

    pub fn add<O: StateOwner>(&mut self, name: &'static str, state: O::State, runner: impl EffectRunner<O>) -> Address<O> {
        let mailbox = Arc::new(Mailbox::new());
        let own = mailbox.clone();
        let slot = self.boxes.len();
        let history = Arc::new(Mutex::new(Vec::new()));
        self.history.push(history.clone());
        self.boxes.push(mailbox);
        let runner: Box<dyn EffectRunner<O>> = Box::new(runner);
        self.bodies.push((name, Box::new(move |router, timeline| run_owner::<O>(OwnerThread { slot, name, cell: Cell::new(state), runner, mailbox: own, history }, router, timeline))));
        Address::new(slot)
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
        for (name, body) in self.bodies {
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
                    owner.cell.apply_failure(owner.name, pending);
                    router.report_failure(owner.name);
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
            for emitted in ran.emitted {
                match emitted {
                    Emitted::Send(outbound) => router.push(outbound.to, Command::Deliver(outbound.stamp, outbound.payload)),
                    Emitted::Broadcast(control) => router.broadcast(control),
                }
            }
            if ran.failed {
                router.report_failure(owner.name);
            }
        }
        router.inflight.done(handled);
    }
}
