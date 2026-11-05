use std::sync::OnceLock;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use super::cell::{Cell, Emitted, EffectRunner};
use super::det::TraceEntry;
use super::mailbox::Mailbox;
use super::owner::{Address, Back, Cause, Control, Epoch, EpochStamp, Message, StateOwner, Time};
use super::sync::{lock, Arc, AtomicUsize, Condvar, Mutex, Ordering};
use crate::task::{spawn_thread, Carry};

enum Command {
    Deliver(EpochStamp, Box<dyn Message>),
    Control(Control),
    Stop,
}

type FailureRoute = Box<dyn Fn(&str) -> Option<(usize, Box<dyn Message>)> + Send + Sync>;

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
            count = self.idle.wait_timeout(count, left).unwrap_or_else(std::sync::PoisonError::into_inner).0;
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

    fn broadcast(&self, control: Control) { (0..self.boxes.len()).for_each(|to| self.push(to, Command::Control(control))); }
}

type Body = Box<dyn FnOnce(Arc<Router>, Instant) + Send>;

#[derive(Default)]
pub struct RuntimeBuilder {
    boxes: Vec<Arc<Mailbox<Command>>>,
    bodies: Vec<(&'static str, Body)>,
    history: Vec<Arc<Mutex<Vec<TraceEntry>>>>,
    failure: Option<FailureRoute>,
    record: bool,
    handle: Handle,
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
        self.bodies.push((name, Box::new(move |router, origin| run_owner::<O>(OwnerThread { slot, name, cell: Cell::new(state), runner, mailbox: own, history }, router, origin))));
        Address::new(slot)
    }

    pub fn on_executor_failed<L: StateOwner>(&mut self, lifecycle: Address<L>) {
        self.failure = Some(Box::new(move |executor| L::executor_failed(executor, Vec::new()).map(|input| (lifecycle.index, Box::new(input) as Box<dyn Message>))));
    }

    pub fn record(&mut self) { self.record = true; }

    pub fn handle(&self) -> Handle { self.handle.clone() }

    pub fn start(self) -> std::io::Result<Runtime> {
        let router = Arc::new(Router { boxes: self.boxes, inflight: Inflight::default(), failure: self.failure, history: self.history, record: self.record });
        let _ = self.handle.router.set(router.clone());
        let origin = Instant::now();
        let mut joins = Vec::new();
        let alive = Arc::new(AtomicUsize::new(0));
        for (name, body) in self.bodies {
            let own = router.clone();
            alive.fetch_add(1, Ordering::SeqCst);
            let running = Alive(alive.clone());
            match spawn_thread(Carry::Internal, name.to_string(), move || { let _running = running; body(own, origin) }) {
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

fn elapsed(origin: Instant) -> Time { Time(origin.elapsed().as_millis() as u64) }

struct OwnerThread<O: StateOwner> {
    slot: usize,
    name: &'static str,
    cell: Cell<O>,
    runner: Box<dyn EffectRunner<O>>,
    mailbox: Arc<Mailbox<Command>>,
    history: Arc<Mutex<Vec<TraceEntry>>>,
}

fn run_owner<O: StateOwner>(mut owner: OwnerThread<O>, router: Arc<Router>, origin: Instant) {
    owner.cell.set_jitter(|| rand_core::RngCore::next_u64(&mut rand_core::OsRng));
    let back = {
        let own = owner.slot;
        let router = router.clone();
        Back::<O>::new(move |stamp, input| router.push(own, Command::Deliver(stamp, Box::new(input))))
    };
    loop {
        let cell = &owner.cell;
        let commands = owner.mailbox.take(|| cell.next_due().map(|at| Duration::from_millis(at.0.saturating_sub(elapsed(origin).0))));
        let handled = commands.len();
        for command in commands {
            match command {
                Command::Deliver(stamp, payload) => owner.cell.push_message(stamp, payload),
                Command::Control(control) => owner.cell.push_control(control),
                Command::Stop => return,
            }
        }
        owner.cell.set_now(elapsed(origin));
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
                if let Some((to, payload)) = router.failure.as_ref().and_then(|route| route(owner.name)) {
                    router.push(to, Command::Deliver(EpochStamp::Control, payload));
                }
            }
        }
        router.inflight.done(handled);
    }
}
