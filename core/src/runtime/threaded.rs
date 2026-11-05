use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use super::cell::{Cell, EffectRunner};
use super::owner::{Address, Cause, Epoch, Message, Owner, Stamp, Time};
use crate::task::{spawn_thread, Carry};

enum Command {
    Deliver(Stamp, Box<dyn Message>),
    StartEpoch(Epoch),
    EndEpoch(Epoch, Cause),
    Close,
    Stop,
}

struct Mailbox {
    commands: Mutex<VecDeque<Command>>,
    wake: Condvar,
    wakeups: AtomicU64,
}

impl Mailbox {
    fn push(&self, command: Command) {
        lock(&self.commands).push_back(command);
        self.wake.notify_one();
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> { mutex.lock().unwrap_or_else(PoisonError::into_inner) }

struct Router {
    boxes: Vec<Arc<Mailbox>>,
}

type Body = Box<dyn FnOnce(Arc<Router>, Instant) + Send>;

#[derive(Default)]
pub struct RuntimeBuilder {
    boxes: Vec<Arc<Mailbox>>,
    bodies: Vec<(String, Body)>,
}

pub struct Runtime {
    router: Arc<Router>,
    joins: Vec<JoinHandle<()>>,
}

impl RuntimeBuilder {
    pub fn new() -> Self { Self::default() }

    pub fn add<O: Owner>(&mut self, name: &str, state: O::State, runner: impl EffectRunner<O>) -> Address<O> {
        let mailbox = Arc::new(Mailbox { commands: Mutex::new(VecDeque::new()), wake: Condvar::new(), wakeups: AtomicU64::new(0) });
        let own = mailbox.clone();
        self.boxes.push(mailbox);
        let runner: Box<dyn EffectRunner<O>> = Box::new(runner);
        self.bodies.push((name.to_string(), Box::new(move |router, origin| run_owner::<O>(Cell::new(state), runner, own, router, origin))));
        Address::new(self.boxes.len() - 1)
    }

    pub fn start(self) -> std::io::Result<Runtime> {
        let router = Arc::new(Router { boxes: self.boxes });
        let origin = Instant::now();
        let mut joins = Vec::new();
        for (name, body) in self.bodies {
            let router = router.clone();
            joins.push(spawn_thread(Carry::Internal, name, move || body(router, origin))?);
        }
        Ok(Runtime { router, joins })
    }
}

impl Runtime {
    pub fn send<O: Owner>(&self, to: Address<O>, stamp: Stamp, input: O::Input) { self.router.boxes[to.index].push(Command::Deliver(stamp, Box::new(input))); }

    pub fn start_epoch(&self, epoch: Epoch) { self.each(|| Command::StartEpoch(epoch)); }

    pub fn end_epoch(&self, epoch: Epoch, cause: Cause) { self.each(|| Command::EndEpoch(epoch, cause)); }

    pub fn close(&self) { self.each(|| Command::Close); }

    pub fn wakeups<O: Owner>(&self, address: Address<O>) -> u64 { self.router.boxes[address.index].wakeups.load(Ordering::SeqCst) }

    fn each(&self, command: impl Fn() -> Command) { self.router.boxes.iter().for_each(|mailbox| mailbox.push(command())); }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        self.each(|| Command::Stop);
        for join in self.joins.drain(..) {
            let _ = join.join();
        }
    }
}

fn elapsed(origin: Instant) -> Time { Time(origin.elapsed().as_millis() as u64) }

fn run_owner<O: Owner>(mut cell: Cell<O>, mut runner: Box<dyn EffectRunner<O>>, mailbox: Arc<Mailbox>, router: Arc<Router>, origin: Instant) {
    loop {
        let commands = {
            let mut queue = lock(&mailbox.commands);
            while queue.is_empty() {
                match cell.next_due() {
                    None => queue = mailbox.wake.wait(queue).unwrap_or_else(PoisonError::into_inner),
                    Some(at) => {
                        let now = elapsed(origin);
                        if at <= now {
                            break;
                        }
                        queue = mailbox.wake.wait_timeout(queue, Duration::from_millis(at.0 - now.0)).unwrap_or_else(PoisonError::into_inner).0;
                    }
                }
                mailbox.wakeups.fetch_add(1, Ordering::SeqCst);
            }
            std::mem::take(&mut *queue)
        };
        for command in commands {
            match command {
                Command::Deliver(stamp, payload) => cell.push_message(stamp, payload),
                Command::StartEpoch(epoch) => cell.start_epoch(epoch),
                Command::EndEpoch(epoch, cause) => cell.end_epoch(epoch, cause),
                Command::Close => cell.close(),
                Command::Stop => return,
            }
        }
        cell.tick(elapsed(origin));
        while let Some(ran) = cell.run_one(runner.as_mut(), false) {
            for outbound in ran.sends {
                router.boxes[outbound.to].push(Command::Deliver(outbound.stamp, outbound.payload));
            }
        }
    }
}
