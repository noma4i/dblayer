use std::any::Any;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::time::Duration;

use super::cell::{Cell, Emitted, EffectRunner, Ran};
use super::owner::{Address, Back, Cause, Control, EffectId, Epoch, EpochStamp, Message, StateOwner, Time};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceEntry {
    pub slot: usize,
    pub owner: &'static str,
    pub stamp: EpochStamp,
    pub input: String,
    pub effects: String,
    pub dropped: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Lane {
    External(u32),
    Runtime,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Choice {
    Step(usize),
    Emit(usize),
    Deliver(Lane),
    Arrive(usize),
    Fire(usize),
}

enum Pending {
    Send { to: usize, stamp: EpochStamp, payload: Box<dyn Message> },
}

impl Pending {
    fn to(&self) -> usize {
        match self {
            Self::Send { to, .. } => *to,
        }
    }
}

trait ErasedSlot {
    fn name(&self) -> &'static str;
    fn ready(&self) -> bool;
    fn arriving(&self) -> bool;
    fn due(&self) -> bool;
    fn run_one(&mut self) -> Option<Ran>;
    fn emitting(&self) -> bool;
    fn emit_one(&mut self) -> (Option<Emitted>, bool);
    fn finish_emission(&mut self) -> bool;
    /// Takes the oldest arrival; true when it was the failure of an asynchronous task.
    fn arrive(&mut self) -> bool;
    fn fire(&mut self);
    fn deliver(&mut self, stamp: EpochStamp, payload: Box<dyn Message>);
    fn push_control(&mut self, control: Control);
    fn set_now(&mut self, now: Time);
    fn as_any(&self) -> &dyn Any;
}

enum Arrival<O: StateOwner> {
    Input(EpochStamp, O::Input),
    Failed(Vec<EffectId>),
}

type Inbox<O> = Arc<Mutex<VecDeque<Arrival<O>>>>;

struct Slot<O: StateOwner> {
    name: &'static str,
    cell: Cell<O>,
    runner: Box<dyn EffectRunner<O>>,
    back: Back<O>,
    inbox: Inbox<O>,
}

impl<O: StateOwner> ErasedSlot for Slot<O> {
    fn name(&self) -> &'static str { self.name }

    fn ready(&self) -> bool { self.cell.ready() }

    fn arriving(&self) -> bool { !self.inbox.lock().unwrap_or_else(PoisonError::into_inner).is_empty() }

    fn due(&self) -> bool { self.cell.due() }

    fn run_one(&mut self) -> Option<Ran> { self.cell.run_one(self.name, self.runner.as_mut(), &self.back, true) }

    fn emitting(&self) -> bool { self.cell.emitting() }

    fn emit_one(&mut self) -> (Option<Emitted>, bool) { self.cell.emit_one(self.name, self.runner.as_mut(), &self.back) }

    fn finish_emission(&mut self) -> bool { self.cell.finish_emission(self.name, self.runner.as_mut()) }

    fn arrive(&mut self) -> bool {
        let arrived = self.inbox.lock().unwrap_or_else(PoisonError::into_inner).pop_front();
        match arrived {
            Some(Arrival::Input(stamp, input)) => self.cell.push(stamp, input),
            Some(Arrival::Failed(pending)) => {
                let deferred = O::defer_executor_failure(&pending);
                self.cell.apply_failure(self.name, pending);
                return !deferred;
            }
            None => {}
        }
        false
    }

    fn fire(&mut self) { self.cell.fire_next_due(); }

    fn deliver(&mut self, stamp: EpochStamp, payload: Box<dyn Message>) { self.cell.push_message(stamp, payload); }

    fn push_control(&mut self, control: Control) { self.cell.push_control(control); }

    fn set_now(&mut self, now: Time) { self.cell.set_now(now); }

    fn as_any(&self) -> &dyn Any { self }
}

type FailureTarget = Box<dyn Fn(&str) -> Vec<(usize, Box<dyn Message>)>>;

/// Counts the replies that other threads hand to the scheduler, and wakes the thread that waits for the next one.
#[derive(Default)]
struct Arrivals {
    count: Mutex<u64>,
    changed: Condvar,
}

static ARRIVALS: Arrivals = Arrivals { count: Mutex::new(0), changed: Condvar::new() };

/// A mock or a peer of a test changed the state a pump waits on. Any pump rechecks.
pub fn wake_pumps() { ARRIVALS.note(); }

impl Arrivals {
    fn note(&self) {
        *self.count.lock().unwrap_or_else(PoisonError::into_inner) += 1;
        self.changed.notify_all();
    }
}

pub struct DetScheduler {
    slots: Vec<Box<dyn ErasedSlot>>,
    lanes: BTreeMap<Lane, VecDeque<Pending>>,
    random: u64,
    trace: Vec<TraceEntry>,
    now: Time,
    failure_target: Option<FailureTarget>,
    jitter: Arc<AtomicU64>,
}

impl DetScheduler {
    pub fn new(seed: u64) -> Self { Self { slots: Vec::new(), lanes: BTreeMap::new(), random: seed, trace: Vec::new(), now: Time(0), failure_target: None, jitter: Arc::new(AtomicU64::new(seed)) } }

    pub fn add<O: StateOwner>(&mut self, name: &'static str, state: O::State, runner: impl EffectRunner<O>) -> Address<O> {
        let inbox: Inbox<O> = Arc::default();
        let (arrivals, failures) = (inbox.clone(), inbox.clone());
        let back = Back::new(move |stamp, input| { arrivals.lock().unwrap_or_else(PoisonError::into_inner).push_back(Arrival::Input(stamp, input)); wake_pumps(); }, move |pending| { failures.lock().unwrap_or_else(PoisonError::into_inner).push_back(Arrival::Failed(pending)); wake_pumps(); });
        let mut cell = Cell::new(state);
        cell.set_retry_owner(name);
        cell.set_now(self.now);
        let (seed, slot) = (self.jitter.clone(), self.slots.len() as u64);
        let mut draws = 0u64;
        cell.set_jitter(move || {
            draws += 1;
            mix(seed.load(Ordering::SeqCst) ^ (slot << 32) ^ draws)
        });
        self.slots.push(Box::new(Slot::<O> { name, cell, runner: Box::new(runner), back, inbox }));
        Address::new(self.slots.len() - 1)
    }

    pub fn on_executor_failed<L: StateOwner>(&mut self, lifecycle: Address<L>) {
        self.failure_target = Some(Box::new(move |executor| L::executor_failed(executor, Vec::new()).into_iter().map(|input| (lifecycle.index, Box::new(input) as Box<dyn Message>)).collect()));
    }

    pub fn post<O: StateOwner>(&mut self, to: Address<O>, stamp: EpochStamp, input: O::Input) { self.slots[to.index].deliver(stamp, Box::new(input)); }

    pub fn inject<O: StateOwner>(&mut self, lane: u32, to: Address<O>, stamp: EpochStamp, input: O::Input) {
        self.lanes.entry(Lane::External(lane)).or_default().push_back(Pending::Send { to: to.index, stamp, payload: Box::new(input) });
    }

    pub fn start_epoch(&mut self, epoch: Epoch) { self.slots.iter_mut().for_each(|slot| slot.push_control(Control::EpochStarted(epoch))); }

    pub fn start_epoch_for<O: StateOwner>(&mut self, to: Address<O>, epoch: Epoch) { self.slots[to.index].push_control(Control::EpochStarted(epoch)); }

    pub fn broadcast(&mut self, control: Control) { self.slots.iter_mut().for_each(|slot| slot.push_control(control)); }

    pub fn end_epoch(&mut self, epoch: Epoch, cause: Cause) { self.broadcast(Control::EpochEnded(epoch, cause)); }

    pub fn close(&mut self) { self.broadcast(Control::Closing); }

    pub fn tick(&mut self, now: Time) {
        self.now = self.now.max(now);
        let now = self.now;
        self.slots.iter_mut().for_each(|slot| slot.set_now(now));
    }

    pub fn choices(&self) -> Vec<Choice> {
        let mut choices: Vec<Choice> = (0..self.slots.len()).filter(|index| self.slots[*index].ready()).map(Choice::Step).collect();
        choices.extend((0..self.slots.len()).filter(|index| self.slots[*index].emitting()).map(Choice::Emit));
        choices.extend(self.lanes.iter().filter(|(_, queue)| !queue.is_empty()).map(|(lane, _)| Choice::Deliver(*lane)));
        choices.extend((0..self.slots.len()).filter(|index| self.slots[*index].arriving()).map(Choice::Arrive));
        choices.extend((0..self.slots.len()).filter(|index| self.slots[*index].due()).map(Choice::Fire));
        choices
    }

    pub fn ready(&self) -> Vec<usize> {
        let mut owners: BTreeSet<usize> = BTreeSet::new();
        for choice in self.choices() {
            match choice {
                Choice::Step(index) | Choice::Emit(index) | Choice::Arrive(index) | Choice::Fire(index) => owners.insert(index),
                Choice::Deliver(lane) => owners.insert(self.lanes[&lane].front().map_or(0, Pending::to)),
            };
        }
        owners.into_iter().collect()
    }

    pub fn step_choice(&mut self, choice: usize) -> bool {
        let Some(&chosen) = self.choices().get(choice) else { return false };
        self.apply(chosen)
    }

    fn apply(&mut self, choice: Choice) -> bool {
        match choice {
            Choice::Step(index) => {
                let Some(ran) = self.slots[index].run_one() else { return false };
                self.finish_step(index, ran);
            }
            Choice::Emit(index) => {
                let (emitted, failed) = self.slots[index].emit_one();
                match emitted {
                    Some(Emitted::Send(outbound)) => self.slots[outbound.to].deliver(outbound.stamp, outbound.payload),
                    Some(Emitted::Broadcast(control)) => self.slots.iter_mut().for_each(|slot| slot.push_control(control)),
                    None => {}
                }
                let finished_failed = self.slots[index].finish_emission();
                if failed || finished_failed { self.report_failure(index); }
            }
            Choice::Deliver(lane) => {
                let Some(pending) = self.lanes.get_mut(&lane).and_then(VecDeque::pop_front) else { return false };
                match pending {
                    Pending::Send { to, stamp, payload } => self.slots[to].deliver(stamp, payload),
                }
            }
            Choice::Arrive(index) => {
                if self.slots[index].arrive() {
                    self.report_failure(index);
                }
            }
            Choice::Fire(index) => self.slots[index].fire(),
        }
        true
    }

    fn finish_step(&mut self, index: usize, ran: Ran) {
        self.trace.push(TraceEntry { slot: index, owner: self.slots[index].name(), stamp: ran.stamp, input: ran.input, effects: ran.effects, dropped: ran.dropped });
        if ran.failed {
            self.report_failure(index);
        }
    }

    fn report_failure(&mut self, index: usize) {
        let name = self.slots[index].name();
        for (to, payload) in self.failure_target.as_ref().map_or_else(Vec::new, |target| target(name)) {
            self.lanes.entry(Lane::Runtime).or_default().push_back(Pending::Send { to, stamp: EpochStamp::Control, payload });
        }
    }

    pub fn reseed(&mut self, seed: u64) {
        self.random = seed;
        self.jitter.store(seed, Ordering::SeqCst);
    }

    pub fn step(&mut self) -> bool {
        let choices = self.choices();
        if choices.is_empty() {
            return false;
        }
        let pick = (self.next_random() % choices.len() as u64) as usize;
        self.apply(choices[pick])
    }

    /// The number of replies that other threads handed to the scheduler so far.
    pub fn arrivals(&self) -> u64 { *ARRIVALS.count.lock().unwrap_or_else(PoisonError::into_inner) }

    /// Returns once a reply arrived after the count `seen`, or false after `limit`.
    pub fn wait_arrival(&self, seen: u64, limit: Duration) -> bool {
        let count = ARRIVALS.count.lock().unwrap_or_else(PoisonError::into_inner);
        let (count, _) = crate::blocking::blocking_unit(|| ARRIVALS.changed.wait_timeout_while(count, limit, |count| *count == seen).unwrap_or_else(PoisonError::into_inner));
        *count != seen
    }

    pub fn run_until_quiet(&mut self, limit: usize) -> Result<usize, usize> {
        let mut steps = 0;
        loop {
            let choices = self.choices();
            if choices.is_empty() { break; }
            if steps == limit && choices.iter().any(|choice| matches!(choice, Choice::Step(_))) {
                return Err(steps);
            }
            let before = self.trace.len();
            self.step();
            steps += self.trace.len() - before;
        }
        Ok(steps)
    }

    pub fn trace(&self) -> &[TraceEntry] { &self.trace }

    pub fn history<O: StateOwner>(&self, address: Address<O>) -> Vec<&TraceEntry> { self.trace.iter().filter(|entry| entry.slot == address.index).collect() }

    fn slot<O: StateOwner>(&self, address: Address<O>) -> Option<&Slot<O>> { self.slots.get(address.index)?.as_any().downcast_ref::<Slot<O>>() }

    pub fn state<O: StateOwner>(&self, address: Address<O>) -> Option<&O::State> { self.slot(address).map(|slot| &slot.cell.state) }

    pub fn dropped<O: StateOwner>(&self, address: Address<O>) -> u64 { self.slot(address).map_or(0, |slot| slot.cell.dropped()) }

    fn next_random(&mut self) -> u64 {
        self.random = self.random.wrapping_add(0x9E37_79B9_7F4A_7C15);
        mix(self.random.wrapping_sub(0x9E37_79B9_7F4A_7C15))
    }
}

fn mix(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}
