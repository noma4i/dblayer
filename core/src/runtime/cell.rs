use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt::Debug;
use std::panic::{catch_unwind, AssertUnwindSafe};

use super::owner::{Back, Cause, Control, Effect, EffectId, Epoch, EpochStamp, Message, Outbound, ScheduleId, StateOwner, Time};

pub trait EffectRunner<O: StateOwner>: Send + 'static {
    fn run(&mut self, stamp: EpochStamp, effect: O::Effect, back: &Back<O>);
    /// Called once after all effects of one `step` ran: the results of a step become visible together.
    fn step_done(&mut self) {}
    fn pending(&mut self) -> Vec<EffectId> { Vec::new() }
}

enum Item<O: StateOwner> {
    Input(EpochStamp, O::Input),
    Start(Epoch),
    End(Epoch, Cause),
}

struct Scheduled<I> {
    id: ScheduleId,
    stamp: EpochStamp,
    input: I,
}

pub(crate) enum Emitted {
    Send(Outbound),
    Broadcast(Control),
}

pub(crate) struct Ran {
    pub stamp: EpochStamp,
    pub dropped: bool,
    pub input: String,
    pub effects: String,
    pub emitted: Vec<Emitted>,
    pub failed: bool,
}

pub(crate) struct Cell<O: StateOwner> {
    pub state: O::State,
    queue: VecDeque<Item<O>>,
    epoch: Option<Epoch>,
    last_epoch: Option<Epoch>,
    schedule: BTreeMap<(Time, u64), Scheduled<O::Input>>,
    keys: HashMap<ScheduleId, (Time, u64)>,
    sequence: u64,
    dropped: u64,
    now: Time,
    executor_dead: bool,
    jitter: Box<dyn FnMut() -> u64 + Send>,
}

impl<O: StateOwner> Cell<O> {
    pub fn new(state: O::State) -> Self {
        Self { state, queue: VecDeque::new(), epoch: None, last_epoch: None, schedule: BTreeMap::new(), keys: HashMap::new(), sequence: 0, dropped: 0, now: Time(0), executor_dead: false, jitter: Box::new(|| 0) }
    }

    pub fn set_jitter(&mut self, jitter: impl FnMut() -> u64 + Send + 'static) { self.jitter = Box::new(jitter); }

    pub fn ready(&self) -> bool { !self.queue.is_empty() }

    pub fn dropped(&self) -> u64 { self.dropped }

    pub fn next_due(&self) -> Option<Time> { self.schedule.keys().next().map(|key| key.0) }

    pub fn due(&self) -> bool { self.next_due().is_some_and(|at| at <= self.now) }

    pub fn push(&mut self, stamp: EpochStamp, input: O::Input) {
        self.queue.push_back(Item::Input(stamp, input));
        self.settle_head();
    }

    pub fn push_message(&mut self, stamp: EpochStamp, payload: Box<dyn Message>) {
        match payload.into_any().downcast::<O::Input>() {
            Ok(input) => self.push(stamp, *input),
            Err(_) => self.dropped += 1,
        }
    }

    pub fn push_control(&mut self, control: Control) {
        match control {
            Control::EpochStarted(epoch) => self.start_epoch(epoch),
            Control::EpochEnded(epoch, cause) => self.end_epoch(epoch, cause),
            Control::Closing => self.close(),
        }
    }

    pub fn close(&mut self) { self.push(EpochStamp::Control, O::closing()); }

    pub fn start_epoch(&mut self, epoch: Epoch) {
        self.queue.push_back(Item::Start(epoch));
        self.settle_head();
    }

    pub fn end_epoch(&mut self, epoch: Epoch, cause: Cause) { self.queue.push_back(Item::End(epoch, cause)); }

    pub fn set_now(&mut self, now: Time) { self.now = self.now.max(now); }

    pub fn fire_next_due(&mut self) -> bool {
        if !self.due() {
            return false;
        }
        let Some(entry) = self.schedule.first_entry() else { return false };
        let scheduled = entry.remove();
        self.keys.remove(&scheduled.id);
        self.push(EpochStamp::Control, O::tick(self.now));
        self.push(scheduled.stamp, scheduled.input);
        true
    }

    pub fn run_one(&mut self, name: &str, runner: &mut dyn EffectRunner<O>, back: &Back<O>, trace: bool) -> Option<Ran> {
        let (stamp, input, ended) = match self.queue.pop_front()? {
            Item::Input(stamp, input) => {
                if !self.admits(stamp) {
                    let ran = self.drop_input(stamp, label(trace, &input));
                    self.settle_head();
                    return Some(ran);
                }
                (stamp, input, false)
            }
            Item::End(epoch, cause) => {
                let input = O::epoch_ended(epoch, cause);
                if O::EPOCHED && self.epoch != Some(epoch) {
                    let ran = self.drop_input(EpochStamp::Epoch(epoch), label(trace, &input));
                    self.settle_head();
                    return Some(ran);
                }
                (EpochStamp::Epoch(epoch), input, true)
            }
            Item::Start(epoch) => {
                self.begin_epoch(epoch);
                return self.run_one(name, runner, back, trace);
            }
        };
        let input_label = label(trace, &input);
        let mut failed = false;
        let mut failures: Vec<EffectId> = Vec::new();
        let effects = match catch_unwind(AssertUnwindSafe(|| O::step(&mut self.state, input))) {
            Ok(effects) => effects,
            Err(_) => {
                self.executor_dead = true;
                failed = true;
                failures.extend(catch_unwind(AssertUnwindSafe(|| runner.pending())).unwrap_or_default());
                Vec::new()
            }
        };
        let effects_label = label(trace, &effects);
        let out = self.owner_stamp();
        let mut emitted = Vec::new();
        for effect in effects {
            match effect {
                Effect::Schedule { id, after, input } => {
                    let at = Time(self.now.0.saturating_add(u64::try_from(after.as_millis()).unwrap_or(u64::MAX)));
                    self.schedule_input(id, at, out, input);
                }
                Effect::Backoff { id, policy, attempt, input } => {
                    let delay = policy.with_jitter(policy.jitter, (self.jitter)()).delay(attempt);
                    let at = Time(self.now.0.saturating_add(u64::try_from(delay.as_millis()).unwrap_or(u64::MAX)));
                    self.schedule_input(id, at, out, input);
                }
                Effect::Cancel(id) => self.cancel(id),
                Effect::Send(mut outbound) => {
                    outbound.stamp = out;
                    emitted.push(Emitted::Send(outbound));
                }
                Effect::Broadcast(control) => {
                    assert!(!O::EPOCHED, "only an owner outside epochs broadcasts control inputs");
                    emitted.push(Emitted::Broadcast(control));
                }
                Effect::Run(effect) => {
                    let id = O::effect_id(&effect);
                    if self.executor_dead {
                        failures.extend(id);
                        continue;
                    }
                    if catch_unwind(AssertUnwindSafe(|| runner.run(out, effect, back))).is_err() {
                        self.executor_dead = true;
                        failed = true;
                        failures.extend(catch_unwind(AssertUnwindSafe(|| runner.pending())).unwrap_or_default());
                        failures.extend(id);
                    }
                }
            }
        }
        if !self.executor_dead {
            runner.step_done();
        }
        if ended {
            self.last_epoch = self.epoch.take();
            self.purge_schedule(|stamp| stamp != EpochStamp::Control);
        }
        failures.sort_unstable();
        failures.dedup();
        if failed || !failures.is_empty() {
            if let Some(input) = O::executor_failed(name, failures) {
                self.push(EpochStamp::Control, input);
            }
        }
        self.settle_head();
        Some(Ran { stamp, dropped: false, input: input_label, effects: effects_label, emitted, failed })
    }

    fn settle_head(&mut self) {
        while let Some(Item::Start(epoch)) = self.queue.front() {
            let epoch = *epoch;
            self.queue.pop_front();
            self.begin_epoch(epoch);
        }
    }

    fn begin_epoch(&mut self, epoch: Epoch) {
        self.epoch = Some(epoch);
        O::epoch_started(&mut self.state, epoch);
        self.purge_schedule(|stamp| stamp != EpochStamp::Epoch(epoch) && stamp != EpochStamp::Control);
    }

    fn drop_input(&mut self, stamp: EpochStamp, input: String) -> Ran {
        self.dropped += 1;
        Ran { stamp, dropped: true, input, effects: String::new(), emitted: Vec::new(), failed: false }
    }

    fn owner_stamp(&self) -> EpochStamp {
        if O::EPOCHED { EpochStamp::Epoch(self.epoch.or(self.last_epoch).unwrap_or(0)) } else { EpochStamp::Control }
    }

    fn admits(&self, stamp: EpochStamp) -> bool {
        match stamp {
            EpochStamp::Control => true,
            EpochStamp::Epoch(epoch) => !O::EPOCHED || self.epoch == Some(epoch),
        }
    }

    fn schedule_input(&mut self, id: ScheduleId, at: Time, stamp: EpochStamp, input: O::Input) {
        self.cancel(id);
        let key = (at, self.sequence);
        self.sequence += 1;
        self.keys.insert(id, key);
        self.schedule.insert(key, Scheduled { id, stamp, input });
    }

    fn cancel(&mut self, id: ScheduleId) {
        if let Some(key) = self.keys.remove(&id) {
            self.schedule.remove(&key);
        }
    }

    fn purge_schedule(&mut self, remove: impl Fn(EpochStamp) -> bool) {
        self.schedule.retain(|_, scheduled| !remove(scheduled.stamp));
        let live: HashSet<ScheduleId> = self.schedule.values().map(|scheduled| scheduled.id).collect();
        self.keys.retain(|id, _| live.contains(id));
    }
}

fn label(trace: bool, value: &impl Debug) -> String { if trace { format!("{value:?}") } else { String::new() } }
