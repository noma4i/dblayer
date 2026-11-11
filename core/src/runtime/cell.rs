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
    retry: bool,
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
    pub failed: bool,
}

struct PendingStep<O: StateOwner> {
    effects: VecDeque<Effect<O>>,
    stamp: EpochStamp,
    ended: bool,
    failed: bool,
    failures: Vec<EffectId>,
}

pub(crate) struct Cell<O: StateOwner> {
    pub state: O::State,
    queue: VecDeque<Item<O>>,
    pending: Option<PendingStep<O>>,
    epoch: Option<Epoch>,
    last_epoch: Option<Epoch>,
    schedule: BTreeMap<(Time, u64), Scheduled<O::Input>>,
    keys: HashMap<ScheduleId, (Time, u64)>,
    sequence: u64,
    dropped: u64,
    now: Time,
    executor_dead: bool,
    jitter: Box<dyn FnMut() -> u64 + Send>,
    retry_owner: Option<crate::transport::RetryOwner>,
}

impl<O: StateOwner> Cell<O> {
    pub fn new(state: O::State) -> Self {
        Self { state, queue: VecDeque::new(), pending: None, epoch: None, last_epoch: None, schedule: BTreeMap::new(), keys: HashMap::new(), sequence: 0, dropped: 0, now: Time(0), executor_dead: false, jitter: Box::new(super::process_seed), retry_owner: None }
    }

    pub fn set_retry_owner(&mut self, name: &str) {
        use crate::transport::RetryOwner::*;
        self.retry_owner = match name { "session" => Some(Session), "windows" => Some(Window), "calls" => Some(Calls), "outbox" => Some(Outbox), "transfer" => Some(Transfer), "feed" => Some(Feed), _ => None };
    }

    pub fn set_jitter(&mut self, jitter: impl FnMut() -> u64 + Send + 'static) { self.jitter = Box::new(jitter); }

    pub fn ready(&self) -> bool { self.pending.is_none() && !self.queue.is_empty() }

    pub fn emitting(&self) -> bool { self.pending.is_some() }

    pub fn dropped(&self) -> u64 { self.dropped }

    pub fn next_due(&self) -> Option<Time> { self.schedule.keys().next().map(|key| key.0) }

    pub fn due(&self) -> bool { self.next_due().is_some_and(|at| at <= self.now) }

    pub fn push(&mut self, stamp: EpochStamp, input: O::Input) {
        self.queue.push_back(Item::Input(stamp, input));
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
        if scheduled.retry { self.leave_retry(scheduled.id); }
        let (_, input) = O::due(scheduled.input, self.now);
        self.push(scheduled.stamp, input);
        true
    }

    pub fn run_one(&mut self, name: &str, runner: &mut dyn EffectRunner<O>, _back: &Back<O>, trace: bool) -> Option<Ran> {
        if self.pending.is_some() { return None; }
        let (stamp, input, ended) = match self.queue.pop_front()? {
            Item::Input(stamp, input) => {
                if !self.admits(stamp) {
                    let ran = self.drop_input(stamp, label(trace, &input));
                    return Some(ran);
                }
                (stamp, input, false)
            }
            Item::End(epoch, cause) => {
                let input = O::epoch_ended(epoch, cause);
                if O::EPOCHED && self.epoch != Some(epoch) {
                    let ran = self.drop_input(EpochStamp::Epoch(epoch), label(trace, &input));
                    return Some(ran);
                }
                (EpochStamp::Epoch(epoch), input, true)
            }
            Item::Start(epoch) => {
                self.epoch = Some(epoch);
                self.purge_schedule(|stamp| stamp != EpochStamp::Epoch(epoch) && stamp != EpochStamp::Control);
                let Some(input) = O::epoch_started(epoch) else {
                    return Some(Ran { stamp: EpochStamp::Control, dropped: false, input: if trace { format!("EpochStarted({epoch})") } else { String::new() }, effects: String::new(), failed: false });
                };
                (EpochStamp::Control, input, false)
            }
        };
        let input_label = label(trace, &input);
        let mut failed = false;
        let mut failures: Vec<EffectId> = Vec::new();
        let effects = match catch_unwind(AssertUnwindSafe(|| O::step(&mut self.state, input, self.now))) {
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
        self.pending = Some(PendingStep { effects: effects.into(), stamp: out, ended, failed, failures });
        let failed = if self.pending.as_ref().is_some_and(|pending| pending.effects.is_empty()) { self.finish_step(name, runner) } else { false };
        Some(Ran { stamp, dropped: false, input: input_label, effects: effects_label, failed })
    }

    pub fn emit_one(&mut self, name: &str, runner: &mut dyn EffectRunner<O>, back: &Back<O>) -> (Option<Emitted>, bool) {
        let Some(mut pending) = self.pending.take() else { return (None, false) };
        let Some(effect) = pending.effects.pop_front() else { self.pending = Some(pending); return (None, self.finish_step(name, runner)) };
        let out = pending.stamp;
        let emitted = match effect {
                Effect::Schedule { id, after, input } => {
                    let at = Time(self.now.0.saturating_add(u64::try_from(after.as_millis()).unwrap_or(u64::MAX)));
                    self.schedule_input(id, at, out, input, false);
                    None
                }
                Effect::ScheduleAt { id, at, input } => {
                    self.schedule_input(id, at, out, input, false);
                    None
                }
                Effect::Backoff { id, policy, attempt, input } => {
                    let delay = policy.delay_seeded(attempt, (self.jitter)());
                    let at = Time(self.now.0.saturating_add(u64::try_from(delay.as_millis()).unwrap_or(u64::MAX)));
                    self.schedule_input(id, at, out, input, true);
                    if let Some(owner) = self.retry_owner {
                        crate::transport::retry::retry_progress().record(crate::transport::RetryEntry { owner, kind: policy, id: id.0, attempt, delay_ms: u64::try_from(delay.as_millis()).unwrap_or(u64::MAX) });
                    }
                    None
                }
                Effect::Cancel(id) => { self.cancel(id); None }
                Effect::Send(mut outbound) => {
                    outbound.stamp = out;
                    Some(Emitted::Send(outbound))
                }
                Effect::Broadcast(control) => {
                    assert!(!O::EPOCHED, "only an owner outside epochs broadcasts control inputs");
                    Some(Emitted::Broadcast(control))
                }
                Effect::Run(effect) => {
                    let id = O::effect_id(&effect);
                    if self.executor_dead && id.is_some() {
                        pending.failures.extend(id);
                    } else if catch_unwind(AssertUnwindSafe(|| runner.run(out, effect, back))).is_err() {
                        self.executor_dead = true;
                        pending.failed = true;
                        pending.failures.extend(catch_unwind(AssertUnwindSafe(|| runner.pending())).unwrap_or_default());
                        pending.failures.extend(id);
                    }
                    None
                }
            };
        self.pending = Some(pending);
        let failed = if emitted.is_none() { self.finish_emission(name, runner) } else { false };
        (emitted, failed)
    }

    pub fn finish_emission(&mut self, name: &str, runner: &mut dyn EffectRunner<O>) -> bool {
        if self.pending.as_ref().is_some_and(|pending| pending.effects.is_empty()) { self.finish_step(name, runner) } else { false }
    }

    fn finish_step(&mut self, name: &str, runner: &mut dyn EffectRunner<O>) -> bool {
        let Some(mut pending) = self.pending.take() else { return false };
        if !self.executor_dead && catch_unwind(AssertUnwindSafe(|| runner.step_done())).is_err() {
            self.executor_dead = true;
            pending.failed = true;
            pending.failures.extend(catch_unwind(AssertUnwindSafe(|| runner.pending())).unwrap_or_default());
        }
        if pending.ended {
            self.last_epoch = self.epoch.take();
            self.purge_schedule(|stamp| stamp != EpochStamp::Control);
        }
        if pending.failed || !pending.failures.is_empty() {
            self.apply_failure(name, pending.failures);
        }
        pending.failed
    }

    /// The pending effects of a dead executor get their failure inputs, in the order of the ids.
    pub fn apply_failure(&mut self, name: &str, mut pending: Vec<EffectId>) {
        pending.sort_unstable();
        pending.dedup();
        for input in O::executor_failed(name, pending) {
            self.push(EpochStamp::Control, input);
        }
    }

    fn drop_input(&mut self, stamp: EpochStamp, input: String) -> Ran {
        self.dropped += 1;
        Ran { stamp, dropped: true, input, effects: String::new(), failed: false }
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

    pub fn scheduled(&self) -> u64 { self.sequence }

    fn schedule_input(&mut self, id: ScheduleId, at: Time, stamp: EpochStamp, input: O::Input, retry: bool) {
        self.cancel(id);
        let key = (at, self.sequence);
        self.sequence += 1;
        self.keys.insert(id, key);
        self.schedule.insert(key, Scheduled { id, stamp, input, retry });
    }

    fn cancel(&mut self, id: ScheduleId) {
        if let Some(key) = self.keys.remove(&id) {
            if self.schedule.remove(&key).is_some_and(|scheduled| scheduled.retry) { self.leave_retry(id); }
        }
    }

    fn purge_schedule(&mut self, remove: impl Fn(EpochStamp) -> bool) {
        let removed: Vec<_> = self.schedule.values().filter(|scheduled| scheduled.retry && remove(scheduled.stamp)).map(|scheduled| scheduled.id).collect();
        self.schedule.retain(|_, scheduled| !remove(scheduled.stamp));
        for id in removed { self.leave_retry(id); }
        let live: HashSet<ScheduleId> = self.schedule.values().map(|scheduled| scheduled.id).collect();
        self.keys.retain(|id, _| live.contains(id));
    }

    fn leave_retry(&self, id: ScheduleId) {
        if let Some(owner) = self.retry_owner { crate::transport::retry::retry_progress().leave(owner, id.0); }
    }
}

impl<O: StateOwner> Drop for Cell<O> {
    fn drop(&mut self) {
        for scheduled in self.schedule.values().filter(|scheduled| scheduled.retry) { self.leave_retry(scheduled.id); }
    }
}

fn label(trace: bool, value: &impl Debug) -> String { if trace { format!("{value:?}") } else { String::new() } }
