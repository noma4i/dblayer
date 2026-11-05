use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt::Debug;

use super::owner::{Back, Cause, Control, Effect, Epoch, Message, Outbound, StateOwner, ScheduleId, EpochStamp, Time};

pub trait EffectRunner<O: StateOwner>: Send + 'static {
    fn run(&mut self, stamp: EpochStamp, effect: O::Effect, back: &Back<O>);
}

enum Item<O: StateOwner> {
    Input(EpochStamp, O::Input),
    EndEpoch(Epoch, Cause),
}

struct Scheduled<I> {
    id: ScheduleId,
    stamp: EpochStamp,
    input: I,
}

pub(crate) struct Ran {
    pub stamp: EpochStamp,
    pub dropped: bool,
    pub input: String,
    pub effects: String,
    pub sends: Vec<Outbound>,
    pub broadcasts: Vec<Control>,
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
}

impl<O: StateOwner> Cell<O> {
    pub fn new(state: O::State) -> Self {
        Self { state, queue: VecDeque::new(), epoch: None, last_epoch: None, schedule: BTreeMap::new(), keys: HashMap::new(), sequence: 0, dropped: 0 }
    }

    pub fn ready(&self) -> bool { !self.queue.is_empty() }

    pub fn dropped(&self) -> u64 { self.dropped }

    pub fn next_due(&self) -> Option<Time> { self.schedule.keys().next().map(|key| key.0) }

    pub fn push(&mut self, stamp: EpochStamp, input: O::Input) { self.queue.push_back(Item::Input(stamp, input)); }

    pub fn push_message(&mut self, stamp: EpochStamp, payload: Box<dyn Message>) {
        match payload.into_any().downcast::<O::Input>() {
            Ok(input) => self.push(stamp, *input),
            Err(_) => self.dropped += 1,
        }
    }

    pub fn close(&mut self) { self.push(EpochStamp::Control, O::closing()); }

    pub fn start_epoch(&mut self, epoch: Epoch) {
        self.epoch = Some(epoch);
        self.purge_schedule(|stamp| stamp != EpochStamp::Epoch(epoch) && stamp != EpochStamp::Control);
    }

    pub fn end_epoch(&mut self, epoch: Epoch, cause: Cause) { self.queue.push_back(Item::EndEpoch(epoch, cause)); }

    pub fn tick(&mut self, now: Time) -> bool {
        let mut due = Vec::new();
        while let Some(entry) = self.schedule.first_entry() {
            if entry.key().0 > now {
                break;
            }
            due.push(entry.remove());
        }
        if due.is_empty() {
            return false;
        }
        self.queue.push_back(Item::Input(EpochStamp::Control, O::tick(now)));
        for scheduled in due {
            self.keys.remove(&scheduled.id);
            self.queue.push_back(Item::Input(scheduled.stamp, scheduled.input));
        }
        true
    }

    pub fn run_one(&mut self, runner: &mut dyn EffectRunner<O>, back: &Back<O>, trace: bool) -> Option<Ran> {
        let (stamp, input, ended) = match self.queue.pop_front()? {
            Item::Input(stamp, input) => {
                if !self.admits(stamp) {
                    return Some(self.drop_input(stamp, label(trace, &input)));
                }
                (stamp, input, false)
            }
            Item::EndEpoch(epoch, cause) => {
                let input = O::epoch_ended(epoch, cause);
                if O::EPOCHED && self.epoch != Some(epoch) {
                    return Some(self.drop_input(EpochStamp::Epoch(epoch), label(trace, &input)));
                }
                (EpochStamp::Epoch(epoch), input, true)
            }
        };
        let input_label = label(trace, &input);
        let effects = O::step(&mut self.state, input);
        let effects_label = label(trace, &effects);
        let out = self.owner_stamp();
        let mut sends = Vec::new();
        let mut broadcasts = Vec::new();
        for effect in effects {
            match effect {
                Effect::Schedule { id, at, input } => self.schedule_input(id, at, out, input),
                Effect::Cancel(id) => self.cancel(id),
                Effect::Send(mut outbound) => {
                    outbound.stamp = out;
                    sends.push(outbound);
                }
                Effect::Broadcast(control) => {
                    assert!(!O::EPOCHED, "only an owner outside epochs broadcasts control inputs");
                    broadcasts.push(control);
                }
                Effect::Run(effect) => runner.run(out, effect, back),
            }
        }
        if ended {
            self.last_epoch = self.epoch.take();
            self.purge_schedule(|stamp| stamp != EpochStamp::Control);
        }
        Some(Ran { stamp, dropped: false, input: input_label, effects: effects_label, sends, broadcasts })
    }

    fn drop_input(&mut self, stamp: EpochStamp, input: String) -> Ran {
        self.dropped += 1;
        Ran { stamp, dropped: true, input, effects: String::new(), sends: Vec::new(), broadcasts: Vec::new() }
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
