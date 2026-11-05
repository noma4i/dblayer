//! Owner `Deliveries`: a dictionary of `Delivery` machines by stream id (`specs/machines/delivery.md`, one machine per stream).
//! Only this owner hands values to the platform. A table row applies to the machine of the stream the input names;
//! `Committed`, `EpochStarted`, `EpochEnded`, `ModelsReset`, `Closing` apply to every machine.

use std::collections::{BTreeMap, VecDeque};

use super::contract::{Committed, Drained, EpochEnded, EpochStarted, ModelsReset};
use crate::runtime::{Cause, Effect, Effects, Epoch, StateOwner, Time};

pub const JOURNAL_LIMIT: usize = 256;
/// A read that moves more rows than this arrives as a `Snapshot`, not a `Delta`.
pub const DELTA_LIMIT: usize = 256;

pub type StreamId = u64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Rows,
    Events,
}

/// What a stream shows: `tables` are the tables of its rows, `model` is the model of its rows or events.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct View {
    pub kind: Kind,
    pub model: String,
    pub tables: Vec<String>,
}

/// A row of a snapshot: `stamp` changes when the content of the row changes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamRow {
    pub id: String,
    pub stamp: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Idle,
    Loading,
    Loaded,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoadState {
    pub phase: Phase,
    pub error: Option<String>,
    pub loaded: Option<i64>,
}

impl LoadState {
    pub fn idle() -> Self { Self { phase: Phase::Idle, error: None, loaded: None } }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EndCause {
    SessionEnded,
    Closed,
    Cancelled,
    Invalid,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    Snapshot { revision: i64, rows: Vec<StreamRow>, state: LoadState },
    Delta { base: i64, revision: i64, upserted: Vec<StreamRow>, removed: Vec<String> },
    Event { position: u64, payload: String },
    Lost { first: u64, last: u64 },
    Ended(EndCause),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StreamEffect {
    Read(i64),
    DropOldRead,
    InterestPlus,
    InterestMinus,
    WantPlus,
    WantMinus,
    /// The answer of `Pull`: the first value of the journal.
    Deliver(Value),
    /// The answer of `Pull`: the journal is empty, the reader waits for `Wake`.
    ReaderWaits,
    /// The answer of `Pull` after `Ended`: the stream is over.
    Done,
    /// The journal was empty and got a value: wake the reader.
    Wake,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeliveryEffect {
    Stream { stream: StreamId, effect: StreamEffect },
    Drained(Drained),
}

#[derive(Clone, Debug)]
pub enum DeliveryInput {
    Open { stream: StreamId, view: View, epoch: Epoch, revision: i64 },
    Committed(Committed),
    ReadDone { stream: StreamId, revision: i64, rows: Vec<StreamRow> },
    ReadFailed { stream: StreamId, revision: i64, error: String },
    WindowState { stream: StreamId, state: LoadState },
    ModelEvent { stream: StreamId, position: u64, payload: String },
    ModelLost { stream: StreamId, first: u64, last: u64 },
    Pull { stream: StreamId },
    Cancel { stream: StreamId },
    EpochStarted(EpochStarted),
    EpochEnded(EpochEnded),
    ModelsReset(ModelsReset),
    Closing,
    Tick(Time),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeliveryStage {
    Opening,
    Live,
    Ended,
}

/// One machine of `delivery.md`.
#[derive(Debug)]
struct Delivery {
    epoch: Epoch,
    view: View,
    stage: DeliveryStage,
    read: i64,
    revision: i64,
    rows: Vec<StreamRow>,
    window: LoadState,
    next: u64,
    journal: VecDeque<Value>,
    ended_given: bool,
}

impl Delivery {
    fn open(view: View, epoch: Epoch, revision: i64, fx: &mut Vec<StreamEffect>) -> Self {
        let mut machine = Self { epoch, view, stage: DeliveryStage::Opening, read: revision, revision: 0, rows: Vec::new(), window: LoadState::idle(), next: 1, journal: VecDeque::new(), ended_given: false };
        match machine.view.kind {
            Kind::Rows => fx.extend([StreamEffect::Read(revision), StreamEffect::InterestPlus]),
            Kind::Events => {
                machine.stage = DeliveryStage::Live;
                fx.push(StreamEffect::WantPlus);
            }
        }
        machine
    }

    fn stale(view: View, epoch: Epoch, fx: &mut Vec<StreamEffect>) -> Self {
        let mut machine = Self { epoch, view, stage: DeliveryStage::Ended, read: 0, revision: 0, rows: Vec::new(), window: LoadState::idle(), next: 1, journal: VecDeque::new(), ended_given: false };
        machine.push(Value::Ended(EndCause::SessionEnded), fx);
        machine
    }

    fn push(&mut self, value: Value, fx: &mut Vec<StreamEffect>) {
        if self.journal.is_empty() {
            fx.push(StreamEffect::Wake);
        }
        if matches!(value, Value::Snapshot { .. }) {
            self.journal.retain(|kept| !matches!(kept, Value::Delta { .. } | Value::Snapshot { .. }));
        }
        self.journal.push_back(value);
        if self.journal.len() <= JOURNAL_LIMIT {
            return;
        }
        let (mut first, mut last, mut at) = (u64::MAX, 0u64, None);
        while self.journal.len() > JOURNAL_LIMIT - 1 {
            let Some(index) = self.journal.iter().position(|kept| matches!(kept, Value::Event { .. } | Value::Lost { .. })) else { break };
            at.get_or_insert(index);
            match self.journal.remove(index) {
                Some(Value::Event { position, .. }) => {
                    first = first.min(position);
                    last = last.max(position);
                }
                Some(Value::Lost { first: from, last: to }) => {
                    first = first.min(from);
                    last = last.max(to);
                }
                _ => {}
            }
        }
        if let Some(index) = at {
            self.journal.insert(index, Value::Lost { first, last });
        }
    }

    fn end(&mut self, cause: EndCause, fx: &mut Vec<StreamEffect>) {
        self.stage = DeliveryStage::Ended;
        self.push(Value::Ended(cause), fx);
    }

    fn leave(&self) -> StreamEffect {
        match self.view.kind {
            Kind::Rows => StreamEffect::InterestMinus,
            Kind::Events => StreamEffect::WantMinus,
        }
    }

    fn pull(&mut self, fx: &mut Vec<StreamEffect>) {
        match self.journal.pop_front() {
            Some(value) => {
                self.ended_given |= matches!(value, Value::Ended(_));
                fx.push(StreamEffect::Deliver(value));
            }
            None if self.ended_given => fx.push(StreamEffect::Done),
            None => fx.push(StreamEffect::ReaderWaits),
        }
    }

    fn touches(&self, committed: &Committed) -> bool { committed.tables.iter().any(|table| self.view.tables.contains(table)) }

    fn committed(&mut self, committed: &Committed, fx: &mut Vec<StreamEffect>) {
        if !self.touches(committed) {
            return;
        }
        match self.stage {
            DeliveryStage::Opening => {
                self.read = committed.revision;
                fx.extend([StreamEffect::Read(committed.revision), StreamEffect::DropOldRead]);
            }
            DeliveryStage::Live => fx.push(StreamEffect::Read(committed.revision)),
            DeliveryStage::Ended => {}
        }
    }

    fn read_done(&mut self, revision: i64, rows: Vec<StreamRow>, fx: &mut Vec<StreamEffect>) {
        match self.stage {
            DeliveryStage::Opening => {
                self.push(Value::Snapshot { revision, rows: rows.clone(), state: self.window.clone() }, fx);
                self.revision = revision;
                self.rows = rows;
                self.stage = DeliveryStage::Live;
            }
            DeliveryStage::Live if revision > self.revision && rows != self.rows => {
                let upserted: Vec<StreamRow> = rows.iter().filter(|row| !self.rows.contains(row)).cloned().collect();
                let removed: Vec<String> = self.rows.iter().filter(|row| !rows.iter().any(|now| now.id == row.id)).map(|row| row.id.clone()).collect();
                let base = self.revision;
                if upserted.len() + removed.len() > DELTA_LIMIT {
                    self.push(Value::Snapshot { revision, rows: rows.clone(), state: self.window.clone() }, fx);
                } else {
                    self.push(Value::Delta { base, revision, upserted, removed }, fx);
                }
                self.revision = revision;
                self.rows = rows;
            }
            DeliveryStage::Live | DeliveryStage::Ended => {}
        }
    }

    fn read_failed(&mut self, revision: i64, error: String, fx: &mut Vec<StreamEffect>) {
        match self.stage {
            DeliveryStage::Opening => {
                let state = LoadState { phase: Phase::Failed, error: Some(error), loaded: None };
                self.push(Value::Snapshot { revision, rows: Vec::new(), state: state.clone() }, fx);
                self.revision = revision;
                self.rows = Vec::new();
                self.window = state;
                self.stage = DeliveryStage::Live;
            }
            DeliveryStage::Live => {
                let phase = if self.rows.is_empty() { Phase::Failed } else { Phase::Loaded };
                let state = LoadState { phase, error: Some(error), loaded: self.window.loaded };
                self.push(Value::Snapshot { revision: self.revision, rows: self.rows.clone(), state: state.clone() }, fx);
                self.window = state;
            }
            DeliveryStage::Ended => {}
        }
    }

    fn window_state(&mut self, state: LoadState, fx: &mut Vec<StreamEffect>) {
        match self.stage {
            DeliveryStage::Opening => self.window = state,
            DeliveryStage::Live => {
                if state != self.window {
                    self.push(Value::Snapshot { revision: self.revision, rows: self.rows.clone(), state: state.clone() }, fx);
                }
                self.window = state;
            }
            DeliveryStage::Ended => {}
        }
    }

    fn model_event(&mut self, position: u64, payload: String, fx: &mut Vec<StreamEffect>) {
        if self.stage != DeliveryStage::Live || self.view.kind != Kind::Events || position < self.next {
            return;
        }
        if position > self.next {
            self.push(Value::Lost { first: self.next, last: position - 1 }, fx);
        }
        self.push(Value::Event { position, payload }, fx);
        self.next = position + 1;
    }

    fn model_lost(&mut self, first: u64, last: u64, fx: &mut Vec<StreamEffect>) {
        if self.stage != DeliveryStage::Live || self.view.kind != Kind::Events {
            return;
        }
        self.push(Value::Lost { first, last }, fx);
        self.next = last + 1;
    }

    fn models_reset(&mut self, reset: &ModelsReset, fx: &mut Vec<StreamEffect>) {
        if self.view.kind != Kind::Rows || !reset.models.contains(&self.view.model) {
            return;
        }
        match self.stage {
            DeliveryStage::Opening => fx.extend([StreamEffect::Read(self.read), StreamEffect::DropOldRead]),
            DeliveryStage::Live => fx.push(StreamEffect::Read(self.revision)),
            DeliveryStage::Ended => {}
        }
    }

    fn cancel(&mut self, fx: &mut Vec<StreamEffect>) {
        if self.stage == DeliveryStage::Ended {
            return;
        }
        let leave = self.leave();
        self.end(EndCause::Cancelled, fx);
        fx.push(leave);
    }

    fn epoch_ended(&mut self, ended: &EpochEnded, fx: &mut Vec<StreamEffect>) {
        if self.stage == DeliveryStage::Ended || ended.epoch != self.epoch {
            return;
        }
        let leave = self.leave();
        self.end(EndCause::SessionEnded, fx);
        fx.push(leave);
    }

    fn closing(&mut self, fx: &mut Vec<StreamEffect>) {
        if self.stage != DeliveryStage::Ended {
            self.end(EndCause::Closed, fx);
        }
    }
}

/// The state of the owner: the epoch it knows and the machines.
pub struct DeliveriesState {
    epoch: Epoch,
    streams: BTreeMap<StreamId, Delivery>,
}

impl DeliveriesState {
    pub fn new(epoch: Epoch) -> Self { Self { epoch, streams: BTreeMap::new() } }

    pub fn stage(&self, stream: StreamId) -> Option<DeliveryStage> { self.streams.get(&stream).map(|machine| machine.stage) }

    pub fn journal_len(&self, stream: StreamId) -> usize { self.streams.get(&stream).map_or(0, |machine| machine.journal.len()) }

    pub fn streams(&self) -> usize { self.streams.len() }

    /// The unread values of a stream, oldest first.
    pub fn journal(&self, stream: StreamId) -> Vec<Value> { self.streams.get(&stream).map(|machine| machine.journal.iter().cloned().collect()).unwrap_or_default() }
}

pub struct Deliveries;

fn keyed(stream: StreamId, effects: Vec<StreamEffect>) -> impl Iterator<Item = Effect<Deliveries>> {
    effects.into_iter().map(move |effect| Effect::Run(DeliveryEffect::Stream { stream, effect }))
}

impl StateOwner for Deliveries {
    const EPOCHED: bool = true;

    type State = DeliveriesState;
    type Input = DeliveryInput;
    type Effect = DeliveryEffect;

    fn step(state: &mut DeliveriesState, input: DeliveryInput) -> Effects<Self> {
        let mut out: Effects<Self> = Vec::new();
        match input {
            DeliveryInput::Open { stream, view, epoch, revision } => {
                if state.streams.contains_key(&stream) {
                    return out;
                }
                let mut fx = Vec::new();
                let machine = if epoch == state.epoch { Delivery::open(view, epoch, revision, &mut fx) } else { Delivery::stale(view, epoch, &mut fx) };
                state.streams.insert(stream, machine);
                out.extend(keyed(stream, fx));
            }
            DeliveryInput::ReadDone { stream, revision, rows } => on(state, stream, &mut out, |machine, fx| machine.read_done(revision, rows, fx)),
            DeliveryInput::ReadFailed { stream, revision, error } => on(state, stream, &mut out, |machine, fx| machine.read_failed(revision, error, fx)),
            DeliveryInput::WindowState { stream, state: window } => on(state, stream, &mut out, |machine, fx| machine.window_state(window, fx)),
            DeliveryInput::ModelEvent { stream, position, payload } => on(state, stream, &mut out, |machine, fx| machine.model_event(position, payload, fx)),
            DeliveryInput::ModelLost { stream, first, last } => on(state, stream, &mut out, |machine, fx| machine.model_lost(first, last, fx)),
            DeliveryInput::Cancel { stream } => on(state, stream, &mut out, |machine, fx| machine.cancel(fx)),
            DeliveryInput::Pull { stream } => match state.streams.get_mut(&stream) {
                Some(machine) => {
                    let mut fx = Vec::new();
                    machine.pull(&mut fx);
                    out.extend(keyed(stream, fx));
                    if state.streams.get(&stream).is_some_and(|machine| machine.ended_given && machine.journal.is_empty()) {
                        state.streams.remove(&stream);
                    }
                }
                None => out.extend(keyed(stream, vec![StreamEffect::Done])),
            },
            DeliveryInput::Committed(committed) => every(state, &mut out, |machine, fx| machine.committed(&committed, fx)),
            DeliveryInput::ModelsReset(reset) => every(state, &mut out, |machine, fx| machine.models_reset(&reset, fx)),
            DeliveryInput::EpochEnded(ended) => every(state, &mut out, |machine, fx| machine.epoch_ended(&ended, fx)),
            DeliveryInput::EpochStarted(started) => state.epoch = started.epoch,
            DeliveryInput::Closing => {
                every(state, &mut out, |machine, fx| machine.closing(fx));
                out.push(Effect::Run(DeliveryEffect::Drained(Drained { owner: "Deliveries" })));
            }
            DeliveryInput::Tick(_) => {}
        }
        out
    }

    fn tick(now: Time) -> DeliveryInput { DeliveryInput::Tick(now) }

    fn epoch_ended(epoch: Epoch, cause: Cause) -> DeliveryInput { DeliveryInput::EpochEnded(EpochEnded { epoch, cause }) }

    fn closing() -> DeliveryInput { DeliveryInput::Closing }
}

fn on(state: &mut DeliveriesState, stream: StreamId, out: &mut Effects<Deliveries>, apply: impl FnOnce(&mut Delivery, &mut Vec<StreamEffect>)) {
    let Some(machine) = state.streams.get_mut(&stream) else { return };
    let mut fx = Vec::new();
    apply(machine, &mut fx);
    out.extend(keyed(stream, fx));
}

fn every(state: &mut DeliveriesState, out: &mut Effects<Deliveries>, mut apply: impl FnMut(&mut Delivery, &mut Vec<StreamEffect>)) {
    for (stream, machine) in state.streams.iter_mut() {
        let mut fx = Vec::new();
        apply(machine, &mut fx);
        out.extend(keyed(*stream, fx));
    }
}
