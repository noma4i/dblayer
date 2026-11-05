//! Owner `Deliveries`: a dictionary of `Delivery` machines by stream id (`specs/machines/delivery.md`, one machine per stream).
//! Only this owner hands values to the platform. A table row applies to the machine of the stream the input names;
//! `Committed`, `EpochStarted`, `EpochEnded`, `ModelsReset`, `Closing` apply to every machine.

use std::collections::{BTreeMap, VecDeque};

use crate::owners::contract::{Committed, Drained, EpochEnded, EpochStarted, ModelsReset, WindowPhase, WindowState};
use crate::runtime::{Cause, Effect, Effects, Epoch, StateOwner, Time};

pub const JOURNAL_LIMIT: usize = 256;
/// A read that moves more rows than this arrives as a `Snapshot`, not a `Delta`.
pub const DELTA_LIMIT: usize = 256;

pub type StreamId = u64;

/// How an `Entity` stream asks the window of its node (`delivery.md`, kinds of streams).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fetch {
    Never,
    Stale,
    Force,
    IfMissing,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    List,
    Entity { fetch: Fetch },
    Count,
    Events,
}

impl Kind {
    /// `Some(force)` when `open` sends `Interest+` to the window.
    fn interest_at_open(self) -> Option<bool> {
        match self {
            Kind::List | Kind::Count | Kind::Entity { fetch: Fetch::Stale } => Some(false),
            Kind::Entity { fetch: Fetch::Force } => Some(true),
            Kind::Entity { .. } | Kind::Events => None,
        }
    }
}

/// What a read gives back: the rows of a `List` or `Entity`, the number of a `Count`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReadData {
    Rows(Vec<StreamRow>),
    Count(u64),
}

/// What a stream shows: `tables` are the tables of its rows, `model` is the model of its rows or events.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct View {
    pub kind: Kind,
    pub model: String,
    pub tables: Vec<String>,
    /// The model is `storage: local`: the stream needs no session.
    pub local: bool,
}

/// A row of a snapshot: `stamp` changes when the content of the row changes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamRow {
    pub id: String,
    pub stamp: i64,
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
    Snapshot { revision: i64, rows: Vec<StreamRow>, state: WindowState },
    Delta { base: i64, revision: i64, upserted: Vec<StreamRow>, removed: Vec<String> },
    Count { revision: i64, count: u64, state: WindowState },
    /// The window changed, the rows of `revision` are the same.
    State { revision: i64, state: WindowState },
    /// The server subscription is alive: once per `Events` stream.
    Ready,
    Event { position: u64, payload: String },
    Lost { first: u64, last: u64 },
    Ended(EndCause),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StreamEffect {
    Read { read_id: u64, revision: i64 },
    InterestPlus { force: bool },
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
    /// `identity` = the published value has an identity, `valid` = the model, relation and params of the request are valid.
    Open { stream: StreamId, view: View, epoch: Epoch, revision: i64, identity: bool, valid: bool },
    Committed(Committed),
    ReadDone { stream: StreamId, read_id: u64, revision: i64, data: ReadData },
    ReadFailed { stream: StreamId, read_id: u64, revision: i64, error: String },
    WindowState { stream: StreamId, state: WindowState },
    Joined { stream: StreamId, position: u64 },
    ModelReady { stream: StreamId },
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
    /// The revision the current read asks for.
    read: i64,
    read_id: u64,
    revision: i64,
    rows: Vec<StreamRow>,
    count: Option<u64>,
    window: WindowState,
    /// The next expected position of the Feed (`Joined.position + 1`), `None` before `Joined`.
    next_feed: Option<u64>,
    /// The last position given to the stream.
    position: u64,
    ready: bool,
    journal: VecDeque<Value>,
    ended_given: bool,
}

impl Delivery {
    fn open(view: View, epoch: Epoch, revision: i64, fx: &mut Vec<StreamEffect>) -> Self {
        let mut machine = Self { epoch, view, stage: DeliveryStage::Opening, read: revision, read_id: 0, revision: 0, rows: Vec::new(), count: None, window: WindowState::idle(), next_feed: None, position: 0, ready: false, journal: VecDeque::new(), ended_given: false };
        if machine.view.kind == Kind::Events {
            machine.stage = DeliveryStage::Live;
            fx.push(StreamEffect::WantPlus);
            return machine;
        }
        fx.push(machine.new_read(revision));
        if let Some(force) = machine.view.kind.interest_at_open() {
            fx.push(StreamEffect::InterestPlus { force });
        }
        machine
    }

    fn new_read(&mut self, revision: i64) -> StreamEffect {
        self.read_id += 1;
        StreamEffect::Read { read_id: self.read_id, revision }
    }

    fn stale(view: View, epoch: Epoch, cause: EndCause, fx: &mut Vec<StreamEffect>) -> Self {
        let mut machine = Self { epoch, view, stage: DeliveryStage::Ended, read: 0, read_id: 0, revision: 0, rows: Vec::new(), count: None, window: WindowState::idle(), next_feed: None, position: 0, ready: false, journal: VecDeque::new(), ended_given: false };
        machine.push(Value::Ended(cause), fx);
        machine
    }

    fn push(&mut self, value: Value, fx: &mut Vec<StreamEffect>) {
        if self.journal.is_empty() {
            fx.push(StreamEffect::Wake);
        }
        match value {
            Value::Snapshot { .. } | Value::Count { .. } => self.journal.retain(|kept| !matches!(kept, Value::Delta { .. } | Value::Snapshot { .. } | Value::Count { .. } | Value::State { .. })),
            Value::State { .. } => self.journal.retain(|kept| !matches!(kept, Value::State { .. })),
            _ => {}
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
            Kind::Events => StreamEffect::WantMinus,
            _ => StreamEffect::InterestMinus,
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
                fx.push(self.new_read(committed.revision));
            }
            DeliveryStage::Live => fx.push(self.new_read(committed.revision)),
            DeliveryStage::Ended => {}
        }
    }

    fn read_done(&mut self, read_id: u64, revision: i64, data: ReadData, fx: &mut Vec<StreamEffect>) {
        if read_id != self.read_id || self.view.kind == Kind::Events {
            return;
        }
        let count = self.view.kind == Kind::Count;
        match (self.stage, data) {
            (DeliveryStage::Opening, ReadData::Rows(rows)) if !count => {
                let missing = rows.is_empty() && self.view.kind == (Kind::Entity { fetch: Fetch::IfMissing });
                self.push(Value::Snapshot { revision, rows: rows.clone(), state: self.window.clone() }, fx);
                self.revision = revision;
                self.rows = rows;
                self.stage = DeliveryStage::Live;
                if missing {
                    fx.push(StreamEffect::InterestPlus { force: false });
                }
            }
            (DeliveryStage::Opening, ReadData::Count(now)) if count => {
                self.push(Value::Count { revision, count: now, state: self.window.clone() }, fx);
                self.revision = revision;
                self.count = Some(now);
                self.stage = DeliveryStage::Live;
            }
            (DeliveryStage::Live, ReadData::Rows(rows)) if !count && revision > self.revision && rows != self.rows => {
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
            (DeliveryStage::Live, ReadData::Count(now)) if count && revision > self.revision && self.count != Some(now) => {
                self.push(Value::Count { revision, count: now, state: self.window.clone() }, fx);
                self.revision = revision;
                self.count = Some(now);
            }
            _ => {}
        }
    }

    fn read_failed(&mut self, read_id: u64, revision: i64, error: String, fx: &mut Vec<StreamEffect>) {
        if read_id != self.read_id {
            return;
        }
        match self.stage {
            DeliveryStage::Opening => {
                let state = WindowState { phase: WindowPhase::Failed, error: Some(error), loaded: None };
                self.push(Value::Snapshot { revision, rows: Vec::new(), state: state.clone() }, fx);
                self.revision = revision;
                self.rows = Vec::new();
                self.window = state;
                self.stage = DeliveryStage::Live;
            }
            DeliveryStage::Live if revision > self.revision => {
                let phase = if self.rows.is_empty() { WindowPhase::Failed } else { WindowPhase::Loaded };
                let state = WindowState { phase, error: Some(error), loaded: self.window.loaded };
                self.push(Value::State { revision: self.revision, state: state.clone() }, fx);
                self.window = state;
            }
            DeliveryStage::Live | DeliveryStage::Ended => {}
        }
    }

    fn window_state(&mut self, state: WindowState, fx: &mut Vec<StreamEffect>) {
        match self.stage {
            DeliveryStage::Opening => self.window = state,
            DeliveryStage::Live => {
                if state != self.window {
                    self.push(Value::State { revision: self.revision, state: state.clone() }, fx);
                }
                self.window = state;
            }
            DeliveryStage::Ended => {}
        }
    }

    fn is_live_events(&self) -> bool { self.stage == DeliveryStage::Live && self.view.kind == Kind::Events }

    fn joined(&mut self, position: u64) {
        if self.is_live_events() && self.next_feed.is_none() {
            self.next_feed = Some(position + 1);
        }
    }

    fn model_ready(&mut self, fx: &mut Vec<StreamEffect>) {
        if self.is_live_events() && !self.ready {
            self.ready = true;
            self.push(Value::Ready, fx);
        }
    }

    fn model_event(&mut self, position: u64, payload: String, fx: &mut Vec<StreamEffect>) {
        let Some(next) = self.next_feed.filter(|_| self.is_live_events()) else { return };
        if position < next {
            return;
        }
        if position > next {
            let gap = position - next;
            self.push(Value::Lost { first: self.position + 1, last: self.position + gap }, fx);
            self.position += gap;
        }
        self.position += 1;
        self.push(Value::Event { position: self.position, payload }, fx);
        self.next_feed = Some(position + 1);
    }

    fn model_lost(&mut self, first: u64, last: u64, fx: &mut Vec<StreamEffect>) {
        let Some(next) = self.next_feed.filter(|_| self.is_live_events()) else { return };
        if last < next {
            return;
        }
        let count = last - first.max(next) + 1;
        self.push(Value::Lost { first: self.position + 1, last: self.position + count }, fx);
        self.position += count;
        self.next_feed = Some(last + 1);
    }

    fn models_reset(&mut self, reset: &ModelsReset, fx: &mut Vec<StreamEffect>) {
        if self.view.kind == Kind::Events || !reset.models.contains(&self.view.model) {
            return;
        }
        match self.stage {
            DeliveryStage::Opening => fx.push(self.new_read(self.read)),
            DeliveryStage::Live => fx.push(self.new_read(self.revision)),
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

    pub fn read_id(&self, stream: StreamId) -> u64 { self.streams.get(&stream).map_or(0, |machine| machine.read_id) }

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
            DeliveryInput::Open { stream, view, epoch, revision, identity, valid } => {
                if state.streams.contains_key(&stream) {
                    return out;
                }
                let mut fx = Vec::new();
                let machine = if !valid {
                    Delivery::stale(view, epoch, EndCause::Invalid, &mut fx)
                } else if epoch != state.epoch || (!identity && !view.local) {
                    Delivery::stale(view, epoch, EndCause::SessionEnded, &mut fx)
                } else {
                    Delivery::open(view, epoch, revision, &mut fx)
                };
                state.streams.insert(stream, machine);
                out.extend(keyed(stream, fx));
            }
            DeliveryInput::ReadDone { stream, read_id, revision, data } => on(state, stream, &mut out, |machine, fx| machine.read_done(read_id, revision, data, fx)),
            DeliveryInput::ReadFailed { stream, read_id, revision, error } => on(state, stream, &mut out, |machine, fx| machine.read_failed(read_id, revision, error, fx)),
            DeliveryInput::WindowState { stream, state: window } => on(state, stream, &mut out, |machine, fx| machine.window_state(window, fx)),
            DeliveryInput::Joined { stream, position } => on(state, stream, &mut out, |machine, _| machine.joined(position)),
            DeliveryInput::ModelReady { stream } => on(state, stream, &mut out, |machine, fx| machine.model_ready(fx)),
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
