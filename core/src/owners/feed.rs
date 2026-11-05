//! Owner `Feed` (`specs/machines/feed.md`): the server subscription of the epoch, the model events and their positions.
//! Timers are effects for the executor (`ScheduleTick`, `ScheduleResubscribe`): the executor adds the clock and the backoff,
//! then posts `Tick` or `ResubscribeDue` back.

use std::collections::BTreeMap;

use super::contract::{BatchId, Committed, ConnectionId, Drained, EpochEnded, EpochStarted, Frame, ModelsReset, Online, RejectReason, Rejected, SubscriptionFrame};
use crate::runtime::{Cause, Effect, Effects, Epoch, StateOwner, Time};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FeedEvent {
    pub model: String,
    pub payload: String,
    /// `model_generation` of the model when the frame arrived.
    pub basis: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    Off,
    Opening { gap: bool },
    Live,
    Ended,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FeedEffect {
    Subscribe { connection: ConnectionId, models: Vec<String> },
    Unsubscribe,
    /// The old connection closes after the new one is ready.
    CloseConnection { connection: ConnectionId },
    ScheduleTick { after_ms: u64 },
    ScheduleResubscribe { connection: ConnectionId },
    Commit { batch: BatchId, events: Vec<FeedEvent> },
    /// Windows of the models reread (`reread` windows only decide in the executor) after a batch was delivered.
    RereadWindows(Vec<String>),
    StaleWindows(Vec<String>),
    /// Observable for `Events` streams.
    ModelEvent { model: String, position: u64, payload: String },
    ModelLost { model: String, first: u64, last: u64 },
    Ended,
    Drained(Drained),
}

#[derive(Clone, Debug)]
pub enum FeedInput {
    /// `debounce_ms` is `updates.debounce` of the model, 0 when not declared.
    WantPlus { model: String, debounce_ms: u64 },
    WantMinus { model: String },
    Frame(SubscriptionFrame),
    Committed(Committed),
    Rejected(Rejected),
    ResubscribeDue { connection: ConnectionId },
    Tick(Time),
    Online(Online),
    ModelsReset(ModelsReset),
    EpochStarted(EpochStarted),
    EpochEnded(EpochEnded),
    Closing,
}

pub struct FeedState {
    epoch: Epoch,
    mode: Mode,
    online: bool,
    session: bool,
    want: BTreeMap<String, u64>,
    position: BTreeMap<String, u64>,
    generation: BTreeMap<String, u64>,
    pending: Vec<FeedEvent>,
    landing: Vec<(BatchId, Vec<FeedEvent>)>,
    connection: ConnectionId,
    switching: Option<ConnectionId>,
    next_connection: ConnectionId,
    next_batch: BatchId,
}

impl FeedState {
    pub fn new(epoch: Epoch, online: bool, session: bool) -> Self {
        Self {
            epoch,
            mode: Mode::Off,
            online,
            session,
            want: BTreeMap::new(),
            position: BTreeMap::new(),
            generation: BTreeMap::new(),
            pending: Vec::new(),
            landing: Vec::new(),
            connection: 0,
            switching: None,
            next_connection: 1,
            next_batch: 1,
        }
    }

    pub fn mode(&self) -> Mode { self.mode }

    pub fn position(&self, model: &str) -> u64 { self.position.get(model).copied().unwrap_or(0) }

    pub fn pending(&self) -> usize { self.pending.len() }

    pub fn landing(&self) -> usize { self.landing.len() }

    pub fn wants(&self) -> Vec<String> { self.want.keys().cloned().collect() }

    fn models(&self) -> Vec<String> { self.want.keys().cloned().collect() }

    fn subscribe(&mut self, fx: &mut Vec<FeedEffect>) {
        let connection = self.next_connection;
        self.next_connection += 1;
        self.connection = connection;
        self.switching = None;
        fx.push(FeedEffect::Subscribe { connection, models: self.models() });
    }

    fn lose(&mut self, model: &str, count: u64, fx: &mut Vec<FeedEffect>) {
        let at = self.position(model);
        self.position.insert(model.to_string(), at + count);
        fx.push(FeedEffect::ModelLost { model: model.to_string(), first: at + 1, last: at + count });
    }

    fn lose_events(&mut self, events: &[FeedEvent], only: Option<&[String]>, fx: &mut Vec<FeedEffect>) -> Vec<String> {
        let mut models: Vec<String> = Vec::new();
        for event in events {
            if only.is_none_or(|only| only.contains(&event.model)) && !models.contains(&event.model) {
                models.push(event.model.clone());
            }
        }
        for model in &models {
            let count = events.iter().filter(|event| &event.model == model).count() as u64;
            self.lose(model, count, fx);
        }
        models
    }

    fn deliver(&mut self, events: Vec<FeedEvent>, fx: &mut Vec<FeedEffect>) {
        let mut models: Vec<String> = Vec::new();
        for event in events {
            let position = self.position(&event.model) + 1;
            self.position.insert(event.model.clone(), position);
            if !models.contains(&event.model) {
                models.push(event.model.clone());
            }
            fx.push(FeedEffect::ModelEvent { model: event.model, position, payload: event.payload });
        }
        if !models.is_empty() {
            fx.push(FeedEffect::RereadWindows(models));
        }
    }

    fn commit(&mut self, events: Vec<FeedEvent>, fx: &mut Vec<FeedEffect>) {
        let batch = self.next_batch;
        self.next_batch += 1;
        self.landing.push((batch, events.clone()));
        fx.push(FeedEffect::Commit { batch, events });
    }

    fn flush_pending(&mut self, fx: &mut Vec<FeedEffect>) {
        if !self.pending.is_empty() {
            let events = std::mem::take(&mut self.pending);
            self.commit(events, fx);
        }
    }

    fn take_batch(&mut self, batch: BatchId) -> Option<Vec<FeedEvent>> {
        let at = self.landing.iter().position(|(id, _)| *id == batch)?;
        Some(self.landing.remove(at).1)
    }

    /// `Committed` and `Rejected` of a batch of `landing`: delivered on `Committed`, lost on `Rejected`. In `Live`, `NoSession` ends the feed.
    fn verdict(&mut self, batch: BatchId, rejected: Option<RejectReason>, fx: &mut Vec<FeedEffect>) {
        let Some(events) = self.take_batch(batch) else { return };
        match rejected {
            None => self.deliver(events, fx),
            Some(RejectReason::StaleGeneration { models, .. }) => {
                let lost = self.lose_events(&events, Some(&models), fx);
                fx.push(FeedEffect::StaleWindows(lost));
                let rest: Vec<FeedEvent> = events.into_iter().filter(|event| !models.contains(&event.model)).collect();
                if !rest.is_empty() {
                    self.commit(rest, fx);
                }
            }
            Some(RejectReason::Storage(_) | RejectReason::Closed) => {
                let lost = self.lose_events(&events, None, fx);
                fx.push(FeedEffect::StaleWindows(lost));
            }
            Some(RejectReason::NoSession) => {
                if self.mode == Mode::Live {
                    self.mode = Mode::Ended;
                }
            }
        }
    }

    fn wake(&mut self, fx: &mut Vec<FeedEffect>) {
        if self.session && self.online && !self.want.is_empty() {
            self.mode = Mode::Opening { gap: false };
            self.subscribe(fx);
        }
    }

    fn frame(&mut self, frame: SubscriptionFrame, fx: &mut Vec<FeedEffect>) {
        let known = frame.connection == self.connection || self.switching == Some(frame.connection);
        if !known || frame.epoch != self.epoch {
            return;
        }
        match (self.mode, frame.frame) {
            (Mode::Opening { gap }, Frame::Ready) if frame.connection == self.connection => {
                self.mode = Mode::Live;
                if gap {
                    for model in self.models() {
                        self.lose(&model, 1, fx);
                    }
                    fx.push(FeedEffect::StaleWindows(self.models()));
                }
            }
            (Mode::Live, Frame::Ready) if self.switching == Some(frame.connection) => {
                let old = self.connection;
                self.connection = frame.connection;
                self.switching = None;
                fx.push(FeedEffect::CloseConnection { connection: old });
            }
            (Mode::Opening { gap }, Frame::Dropped) if frame.connection == self.connection => {
                self.mode = Mode::Opening { gap };
                self.connection = self.next_connection;
                self.next_connection += 1;
                if self.online {
                    fx.push(FeedEffect::ScheduleResubscribe { connection: self.connection });
                }
            }
            (Mode::Live, Frame::Event { model, payload }) if frame.connection == self.connection => self.event(model, payload, fx),
            (Mode::Live, Frame::Lost { dropped, models }) if frame.connection == self.connection => {
                self.flush_pending(fx);
                for model in &models {
                    self.lose(model, dropped, fx);
                }
                fx.push(FeedEffect::StaleWindows(models));
            }
            (Mode::Live, Frame::Dropped) if frame.connection == self.connection => {
                self.mode = Mode::Opening { gap: true };
                self.flush_pending(fx);
                self.subscribe(fx);
            }
            _ => {}
        }
    }

    fn event(&mut self, model: String, payload: String, fx: &mut Vec<FeedEffect>) {
        let Some(&debounce_ms) = self.want.get(&model) else { return };
        let basis = self.generation.get(&model).copied().unwrap_or(0);
        let event = FeedEvent { model, payload, basis };
        if debounce_ms == 0 {
            self.commit(vec![event], fx);
            return;
        }
        let first = self.pending.is_empty();
        self.pending.push(event);
        if first {
            fx.push(FeedEffect::ScheduleTick { after_ms: debounce_ms });
        }
    }

    fn want_plus(&mut self, model: String, debounce_ms: u64, fx: &mut Vec<FeedEffect>) {
        let changed = self.want.insert(model, debounce_ms).is_none();
        match self.mode {
            Mode::Off => self.wake(fx),
            Mode::Opening { .. } | Mode::Live if changed => {
                if self.mode == Mode::Live {
                    let connection = self.next_connection;
                    self.next_connection += 1;
                    self.switching = Some(connection);
                    fx.push(FeedEffect::Subscribe { connection, models: self.models() });
                } else {
                    self.subscribe(fx);
                }
            }
            _ => {}
        }
    }

    fn want_minus(&mut self, model: &str, fx: &mut Vec<FeedEffect>) {
        let changed = self.want.remove(model).is_some();
        match self.mode {
            Mode::Opening { .. } if changed => self.subscribe(fx),
            Mode::Live if self.want.is_empty() => {
                self.mode = Mode::Off;
                self.flush_pending(fx);
                self.connection = 0;
                self.switching = None;
                fx.push(FeedEffect::Unsubscribe);
            }
            _ => {}
        }
    }

    fn online_changed(&mut self, online: bool, fx: &mut Vec<FeedEffect>) {
        self.online = online;
        match self.mode {
            Mode::Off if online => self.wake(fx),
            Mode::Live if !online => {
                self.mode = Mode::Opening { gap: true };
                self.flush_pending(fx);
                self.connection = 0;
                self.switching = None;
                fx.push(FeedEffect::Unsubscribe);
            }
            _ => {}
        }
    }

    fn end(&mut self, drained: bool, fx: &mut Vec<FeedEffect>) {
        self.mode = Mode::Ended;
        self.connection = 0;
        self.switching = None;
        if !drained {
            self.pending.clear();
        }
        fx.push(FeedEffect::Unsubscribe);
        if drained {
            fx.push(FeedEffect::Drained(Drained { owner: "Feed" }));
        }
        fx.push(FeedEffect::Ended);
    }
}

pub struct Feed;

impl StateOwner for Feed {
    const EPOCHED: bool = true;

    type State = FeedState;
    type Input = FeedInput;
    type Effect = FeedEffect;

    fn step(state: &mut FeedState, input: FeedInput) -> Effects<Self> {
        let mut fx = Vec::new();
        let ended = state.mode == Mode::Ended;
        match input {
            FeedInput::ModelsReset(reset) => {
                for model in reset.models {
                    state.generation.insert(model, reset.generation);
                }
            }
            FeedInput::EpochStarted(started) => {
                state.session = true;
                state.epoch = started.epoch;
                if state.mode == Mode::Off {
                    state.wake(&mut fx);
                }
            }
            FeedInput::Closing if ended => fx.push(FeedEffect::Drained(Drained { owner: "Feed" })),
            _ if ended => {}
            FeedInput::EpochEnded(ended) if ended.epoch == state.epoch => state.end(false, &mut fx),
            FeedInput::EpochEnded(_) => {}
            FeedInput::Closing => state.end(true, &mut fx),
            FeedInput::WantPlus { model, debounce_ms } => state.want_plus(model, debounce_ms, &mut fx),
            FeedInput::WantMinus { model } => state.want_minus(&model, &mut fx),
            FeedInput::Frame(frame) => state.frame(frame, &mut fx),
            FeedInput::Committed(committed) => state.verdict(committed.batch, None, &mut fx),
            FeedInput::Rejected(rejected) => state.verdict(rejected.batch, Some(rejected.reason), &mut fx),
            FeedInput::ResubscribeDue { connection } => {
                if matches!(state.mode, Mode::Opening { .. }) && state.online && connection == state.connection {
                    let models = state.models();
                    fx.push(FeedEffect::Subscribe { connection, models });
                }
            }
            FeedInput::Tick(_) => {
                if state.mode == Mode::Live {
                    state.flush_pending(&mut fx);
                }
            }
            FeedInput::Online(Online(online)) => state.online_changed(online, &mut fx),
        }
        fx.into_iter().map(Effect::Run).collect()
    }

    fn tick(now: Time) -> FeedInput { FeedInput::Tick(now) }

    fn epoch_ended(epoch: Epoch, cause: Cause) -> FeedInput { FeedInput::EpochEnded(EpochEnded { epoch, cause }) }

    fn closing() -> FeedInput { FeedInput::Closing }
}
