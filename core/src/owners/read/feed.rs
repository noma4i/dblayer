//! Owner `Feed` (`specs/machines/feed.md`): the server subscription of the epoch, the model events and their positions.
//! Timers are `Effect::Schedule` of the owner (`after` = the debounce or the backoff). Inputs for other owners
//! (`Commit`, `Stale`, `Joined`, `ModelReady`, `ModelEvent`, `ModelLost`) are `Effect::Send` to the addresses of the state.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use crate::owners::contract::{BatchAuthor, BatchId, Committed, ConnectionId, Drained, EpochEnded, EpochStarted, Frame, ModelsReset, Online, RejectReason, Rejected, Stale, SubscriptionFrame};
use crate::owners::read::deliveries::{DeliveryInput, Deliveries, StreamId};
use crate::owners::store::{StoreBatch, StoreInput, StoreWriter};
use crate::owners::read::windows::{Windows, WindowsInput};
use crate::pacing::RetryPolicy;
use crate::runtime::{Address, Cause, Effect, Effects, Epoch, Outbound, ScheduleId, StateOwner, Time};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FeedEvent {
    pub model: String,
    pub payload: String,
    /// `model_generation` of the model when the frame arrived.
    pub basis: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    Off { gap: bool },
    Opening { gap: bool },
    Live,
    Ended,
}

/// The effects of the runner: IO of the subscription and the end of the owner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FeedEffect {
    Subscribe { connection: ConnectionId, models: Vec<String> },
    Unsubscribe,
    /// The old connection closes after the new one is ready.
    CloseConnection { connection: ConnectionId },
    Ended,
    Drained(Drained),
}

/// What the steps give out: an effect of the runner, a timer, or an input for another owner.
enum Fx {
    Run(FeedEffect),
    Debounce { after_ms: u64 },
    CancelDebounce,
    Resubscribe { connection: ConnectionId, attempt: u32 },
    CancelResubscribe { connection: ConnectionId },
    Commit { batch: BatchId, events: Vec<FeedEvent> },
    /// Windows of the models reread after a batch was delivered.
    RereadWindows(Vec<String>),
    StaleWindows(Vec<String>),
    /// Delivery answer to `Want+`: the position of the model at the join.
    Joined { stream: StreamId, position: u64 },
    /// `stream` = `None`: every stream that wants the model.
    ModelReady { model: String, stream: Option<StreamId> },
    ModelEvent { model: String, position: u64, payload: String },
    ModelLost { model: String, first: u64, last: u64 },
}

#[derive(Clone, Debug)]
pub enum FeedInput {
    /// `debounce_ms` is `updates.debounce` of the model, 0 when not declared. `delivery` = the stream `Events` that wants the model.
    WantPlus { model: String, debounce_ms: u64, delivery: Option<StreamId> },
    WantMinus { model: String, delivery: Option<StreamId> },
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

struct Want {
    count: u32,
    debounce_ms: u64,
    streams: BTreeSet<StreamId>,
}

/// The owners `Feed` sends inputs to.
#[derive(Clone, Copy, Debug)]
pub struct FeedPeers {
    pub store: Address<StoreWriter>,
    pub windows: Address<Windows>,
    pub deliveries: Address<Deliveries>,
}

pub struct FeedState {
    epoch: Epoch,
    mode: Mode,
    online: bool,
    want: BTreeMap<String, Want>,
    position: BTreeMap<String, u64>,
    generation: BTreeMap<String, u64>,
    base_generation: u64,
    pending: Vec<FeedEvent>,
    landing: Vec<(BatchId, Vec<FeedEvent>)>,
    connection: ConnectionId,
    switching: Option<ConnectionId>,
    next_connection: ConnectionId,
    next_batch: BatchId,
    attempts: u32,
    peers: FeedPeers,
}

impl FeedState {
    pub fn new(epoch: Epoch, online: bool, peers: FeedPeers) -> Self {
        Self {
            epoch,
            mode: Mode::Off { gap: false },
            online,
            want: BTreeMap::new(),
            position: BTreeMap::new(),
            generation: BTreeMap::new(),
            base_generation: 0,
            pending: Vec::new(),
            landing: Vec::new(),
            connection: 0,
            switching: None,
            next_connection: 1,
            next_batch: 1,
            attempts: 0,
            peers,
        }
    }

    pub fn mode(&self) -> Mode { self.mode }

    pub fn position(&self, model: &str) -> u64 { self.position.get(model).copied().unwrap_or(0) }

    pub fn pending(&self) -> usize { self.pending.len() }

    pub fn landing(&self) -> usize { self.landing.len() }

    pub fn wants(&self) -> Vec<String> { self.want.keys().cloned().collect() }

    /// The streams `Events` that want the model.
    pub fn streams(&self, model: &str) -> Vec<StreamId> { self.want.get(model).map(|want| want.streams.iter().copied().collect()).unwrap_or_default() }

    fn models(&self) -> Vec<String> { self.want.keys().cloned().collect() }

    fn subscribe(&mut self, fx: &mut Vec<Fx>) {
        let connection = self.next_connection;
        self.next_connection += 1;
        self.connection = connection;
        self.switching = None;
        self.attempts = 0;
        fx.push(Fx::Run(FeedEffect::Subscribe { connection, models: self.models() }));
    }

    fn lose(&mut self, model: &str, count: u64, fx: &mut Vec<Fx>) {
        let at = self.position(model);
        self.position.insert(model.to_string(), at + count);
        fx.push(Fx::ModelLost { model: model.to_string(), first: at + 1, last: at + count });
    }

    fn lose_events(&mut self, events: &[FeedEvent], only: Option<&[String]>, fx: &mut Vec<Fx>) -> Vec<String> {
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

    fn deliver(&mut self, events: Vec<FeedEvent>, fx: &mut Vec<Fx>) {
        let mut models: Vec<String> = Vec::new();
        for event in events {
            let position = self.position(&event.model) + 1;
            self.position.insert(event.model.clone(), position);
            if !models.contains(&event.model) {
                models.push(event.model.clone());
            }
            fx.push(Fx::ModelEvent { model: event.model, position, payload: event.payload });
        }
        if !models.is_empty() {
            fx.push(Fx::RereadWindows(models));
        }
    }

    fn commit(&mut self, events: Vec<FeedEvent>, fx: &mut Vec<Fx>) {
        let batch = self.next_batch;
        self.next_batch += 1;
        self.landing.push((batch, events.clone()));
        fx.push(Fx::Commit { batch, events });
    }

    fn flush_pending(&mut self, fx: &mut Vec<Fx>) {
        if !self.pending.is_empty() {
            let events = std::mem::take(&mut self.pending);
            fx.push(Fx::CancelDebounce);
            self.commit(events, fx);
        }
    }

    fn take_batch(&mut self, batch: BatchId) -> Option<Vec<FeedEvent>> {
        let at = self.landing.iter().position(|(id, _)| *id == batch)?;
        Some(self.landing.remove(at).1)
    }

    /// `Committed` and `Rejected` of a batch of `landing`: delivered on `Committed`, lost on `Rejected`. `NoSession` ends a `Live` feed.
    fn verdict(&mut self, batch: BatchId, rejected: Option<RejectReason>, fx: &mut Vec<Fx>) {
        let Some(events) = self.take_batch(batch) else { return };
        match rejected {
            None => self.deliver(events, fx),
            Some(RejectReason::StaleGeneration { models, .. }) => {
                let lost = self.lose_events(&events, Some(&models), fx);
                fx.push(Fx::StaleWindows(lost));
                let rest: Vec<FeedEvent> = events.into_iter().filter(|event| !models.contains(&event.model)).collect();
                if !rest.is_empty() {
                    self.commit(rest, fx);
                }
            }
            Some(RejectReason::Storage(_) | RejectReason::Closed) => {
                let lost = self.lose_events(&events, None, fx);
                fx.push(Fx::StaleWindows(lost));
            }
            Some(RejectReason::NoSession) => {
                if self.mode == Mode::Live {
                    self.mode = Mode::Ended;
                    self.connection = 0;
                    self.switching = None;
                    fx.push(Fx::Run(FeedEffect::Unsubscribe));
                }
            }
        }
    }

    fn ready_all(&self, fx: &mut Vec<Fx>) {
        for model in self.models() {
            fx.push(Fx::ModelReady { model, stream: None });
        }
    }

    fn frame(&mut self, frame: SubscriptionFrame, fx: &mut Vec<Fx>) {
        let current = frame.connection_id == self.connection;
        let known = current || self.switching == Some(frame.connection_id);
        if !known {
            return;
        }
        match (self.mode, frame.frame) {
            (Mode::Opening { gap }, Frame::Ready) if current => {
                self.mode = Mode::Live;
                if gap {
                    for model in self.models() {
                        self.lose(&model, 1, fx);
                    }
                    fx.push(Fx::StaleWindows(self.models()));
                }
                self.ready_all(fx);
            }
            (Mode::Live, Frame::Ready) if self.switching == Some(frame.connection_id) => {
                let old = self.connection;
                self.connection = frame.connection_id;
                self.switching = None;
                fx.push(Fx::Run(FeedEffect::CloseConnection { connection: old }));
                self.ready_all(fx);
            }
            (Mode::Opening { .. } | Mode::Live, Frame::Event { model, payload }) if current => self.event(model, payload, fx),
            (Mode::Opening { .. }, Frame::ChannelLost { dropped, models }) if current => {
                for model in &models {
                    self.lose(model, dropped, fx);
                }
                fx.push(Fx::StaleWindows(models));
            }
            (Mode::Live, Frame::ChannelLost { dropped, models }) if current => {
                self.flush_pending(fx);
                for model in &models {
                    self.lose(model, dropped, fx);
                }
                fx.push(Fx::StaleWindows(models));
            }
            (Mode::Opening { gap }, Frame::Dropped | Frame::Ended) if current => {
                if self.online {
                    self.connection = self.next_connection;
                    self.next_connection += 1;
                    self.attempts += 1;
                    fx.push(Fx::Resubscribe { connection: self.connection, attempt: self.attempts });
                } else {
                    self.mode = Mode::Off { gap };
                }
            }
            (Mode::Live, Frame::Dropped | Frame::Ended) if current => {
                self.flush_pending(fx);
                if self.online {
                    self.mode = Mode::Opening { gap: true };
                    self.subscribe(fx);
                } else {
                    self.mode = Mode::Off { gap: true };
                    self.connection = 0;
                    self.switching = None;
                }
            }
            _ => {}
        }
    }

    fn event(&mut self, model: String, payload: String, fx: &mut Vec<Fx>) {
        let Some(debounce_ms) = self.want.get(&model).map(|want| want.debounce_ms) else { return };
        let basis = self.generation.get(&model).copied().unwrap_or(self.base_generation);
        let event = FeedEvent { model, payload, basis };
        if debounce_ms == 0 {
            self.commit(vec![event], fx);
            return;
        }
        let first = self.pending.is_empty();
        self.pending.push(event);
        if first {
            fx.push(Fx::Debounce { after_ms: debounce_ms });
        }
    }

    fn want_plus(&mut self, model: String, debounce_ms: u64, delivery: Option<StreamId>, fx: &mut Vec<Fx>) {
        let new = !self.want.contains_key(&model);
        let position = self.position(&model);
        let want = self.want.entry(model.clone()).and_modify(|want| want.count += 1).or_insert(Want { count: 1, debounce_ms, streams: BTreeSet::new() });
        if let Some(stream) = delivery {
            want.streams.insert(stream);
            fx.push(Fx::Joined { stream, position });
        }
        match self.mode {
            Mode::Off { gap } if self.online => {
                self.mode = Mode::Opening { gap };
                self.subscribe(fx);
            }
            Mode::Opening { .. } if new => {
                fx.push(Fx::Run(FeedEffect::Unsubscribe));
                self.subscribe(fx);
            }
            Mode::Live if new => {
                let connection = self.next_connection;
                self.next_connection += 1;
                self.switching = Some(connection);
                fx.push(Fx::Run(FeedEffect::Subscribe { connection, models: self.models() }));
            }
            Mode::Live if delivery.is_some() => fx.push(Fx::ModelReady { model, stream: delivery }),
            _ => {}
        }
    }

    fn want_minus(&mut self, model: &str, delivery: Option<StreamId>, fx: &mut Vec<Fx>) {
        let Some(want) = self.want.get_mut(model) else { return };
        want.count -= 1;
        if let Some(stream) = delivery {
            want.streams.remove(&stream);
        }
        if want.count > 0 {
            return;
        }
        self.want.remove(model);
        if !self.want.is_empty() {
            return;
        }
        match self.mode {
            Mode::Opening { gap } => {
                self.mode = Mode::Off { gap };
                fx.push(Fx::CancelResubscribe { connection: self.connection });
                self.connection = 0;
                fx.push(Fx::Run(FeedEffect::Unsubscribe));
            }
            Mode::Live => {
                self.mode = Mode::Off { gap: false };
                self.flush_pending(fx);
                self.connection = 0;
                self.switching = None;
                fx.push(Fx::Run(FeedEffect::Unsubscribe));
            }
            _ => {}
        }
    }

    fn online_changed(&mut self, online: bool, fx: &mut Vec<Fx>) {
        self.online = online;
        match self.mode {
            Mode::Off { gap } if online && !self.want.is_empty() => {
                self.mode = Mode::Opening { gap };
                self.subscribe(fx);
            }
            Mode::Opening { gap } if !online => {
                self.mode = Mode::Off { gap };
                fx.push(Fx::CancelResubscribe { connection: self.connection });
                self.connection = 0;
                fx.push(Fx::Run(FeedEffect::Unsubscribe));
            }
            Mode::Live if !online => {
                self.mode = Mode::Off { gap: true };
                self.flush_pending(fx);
                self.connection = 0;
                self.switching = None;
                fx.push(Fx::Run(FeedEffect::Unsubscribe));
            }
            _ => {}
        }
    }

    fn resubscribe(&mut self, connection: ConnectionId, fx: &mut Vec<Fx>) {
        let Mode::Opening { gap } = self.mode else { return };
        if connection != self.connection {
            return;
        }
        if self.online {
            fx.push(Fx::Run(FeedEffect::Subscribe { connection, models: self.models() }));
        } else {
            self.mode = Mode::Off { gap };
            self.connection = 0;
        }
    }

    fn end(&mut self, drained: bool, fx: &mut Vec<Fx>) {
        self.mode = Mode::Ended;
        self.connection = 0;
        self.switching = None;
        if !drained {
            self.pending.clear();
        }
        fx.push(Fx::Run(FeedEffect::Unsubscribe));
        if drained {
            fx.push(Fx::Run(FeedEffect::Drained(Drained { owner: "Feed" })));
        }
        fx.push(Fx::Run(FeedEffect::Ended));
    }
}

pub struct Feed;

const DEBOUNCE: ScheduleId = ScheduleId(0);

impl Feed {
    fn to_streams(state: &FeedState, model: &str, out: &mut Effects<Self>, input: impl Fn(StreamId) -> DeliveryInput) {
        for stream in state.streams(model) {
            out.push(Effect::Send(Outbound::new(state.peers.deliveries, input(stream))));
        }
    }

    fn commit_batch(state: &FeedState, batch: BatchId, events: Vec<FeedEvent>) -> StoreBatch {
        let mut basis = BTreeMap::new();
        for event in &events {
            let entry = basis.entry(event.model.clone()).or_insert(event.basis);
            *entry = (*entry).min(event.basis);
        }
        let rows = events.into_iter().map(|event| (event.model, event.payload)).collect();
        StoreBatch { author: BatchAuthor::Feed, id: batch, epoch: state.epoch, basis, rows, intents: 0, credential: None, ops: Vec::new() }
    }

    fn lower(state: &FeedState, fx: Vec<Fx>) -> Effects<Self> {
        let peers = state.peers;
        let mut out: Effects<Self> = Vec::new();
        for item in fx {
            match item {
                Fx::Run(effect) => out.push(Effect::Run(effect)),
                Fx::Debounce { after_ms } => {
                    out.push(Effect::Schedule { id: DEBOUNCE, after: Duration::from_millis(after_ms), input: FeedInput::Tick(Time(0)) });
                }
                Fx::CancelDebounce => out.push(Effect::Cancel(DEBOUNCE)),
                Fx::Resubscribe { connection, attempt } => {
                    let after = RetryPolicy::Subscription.backoff().delay(attempt - 1);
                    out.push(Effect::Schedule { id: ScheduleId(connection), after, input: FeedInput::ResubscribeDue { connection } });
                }
                Fx::CancelResubscribe { connection } => out.push(Effect::Cancel(ScheduleId(connection))),
                Fx::Commit { batch, events } => out.push(Effect::Send(Outbound::new(peers.store, StoreInput::Commit(Self::commit_batch(state, batch, events))))),
                Fx::RereadWindows(models) => out.extend(models.into_iter().map(|model| Effect::Send(Outbound::new(peers.windows, WindowsInput::Reread { model })))),
                Fx::StaleWindows(models) => out.extend(models.into_iter().map(|model| Effect::Send(Outbound::new(peers.windows, WindowsInput::Stale(Stale { model }))))),
                Fx::Joined { stream, position } => out.push(Effect::Send(Outbound::new(peers.deliveries, DeliveryInput::Joined { stream, position }))),
                Fx::ModelReady { stream: Some(stream), .. } => out.push(Effect::Send(Outbound::new(peers.deliveries, DeliveryInput::ModelReady { stream }))),
                Fx::ModelReady { model, stream: None } => Self::to_streams(state, &model, &mut out, |stream| DeliveryInput::ModelReady { stream }),
                Fx::ModelEvent { model, position, payload } => Self::to_streams(state, &model, &mut out, |stream| DeliveryInput::ModelEvent { stream, position, payload: payload.clone() }),
                Fx::ModelLost { model, first, last } => Self::to_streams(state, &model, &mut out, |stream| DeliveryInput::ModelLost { stream, first, last }),
            }
        }
        out
    }
}

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
                state.epoch = started.epoch;
                state.base_generation = started.generation;
            }
            FeedInput::Closing if ended => fx.push(Fx::Run(FeedEffect::Drained(Drained { owner: "Feed" }))),
            _ if ended => {}
            FeedInput::EpochEnded(ended) if ended.epoch == state.epoch => state.end(false, &mut fx),
            FeedInput::EpochEnded(_) => {}
            FeedInput::Closing => state.end(true, &mut fx),
            FeedInput::WantPlus { model, debounce_ms, delivery } => state.want_plus(model, debounce_ms, delivery, &mut fx),
            FeedInput::WantMinus { model, delivery } => state.want_minus(&model, delivery, &mut fx),
            FeedInput::Frame(frame) => state.frame(frame, &mut fx),
            FeedInput::Committed(committed) => state.verdict(committed.batch, None, &mut fx),
            FeedInput::Rejected(rejected) => state.verdict(rejected.batch, Some(rejected.reason), &mut fx),
            FeedInput::ResubscribeDue { connection } => state.resubscribe(connection, &mut fx),
            FeedInput::Tick(_) => {
                if state.mode == Mode::Live {
                    state.flush_pending(&mut fx);
                }
            }
            FeedInput::Online(Online(online)) => state.online_changed(online, &mut fx),
        }
        Self::lower(state, fx)
    }

    fn tick(now: Time) -> FeedInput { FeedInput::Tick(now) }

    fn epoch_ended(epoch: Epoch, cause: Cause) -> FeedInput { FeedInput::EpochEnded(EpochEnded { epoch, cause }) }

    fn closing() -> FeedInput { FeedInput::Closing }
}
