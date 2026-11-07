//! The `feed` machine as code: one match arm per table row. No library code.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Reject {
    StaleGeneration(Vec<String>),
    Storage,
    Invalid,
    NoSession,
    Closed,
}

/// The connection an input names: the active one, the opening one, or one that is neither.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum To {
    Active,
    Opening,
    /// The connection that `Closing` left to drain.
    Draining,
    Stray,
}

/// The counter of an opened frame against the `recv_counter` of the connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Counter {
    Expected,
    Replayed,
}

/// A frame of the server that carries no model event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Kind {
    Ping,
    /// `complete` of the subscription of the connection.
    Complete,
    /// `complete` and `next` of another subscription id.
    OtherId,
    /// `ready`, `subscribed`, `pong`.
    Quiet,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Input {
    WantPlus { model: String, id: u64, stream: bool },
    WantMinus { model: String, id: u64 },
    HandshakeDone,
    FrameOpened { to: To, counter: Counter, events: Vec<(String, String)> },
    Frame { to: To, counter: Counter, kind: Kind },
    ProtocolError { to: To },
    Event { to: To, model: String, payload: String },
    ChannelLost { to: To, dropped: Option<Vec<(String, u64)>> },
    Dropped { to: To },
    /// `SubscriptionEnded`: the cells of `Dropped`.
    Ended { to: To },
    Resubscribe { to: To },
    /// The deadline of a `Connect`: a connection in `Handshake` fails like a protocol error.
    HandshakeDeadline { to: To },
    Committed,
    /// `Committed` of a batch whose children left parents without a row: the windows of these models turn `Stale`.
    CommittedStale(Vec<String>),
    Rejected(Reject),
    /// `Committed` or `Rejected` of a batch that is not in `landing`.
    StrayBatch,
    Tick,
    Online(bool),
    ModelsReset { models: Vec<String>, generation: u64 },
    EpochEnded,
    Closing,
    /// `Tick(drain{connection_id})`: `own` names the draining connection, otherwise a stray one.
    Drain { own: bool },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Effect {
    Connect,
    SealSubscribe,
    SealUnsubscribe,
    SealPong,
    Close,
    Cancel,
    ScheduleDrain,
    CancelDrain,
    ScheduleTick,
    ScheduleResubscribe,
    Commit,
    Stale { model: String, all: bool },
    Joined(u64),
    ModelReady(u64),
    Drained,
}

/// What the stream `stream` of Deliveries sees.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Obs {
    Event { stream: u64, position: u64, payload: String },
    Lost { stream: u64, first: u64, last: u64 },
    Gap { stream: u64 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Mode {
    Off { gap: bool },
    Opening { gap: bool },
    Live,
    Draining,
    Ended,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Proto {
    Handshake,
    Established,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Slot {
    Pending,
    Landed(String),
    Lost,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Ev {
    model: String,
    payload: String,
    position: u64,
    basis: u64,
}

pub(super) struct Feed {
    pub(super) mode: Mode,
    pub(super) online: Option<bool>,
    pub(super) want: BTreeMap<String, BTreeSet<u64>>,
    streams: BTreeSet<u64>,
    pub(super) position: BTreeMap<String, u64>,
    ledger: BTreeMap<String, Vec<Slot>>,
    shown: BTreeMap<String, usize>,
    pub(super) held: Vec<Ev>,
    pub(super) pending: Vec<Ev>,
    pub(super) landing: VecDeque<Vec<Ev>>,
    /// The protocol of the connections: `active` and `opening`.
    active: Option<Proto>,
    opening: Option<Proto>,
    /// `updates.debounce` is declared for every model: an event waits for a `Tick`.
    pub(super) debounce: bool,
    generation: BTreeMap<String, u64>,
    /// Every event declares `reread` keys: a landed batch turns the keys of its models stale.
    pub(super) reread: bool,
    pub(super) obs: Vec<Obs>,
    drained: bool,
}

impl Feed {
    pub(super) fn new() -> Self {
        Self {
            mode: Mode::Off { gap: false },
            online: None,
            want: BTreeMap::new(),
            streams: BTreeSet::new(),
            position: BTreeMap::new(),
            ledger: BTreeMap::new(),
            shown: BTreeMap::new(),
            held: Vec::new(),
            pending: Vec::new(),
            landing: VecDeque::new(),
            active: None,
            opening: None,
            debounce: true,
            generation: BTreeMap::new(),
            reread: false,
            obs: Vec::new(),
            drained: false,
        }
    }

    pub(super) fn take_obs(&mut self) -> Vec<Obs> { std::mem::take(&mut self.obs) }

    fn at(&self, model: &str) -> u64 { self.position.get(model).copied().unwrap_or(0) }

    fn models(&self) -> Vec<String> { self.want.keys().cloned().collect() }

    fn online(&self) -> bool { self.online == Some(true) }

    fn streams_of(&self, model: &str) -> Vec<u64> { self.want.get(model).map(|ids| ids.iter().copied().filter(|id| self.streams.contains(id)).collect()).unwrap_or_default() }

    fn basis(&self, model: &str) -> u64 { self.generation.get(model).copied().unwrap_or(0) }

    /// Does the connection named by `to` exist, and in which protocol state?
    fn named(&self, to: To) -> Option<Proto> {
        match to {
            To::Active => self.active,
            To::Opening => self.opening,
            To::Draining | To::Stray => None,
        }
    }

    fn accept(&mut self, model: &str, payload: &str) -> Ev {
        let position = self.at(model) + 1;
        self.position.insert(model.to_string(), position);
        self.ledger.entry(model.to_string()).or_default().push(Slot::Pending);
        Ev { model: model.to_string(), payload: payload.to_string(), position, basis: self.basis(model) }
    }

    fn settle(&mut self, model: &str) {
        let streams = self.streams_of(model);
        let shown = self.shown.entry(model.to_string()).or_default();
        let slots = self.ledger.entry(model.to_string()).or_default();
        while *shown < slots.len() {
            match slots[*shown].clone() {
                Slot::Pending => break,
                Slot::Landed(payload) => {
                    let position = *shown as u64 + 1;
                    self.obs.extend(streams.iter().map(|stream| Obs::Event { stream: *stream, position, payload: payload.clone() }));
                    *shown += 1;
                }
                Slot::Lost => {
                    let first = *shown as u64 + 1;
                    while *shown < slots.len() && slots[*shown] == Slot::Lost {
                        *shown += 1;
                    }
                    let last = *shown as u64;
                    self.obs.extend(streams.iter().map(|stream| Obs::Lost { stream: *stream, first, last }));
                }
            }
        }
    }

    fn lose(&mut self, model: &str, count: u64) {
        self.position.insert(model.to_string(), self.at(model) + count);
        let slots = self.ledger.entry(model.to_string()).or_default();
        slots.extend((0..count).map(|_| Slot::Lost));
        self.settle(model);
    }

    fn mark(&mut self, events: &[Ev], slot: impl Fn(&Ev) -> Slot) -> Vec<String> {
        let mut models: Vec<String> = Vec::new();
        for event in events {
            self.ledger.entry(event.model.clone()).or_default()[event.position as usize - 1] = slot(event);
            if !models.contains(&event.model) {
                models.push(event.model.clone());
            }
        }
        for model in &models {
            self.settle(model);
        }
        models
    }

    fn stale(models: &[String], effects: &mut Vec<Effect>) { effects.extend(models.iter().map(|model| Effect::Stale { model: model.clone(), all: true })); }

    fn gap_all(&mut self, effects: &mut Vec<Effect>) {
        for model in self.models() {
            for stream in self.streams_of(&model) {
                self.obs.push(Obs::Gap { stream });
            }
        }
        Self::stale(&self.models(), effects);
    }

    fn ready_all(&self, effects: &mut Vec<Effect>) {
        for model in self.models() {
            effects.extend(self.streams_of(&model).into_iter().map(Effect::ModelReady));
        }
    }

    /// `held` -> `Lost` (all, or the models named): the models whose windows turn stale.
    fn lose_held(&mut self, only: Option<&[String]>) -> Vec<String> {
        let (lost, kept): (Vec<Ev>, Vec<Ev>) = std::mem::take(&mut self.held).into_iter().partition(|event| only.is_none_or(|only| only.contains(&event.model)));
        self.held = kept;
        self.mark(&lost, |_| Slot::Lost)
    }

    fn commit(&mut self, events: Vec<Ev>, effects: &mut Vec<Effect>) {
        let mut bases: Vec<u64> = Vec::new();
        for event in &events {
            if !bases.contains(&event.basis) {
                bases.push(event.basis);
            }
        }
        for basis in bases {
            self.landing.push_back(events.iter().filter(|event| event.basis == basis).cloned().collect());
            effects.push(Effect::Commit);
        }
    }

    fn commit_pending(&mut self, effects: &mut Vec<Effect>) {
        let events = std::mem::take(&mut self.pending);
        self.commit(events, effects);
    }

    fn seat(&mut self, events: Vec<Ev>, effects: &mut Vec<Effect>) {
        let (mut now, mut waiting) = (Vec::new(), false);
        for event in events {
            if self.debounce {
                waiting |= self.pending.is_empty() && !waiting;
                self.pending.push(event);
            } else {
                now.push(event);
            }
        }
        if waiting {
            effects.push(Effect::ScheduleTick);
        }
        self.commit(now, effects);
    }

    fn verdict(&mut self, rejected: Option<Reject>) -> Vec<Effect> {
        let Some(batch) = self.landing.pop_front() else { return Vec::new() };
        let mut effects = Vec::new();
        match rejected {
            None => {
                let mut models: Vec<String> = Vec::new();
                for event in &batch {
                    self.ledger.entry(event.model.clone()).or_default()[event.position as usize - 1] = Slot::Landed(event.payload.clone());
                    if !models.contains(&event.model) {
                        models.push(event.model.clone());
                    }
                }
                for model in &models {
                    self.settle(model);
                }
                if self.reread {
                    effects.extend(models.into_iter().map(|model| Effect::Stale { model, all: false }));
                }
            }
            Some(Reject::StaleGeneration(models)) => {
                let (lost, rest): (Vec<Ev>, Vec<Ev>) = batch.into_iter().partition(|event| models.contains(&event.model));
                let lost_models = self.mark(&lost, |_| Slot::Lost);
                Self::stale(&lost_models, &mut effects);
                let rest = rest.into_iter().map(|event| Ev { basis: self.basis(&event.model), ..event }).collect();
                self.commit(rest, &mut effects);
            }
            Some(Reject::Storage | Reject::Invalid) => {
                let lost_models = self.mark(&batch, |_| Slot::Lost);
                Self::stale(&lost_models, &mut effects);
            }
            Some(Reject::NoSession | Reject::Closed) => {
                self.mode = Mode::Ended;
                self.close_all(&mut effects);
                self.held.clear();
                self.pending.clear();
            }
        }
        effects
    }

    /// A connection closes: an established one first says goodbye.
    fn goodbye(proto: Option<Proto>, effects: &mut Vec<Effect>) {
        let Some(proto) = proto else { return };
        if proto == Proto::Established {
            effects.push(Effect::SealUnsubscribe);
        }
        effects.push(Effect::Close);
    }

    /// Both connections close, the active one first.
    fn close_all(&mut self, effects: &mut Vec<Effect>) {
        Self::goodbye(self.active.take(), effects);
        Self::goodbye(self.opening.take(), effects);
    }

    /// `Closing` of a live feed with an established connection: the connection drains before it is cancelled.
    fn drain_all(&mut self, effects: &mut Vec<Effect>) {
        Self::goodbye(self.active.take(), effects);
        effects.push(Effect::ScheduleDrain);
        Self::goodbye(self.opening.take(), effects);
    }

    fn draining(&mut self, input: Input) -> Vec<Effect> {
        let mut effects = Vec::new();
        let ends = match input {
            Input::Drain { own: true } => Some(Vec::new()),
            Input::Dropped { to: To::Draining } | Input::Ended { to: To::Draining } | Input::ChannelLost { to: To::Draining, .. } => Some(vec![Effect::CancelDrain]),
            _ => None,
        };
        if let Some(timer) = ends {
            self.mode = Mode::Ended;
            effects.push(Effect::Cancel);
            effects.extend(timer);
            self.drained = true;
            effects.push(Effect::Drained);
        }
        effects
    }

    pub(super) fn step(&mut self, input: Input) -> Vec<Effect> {
        let mut effects = Vec::new();
        if self.mode == Mode::Draining {
            return self.draining(input);
        }
        if input == Input::Closing && self.mode == Mode::Live && self.active == Some(Proto::Established) {
            self.mode = Mode::Draining;
            self.drain_all(&mut effects);
            self.held.clear();
            self.pending.clear();
            return effects;
        }
        if self.mode == Mode::Ended {
            if input == Input::Closing && !std::mem::replace(&mut self.drained, true) {
                effects.push(Effect::Drained);
            }
            return effects;
        }
        match input {
            Input::Drain { .. } => {}
            Input::EpochEnded | Input::Closing => {
                self.mode = Mode::Ended;
                self.close_all(&mut effects);
                self.held.clear();
                self.pending.clear();
                if input == Input::Closing {
                    self.drained = true;
                    effects.push(Effect::Drained);
                }
            }
            Input::WantPlus { model, id, stream } => return self.want_plus(&model, id, stream),
            Input::WantMinus { model, id } => return self.want_minus(&model, id),
            Input::HandshakeDone => {
                if self.opening == Some(Proto::Handshake) {
                    self.opening = Some(Proto::Established);
                    return self.ready();
                }
            }
            Input::FrameOpened { to, counter, events } => match (self.named(to), counter) {
                (Some(Proto::Established), Counter::Expected) => {
                    for (model, payload) in events {
                        self.event(to, &model, &payload, &mut effects);
                    }
                }
                (Some(_), _) => {
                    effects.push(Effect::Close);
                    effects.extend(self.dropped(to));
                }
                (None, _) => {}
            },
            Input::Frame { to, counter, kind } => match (self.named(to), counter) {
                (Some(Proto::Established), Counter::Expected) => match kind {
                    Kind::Ping => effects.push(Effect::SealPong),
                    Kind::Complete => return self.dropped(to),
                    Kind::OtherId | Kind::Quiet => {}
                },
                (Some(_), _) => {
                    effects.push(Effect::Close);
                    effects.extend(self.dropped(to));
                }
                (None, _) => {}
            },
            Input::ProtocolError { to } => {
                if self.named(to).is_some() {
                    return self.dropped(to);
                }
            }
            Input::Event { to, model, payload } => self.event(to, &model, &payload, &mut effects),
            Input::ChannelLost { to, dropped } => {
                let current = matches!((self.mode, to), (Mode::Opening { .. }, To::Opening) | (Mode::Live, To::Active));
                if !current {
                    return effects;
                }
                if self.mode == Mode::Live && !self.pending.is_empty() {
                    self.commit_pending(&mut effects);
                }
                match dropped {
                    Some(counts) => {
                        let models: Vec<String> = counts.iter().filter(|(model, _)| self.want.contains_key(model)).map(|(model, _)| model.clone()).collect();
                        for (model, count) in &counts {
                            if self.want.contains_key(model) {
                                self.lose(model, *count);
                            }
                        }
                        Self::stale(&models, &mut effects);
                    }
                    None => self.gap_all(&mut effects),
                }
            }
            Input::Dropped { to } | Input::Ended { to } => return self.dropped(to),
            Input::Resubscribe { to } => {
                if matches!(self.mode, Mode::Opening { .. }) && self.online() && to == To::Opening && self.opening.is_some() {
                    self.opening = Some(Proto::Handshake);
                    effects.push(Effect::Connect);
                }
            }
            Input::HandshakeDeadline { to } => {
                if self.named(to) == Some(Proto::Handshake) {
                    effects.push(Effect::Close);
                    effects.extend(self.dropped(to));
                }
            }
            Input::Committed => return self.verdict(None),
            Input::CommittedStale(parents) => {
                let landed = !self.landing.is_empty();
                let mut effects = self.verdict(None);
                if landed { effects.extend(parents.into_iter().map(|model| Effect::Stale { model, all: true })); }
                return effects;
            }
            Input::Rejected(reason) => return self.verdict(Some(reason)),
            Input::StrayBatch => {}
            Input::Tick => {
                if self.mode == Mode::Live && !self.pending.is_empty() {
                    self.commit_pending(&mut effects);
                }
            }
            Input::Online(on) => return self.online_changed(on),
            Input::ModelsReset { models, generation } => {
                for model in &models {
                    self.generation.insert(model.clone(), generation);
                }
                if matches!(self.mode, Mode::Opening { .. }) {
                    let lost = self.lose_held(Some(&models));
                    Self::stale(&lost, &mut effects);
                }
            }
        }
        effects
    }
}

mod ref_feed_wants;
