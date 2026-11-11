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
pub(super) enum Reread {
    None,
    All,
    Set(Vec<String>),
}

pub(super) fn absent_payload(via_node: bool, incomplete_object: bool) -> Option<Reread> {
    if via_node && !incomplete_object { Some(Reread::Set(vec!["node".into()])) } else { None }
}

impl Reread {
    fn union(self, other: &Reread) -> Reread {
        match (self, other) {
            (Reread::All, _) | (_, Reread::All) => Reread::All,
            (Reread::None, other) => other.clone(),
            (keys, Reread::None) => keys,
            (Reread::Set(mut keys), Reread::Set(more)) => {
                let fresh: Vec<String> = more.iter().filter(|key| !keys.contains(key)).cloned().collect();
                keys.extend(fresh);
                Reread::Set(keys)
            }
        }
    }
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
    Dropped { to: To },
    /// `SubscriptionEnded`: the cells of `Dropped`.
    Ended { to: To },
    Resubscribe { to: To },
    /// The deadline of a `Connect`: a connection in `Handshake` fails like a protocol error.
    HandshakeDeadline { to: To },
    Committed,
    CommittedAt(usize),
    RejectedAt(usize, Reject),
    Refused { model: String },
    /// `Committed` of a batch whose children left parents without a row: the windows of these models turn `Stale`.
    CommittedStale(Vec<String>),
    Rejected(Reject),
    /// `Committed` or `Rejected` of a batch that is not in `landing`.
    StrayBatch,
    StrayRepeat,
    Tick,
    /// `Tick(keepalive{connection_id})` of the active connection.
    Keepalive,
    /// `Tick(pong_deadline{connection_id})` of the active connection.
    PongDeadline,
    Online(bool),
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
    SealPing,
    SchedulePong,
    Close,
    Cancel,
    ScheduleDrain,
    CancelDrain,
    ScheduleTick,
    ScheduleResubscribe,
    Gap(Vec<String>),
    Commit,
    Stale { model: String, all: bool },
    StaleSet { model: String, keys: Vec<String> },
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

/// What a draining feed becomes when the drain is over.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Then {
    End,
    Off { gap: bool },
    Reopen,
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
    reread: Reread,
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
    stale_landing: VecDeque<Option<Vec<String>>>,
    stale_retry: Vec<Vec<String>>,
    /// The protocol of the connections: `active` and `opening`.
    active: Option<Proto>,
    opening: Option<Proto>,
    /// `updates.debounce` is declared for every model: an event waits for a `Tick`.
    pub(super) debounce: bool,
    generation: BTreeMap<String, u64>,
    pub(super) reread: Reread,
    pub(super) refused: u64,
    /// Connection attempts since the last data: a reconnect bumps them, the first `Event` after `Ready` resets them.
    pub(super) attempts: u32,
    pub(super) marks: Vec<bool>,
    pub(super) obs: Vec<Obs>,
    drained: bool,
    then: Then,
    closing_asked: bool,
    /// The time of the step that is running (R5): the one time every term is compared with.
    pub(super) now: u64,
    last_sent: u64,
    keepalive_at: Option<u64>,
    pong_due: Option<u64>,
}

const KEEPALIVE_MS: u64 = 25_000;
const PONG_DEADLINE_MS: u64 = 10_000;

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
            stale_landing: VecDeque::new(),
            stale_retry: Vec::new(),
            active: None,
            opening: None,
            debounce: true,
            generation: BTreeMap::new(),
            reread: Reread::None,
            refused: 0,
            attempts: 0,
            marks: Vec::new(),
            obs: Vec::new(),
            drained: false,
            then: Then::End,
            closing_asked: false,
            now: 0,
            last_sent: 0,
            keepalive_at: None,
            pong_due: None,
        }
    }

    pub(super) fn take_obs(&mut self) -> Vec<Obs> { std::mem::take(&mut self.obs) }

    pub(super) fn take_marks(&mut self) -> Vec<bool> { std::mem::take(&mut self.marks) }

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
        Ev { model: model.to_string(), payload: payload.to_string(), position, basis: self.basis(model), reread: self.reread.clone() }
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

    fn lose_held(&mut self, effects: &mut Vec<Effect>) -> Vec<String> {
        let lost = std::mem::take(&mut self.held);
        let models = self.mark(&lost, |_| Slot::Lost);
        effects.push(Effect::Gap(models.clone()));
        models
    }

    fn commit(&mut self, events: Vec<Ev>, effects: &mut Vec<Effect>) {
        let mut bases: Vec<u64> = Vec::new();
        for event in &events {
            if !bases.contains(&event.basis) {
                bases.push(event.basis);
            }
        }
        for basis in bases {
            let batch: Vec<Ev> = events.iter().filter(|event| event.basis == basis).cloned().collect();
            let observed = batch.iter().any(|event| event.payload.starts_with('{') && !self.streams_of(&event.model).is_empty());
            self.marks.push(observed);
            self.landing.push_back(batch);
            self.stale_landing.push_back(None);
            effects.push(Effect::Commit);
        }
    }

    fn commit_gap(&mut self, models: Vec<String>, effects: &mut Vec<Effect>) {
        if models.is_empty() { return; }
        self.landing.push_back(Vec::new());
        self.stale_landing.push_back(Some(models));
        self.marks.push(false);
        effects.push(Effect::Commit);
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

    fn verdict(&mut self, nth: usize, rejected: Option<Reject>) -> Vec<Effect> {
        let Some(batch) = self.landing.remove(nth) else { return Vec::new() };
        if let Some(Some(models)) = self.stale_landing.remove(nth) {
            let mut effects = Vec::new();
            match rejected {
                Some(Reject::StaleGeneration(stale)) => {
                    self.generation.extend(stale.into_iter().map(|model| (model, 4)));
                    self.commit_gap(models, &mut effects);
                }
                Some(Reject::Storage) => self.stale_retry.push(models),
                Some(Reject::Invalid) => Self::stale(&models, &mut effects),
                _ => {},
            }
            return effects;
        }
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
                for model in models {
                    let keys = batch.iter().filter(|event| event.model == model).fold(Reread::None, |keys, event| keys.union(&event.reread));
                    match keys {
                        Reread::None => {}
                        Reread::All => effects.push(Effect::Stale { model, all: true }),
                        Reread::Set(mut keys) => {
                            keys.sort();
                            keys.dedup();
                            effects.push(Effect::StaleSet { model, keys });
                        }
                    }
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

    /// A live feed with an established connection closes it: the connection drains before it is cancelled, then the feed goes on as `then`.
    fn drain_all(&mut self, then: Then, effects: &mut Vec<Effect>) {
        self.mode = Mode::Draining;
        self.then = then;
        Self::goodbye(self.active.take(), effects);
        effects.push(Effect::ScheduleDrain);
        Self::goodbye(self.opening.take(), effects);
    }

    fn drain_over(&mut self, timer: bool, then: Then) -> Vec<Effect> {
        let mut effects = vec![Effect::Cancel];
        if timer {
            effects.push(Effect::CancelDrain);
        }
        match then {
            Then::End => {
                self.mode = Mode::Ended;
                if std::mem::take(&mut self.closing_asked) {
                    self.drained = true;
                    effects.push(Effect::Drained);
                }
            }
            Then::Off { gap } => self.go_on(gap, &mut effects),
            Then::Reopen => self.go_on(true, &mut effects),
        }
        effects
    }

    fn go_on(&mut self, gap: bool, effects: &mut Vec<Effect>) {
        if self.want.is_empty() || !self.online() {
            self.mode = Mode::Off { gap };
            return;
        }
        self.mode = Mode::Opening { gap };
        self.opening = Some(Proto::Handshake);
        effects.push(Effect::Connect);
    }

    fn draining(&mut self, input: Input) -> Vec<Effect> {
        let then = self.then;
        match input {
            Input::Drain { own: true } => self.drain_over(false, then),
            Input::Dropped { to: To::Draining } | Input::Ended { to: To::Draining } | Input::ProtocolError { to: To::Draining } => self.drain_over(true, then),
            Input::EpochEnded => self.drain_over(true, Then::End),
            Input::Closing => {
                self.closing_asked = true;
                self.then = Then::End;
                Vec::new()
            }
            Input::WantPlus { model, id, stream } if then != Then::End => self.want_plus(&model, id, stream),
            Input::WantMinus { model, id } if then != Then::End => self.want_minus(&model, id),
            Input::Online(on) if then != Then::End => self.online_changed(on),
            _ => Vec::new(),
        }
    }

    pub(super) fn step(&mut self, input: Input) -> Vec<Effect> {
        let mut effects = Vec::new();
        for models in std::mem::take(&mut self.stale_retry) { self.commit_gap(models, &mut effects); }
        effects.extend(self.step_inner(input));
        let first = effects.iter().position(|effect| matches!(effect, Effect::Gap(_)));
        let models: BTreeSet<_> = effects.iter().filter_map(|effect| match effect { Effect::Gap(models) => Some(models), _ => None }).flatten().cloned().collect();
        effects.retain(|effect| !matches!(effect, Effect::Gap(_)));
        if let Some(first) = first {
            let mut batch = Vec::new();
            self.commit_gap(models.into_iter().collect(), &mut batch);
            effects.splice(first..first, batch);
        }
        if effects.iter().any(|effect| matches!(effect, Effect::SealSubscribe | Effect::SealUnsubscribe | Effect::SealPong | Effect::SealPing)) {
            self.last_sent = self.now;
            self.keepalive_at = Some(self.now + KEEPALIVE_MS);
        }
        if effects.contains(&Effect::SealPing) {
            self.pong_due = Some(self.now + PONG_DEADLINE_MS);
        }
        if self.mode != Mode::Live {
            self.keepalive_at = None;
            self.pong_due = None;
        }
        effects
    }

    /// The timers of the active connection that are due at the time of the step run as the steps of their own.
    pub(super) fn fire_due(&mut self) -> Vec<Effect> {
        let mut effects = Vec::new();
        if self.keepalive_at.is_some_and(|at| at <= self.now) {
            effects.extend(self.step(Input::Keepalive));
        }
        if self.pong_due.is_some_and(|due| due <= self.now) {
            effects.extend(self.step(Input::PongDeadline));
        }
        effects
    }

    fn keepalive(&mut self) -> Vec<Effect> {
        if self.mode != Mode::Live || self.active != Some(Proto::Established) || self.pong_due.is_some() || self.now.saturating_sub(self.last_sent) < KEEPALIVE_MS {
            return Vec::new();
        }
        vec![Effect::SealPing, Effect::SchedulePong]
    }

    fn pong_deadline(&mut self) -> Vec<Effect> {
        if self.mode != Mode::Live || self.active != Some(Proto::Established) || !self.pong_due.is_some_and(|due| self.now >= due) {
            return Vec::new();
        }
        let mut effects = vec![Effect::Cancel];
        effects.extend(self.dropped(To::Active));
        effects
    }

    fn step_inner(&mut self, input: Input) -> Vec<Effect> {
        let verdict = matches!(input, Input::Committed | Input::CommittedStale(_) | Input::Rejected(_));
        if self.mode == Mode::Draining && !verdict {
            return self.draining(input);
        }
        let draining = self.mode == Mode::Draining;
        let mut effects = self.step_main(input);
        if draining && self.mode == Mode::Ended {
            effects.extend([Effect::Cancel, Effect::CancelDrain]);
            if std::mem::take(&mut self.closing_asked) {
                self.drained = true;
                effects.push(Effect::Drained);
            }
        }
        effects
    }

    fn step_main(&mut self, input: Input) -> Vec<Effect> {
        let mut effects = Vec::new();
        if input == Input::Closing && self.mode == Mode::Live && self.active == Some(Proto::Established) {
            self.closing_asked = true;
            self.drain_all(Then::End, &mut effects);
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
                (Some(proto), _) => {
                    effects.push(Effect::Close);
                    if self.mode == Mode::Live && to == To::Active && proto == Proto::Established {
                        effects.push(Effect::ScheduleDrain);
                        self.fail_live(&mut effects);
                    } else {
                        effects.extend(self.dropped(to));
                    }
                }
                (None, _) => {}
            },
            Input::Frame { to, counter, kind } => match (self.named(to), counter) {
                (Some(Proto::Established), Counter::Expected) => match kind {
                    Kind::Ping => effects.push(Effect::SealPong),
                    Kind::Complete => return self.dropped(to),
                    Kind::OtherId | Kind::Quiet => {}
                },
                (Some(proto), _) => {
                    effects.push(Effect::Close);
                    if self.mode == Mode::Live && to == To::Active && proto == Proto::Established {
                        effects.push(Effect::ScheduleDrain);
                        self.fail_live(&mut effects);
                    } else {
                        effects.extend(self.dropped(to));
                    }
                }
                (None, _) => {}
            },
            Input::ProtocolError { to } => {
                if self.mode == Mode::Live && to == To::Active && self.named(to) == Some(Proto::Established) {
                    effects.push(Effect::Close);
                    effects.push(Effect::ScheduleDrain);
                    self.fail_live(&mut effects);
                } else if self.named(to).is_some() {
                    return self.dropped(to);
                }
            }
            Input::Event { to, model, payload } => self.event(to, &model, &payload, &mut effects),
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
            Input::Committed => return self.verdict(0, None),
            Input::CommittedAt(nth) => return self.verdict(nth, None),
            Input::RejectedAt(nth, reason) => return self.verdict(nth, Some(reason)),
            Input::Refused { model } => {
                if self.mode == Mode::Live && self.want.contains_key(&model) {
                    self.refused += 1;
                    self.lose(&model, 1);
                }
            }
            Input::CommittedStale(parents) => {
                let landed = !self.landing.is_empty();
                let mut effects = self.verdict(0, None);
                if landed { effects.extend(parents.into_iter().map(|model| Effect::Stale { model, all: true })); }
                return effects;
            }
            Input::Rejected(reason) => return self.verdict(0, Some(reason)),
            Input::StrayBatch | Input::StrayRepeat => {}
            Input::Tick => {
                if self.mode == Mode::Live && !self.pending.is_empty() {
                    self.commit_pending(&mut effects);
                }
            }
            Input::Keepalive => return self.keepalive(),
            Input::PongDeadline => return self.pong_deadline(),
            Input::Online(on) => return self.online_changed(on),
        }
        effects
    }
}

pub(in crate::spec) mod ref_feed_channels;
mod ref_feed_wants;
