//! `specs/machines/feed.md` as code: one match arm per table row. No library code.

use std::collections::{BTreeMap, VecDeque};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Reject {
    StaleGeneration(Vec<String>),
    Storage,
    NoSession,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Input {
    /// A want of a window with `live`: no `Joined` answer.
    WantPlus { model: String },
    /// A want of an `Events` stream: `Joined` answers it.
    WantDelivery { model: String },
    WantMinus { model: String },
    Ready,
    Event { current: bool, model: String, id: String },
    ChannelLost { dropped: u64, models: Vec<String> },
    Dropped,
    /// `SubscriptionEnded`: the cells of `Dropped`.
    Ended,
    Resubscribe,
    /// Every frame and every `Resubscribe` of a connection that is not the current one.
    Stray,
    Committed,
    Rejected(Reject),
    /// `Committed` or `Rejected` of a batch that is not in `landing`.
    StrayBatch,
    Tick,
    Online(bool),
    ModelsReset { models: Vec<String> },
    EpochStarted,
    EpochEnded,
    Closing,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Effect {
    Subscribe,
    Unsubscribe,
    ScheduleTick,
    ScheduleResubscribe,
    Commit,
    StaleWindows(Vec<String>),
    Joined(String),
    ModelReady(String),
    Drained,
}

/// What a reader of `Events` sees.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Obs {
    Event { model: String, position: u64, id: String },
    Lost { model: String, first: u64, last: u64 },
    Ended,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Mode {
    Off { gap: bool },
    Opening { gap: bool },
    Live,
    Ended,
}

pub(super) struct Feed {
    pub(super) mode: Mode,
    /// Readers per model: `Want-` removes the model at zero.
    pub(super) want: BTreeMap<String, u32>,
    pub(super) position: BTreeMap<String, u64>,
    pub(super) pending: Vec<(String, String)>,
    pub(super) landing: VecDeque<Vec<(String, String)>>,
    /// `updates.debounce` of the model is declared: an event waits for a `Tick`.
    pub(super) debounce: bool,
    pub(super) online: bool,
    /// A `Want+` of a new model opened a connection beside the live one.
    switching: bool,
    pub(super) obs: Vec<Obs>,
}

impl Feed {
    pub(super) fn new() -> Self {
        Self { mode: Mode::Off { gap: false }, want: BTreeMap::new(), position: BTreeMap::new(), pending: Vec::new(), landing: VecDeque::new(), debounce: true, online: true, switching: false, obs: Vec::new() }
    }

    fn at(&self, model: &str) -> u64 { self.position.get(model).copied().unwrap_or(0) }

    fn wanted(&self) -> Vec<String> { self.want.keys().cloned().collect() }

    fn deliver(&mut self, events: Vec<(String, String)>) {
        for (model, id) in events {
            let position = self.at(&model) + 1;
            self.position.insert(model.clone(), position);
            self.obs.push(Obs::Event { model, position, id });
        }
    }

    fn lose(&mut self, model: &str, count: u64) {
        let first = self.at(model) + 1;
        let last = self.at(model) + count;
        self.position.insert(model.to_string(), last);
        self.obs.push(Obs::Lost { model: model.to_string(), first, last });
    }

    fn lose_events(&mut self, events: Vec<(String, String)>, only: Option<&[String]>) -> Vec<String> {
        let mut models: Vec<String> = Vec::new();
        for (model, _) in &events {
            if only.is_none_or(|only| only.contains(model)) && !models.contains(model) { models.push(model.clone()); }
        }
        for model in &models {
            let count = events.iter().filter(|(found, _)| found == model).count() as u64;
            self.lose(model, count);
        }
        models
    }

    /// The Store answers the oldest batch of `landing`: delivered on `Committed`, lost on `Rejected`. No batch in `landing` is a stray answer.
    fn verdict(&mut self, rejected: Option<Reject>) -> Vec<Effect> {
        let Some(batch) = self.landing.pop_front() else { return Vec::new() };
        match rejected {
            None => { self.deliver(batch); Vec::new() }
            Some(Reject::StaleGeneration(models)) => {
                let lost = self.lose_events(batch.clone(), Some(&models));
                let rest: Vec<_> = batch.into_iter().filter(|(model, _)| !models.contains(model)).collect();
                let mut effects = vec![Effect::StaleWindows(lost)];
                if !rest.is_empty() { self.landing.push_back(rest); effects.push(Effect::Commit); }
                effects
            }
            Some(Reject::Storage) => {
                let lost = self.lose_events(batch, None);
                vec![Effect::StaleWindows(lost)]
            }
            Some(Reject::NoSession) => {
                if self.mode == Mode::Live {
                    self.mode = Mode::Ended;
                    vec![Effect::Unsubscribe]
                } else {
                    Vec::new()
                }
            }
        }
    }

    pub(super) fn take_obs(&mut self) -> Vec<Obs> { std::mem::take(&mut self.obs) }

    /// Commits `pending` as a batch of `landing`.
    fn flush(&mut self, effects: &mut Vec<Effect>) {
        if self.pending.is_empty() { return; }
        self.landing.push_back(std::mem::take(&mut self.pending));
        effects.push(Effect::Commit);
    }

    fn ready_all(&self, effects: &mut Vec<Effect>) { effects.extend(self.wanted().into_iter().map(Effect::ModelReady)); }

    /// `Event` of the current connection in `Opening` or `Live`.
    fn event(&mut self, model: String, id: String) -> Vec<Effect> {
        if !self.want.contains_key(&model) { return Vec::new(); }
        if !self.debounce {
            self.landing.push_back(vec![(model, id)]);
            return vec![Effect::Commit];
        }
        let first = self.pending.is_empty();
        self.pending.push((model, id));
        if first { vec![Effect::ScheduleTick] } else { Vec::new() }
    }

    fn want_plus(&mut self, model: String, delivery: bool) -> Vec<Effect> {
        let new = !self.want.contains_key(&model);
        *self.want.entry(model.clone()).or_insert(0) += 1;
        let mut effects = Vec::new();
        if delivery { effects.push(Effect::Joined(model.clone())); }
        match self.mode {
            Mode::Off { gap } if self.online => { self.mode = Mode::Opening { gap }; effects.push(Effect::Subscribe); }
            Mode::Opening { .. } if new => { effects.push(Effect::Unsubscribe); effects.push(Effect::Subscribe); }
            Mode::Live if new => { self.switching = true; effects.push(Effect::Subscribe); }
            Mode::Live if delivery => effects.push(Effect::ModelReady(model)),
            _ => {}
        }
        effects
    }

    fn want_minus(&mut self, model: &str) -> Vec<Effect> {
        let Some(count) = self.want.get_mut(model) else { return Vec::new() };
        *count -= 1;
        if *count > 0 { return Vec::new(); }
        self.want.remove(model);
        if !self.want.is_empty() { return Vec::new(); }
        match self.mode {
            Mode::Opening { gap } => { self.mode = Mode::Off { gap }; vec![Effect::Unsubscribe] }
            Mode::Live => {
                self.mode = Mode::Off { gap: false };
                let mut effects = Vec::new();
                self.flush(&mut effects);
                effects.push(Effect::Unsubscribe);
                effects
            }
            _ => Vec::new(),
        }
    }

    pub(super) fn step(&mut self, input: Input) -> Vec<Effect> {
        match (self.mode, input) {
            (Mode::Ended, Input::Closing) => vec![Effect::Drained],
            (Mode::Ended, _) => Vec::new(),
            (_, Input::EpochEnded) => {
                self.mode = Mode::Ended;
                self.pending.clear();
                self.obs.push(Obs::Ended);
                vec![Effect::Unsubscribe]
            }
            (_, Input::Closing) => {
                self.mode = Mode::Ended;
                self.obs.push(Obs::Ended);
                vec![Effect::Unsubscribe, Effect::Drained]
            }
            (_, Input::WantPlus { model }) => self.want_plus(model, false),
            (_, Input::WantDelivery { model }) => self.want_plus(model, true),
            (_, Input::WantMinus { model }) => self.want_minus(&model),
            (_, Input::Committed) => self.verdict(None),
            (_, Input::Rejected(reason)) => self.verdict(Some(reason)),
            (_, Input::StrayBatch | Input::Stray | Input::ModelsReset { .. } | Input::EpochStarted) => Vec::new(),
            (Mode::Off { gap }, Input::Online(on)) => {
                self.online = on;
                if on && !self.want.is_empty() { self.mode = Mode::Opening { gap }; vec![Effect::Subscribe] } else { Vec::new() }
            }
            (Mode::Off { .. }, _) => Vec::new(),
            (Mode::Opening { gap }, Input::Ready) => {
                self.mode = Mode::Live;
                let mut effects = Vec::new();
                if gap {
                    let wanted = self.wanted();
                    for model in &wanted { self.lose(model, 1); }
                    effects.push(Effect::StaleWindows(wanted));
                }
                self.ready_all(&mut effects);
                effects
            }
            (Mode::Opening { gap }, Input::Dropped | Input::Ended) => {
                if self.online { vec![Effect::ScheduleResubscribe] } else { self.mode = Mode::Off { gap }; Vec::new() }
            }
            (Mode::Opening { gap }, Input::Resubscribe) => {
                if self.online { vec![Effect::Subscribe] } else { self.mode = Mode::Off { gap }; Vec::new() }
            }
            (Mode::Opening { .. }, Input::Event { current: true, model, id }) => self.event(model, id),
            (Mode::Opening { .. }, Input::ChannelLost { dropped, models }) => {
                for model in &models { self.lose(model, dropped); }
                vec![Effect::StaleWindows(models)]
            }
            (Mode::Opening { gap }, Input::Online(on)) => {
                self.online = on;
                if on { Vec::new() } else { self.mode = Mode::Off { gap }; vec![Effect::Unsubscribe] }
            }
            (Mode::Opening { .. }, _) => Vec::new(),
            (Mode::Live, Input::Event { current: true, model, id }) => self.event(model, id),
            (Mode::Live, Input::Tick) => {
                let mut effects = Vec::new();
                self.flush(&mut effects);
                effects
            }
            (Mode::Live, Input::ChannelLost { dropped, models }) => {
                let mut effects = Vec::new();
                self.flush(&mut effects);
                for model in &models { self.lose(model, dropped); }
                effects.push(Effect::StaleWindows(models));
                effects
            }
            (Mode::Live, Input::Dropped | Input::Ended) => {
                let mut effects = Vec::new();
                self.flush(&mut effects);
                if self.online {
                    self.mode = Mode::Opening { gap: true };
                    effects.push(Effect::Subscribe);
                } else {
                    self.mode = Mode::Off { gap: true };
                }
                effects
            }
            (Mode::Live, Input::Online(on)) => {
                self.online = on;
                if on { return Vec::new(); }
                self.mode = Mode::Off { gap: true };
                let mut effects = Vec::new();
                self.flush(&mut effects);
                effects.push(Effect::Unsubscribe);
                effects
            }
            (Mode::Live, Input::Ready) => {
                if !self.switching { return Vec::new(); }
                self.switching = false;
                let mut effects = vec![Effect::Unsubscribe];
                self.ready_all(&mut effects);
                effects
            }
            (Mode::Live, _) => Vec::new(),
        }
    }
}
