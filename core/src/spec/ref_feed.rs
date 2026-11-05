//! `specs/machines/feed.md` as code: one match arm per table row. No library code.

use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Reject {
    StaleGeneration(Vec<String>),
    Storage,
    NoSession,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Input {
    WantPlus { model: String },
    WantMinus { model: String },
    Ready,
    Event { current: bool, model: String, id: String },
    ChannelLost { dropped: u64, models: Vec<String> },
    Dropped,
    Committed,
    Rejected(Reject),
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
    Off,
    Opening { gap: bool },
    Live,
    Ended,
}

pub(super) struct Feed {
    pub(super) mode: Mode,
    pub(super) want: BTreeSet<String>,
    pub(super) position: BTreeMap<String, u64>,
    pub(super) pending: Vec<(String, String)>,
    pub(super) landing: Vec<(String, String)>,
    pub(super) online: bool,
    pub(super) session: bool,
    pub(super) obs: Vec<Obs>,
}

impl Feed {
    pub(super) fn new() -> Self {
        Self { mode: Mode::Off, want: BTreeSet::new(), position: BTreeMap::new(), pending: Vec::new(), landing: Vec::new(), online: true, session: true, obs: Vec::new() }
    }

    fn at(&self, model: &str) -> u64 { self.position.get(model).copied().unwrap_or(0) }

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

    pub(super) fn take_obs(&mut self) -> Vec<Obs> { std::mem::take(&mut self.obs) }

    pub(super) fn step(&mut self, input: Input) -> Vec<Effect> {
        match (self.mode, input) {
            (Mode::Ended, _) => Vec::new(),
            (Mode::Off | Mode::Opening { .. } | Mode::Live, Input::EpochEnded) => {
                self.mode = Mode::Ended;
                self.pending.clear();
                self.obs.push(Obs::Ended);
                vec![Effect::Unsubscribe]
            }
            (Mode::Off | Mode::Opening { .. } | Mode::Live, Input::Closing) => {
                self.mode = Mode::Ended;
                self.obs.push(Obs::Ended);
                vec![Effect::Unsubscribe, Effect::Drained]
            }
            (Mode::Off, Input::WantPlus { model }) => {
                self.want.insert(model);
                if self.session && self.online {
                    self.mode = Mode::Opening { gap: false };
                    vec![Effect::Subscribe]
                } else {
                    Vec::new()
                }
            }
            (Mode::Off, Input::WantMinus { model }) => { self.want.remove(&model); Vec::new() }
            (Mode::Off, Input::Online(on)) => {
                self.online = on;
                if on && !self.want.is_empty() { self.mode = Mode::Opening { gap: false }; vec![Effect::Subscribe] } else { Vec::new() }
            }
            (Mode::Off, Input::EpochStarted) => {
                self.session = true;
                if self.online && !self.want.is_empty() { self.mode = Mode::Opening { gap: false }; vec![Effect::Subscribe] } else { Vec::new() }
            }
            (Mode::Off, _) => Vec::new(),
            (Mode::Opening { gap }, Input::Ready) => {
                self.mode = Mode::Live;
                if !gap { return Vec::new(); }
                let wanted: Vec<String> = self.want.iter().cloned().collect();
                for model in &wanted { self.lose(model, 1); }
                vec![Effect::StaleWindows(wanted)]
            }
            (Mode::Opening { gap }, Input::Dropped) => {
                self.mode = Mode::Opening { gap };
                if self.online { vec![Effect::ScheduleResubscribe] } else { Vec::new() }
            }
            (Mode::Opening { .. }, Input::WantPlus { model }) => { let changed = self.want.insert(model); if changed { vec![Effect::Subscribe] } else { Vec::new() } }
            (Mode::Opening { .. }, Input::WantMinus { model }) => { let changed = self.want.remove(&model); if changed { vec![Effect::Subscribe] } else { Vec::new() } }
            (Mode::Opening { .. }, Input::Online(on)) => { self.online = on; Vec::new() }
            (Mode::Opening { .. }, Input::Committed) => {
                // Hole of the table: a batch committed after the connection broke. P-FEED-02 requires delivery.
                let batch = std::mem::take(&mut self.landing);
                self.deliver(batch);
                Vec::new()
            }
            (Mode::Opening { .. }, _) => Vec::new(),
            (Mode::Live, Input::Event { current: true, model, id }) => {
                let first = self.pending.is_empty();
                self.pending.push((model, id));
                if first { vec![Effect::ScheduleTick] } else { Vec::new() }
            }
            (Mode::Live, Input::Event { current: false, .. }) => Vec::new(),
            (Mode::Live, Input::Tick) => {
                if self.pending.is_empty() { return Vec::new(); }
                let batch = std::mem::take(&mut self.pending);
                self.landing.extend(batch);
                vec![Effect::Commit]
            }
            (Mode::Live, Input::Committed) => {
                let batch = std::mem::take(&mut self.landing);
                self.deliver(batch);
                Vec::new()
            }
            (Mode::Live, Input::Rejected(reason)) => {
                let batch = std::mem::take(&mut self.landing);
                match reason {
                    Reject::StaleGeneration(models) => {
                        let lost = self.lose_events(batch.clone(), Some(&models));
                        let rest: Vec<_> = batch.into_iter().filter(|(model, _)| !models.contains(model)).collect();
                        let mut effects = vec![Effect::StaleWindows(lost)];
                        if !rest.is_empty() { self.landing.extend(rest); effects.push(Effect::Commit); }
                        effects
                    }
                    Reject::Storage => {
                        let lost = self.lose_events(batch, None);
                        vec![Effect::StaleWindows(lost)]
                    }
                    Reject::NoSession => { self.mode = Mode::Ended; Vec::new() }
                }
            }
            (Mode::Live, Input::ChannelLost { dropped, models }) => {
                let pending = std::mem::take(&mut self.pending);
                let mut effects = Vec::new();
                if !pending.is_empty() {
                    self.landing.extend(pending);
                    effects.push(Effect::Commit);
                }
                for model in &models { self.lose(model, dropped); }
                effects.push(Effect::StaleWindows(models));
                effects
            }
            (Mode::Live, Input::Dropped) => {
                self.mode = Mode::Opening { gap: true };
                let mut effects = Vec::new();
                if !self.pending.is_empty() { self.landing.extend(std::mem::take(&mut self.pending)); effects.push(Effect::Commit); }
                effects.push(Effect::Subscribe);
                effects
            }
            (Mode::Live, Input::Online(on)) => {
                self.online = on;
                if on { return Vec::new(); }
                self.mode = Mode::Opening { gap: true };
                let mut effects = Vec::new();
                if !self.pending.is_empty() { self.landing.extend(std::mem::take(&mut self.pending)); effects.push(Effect::Commit); }
                effects.push(Effect::Unsubscribe);
                effects
            }
            (Mode::Live, Input::ModelsReset { .. }) => Vec::new(),
            (Mode::Live, Input::WantMinus { model }) => {
                self.want.remove(&model);
                if !self.want.is_empty() { return Vec::new(); }
                self.mode = Mode::Off;
                let mut effects = Vec::new();
                if !self.pending.is_empty() { self.landing.extend(std::mem::take(&mut self.pending)); effects.push(Effect::Commit); }
                effects.push(Effect::Unsubscribe);
                effects
            }
            (Mode::Live, Input::WantPlus { model }) => { let changed = self.want.insert(model); if changed { vec![Effect::Subscribe] } else { Vec::new() } }
            (Mode::Live, _) => Vec::new(),
        }
    }
}
