use std::collections::{BTreeMap, BTreeSet};

pub(crate) type FlightId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum LoadKind {
    Initial,
    Refresh,
    More,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadPhase {
    NotLoaded,
    Loading,
    Loaded,
    Empty,
    Failed,
    Closed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadError {
    pub kind: &'static str,
    pub message: String,
}

impl LoadError {
    pub(crate) const NETWORK: &'static str = "network";

    pub(crate) fn internal() -> Self {
        Self { kind: "internal", message: "load panicked".to_string() }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationState {
    pub phase: LoadPhase,
    pub initial: bool,
    pub refreshing: bool,
    pub fetching_more: bool,
    pub has_more: bool,
    pub total: Option<i64>,
    pub error: Option<LoadError>,
    pub loaded_at: Option<i64>,
}

impl RelationState {
    pub(crate) fn closed() -> Self {
        Self { phase: LoadPhase::Closed, initial: false, refreshing: false, fetching_more: false, has_more: false, total: None, error: None, loaded_at: None }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Data {
    Unread,
    NotLoaded,
    Local,
    Landed { has_more: bool, total: Option<i64>, loaded_at: i64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Order {
    pub applied: i64,
    pub pages: i64,
}

impl Order {
    pub(crate) const NONE: Order = Order { applied: -1, pages: 0 };
}

/// What the store held for a window when a flight thread read it. `epoch` is the reset counter read before the store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Stored {
    pub epoch: u64,
    pub order: Order,
    pub data: Data,
    pub has_rows: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum LastEnd {
    None,
    Landed,
    Failed(LoadError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WindowFacts {
    model: String,
    flights: BTreeMap<FlightId, LoadKind>,
    data: Data,
    has_rows: bool,
    order: Order,
    last_end: LastEnd,
    retry_on_online: Option<LoadKind>,
    invalid: Option<LoadError>,
    read_error: Option<LoadError>,
}

impl WindowFacts {
    fn new(model: &str) -> Self {
        Self {
            model: model.to_string(),
            flights: BTreeMap::new(),
            data: Data::Unread,
            has_rows: false,
            order: Order::NONE,
            last_end: LastEnd::None,
            retry_on_online: None,
            invalid: None,
            read_error: None,
        }
    }

    fn phase(&self, closed: bool) -> LoadPhase {
        if closed {
            return LoadPhase::Closed;
        }
        if self.invalid.is_some() {
            return LoadPhase::Failed;
        }
        if !self.flights.is_empty() {
            return LoadPhase::Loading;
        }
        if self.read_error.is_some() {
            return LoadPhase::Failed;
        }
        if matches!(self.last_end, LastEnd::Failed(_)) {
            return if self.has_rows { LoadPhase::Loaded } else { LoadPhase::Failed };
        }
        match self.data {
            Data::Unread | Data::NotLoaded => LoadPhase::NotLoaded,
            Data::Local | Data::Landed { .. } if !self.has_rows => LoadPhase::Empty,
            _ => LoadPhase::Loaded,
        }
    }

    fn view(&self, closed: bool) -> RelationState {
        let (has_more, total, loaded_at) = match self.data {
            Data::Landed { has_more, total, loaded_at } => (has_more, total, Some(loaded_at)),
            _ => (false, None, None),
        };
        let failed = match &self.last_end {
            LastEnd::Failed(error) => Some(error),
            _ => None,
        };
        let error = self.invalid.as_ref().or(self.read_error.as_ref()).or(failed).cloned();
        let running = |kind: LoadKind| self.flights.values().any(|running| *running == kind);
        RelationState {
            phase: self.phase(closed),
            initial: running(LoadKind::Initial),
            refreshing: running(LoadKind::Refresh),
            fetching_more: running(LoadKind::More),
            has_more,
            total,
            error,
            loaded_at,
        }
    }

    fn seed(&mut self, stored: Option<Stored>) {
        if self.data == Data::Unread {
            self.accept(stored);
        }
    }

    fn accept(&mut self, stored: Option<Stored>) {
        let Some(stored) = stored else { return };
        if stored.order < self.order {
            return;
        }
        self.order = stored.order;
        self.data = stored.data;
        self.has_rows = stored.has_rows;
    }

    fn end(&mut self, flight: FlightId) -> Option<LoadKind> {
        self.flights.remove(&flight)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Event {
    Observe { window: String, model: String, seed: Option<Stored> },
    Start { window: String, model: String, flight: FlightId, cursor: bool, seed: Option<Stored> },
    Landed { window: String, flight: FlightId, stored: Option<Stored> },
    Discarded { window: String, flight: FlightId },
    Failed { window: String, flight: FlightId, error: LoadError },
    Abandoned { window: String, flight: FlightId },
    Panicked { window: String, flight: FlightId },
    Reset { epoch: u64, models: Option<BTreeSet<String>> },
    Offline,
    Online { observed: BTreeSet<String> },
    Close,
    #[allow(dead_code)]
    Rejected { window: String, model: String, error: LoadError },
    #[allow(dead_code)]
    ReadOk { window: String, has_rows: bool },
    #[allow(dead_code)]
    ReadFailed { window: String, error: LoadError },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Effect {
    Publish { window: String, state: RelationState },
    Retry { window: String, kind: LoadKind },
}

#[derive(Debug, Default)]
pub(crate) struct Machine {
    windows: BTreeMap<String, WindowFacts>,
    closed: bool,
    all_epoch: u64,
    model_epochs: BTreeMap<String, u64>,
    pub(crate) orphan_ends: u32,
    pub(crate) after_close: u32,
}

impl Machine {
    pub(crate) fn view(&self, window: &str) -> Option<RelationState> {
        self.windows.get(window).map(|facts| facts.view(self.closed))
    }

    #[cfg(test)]
    pub(crate) fn flights(&self, window: &str) -> usize {
        self.windows.get(window).map_or(0, |facts| facts.flights.len())
    }

    fn epoch_of(&self, model: &str) -> u64 {
        self.all_epoch.max(self.model_epochs.get(model).copied().unwrap_or(0))
    }

    fn fresh(&self, model: &str, stored: Option<Stored>) -> Option<Stored> {
        stored.filter(|stored| stored.epoch >= self.epoch_of(model))
    }

    fn facts(&mut self, window: &str, model: &str) -> &mut WindowFacts {
        self.windows.entry(window.to_string()).or_insert_with(|| WindowFacts::new(model))
    }

    fn open(&mut self, window: &str, flight: FlightId) -> Option<(&mut WindowFacts, LoadKind)> {
        let Some(facts) = self.windows.get_mut(window) else {
            self.orphan_ends += 1;
            return None;
        };
        match facts.end(flight) {
            Some(kind) => Some((facts, kind)),
            None => {
                self.orphan_ends += 1;
                None
            }
        }
    }

    fn touched(&self, event: &Event) -> Vec<String> {
        match event {
            Event::Observe { window, .. }
            | Event::Start { window, .. }
            | Event::Landed { window, .. }
            | Event::Discarded { window, .. }
            | Event::Failed { window, .. }
            | Event::Abandoned { window, .. }
            | Event::Panicked { window, .. }
            | Event::Rejected { window, .. }
            | Event::ReadOk { window, .. }
            | Event::ReadFailed { window, .. } => vec![window.clone()],
            Event::Reset { .. } | Event::Online { .. } | Event::Close | Event::Offline => self.windows.keys().cloned().collect(),
        }
    }

    fn apply(&mut self, event: Event, effects: &mut Vec<Effect>) {
        if self.closed {
            self.after_close += 1;
            return;
        }
        match event {
            Event::Observe { window, model, seed } => {
                let seed = self.fresh(&model, seed);
                self.facts(&window, &model).seed(seed);
            }
            Event::Start { window, model, flight, cursor, seed } => {
                let seed = self.fresh(&model, seed);
                let facts = self.facts(&window, &model);
                facts.seed(seed);
                let kind = match (cursor, &facts.data) {
                    (true, _) => LoadKind::More,
                    (false, Data::Landed { .. } | Data::Local) => LoadKind::Refresh,
                    (false, _) => LoadKind::Initial,
                };
                facts.flights.insert(flight, kind);
                facts.last_end = LastEnd::None;
                facts.retry_on_online = None;
            }
            Event::Landed { window, flight, stored } => {
                let Some(model) = self.windows.get(&window).map(|facts| facts.model.clone()) else {
                    self.orphan_ends += 1;
                    return;
                };
                let stored = self.fresh(&model, stored);
                if let Some((facts, _)) = self.open(&window, flight) {
                    facts.last_end = LastEnd::Landed;
                    facts.retry_on_online = None;
                    facts.accept(stored);
                }
            }
            Event::Discarded { window, flight } | Event::Abandoned { window, flight } => {
                self.open(&window, flight);
            }
            Event::Failed { window, flight, error } => {
                if let Some((facts, kind)) = self.open(&window, flight) {
                    facts.retry_on_online = (error.kind == LoadError::NETWORK).then_some(kind);
                    facts.last_end = LastEnd::Failed(error);
                }
            }
            Event::Panicked { window, flight } => {
                if let Some((facts, _)) = self.open(&window, flight) {
                    facts.retry_on_online = None;
                    facts.last_end = LastEnd::Failed(LoadError::internal());
                }
            }
            Event::Reset { epoch, models } => {
                match &models {
                    Some(models) => {
                        for model in models {
                            let known = self.model_epochs.entry(model.clone()).or_insert(0);
                            *known = (*known).max(epoch);
                        }
                    }
                    None => self.all_epoch = self.all_epoch.max(epoch),
                }
                for facts in self.windows.values_mut().filter(|facts| models.as_ref().is_none_or(|models| models.contains(&facts.model))) {
                    facts.data = Data::NotLoaded;
                    facts.has_rows = false;
                    facts.order = Order::NONE;
                    facts.last_end = LastEnd::None;
                    facts.read_error = None;
                    facts.retry_on_online = None;
                }
            }
            Event::Offline => {}
            Event::Online { observed } => {
                for (window, facts) in &mut self.windows {
                    if !facts.flights.is_empty() || !observed.contains(window) {
                        continue;
                    }
                    if let Some(kind) = facts.retry_on_online.take() {
                        effects.push(Effect::Retry { window: window.clone(), kind });
                    }
                }
            }
            Event::Close => self.closed = true,
            Event::Rejected { window, model, error } => self.facts(&window, &model).invalid = Some(error),
            Event::ReadOk { window, has_rows } => {
                if let Some(facts) = self.windows.get_mut(&window) {
                    facts.has_rows = has_rows;
                    facts.read_error = None;
                }
            }
            Event::ReadFailed { window, error } => {
                if let Some(facts) = self.windows.get_mut(&window) {
                    facts.read_error = Some(error);
                }
            }
        }
    }
}

pub(crate) fn step(machine: &mut Machine, event: Event) -> Vec<Effect> {
    let touched = machine.touched(&event);
    let before: BTreeMap<String, Option<RelationState>> = touched.iter().map(|window| (window.clone(), machine.view(window))).collect();
    let mut effects = Vec::new();
    let was_closed = machine.closed;
    machine.apply(event, &mut effects);
    let mut windows = touched;
    if !was_closed && machine.closed {
        windows = machine.windows.keys().cloned().collect();
    }
    for window in windows {
        let after = machine.view(&window);
        if let Some(state) = after.filter(|after| before.get(&window).is_none_or(|before| before.as_ref() != Some(after))) {
            effects.push(Effect::Publish { window, state });
        }
    }
    effects
}
