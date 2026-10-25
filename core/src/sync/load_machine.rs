use std::collections::{BTreeMap, BTreeSet};

use statig::prelude::*;

use super::error::SyncError;

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
    pub cause: Option<SyncError>,
}

impl LoadError {
    pub(crate) const NETWORK: &'static str = "network";

    pub(crate) fn new(kind: &'static str, message: impl Into<String>) -> Self {
        Self { kind, message: message.into(), cause: None }
    }

    pub(crate) fn internal() -> Self {
        Self::new("internal", "load panicked")
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
    pub revision: i64,
}

impl RelationState {
    pub(crate) fn closed() -> Self {
        Self { phase: LoadPhase::Closed, initial: false, refreshing: false, fetching_more: false, has_more: false, total: None, error: None, loaded_at: None, revision: 0 }
    }
}

pub(crate) fn snapshot_phase(state: &RelationState, size: usize) -> LoadPhase {
    match state.phase {
        LoadPhase::Closed | LoadPhase::Loading => state.phase,
        _ if state.error.is_some() => if size > 0 { LoadPhase::Loaded } else { LoadPhase::Failed },
        LoadPhase::NotLoaded => LoadPhase::NotLoaded,
        _ => if size == 0 { LoadPhase::Empty } else { LoadPhase::Loaded },
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
    pub revision: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum LastEnd {
    None,
    Landed,
    Failed(LoadError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WindowFacts {
    window: String,
    model: String,
    flights: BTreeMap<FlightId, LoadKind>,
    data: Data,
    has_rows: bool,
    order: Order,
    needs: i64,
    last_end: LastEnd,
    retry_on_online: Option<LoadKind>,
    read_error: Option<LoadError>,
    published: Option<RelationState>,
}

#[derive(Debug, Clone, Copy, Default)]
struct Epochs<'a> {
    all: u64,
    by_model: Option<&'a BTreeMap<String, u64>>,
}

impl Epochs<'_> {
    fn of(&self, model: &str) -> u64 {
        self.all.max(self.by_model.and_then(|epochs| epochs.get(model)).copied().unwrap_or(0))
    }
}

struct Context<'a> {
    epochs: Epochs<'a>,
    effects: &'a mut Vec<Effect>,
    orphan_ends: u32,
    quiet: bool,
}

impl WindowFacts {
    fn new(window: &str, model: &str) -> Self {
        Self {
            window: window.to_string(),
            model: model.to_string(),
            flights: BTreeMap::new(),
            data: Data::Unread,
            has_rows: false,
            order: Order::NONE,
            needs: 0,
            last_end: LastEnd::None,
            retry_on_online: None,
            read_error: None,
            published: None,
        }
    }

    fn rest(&self) -> State {
        if !self.flights.is_empty() {
            return State::loading();
        }
        if self.read_error.is_some() {
            return State::failed();
        }
        if matches!(self.last_end, LastEnd::Failed(_)) {
            return if self.has_rows { State::loaded() } else { State::failed() };
        }
        match self.data {
            Data::Unread | Data::NotLoaded => State::not_loaded(),
            Data::Local | Data::Landed { .. } if !self.has_rows => State::empty(),
            _ => State::loaded(),
        }
    }

    fn view(&self, phase: LoadPhase) -> RelationState {
        let (has_more, total, loaded_at) = match self.data {
            Data::Landed { has_more, total, loaded_at } => (has_more, total, Some(loaded_at)),
            _ => (false, None, None),
        };
        let failed = match &self.last_end {
            LastEnd::Failed(error) => Some(error),
            _ => None,
        };
        let error = self.read_error.as_ref().or(failed).cloned();
        let running = |kind: LoadKind| self.flights.values().any(|running| *running == kind);
        RelationState {
            phase,
            initial: running(LoadKind::Initial),
            refreshing: running(LoadKind::Refresh),
            fetching_more: running(LoadKind::More),
            has_more,
            total,
            error,
            loaded_at,
            revision: self.needs,
        }
    }

    fn announce(&mut self, context: &mut Context<'_>, phase: LoadPhase) {
        if context.quiet {
            return;
        }
        let view = self.view(phase);
        if self.published.as_ref() != Some(&view) {
            self.published = Some(view.clone());
            context.effects.push(Effect::Publish { window: self.window.clone(), state: view });
        }
    }

    fn fresh(&self, context: &Context<'_>, stored: &Option<Stored>) -> Option<Stored> {
        stored.clone().filter(|stored| stored.epoch >= context.epochs.of(&self.model))
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

    fn end(&mut self, context: &mut Context<'_>, flight: FlightId) -> Option<LoadKind> {
        let kind = self.flights.remove(&flight);
        if kind.is_none() {
            context.orphan_ends += 1;
        }
        kind
    }

    fn load_kind(&self, cursor: bool) -> LoadKind {
        match (cursor, &self.data) {
            (true, _) => LoadKind::More,
            (false, Data::Landed { .. } | Data::Local) => LoadKind::Refresh,
            (false, _) => LoadKind::Initial,
        }
    }

    fn resets(&self, models: &Option<BTreeSet<String>>) -> bool {
        models.as_ref().is_none_or(|models| models.contains(&self.model))
    }
}

#[state_machine(initial = "State::not_loaded()")]
impl WindowFacts {
    #[superstate]
    fn open(&mut self, context: &mut Context<'_>, event: &Event) -> Outcome<State> {
        match event {
            Event::Observe { seed, .. } => {
                let seed = self.fresh(context, seed);
                self.seed(seed);
            }
            Event::Start { flight, cursor, seed, .. } => {
                let seed = self.fresh(context, seed);
                self.seed(seed);
                let kind = self.load_kind(*cursor);
                self.flights.insert(*flight, kind);
                self.last_end = LastEnd::None;
                self.retry_on_online = None;
            }
            Event::Landed { flight, stored, .. } => {
                let stored = self.fresh(context, stored);
                if self.end(context, *flight).is_some() {
                    self.last_end = LastEnd::Landed;
                    self.retry_on_online = None;
                    self.needs = self.needs.max(stored.as_ref().map_or(0, |stored| stored.revision));
                    self.accept(stored);
                }
            }
            Event::Discarded { flight, .. } | Event::Abandoned { flight, .. } => {
                self.end(context, *flight);
            }
            Event::Failed { flight, error, .. } => {
                if let Some(kind) = self.end(context, *flight) {
                    self.retry_on_online = (error.kind == LoadError::NETWORK).then_some(kind);
                    self.last_end = LastEnd::Failed(error.clone());
                }
            }
            Event::Panicked { flight, .. } => {
                if self.end(context, *flight).is_some() {
                    self.retry_on_online = None;
                    self.last_end = LastEnd::Failed(LoadError::internal());
                }
            }
            Event::Reset { models, .. } => {
                if self.resets(models) {
                    self.data = Data::NotLoaded;
                    self.has_rows = false;
                    self.order = Order::NONE;
                    self.last_end = LastEnd::None;
                    self.read_error = None;
                    self.retry_on_online = None;
                }
            }
            Event::Offline => {}
            Event::Online { observed } => {
                if self.flights.is_empty() && observed.contains(&self.window) {
                    if let Some(kind) = self.retry_on_online.take() {
                        context.effects.push(Effect::Retry { window: self.window.clone(), kind });
                    }
                }
            }
            Event::Close => return Transition(State::closed()),
            Event::ReadOk { has_rows, .. } => {
                self.has_rows = *has_rows;
                self.read_error = None;
            }
            Event::ReadFailed { error, .. } => self.read_error = Some(error.clone()),
        }
        Transition(self.rest())
    }

    #[action]
    fn enter_not_loaded(&mut self, context: &mut Context<'_>) {
        self.announce(context, LoadPhase::NotLoaded);
    }

    #[state(superstate = "open", entry_action = "enter_not_loaded")]
    fn not_loaded() -> Outcome<State> {
        Super
    }

    #[action]
    fn enter_loading(&mut self, context: &mut Context<'_>) {
        self.announce(context, LoadPhase::Loading);
    }

    #[state(superstate = "open", entry_action = "enter_loading")]
    fn loading() -> Outcome<State> {
        Super
    }

    #[action]
    fn enter_loaded(&mut self, context: &mut Context<'_>) {
        self.announce(context, LoadPhase::Loaded);
    }

    #[state(superstate = "open", entry_action = "enter_loaded")]
    fn loaded() -> Outcome<State> {
        Super
    }

    #[action]
    fn enter_empty(&mut self, context: &mut Context<'_>) {
        self.announce(context, LoadPhase::Empty);
    }

    #[state(superstate = "open", entry_action = "enter_empty")]
    fn empty() -> Outcome<State> {
        Super
    }

    #[action]
    fn enter_failed(&mut self, context: &mut Context<'_>) {
        self.announce(context, LoadPhase::Failed);
    }

    #[state(superstate = "open", entry_action = "enter_failed")]
    fn failed() -> Outcome<State> {
        Super
    }

    #[action]
    fn enter_closed(&mut self, context: &mut Context<'_>) {
        self.announce(context, LoadPhase::Closed);
    }

    #[state(entry_action = "enter_closed")]
    fn closed() -> Outcome<State> {
        Handled
    }
}

fn phase_of(state: &State) -> LoadPhase {
    match state {
        State::NotLoaded {} => LoadPhase::NotLoaded,
        State::Loading {} => LoadPhase::Loading,
        State::Loaded {} => LoadPhase::Loaded,
        State::Empty {} => LoadPhase::Empty,
        State::Failed {} => LoadPhase::Failed,
        State::Closed {} => LoadPhase::Closed,
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
    ReadOk { window: String, has_rows: bool },
    ReadFailed { window: String, error: LoadError },
}

impl Event {
    pub(crate) fn ends_flight(&self) -> bool {
        matches!(self, Event::Landed { .. } | Event::Discarded { .. } | Event::Failed { .. } | Event::Abandoned { .. } | Event::Panicked { .. })
    }

    pub(crate) fn window(&self) -> Option<&str> {
        match self {
            Event::Observe { window, .. }
            | Event::Start { window, .. }
            | Event::Landed { window, .. }
            | Event::Discarded { window, .. }
            | Event::Failed { window, .. }
            | Event::Abandoned { window, .. }
            | Event::Panicked { window, .. }
            | Event::ReadOk { window, .. }
            | Event::ReadFailed { window, .. } => Some(window),
            Event::Reset { .. } | Event::Online { .. } | Event::Close | Event::Offline => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Effect {
    Publish { window: String, state: RelationState },
    Retry { window: String, kind: LoadKind },
}

#[derive(Default)]
pub(crate) struct Machine {
    windows: BTreeMap<String, InitializedStateMachine<WindowFacts>>,
    closed: bool,
    all_epoch: u64,
    model_epochs: BTreeMap<String, u64>,
    pub(crate) orphan_ends: u32,
    pub(crate) after_close: u32,
}

impl Machine {
    pub(crate) fn view(&self, window: &str) -> Option<RelationState> {
        self.windows.get(window).map(|machine| machine.view(phase_of(machine.state())))
    }

    pub(crate) fn forget(&mut self, keep: impl Fn(&str) -> bool) {
        self.windows.retain(|window, machine| !machine.flights.is_empty() || keep(window));
    }

    pub(crate) fn forget_window(&mut self, window: &str) -> bool {
        if self.windows.get(window).is_some_and(|machine| !machine.flights.is_empty()) { return false; }
        self.windows.remove(window);
        true
    }

    #[cfg(test)]
    pub(crate) fn windows_len(&self) -> usize {
        self.windows.len()
    }

    #[cfg(test)]
    pub(crate) fn flights(&self, window: &str) -> usize {
        self.windows.get(window).map_or(0, |machine| machine.flights.len())
    }

    fn dispatch(&mut self, event: &Event, effects: &mut Vec<Effect>) {
        if self.closed {
            self.after_close += 1;
            return;
        }
        if let Event::Reset { epoch, models } = event {
            match models {
                Some(models) => {
                    for model in models {
                        let known = self.model_epochs.entry(model.clone()).or_insert(0);
                        *known = (*known).max(*epoch);
                    }
                }
                None => self.all_epoch = self.all_epoch.max(*epoch),
            }
        }
        let mut context = Context { epochs: Epochs { all: self.all_epoch, by_model: Some(&self.model_epochs) }, effects, orphan_ends: 0, quiet: false };
        if let Event::Observe { window, model, .. } | Event::Start { window, model, .. } = event {
            self.windows.entry(window.clone()).or_insert_with(|| {
                context.quiet = true;
                let machine = WindowFacts::new(window, model).uninitialized_state_machine().init_with_context(&mut context);
                context.quiet = false;
                machine
            });
        }
        match event.window() {
            None => {
                for machine in self.windows.values_mut() {
                    machine.handle_with_context(event, &mut context);
                }
            }
            Some(window) => match self.windows.get_mut(window) {
                Some(machine) => machine.handle_with_context(event, &mut context),
                None => context.orphan_ends += u32::from(event.ends_flight()),
            },
        }
        self.orphan_ends += context.orphan_ends;
        if matches!(event, Event::Close) {
            self.closed = true;
        }
    }
}

pub(crate) fn step(machine: &mut Machine, event: Event) -> Vec<Effect> {
    let mut effects = Vec::new();
    machine.dispatch(&event, &mut effects);
    effects
}
