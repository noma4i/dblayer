//! Owner `Windows`: a dictionary of `Window` machines by key (`specs/machines/window.md`, one machine per key, `statig`).
//! A table row applies to the machine of the key an input names. Inputs that name a request (`Transport`, `Committed`, `Rejected`, `RetryDue`)
//! go to the machine that owns the request. `Online`, `EpochStarted`, `EpochEnded`, `ModelsReset`, `Closing` apply to every machine,
//! `Stale` to every machine of the model. Request ids are unique in the owner. The retry timer is `Effect::Schedule` of the owner
//! (`after` = the backoff). Inputs for other owners (`Commit`, `WindowState`) are `Effect::Send` to the addresses of the state.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use statig::prelude::*;

use crate::owners::read::deliveries::{DeliveryInput, Deliveries};
use crate::owners::store::{StoreBatch, StoreInput, StoreWriter};
use crate::pacing::RetryPolicy;
use crate::owners::contract::{BatchAuthor, Committed, Drained, EpochEnded, EpochStarted, ErrorClass, ModelsReset, Online, RejectReason, Rejected, RequestError, RequestId, Stale, Transport, TransportOutcome, WindowPhase as Phase, WindowState};
use crate::runtime::{Address, Cause, Effect, Effects, Epoch, Outbound, ScheduleId, StateOwner, Time};

/// `(model, relation, params)`, `(model, node, id)` or `(model, node, ids)` as one name; `model` is the model of the window.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct WindowKey {
    pub model: String,
    pub name: String,
}

impl WindowKey {
    const NODES: &'static str = "nodes:";

    /// The key `(model, node, ids)`: the order of `ids` is the order of the call.
    pub fn nodes(model: &str, ids: &[&str]) -> Self { Self { model: model.into(), name: format!("{}{}", Self::NODES, ids.join(",")) } }

    fn is_empty_nodes(&self) -> bool { self.name == Self::NODES }
}

/// An id of a caller or a stream that waits for the window. `PRELOAD` is the interest of a key declared `preload`.
pub type InterestId = u64;

pub const PRELOAD: InterestId = InterestId::MAX;

/// The answer of the server for one page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AnswerPage {
    pub page: u32,
    pub rows: Vec<String>,
    pub cursor: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WindowOutcome {
    Loaded { revision: i64 },
    Failed { error: String },
    Interrupted,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WindowEffect {
    Request { request: RequestId, page: u32, cursor: Option<String>, basis: u64 },
    Cancel { request: RequestId },
    Outcome { id: InterestId, outcome: WindowOutcome },
}

/// What a machine gives out: an effect of the runner, or an input for another owner or a timer that the owner `Windows` turns into `Effect`.
enum Emit {
    Run(WindowEffect),
    Commit { request: RequestId, basis: u64, page: AnswerPage },
    Retry { request: RequestId, attempt: u32, class: ErrorClass },
    Announce { state: WindowState, streams: Vec<InterestId> },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WindowsEffect {
    Window { key: WindowKey, effect: WindowEffect },
    Drained(Drained),
}

#[derive(Clone, Debug)]
pub enum WindowsInput {
    /// `all` marks `begin_load_all`: its outcome comes after the last page. `stream` keeps the id in the interest after the outcome.
    /// `store_revision` is the Published revision of the Store at the entry: the outcome of a `nodes` key without ids.
    InterestPlus { key: WindowKey, id: InterestId, force: bool, all: bool, stream: bool, store_revision: i64 },
    InterestMinus { key: WindowKey, id: InterestId },
    LoadMore { key: WindowKey, id: InterestId },
    Transport(Transport<AnswerPage>),
    Committed(Committed),
    Rejected(Rejected),
    Stale(Stale),
    RetryDue { request: RequestId },
    /// A model event of the Feed: the windows of the model declared `reread` load again.
    Reread { model: String },
    Online(Online),
    EpochStarted(EpochStarted),
    EpochEnded(EpochEnded),
    ModelsReset(ModelsReset),
    Closing,
    Tick(Time),
}

/// An input of one machine.
#[derive(Clone, Debug)]
enum Ev {
    InterestPlus { id: InterestId, force: bool, all: bool, stream: bool },
    InterestMinus { id: InterestId },
    LoadMore { id: InterestId },
    Response { request: RequestId, page: AnswerPage },
    /// `TransportError` and `Dropped`: the cells are the same, a dropped request is a temporary network error.
    Failure { request: RequestId, error: RequestError },
    Committed { request: RequestId, revision: i64 },
    Rejected { request: RequestId, reason: RejectReason },
    Stale,
    RetryDue { request: RequestId },
    Online(bool),
    ModelsReset { generation: u64, models: Vec<String> },
    EpochEnded,
    Closing,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LastError {
    Network,
    Server,
    Permanent,
    Storage,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WindowStage {
    Idle,
    Loading,
    Landing,
    Waiting,
    Ended,
}

/// Attempts of a window per error class (`design.md` 1a).
fn attempt_limit(class: ErrorClass) -> u32 {
    match class {
        ErrorClass::Network => 3,
        ErrorClass::Server => 2,
        ErrorClass::Permanent => 0,
    }
}

struct Ctx<'a> {
    next_request: &'a mut RequestId,
}

impl Ctx<'_> {
    fn fresh(&mut self) -> RequestId {
        let request = *self.next_request;
        *self.next_request += 1;
        request
    }
}

/// The values every state of the machine shares.
struct Facts {
    model: String,
    epoch: Epoch,
    online: bool,
    generation: u64,
    interest: BTreeSet<InterestId>,
    streams: BTreeSet<InterestId>,
    all: BTreeSet<InterestId>,
    cursor: Option<String>,
    /// The page the last landing wrote.
    page: u32,
    has_rows: bool,
    phase: Phase,
    error: Option<String>,
    /// The revision of the last `Committed` of a landing of this machine, 0 before the first.
    landed_revision: i64,
    fx: RefCell<Vec<Emit>>,
}

impl Facts {
    fn env(&mut self, event: &Ev) {
        match event {
            Ev::Online(online) => self.online = *online,
            Ev::ModelsReset { generation, models } if models.contains(&self.model) => self.generation = *generation,
            _ => {}
        }
    }

    fn push_fx(&self, effect: WindowEffect) { self.fx.borrow_mut().push(Emit::Run(effect)); }

    fn emit(&self, emit: Emit) { self.fx.borrow_mut().push(emit); }

    fn join(&mut self, id: InterestId, all: bool, stream: bool) {
        self.interest.insert(id);
        if stream {
            self.streams.insert(id);
        }
        if all {
            self.all.insert(id);
        }
    }

    fn leave(&mut self, id: InterestId) {
        self.interest.remove(&id);
        self.streams.remove(&id);
        self.all.remove(&id);
    }

    fn load_state(&self) -> WindowState { WindowState { phase: self.phase, error: self.error.clone(), loaded: (self.landed_revision != 0).then_some(self.landed_revision) } }

    fn announce(&mut self) {
        let streams = self.streams.iter().copied().filter(|id| *id != PRELOAD).collect();
        self.emit(Emit::Announce { state: self.load_state(), streams });
    }

    fn answer_one(&mut self, id: InterestId, outcome: WindowOutcome) {
        self.push_fx(WindowEffect::Outcome { id, outcome });
        if !self.streams.contains(&id) {
            self.leave(id);
        }
    }

    fn answer_ids(&mut self, ids: Vec<InterestId>, outcome: &WindowOutcome) {
        for id in ids {
            self.answer_one(id, outcome.clone());
        }
    }

    fn answer(&mut self, outcome: &WindowOutcome) {
        let ids: Vec<InterestId> = self.interest.iter().copied().collect();
        self.answer_ids(ids, outcome);
    }

    fn failed_phase(&self) -> Phase { if self.has_rows { Phase::Loaded } else { Phase::Failed } }

    fn begin(&mut self, ctx: &mut Ctx<'_>, page: u32) -> RequestId {
        let request = ctx.fresh();
        self.phase = Phase::Loading;
        self.push_fx(WindowEffect::Request { request, page, cursor: self.page_cursor(page), basis: self.generation });
        request
    }

    fn page_cursor(&self, page: u32) -> Option<String> { if page > 1 { self.cursor.clone() } else { None } }

    /// Idle, Loading or Landing needs a load: a request when online, `Waiting` otherwise.
    fn load(&mut self, ctx: &mut Ctx<'_>, page: u32, force: bool, announce: bool) -> Outcome<WinState> {
        if self.online {
            let request = self.begin(ctx, page);
            if announce {
                self.announce();
            }
            Transition(WinState::loading(request, page, self.generation, force, 0, false, false))
        } else {
            if announce {
                self.announce();
            }
            Transition(WinState::waiting(force, page))
        }
    }

    fn finish(&mut self, answer: bool, cancel: Option<RequestId>) -> Outcome<WinState> {
        if answer {
            self.answer(&WindowOutcome::Interrupted);
        }
        if let Some(request) = cancel {
            self.push_fx(WindowEffect::Cancel { request });
        }
        Transition(WinState::ended())
    }

    fn fail(&mut self, message: &str, last: LastError) -> Outcome<WinState> {
        self.phase = self.failed_phase();
        self.error = Some(message.to_string());
        self.answer(&WindowOutcome::Failed { error: message.to_string() });
        self.announce();
        Transition(WinState::idle(false, Some(last)))
    }
}

#[state_machine(initial = "WinState::idle(false, None)", state(name = "WinState"))]
impl Facts {
    #[state]
    fn idle(&mut self, context: &mut Ctx<'_>, fresh: &mut bool, last_error: &mut Option<LastError>, event: &Ev) -> Outcome<WinState> {
        self.env(event);
        match event {
            Ev::InterestPlus { id, force, all, stream } => {
                self.join(*id, *all, *stream);
                if *fresh && !*force && !(*all && self.cursor.is_some()) {
                    self.phase = Phase::Loaded;
                    let revision = self.landed_revision;
                    self.answer_one(*id, WindowOutcome::Loaded { revision });
                    self.announce();
                    Handled
                } else if *fresh && !*force {
                    self.load(context, self.page + 1, false, true)
                } else {
                    self.load(context, 1, *force, true)
                }
            }
            Ev::InterestMinus { id } => {
                self.leave(*id);
                Handled
            }
            Ev::LoadMore { id } => {
                if self.cursor.is_some() {
                    self.join(*id, false, false);
                    self.load(context, self.page + 1, false, true)
                } else {
                    let revision = self.landed_revision;
                    self.answer_one(*id, WindowOutcome::Loaded { revision });
                    Handled
                }
            }
            Ev::Stale if self.interest.is_empty() => {
                *fresh = false;
                Handled
            }
            Ev::Stale => self.load(context, 1, false, self.online),
            Ev::Online(true) if *last_error == Some(LastError::Network) && !self.interest.is_empty() => self.load(context, 1, false, true),
            Ev::ModelsReset { models, .. } if models.contains(&self.model) => {
                *fresh = false;
                if self.interest.is_empty() { Handled } else { self.load(context, 1, false, self.online) }
            }
            Ev::EpochEnded | Ev::Closing => self.finish(false, None),
            _ => Handled,
        }
    }

    #[state]
    fn loading(&mut self, context: &mut Ctx<'_>, request: &mut RequestId, page: &mut u32, basis: &mut u64, force: &mut bool, attempts: &mut u32, again: &mut bool, more: &mut bool, event: &Ev) -> Outcome<WinState> {
        self.env(event);
        match event {
            Ev::InterestPlus { id, force: wider, all, stream } => {
                self.join(*id, *all, *stream);
                *force |= *wider;
                Handled
            }
            Ev::InterestMinus { id } => {
                self.leave(*id);
                if self.interest.is_empty() {
                    self.phase = if self.has_rows { Phase::Loaded } else { Phase::Idle };
                    self.push_fx(WindowEffect::Cancel { request: *request });
                    Transition(WinState::idle(false, None))
                } else {
                    Handled
                }
            }
            Ev::LoadMore { id } => {
                self.join(*id, false, false);
                if *page == 1 {
                    *again = true;
                    *more = true;
                }
                Handled
            }
            Ev::Response { request: got, page: answer } if got == request => {
                self.emit(Emit::Commit { request: *request, basis: *basis, page: answer.clone() });
                Transition(WinState::landing(*request, *page, *again, *more, answer.cursor.clone(), !answer.rows.is_empty()))
            }
            Ev::Failure { request: got, error } if got == request => {
                if error.temporary() && *attempts + 1 < attempt_limit(error.class) {
                    *attempts += 1;
                    self.emit(Emit::Retry { request: *request, attempt: *attempts, class: error.class });
                    return Handled;
                }
                let last = match error.class {
                    ErrorClass::Network => LastError::Network,
                    ErrorClass::Server => LastError::Server,
                    ErrorClass::Permanent => LastError::Permanent,
                };
                self.fail(&error.message, last)
            }
            Ev::RetryDue { request: got } if got == request => {
                self.push_fx(WindowEffect::Request { request: *request, page: *page, cursor: self.page_cursor(*page), basis: *basis });
                Handled
            }
            Ev::Online(false) => {
                self.push_fx(WindowEffect::Cancel { request: *request });
                Transition(WinState::waiting(*force, *page))
            }
            Ev::Stale => {
                *again = true;
                Handled
            }
            Ev::ModelsReset { models, .. } if models.contains(&self.model) => {
                let fresh = context.fresh();
                self.push_fx(WindowEffect::Cancel { request: *request });
                self.push_fx(WindowEffect::Request { request: fresh, page: *page, cursor: self.page_cursor(*page), basis: self.generation });
                Transition(WinState::loading(fresh, *page, self.generation, *force, 0, *again, *more))
            }
            Ev::EpochEnded | Ev::Closing => self.finish(true, Some(*request)),
            _ => Handled,
        }
    }

    #[state]
    fn landing(&mut self, context: &mut Ctx<'_>, request: &mut RequestId, page: &mut u32, again: &mut bool, more: &mut bool, cursor: &mut Option<String>, rows: &mut bool, event: &Ev) -> Outcome<WinState> {
        self.env(event);
        match event {
            Ev::Committed { request: got, revision } if got == request => {
                self.has_rows |= *rows;
                self.cursor = cursor.clone();
                self.page = *page;
                self.landed_revision = *revision;
                self.error = None;
                let watched = !self.interest.is_empty();
                let waiting_all = self.interest.iter().any(|id| self.all.contains(id)) && self.cursor.is_some();
                let outcome = WindowOutcome::Loaded { revision: *revision };
                let calls: Vec<InterestId> = self.interest.iter().copied().filter(|id| !(waiting_all && self.all.contains(id))).collect();
                self.answer_ids(calls, &outcome);
                if waiting_all {
                    let next = self.page + 1;
                    return if self.online {
                        let request = self.begin(context, next);
                        self.announce();
                        Transition(WinState::loading(request, next, self.generation, false, 0, *again, *more))
                    } else {
                        self.phase = Phase::Loaded;
                        self.announce();
                        Transition(WinState::waiting(false, next))
                    };
                }
                if !*again {
                    self.phase = Phase::Loaded;
                    self.announce();
                    return Transition(WinState::idle(true, None));
                }
                if !watched {
                    self.phase = Phase::Loaded;
                    self.announce();
                    return Transition(WinState::idle(false, None));
                }
                let next = if *more && self.cursor.is_some() { self.page + 1 } else { 1 };
                if self.online {
                    let request = self.begin(context, next);
                    self.announce();
                    Transition(WinState::loading(request, next, self.generation, false, 0, false, false))
                } else {
                    self.phase = Phase::Loaded;
                    self.announce();
                    Transition(WinState::waiting(false, next))
                }
            }
            Ev::Rejected { request: got, reason } if got == request => match reason {
                RejectReason::StaleGeneration { .. } => {
                    if self.interest.is_empty() {
                        return Transition(WinState::idle(false, None));
                    }
                    self.load(context, 1, false, false)
                }
                RejectReason::NoSession | RejectReason::Closed => {
                    self.answer(&WindowOutcome::Interrupted);
                    Transition(WinState::ended())
                }
                RejectReason::Storage(message) => self.fail(message, LastError::Storage),
            },
            Ev::InterestPlus { id, all, stream, .. } => {
                self.join(*id, *all, *stream);
                Handled
            }
            Ev::LoadMore { id } => {
                self.join(*id, false, false);
                *again = true;
                *more = true;
                Handled
            }
            Ev::Stale => {
                *again = true;
                Handled
            }
            Ev::EpochEnded | Ev::Closing => self.finish(true, None),
            _ => Handled,
        }
    }

    #[state]
    fn waiting(&mut self, context: &mut Ctx<'_>, force: &mut bool, page: &mut u32, event: &Ev) -> Outcome<WinState> {
        self.env(event);
        match event {
            Ev::Online(true) => self.load(context, *page, *force, true),
            Ev::InterestPlus { id, force: wider, all, stream } => {
                self.join(*id, *all, *stream);
                *force |= *wider;
                Handled
            }
            Ev::LoadMore { id } => {
                self.join(*id, false, false);
                Handled
            }
            Ev::InterestMinus { id } => {
                self.leave(*id);
                if self.interest.is_empty() { Transition(WinState::idle(false, None)) } else { Handled }
            }
            Ev::EpochEnded | Ev::Closing => self.finish(true, None),
            _ => Handled,
        }
    }

    #[state]
    fn ended(&mut self, event: &Ev) -> Outcome<WinState> {
        if let Ev::InterestPlus { id, .. } | Ev::LoadMore { id } = event {
            self.push_fx(WindowEffect::Outcome { id: *id, outcome: WindowOutcome::Interrupted });
        }
        Handled
    }
}

/// One machine of `window.md`.
struct Window {
    machine: InitializedStateMachine<Facts>,
}

impl Window {
    fn new(model: String, epoch: Epoch, online: bool, generation: u64, next_request: &mut RequestId) -> Self {
        let facts = Facts {
            model,
            epoch,
            online,
            generation,
            interest: BTreeSet::new(),
            streams: BTreeSet::new(),
            all: BTreeSet::new(),
            cursor: None,
            page: 0,
            has_rows: false,
            phase: Phase::Idle,
            error: None,
            landed_revision: 0,
            fx: RefCell::new(Vec::new()),
        };
        let mut ctx = Ctx { next_request };
        Self { machine: facts.uninitialized_state_machine().init_with_context(&mut ctx) }
    }

    fn handle(&mut self, event: Ev, next_request: &mut RequestId) -> Vec<Emit> {
        let mut ctx = Ctx { next_request };
        self.machine.handle_with_context(&event, &mut ctx);
        self.machine.fx.take()
    }

    fn stage(&self) -> WindowStage {
        match self.machine.state() {
            WinState::Idle { .. } => WindowStage::Idle,
            WinState::Loading { .. } => WindowStage::Loading,
            WinState::Landing { .. } => WindowStage::Landing,
            WinState::Waiting { .. } => WindowStage::Waiting,
            WinState::Ended {} => WindowStage::Ended,
        }
    }

    fn owns(&self, id: RequestId) -> bool {
        match self.machine.state() {
            WinState::Loading { request, .. } | WinState::Landing { request, .. } => *request == id,
            _ => false,
        }
    }
}

/// The state of the owner: the fields the machines read (`online`, `generation[m]`), the keys declared `preload` and the machines.
pub struct WindowsState {
    epoch: Epoch,
    online: bool,
    generations: BTreeMap<String, u64>,
    base_generation: u64,
    preload: Vec<WindowKey>,
    next_request: RequestId,
    windows: BTreeMap<WindowKey, Window>,
    reread: Vec<WindowKey>,
    peers: WindowsPeers,
}

/// The owners `Windows` sends inputs to.
#[derive(Clone, Copy, Debug)]
pub struct WindowsPeers {
    pub store: Address<StoreWriter>,
    pub deliveries: Address<Deliveries>,
}

impl WindowsState {
    pub fn new(epoch: Epoch, online: bool, peers: WindowsPeers) -> Self {
        Self { epoch, online, generations: BTreeMap::new(), base_generation: 0, preload: Vec::new(), next_request: 1, windows: BTreeMap::new(), reread: Vec::new(), peers }
    }

    /// Keys that load again at every model event of their model.
    pub fn with_reread(mut self, keys: Vec<WindowKey>) -> Self {
        self.reread = keys;
        self
    }

    /// Keys that `EpochStarted` loads with `Interest+(PRELOAD)`.
    pub fn with_preload(mut self, keys: Vec<WindowKey>) -> Self {
        self.preload = keys;
        self
    }

    pub fn stage(&self, key: &WindowKey) -> Option<WindowStage> { self.windows.get(key).map(Window::stage) }

    pub fn interest(&self, key: &WindowKey) -> Vec<InterestId> { self.windows.get(key).map(|window| window.machine.interest.iter().copied().collect()).unwrap_or_default() }

    pub fn load_state(&self, key: &WindowKey) -> Option<WindowState> { self.windows.get(key).map(|window| window.machine.load_state()) }

    pub fn windows(&self) -> usize { self.windows.len() }

    fn generation_of(&self, model: &str) -> u64 { self.generations.get(model).copied().unwrap_or(self.base_generation) }

    fn ensure(&mut self, key: &WindowKey) {
        if !self.windows.contains_key(key) {
            let window = Window::new(key.model.clone(), self.epoch, self.online, self.generation_of(&key.model), &mut self.next_request);
            self.windows.insert(key.clone(), window);
        }
    }
}

pub struct Windows;

impl Windows {
    fn on(state: &mut WindowsState, key: &WindowKey, event: Ev, out: &mut Effects<Self>) {
        let peers = state.peers;
        let Some(window) = state.windows.get_mut(key) else { return };
        let epoch = window.machine.epoch;
        for emit in window.handle(event, &mut state.next_request) {
            match emit {
                Emit::Run(effect) => {
                    if let WindowEffect::Cancel { request } = &effect {
                        out.push(Effect::Cancel(ScheduleId(*request)));
                    }
                    out.push(Effect::Run(WindowsEffect::Window { key: key.clone(), effect }));
                }
                Emit::Commit { request, basis, page } => {
                    let rows = page.rows.into_iter().map(|row| (key.model.clone(), row)).collect();
                    let batch = StoreBatch { author: BatchAuthor::Window, id: request, epoch, basis: BTreeMap::from([(key.model.clone(), basis)]), rows, intents: 0, credential: None, ops: Vec::new() };
                    out.push(Effect::Send(Outbound::new(peers.store, StoreInput::Commit(batch))));
                }
                Emit::Retry { request, attempt, class } => {
                    let policy = if class == ErrorClass::Network { RetryPolicy::QueryNetwork } else { RetryPolicy::QueryServer };
                    let after: Duration = policy.backoff().delay(attempt - 1);
                    out.push(Effect::Schedule { id: ScheduleId(request), after, input: WindowsInput::RetryDue { request } });
                }
                Emit::Announce { state, streams } => {
                    for stream in streams {
                        out.push(Effect::Send(Outbound::new(peers.deliveries, DeliveryInput::WindowState { stream, state: state.clone() })));
                    }
                }
            }
        }
    }

    fn by_request(state: &mut WindowsState, request: RequestId, event: Ev, out: &mut Effects<Self>) {
        let Some(key) = state.windows.iter().find(|(_, window)| window.owns(request)).map(|(key, _)| key.clone()) else { return };
        Self::on(state, &key, event, out);
    }

    fn every(state: &mut WindowsState, event: &Ev, out: &mut Effects<Self>) {
        let keys: Vec<WindowKey> = state.windows.keys().cloned().collect();
        for key in keys {
            Self::on(state, &key, event.clone(), out);
        }
    }

    fn interest_plus(state: &mut WindowsState, key: WindowKey, id: InterestId, force: bool, all: bool, stream: bool, store_revision: i64, out: &mut Effects<Self>) {
        if key.is_empty_nodes() {
            let effect = WindowEffect::Outcome { id, outcome: WindowOutcome::Loaded { revision: store_revision } };
            out.push(Effect::Run(WindowsEffect::Window { key, effect }));
            return;
        }
        state.ensure(&key);
        Self::on(state, &key, Ev::InterestPlus { id, force, all, stream }, out);
    }
}

impl StateOwner for Windows {
    const EPOCHED: bool = true;

    type State = WindowsState;
    type Input = WindowsInput;
    type Effect = WindowsEffect;

    fn step(state: &mut WindowsState, input: WindowsInput) -> Effects<Self> {
        let mut out: Effects<Self> = Vec::new();
        match input {
            WindowsInput::InterestPlus { key, id, force, all, stream, store_revision } => Self::interest_plus(state, key, id, force, all, stream, store_revision, &mut out),
            WindowsInput::InterestMinus { key, id } => Self::on(state, &key, Ev::InterestMinus { id }, &mut out),
            WindowsInput::LoadMore { key, id } => {
                state.ensure(&key);
                Self::on(state, &key, Ev::LoadMore { id }, &mut out);
            }
            WindowsInput::Stale(stale) => {
                let keys: Vec<WindowKey> = state.windows.keys().filter(|key| key.model == stale.model).cloned().collect();
                for key in keys {
                    Self::on(state, &key, Ev::Stale, &mut out);
                }
            }
            WindowsInput::Transport(transport) => {
                let request = transport.req_id;
                let event = match transport.outcome {
                    TransportOutcome::Response(page) => Ev::Response { request, page },
                    TransportOutcome::Error(error) => Ev::Failure { request, error },
                    TransportOutcome::Dropped => Ev::Failure { request, error: RequestError { message: "dropped".into(), class: ErrorClass::Network } },
                };
                Self::by_request(state, request, event, &mut out);
            }
            WindowsInput::Committed(committed) => {
                Self::by_request(state, committed.batch, Ev::Committed { request: committed.batch, revision: committed.revision }, &mut out);
            }
            WindowsInput::Rejected(rejected) => Self::by_request(state, rejected.batch, Ev::Rejected { request: rejected.batch, reason: rejected.reason }, &mut out),
            WindowsInput::RetryDue { request } => Self::by_request(state, request, Ev::RetryDue { request }, &mut out),
            WindowsInput::Reread { model } => {
                let keys: Vec<WindowKey> = state.reread.iter().filter(|key| key.model == model && state.windows.contains_key(key)).cloned().collect();
                for key in keys {
                    Self::on(state, &key, Ev::Stale, &mut out);
                }
            }
            WindowsInput::Online(Online(online)) => {
                state.online = online;
                Self::every(state, &Ev::Online(online), &mut out);
            }
            WindowsInput::EpochStarted(started) => {
                state.epoch = started.epoch;
                state.base_generation = started.generation;
                if started.identity.is_some() {
                    for key in state.preload.clone() {
                        Self::interest_plus(state, key, PRELOAD, false, false, true, 0, &mut out);
                    }
                }
            }
            WindowsInput::EpochEnded(ended) => {
                let keys: Vec<WindowKey> = state.windows.iter().filter(|(_, window)| window.machine.epoch == ended.epoch).map(|(key, _)| key.clone()).collect();
                for key in keys {
                    Self::on(state, &key, Ev::EpochEnded, &mut out);
                }
            }
            WindowsInput::ModelsReset(reset) => {
                for model in &reset.models {
                    state.generations.insert(model.clone(), reset.generation);
                }
                Self::every(state, &Ev::ModelsReset { generation: reset.generation, models: reset.models }, &mut out);
            }
            WindowsInput::Closing => {
                Self::every(state, &Ev::Closing, &mut out);
                out.push(Effect::Run(WindowsEffect::Drained(Drained { owner: "Windows" })));
            }
            WindowsInput::Tick(_) => {}
        }
        out
    }

    fn tick(now: Time) -> WindowsInput { WindowsInput::Tick(now) }

    fn epoch_ended(epoch: Epoch, cause: Cause) -> WindowsInput { WindowsInput::EpochEnded(EpochEnded { epoch, cause }) }

    fn closing() -> WindowsInput { WindowsInput::Closing }
}
