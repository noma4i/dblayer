//! Owner `Windows`: a dictionary of `Window` machines by key (`specs/machines/window.md`, one machine per key, `statig`).
//! A table row applies to the machine of the key an input names. Inputs that name a request (`Transport`, `Committed`, `Rejected`, `RetryDue`)
//! go to the machine that owns the request. `Online`, `EpochStarted`, `EpochEnded`, `ModelsReset`, `Closing` apply to every machine.
//! Request ids are unique in the owner. The retry timer is an effect for the executor (`ScheduleRetry`): it adds the backoff and the clock,
//! then posts `RetryDue`.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};

use statig::prelude::*;

use super::contract::{Committed, Drained, EpochEnded, EpochStarted, ErrorClass, ModelsReset, Online, RejectReason, Rejected, RequestId, Transport, RequestError, TransportOutcome};
use super::deliveries::{LoadState, Phase};
use crate::runtime::{Cause, Effect, Effects, Epoch, StateOwner, Time};

/// `(model, relation, params)` or `(model, node, id)` as one name; `model` is the model of the window.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct WindowKey {
    pub model: String,
    pub name: String,
}

/// An id of a caller or a stream that waits for the window.
pub type InterestId = u32;

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
    Commit { request: RequestId, basis: u64, page: AnswerPage },
    ScheduleRetry { request: RequestId, attempt: u32 },
    Announce(LoadState),
    Outcome { id: InterestId, outcome: WindowOutcome },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WindowsEffect {
    Window { key: WindowKey, effect: WindowEffect },
    Drained(Drained),
}

#[derive(Clone, Debug)]
pub enum WindowsInput {
    InterestPlus { key: WindowKey, id: InterestId, force: bool, stream: bool },
    InterestMinus { key: WindowKey, id: InterestId },
    LoadMore { key: WindowKey, id: InterestId },
    Transport(Transport<AnswerPage>),
    Committed(Committed),
    Rejected(Rejected),
    Stale { key: WindowKey, revision: i64 },
    RetryDue { request: RequestId },
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
    InterestPlus { id: InterestId, force: bool, stream: bool },
    InterestMinus { id: InterestId },
    LoadMore { id: InterestId },
    Response { request: RequestId, page: AnswerPage },
    Failure { request: RequestId, error: RequestError },
    Dropped,
    Committed { request: RequestId, revision: i64 },
    Rejected { request: RequestId, reason: RejectReason },
    Stale { revision: i64 },
    RetryDue { request: RequestId },
    Online(bool),
    EpochStarted,
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
    session: bool,
    generation: u64,
    known_revision: i64,
    interest: BTreeSet<InterestId>,
    streams: BTreeSet<InterestId>,
    cursor: Option<String>,
    has_rows: bool,
    phase: Phase,
    error: Option<String>,
    loaded: Option<i64>,
    fx: RefCell<Vec<WindowEffect>>,
}

impl Facts {
    fn env(&mut self, event: &Ev) {
        match event {
            Ev::Online(online) => self.online = *online,
            Ev::EpochStarted => self.session = true,
            Ev::ModelsReset { generation, models } if models.contains(&self.model) => self.generation = *generation,
            Ev::Stale { revision } => self.known_revision = self.known_revision.max(*revision),
            _ => {}
        }
    }

    fn push_fx(&self, effect: WindowEffect) { self.fx.borrow_mut().push(effect); }

    fn can_run(&self) -> bool { self.online && self.session }

    fn join(&mut self, id: InterestId, stream: bool) {
        self.interest.insert(id);
        if stream {
            self.streams.insert(id);
        }
    }

    fn leave(&mut self, id: InterestId) {
        self.interest.remove(&id);
        self.streams.remove(&id);
    }

    fn load_state(&self) -> LoadState { LoadState { phase: self.phase, error: self.error.clone(), loaded: self.loaded } }

    fn announce(&mut self) { self.push_fx(WindowEffect::Announce(self.load_state())); }

    fn answer_one(&mut self, id: InterestId, outcome: WindowOutcome) {
        self.push_fx(WindowEffect::Outcome { id, outcome });
        if !self.streams.contains(&id) {
            self.interest.remove(&id);
        }
    }

    fn answer(&mut self, outcome: &WindowOutcome) {
        let ids: Vec<InterestId> = self.interest.iter().copied().collect();
        for id in ids {
            self.answer_one(id, outcome.clone());
        }
    }

    fn failed_phase(&self) -> Phase { if self.has_rows { Phase::Loaded } else { Phase::Failed } }

    fn begin(&mut self, ctx: &mut Ctx<'_>, page: u32) -> RequestId {
        let request = ctx.fresh();
        self.phase = Phase::Loading;
        self.push_fx(WindowEffect::Request { request, page, cursor: self.page_cursor(page), basis: self.generation });
        request
    }

    fn page_cursor(&self, page: u32) -> Option<String> { if page > 1 { self.cursor.clone() } else { None } }

    fn finish(&mut self, answer: bool, cancel: Option<RequestId>) -> Outcome<WinState> {
        if answer {
            self.answer(&WindowOutcome::Interrupted);
        }
        if let Some(request) = cancel {
            self.push_fx(WindowEffect::Cancel { request });
        }
        Transition(WinState::ended())
    }

    fn reload(&mut self, ctx: &mut Ctx<'_>) -> Outcome<WinState> {
        let request = self.begin(ctx, 1);
        Transition(WinState::loading(request, 1, self.known_revision, self.generation, false, 0, false))
    }
}

#[state_machine(initial = "WinState::idle(false, None, None)", state(name = "WinState"))]
impl Facts {
    #[state]
    fn idle(&mut self, context: &mut Ctx<'_>, fresh: &mut bool, stale_since: &mut Option<i64>, last_error: &mut Option<LastError>, event: &Ev) -> Outcome<WinState> {
        self.env(event);
        match event {
            Ev::InterestPlus { id, force, stream } => {
                self.join(*id, *stream);
                if *fresh && !*force && stale_since.is_none() {
                    self.phase = Phase::Loaded;
                    let revision = self.loaded.unwrap_or(0);
                    self.answer_one(*id, WindowOutcome::Loaded { revision });
                    self.announce();
                    Handled
                } else if self.can_run() {
                    let request = self.begin(context, 1);
                    self.announce();
                    Transition(WinState::loading(request, 1, self.known_revision, self.generation, *force, 0, false))
                } else {
                    self.announce();
                    Transition(WinState::waiting(*force))
                }
            }
            Ev::InterestMinus { id } => {
                self.leave(*id);
                Handled
            }
            Ev::LoadMore { id } => {
                if self.cursor.is_some() && self.can_run() {
                    self.join(*id, false);
                    let request = self.begin(context, 2);
                    Transition(WinState::loading(request, 2, self.known_revision, self.generation, false, 0, false))
                } else {
                    let revision = self.loaded.unwrap_or(0);
                    self.answer_one(*id, WindowOutcome::Loaded { revision });
                    Handled
                }
            }
            Ev::Stale { revision } => {
                if !self.interest.is_empty() && self.online {
                    self.reload(context)
                } else {
                    *fresh = false;
                    *stale_since = Some(*revision);
                    Handled
                }
            }
            Ev::Online(true) if *last_error == Some(LastError::Network) && !self.interest.is_empty() => self.reload(context),
            Ev::ModelsReset { models, .. } if models.contains(&self.model) => {
                *fresh = false;
                if self.interest.is_empty() { Handled } else { self.reload(context) }
            }
            Ev::EpochEnded | Ev::Closing => self.finish(false, None),
            _ => Handled,
        }
    }

    #[state]
    fn loading(&mut self, context: &mut Ctx<'_>, request: &mut RequestId, page: &mut u32, issued: &mut i64, basis: &mut u64, force: &mut bool, attempts: &mut u32, again: &mut bool, event: &Ev) -> Outcome<WinState> {
        self.env(event);
        match event {
            Ev::InterestPlus { id, force: more, stream } => {
                self.join(*id, *stream);
                *force |= *more;
                Handled
            }
            Ev::InterestMinus { id } => {
                self.leave(*id);
                if self.interest.is_empty() {
                    self.phase = if self.has_rows { Phase::Loaded } else { Phase::Idle };
                    self.push_fx(WindowEffect::Cancel { request: *request });
                    Transition(WinState::idle(false, None, None))
                } else {
                    Handled
                }
            }
            Ev::Response { request: got, page: answer } if got == request => {
                self.push_fx(WindowEffect::Commit { request: *request, basis: *basis, page: answer.clone() });
                Transition(WinState::landing(*request, *again, *issued, answer.cursor.clone(), !answer.rows.is_empty()))
            }
            Ev::Failure { request: got, error } if got == request => {
                if error.temporary() && self.online && *attempts + 1 < attempt_limit(error.class) {
                    *attempts += 1;
                    self.push_fx(WindowEffect::ScheduleRetry { request: *request, attempt: *attempts });
                    return Handled;
                }
                let last = match error.class {
                    ErrorClass::Network => LastError::Network,
                    ErrorClass::Server => LastError::Server,
                    ErrorClass::Permanent => LastError::Permanent,
                };
                self.phase = self.failed_phase();
                self.error = Some(error.message.clone());
                self.answer(&WindowOutcome::Failed { error: error.message.clone() });
                self.announce();
                Transition(WinState::idle(false, None, Some(last)))
            }
            Ev::RetryDue { request: got } if got == request => {
                self.push_fx(WindowEffect::Request { request: *request, page: *page, cursor: self.page_cursor(*page), basis: *basis });
                Handled
            }
            Ev::Online(false) => {
                self.push_fx(WindowEffect::Cancel { request: *request });
                Transition(WinState::waiting(*force))
            }
            Ev::Stale { revision } => {
                *again |= *revision > *issued;
                Handled
            }
            Ev::ModelsReset { models, .. } if models.contains(&self.model) => {
                let fresh = context.fresh();
                self.push_fx(WindowEffect::Cancel { request: *request });
                self.push_fx(WindowEffect::Request { request: fresh, page: *page, cursor: self.page_cursor(*page), basis: self.generation });
                Transition(WinState::loading(fresh, *page, self.known_revision, self.generation, *force, 0, *again))
            }
            Ev::EpochEnded | Ev::Closing => self.finish(true, Some(*request)),
            _ => Handled,
        }
    }

    #[state]
    fn landing(&mut self, context: &mut Ctx<'_>, request: &mut RequestId, again: &mut bool, issued: &mut i64, cursor: &mut Option<String>, rows: &mut bool, event: &Ev) -> Outcome<WinState> {
        self.env(event);
        match event {
            Ev::Committed { request: got, revision } if got == request => {
                self.known_revision = self.known_revision.max(*revision);
                self.has_rows |= *rows;
                self.cursor = cursor.clone();
                self.loaded = Some(*revision);
                self.error = None;
                self.answer(&WindowOutcome::Loaded { revision: *revision });
                if *again {
                    let next = self.begin(context, 1);
                    self.announce();
                    Transition(WinState::loading(next, 1, self.known_revision, self.generation, false, 0, false))
                } else {
                    self.phase = Phase::Loaded;
                    self.announce();
                    Transition(WinState::idle(true, None, None))
                }
            }
            Ev::Rejected { request: got, reason } if got == request => match reason {
                RejectReason::StaleGeneration { .. } => {
                    let next = self.begin(context, 1);
                    Transition(WinState::loading(next, 1, self.known_revision, self.generation, false, 0, false))
                }
                RejectReason::NoSession | RejectReason::Closed => {
                    self.answer(&WindowOutcome::Interrupted);
                    Transition(WinState::ended())
                }
                RejectReason::Storage(message) => {
                    self.phase = self.failed_phase();
                    self.error = Some(message.clone());
                    self.answer(&WindowOutcome::Failed { error: message.clone() });
                    self.announce();
                    Transition(WinState::idle(false, None, Some(LastError::Storage)))
                }
            },
            Ev::InterestPlus { id, stream, .. } => {
                self.join(*id, *stream);
                Handled
            }
            Ev::Stale { revision } => {
                *again |= *revision > *issued;
                Handled
            }
            Ev::EpochEnded | Ev::Closing => self.finish(true, None),
            _ => Handled,
        }
    }

    #[state]
    fn waiting(&mut self, context: &mut Ctx<'_>, force: &mut bool, event: &Ev) -> Outcome<WinState> {
        self.env(event);
        match event {
            Ev::Online(true) | Ev::EpochStarted if self.can_run() => {
                let request = self.begin(context, 1);
                Transition(WinState::loading(request, 1, self.known_revision, self.generation, *force, 0, false))
            }
            Ev::InterestPlus { id, force: more, stream } => {
                self.join(*id, *stream);
                *force |= *more;
                Handled
            }
            Ev::InterestMinus { id } => {
                self.leave(*id);
                if self.interest.is_empty() { Transition(WinState::idle(false, None, None)) } else { Handled }
            }
            Ev::EpochEnded | Ev::Closing => self.finish(true, None),
            _ => Handled,
        }
    }

    #[state]
    fn ended(&mut self, event: &Ev) -> Outcome<WinState> {
        if let Ev::InterestPlus { id, .. } = event {
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
    fn new(model: String, epoch: Epoch, online: bool, session: bool, generation: u64, known_revision: i64, next_request: &mut RequestId) -> Self {
        let facts = Facts {
            model,
            epoch,
            online,
            session,
            generation,
            known_revision,
            interest: BTreeSet::new(),
            streams: BTreeSet::new(),
            cursor: None,
            has_rows: false,
            phase: Phase::Idle,
            error: None,
            loaded: None,
            fx: RefCell::new(Vec::new()),
        };
        let mut ctx = Ctx { next_request };
        Self { machine: facts.uninitialized_state_machine().init_with_context(&mut ctx) }
    }

    fn handle(&mut self, event: Ev, next_request: &mut RequestId) -> Vec<WindowEffect> {
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

/// The state of the owner: the environment new machines start from and the machines.
pub struct WindowsState {
    epoch: Epoch,
    online: bool,
    session: bool,
    generations: BTreeMap<String, u64>,
    next_request: RequestId,
    windows: BTreeMap<WindowKey, Window>,
}

impl WindowsState {
    pub fn new(epoch: Epoch, online: bool, session: bool) -> Self { Self { epoch, online, session, generations: BTreeMap::new(), next_request: 1, windows: BTreeMap::new() } }

    pub fn stage(&self, key: &WindowKey) -> Option<WindowStage> { self.windows.get(key).map(Window::stage) }

    pub fn interest(&self, key: &WindowKey) -> Vec<InterestId> { self.windows.get(key).map(|window| window.machine.interest.iter().copied().collect()).unwrap_or_default() }

    pub fn load_state(&self, key: &WindowKey) -> Option<LoadState> { self.windows.get(key).map(|window| window.machine.load_state()) }

    pub fn windows(&self) -> usize { self.windows.len() }
}

pub struct Windows;

impl Windows {
    fn on(state: &mut WindowsState, key: &WindowKey, event: Ev, out: &mut Vec<WindowsEffect>) {
        let Some(window) = state.windows.get_mut(key) else { return };
        for effect in window.handle(event, &mut state.next_request) {
            out.push(WindowsEffect::Window { key: key.clone(), effect });
        }
    }

    fn by_request(state: &mut WindowsState, request: RequestId, event: Ev, out: &mut Vec<WindowsEffect>) {
        let Some(key) = state.windows.iter().find(|(_, window)| window.owns(request)).map(|(key, _)| key.clone()) else { return };
        Self::on(state, &key, event, out);
    }

    fn every(state: &mut WindowsState, event: &Ev, out: &mut Vec<WindowsEffect>) {
        let keys: Vec<WindowKey> = state.windows.keys().cloned().collect();
        for key in keys {
            Self::on(state, &key, event.clone(), out);
        }
    }
}

impl StateOwner for Windows {
    const EPOCHED: bool = true;

    type State = WindowsState;
    type Input = WindowsInput;
    type Effect = WindowsEffect;

    fn step(state: &mut WindowsState, input: WindowsInput) -> Effects<Self> {
        let mut out = Vec::new();
        match input {
            WindowsInput::InterestPlus { key, id, force, stream } => {
                if !state.windows.contains_key(&key) {
                    let generation = state.generations.get(&key.model).copied().unwrap_or(0);
                    let window = Window::new(key.model.clone(), state.epoch, state.online, state.session, generation, 0, &mut state.next_request);
                    state.windows.insert(key.clone(), window);
                }
                Self::on(state, &key, Ev::InterestPlus { id, force, stream }, &mut out);
            }
            WindowsInput::InterestMinus { key, id } => Self::on(state, &key, Ev::InterestMinus { id }, &mut out),
            WindowsInput::LoadMore { key, id } => Self::on(state, &key, Ev::LoadMore { id }, &mut out),
            WindowsInput::Stale { key, revision } => Self::on(state, &key, Ev::Stale { revision }, &mut out),
            WindowsInput::Transport(transport) => {
                let request = transport.req_id;
                let event = match transport.outcome {
                    TransportOutcome::Response(page) => Ev::Response { request, page },
                    TransportOutcome::Error(error) => Ev::Failure { request, error },
                    TransportOutcome::Dropped => Ev::Dropped,
                };
                Self::by_request(state, request, event, &mut out);
            }
            WindowsInput::Committed(committed) => Self::by_request(state, committed.batch, Ev::Committed { request: committed.batch, revision: committed.revision }, &mut out),
            WindowsInput::Rejected(rejected) => Self::by_request(state, rejected.batch, Ev::Rejected { request: rejected.batch, reason: rejected.reason }, &mut out),
            WindowsInput::RetryDue { request } => Self::by_request(state, request, Ev::RetryDue { request }, &mut out),
            WindowsInput::Online(Online(online)) => {
                state.online = online;
                Self::every(state, &Ev::Online(online), &mut out);
            }
            WindowsInput::EpochStarted(started) => {
                state.session = true;
                state.epoch = started.epoch;
                Self::every(state, &Ev::EpochStarted, &mut out);
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
                out.push(WindowsEffect::Drained(Drained { owner: "Windows" }));
            }
            WindowsInput::Tick(_) => {}
        }
        out.into_iter().map(Effect::Run).collect()
    }

    fn tick(now: Time) -> WindowsInput { WindowsInput::Tick(now) }

    fn epoch_ended(epoch: Epoch, cause: Cause) -> WindowsInput { WindowsInput::EpochEnded(EpochEnded { epoch, cause }) }

    fn closing() -> WindowsInput { WindowsInput::Closing }
}
