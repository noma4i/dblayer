//! Owner `Windows`: a dictionary of `Window` machines by key (`specs/machines/window.md`, one machine per key, `statig`).
//! A table row applies to the machine of the key an input names. Inputs that name a request (`Response`, `Failure`, `Dropped`, `RetryDue`,
//! `Committed`, `Rejected`) go to the machine that owns the request. `Online`, `EpochStarted`, `EpochEnded`, `ModelsReset`, `Closing` apply to
//! every machine, `Stale` to the machines of its model named in `keys`. Request ids are unique in the owner. The retry timer is `Effect::Schedule`
//! of the owner (`after` = the backoff). Inputs for other owners (`Commit`, `WindowState`) are `Effect::Send` to the addresses of the state.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use statig::prelude::*;

use crate::owners::read::deliveries::{DeliveryInput, Deliveries};
use crate::owners::store::{StoreBatch, StoreInput, StoreOp, StoreWriter};
use crate::pacing::RetryPolicy;
use crate::owners::contract::{BatchAuthor, Committed, Drained, EpochEnded, EpochStarted, ErrorClass, ModelsReset, Online, RejectReason, Rejected, RequestError, RequestId, WindowPhase as Phase, WindowState};
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
    pub fn nodes(model: &str, ids: &[&str]) -> Self { Self { model: model.into(), name: format!("{}{}", Self::NODES, serde_json::to_string(ids).unwrap_or_default()) } }

    fn is_empty_nodes(&self) -> bool { matches!(self.kind(), WindowKind::Nodes { ids } if ids.is_empty()) }

    /// The key `(model, relation, params)`: `params` in the canonical form, keys in alphabetical order.
    pub fn relation(model: &str, relation: &str, params: &serde_json::Map<String, serde_json::Value>) -> Self {
        let canonical: BTreeMap<&String, &serde_json::Value> = params.iter().collect();
        Self { model: model.into(), name: format!("{relation}|{}", serde_json::to_string(&canonical).unwrap_or_default()) }
    }

    pub fn kind(&self) -> WindowKind {
        if let Some(ids) = self.name.strip_prefix(Self::NODES) {
            return WindowKind::Nodes { ids: serde_json::from_str(ids).unwrap_or_default() };
        }
        let (relation, params) = self.name.split_once('|').unwrap_or((self.name.as_str(), "{}"));
        let params = serde_json::from_str(params).unwrap_or_default();
        WindowKind::Relation { relation: relation.to_string(), params }
    }
}

/// What a key names: a relation with params (a `node` read is a relation with the param `NODE_PARAM`) or several nodes of the root of the model.
#[derive(Clone, Debug, PartialEq)]
pub enum WindowKind {
    Relation { relation: String, params: serde_json::Map<String, serde_json::Value> },
    Nodes { ids: Vec<String> },
}

/// An id of a stream that watches the window or of a caller that waits for it. `PRELOAD` is the interest of a key declared `preload`.
pub type InterestId = u64;

pub const PRELOAD: InterestId = InterestId::MAX;

/// What a `Load` asks for: the first page, the page after the last one the caller saw, or every page.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Goal {
    First,
    Next,
    All,
}

/// A stored goal: `Next(c)` waits for the landing of the page `c`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Want {
    First,
    Next(u32),
    All,
}

/// The rows of a page as the server sent them, normalized by the codec of the runner. The owner passes them to the Store untouched.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PageRows {
    pub roots: Vec<crate::owners::store::RowValue>,
    pub related: Vec<(String, crate::owners::store::RowValue)>,
    pub total: Option<i64>,
}

/// The answer of the server for one page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AnswerPage {
    pub page: u32,
    pub rows: Vec<String>,
    pub cursor: Option<String>,
    pub dependency_models: BTreeSet<String>,
    pub payload: PageRows,
}

impl AnswerPage {
    pub fn new(page: u32, rows: Vec<String>, cursor: Option<String>) -> Self { Self { page, rows, cursor, dependency_models: BTreeSet::new(), payload: PageRows::default() } }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WindowOutcome {
    Loaded { revision: i64 },
    Failed { error: String },
    PageLimit,
    Interrupted,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WindowEffect {
    Request { request: RequestId, attempt: u32, page: u32, cursor: Option<String> },
    Cancel { request: RequestId, attempt: u32 },
    Outcome { id: InterestId, outcome: WindowOutcome },
    DerivePreload { revision: i64 },
}

/// What a machine gives out: an effect of the runner, or an input for another owner or a timer that the owner `Windows` turns into `Effect`.
enum Emit {
    Run(WindowEffect),
    Commit { request: RequestId, basis: BTreeMap<String, u64>, page: AnswerPage, number: u32, replace_all: bool, base_revision: i64 },
    Retry { request: RequestId, attempt: u32, class: ErrorClass },
    CancelRetry { request: RequestId },
    Announce { state: WindowState, streams: Vec<InterestId> },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WindowsEffect {
    Window { key: WindowKey, effect: WindowEffect },
    Drained(Drained),
}

/// Which keys of a model a `Stale` names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StaleKeys {
    All,
    Set(BTreeSet<WindowKey>),
}

#[derive(Clone, Debug)]
pub enum WindowsInput {
    /// A stream opened the key (Delivery open) or the preload of the key began.
    InterestPlus { key: WindowKey, id: InterestId },
    InterestMinus { key: WindowKey, id: InterestId },
    /// `begin_load`, `begin_load_all`, `refresh`, `load_more`, `node`, `nodes`. `store_revision` is the Published revision of the Store at the entry.
    Load { key: WindowKey, id: InterestId, goal: Goal, force: bool, store_revision: i64 },
    Response { request: RequestId, attempt: u32, page: AnswerPage },
    Failure { request: RequestId, attempt: u32, error: RequestError },
    Dropped { request: RequestId, attempt: u32 },
    Committed(Committed),
    Rejected(Rejected),
    Stale { model: String, keys: StaleKeys },
    RetryDue { request: RequestId },
    /// The executor of `DerivePreload` found these keys in the rows of a source window.
    PreloadKeys { keys: Vec<WindowKey> },
    /// A Store broadcast `Committed{tables}`: the models whose tables changed and the revision.
    StoreCommitted { models: Vec<String>, revision: i64 },
    Online(Online),
    /// The application came to the front; `seq` grows by 1 with every return.
    Foreground(u64),
    EpochStarted(EpochStarted),
    EpochEnded(EpochEnded),
    ModelsReset(ModelsReset),
    Closing,
    Tick(Time),
}

/// An input of one machine.
#[derive(Clone, Debug)]
enum Ev {
    InterestPlus { id: InterestId },
    InterestMinus { id: InterestId },
    Load { id: InterestId, goal: Goal, force: bool, store_revision: i64 },
    Response { request: RequestId, attempt: u32, page: AnswerPage },
    /// `Failure` and `Dropped`: the cells are the same, a dropped request is a temporary network error.
    Failure { request: RequestId, attempt: u32, error: RequestError },
    Committed { request: RequestId, revision: i64 },
    Rejected { request: RequestId, reason: RejectReason },
    Stale,
    Foreground,
    SourceChanged { revision: i64 },
    RetryDue { request: RequestId },
    Online(bool),
    ModelsReset { generation: u64, models: Vec<String> },
    EpochEnded,
    Closing,
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
        ErrorClass::Permanent | ErrorClass::InvalidCursor => 0,
    }
}

struct Ctx<'a> {
    next_request: &'a mut RequestId,
    next_attempt: &'a mut u32,
}

impl Ctx<'_> {
    fn request(&mut self) -> (RequestId, u32) {
        let request = *self.next_request;
        *self.next_request += 1;
        (request, self.attempt())
    }

    fn attempt(&mut self) -> u32 {
        let attempt = *self.next_attempt;
        *self.next_attempt += 1;
        attempt
    }
}

/// The generations of the models at the moment a request left.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GenerationsAtIssue {
    base: u64,
    generations: BTreeMap<String, u64>,
}

impl GenerationsAtIssue {
    fn of(&self, model: &str) -> u64 { self.generations.get(model).copied().unwrap_or(self.base) }
}

/// The values every state of the machine shares.
struct Facts {
    epoch: Epoch,
    online: Option<bool>,
    base_generation: u64,
    generations: BTreeMap<String, u64>,
    dependency_models: BTreeSet<String>,
    interest: BTreeSet<InterestId>,
    waiters: BTreeMap<InterestId, Want>,
    fresh: bool,
    cursor: Option<String>,
    /// The last page that landed.
    page: u32,
    page_count: u32,
    max_pages: u32,
    cursor_reset_used: bool,
    replace_all: bool,
    reread_due: bool,
    has_rows: bool,
    phase: Phase,
    error: Option<String>,
    /// The revision of the last `Committed` of a landing of this machine, 0 before the first.
    landed_revision: i64,
    store_seen: i64,
    request_base: std::cell::Cell<i64>,
    fx: RefCell<Vec<Emit>>,
}

impl Facts {
    fn env(&mut self, event: &Ev) {
        match event {
            Ev::Online(online) => self.online = Some(*online),
            Ev::ModelsReset { generation, models } => {
                for model in models {
                    self.generations.insert(model.clone(), *generation);
                }
            }
            Ev::Load { store_revision, .. } => self.store_seen = self.store_seen.max(*store_revision),
            _ => {}
        }
    }

    fn is_online(&self) -> bool { self.online == Some(true) }

    fn watched(&self) -> bool { !self.interest.is_empty() || !self.waiters.is_empty() }

    fn push_fx(&self, effect: WindowEffect) {
        if matches!(effect, WindowEffect::Request { .. }) {
            self.request_base.set(self.store_seen.max(self.landed_revision));
        }
        self.fx.borrow_mut().push(Emit::Run(effect));
    }

    fn emit(&self, emit: Emit) { self.fx.borrow_mut().push(emit); }

    fn generation_of(&self, model: &str) -> u64 { self.generations.get(model).copied().unwrap_or(self.base_generation) }

    /// The generation of every model when the request leaves: the answer may name models the window did not know before.
    fn basis(&self) -> GenerationsAtIssue { GenerationsAtIssue { base: self.base_generation, generations: self.generations.clone() } }

    fn depends_on(&self, models: &[String]) -> bool { models.iter().any(|model| self.dependency_models.contains(model)) }

    fn load_state(&self) -> WindowState { WindowState { phase: self.phase, error: self.error.clone(), loaded: (self.landed_revision != 0).then_some(self.landed_revision) } }

    fn announce(&mut self) {
        let streams = self.interest.iter().copied().filter(|id| *id != PRELOAD).collect();
        self.emit(Emit::Announce { state: self.load_state(), streams });
    }

    fn answer(&mut self, id: InterestId, outcome: WindowOutcome) { self.push_fx(WindowEffect::Outcome { id, outcome }); }

    fn answer_all(&mut self, outcome: &WindowOutcome) {
        for id in std::mem::take(&mut self.waiters).into_keys() {
            self.answer(id, outcome.clone());
        }
    }

    fn failed_phase(&self) -> Phase { if self.has_rows { Phase::Loaded } else { Phase::Failed } }

    fn page_cursor(&self, page: u32) -> Option<String> { if page > 1 { self.cursor.clone() } else { None } }

    fn page_limit_reached(&self) -> bool { self.page_count >= self.max_pages }

    /// A load of `page`: a request when online, `Waiting` otherwise. `announce` adds the window state.
    fn begin(&mut self, ctx: &mut Ctx<'_>, page: u32, force: bool, announce: bool) -> Outcome<WinState> {
        self.replace_all = false;
        let next = if self.is_online() {
            let (request, attempt) = ctx.request();
            self.phase = Phase::Loading;
            self.push_fx(WindowEffect::Request { request, attempt, page, cursor: self.page_cursor(page) });
            WinState::loading(request, attempt, false, page, force, 0, self.basis())
        } else {
            WinState::waiting(force, page)
        };
        if announce {
            self.announce();
        }
        Transition(next)
    }

    fn finish(&mut self, request: Option<(RequestId, u32)>) -> Outcome<WinState> {
        self.answer_all(&WindowOutcome::Interrupted);
        if let Some((request, attempt)) = request {
            self.push_fx(WindowEffect::Cancel { request, attempt });
        }
        Transition(WinState::ended())
    }

    fn fail(&mut self, message: &str) -> Outcome<WinState> {
        self.fresh = false;
        self.reread_due = false;
        self.phase = self.failed_phase();
        self.error = Some(message.to_string());
        self.answer_all(&WindowOutcome::Failed { error: message.to_string() });
        self.announce();
        Transition(WinState::idle())
    }

    fn join_load(&mut self, id: InterestId, goal: Goal, page: u32) {
        let want = match goal {
            Goal::First => Want::First,
            Goal::All => Want::All,
            Goal::Next => Want::Next(page + 1),
        };
        self.waiters.insert(id, want);
    }

    fn immediate(&mut self, id: InterestId, outcome: WindowOutcome) -> Outcome<WinState> {
        self.answer(id, outcome);
        Handled
    }

    fn idle_load(&mut self, ctx: &mut Ctx<'_>, id: InterestId, goal: Goal, force: bool) -> Outcome<WinState> {
        let loaded = WindowOutcome::Loaded { revision: self.landed_revision };
        let next_page = self.page + 1;
        match goal {
            Goal::First if self.fresh && !force => self.immediate(id, loaded),
            Goal::All if self.fresh && !force => {
                if self.cursor.is_none() {
                    self.immediate(id, loaded)
                } else if self.page_limit_reached() {
                    self.immediate(id, WindowOutcome::PageLimit)
                } else {
                    self.waiters.insert(id, Want::All);
                    self.begin(ctx, next_page, false, true)
                }
            }
            Goal::First | Goal::All => {
                self.waiters.insert(id, if goal == Goal::All { Want::All } else { Want::First });
                self.begin(ctx, 1, force, true)
            }
            Goal::Next => {
                if self.cursor.is_none() {
                    self.immediate(id, loaded)
                } else if self.page_limit_reached() {
                    self.immediate(id, WindowOutcome::PageLimit)
                } else {
                    self.waiters.insert(id, Want::Next(next_page));
                    self.begin(ctx, next_page, false, true)
                }
            }
        }
    }

    fn landed(&mut self, ctx: &mut Ctx<'_>, page: u32, cursor: Option<String>, rows: bool, dependency_models: &BTreeSet<String>, revision: i64) -> Outcome<WinState> {
        self.has_rows |= rows;
        self.cursor = cursor;
        self.page = page;
        self.landed_revision = revision;
        self.error = None;
        self.cursor_reset_used = false;
        self.replace_all = false;
        self.phase = Phase::Loaded;
        if !dependency_models.is_empty() {
            self.dependency_models = dependency_models.clone();
        }
        let wants_next = |want: &Want| matches!(want, Want::All) || matches!(want, Want::Next(c) if *c > page);
        let loaded = WindowOutcome::Loaded { revision };
        if self.cursor.is_some() && self.waiters.values().any(wants_next) {
            if self.page_limit_reached() {
                for (id, want) in std::mem::take(&mut self.waiters) {
                    self.answer(id, if wants_next(&want) { WindowOutcome::PageLimit } else { loaded.clone() });
                }
                self.announce();
                self.fresh = true;
                return Transition(WinState::idle());
            }
            let finished: Vec<InterestId> = self.waiters.iter().filter(|(_, want)| !wants_next(want)).map(|(id, _)| *id).collect();
            for id in finished {
                self.waiters.remove(&id);
                self.answer(id, loaded.clone());
            }
            return self.begin(ctx, page + 1, false, true);
        }
        self.answer_all(&loaded);
        if !self.reread_due {
            self.fresh = true;
            self.announce();
            return Transition(WinState::idle());
        }
        self.reread_due = false;
        if !self.watched() {
            self.fresh = false;
            self.announce();
            return Transition(WinState::idle());
        }
        self.begin(ctx, 1, false, true)
    }
}

#[state_machine(initial = "WinState::idle()", state(name = "WinState"))]
impl Facts {
    #[state]
    fn idle(&mut self, context: &mut Ctx<'_>, event: &Ev) -> Outcome<WinState> {
        self.env(event);
        match event {
            Ev::InterestPlus { id } => {
                self.interest.insert(*id);
                if self.fresh {
                    self.phase = Phase::Loaded;
                    self.announce();
                    Handled
                } else {
                    self.begin(context, 1, false, true)
                }
            }
            Ev::Load { id, goal, force, .. } => self.idle_load(context, *id, *goal, *force),
            Ev::InterestMinus { id } => {
                self.interest.remove(id);
                self.waiters.remove(id);
                Handled
            }
            Ev::Stale if self.watched() => self.begin(context, 1, false, self.is_online()),
            Ev::Stale => {
                self.fresh = false;
                Handled
            }
            Ev::Foreground if !self.interest.is_empty() && self.is_online() => self.begin(context, 1, false, true),
            Ev::SourceChanged { revision } => {
                self.push_fx(WindowEffect::DerivePreload { revision: *revision });
                Handled
            }
            Ev::Online(true) if !self.fresh && self.watched() => self.begin(context, 1, false, true),
            Ev::ModelsReset { models, .. } if self.depends_on(models) => {
                self.fresh = false;
                if self.watched() { self.begin(context, 1, false, self.is_online()) } else { Handled }
            }
            Ev::EpochEnded | Ev::Closing => self.finish(None),
            _ => Handled,
        }
    }

    #[state]
    #[allow(clippy::too_many_arguments)]
    fn loading(&mut self, context: &mut Ctx<'_>, request: &mut RequestId, attempt: &mut u32, retry: &mut bool, page: &mut u32, force: &mut bool, attempts: &mut u32, basis: &mut GenerationsAtIssue, event: &Ev) -> Outcome<WinState> {
        self.env(event);
        let mine = |got: &RequestId, got_attempt: &u32| got == request && got_attempt == attempt && !*retry;
        match event {
            Ev::InterestPlus { id } => {
                self.interest.insert(*id);
                self.announce();
                Handled
            }
            Ev::Load { id, goal, force: wider, .. } => {
                self.join_load(*id, *goal, *page);
                *force |= *wider && *goal != Goal::Next;
                Handled
            }
            Ev::InterestMinus { id } => {
                self.interest.remove(id);
                self.waiters.remove(id);
                if self.watched() {
                    return Handled;
                }
                self.fresh = false;
                self.phase = if self.has_rows { Phase::Loaded } else { Phase::Idle };
                self.push_fx(WindowEffect::Cancel { request: *request, attempt: *attempt });
                self.emit(Emit::CancelRetry { request: *request });
                Transition(WinState::idle())
            }
            Ev::Response { request: got, attempt: got_attempt, page: answer } if mine(got, got_attempt) => {
                self.page_count = if *page == 1 { 1 } else { self.page_count + 1 };
                let models: BTreeSet<&String> = self.dependency_models.iter().chain(&answer.dependency_models).chain(answer.payload.related.iter().map(|(model, _)| model)).collect();
                let basis = models.into_iter().map(|model| (model.clone(), basis.of(model))).collect();
                self.emit(Emit::Commit { request: *request, basis, page: answer.clone(), number: *page, replace_all: self.replace_all, base_revision: self.request_base.get() });
                Transition(WinState::landing(*request, *page, answer.cursor.clone(), !answer.rows.is_empty(), answer.dependency_models.clone()))
            }
            Ev::Failure { request: got, attempt: got_attempt, error } if mine(got, got_attempt) => {
                if error.class == ErrorClass::InvalidCursor && !self.cursor_reset_used {
                    let (new_request, new_attempt) = context.request();
                    self.cursor_reset_used = true;
                    self.replace_all = true;
                    self.push_fx(WindowEffect::Request { request: new_request, attempt: new_attempt, page: 1, cursor: None });
                    return Transition(WinState::loading(new_request, new_attempt, false, 1, *force, 0, self.basis()));
                }
                let class = if error.class == ErrorClass::InvalidCursor { ErrorClass::Permanent } else { error.class };
                if class != ErrorClass::Permanent && *attempts + 1 < attempt_limit(class) {
                    *attempts += 1;
                    *retry = true;
                    self.emit(Emit::Retry { request: *request, attempt: *attempts, class });
                    return Handled;
                }
                self.fail(&error.message)
            }
            Ev::RetryDue { request: got } if got == request && *retry && self.is_online() => {
                *attempt = context.attempt();
                *retry = false;
                self.push_fx(WindowEffect::Request { request: *request, attempt: *attempt, page: *page, cursor: self.page_cursor(*page) });
                Handled
            }
            Ev::Online(false) => {
                if !*retry {
                    self.push_fx(WindowEffect::Cancel { request: *request, attempt: *attempt });
                }
                self.emit(Emit::CancelRetry { request: *request });
                Transition(WinState::waiting(*force, *page))
            }
            Ev::Stale => {
                self.reread_due = true;
                Handled
            }
            Ev::SourceChanged { revision } => {
                self.push_fx(WindowEffect::DerivePreload { revision: *revision });
                Handled
            }
            Ev::ModelsReset { models, .. } if self.depends_on(models) => {
                let (new_request, new_attempt) = context.request();
                self.push_fx(WindowEffect::Cancel { request: *request, attempt: *attempt });
                self.emit(Emit::CancelRetry { request: *request });
                self.push_fx(WindowEffect::Request { request: new_request, attempt: new_attempt, page: *page, cursor: self.page_cursor(*page) });
                *request = new_request;
                *attempt = new_attempt;
                *retry = false;
                *attempts = 0;
                *basis = self.basis();
                Handled
            }
            Ev::EpochEnded | Ev::Closing => {
                self.emit(Emit::CancelRetry { request: *request });
                self.finish((!*retry).then_some((*request, *attempt)))
            }
            _ => Handled,
        }
    }

    #[state]
    #[allow(clippy::too_many_arguments)]
    fn landing(&mut self, context: &mut Ctx<'_>, request: &mut RequestId, page: &mut u32, cursor: &mut Option<String>, rows: &mut bool, models: &mut BTreeSet<String>, event: &Ev) -> Outcome<WinState> {
        self.env(event);
        match event {
            Ev::Committed { request: got, revision } if got == request => self.landed(context, *page, cursor.clone(), *rows, models, *revision),
            Ev::Rejected { request: got, reason } if got == request => match reason {
                RejectReason::StaleGeneration { models: stale, generation } => {
                    for model in stale {
                        self.generations.insert(model.clone(), *generation);
                    }
                    if self.watched() {
                        self.begin(context, 1, false, false)
                    } else {
                        self.fresh = false;
                        Transition(WinState::idle())
                    }
                }
                RejectReason::NoSession | RejectReason::Closed => {
                    self.answer_all(&WindowOutcome::Interrupted);
                    Transition(WinState::ended())
                }
                RejectReason::Storage(message) => self.fail(message),
            },
            Ev::InterestPlus { id } => {
                self.interest.insert(*id);
                self.announce();
                Handled
            }
            Ev::Load { id, goal, .. } => {
                self.join_load(*id, *goal, *page);
                Handled
            }
            Ev::InterestMinus { id } => {
                self.interest.remove(id);
                self.waiters.remove(id);
                Handled
            }
            Ev::Stale => {
                self.reread_due = true;
                Handled
            }
            Ev::SourceChanged { revision } => {
                self.push_fx(WindowEffect::DerivePreload { revision: *revision });
                Handled
            }
            Ev::EpochEnded | Ev::Closing => self.finish(None),
            _ => Handled,
        }
    }

    #[state]
    fn waiting(&mut self, context: &mut Ctx<'_>, force: &mut bool, page: &mut u32, event: &Ev) -> Outcome<WinState> {
        self.env(event);
        match event {
            Ev::Online(true) => {
                let (request, attempt) = context.request();
                self.phase = Phase::Loading;
                self.push_fx(WindowEffect::Request { request, attempt, page: *page, cursor: self.page_cursor(*page) });
                self.announce();
                Transition(WinState::loading(request, attempt, false, *page, *force, 0, self.basis()))
            }
            Ev::InterestPlus { id } => {
                self.interest.insert(*id);
                self.announce();
                Handled
            }
            Ev::Load { id, goal, force: wider, .. } => {
                self.join_load(*id, *goal, *page);
                *force |= *wider && *goal != Goal::Next;
                Handled
            }
            Ev::InterestMinus { id } => {
                self.interest.remove(id);
                self.waiters.remove(id);
                if self.watched() {
                    Handled
                } else {
                    self.fresh = false;
                    Transition(WinState::idle())
                }
            }
            Ev::SourceChanged { revision } => {
                self.push_fx(WindowEffect::DerivePreload { revision: *revision });
                Handled
            }
            Ev::EpochEnded | Ev::Closing => self.finish(None),
            _ => Handled,
        }
    }

    #[state]
    fn ended(&mut self, event: &Ev) -> Outcome<WinState> {
        if let Ev::InterestPlus { id } | Ev::Load { id, .. } = event {
            self.answer(*id, WindowOutcome::Interrupted);
        }
        Handled
    }
}

/// One machine of `window.md`.
struct Window {
    machine: InitializedStateMachine<Facts>,
}

impl Window {
    fn new(model: String, epoch: Epoch, online: Option<bool>, base_generation: u64, generations: BTreeMap<String, u64>, max_pages: u32, ids: (&mut RequestId, &mut u32)) -> Self {
        let facts = Facts {
            dependency_models: BTreeSet::from([model]),
            epoch,
            online,
            base_generation,
            generations,
            interest: BTreeSet::new(),
            waiters: BTreeMap::new(),
            fresh: false,
            cursor: None,
            page: 0,
            page_count: 0,
            max_pages,
            cursor_reset_used: false,
            replace_all: false,
            reread_due: false,
            has_rows: false,
            phase: Phase::Idle,
            error: None,
            landed_revision: 0,
            store_seen: 0,
            request_base: std::cell::Cell::new(0),
            fx: RefCell::new(Vec::new()),
        };
        let mut ctx = Ctx { next_request: ids.0, next_attempt: ids.1 };
        Self { machine: facts.uninitialized_state_machine().init_with_context(&mut ctx) }
    }

    fn handle(&mut self, event: Ev, ids: (&mut RequestId, &mut u32)) -> Vec<Emit> {
        let mut ctx = Ctx { next_request: ids.0, next_attempt: ids.1 };
        self.machine.handle_with_context(&event, &mut ctx);
        self.machine.fx.take()
    }

    fn stage(&self) -> WindowStage {
        match self.machine.state() {
            WinState::Idle {} => WindowStage::Idle,
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

/// How many pages a key may land at most: the `max_pages` of its operation in the `.dbl`.
pub type MaxPages = Arc<dyn Fn(&WindowKey) -> u32 + Send + Sync>;

/// Whether the `.dbl` declares `refresh: foreground` for the relation of a key.
pub type OnForeground = Arc<dyn Fn(&WindowKey) -> bool + Send + Sync>;

/// The state of the owner: the fields the machines read (`online`, `generation[m]`), the keys declared `preload` and the machines.
pub struct WindowsState {
    epoch: Epoch,
    online: Option<bool>,
    generations: BTreeMap<String, u64>,
    base_generation: u64,
    preload: Vec<WindowKey>,
    sources: BTreeSet<WindowKey>,
    next_request: RequestId,
    next_attempt: u32,
    windows: BTreeMap<WindowKey, Window>,
    max_pages: MaxPages,
    on_foreground: OnForeground,
    foreground_seen: u64,
    peers: WindowsPeers,
}

/// The owners `Windows` sends inputs to.
#[derive(Clone, Copy, Debug)]
pub struct WindowsPeers {
    pub store: Address<StoreWriter>,
    pub deliveries: Address<Deliveries>,
}

impl WindowsState {
    /// `online` is the value of the first addressed `Online`, `None` before it.
    pub fn new(epoch: Epoch, online: Option<bool>, peers: WindowsPeers) -> Self {
        Self { epoch, online, generations: BTreeMap::new(), base_generation: 0, preload: Vec::new(), sources: BTreeSet::new(), next_request: 1, next_attempt: 1, windows: BTreeMap::new(), max_pages: Arc::new(|_| u32::MAX), on_foreground: Arc::new(|_| false), foreground_seen: 0, peers }
    }

    /// Keys that `EpochStarted` loads with `Interest+(PRELOAD)`.
    pub fn with_preload(mut self, keys: Vec<WindowKey>) -> Self {
        self.preload = keys;
        self
    }

    /// Keys whose rows name other keys to preload: a Store commit of their model asks `DerivePreload`.
    pub fn with_sources(mut self, keys: Vec<WindowKey>) -> Self {
        self.sources = keys.into_iter().collect();
        self
    }

    pub fn with_max_pages(mut self, max_pages: MaxPages) -> Self {
        self.max_pages = max_pages;
        self
    }

    pub fn with_foreground(mut self, on_foreground: OnForeground) -> Self {
        self.on_foreground = on_foreground;
        self
    }

    pub fn stage(&self, key: &WindowKey) -> Option<WindowStage> { self.windows.get(key).map(Window::stage) }

    pub fn interest(&self, key: &WindowKey) -> Vec<InterestId> { self.windows.get(key).map(|window| window.machine.interest.iter().copied().collect()).unwrap_or_default() }

    pub fn waiters(&self, key: &WindowKey) -> Vec<InterestId> { self.windows.get(key).map(|window| window.machine.waiters.keys().copied().collect()).unwrap_or_default() }

    pub fn load_state(&self, key: &WindowKey) -> Option<WindowState> { self.windows.get(key).map(|window| window.machine.load_state()) }

    pub fn windows(&self) -> usize { self.windows.len() }

    fn ensure(&mut self, key: &WindowKey) {
        if !self.windows.contains_key(key) {
            let ids = (&mut self.next_request, &mut self.next_attempt);
            let window = Window::new(key.model.clone(), self.epoch, self.online, self.base_generation, self.generations.clone(), (self.max_pages)(key), ids);
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
        for emit in window.handle(event, (&mut state.next_request, &mut state.next_attempt)) {
            match emit {
                Emit::Run(effect) => out.push(Effect::Run(WindowsEffect::Window { key: key.clone(), effect })),
                Emit::Commit { request, basis, page, number, replace_all, base_revision } => {
                    let rows: Vec<(String, String)> = page.payload.roots.iter().map(|row| (key.model.clone(), row.row_id.clone())).chain(page.payload.related.iter().map(|(model, row)| (model.clone(), row.row_id.clone()))).collect();
                    let op = StoreOp::Window { key: key.name.clone(), model: key.model.clone(), page: number, replace_all, cursor: page.cursor.clone(), roots: page.payload.roots.clone(), related: page.payload.related.clone(), total: page.payload.total, base_revision };
                    let batch = StoreBatch { author: BatchAuthor::Window, id: request, epoch, basis, rows, intents: 0, credential: None, ops: vec![op] };
                    out.push(Effect::Send(Outbound::new(peers.store, StoreInput::Commit(batch))));
                }
                Emit::Retry { request, attempt, class } => {
                    let policy = if class == ErrorClass::Network { RetryPolicy::QueryNetwork } else { RetryPolicy::QueryServer };
                    let after: Duration = policy.backoff().delay(attempt - 1);
                    out.push(Effect::Schedule { id: ScheduleId(request), after, input: WindowsInput::RetryDue { request } });
                }
                Emit::CancelRetry { request } => out.push(Effect::Cancel(ScheduleId(request))),
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

    fn interest_plus(state: &mut WindowsState, key: WindowKey, id: InterestId, out: &mut Effects<Self>) {
        state.ensure(&key);
        Self::on(state, &key, Ev::InterestPlus { id }, out);
    }

    fn load(state: &mut WindowsState, key: WindowKey, id: InterestId, goal: Goal, force: bool, store_revision: i64, out: &mut Effects<Self>) {
        if key.is_empty_nodes() {
            let effect = WindowEffect::Outcome { id, outcome: WindowOutcome::Loaded { revision: store_revision } };
            out.push(Effect::Run(WindowsEffect::Window { key, effect }));
            return;
        }
        state.ensure(&key);
        Self::on(state, &key, Ev::Load { id, goal, force, store_revision }, out);
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
            WindowsInput::InterestPlus { key, id } => Self::interest_plus(state, key, id, &mut out),
            WindowsInput::InterestMinus { key, id } => Self::on(state, &key, Ev::InterestMinus { id }, &mut out),
            WindowsInput::Load { key, id, goal, force, store_revision } => Self::load(state, key, id, goal, force, store_revision, &mut out),
            WindowsInput::Response { request, attempt, page } => Self::by_request(state, request, Ev::Response { request, attempt, page }, &mut out),
            WindowsInput::Failure { request, attempt, error } => Self::by_request(state, request, Ev::Failure { request, attempt, error: error.clone() }, &mut out),
            WindowsInput::Dropped { request, attempt } => Self::by_request(state, request, Ev::Failure { request, attempt, error: RequestError { message: "dropped".into(), class: ErrorClass::Network } }, &mut out),
            WindowsInput::Committed(committed) => Self::by_request(state, committed.batch, Ev::Committed { request: committed.batch, revision: committed.revision }, &mut out),
            WindowsInput::Rejected(rejected) => Self::by_request(state, rejected.batch, Ev::Rejected { request: rejected.batch, reason: rejected.reason }, &mut out),
            WindowsInput::Stale { model, keys } => {
                let named: Vec<WindowKey> = state.windows.keys().filter(|key| key.model == model && match &keys { StaleKeys::All => true, StaleKeys::Set(set) => set.contains(key) }).cloned().collect();
                for key in named {
                    Self::on(state, &key, Ev::Stale, &mut out);
                }
            }
            WindowsInput::RetryDue { request } => Self::by_request(state, request, Ev::RetryDue { request }, &mut out),
            WindowsInput::PreloadKeys { keys } => {
                for key in keys {
                    if !state.windows.contains_key(&key) {
                        Self::interest_plus(state, key, PRELOAD, &mut out);
                    }
                }
            }
            WindowsInput::StoreCommitted { models, revision } => {
                let sources: Vec<WindowKey> = state.sources.iter().filter(|key| models.contains(&key.model) && state.windows.contains_key(*key)).cloned().collect();
                for key in sources {
                    Self::on(state, &key, Ev::SourceChanged { revision }, &mut out);
                }
            }
            WindowsInput::Foreground(seq) if seq > state.foreground_seen => {
                state.foreground_seen = seq;
                let named: Vec<WindowKey> = state.windows.keys().filter(|key| (state.on_foreground)(key)).cloned().collect();
                for key in named {
                    Self::on(state, &key, Ev::Foreground, &mut out);
                }
            }
            WindowsInput::Foreground(_) => {}
            WindowsInput::Online(Online(online)) => {
                state.online = Some(online);
                Self::every(state, &Ev::Online(online), &mut out);
            }
            WindowsInput::EpochStarted(started) => {
                state.epoch = started.epoch;
                state.base_generation = started.generation;
                state.generations.clear();
                state.windows.clear();
                state.online = None;
                state.foreground_seen = 0;
                if started.identity.is_some() {
                    for key in state.preload.clone() {
                        Self::interest_plus(state, key, PRELOAD, &mut out);
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
                out.push(Effect::Run(WindowsEffect::Drained(Drained { owner: "windows" })));
            }
            WindowsInput::Tick(_) => {}
        }
        out
    }

    fn tick(now: Time) -> WindowsInput { WindowsInput::Tick(now) }

    fn epoch_ended(epoch: Epoch, cause: Cause) -> WindowsInput { WindowsInput::EpochEnded(EpochEnded { epoch, cause }) }

    fn closing() -> WindowsInput { WindowsInput::Closing }
}
