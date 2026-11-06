//! The `window` machine as code: one match arm per table row. No library code.

use std::collections::{BTreeMap, BTreeSet};


/// Observables shared by the references and the core.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Phase {
    Idle,
    Loading,
    Loaded,
    Failed,
}

pub(super) const PRELOAD: u32 = u32::MAX;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Result3 {
    Loaded,
    Failed,
    PageLimit,
    Interrupted,
}

/// Failure classes of a transport error: the first two are temporary and retried up to `retry_limit`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Failure {
    Network,
    Server,
    Permanent,
    InvalidCursor,
}

impl Failure {
    /// Attempts of a window per class.
    pub(super) fn retry_limit(self) -> u32 {
        match self {
            Failure::Network => 3,
            Failure::Server => 2,
            Failure::Permanent | Failure::InvalidCursor => 0,
        }
    }

    fn temporary(self) -> bool { matches!(self, Failure::Network | Failure::Server) }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Reject {
    StaleGeneration,
    NoSession,
    Closed,
    Storage,
    Invalid,
}

/// What a `Load` asks for: the first page, the page after the last one the caller saw, or every page.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Goal {
    First,
    Next,
    All,
}

/// A stored goal: `Next(c)` waits for the landing of the page `c`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Want {
    First,
    Next(u32),
    All,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Input {
    InterestPlus { id: u32 },
    InterestPassive { id: u32 },
    InterestMinus { id: u32 },
    Load { id: u32, goal: Goal, force: bool },
    Response { request: u64, attempt: u32, cursor: Option<u32>, rows: bool },
    TransportError { request: u64, attempt: u32, failure: Failure },
    Dropped { request: u64, attempt: u32 },
    Retry { request: u64 },
    Committed { request: u64, revision: i64 },
    Rejected { request: u64, reason: Reject },
    Stale { in_keys: bool },
    Foreground { seq: u64 },
    SourceChanged { revision: i64 },
    Online(bool),
    EpochEnded,
    ModelsReset { own_model: bool },
    Closing,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Effect {
    Request { request: u64, attempt: u32, page: u32 },
    Cancel { request: u64, attempt: u32 },
    Commit { request: u64, replace_all: bool },
    ScheduleRetry { request: u64 },
    WindowState,
    Drained,
    DerivePreload { revision: i64 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum State {
    Idle,
    Loading { request: u64, attempt: u32, retry: bool, page: u32, force: bool, attempts: u32 },
    Landing { request: u64, page: u32, cursor: Option<u32>, rows: bool },
    Waiting { force: bool, page: u32 },
    Ended,
}

pub(super) struct Window {
    pub(super) state: State,
    pub(super) online: Option<bool>,
    pub(super) fresh: bool,
    /// The page the next cursor points at, `None` = no more pages.
    pub(super) cursor: Option<u32>,
    pub(super) last_error: Option<bool>,
    pub(super) interest: BTreeSet<u32>,
    pub(super) waiters: BTreeMap<u32, Want>,
    pub(super) reread_due: bool,
    /// The last page that landed.
    pub(super) page: u32,
    pub(super) page_count: u32,
    pub(super) max_pages: u32,
    pub(super) cursor_reset_used: bool,
    pub(super) replace_all: bool,
    pub(super) has_rows: bool,
    pub(super) landed_revision: i64,
    pub(super) foreground_seen: u64,
    pub(super) phase: Phase,
    pub(super) settled: bool,
    pub(super) outcomes: Vec<(u32, Result3)>,
    next_request: u64,
    next_attempt: u32,
}

impl Window {
    pub(super) fn new() -> Self {
        Self {
            state: State::Idle,
            online: Some(true),
            fresh: false,
            cursor: None,
            last_error: None,
            interest: BTreeSet::new(),
            waiters: BTreeMap::new(),
            reread_due: false,
            page: 0,
            page_count: 0,
            max_pages: 1000,
            cursor_reset_used: false,
            replace_all: false,
            has_rows: false,
            landed_revision: 0,
            foreground_seen: 0,
            phase: Phase::Idle,
            settled: false,
            outcomes: Vec::new(),
            next_request: 1,
            next_attempt: 1,
        }
    }


    /// Before the first `Online` of the dictionary.
    pub(super) fn before_online() -> Self {
        let mut window = Self::new();
        window.online = None;
        window
    }

    pub(super) fn take_outcomes(&mut self) -> Vec<(u32, Result3)> { std::mem::take(&mut self.outcomes) }

    /// Row `win_nodes_empty`: a key `nodes` without ids creates no machine.
    pub(super) fn empty_nodes_outcome(id: u32) -> (u32, Result3) { (id, Result3::Loaded) }

    fn is_online(&self) -> bool { self.online == Some(true) }

    fn watched(&self) -> bool { !self.interest.is_empty() || !self.waiters.is_empty() }

    fn request_ids(&mut self) -> (u64, u32) {
        let request = self.next_request;
        self.next_request += 1;
        (request, self.attempt_id())
    }

    fn attempt_id(&mut self) -> u32 {
        let attempt = self.next_attempt;
        self.next_attempt += 1;
        attempt
    }

    fn answer(&mut self, id: u32, result: Result3) { self.outcomes.push((id, result)); }

    fn answer_all(&mut self, result: Result3) {
        for id in std::mem::take(&mut self.waiters).into_keys() { self.outcomes.push((id, result)); }
    }

    fn failed_phase(&self) -> Phase { if self.has_rows { Phase::Loaded } else { Phase::Failed } }

    /// A load of `page`: a request online, `Waiting` otherwise. `announce` adds `WindowState`.
    fn begin(&mut self, page: u32, force: bool, announce: bool) -> Vec<Effect> {
        let mut effects = Vec::new();
        self.replace_all = false;
        if self.is_online() {
            let (request, attempt) = self.request_ids();
            self.state = State::Loading { request, attempt, retry: false, page, force, attempts: 0 };
            self.phase = Phase::Loading;
            effects.push(Effect::Request { request, attempt, page });
        } else {
            self.state = State::Waiting { force, page };
        }
        if announce { effects.push(Effect::WindowState); }
        effects
    }

    /// The goal a waiter gets when it joins while `page` is the page in flight or about to be asked.
    fn after(page: u32) -> Want { Want::Next(page + 1) }

    pub(super) fn step(&mut self, input: Input) -> Vec<Effect> {
        if self.state == State::Ended {
            return match input {
                Input::Closing => vec![Effect::Drained],
                Input::InterestPlus { id } | Input::Load { id, .. } => {
                    self.answer(id, Result3::Interrupted);
                    Vec::new()
                }
                _ => Vec::new(),
            };
        }
        match input {
            Input::EpochEnded => return self.end(false),
            Input::Closing => return self.end(true),
            Input::SourceChanged { revision } => return vec![Effect::DerivePreload { revision }],
            Input::Online(online) => self.online = Some(online),
            Input::Foreground { seq } => {
                if seq <= self.foreground_seen {
                    return Vec::new();
                }
                self.foreground_seen = seq;
            }
            _ => {}
        }
        match self.state {
            State::Idle => self.idle(input),
            State::Loading { request, attempt, retry, page, force, attempts } => self.loading(request, attempt, retry, page, force, attempts, input),
            State::Landing { request, page, cursor, rows } => self.landing(request, page, cursor, rows, input),
            State::Waiting { force, page } => self.waiting(force, page, input),
            State::Ended => Vec::new(),
        }
    }

    fn immediate(&mut self, id: u32, result: Result3) -> Vec<Effect> {
        self.answer(id, result);
        Vec::new()
    }

    fn set_phase(&mut self, phase: Phase) {
        self.phase = phase;
        self.settled |= matches!(phase, Phase::Loaded | Phase::Failed);
    }

    pub(super) fn has_more(&self) -> bool { self.cursor.is_some() }

    fn page_limit_reached(&self) -> bool { self.page_count >= self.max_pages }

    fn idle(&mut self, input: Input) -> Vec<Effect> {
        match input {
            Input::InterestPassive { id } => {
                self.interest.insert(id);
                vec![Effect::WindowState]
            }
            Input::InterestPlus { id } => {
                self.interest.insert(id);
                if self.fresh {
                    self.set_phase(Phase::Loaded);
                    vec![Effect::WindowState]
                } else {
                    self.begin(1, false, true)
                }
            }
            Input::Load { id, goal, force } => self.idle_load(id, goal, force),
            Input::InterestMinus { id } => {
                self.interest.remove(&id);
                if self.interest.is_empty() {
                    self.settled = false;
                }
                self.waiters.remove(&id);
                Vec::new()
            }
            Input::Stale { in_keys: false } => Vec::new(),
            Input::Stale { in_keys: true } => {
                if self.watched() {
                    self.begin(1, false, self.is_online())
                } else {
                    self.fresh = false;
                    Vec::new()
                }
            }
            Input::Foreground { .. } if !self.interest.is_empty() && self.is_online() => self.begin(1, false, true),
            Input::Online(true) if !self.fresh && self.watched() => self.begin(1, false, true),
            Input::ModelsReset { own_model: true } => {
                self.fresh = false;
                if self.watched() { self.begin(1, false, self.is_online()) } else { Vec::new() }
            }
            _ => Vec::new(),
        }
    }

    fn idle_load(&mut self, id: u32, goal: Goal, force: bool) -> Vec<Effect> {
        match goal {
            Goal::First if self.fresh && !force => self.immediate(id, Result3::Loaded),
            Goal::All if self.fresh && !force => match self.cursor {
                None => self.immediate(id, Result3::Loaded),
                Some(_) if self.page_limit_reached() => self.immediate(id, Result3::PageLimit),
                Some(cursor) => {
                    self.waiters.insert(id, Want::All);
                    self.begin(cursor, false, true)
                }
            },
            Goal::First | Goal::All => {
                self.waiters.insert(id, if goal == Goal::All { Want::All } else { Want::First });
                self.begin(1, force, true)
            }
            Goal::Next => match self.cursor {
                None => self.immediate(id, Result3::Loaded),
                Some(_) if self.page_limit_reached() => self.immediate(id, Result3::PageLimit),
                Some(cursor) => {
                    self.waiters.insert(id, Want::Next(cursor));
                    self.begin(cursor, false, true)
                }
            },
        }
    }

    fn join_load(&mut self, id: u32, goal: Goal, page: u32) {
        let want = match goal {
            Goal::First => Want::First,
            Goal::All => Want::All,
            Goal::Next => Self::after(page),
        };
        self.waiters.insert(id, want);
    }

    #[allow(clippy::too_many_arguments)]
    fn loading(&mut self, request: u64, attempt: u32, retry: bool, page: u32, force: bool, attempts: u32, input: Input) -> Vec<Effect> {
        let mine = |got_request: u64, got_attempt: u32| got_request == request && got_attempt == attempt && !retry;
        match input {
            Input::InterestPlus { id } => {
                self.interest.insert(id);
                vec![Effect::WindowState]
            }
            Input::Load { id, goal, force: wider } => {
                self.join_load(id, goal, page);
                self.state = State::Loading { request, attempt, retry, page, force: force || (wider && goal != Goal::Next), attempts };
                Vec::new()
            }
            Input::InterestMinus { id } => {
                self.interest.remove(&id);
                if self.interest.is_empty() {
                    self.settled = false;
                }
                self.waiters.remove(&id);
                if self.watched() {
                    Vec::new()
                } else {
                    self.state = State::Idle;
                    self.fresh = false;
                    self.set_phase(if self.has_rows { Phase::Loaded } else { Phase::Idle });
                    vec![Effect::Cancel { request, attempt }]
                }
            }
            Input::Response { request: got, attempt: got_attempt, cursor, rows } if mine(got, got_attempt) => {
                self.page_count = if page == 1 { 1 } else { self.page_count + 1 };
                self.state = State::Landing { request, page, cursor, rows };
                vec![Effect::Commit { request, replace_all: self.replace_all }]
            }
            Input::TransportError { request: got, attempt: got_attempt, failure } if mine(got, got_attempt) => self.failed(request, page, force, attempts, failure),
            Input::Dropped { request: got, attempt: got_attempt } if mine(got, got_attempt) => self.failed(request, page, force, attempts, Failure::Network),
            Input::Retry { request: got } if got == request && retry && self.is_online() => {
                let attempt = self.attempt_id();
                self.state = State::Loading { request, attempt, retry: false, page, force, attempts };
                vec![Effect::Request { request, attempt, page }]
            }
            Input::Online(false) => {
                self.state = State::Waiting { force, page };
                if retry { Vec::new() } else { vec![Effect::Cancel { request, attempt }] }
            }
            Input::Stale { in_keys: true } => {
                self.reread_due = true;
                Vec::new()
            }
            Input::ModelsReset { own_model: true } => {
                let (new_request, new_attempt) = self.request_ids();
                self.state = State::Loading { request: new_request, attempt: new_attempt, retry: false, page, force, attempts: 0 };
                vec![Effect::Cancel { request, attempt }, Effect::Request { request: new_request, attempt: new_attempt, page }]
            }
            _ => Vec::new(),
        }
    }

    fn failed(&mut self, request: u64, page: u32, force: bool, attempts: u32, failure: Failure) -> Vec<Effect> {
        if failure == Failure::InvalidCursor && !self.cursor_reset_used {
            let (new_request, new_attempt) = self.request_ids();
            self.cursor_reset_used = true;
            self.replace_all = true;
            self.state = State::Loading { request: new_request, attempt: new_attempt, retry: false, page: 1, force, attempts: 0 };
            return vec![Effect::Request { request: new_request, attempt: new_attempt, page: 1 }];
        }
        if failure.temporary() && attempts + 1 < failure.retry_limit() {
            let State::Loading { attempt, .. } = self.state else { return Vec::new() };
            self.state = State::Loading { request, attempt, retry: true, page, force, attempts: attempts + 1 };
            return vec![Effect::ScheduleRetry { request }];
        }
        self.fail(failure == Failure::Network)
    }

    fn fail(&mut self, network: bool) -> Vec<Effect> {
        self.state = State::Idle;
        self.fresh = false;
        self.last_error = Some(network);
        self.reread_due = false;
        self.set_phase(self.failed_phase());
        self.answer_all(Result3::Failed);
        vec![Effect::WindowState]
    }

    fn landing(&mut self, request: u64, page: u32, cursor: Option<u32>, rows: bool, input: Input) -> Vec<Effect> {
        match input {
            Input::Committed { request: got, revision } if got == request => self.landed(page, cursor, rows, revision),
            Input::Rejected { request: got, reason } if got == request => self.rejected(reason),
            Input::InterestPlus { id } => {
                self.interest.insert(id);
                vec![Effect::WindowState]
            }
            Input::Load { id, goal, .. } => {
                self.join_load(id, goal, page);
                Vec::new()
            }
            Input::InterestMinus { id } => {
                self.interest.remove(&id);
                if self.interest.is_empty() {
                    self.settled = false;
                }
                self.waiters.remove(&id);
                Vec::new()
            }
            Input::Stale { in_keys: true } => {
                self.reread_due = true;
                Vec::new()
            }
            _ => Vec::new(),
        }
    }

    fn landed(&mut self, page: u32, cursor: Option<u32>, rows: bool, revision: i64) -> Vec<Effect> {
        self.has_rows |= rows;
        self.cursor = cursor;
        self.page = page;
        self.landed_revision = revision;
        self.last_error = None;
        self.cursor_reset_used = false;
        self.replace_all = false;
        self.set_phase(Phase::Loaded);
        let next_target = |window: &Window| window.waiters.values().any(|want| matches!(want, Want::All) || matches!(want, Want::Next(c) if *c > page));
        let answered: Vec<(u32, Want)> = self.waiters.iter().map(|(id, want)| (*id, *want)).collect();
        let settle = |window: &mut Window, result_for: &dyn Fn(Want) -> Option<Result3>| {
            for (id, want) in &answered {
                if let Some(result) = result_for(*want) {
                    window.waiters.remove(id);
                    window.outcomes.push((*id, result));
                }
            }
        };
        if self.cursor.is_some() && next_target(self) {
            let next = self.cursor.expect("a cursor");
            if self.page_limit_reached() {
                settle(self, &|want| Some(if matches!(want, Want::All) || matches!(want, Want::Next(c) if c > page) { Result3::PageLimit } else { Result3::Loaded }));
                self.state = State::Idle;
                self.fresh = true;
                return vec![Effect::WindowState];
            }
            settle(self, &|want| match want {
                Want::First => Some(Result3::Loaded),
                Want::Next(c) if c <= page => Some(Result3::Loaded),
                _ => None,
            });
            if self.is_online() {
                let (request, attempt) = self.request_ids();
                self.state = State::Loading { request, attempt, retry: false, page: next, force: false, attempts: 0 };
                self.phase = Phase::Loading;
                return vec![Effect::Request { request, attempt, page: next }, Effect::WindowState];
            }
            self.state = State::Waiting { force: false, page: next };
            return vec![Effect::WindowState];
        }
        settle(self, &|want| Some(match want {
            Want::All | Want::First | Want::Next(_) => Result3::Loaded,
        }));
        if !self.reread_due {
            self.state = State::Idle;
            self.fresh = true;
            return vec![Effect::WindowState];
        }
        self.reread_due = false;
        if !self.watched() {
            self.state = State::Idle;
            self.fresh = false;
            return vec![Effect::WindowState];
        }
        self.begin(1, false, true)
    }

    fn rejected(&mut self, reason: Reject) -> Vec<Effect> {
        match reason {
            Reject::StaleGeneration => {
                if self.watched() {
                    self.begin(1, false, false)
                } else {
                    self.state = State::Idle;
                    self.fresh = false;
                    Vec::new()
                }
            }
            Reject::NoSession | Reject::Closed => {
                self.answer_all(Result3::Interrupted);
                self.state = State::Ended;
                Vec::new()
            }
            Reject::Storage | Reject::Invalid => self.fail(false),
        }
    }

    fn waiting(&mut self, force: bool, page: u32, input: Input) -> Vec<Effect> {
        match input {
            Input::Online(true) => {
                let (request, attempt) = self.request_ids();
                self.state = State::Loading { request, attempt, retry: false, page, force, attempts: 0 };
                self.phase = Phase::Loading;
                vec![Effect::Request { request, attempt, page }, Effect::WindowState]
            }
            Input::InterestPlus { id } => {
                self.interest.insert(id);
                vec![Effect::WindowState]
            }
            Input::Load { id, goal, force: wider } => {
                self.join_load(id, goal, page);
                self.state = State::Waiting { force: force || (wider && goal != Goal::Next), page };
                Vec::new()
            }
            Input::InterestMinus { id } => {
                self.interest.remove(&id);
                if self.interest.is_empty() {
                    self.settled = false;
                }
                self.waiters.remove(&id);
                if !self.watched() {
                    self.state = State::Idle;
                    self.fresh = false;
                }
                Vec::new()
            }
            _ => Vec::new(),
        }
    }

    /// Rows `win_epoch_ended` and `win_closing`.
    fn end(&mut self, closing: bool) -> Vec<Effect> {
        let was = self.state;
        self.state = State::Ended;
        self.answer_all(Result3::Interrupted);
        let mut effects = Vec::new();
        if let State::Loading { request, attempt, retry: false, .. } = was { effects.push(Effect::Cancel { request, attempt }); }
        if closing { effects.push(Effect::Drained); }
        effects
    }
}
