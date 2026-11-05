//! `specs/machines/window.md` as code: one match arm per table row. No library code.

use std::collections::BTreeSet;

use super::harness::Phase;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Result3 {
    Loaded,
    Failed,
    Interrupted,
}

/// Failure classes of a transport error: the first two are temporary and retried up to `retry_limit`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Failure {
    Network,
    Server,
    Permanent,
}

impl Failure {
    /// `design.md` 1a: attempts of a window per class.
    pub(super) fn retry_limit(self) -> u32 {
        match self {
            Failure::Network => 3,
            Failure::Server => 2,
            Failure::Permanent => 0,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Reject {
    StaleGeneration,
    NoSession,
    Storage,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Input {
    InterestPlus { id: u32, force: bool, all: bool },
    InterestMinus { id: u32 },
    LoadMore { id: u32 },
    Response { request: u64 },
    TransportError { request: u64, failure: Failure },
    Dropped { request: u64 },
    Committed { request: u64 },
    Rejected { request: u64, reason: Reject },
    Stale,
    Online(bool),
    EpochEnded,
    ModelsReset { own_model: bool },
    Closing,
    RetryDue,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Effect {
    Request { request: u64, page: u32 },
    Cancel { request: u64 },
    Commit { request: u64 },
    ScheduleRetry,
    WindowState,
    Drained,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum State {
    Idle { fresh: bool, last_error: Option<bool> },
    Loading { request: u64, page: u32, force: bool, again: bool, more: bool, attempts: u32 },
    Landing { request: u64, page: u32, again: bool, more: bool },
    Waiting { force: bool, page: u32 },
    Ended,
}

/// What the environment of the key is: network, whether a next page exists, whether rows are in the base, and the interest set.
pub(super) struct Window {
    pub(super) state: State,
    pub(super) online: bool,
    /// A next page exists (the cursor of the last response).
    pub(super) cursor: bool,
    pub(super) has_rows: bool,
    /// New interest ids join as streams: they stay after their outcome.
    pub(super) stream_mode: bool,
    pub(super) interest: BTreeSet<u32>,
    pub(super) all: BTreeSet<u32>,
    streams: BTreeSet<u32>,
    /// The page the last landing wrote.
    pub(super) page: u32,
    pub(super) phase: Phase,
    pub(super) outcomes: Vec<(u32, Result3)>,
    next_request: u64,
}

impl Window {
    pub(super) fn new() -> Self {
        Self { state: State::Idle { fresh: false, last_error: None }, online: true, cursor: false, has_rows: false, stream_mode: false, interest: BTreeSet::new(), all: BTreeSet::new(), streams: BTreeSet::new(), page: 0, phase: Phase::Idle, outcomes: Vec::new(), next_request: 1 }
    }

    pub(super) fn idle_fresh() -> Self {
        let mut window = Self::new();
        window.state = State::Idle { fresh: true, last_error: None };
        window.phase = Phase::Loaded;
        window.has_rows = true;
        window.page = 1;
        window
    }

    fn request(&mut self) -> u64 {
        let request = self.next_request;
        self.next_request += 1;
        request
    }

    fn join(&mut self, id: u32, all: bool) {
        self.interest.insert(id);
        if self.stream_mode { self.streams.insert(id); }
        if all { self.all.insert(id); }
    }

    fn leave(&mut self, id: u32) {
        self.interest.remove(&id);
        self.streams.remove(&id);
        self.all.remove(&id);
    }

    fn answer_one(&mut self, id: u32, result: Result3) {
        self.outcomes.push((id, result));
        if !self.streams.contains(&id) { self.leave(id); }
    }

    fn answer_ids(&mut self, ids: Vec<u32>, result: Result3) {
        for id in ids { self.answer_one(id, result); }
    }

    fn answer(&mut self, result: Result3) { self.answer_ids(self.interest.iter().copied().collect(), result); }

    pub(super) fn take_outcomes(&mut self) -> Vec<(u32, Result3)> { std::mem::take(&mut self.outcomes) }

    /// A load of `page`: a request online, `Waiting` offline. `announce` adds the window state to the effects.
    fn load(&mut self, page: u32, force: bool, announce: bool) -> Vec<Effect> {
        let mut effects = Vec::new();
        if self.online {
            let request = self.request();
            self.state = State::Loading { request, page, force, again: false, more: false, attempts: 0 };
            self.phase = Phase::Loading;
            effects.push(Effect::Request { request, page });
        } else {
            self.state = State::Waiting { force, page };
        }
        if announce { effects.push(Effect::WindowState); }
        effects
    }

    pub(super) fn step(&mut self, input: Input) -> Vec<Effect> {
        match (&self.state, &input) {
            (State::Ended, Input::Closing) => return vec![Effect::Drained],
            (State::Ended, _) => return Vec::new(),
            (_, Input::EpochEnded) => return self.end(false),
            (_, Input::Closing) => return self.end(true),
            _ => {}
        }
        if let Input::Online(on) = input { self.online = on; }
        let state = std::mem::replace(&mut self.state, State::Ended);
        match state {
            State::Idle { fresh, last_error } => self.idle(fresh, last_error, input),
            State::Loading { request, page, force, again, more, attempts } => self.loading(request, page, force, again, more, attempts, input),
            State::Landing { request, page, again, more } => self.landing(request, page, again, more, input),
            State::Waiting { force, page } => self.waiting(force, page, input),
            State::Ended => Vec::new(),
        }
    }

    fn idle(&mut self, fresh: bool, last_error: Option<bool>, input: Input) -> Vec<Effect> {
        self.state = State::Idle { fresh, last_error };
        match input {
            Input::InterestPlus { id, force, all } => {
                self.join(id, all);
                if fresh && !force && !(all && self.cursor) {
                    self.phase = Phase::Loaded;
                    self.answer_one(id, Result3::Loaded);
                    vec![Effect::WindowState]
                } else if fresh && !force {
                    self.load(self.page + 1, false, true)
                } else {
                    self.load(1, force, true)
                }
            }
            Input::InterestMinus { id } => { self.leave(id); Vec::new() }
            Input::LoadMore { id } => {
                if self.cursor {
                    self.join(id, false);
                    self.load(self.page + 1, false, true)
                } else {
                    self.answer_one(id, Result3::Loaded);
                    Vec::new()
                }
            }
            Input::Stale => {
                if self.interest.is_empty() {
                    self.state = State::Idle { fresh: false, last_error };
                    Vec::new()
                } else {
                    self.load(1, false, self.online)
                }
            }
            Input::Online(true) if last_error == Some(true) && !self.interest.is_empty() => self.load(1, false, true),
            Input::ModelsReset { own_model: true } => {
                self.state = State::Idle { fresh: false, last_error };
                if self.interest.is_empty() { Vec::new() } else { self.load(1, false, self.online) }
            }
            _ => Vec::new(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn loading(&mut self, request: u64, page: u32, force: bool, again: bool, more: bool, attempts: u32, input: Input) -> Vec<Effect> {
        self.state = State::Loading { request, page, force, again, more, attempts };
        match input {
            Input::InterestPlus { id, force: wider, all } => {
                self.join(id, all);
                self.state = State::Loading { request, page, force: force || wider, again, more, attempts };
                Vec::new()
            }
            Input::InterestMinus { id } => {
                self.leave(id);
                if self.interest.is_empty() {
                    self.state = State::Idle { fresh: false, last_error: None };
                    self.phase = if self.has_rows { Phase::Loaded } else { Phase::Idle };
                    vec![Effect::Cancel { request }]
                } else {
                    Vec::new()
                }
            }
            Input::LoadMore { id } => {
                self.join(id, false);
                if page == 1 { self.state = State::Loading { request, page, force, again: true, more: true, attempts }; }
                Vec::new()
            }
            Input::Response { request: got } if got == request => {
                self.state = State::Landing { request, page, again, more };
                vec![Effect::Commit { request }]
            }
            Input::TransportError { request: got, failure } if got == request => self.failed(request, page, force, again, more, attempts, failure),
            Input::Dropped { request: got } if got == request => self.failed(request, page, force, again, more, attempts, Failure::Network),
            Input::RetryDue => vec![Effect::Request { request, page }],
            Input::Online(false) => {
                self.state = State::Waiting { force, page };
                vec![Effect::Cancel { request }]
            }
            Input::Stale => { self.state = State::Loading { request, page, force, again: true, more, attempts }; Vec::new() }
            Input::ModelsReset { own_model: true } => {
                let new = self.request();
                self.state = State::Loading { request: new, page, force, again, more, attempts: 0 };
                vec![Effect::Cancel { request }, Effect::Request { request: new, page }]
            }
            _ => Vec::new(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn failed(&mut self, request: u64, page: u32, force: bool, again: bool, more: bool, attempts: u32, failure: Failure) -> Vec<Effect> {
        let temporary = failure != Failure::Permanent;
        if temporary && attempts + 1 < failure.retry_limit() {
            self.state = State::Loading { request, page, force, again, more, attempts: attempts + 1 };
            vec![Effect::ScheduleRetry]
        } else {
            self.state = State::Idle { fresh: false, last_error: Some(failure == Failure::Network) };
            self.phase = if self.has_rows { Phase::Loaded } else { Phase::Failed };
            self.answer(Result3::Failed);
            vec![Effect::WindowState]
        }
    }

    fn landing(&mut self, request: u64, page: u32, again: bool, more: bool, input: Input) -> Vec<Effect> {
        self.state = State::Landing { request, page, again, more };
        match input {
            Input::Committed { request: got } if got == request => {
                self.has_rows = true;
                self.page = page;
                let watched = !self.interest.is_empty();
                let waiting_all = self.interest.iter().any(|id| self.all.contains(id)) && self.cursor;
                let calls: Vec<u32> = self.interest.iter().copied().filter(|id| !(waiting_all && self.all.contains(id))).collect();
                self.answer_ids(calls, Result3::Loaded);
                if waiting_all {
                    let next = page + 1;
                    return if self.online {
                        let new = self.request();
                        self.state = State::Loading { request: new, page: next, force: false, again, more, attempts: 0 };
                        self.phase = Phase::Loading;
                        vec![Effect::Request { request: new, page: next }, Effect::WindowState]
                    } else {
                        self.state = State::Waiting { force: false, page: next };
                        self.phase = Phase::Loaded;
                        vec![Effect::WindowState]
                    };
                }
                self.phase = Phase::Loaded;
                if !again {
                    self.state = State::Idle { fresh: true, last_error: None };
                    return vec![Effect::WindowState];
                }
                if !watched {
                    self.state = State::Idle { fresh: false, last_error: None };
                    return vec![Effect::WindowState];
                }
                let next = if more && self.cursor { page + 1 } else { 1 };
                self.load(next, false, true)
            }
            Input::Rejected { request: got, reason } if got == request => match reason {
                Reject::StaleGeneration => {
                    if self.interest.is_empty() {
                        self.state = State::Idle { fresh: false, last_error: None };
                        Vec::new()
                    } else {
                        self.load(1, false, false)
                    }
                }
                Reject::NoSession => { self.answer(Result3::Interrupted); self.state = State::Ended; Vec::new() }
                Reject::Storage => {
                    self.state = State::Idle { fresh: false, last_error: Some(false) };
                    self.phase = if self.has_rows { Phase::Loaded } else { Phase::Failed };
                    self.answer(Result3::Failed);
                    vec![Effect::WindowState]
                }
            },
            Input::InterestPlus { id, all, .. } => { self.join(id, all); Vec::new() }
            Input::LoadMore { id } => {
                self.join(id, false);
                self.state = State::Landing { request, page, again: true, more: true };
                Vec::new()
            }
            Input::Stale => { self.state = State::Landing { request, page, again: true, more }; Vec::new() }
            _ => Vec::new(),
        }
    }

    fn waiting(&mut self, force: bool, page: u32, input: Input) -> Vec<Effect> {
        self.state = State::Waiting { force, page };
        match input {
            Input::Online(true) => self.load(page, force, true),
            Input::InterestPlus { id, force: wider, all } => {
                self.join(id, all);
                self.state = State::Waiting { force: force || wider, page };
                Vec::new()
            }
            Input::LoadMore { id } => { self.join(id, false); Vec::new() }
            Input::InterestMinus { id } => {
                self.leave(id);
                if self.interest.is_empty() { self.state = State::Idle { fresh: false, last_error: None }; }
                Vec::new()
            }
            _ => Vec::new(),
        }
    }

    /// Rows `win_idle_stale_watched` and `win_idle_stale_unwatched`: the row depends on whether the key has interest.
    pub(super) fn stale_idle(&mut self, watched: bool) -> Vec<Effect> {
        if watched { self.interest.insert(0); }
        self.step(Input::Stale)
    }

    /// Row `win_idle_online_retry`: `Online(true)` in `Idle` after a network error, with interest or without.
    pub(super) fn online_idle(&mut self, has_interest: bool) -> Vec<Effect> {
        if has_interest { self.interest.insert(0); }
        self.step(Input::Online(true))
    }

    /// Rows `win_epoch_ended` and `win_closing`.
    pub(super) fn end(&mut self, closing: bool) -> Vec<Effect> {
        let state = std::mem::replace(&mut self.state, State::Ended);
        let mut effects = Vec::new();
        match state {
            State::Loading { request, .. } => { self.answer(Result3::Interrupted); effects.push(Effect::Cancel { request }); }
            State::Landing { .. } | State::Waiting { .. } => self.answer(Result3::Interrupted),
            State::Idle { .. } | State::Ended => {}
        }
        if closing { effects.push(Effect::Drained); }
        effects
    }

    /// Row `win_ended`: a new interest or `LoadMore` on an ended key.
    pub(super) fn ended_interest(&mut self, id: u32) {
        assert_eq!(self.state, State::Ended);
        self.outcomes.push((id, Result3::Interrupted));
    }
}
