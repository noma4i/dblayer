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
    InterestPlus { id: u32, force: bool },
    InterestMinus { id: u32 },
    LoadMore { id: u32 },
    Response { request: u64 },
    TransportError { request: u64, failure: Failure },
    Committed { request: u64 },
    Rejected { request: u64, reason: Reject },
    Stale { newer_than_issue: bool },
    Online(bool),
    EpochStarted,
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
    Idle { fresh: bool, stale: bool, last_error: Option<bool> },
    Loading { request: u64, page: u32, interest: BTreeSet<u32>, force: bool, again: bool, attempts: u32 },
    Landing { request: u64, interest: BTreeSet<u32>, again: bool },
    Waiting { interest: BTreeSet<u32>, force: bool },
    Ended,
}

/// What the environment of the key is: session, network, whether a next page exists, whether rows are in the base.
pub(super) struct Window {
    pub(super) state: State,
    pub(super) online: bool,
    pub(super) session: bool,
    pub(super) cursor: bool,
    pub(super) has_rows: bool,
    /// Whether the key has interest while it is `Idle` (the state itself keeps none).
    pub(super) watched: bool,
    pub(super) phase: Phase,
    pub(super) outcomes: Vec<(u32, Result3)>,
    next_request: u64,
}

impl Window {
    pub(super) fn new() -> Self {
        Self { state: State::Idle { fresh: false, stale: false, last_error: None }, online: true, session: true, cursor: false, has_rows: false, watched: false, phase: Phase::Idle, outcomes: Vec::new(), next_request: 1 }
    }

    pub(super) fn idle_fresh() -> Self {
        let mut window = Self::new();
        window.state = State::Idle { fresh: true, stale: false, last_error: None };
        window.phase = Phase::Loaded;
        window.has_rows = true;
        window
    }

    fn request(&mut self) -> u64 {
        let request = self.next_request;
        self.next_request += 1;
        request
    }

    fn answer(&mut self, ids: &BTreeSet<u32>, result: Result3) {
        for id in ids { self.outcomes.push((*id, result)); }
    }

    pub(super) fn take_outcomes(&mut self) -> Vec<(u32, Result3)> { std::mem::take(&mut self.outcomes) }

    pub(super) fn step(&mut self, input: Input) -> Vec<Effect> {
        match input {
            Input::EpochEnded if self.state != State::Ended => return self.end(false),
            Input::Closing if self.state != State::Ended => return self.end(true),
            _ => {}
        }
        let state = std::mem::replace(&mut self.state, State::Ended);
        let can_run = self.online && self.session;
        match (state, input) {
            (State::Idle { fresh, stale, last_error }, Input::InterestPlus { id, force }) => {
                if fresh && !force && !stale {
                    self.state = State::Idle { fresh, stale, last_error };
                    self.phase = Phase::Loaded;
                    self.outcomes.push((id, Result3::Loaded));
                    vec![Effect::WindowState]
                } else if can_run {
                    let request = self.request();
                    self.state = State::Loading { request, page: 1, interest: [id].into(), force, again: false, attempts: 0 };
                    self.phase = Phase::Loading;
                    vec![Effect::Request { request, page: 1 }, Effect::WindowState]
                } else {
                    self.state = State::Waiting { interest: [id].into(), force };
                    vec![Effect::WindowState]
                }
            }
            (State::Idle { fresh, stale, last_error }, Input::InterestMinus { .. }) => { self.state = State::Idle { fresh, stale, last_error }; Vec::new() }
            (State::Idle { fresh, stale, last_error }, Input::LoadMore { id }) => {
                if self.cursor && can_run {
                    let request = self.request();
                    self.state = State::Loading { request, page: 2, interest: [id].into(), force: false, again: false, attempts: 0 };
                    self.phase = Phase::Loading;
                    vec![Effect::Request { request, page: 2 }]
                } else {
                    self.state = State::Idle { fresh, stale, last_error };
                    self.outcomes.push((id, Result3::Loaded));
                    Vec::new()
                }
            }
            (State::Idle { fresh: _, stale: _, last_error }, Input::Stale { .. }) => {
                if self.watched && self.online {
                    self.load_for_watchers()
                } else {
                    self.state = State::Idle { fresh: false, stale: true, last_error };
                    Vec::new()
                }
            }
            (State::Idle { fresh, stale, last_error }, Input::Online(on)) => {
                self.online = on;
                if on && last_error == Some(true) && self.watched {
                    self.load_for_watchers()
                } else {
                    self.state = State::Idle { fresh, stale, last_error };
                    Vec::new()
                }
            }
            (State::Idle { fresh, stale, last_error }, Input::ModelsReset { own_model }) => {
                if own_model && self.watched {
                    self.state = State::Idle { fresh: false, stale, last_error };
                    self.load_for_watchers()
                } else {
                    self.state = if own_model { State::Idle { fresh: false, stale, last_error } } else { State::Idle { fresh, stale, last_error } };
                    Vec::new()
                }
            }
            (State::Loading { request, page, mut interest, force, again, attempts }, Input::InterestPlus { id, force: more }) => {
                interest.insert(id);
                self.state = State::Loading { request, page, interest, force: force || more, again, attempts };
                Vec::new()
            }
            (State::Loading { request, page, mut interest, force, again, attempts }, Input::InterestMinus { id }) => {
                interest.remove(&id);
                if interest.is_empty() {
                    self.state = State::Idle { fresh: false, stale: false, last_error: None };
                    self.phase = if self.has_rows { Phase::Loaded } else { Phase::Idle };
                    vec![Effect::Cancel { request }]
                } else {
                    self.state = State::Loading { request, page, interest, force, again, attempts };
                    Vec::new()
                }
            }
            (State::Loading { request, page, interest, force, again, attempts }, Input::Response { request: got }) => {
                if got == request {
                    self.state = State::Landing { request, interest, again };
                    vec![Effect::Commit { request }]
                } else {
                    self.state = State::Loading { request, page, interest, force, again, attempts };
                    Vec::new()
                }
            }
            (State::Loading { request, page, interest, force, again, attempts }, Input::TransportError { request: got, failure }) => {
                if got != request {
                    self.state = State::Loading { request, page, interest, force, again, attempts };
                    Vec::new()
                } else if failure != Failure::Permanent && self.online && attempts + 1 < failure.retry_limit() {
                    self.state = State::Loading { request, page, interest, force, again, attempts: attempts + 1 };
                    vec![Effect::ScheduleRetry]
                } else {
                    self.state = State::Idle { fresh: false, stale: false, last_error: Some(failure == Failure::Network) };
                    self.phase = if self.has_rows { Phase::Loaded } else { Phase::Failed };
                    self.answer(&interest, Result3::Failed);
                    vec![Effect::WindowState]
                }
            }
            (State::Loading { request, page, interest, force, again, attempts }, Input::RetryDue) => {
                self.state = State::Loading { request, page, interest, force, again, attempts };
                vec![Effect::Request { request, page }]
            }
            (State::Loading { request, interest, force, .. }, Input::Online(false)) => {
                self.online = false;
                self.state = State::Waiting { interest, force };
                self.phase = Phase::Loading;
                vec![Effect::Cancel { request }]
            }
            (State::Loading { request, page, interest, force, again, attempts }, Input::Online(true)) => {
                self.online = true;
                self.state = State::Loading { request, page, interest, force, again, attempts };
                Vec::new()
            }
            (State::Loading { request, page, interest, force, again, attempts }, Input::Stale { newer_than_issue }) => {
                self.state = State::Loading { request, page, interest, force, again: again || newer_than_issue, attempts };
                Vec::new()
            }
            (State::Loading { request, page, interest, force, again, attempts }, Input::ModelsReset { own_model }) => {
                if own_model {
                    let new = self.request();
                    self.state = State::Loading { request: new, page, interest, force, again, attempts: 0 };
                    vec![Effect::Cancel { request }, Effect::Request { request: new, page }]
                } else {
                    self.state = State::Loading { request, page, interest, force, again, attempts };
                    Vec::new()
                }
            }
            (State::Landing { request, interest, again }, Input::Committed { request: got }) if got == request => {
                self.has_rows = true;
                self.answer(&interest, Result3::Loaded);
                if again {
                    let new = self.request();
                    self.state = State::Loading { request: new, page: 1, interest, force: false, again: false, attempts: 0 };
                    self.phase = Phase::Loading;
                    vec![Effect::Request { request: new, page: 1 }, Effect::WindowState]
                } else {
                    self.state = State::Idle { fresh: true, stale: false, last_error: None };
                    self.phase = Phase::Loaded;
                    vec![Effect::WindowState]
                }
            }
            (State::Landing { request, interest, .. }, Input::Rejected { request: got, reason }) if got == request => match reason {
                Reject::StaleGeneration => {
                    let new = self.request();
                    self.state = State::Loading { request: new, page: 1, interest, force: false, again: false, attempts: 0 };
                    vec![Effect::Request { request: new, page: 1 }]
                }
                Reject::NoSession => { self.answer(&interest, Result3::Interrupted); self.state = State::Ended; Vec::new() }
                Reject::Storage => {
                    self.state = State::Idle { fresh: false, stale: false, last_error: Some(false) };
                    self.answer(&interest, Result3::Failed);
                    vec![Effect::WindowState]
                }
            },
            (State::Landing { request, mut interest, again }, Input::InterestPlus { id, .. }) => {
                interest.insert(id);
                self.state = State::Landing { request, interest, again };
                Vec::new()
            }
            (State::Landing { request, interest, again }, Input::Stale { newer_than_issue }) => {
                self.state = State::Landing { request, interest, again: again || newer_than_issue };
                Vec::new()
            }
            (State::Waiting { interest, force }, Input::Online(true) | Input::EpochStarted) => {
                self.online = true;
                self.session = true;
                let request = self.request();
                self.state = State::Loading { request, page: 1, interest, force, again: false, attempts: 0 };
                self.phase = Phase::Loading;
                vec![Effect::Request { request, page: 1 }]
            }
            (State::Waiting { mut interest, force }, Input::InterestPlus { id, force: more }) => {
                interest.insert(id);
                self.state = State::Waiting { interest, force: force || more };
                Vec::new()
            }
            (State::Waiting { mut interest, force }, Input::InterestMinus { id }) => {
                interest.remove(&id);
                self.state = if interest.is_empty() { State::Idle { fresh: false, stale: false, last_error: None } } else { State::Waiting { interest, force } };
                Vec::new()
            }
            (state, _) => {
                // Every other pair is a cell that changes nothing: a stray answer, a wait in `Landing`, `Ended`.
                self.state = state;
                Vec::new()
            }
        }
    }

    fn load_for_watchers(&mut self) -> Vec<Effect> {
        let request = self.request();
        self.state = State::Loading { request, page: 1, interest: BTreeSet::new(), force: false, again: false, attempts: 0 };
        self.phase = Phase::Loading;
        vec![Effect::Request { request, page: 1 }]
    }

    /// Rows `win_idle_stale_watched` and `win_idle_stale_unwatched`: the row depends on whether the key has interest.
    pub(super) fn stale_idle(&mut self, watched: bool) -> Vec<Effect> {
        self.watched = watched;
        self.step(Input::Stale { newer_than_issue: true })
    }

    /// Row `win_idle_online_retry`: Online(true) in Idle after a network error with interest.
    pub(super) fn online_idle(&mut self, has_interest: bool) -> Vec<Effect> {
        self.watched = has_interest;
        self.step(Input::Online(true))
    }

    /// Rows `win_epoch_ended` and `win_closing`.
    pub(super) fn end(&mut self, closing: bool) -> Vec<Effect> {
        let state = std::mem::replace(&mut self.state, State::Ended);
        let mut effects = Vec::new();
        match state {
            State::Loading { request, interest, .. } => { self.answer(&interest, Result3::Interrupted); effects.push(Effect::Cancel { request }); }
            State::Landing { interest, .. } | State::Waiting { interest, .. } => self.answer(&interest, Result3::Interrupted),
            State::Idle { .. } | State::Ended => {}
        }
        if closing { effects.push(Effect::Drained); }
        effects
    }

    /// Row `win_ended`: a new interest on an ended key.
    pub(super) fn ended_interest(&mut self, id: u32) {
        assert_eq!(self.state, State::Ended);
        self.outcomes.push((id, Result3::Interrupted));
    }
}
