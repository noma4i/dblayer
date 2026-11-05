//! `specs/machines/window.md` as code: one match arm per table row. No library code.

use std::collections::BTreeSet;

use super::harness::Phase;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Result3 {
    Loaded,
    Failed,
    Interrupted,
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
    TransportError { request: u64, temporary: bool },
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
    Loading { request: u64, page: u32, interest: BTreeSet<u32>, force: bool, again: bool },
    Landing { request: u64, interest: BTreeSet<u32> },
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
    pub(super) phase: Phase,
    pub(super) outcomes: Vec<(u32, Result3)>,
    next_request: u64,
}

impl Window {
    pub(super) fn new() -> Self {
        Self { state: State::Idle { fresh: false, stale: false, last_error: None }, online: true, session: true, cursor: false, has_rows: false, phase: Phase::Idle, outcomes: Vec::new(), next_request: 1 }
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
                    self.state = State::Loading { request, page: 1, interest: [id].into(), force, again: false };
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
                    self.state = State::Loading { request, page: 2, interest: [id].into(), force: false, again: false };
                    self.phase = Phase::Loading;
                    vec![Effect::Request { request, page: 2 }]
                } else {
                    self.state = State::Idle { fresh, stale, last_error };
                    self.outcomes.push((id, Result3::Loaded));
                    Vec::new()
                }
            }
            (State::Idle { .. }, Input::Stale { .. }) => panic!("use stale_idle: the row depends on the interest"),
            (State::Idle { fresh, stale, last_error }, Input::Online(on)) => {
                self.online = on;
                self.state = State::Idle { fresh, stale, last_error };
                Vec::new()
            }
            (State::Idle { fresh, stale, last_error }, Input::ModelsReset { own_model }) => {
                self.state = if own_model { State::Idle { fresh: false, stale, last_error } } else { State::Idle { fresh, stale, last_error } };
                Vec::new()
            }
            (state @ State::Idle { .. }, Input::Response { .. } | Input::TransportError { .. } | Input::Committed { .. } | Input::Rejected { .. }) => { self.state = state; Vec::new() }
            (State::Loading { request, page, mut interest, force, again }, Input::InterestPlus { id, force: more }) => {
                interest.insert(id);
                self.state = State::Loading { request, page, interest, force: force || more, again };
                Vec::new()
            }
            (State::Loading { request, page, mut interest, force, again }, Input::InterestMinus { id }) => {
                interest.remove(&id);
                if interest.is_empty() {
                    self.state = State::Idle { fresh: false, stale: false, last_error: None };
                    self.phase = if self.has_rows { Phase::Loaded } else { Phase::Idle };
                    vec![Effect::Cancel { request }]
                } else {
                    self.state = State::Loading { request, page, interest, force, again };
                    Vec::new()
                }
            }
            (State::Loading { request, page, interest, force, again }, Input::Response { request: got }) => {
                if got == request {
                    self.state = State::Landing { request, interest };
                    vec![Effect::Commit { request }]
                } else {
                    self.state = State::Loading { request, page, interest, force, again };
                    Vec::new()
                }
            }
            (State::Loading { request, page, interest, force, again }, Input::TransportError { request: got, temporary }) => {
                if got != request {
                    self.state = State::Loading { request, page, interest, force, again };
                    Vec::new()
                } else if temporary && self.online {
                    self.state = State::Loading { request, page, interest, force, again };
                    vec![Effect::ScheduleRetry]
                } else {
                    self.state = State::Idle { fresh: false, stale: false, last_error: Some(true) };
                    self.phase = if self.has_rows { Phase::Loaded } else { Phase::Failed };
                    self.answer(&interest, Result3::Failed);
                    vec![Effect::WindowState]
                }
            }
            (State::Loading { request, page, interest, force, again }, Input::RetryDue) => {
                self.state = State::Loading { request, page, interest, force, again };
                vec![Effect::Request { request, page }]
            }
            (State::Loading { request, interest, force, .. }, Input::Online(false)) => {
                self.online = false;
                self.state = State::Waiting { interest, force };
                self.phase = Phase::Loading;
                vec![Effect::Cancel { request }]
            }
            (State::Loading { request, page, interest, force, again }, Input::Online(true)) => {
                self.online = true;
                self.state = State::Loading { request, page, interest, force, again };
                Vec::new()
            }
            (State::Loading { request, page, interest, force, .. }, Input::Stale { newer_than_issue }) => {
                self.state = State::Loading { request, page, interest, force, again: newer_than_issue };
                Vec::new()
            }
            (State::Loading { request, page, interest, force, again }, Input::ModelsReset { own_model }) => {
                if own_model {
                    let new = self.request();
                    self.state = State::Loading { request: new, page, interest, force, again };
                    vec![Effect::Cancel { request }, Effect::Request { request: new, page }]
                } else {
                    self.state = State::Loading { request, page, interest, force, again };
                    Vec::new()
                }
            }
            (State::Landing { request, interest }, Input::Committed { request: got }) if got == request => {
                self.state = State::Idle { fresh: true, stale: false, last_error: None };
                self.phase = Phase::Loaded;
                self.has_rows = true;
                self.answer(&interest, Result3::Loaded);
                vec![Effect::WindowState]
            }
            (State::Landing { request, interest }, Input::Rejected { request: got, reason }) if got == request => match reason {
                Reject::StaleGeneration => {
                    let new = self.request();
                    self.state = State::Loading { request: new, page: 1, interest, force: false, again: false };
                    vec![Effect::Request { request: new, page: 1 }]
                }
                Reject::NoSession => { self.answer(&interest, Result3::Interrupted); self.state = State::Ended; Vec::new() }
                Reject::Storage => {
                    self.state = State::Idle { fresh: false, stale: false, last_error: Some(false) };
                    self.answer(&interest, Result3::Failed);
                    vec![Effect::WindowState]
                }
            },
            (State::Landing { request, mut interest }, Input::InterestPlus { id, .. }) => {
                interest.insert(id);
                self.state = State::Landing { request, interest };
                Vec::new()
            }
            (State::Landing { request, interest }, Input::InterestMinus { .. } | Input::Stale { .. } | Input::Online(_) | Input::ModelsReset { .. }) => {
                self.state = State::Landing { request, interest };
                Vec::new()
            }
            (State::Waiting { interest, force }, Input::Online(true) | Input::EpochStarted) => {
                self.online = true;
                self.session = true;
                let request = self.request();
                self.state = State::Loading { request, page: 1, interest, force, again: false };
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
            (state, input) => {
                self.state = state;
                panic!("a row of window.md is missing: {:?} x {input:?}", self.state)
            }
        }
    }

    /// Rows `win_idle_stale_watched` and `win_idle_stale_unwatched`: the row depends on whether the key has interest.
    pub(super) fn stale_idle(&mut self, watched: bool) -> Vec<Effect> {
        let State::Idle { .. } = self.state else { panic!("not idle") };
        if watched && self.online {
            let request = self.request();
            self.state = State::Loading { request, page: 1, interest: BTreeSet::new(), force: false, again: false };
            self.phase = Phase::Loading;
            vec![Effect::Request { request, page: 1 }]
        } else {
            self.state = State::Idle { fresh: false, stale: true, last_error: None };
            Vec::new()
        }
    }

    /// Row `win_idle_online_retry`: Online(true) in Idle after a network error with interest.
    pub(super) fn online_idle(&mut self, has_interest: bool) -> Vec<Effect> {
        let State::Idle { fresh, stale, last_error } = self.state.clone() else { panic!("not idle") };
        self.online = true;
        if last_error == Some(true) && has_interest {
            let request = self.request();
            self.state = State::Loading { request, page: 1, interest: BTreeSet::new(), force: false, again: false };
            self.phase = Phase::Loading;
            vec![Effect::Request { request, page: 1 }]
        } else {
            self.state = State::Idle { fresh, stale, last_error };
            Vec::new()
        }
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
