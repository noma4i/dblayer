//! The `window` machine as code: one match arm per table row. No library code.

use std::collections::{BTreeMap, BTreeSet};


mod ref_window_types;
pub(super) use ref_window_types::*;

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
            members_complete: false,
            cursor_reset_used: false,
            replace_all: false,
            has_rows: false,
            landed_revision: 0,
            foreground_seen: 0,
            phase: Phase::Idle,
            settled: false,
            outcomes: Vec::new(),
            now: 1_000_000,
            landed_at: 0,
            fresh_until: 0,
            answered_at: 0,
            lifetime: (300_000, 60_000),
            saving: false,
            unread: true,
            held: Vec::new(),
            next_request: 1,
            step: 0,
            next_attempt: 1,
            media: false,
            marker: None,
            members: 0,
            chain: None,
            stale_marked: false,
            stale_owed: false,
            online_seq: 0,
            load_seq: 0,
            landing_marker: None,
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

    /// Loaded, not marked stale and `landed_at <= now < fresh_until` by the `now` of the input.
    fn fresh_at(&self) -> bool { self.fresh && self.landed_at <= self.now && self.now < self.fresh_until }

    fn preload_only(&self) -> bool { self.waiters.is_empty() && self.interest.len() == 1 && self.interest.contains(&PRELOAD) }

    fn deferred(&self) -> bool { self.saving && self.preload_only() }

    fn watched(&self) -> bool { !self.interest.is_empty() || !self.waiters.is_empty() }

    fn request_ids(&mut self) -> (u64, u32) {
        let request = self.next_request;
        self.next_request += 1;
        self.step = request;
        self.load_seq = self.online_seq;
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
        if self.is_online() && !self.deferred() {
            let (request, attempt) = self.request_ids();
            self.state = State::Loading { request, attempt, retry: false, page, force, attempts: 0 };
            self.phase = Phase::Loading;
            effects.push(self.issue_effect(request, attempt, page));
        } else {
            self.state = State::Waiting { force, page };
        }
        if announce { effects.push(Effect::WindowState); }
        effects
    }

    /// The goal a waiter gets when it joins while `page` is the page in flight or about to be asked.
    fn after(page: u32) -> Want { Want::Next(page + 1) }

    pub(super) fn step(&mut self, input: Input) -> Vec<Effect> {
        if self.unread && self.state == State::Idle && matches!(input, Input::InterestPlus { .. } | Input::InterestPassive { .. } | Input::Load { .. }) {
            self.unread = false;
            if !matches!(input, Input::InterestPlus { id: PRELOAD }) {
                self.state = State::Opening;
                return self.hold(input, vec![Effect::ReadWindow]);
            }
        }
        if let Input::Online(true) = input {
            self.online_seq += 1;
        }
        let input = match input {
            Input::StaleGap if matches!(self.state, State::Idle | State::Loading { .. } | State::Landing { .. }) && self.online_seq > 0 && self.load_seq >= self.online_seq => return Vec::new(),
            Input::StaleGap => Input::Stale { in_keys: true },
            other => other,
        };
        if self.state == State::Opening {
            return self.opening(input);
        }
        if let Input::PowerSaving(on) = input {
            self.saving = on;
        }
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
            State::Loading { request, attempt, retry, page, force, attempts } => self.loading((request, attempt, retry), (page, force, attempts), input),
            State::Landing { request, page, cursor, rows } => self.landing(request, page, cursor, rows, input),
            State::Waiting { force, page } => self.waiting(force, page, input),
            State::Opening | State::Ended => Vec::new(),
        }
    }

    /// A held input changes the interest and the waiters at once, the machine runs it after the read.
    fn hold(&mut self, input: Input, effects: Vec<Effect>) -> Vec<Effect> {
        match &input {
            Input::InterestPlus { id } | Input::InterestPassive { id } => { self.interest.insert(*id); }
            Input::Load { id, .. } => { self.waiters.insert(*id, Want::First); }
            Input::InterestMinus { id } => {
                self.interest.remove(id);
                self.waiters.remove(id);
            }
            _ => {}
        }
        self.held.push(input);
        effects
    }

    /// Rows of `Opening`.
    fn opening(&mut self, input: Input) -> Vec<Effect> {
        match input {
            Input::Read(Read::Stray) => Vec::new(),
            Input::Read(read) => {
                if let Read::Media { marker, members } | Read::MediaMarked { marker, members } = read {
                    if !self.held.iter().any(|held| matches!(held, Input::ModelsReset { own_model: true })) {
                        self.marker = Some(marker);
                        self.members = members;
                        self.has_rows = true;
                        self.stale_marked = matches!(read, Read::MediaMarked { .. });
                    }
                }
                if let Read::Saved { landed_at, fresh_until, stale, generation_ok: true } = read {
                    if !self.held.iter().any(|held| matches!(held, Input::ModelsReset { own_model: true })) {
                        self.fresh = !stale;
                        self.landed_at = landed_at;
                        self.fresh_until = fresh_until;
                    }
                }
                self.state = State::Idle;
                let held = std::mem::take(&mut self.held);
                for held in &held {
                    if let Input::Load { id, .. } = held { self.waiters.remove(id); }
                }
                let mut effects = Vec::new();
                for held in held {
                    effects.extend(self.step(held));
                }
                effects
            }
            Input::EpochEnded | Input::Closing => {
                for held in std::mem::take(&mut self.held) {
                    if let Input::Load { id, .. } = held { self.outcomes.push((id, Result3::Interrupted)); }
                }
                self.waiters.clear();
                self.state = State::Idle;
                let closing = input == Input::Closing;
                self.end(closing)
            }
            Input::SourceChanged { revision } => vec![Effect::DerivePreload { revision }],
            Input::Foreground { .. } => Vec::new(),
            other => self.hold(other, Vec::new()),
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
                if self.fresh_at() {
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
            Input::Online(true) if !self.fresh_at() && self.watched() => self.begin(1, false, true),
            Input::OfflineRefused => {
                self.last_error = Some(true);
                self.set_phase(self.failed_phase());
                vec![Effect::WindowState]
            }
            Input::ModelsReset { own_model: true } => {
                self.fresh = false;
                if self.watched() { self.begin(1, false, self.is_online()) } else { Vec::new() }
            }
            _ => Vec::new(),
        }
    }

    fn idle_load(&mut self, id: u32, goal: Goal, force: bool) -> Vec<Effect> {
        match goal {
            Goal::First if self.fresh_at() && !force => self.immediate(id, Result3::Loaded),
            Goal::All if self.fresh_at() && !force => match self.cursor {
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

    fn loading(&mut self, (request, attempt, retry): (u64, u32, bool), (page, force, attempts): (u32, bool, u32), input: Input) -> Vec<Effect> {
        let step = self.step;
        let mine = |got_request: u64, got_attempt: u32| got_request == step && got_attempt == attempt && !retry;
        match input {
            Input::InterestPlus { id } => {
                self.interest.insert(id);
                vec![Effect::WindowState]
            }
            Input::Load { id, goal: Goal::First, force: true } if page > 1 => {
                self.join_load(id, Goal::First, page);
                let (new_request, new_attempt) = self.request_ids();
                self.state = State::Loading { request: new_request, attempt: new_attempt, retry: false, page: 1, force: true, attempts: 0 };
                let mut effects = if retry { Vec::new() } else { vec![Effect::Cancel { request: step, attempt }] };
                effects.extend([self.issue_effect(new_request, new_attempt, 1), Effect::WindowState]);
                effects
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
                    vec![Effect::Cancel { request: step, attempt }]
                }
            }
            Input::MarkerAnswer { request: got, attempt: got_attempt, marker } if mine(got, got_attempt) && self.chain.is_some() => self.media_marker(request, attempt, (force, attempts), marker),
            Input::Response { request: got, attempt: got_attempt, cursor, rows } if mine(got, got_attempt) && self.chain.is_some() => self.media_page(request, force, cursor, rows, false),
            Input::MoreWithoutCursor { request: got, attempt: got_attempt } if mine(got, got_attempt) && self.chain.is_some() => self.media_page(request, force, None, false, true),
            Input::Response { request: got, attempt: got_attempt, cursor, rows } if mine(got, got_attempt) => {
                self.answered_at = self.now;
                self.page_count = if page == 1 { 1 } else { self.page_count + 1 };
                self.state = State::Landing { request, page, cursor, rows };
                vec![Effect::Commit { request, replace_all: self.replace_all }]
            }
            Input::TransportError { request: got, attempt: got_attempt, failure } if mine(got, got_attempt) => self.failed(request, page, force, attempts, failure),
            Input::Dropped { request: got, attempt: got_attempt } if mine(got, got_attempt) => self.failed(request, page, force, attempts, Failure::Network),
            Input::Retry { request: got } if got == request && retry && self.is_online() => {
                let attempt = self.attempt_id();
                self.state = State::Loading { request, attempt, retry: false, page, force, attempts };
                vec![self.current_effect(step, attempt, page)]
            }
            Input::Online(false) => {
                self.state = State::Waiting { force, page };
                if retry { Vec::new() } else { vec![Effect::Cancel { request: step, attempt }] }
            }
            Input::Stale { in_keys: true } => {
                self.reread_due = true;
                Vec::new()
            }
            Input::ModelsReset { own_model: true } => {
                let (new_request, new_attempt) = self.request_ids();
                self.state = State::Loading { request: new_request, attempt: new_attempt, retry: false, page, force, attempts: 0 };
                vec![Effect::Cancel { request: step, attempt }, Effect::Request { request: new_request, attempt: new_attempt, page }]
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
            return vec![self.issue_effect(new_request, new_attempt, 1)];
        }
        if failure.temporary() && attempts + 1 < failure.retry_limit() {
            let State::Loading { attempt, .. } = self.state else { return Vec::new() };
            self.state = State::Loading { request, attempt, retry: true, page, force, attempts: attempts + 1 };
            return vec![Effect::ScheduleRetry { request }];
        }
        self.fail(failure == Failure::Network)
    }

    fn fail(&mut self, network: bool) -> Vec<Effect> {
        self.landing_marker = None;
        self.chain = None;
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

    /// Rows `win_epoch_ended` and `win_closing`.
    fn end(&mut self, closing: bool) -> Vec<Effect> {
        let was = self.state;
        self.state = State::Ended;
        self.answer_all(Result3::Interrupted);
        let mut effects = Vec::new();
        if let State::Loading { attempt, retry: false, .. } = was { effects.push(Effect::Cancel { request: self.step, attempt }); }
        if closing { effects.push(Effect::Drained); }
        effects
    }
}

mod ref_window_landing;
mod ref_window_media;
