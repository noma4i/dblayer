use super::*;

impl Window {
    pub(super) fn landed(&mut self, page: u32, cursor: Option<u32>, rows: bool, revision: i64) -> Vec<Effect> {
        self.has_rows |= rows;
        if let Some((marker, members, replaced)) = self.landing_marker.take() {
            self.marker = Some(marker);
            self.members = members;
            if replaced { self.stale_owed = false; }
        }
        self.cursor = cursor;
        self.page = page;
        self.landed_revision = revision;
        if page == 1 {
            self.landed_at = self.answered_at;
            self.fresh_until = self.answered_at + if rows { self.lifetime.0 } else { self.lifetime.1 };
        }
        self.last_error = None;
        self.cursor_reset_used = false;
        self.replace_all = false;
        let chain = self.members_complete && self.cursor.is_some();
        let settled = self.settled;
        self.set_phase(Phase::Loaded);
        if chain {
            self.settled = settled;
            let watched = self.watched();
            for (id, want) in self.waiters.clone() {
                if !(matches!(want, Want::All) || matches!(want, Want::Next(c) if c > page)) {
                    self.waiters.remove(&id);
                    self.outcomes.push((id, Result3::Loaded));
                }
            }
            if watched {
                let next = self.cursor.expect("a cursor");
                if self.is_online() {
                    let (request, attempt) = self.request_ids();
                    self.state = State::Loading { request, attempt, retry: false, page: next, force: false, attempts: 0 };
                    self.phase = Phase::Loading;
                    return vec![Effect::Request { request, attempt, page: next }, Effect::WindowState];
                }
                self.state = State::Waiting { force: false, page: next };
                return vec![Effect::WindowState];
            }
            for (id, _) in std::mem::take(&mut self.waiters) {
                self.outcomes.push((id, Result3::Loaded));
            }
            self.state = State::Idle;
            self.fresh = false;
            return vec![Effect::WindowState];
        }
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

    pub(super) fn rejected(&mut self, reason: Reject) -> Vec<Effect> {
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

    pub(super) fn waiting(&mut self, force: bool, page: u32, input: Input) -> Vec<Effect> {
        match input {
            Input::Online(true) if self.deferred() => Vec::new(),
            Input::Online(true) => self.leave_waiting(force, page),
            Input::PowerSaving(_) if self.is_online() && !self.deferred() => self.leave_waiting(force, page),
            Input::OfflineRefused => {
                self.last_error = Some(true);
                self.set_phase(self.failed_phase());
                vec![Effect::WindowState]
            }
            Input::InterestPlus { id } => {
                self.interest.insert(id);
                if self.is_online() && !self.deferred() {
                    return self.leave_waiting(force, page);
                }
                vec![Effect::WindowState]
            }
            Input::Load { id, goal, force: wider } => {
                self.join_load(id, goal, page);
                let force = force || (wider && goal != Goal::Next);
                self.state = State::Waiting { force, page };
                if self.is_online() && !self.deferred() {
                    return self.leave_waiting(force, page);
                }
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

    pub(super) fn leave_waiting(&mut self, force: bool, page: u32) -> Vec<Effect> {
        let (request, attempt) = self.request_ids();
        self.state = State::Loading { request, attempt, retry: false, page, force, attempts: 0 };
        self.phase = Phase::Loading;
        vec![self.issue_effect(request, attempt, page), Effect::WindowState]
    }
}
