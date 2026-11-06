/// The values every state of the machine shares.
struct Facts {
    epoch: Epoch,
    online: Option<bool>,
    base_generation: u64,
    generations: BTreeMap<String, u64>,
    dependency_models: BTreeSet<String>,
    interest: BTreeSet<InterestId>,
    /// Interests that load the window again each time the network returns.
    reload: BTreeSet<InterestId>,
    /// The network was away since the last `Online(true)`; the last `Online(true)` ended such a gap.
    dropped: bool,
    reconnected: bool,
    waiters: BTreeMap<InterestId, Want>,
    /// The first page landed and nothing marked the window stale since.
    fresh: bool,
    /// The device saves power (`saving` of the dictionary).
    saving: bool,
    landed_at: i64,
    fresh_until: i64,
    /// `expires_in` and `empty_in` of the relation in ms.
    lifetime: (i64, i64),
    /// The `now` of the response that is landing.
    answered_at: i64,
    cursor: Option<String>,
    /// The last page that landed.
    page: u32,
    page_count: u32,
    max_pages: u32,
    members_complete: bool,
    cursor_reset_used: bool,
    replace_all: bool,
    reread_due: bool,
    has_rows: bool,
    phase: Phase,
    /// The first load of the interest now held ended with an outcome.
    settled: bool,
    error: Option<crate::ffi::DbError>,
    /// The revision of the last `Committed` of a landing of this machine, 0 before the first.
    landed_revision: i64,
    store_seen: i64,
    request_base: std::cell::Cell<i64>,
    fx: RefCell<Vec<Emit>>,
}

impl Facts {
    fn env(&mut self, event: &Ev) {
        match event {
            Ev::Online { online, .. } => {
                self.online = Some(*online);
                if *online {
                    self.reconnected = std::mem::take(&mut self.dropped);
                } else {
                    self.dropped = true;
                }
            }
            Ev::PowerSaving { saving } => self.saving = *saving,
            Ev::ReloadOnConnect { id } => {
                self.reload.insert(*id);
            }
            Ev::InterestMinus { id } => {
                self.reload.remove(id);
            }
            Ev::ModelsReset { generation, models } => {
                for model in models {
                    self.generations.insert(model.clone(), *generation);
                }
            }
            Ev::Saved(Some(saved)) => {
                self.fresh = !saved.stale;
                self.landed_at = saved.landed_at;
                self.fresh_until = saved.fresh_until;
                self.cursor.clone_from(&saved.cursor);
                self.page = saved.pages;
                self.page_count = saved.pages;
                self.has_rows = saved.has_rows;
            }
            Ev::Load { store_revision, .. } => self.store_seen = self.store_seen.max(*store_revision),
            Ev::StoreSeen { revision } => self.store_seen = self.store_seen.max(*revision),
            _ => {}
        }
    }

    /// Loaded, not marked stale, and `now` inside `landed_at <= now < fresh_until`: a clock that went back makes the window stale.
    fn is_fresh(&self, now: i64) -> bool { self.fresh && self.landed_at <= now && now < self.fresh_until }

    /// Only the preload watches the window.
    fn preload_only(&self) -> bool { self.waiters.is_empty() && self.interest.len() == 1 && self.interest.contains(&PRELOAD) }

    /// A load waits for the end of power saving: no reader and no waiter asked for it.
    fn deferred(&self) -> bool { self.saving && self.preload_only() }

    fn is_online(&self) -> bool { self.online == Some(true) }

    fn watched(&self) -> bool { !self.interest.is_empty() || !self.waiters.is_empty() }

    fn push_fx(&self, effect: WindowEffect) {
        self.fx.borrow_mut().push(Emit::Run(effect));
    }

    fn emit(&self, emit: Emit) { self.fx.borrow_mut().push(emit); }

    /// The generation of every model when the request leaves: the answer may name models the window did not know before.
    fn basis(&self) -> GenerationsAtIssue { GenerationsAtIssue { base: self.base_generation, generations: self.generations.clone() } }

    fn depends_on(&self, models: &[String]) -> bool { models.iter().any(|model| self.dependency_models.contains(model)) }

    fn load_state(&self) -> WindowState { WindowState { phase: self.phase, error: self.error.clone(), loaded: (self.landed_revision != 0 || self.phase == Phase::Loaded).then_some(self.landed_revision), has_more: self.cursor.is_some(), settled: self.settled, total: None } }

    fn set_phase(&mut self, phase: Phase) {
        self.phase = phase;
        self.settled |= matches!(phase, Phase::Loaded | Phase::Failed);
    }

    fn drop_interest(&mut self, id: &InterestId) {
        self.interest.remove(id);
        self.waiters.remove(id);
        if self.interest.is_empty() {
            self.settled = false;
        }
    }

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
        let next = if self.is_online() && !self.deferred() {
            let (request, attempt) = ctx.request();
            self.phase = Phase::Loading;
            self.emit(Emit::Pin { epoch: self.epoch, request });
            WinState::loading(Load::issued(request, attempt, page, force, self.basis()))
        } else {
            WinState::waiting(force, page)
        };
        if announce {
            self.announce();
        }
        Transition(next)
    }

    /// A window that waited goes to the network: `Loading` with a new request.
    fn leave_waiting(&mut self, ctx: &mut Ctx<'_>, force: bool, page: u32) -> Outcome<WinState> {
        let (request, attempt) = ctx.request();
        self.phase = Phase::Loading;
        self.emit(Emit::Pin { epoch: self.epoch, request });
        self.announce();
        Transition(WinState::loading(Load::issued(request, attempt, page, force, self.basis())))
    }

    fn finish(&mut self, request: Option<(RequestId, u32)>) -> Outcome<WinState> {
        self.answer_all(&WindowOutcome::Interrupted);
        if let Some((request, attempt)) = request {
            self.push_fx(WindowEffect::Cancel { request, attempt });
        }
        Transition(WinState::ended())
    }

    fn fail(&mut self, message: &str, source: Option<crate::transport::TransportError>) -> Outcome<WinState> {
        self.fresh = false;
        self.reread_due = false;
        self.set_phase(self.failed_phase());
        self.error = Some(source.clone().map_or_else(|| crate::ffi::DbError::Database { detail: message.to_string() }, crate::ffi::DbError::from));
        self.answer_all(&WindowOutcome::Failed { error: message.to_string(), source });
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

    fn idle_load(&mut self, ctx: &mut Ctx<'_>, (id, now): (InterestId, i64), goal: Goal, force: bool) -> Outcome<WinState> {
        let fresh = self.is_fresh(now);
        let loaded = WindowOutcome::Loaded { revision: self.landed_revision, has_more: self.cursor.is_some(), fetched: false };
        let next_page = self.page + 1;
        match goal {
            Goal::First if fresh && !force => self.immediate(id, loaded),
            Goal::All if fresh && !force => {
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
        if page == 1 {
            self.landed_at = self.answered_at;
            self.fresh_until = self.answered_at + if rows { self.lifetime.0 } else { self.lifetime.1 };
        }
        self.error = None;
        self.cursor_reset_used = false;
        self.replace_all = false;
        let chain = self.members_complete && self.cursor.is_some();
        let settled = self.settled;
        self.set_phase(Phase::Loaded);
        if !dependency_models.is_empty() {
            self.dependency_models = dependency_models.clone();
        }
        if chain {
            self.settled = settled;
            if self.watched() {
                let loaded = WindowOutcome::Loaded { revision, has_more: true, fetched: true };
                let wants_next = |want: &Want| matches!(want, Want::All) || matches!(want, Want::Next(c) if *c > page);
                let finished: Vec<InterestId> = self.waiters.iter().filter(|(_, want)| !wants_next(want)).map(|(id, _)| *id).collect();
                for id in finished {
                    self.waiters.remove(&id);
                    self.answer(id, loaded.clone());
                }
                return self.begin(ctx, page + 1, false, true);
            }
            self.answer_all(&WindowOutcome::Loaded { revision, has_more: true, fetched: true });
            self.fresh = false;
            self.announce();
            return Transition(WinState::idle());
        }
        let wants_next = |want: &Want| matches!(want, Want::All) || matches!(want, Want::Next(c) if *c > page);
        let loaded = WindowOutcome::Loaded { revision, has_more: self.cursor.is_some(), fetched: true };
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
