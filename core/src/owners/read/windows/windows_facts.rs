use super::*;

/// The values every state of the machine shares.
pub(super) struct Facts {
    pub(super) epoch: Epoch,
    pub(super) online: Option<bool>,
    pub(super) online_seq: u64,
    pub(super) load_seq: std::cell::Cell<u64>,
    pub(super) dependency_models: BTreeSet<String>,
    pub(super) interest: BTreeSet<InterestId>,
    /// Interests that load the window again each time the network returns.
    pub(super) reload: BTreeSet<InterestId>,
    /// The network was away since the last `Online(true)`; the last `Online(true)` ended such a gap.
    pub(super) dropped: bool,
    pub(super) reconnected: bool,
    pub(super) waiters: BTreeMap<InterestId, Want>,
    /// The first page landed and nothing marked the window stale since.
    pub(super) fresh: bool,
    /// The device saves power (`saving` of the dictionary).
    pub(super) saving: bool,
    pub(super) landed_at: i64,
    pub(super) fresh_until: i64,
    /// `expires_in` and `empty_in` of the relation in ms.
    pub(super) lifetime: (i64, i64),
    /// The `now` of the response that is landing.
    pub(super) answered_at: i64,
    pub(super) cursor: Option<String>,
    /// The last page that landed.
    pub(super) page: u32,
    pub(super) page_count: u32,
    pub(super) local_order: bool,
    pub(super) max_pages: u32,
    pub(super) members_complete: bool,
    pub(super) cursor_reset_used: bool,
    pub(super) replace_all: bool,
    pub(super) reread_due: bool,
    /// A first-page request waits in `Waiting` for the end of the gap after the landing of the last answer (P-WIN-37).
    pub(super) gap_wait: bool,
    /// The window lost its freshness here and the Store still carries it fresh: a mark is owed before the machine leaves memory.
    pub(super) mark_owed: std::cell::Cell<bool>,
    /// The largest Store revision of a commit that changed rows of this window (D-454).
    pub(super) change_due: i64,
    /// The Store base of the load that rereads every row (page 1, not media) and answers the window or answered it last: -1 none, `i64::MAX` its pin is pending (D-454).
    pub(super) answer_base: std::cell::Cell<i64>,
    /// That load carries (or will take) the `node` mark (D-454).
    pub(super) answer_marked: std::cell::Cell<bool>,
    pub(super) has_rows: bool,
    /// How the last load of the interest now held ended: the phase, `settled` and `error` of the value come from it.
    pub(super) outcome: LastLoad,
    /// The revision of the last `Committed` of a landing of this machine, 0 before the first.
    pub(super) landed_revision: i64,
    pub(super) request_base: std::cell::Cell<i64>,
    pub(super) end_cause: EndCause,
    pub(super) stale_marked: std::cell::Cell<bool>,
    pub(super) stale_owed: std::cell::Cell<bool>,
    /// The window has a marker and a tail (`media` in the `.dbl`).
    pub(super) media: bool,
    pub(super) marker: Option<Marker>,
    pub(super) members: i64,
    /// The marker and the member count that the landing in flight makes the saved ones.
    pub(super) landing_marker: Option<(Marker, i64, bool)>,
    /// The window reads a `node`: only such a window takes the mark of a `changed_by` event (P-WIN-34).
    pub(super) reads_node: bool,
    /// A `changed_by` event marked the `node` window: the next request a load sends takes the mark, and that load lands as a `NodeReread` (P-WIN-34, M24).
    pub(super) reread_by_event: std::cell::Cell<bool>,
    pub(super) fx: RefCell<Vec<Emit>>,
}

impl Facts {
    pub(super) fn env(&mut self, event: &Ev) {
        match event {
            Ev::EpochEnded { cause } => self.end_cause = cause.clone(),
            Ev::Closing => self.end_cause = EndCause::Closed,
            Ev::Foreground { online_seq } => self.online_seq = self.online_seq.max(*online_seq),
            Ev::Online { online, online_seq, .. } => {
                self.online = Some(*online);
                if *online {
                    self.online_seq = *online_seq;
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
            Ev::Saved(Some(saved), store_seen) => {
                self.landed_revision = *store_seen;
                self.fresh = !saved.stale;
                self.mark_owed.set(false);
                if saved.stale { self.stale_marked.set(true); }
                if saved.owed && self.reads_node { self.reread_by_event.set(true); }
                self.landed_at = saved.landed_at;
                self.fresh_until = saved.fresh_until;
                self.cursor.clone_from(&saved.cursor);
                self.page = saved.pages;
                self.page_count = saved.pages;
                self.has_rows = saved.has_rows;
                self.marker.clone_from(&saved.marker);
                self.members = saved.members;
            }
            Ev::Stale { cause } => {
                if let StaleCause::Change { revision: Some(revision) } = cause { self.change_due = self.change_due.max(*revision); }
                if !self.covers(*cause) { self.stale_marked.set(true) }
                if matches!(cause, StaleCause::Change { .. }) && self.reads_node && (!self.covers(*cause) || self.answer_base.get() == i64::MAX) { self.reread_by_event.set(true); }
            }
            _ => {}
        }
    }

    /// A backward clock remains fresh while its offset is shorter than the window lifetime.
    pub(super) fn drop_fresh(&mut self) {
        self.mark_owed.set(self.mark_owed.get() || self.fresh);
        self.fresh = false;
    }

    pub(super) fn is_fresh(&self, now: i64) -> bool { self.fresh && now < self.fresh_until && i128::from(self.landed_at) - i128::from(now) < i128::from(self.fresh_until) - i128::from(self.landed_at) }

    pub(super) fn page_source(marked: bool) -> crate::schema::RowSource { if marked { crate::schema::RowSource::NodeReread } else { crate::schema::RowSource::Page } }

    pub(super) fn covers(&self, cause: StaleCause) -> bool {
        match cause {
            StaleCause::Gap { online_seq } => online_seq > 0 && self.load_seq.get() >= online_seq,
            StaleCause::Change { revision: Some(revision) } => self.answer_base.get() >= revision && (!self.reads_node || self.answer_marked.get()),
            _ => false,
        }
    }

    /// Only the preload watches the window.
    pub(super) fn preload_only(&self) -> bool { self.waiters.is_empty() && self.interest.len() == 1 && self.interest.contains(&PRELOAD) }

    /// A load waits for the end of power saving: no reader and no waiter asked for it.
    pub(super) fn deferred(&self) -> bool { (self.saving && self.preload_only()) || self.gap_wait }

    /// P-WIN-37: `now` is less than `REQUEST_GAP` after the landing of an answer that did not fail.
    pub(super) fn in_gap(&self, now: i64) -> bool { self.fresh_until != 0 && !matches!(self.outcome, LastLoad::Failed(_)) && (0..REQUEST_GAP).contains(&(now - self.landed_at)) }

    pub(super) fn is_online(&self) -> bool { self.online == Some(true) }

    pub(super) fn watched(&self) -> bool { !self.interest.is_empty() || !self.waiters.is_empty() }

    pub(super) fn push_fx(&self, effect: WindowEffect) {
        self.fx.borrow_mut().push(Emit::Run(effect));
    }

    pub(super) fn emit(&self, emit: Emit) { self.fx.borrow_mut().push(emit); }

    /// P-WIN-32: the value of the window from the stage (`loading` = a request in flight or landing), the outcome and the rows.
    pub(super) fn load_state(&self, loading: bool) -> WindowState {
        let phase = match &self.outcome {
            _ if loading => Phase::Loading,
            LastLoad::None => Phase::Idle,
            LastLoad::Fresh | LastLoad::Landed => Phase::Loaded,
            LastLoad::Failed(_) => Phase::Failed,
        };
        let settled = match &self.outcome {
            LastLoad::None => false,
            LastLoad::Landed => !(self.members_complete && self.cursor.is_some()),
            LastLoad::Fresh | LastLoad::Failed(_) => true,
        };
        let error = match &self.outcome { LastLoad::Failed(error) => Some(error.clone()), _ => None };
        let loaded = (self.landed_revision != 0 || matches!(self.outcome, LastLoad::Fresh | LastLoad::Landed)).then_some(self.landed_revision);
        WindowState { phase, error, loaded, has_more: self.can_load_next(), settled, total: None }
    }

    /// An interest or a load meets a fresh window that has no outcome yet.
    pub(super) fn enter_fresh(&mut self) {
        if self.outcome == LastLoad::None { self.outcome = LastLoad::Fresh; }
    }

    /// The outcome belongs to the interest now held: the last stream or call that leaves takes it away.
    pub(super) fn release_outcome(&mut self, _: StateOrSuperstate<'_, WinState, Superstate>, _: &Ev) {
        if !self.watched() { self.outcome = LastLoad::None; }
    }

    pub(super) fn add_interest(&mut self, id: InterestId) {
        self.interest.insert(id);
    }

    pub(super) fn drop_interest(&mut self, id: &InterestId) {
        self.interest.remove(id);
        self.waiters.remove(id);
    }

    pub(super) fn announce(&mut self) {
        let streams = self.interest.iter().copied().filter(|id| *id != PRELOAD).collect();
        self.emit(Emit::Announce { streams });
    }

    pub(super) fn answer(&mut self, id: InterestId, outcome: WindowOutcome) { self.push_fx(WindowEffect::Outcome { id, outcome }); }

    pub(super) fn answer_all(&mut self, outcome: &WindowOutcome) {
        for id in std::mem::take(&mut self.waiters).into_keys() {
            self.answer(id, outcome.clone());
        }
    }

    pub(super) fn page_cursor(&self, page: u32) -> Option<String> { if page > 1 { self.cursor.clone() } else { None } }

    pub(super) fn can_load_next(&self) -> bool { self.cursor.is_some() && self.page_count < self.max_pages }

    /// A load of `page`: a request when online, `Waiting` otherwise. `announce` adds the window state.
    pub(super) fn begin(&mut self, ctx: &mut Ctx<'_>, page: u32, force: bool, announce: bool) -> Outcome<WinState> {
        self.replace_all = false;
        if page == 1 && self.is_online() && !self.deferred() && self.in_gap(ctx.now) {
            self.gap_wait = true;
            self.emit(Emit::GapEnd { at: self.landed_at + REQUEST_GAP });
        }
        let next = if self.is_online() && !self.deferred() {
            let (request, attempt) = ctx.request();
            self.emit(Emit::Pin { epoch: self.epoch, request });
            WinState::loading(self.issue(request, attempt, page, force))
        } else {
            WinState::waiting(force, page)
        };
        if announce {
            self.announce();
        }
        Transition(next)
    }

    /// A window that waited goes to the network: `Loading` with a new request.
    pub(super) fn leave_waiting(&mut self, ctx: &mut Ctx<'_>, force: bool, page: u32) -> Outcome<WinState> {
        self.gap_wait = false;
        let (request, attempt) = ctx.request();
        self.emit(Emit::Pin { epoch: self.epoch, request });
        self.announce();
        Transition(WinState::loading(self.issue(request, attempt, page, force)))
    }

    /// A load of the first page of a media window starts with a check of its marker; every other load asks for a page.
    pub(super) fn issue(&self, request: RequestId, attempt: u32, page: u32, force: bool) -> Load {
        self.load_seq.set(self.online_seq);
        self.answer_base.set(if page == 1 && !self.media { i64::MAX } else { -1 });
        let load = Load::issued(request, attempt, page, force);
        if !(self.media && page == 1) { return load; }
        let up_to = self.marker.as_ref().map_or(0, |marker| marker.sequence);
        let stale = self.stale_marked.take() || self.stale_owed.get();
        self.stale_owed.set(stale);
        Load { chain: Some(Chain { phase: MediaPhase::MarkerCheck { up_to }, cursor: None, ids: Vec::new(), rows: PageRows::default(), models: BTreeSet::new(), latest: 0, stale, base: 0 }), ..load }
    }

    pub(super) fn request_effect(&self, load: &mut Load) -> WindowEffect {
        load.marked |= self.reread_by_event.take();
        let (cursor, kind) = match &load.chain {
            Some(chain) => (chain.cursor.clone(), chain.kind()),
            None => (self.page_cursor(load.page), RequestKind::Page),
        };
        WindowEffect::Request { request: load.step, attempt: load.attempt, page: load.page, cursor, epoch: self.epoch, kind }
    }

    /// The next step of the chain: its own request id, the Store pin and the batch stay with the load. A confirm asked again after a counted answer is a new step too.
    pub(super) fn chain_request(&mut self, context: &mut Ctx<'_>, load: &mut Load, chain: Chain) -> Outcome<WinState> {
        (load.step, load.attempt) = context.request();
        load.attempts = 0;
        load.chain = Some(chain);
        let effect = self.request_effect(load);
        self.push_fx(effect);
        Handled
    }

    /// The chain ends: its rows and its marker land in 1 batch, or only the marker when the rows stay.
    pub(super) fn land_chain(&mut self, load: &mut Load, chain: &Chain, marker: Marker, keep_rows: bool) -> Outcome<WinState> {
        let members = if keep_rows { self.members } else { chain.base + i64::try_from(chain.ids.len()).unwrap_or(i64::MAX) };
        let mut page = AnswerPage::new(1, chain.ids.clone(), None);
        page.payload = chain.rows.clone();
        page.dependency_models = chain.models.clone();
        let number = if chain.base > 0 { 2 } else { 1 };
        let replace_all = self.replace_all || (!keep_rows && chain.base == 0 && self.marker.as_ref().is_some_and(|saved| saved.values != marker.values));
        self.replace_all = replace_all;
        self.page_count = landed_tail(self.page_count, number, replace_all, self.local_order || self.media).0;
        let marked = std::mem::take(&mut load.marked);
        let source = Self::page_source(marked);
        self.emit(Emit::Commit { landed_at: self.answered_at, request: load.request, page, number, replace_all, base_revision: self.request_base.get(), marker: Some(marker.clone()), keep_rows, source });
        self.landing_marker = Some((marker, members, !keep_rows));
        let cursor = if keep_rows { self.cursor.clone() } else { None };
        Transition(WinState::landing(Landing { request: load.request, page: number, cursor, rows: keep_rows && self.has_rows || !chain.ids.is_empty(), models: chain.models.clone(), marked }))
    }

    /// The answer of a request of a media chain, by the phase of the chain.
    pub(super) fn media_response(&mut self, context: &mut Ctx<'_>, load: &mut Load, answer: &AnswerPage) -> Outcome<WinState> {
        self.answered_at = context.now;
        let Some(mut chain) = load.chain.take() else { return Handled };
        chain.models.extend(answer.dependency_models.iter().cloned());
        match chain.phase {
            MediaPhase::MarkerCheck { up_to } => {
                let Some(found) = answer.marker.clone() else { return self.fail(invalid_response()) };
                chain.latest = found.sequence;
                let prefix = !chain.stale && self.marker.as_ref().is_some_and(|saved| saved.values == found.values && saved.sequence == up_to);
                if prefix && found.sequence == up_to {
                    return self.land_chain(load, &chain, found, true);
                }
                if prefix && found.sequence > up_to {
                    chain.phase = MediaPhase::Tail { after: up_to };
                    chain.base = self.members;
                } else {
                    chain.phase = MediaPhase::Full;
                }
                self.chain_request(context, load, chain)
            }
            MediaPhase::Tail { .. } | MediaPhase::Full => {
                chain.ids.extend(answer.rows.iter().cloned());
                chain.rows.roots.extend(answer.payload.roots.iter().cloned());
                chain.rows.related.extend(answer.payload.related.iter().cloned());
                chain.rows.total = answer.payload.total.or(chain.rows.total);
                if !answer.more {
                    chain.phase = MediaPhase::Confirm { up_to: chain.latest };
                    chain.cursor = None;
                } else {
                    match &answer.cursor {
                        Some(next) if chain.cursor.as_ref() != Some(next) => chain.cursor = Some(next.clone()),
                        _ => return self.fail(invalid_response()),
                    }
                }
                self.chain_request(context, load, chain)
            }
            MediaPhase::Confirm { .. } => {
                let Some(found) = answer.marker.clone() else { return self.fail(invalid_response()) };
                let counted: i64 = found.values.iter().filter(|(field, _)| field.as_str() != "revision").map(|(_, count)| *count).sum();
                if counted == chain.base + i64::try_from(chain.ids.len()).unwrap_or(i64::MAX) {
                    return self.land_chain(load, &chain, Marker { sequence: chain.latest, values: found.values }, false);
                }
                if may_retry(ErrorClass::Server, load.attempts + 1) {
                    load.attempts += 1;
                    load.retry = true;
                    load.step = context.request().0;
                    load.chain = Some(chain);
                    self.emit(Emit::Retry { request: load.request, attempt: load.attempts, class: ErrorClass::Server });
                    return Handled;
                }
                self.fail(invalid_response())
            }
        }
    }

    pub(super) fn finish(&mut self, request: Option<(RequestId, u32)>) -> Outcome<WinState> {
        self.answer_all(&WindowOutcome::Ended { cause: self.end_cause.clone() });
        if let Some((request, attempt)) = request {
            self.push_fx(WindowEffect::Cancel { request, attempt });
        }
        Transition(WinState::ended())
    }

    pub(super) fn fail(&mut self, failure: crate::ffi::DbError) -> Outcome<WinState> {
        self.landing_marker = None;
        self.answer_base.set(-1);
        self.drop_fresh();
        self.reread_due = false;
        self.outcome = LastLoad::Failed(failure.clone());
        self.answer_all(&WindowOutcome::Failed { failure });
        self.announce();
        Transition(WinState::idle())
    }

    pub(super) fn join_load(&mut self, id: InterestId, goal: Goal, page: u32) {
        let want = match goal {
            Goal::First => Want::First,
            Goal::All => Want::All,
            Goal::Next => Want::Next(page + 1),
        };
        self.waiters.insert(id, want);
    }

    pub(super) fn immediate(&mut self, id: InterestId, outcome: WindowOutcome) -> Outcome<WinState> {
        self.answer(id, outcome);
        Handled
    }

    pub(super) fn idle_load(&mut self, ctx: &mut Ctx<'_>, id: InterestId, goal: Goal, force: bool) -> Outcome<WinState> {
        let fresh = self.is_fresh(ctx.now);
        let loaded = WindowOutcome::Loaded { revision: self.landed_revision, has_more: self.can_load_next(), fetched: false };
        let next_page = self.page + 1;
        match goal {
            Goal::First if fresh && !force => {
                self.enter_fresh();
                self.immediate(id, loaded)
            }
            Goal::All if fresh && !force => {
                if self.cursor.is_none() {
                    self.enter_fresh();
                    self.immediate(id, loaded)
                } else if !self.can_load_next() {
                    self.immediate(id, WindowOutcome::PageLimit)
                } else {
                    self.waiters.insert(id, Want::All);
                    self.begin(ctx, next_page, false, true)
                }
            }
            Goal::First | Goal::All if self.in_gap(ctx.now) => {
                self.enter_fresh();
                self.immediate(id, loaded)
            }
            Goal::First | Goal::All => {
                self.waiters.insert(id, if goal == Goal::All { Want::All } else { Want::First });
                self.begin(ctx, 1, force, true)
            }
            Goal::Next => {
                if !self.can_load_next() {
                    self.immediate(id, loaded)
                } else {
                    self.waiters.insert(id, Want::Next(next_page));
                    self.begin(ctx, next_page, false, true)
                }
            }
        }
    }

    pub(super) fn landed(&mut self, ctx: &mut Ctx<'_>, page: u32, cursor: Option<String>, rows: bool, dependency_models: &BTreeSet<String>, revision: i64) -> Outcome<WinState> {
        self.has_rows |= rows;
        if let Some((marker, members, replaced)) = self.landing_marker.take() {
            self.marker = Some(marker);
            self.members = members;
            if replaced { self.stale_owed.set(false); }
        }
        if !landed_tail(self.page_count, page, self.replace_all, self.local_order || self.media).1 {
            self.cursor = cursor;
            self.page = page;
        }
        self.landed_revision = revision;
        if page == 1 {
            self.landed_at = self.answered_at;
            self.fresh_until = self.answered_at + if rows { self.lifetime.0 } else { self.lifetime.1 };
        }
        self.cursor_reset_used = false;
        self.replace_all = false;
        let chain = self.members_complete && self.cursor.is_some();
        self.outcome = LastLoad::Landed;
        if !dependency_models.is_empty() {
            self.dependency_models = dependency_models.clone();
        }
        if chain {
            if self.watched() {
                let loaded = WindowOutcome::Loaded { revision, has_more: self.can_load_next(), fetched: true };
                let wants_next = |want: &Want| matches!(want, Want::All) || matches!(want, Want::Next(c) if *c > page);
                let finished: Vec<InterestId> = self.waiters.iter().filter(|(_, want)| !wants_next(want)).map(|(id, _)| *id).collect();
                for id in finished {
                    self.waiters.remove(&id);
                    self.answer(id, loaded.clone());
                }
                return self.begin(ctx, page + 1, false, true);
            }
            self.answer_all(&WindowOutcome::Loaded { revision, has_more: self.can_load_next(), fetched: true });
            self.drop_fresh();
            self.announce();
            return Transition(WinState::idle());
        }
        let wants_next = |want: &Want| matches!(want, Want::All) || matches!(want, Want::Next(c) if *c > page);
        let loaded = WindowOutcome::Loaded { revision, has_more: self.can_load_next(), fetched: true };
        if self.cursor.is_some() && self.waiters.values().any(wants_next) {
            if !self.can_load_next() {
                for (id, want) in std::mem::take(&mut self.waiters) {
                    self.answer(id, if matches!(want, Want::All) { WindowOutcome::PageLimit } else { loaded.clone() });
                }
                return self.land_end(ctx);
            }
            let finished: Vec<InterestId> = self.waiters.iter().filter(|(_, want)| !wants_next(want)).map(|(id, _)| *id).collect();
            for id in finished {
                self.waiters.remove(&id);
                self.answer(id, loaded.clone());
            }
            return self.begin(ctx, page + 1, false, true);
        }
        self.answer_all(&loaded);
        self.land_end(ctx)
    }

    fn land_end(&mut self, ctx: &mut Ctx<'_>) -> Outcome<WinState> {
        if !self.reread_due {
            self.fresh = true;
            self.mark_owed.set(false);
            self.announce();
            return Transition(WinState::idle());
        }
        self.reread_due = false;
        if !self.watched() {
            self.drop_fresh();
            self.announce();
            return Transition(WinState::idle());
        }
        self.begin(ctx, 1, false, true)
    }
}

fn invalid_response() -> crate::ffi::DbError { crate::ffi::DbError::Invalid { detail: "the response of the window is invalid".into() } }
