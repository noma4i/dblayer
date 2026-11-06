#[state_machine(initial = "WinState::idle()", state(name = "WinState"))]
impl Facts {
    #[state]
    fn idle(&mut self, context: &mut Ctx<'_>, event: &Ev) -> Outcome<WinState> {
        self.env(event);
        match event {
            Ev::InterestPassive { id } => {
                self.interest.insert(*id);
                self.announce();
                Handled
            }
            Ev::InterestPlus { id } => {
                self.interest.insert(*id);
                if self.fresh {
                    self.set_phase(Phase::Loaded);
                    self.announce();
                    Handled
                } else {
                    self.begin(context, 1, false, true)
                }
            }
            Ev::Load { id, goal, force, .. } => self.idle_load(context, *id, *goal, *force),
            Ev::OfflineRefused { message } => {
                self.error = Some(crate::ffi::DbError::Network { detail: message.clone() });
                self.set_phase(self.failed_phase());
                self.announce();
                Handled
            }
            Ev::InterestMinus { id } => {
                self.drop_interest(id);
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
            Ev::Online(true) if self.reconnected && self.interest.iter().any(|id| self.reload.contains(id)) => self.begin(context, 1, false, true),
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
    fn loading(&mut self, context: &mut Ctx<'_>, request: &mut RequestId, attempt: &mut u32, retry: &mut bool, pinning: &mut bool, page: &mut u32, force: &mut bool, attempts: &mut u32, basis: &mut GenerationsAtIssue, event: &Ev) -> Outcome<WinState> {
        self.env(event);
        let mine = |got: &RequestId, got_attempt: &u32| got == request && got_attempt == attempt && !*retry;
        match event {
            Ev::Pinned { request: got, base_revision } if got == request && *pinning => {
                self.request_base.set(i64::try_from(*base_revision).unwrap_or(i64::MAX));
                *pinning = false;
                self.push_fx(WindowEffect::Request { request: *request, attempt: *attempt, page: *page, cursor: self.page_cursor(*page), epoch: self.epoch });
                Handled
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
                self.drop_interest(id);
                if self.watched() {
                    return Handled;
                }
                self.fresh = false;
                self.set_phase(if self.has_rows { Phase::Loaded } else { Phase::Idle });
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
                    self.emit(Emit::Pin { epoch: self.epoch, request: new_request });
                    return Transition(WinState::loading(new_request, new_attempt, false, true, 1, *force, 0, self.basis()));
                }
                let class = if error.class == ErrorClass::InvalidCursor { ErrorClass::Permanent } else { error.class };
                if class != ErrorClass::Permanent && *attempts + 1 < attempt_limit(class) {
                    *attempts += 1;
                    *retry = true;
                    self.emit(Emit::Retry { request: *request, attempt: *attempts, class });
                    return Handled;
                }
                self.fail(&error.message, error.source.clone())
            }
            Ev::RetryDue { request: got } if got == request && *retry && self.is_online() => {
                *attempt = context.attempt();
                *retry = false;
                self.push_fx(WindowEffect::Request { request: *request, attempt: *attempt, page: *page, cursor: self.page_cursor(*page), epoch: self.epoch });
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
                self.emit(Emit::Pin { epoch: self.epoch, request: new_request });
                *request = new_request;
                *attempt = new_attempt;
                *retry = false;
                *pinning = true;
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
                RejectReason::Storage(message) => self.fail(message, None),
                RejectReason::Invalid => self.fail("the batch has no basis for a model of its rows", None),
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
                self.drop_interest(id);
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
                self.emit(Emit::Pin { epoch: self.epoch, request });
                self.announce();
                Transition(WinState::loading(request, attempt, false, true, *page, *force, 0, self.basis()))
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
                self.drop_interest(id);
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
