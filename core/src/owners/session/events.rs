use super::*;

fn carries_temp_id(body: &str) -> bool {
    if !body.contains(crate::identity::TEMP_ID_PREFIX) { return false; }
    let Ok(mut body) = serde_json::from_str::<serde_json::Value>(body) else { return false };
    let mut found = false;
    crate::identity::walk_temp_ids(&mut body, &mut |_| found = true);
    found
}

impl SessionState {
    fn retry_direct(&mut self, pending: PendingCall, attempts: u64, error: SessionError, fx: &mut Vec<SessionEffect>) {
        let exhausted = crate::runtime::RetryPolicy::Http.limit().is_some_and(|limit| attempts + 1 >= u64::from(limit));
        if pending.reply.is_some() && exhausted {
            if pending.class == OperationClass::Sessionless {
                self.land(pending, Err(error), None, None, false, fx);
            } else {
                self.detach_error = Some(error);
                self.persist(PendingCall { owner: Some(MutateOwner::Detached), ..pending }, None, fx);
            }
        } else if self.is_online() {
            fx.push(SessionEffect::ScheduleTick { attempts: attempts + 1 });
            self.phase = SessionPhase::Calling { pending, attempts: attempts + 1, phase: AttemptPhase::WaitingRetry };
        } else {
            self.phase = SessionPhase::Calling { pending, attempts, phase: AttemptPhase::WaitingNetwork };
        }
    }
    pub(super) fn run(&mut self, input: SessionInput, fx: &mut Vec<SessionEffect>) {
        let phase = std::mem::replace(&mut self.phase, SessionPhase::Closed);
        let taking_result = match &input {
            SessionInput::Committed(committed) => Some((committed.batch, true)),
            SessionInput::Rejected(rejected) => Some((rejected.batch, false)),
            _ => None,
        };
        if let Some((batch, committed)) = taking_result {
            if let Some((_, waiters)) = self.resume_taking.take_if(|(expected, _)| *expected == batch) {
                let closing = matches!(phase, SessionPhase::Closed);
                let answer = if committed { self.restored_answer.take() } else { self.restored_answer.clone() }.and_then(|answer| match answer { RestoredAnswer::Saved(answer) => Some(answer), RestoredAnswer::Local(_) => None });
                for (index, id) in waiters.into_iter().enumerate() {
                    let result = if committed { if index == 0 { answer.clone().ok_or_else(|| SessionError::Storage("the restored answer is missing".into())).and_then(|answer| answer.map(Some)) } else { Ok(None) } } else if closing { Err(SessionError::Closed) } else { Err(SessionError::Storage("the answer could not be removed".into())) };
                    fx.push(SessionEffect::ResumeReply { id, answer: result });
                }
                self.phase = phase;
                if closing { fx.push(SessionEffect::Drained(Drained { owner: "session" })); }
                return;
            }
            if let Some((_, answer)) = self.resume_saving.take_if(|(expected, _)| *expected == batch) {
                self.phase = phase;
                if committed {
                    self.restored_answer = Some(RestoredAnswer::Saved(answer));
                    if !self.resume_waiters.is_empty() {
                        let waiters = std::mem::take(&mut self.resume_waiters);
                        self.take_resume(waiters, fx);
                    }
                } else {
                    for id in self.resume_waiters.drain(..) { fx.push(SessionEffect::ResumeReply { id, answer: Err(SessionError::Storage("the answer could not be saved".into())) }); }
                }
                return;
            }
        }
        if matches!(phase, SessionPhase::Closed) {
            match input {
                SessionInput::SessionCall { reply: id, .. } => reply(fx, id, Err(SessionError::Closed)),
                SessionInput::SessionResume { reply: id } => fx.push(SessionEffect::ResumeReply { id, answer: Err(SessionError::Closed) }),
                SessionInput::Mutate { author, req_id, .. } => author_error(fx, author, req_id, ErrorClass::Permanent, SessionError::Closed),
                SessionInput::HttpFailed { .. } => fx.push(SessionEffect::ExecutorFailed),
                _ => {}
            }
            return;
        }
        if let SessionInput::Revoked { author_ref: Some(_), epoch, .. } = &input {
            self.phase = phase;
            if *epoch != self.epoch || !self.current(*epoch) { return; }
            let batch = self.batch();
            self.external_revoked.insert(batch, *epoch);
            fx.push(SessionEffect::CommitRevoked { batch, epoch: *epoch });
            return;
        }
        if let SessionInput::Committed(committed) = &input {
            if let Some(epoch) = self.external_revoked.remove(&committed.batch) {
                self.phase = phase;
                fx.push(SessionEffect::ServerEnded(ServerEnded { epoch }));
                return;
            }
        }
        if let SessionInput::Rejected(rejected) = &input {
            if self.external_revoked.remove(&rejected.batch).is_some() {
                self.phase = phase;
                match rejected.reason {
                    RejectReason::Storage(_) | RejectReason::Invalid(_) | RejectReason::StaleGeneration { .. } => fx.push(SessionEffect::ExecutorFailed),
                    RejectReason::NoSession | RejectReason::Closed => {}
                }
                return;
            }
        }
        if let SessionInput::HttpFailed { attempt_id } = &input {
            if !matches!(&phase, SessionPhase::Calling { phase: AttemptPhase::InFlight(id), .. } if id == attempt_id) {
                self.phase = phase;
                fx.push(SessionEffect::ExecutorFailed);
                return;
            }
        }
        if let Some(error) = self.restore_error.clone() {
            match &input {
                SessionInput::SessionCall { reply: id, .. } => {
                    self.phase = phase;
                    reply(fx, *id, Err(SessionError::Storage(error)));
                    return;
                }
                SessionInput::Mutate { author, req_id, epoch, .. } if *epoch == self.epoch => {
                    self.phase = phase;
                    author_error(fx, *author, *req_id, ErrorClass::Permanent, SessionError::Storage(error));
                    return;
                }
                _ => {}
            }
        }
        match &input {
            SessionInput::SessionCall { reply: id, body, .. } if carries_temp_id(body) => {
                self.phase = phase;
                reply(fx, *id, Err(SessionError::Invalid));
                return;
            }
            SessionInput::Mutate { author, req_id, epoch, body, .. } if *epoch == self.epoch && carries_temp_id(body) => {
                self.phase = phase;
                author_error(fx, *author, *req_id, ErrorClass::Permanent, SessionError::Invalid);
                return;
            }
            _ => {}
        }
        match input {
            SessionInput::Closing => self.closing(phase, fx),
            SessionInput::SessionResume { reply: id } => {
                self.phase = phase;
                if let Some((_, waiters)) = &mut self.resume_taking { waiters.push(id); }
                else if let Some(answer) = self.restored_answer.clone() {
                    match answer {
                        RestoredAnswer::Saved(_) => self.take_resume(vec![id], fx),
                        RestoredAnswer::Local(error) => { self.restored_answer = None; fx.push(SessionEffect::ResumeReply { id, answer: Err(error) }); }
                    }
                }
                else if self.restored_direct.is_some() || self.restored_active.is_some() || self.resume_published.is_some() || self.resume_saving.is_some() { self.resume_waiters.push(id); }
                else { fx.push(SessionEffect::ResumeReply { id, answer: Ok(None) }); }
            }
            SessionInput::Online(Online(online)) if self.online.is_some() => self.online_changed(phase, online, fx),
            SessionInput::Online(_) => self.phase = phase,
            SessionInput::Snapshot { epoch, online } if epoch == self.epoch && self.current(epoch) => self.online_changed(phase, online, fx),
            SessionInput::Snapshot { .. } => self.phase = phase,
            SessionInput::Published(Published { epoch, identity, state, .. }) => {
                if epoch != self.epoch && matches!(state, PublishedState::Session | PublishedState::NoSession) {
                    self.online = None;
                    fx.push(SessionEffect::SnapshotRequest { epoch });
                }
                self.epoch = epoch;
                self.published = state;
                self.phase = phase;
                if std::mem::take(&mut self.adopt) { self.adopt_epoch(epoch); }
                self.answer_publication(fx, epoch, state, identity);
                if matches!(self.phase, SessionPhase::Idle) { self.drain(fx); }
            }
            SessionInput::Restored { epoch, pending, resume_answer, .. } => {
                self.phase = phase;
                fx.push(SessionEffect::RestoreClassified { epoch });
                self.restored = true;
                if epoch == self.epoch && self.current(epoch) && self.restored_answer.is_none() { self.restored_answer = resume_answer.map(RestoredAnswer::Saved); }
                let early = std::mem::take(&mut self.released);
                let released = matches!((&self.phase, &pending), (SessionPhase::PendingHeld { pending: held }, Some(saved)) if held.request_id == saved.request_id && (early.contains(&saved.request_id) || saved.owner == Some(MutateOwner::Detached) && held.owner != saved.owner));
                if let (SessionPhase::PendingHeld { pending: held }, true) = (self.phase.clone(), released) {
                    self.start_calling(PendingCall { author: None, owner: Some(MutateOwner::Detached), ..held }, fx);
                } else if let (Some(pending), true) = (pending, epoch == self.epoch && self.current(epoch)) {
                    match pending.owner {
                        None if pending.author.is_none() => self.restored_direct = Some(PendingCall { epoch, ..pending }),
                        Some(MutateOwner::Detached) => self.restored_detached = Some(PendingCall { epoch, ..pending }),
                        Some(MutateOwner::Transfer { .. } | MutateOwner::Orphan { .. }) => self.restored_transfer = Some(PendingCall { epoch, author: Some(TRANSFER_AUTHOR), ..pending }),
                        _ => {}
                    }
                }
                if self.restore_ready && matches!(self.phase, SessionPhase::Idle) { self.start_restored(fx); }
            }
            SessionInput::RestoreReady { epoch } => {
                self.phase = phase;
                if epoch == self.epoch && self.current(epoch) { self.restore_ready = true; }
                if self.restore_ready && matches!(self.phase, SessionPhase::Idle) { self.start_restored(fx); }
                else if self.restore_ready { self.drive_held_transfer(fx); }
            }
            SessionInput::RestoreFailed { epoch, error } => {
                self.phase = phase;
                if epoch == self.epoch {
                    self.restore_error = Some(error);
                    fx.push(SessionEffect::RestoreClassified { epoch });
                }
            }
            SessionInput::Cancel { req_id } => {
                self.phase = phase;
                if let Some(at) = self.queue.iter().position(|queued| matches!(queued, Queued::Mutate { req_id: queued_id, .. } if *queued_id == req_id)) {
                    if let Some(removed) = self.queue.remove(at) { refuse(fx, removed, SessionError::NotSent); }
                }
            }
            SessionInput::Release { req_id } => self.release(phase, req_id, fx),
            SessionInput::EpochEnded(EpochEnded { epoch, cause }) => {
                if epoch != self.epoch { self.phase = phase; return; }
                if cause == Cause::Close { self.phase = phase; return; }
                self.write_settled();
                if cause != Cause::SessionStarted || self.resume_published.is_none() {
                    self.end_resume(SessionError::NoSession, fx);
                }
                self.restored_direct = None;
                self.restored_detached = None;
                self.restored_transfer = None;
                self.restore_ready = false;
                self.restore_floor = epoch + 1;
                self.restore_error = None;
                if is_ended(cause) {
                    if self.after_publish.as_ref().is_some_and(|held| held.wait == PublishWait::Identity) {
                        if let Some(held) = self.after_publish.take() { reply(fx, held.reply, Err(SessionError::NoSession)); }
                    }
                    self.held = None;
                }
                self.on_epoch_ended(phase, epoch, cause, fx);
                if is_ended(cause) { self.report_failed(fx); }
            }
            input => self.by_phase(phase, input, fx),
        }
    }

    /// The author of `pending` hears `Dropped`, the pair goes on with no author (it keeps one only where its outcome lands after `Committed`).
    fn detached(pending: PendingCall, fx: &mut Vec<SessionEffect>) -> PendingCall {
        if let Some(author) = pending.author { fx.push(SessionEffect::Answer { author, req_id: pending.request_id, outcome: AuthorOutcome::Dropped }); }
        let keeps = matches!(pending.owner, Some(MutateOwner::Operation { .. } | MutateOwner::Unmatched { .. } | MutateOwner::Transfer { step: crate::owners::write::upload::UploadStage::Init, .. }))
            || (pending.owner == Some(MutateOwner::Detached) && pending.author == Some(OUTBOX_AUTHOR));
        let author = pending.author.filter(|_| keeps);
        PendingCall { author, owner: Some(MutateOwner::Detached), ..pending }
    }

    pub(super) fn release(&mut self, phase: SessionPhase, req_id: RequestId, fx: &mut Vec<SessionEffect>) {
        if let Some(at) = self.queue.iter().position(|queued| matches!(queued, Queued::Mutate { req_id: queued_id, .. } if *queued_id == req_id)) {
            self.phase = phase;
            if let Some(removed) = self.queue.remove(at) { refuse(fx, removed, SessionError::NotSent); }
            return;
        }
        match phase {
            SessionPhase::Sealing { pending, .. } if pending.request_id == req_id && pending.author.is_some() => {
                if let Some(author) = pending.author { author_error(fx, author, req_id, ErrorClass::Permanent, SessionError::NotSent); }
                self.idle(fx);
            }
            SessionPhase::PendingHeld { pending } if pending.request_id == req_id => {
                let pending = Self::detached(pending, fx);
                self.start_calling(pending, fx);
            }
            SessionPhase::Persisting { pending, batch } if pending.request_id == req_id => self.phase = SessionPhase::Persisting { pending: Self::detached(pending, fx), batch },
            SessionPhase::Calling { pending, attempts, phase } if pending.request_id == req_id => self.phase = SessionPhase::Calling { pending: Self::detached(pending, fx), attempts, phase },
            SessionPhase::Landing { pending, batch, answer, identity, sequence, server_ended } if pending.request_id == req_id => self.phase = SessionPhase::Landing { pending: Self::detached(pending, fx), batch, answer, identity, sequence, server_ended },
            SessionPhase::Blocked { pending: Some(pending), reason } if pending.request_id == req_id => self.phase = SessionPhase::Blocked { pending: Some(Self::detached(pending, fx)), reason },
            SessionPhase::Idle if !self.restored => {
                self.released.insert(req_id);
                self.phase = SessionPhase::Idle;
            }
            phase => self.phase = phase,
        }
    }

    pub(super) fn on_epoch_ended(&mut self, phase: SessionPhase, epoch: Epoch, cause: Cause, fx: &mut Vec<SessionEffect>) {
        match phase {
            SessionPhase::Sealing { pending, .. } => {
                finish(fx, &pending, Err(SessionError::NoSession));
                self.refuse_queue(fx, SessionError::NoSession);
                if is_ended(cause) { self.credential = None; }
                self.phase = SessionPhase::Idle;
            }
            SessionPhase::Idle => {
                if is_ended(cause) { self.credential = None; }
                self.phase = SessionPhase::Idle;
            }
            SessionPhase::Generating { call, .. } if is_ended(cause) => {
                refuse(fx, call.into(), SessionError::NoSession);
                self.refuse_queue(fx, SessionError::NoSession);
                self.credential = None;
                self.phase = SessionPhase::Idle;
            }
            SessionPhase::Storing { call, credential_new, op, after } if is_ended(cause) => {
                self.credential = None;
                let after = if matches!(after, AfterStore::Drain) { AfterStore::Drain } else { AfterStore::Clear(SessionError::NoSession) };
                self.phase = SessionPhase::Storing { call, credential_new, op, after };
            }
            SessionPhase::ClearingSecret { call, reason, op, after } => {
                if is_ended(cause) { self.credential = None; }
                self.phase = SessionPhase::ClearingSecret { call, reason, op, after };
            }
            SessionPhase::PendingHeld { .. } | SessionPhase::Blocked { .. } | SessionPhase::SecretMismatch | SessionPhase::SecretUnavailable { .. } if is_ended(cause) => {
                self.credential = None;
                self.refuse_queue(fx, SessionError::NoSession);
                self.idle(fx);
            }
            phase @ (SessionPhase::Persisting { .. } | SessionPhase::Calling { .. } | SessionPhase::Landing { .. }) => {
                let Some(pending) = pending_of(&phase).filter(|pending| epoch == pending.epoch && is_ended(cause)) else {
                    self.phase = phase;
                    return;
                };
                finish(fx, &pending, Err(SessionError::NoSession));
                if let SessionPhase::Calling { phase: AttemptPhase::InFlight(attempt_id), .. } = phase { fx.push(SessionEffect::HttpCancel { attempt_id }); }
                self.refuse_queue(fx, SessionError::NoSession);
                self.credential = None;
                self.idle(fx);
            }
            other => self.phase = other,
        }
    }

    pub(super) fn closing(&mut self, phase: SessionPhase, fx: &mut Vec<SessionEffect>) {
        let taking = self.resume_taking.take();
        let answer = if taking.is_some() { self.restored_answer.take() } else { None };
        self.end_resume(SessionError::Closed, fx);
        self.resume_taking = taking;
        if self.resume_taking.is_some() { self.restored_answer = answer; }
        if let Some(held) = self.after_publish.take() { reply(fx, held.reply, Err(SessionError::Closed)); }
        match phase {
            SessionPhase::Sealing { pending, .. } => finish(fx, &pending, Err(SessionError::Closed)),
            SessionPhase::Persisting { pending, .. } | SessionPhase::Landing { pending, .. } => finish(fx, &pending, Err(SessionError::Closed)),
            SessionPhase::Calling { pending, phase, .. } => {
                finish(fx, &pending, Err(SessionError::Closed));
                if let AttemptPhase::InFlight(attempt_id) = phase { fx.push(SessionEffect::HttpCancel { attempt_id }); }
            }
            SessionPhase::Generating { call, .. } => refuse(fx, call.into(), SessionError::Closed),
            SessionPhase::Storing { call, credential_new, op, .. } => {
                self.refuse_queue(fx, SessionError::Closed);
                self.phase = SessionPhase::Storing { call, credential_new, op, after: AfterStore::Drain };
                return;
            }
            SessionPhase::ClearingSecret { call, reason, op, .. } => {
                self.refuse_queue(fx, SessionError::Closed);
                self.phase = SessionPhase::ClearingSecret { call, reason, op, after: AfterClear::Drain };
                return;
            }
            _ => {}
        }
        self.report_failed(fx);
        self.drained(fx);
    }

    pub(super) fn online_changed(&mut self, phase: SessionPhase, online: bool, fx: &mut Vec<SessionEffect>) {
        self.online = Some(online);
        match phase {
            SessionPhase::Calling { pending, attempts, phase: AttemptPhase::InFlight(attempt_id) } if !online => {
                fx.push(SessionEffect::HttpCancel { attempt_id });
                if let Some(author) = pending.author {
                    fx.push(SessionEffect::Answer { author, req_id: pending.request_id, outcome: AuthorOutcome::Dropped });
                    self.phase = SessionPhase::PendingHeld { pending };
                } else {
                    self.phase = SessionPhase::Calling { pending, attempts, phase: AttemptPhase::WaitingNetwork };
                }
            }
            SessionPhase::Calling { pending, attempts, phase: AttemptPhase::WaitingNetwork } if online => self.request_http(pending, attempts, fx),
            SessionPhase::Calling { pending, attempts, phase: AttemptPhase::WaitingRetry } if !online => self.phase = SessionPhase::Calling { pending, attempts, phase: AttemptPhase::WaitingNetwork },
            other => self.phase = other,
        }
    }

    pub(super) fn open(&mut self, credential: Option<Credential>, pending: Option<PendingCall>, dropped_pending: bool, secret: SecretState, fx: &mut Vec<SessionEffect>) {
        self.credential = credential.clone();
        if dropped_pending {
            self.phase = SessionPhase::Blocked { pending: None, reason: Block::StateLost };
            return;
        }
        let secret = match secret {
            SecretState::Error(error) => {
                self.phase = SessionPhase::SecretUnavailable { error, read: None };
                return;
            }
            other => other,
        };
        self.phase = SessionPhase::Idle;
        match (credential, secret) {
            (None, SecretState::Missing) => {}
            (None, _) => self.clearing(None, None, AfterClear::Idle, fx),
            (Some(_), SecretState::Missing) => self.missing_secret(fx),
            (Some(credential), SecretState::Present(digest)) if digest != credential.digest => self.phase = SessionPhase::SecretMismatch,
            (Some(_), _) => {
                match pending {
                    Some(pending) if pending.owner.as_ref().and_then(author_of).is_some() => {
                        let author = pending.owner.as_ref().and_then(author_of).or(pending.author);
                        self.adopt = true;
                        self.phase = SessionPhase::PendingHeld { pending: PendingCall { reply: None, author, ..pending } };
                    }
                    _ => {}
                }
            }
        }
    }

    pub(super) fn missing_secret(&mut self, fx: &mut Vec<SessionEffect>) {
        self.credential = None;
        fx.push(SessionEffect::SessionReset);
    }

    pub(super) fn idle_call(&mut self, operation: String, body: String, id: ReplyId, fx: &mut Vec<SessionEffect>) {
        self.phase = SessionPhase::Idle;
        if self.def(&operation).is_none() {
            reply(fx, id, Err(SessionError::Invalid));
            return;
        }
        match self.published {
            PublishedState::EndingFailed | PublishedState::StorageFailed => reply(fx, id, Err(SessionError::Storage("the session could not be saved".into()))),
            PublishedState::Ending => self.queue.push_back(Queued::Call { operation, body, reply: id }),
            _ if self.awaits_publication() => self.queue.push_back(Queued::Call { operation, body, reply: id }),
            _ => self.begin(Queued::Call { operation, body, reply: id }, fx),
        }
    }

    pub(super) fn held_call(&mut self, held: SessionPhase, operation: String, body: String, id: ReplyId, error: SessionError, fx: &mut Vec<SessionEffect>) {
        match self.def(&operation) {
            None => {
                reply(fx, id, Err(SessionError::Invalid));
                self.phase = held;
            }
            Some(def) if def.class != OperationClass::Sessionless => {
                reply(fx, id, Err(error));
                self.phase = held;
            }
            Some(_) => {
                self.held = Some(held);
                self.phase = SessionPhase::Idle;
                self.begin(Queued::Call { operation, body, reply: id }, fx);
            }
        }
    }

    pub(super) fn by_phase(&mut self, phase: SessionPhase, input: SessionInput, fx: &mut Vec<SessionEffect>) {
        match (phase, input) {
            (SessionPhase::Sealing { pending, seal_id }, SessionInput::Sealed { seal_id: id, size }) if id == seal_id => {
                if size > 262144 { finish(fx, &pending, Err(SessionError::TooLarge { size, limit: 262144 })); self.idle(fx); }
                else { self.persist(pending, None, fx); }
            }
            (SessionPhase::Sealing { pending, seal_id }, SessionInput::SealFailed { seal_id: id, error }) if id == seal_id => {
                finish(fx, &pending, Err(error));
                self.idle(fx);
            }
            (phase @ SessionPhase::Sealing { .. }, SessionInput::SessionCall { operation, body, reply }) => {
                self.phase = phase;
                self.queue.push_back(Queued::Call { operation, body, reply });
            }
            (phase @ SessionPhase::Sealing { .. }, SessionInput::Mutate { author, owner, req_id, epoch, path, body, purpose }) => {
                self.phase = phase;
                self.queue_mutate(Queued::Mutate { author, owner, req_id, epoch, path, body, purpose });
            }
            (SessionPhase::Idle, SessionInput::Open { credential, pending, dropped_pending, secret }) => self.open(credential, pending, dropped_pending, secret, fx),
            (phase, SessionInput::Open { .. }) => self.phase = phase,
            (SessionPhase::Idle, SessionInput::SessionCall { operation, body, reply: id }) => {
                if let Some(error) = self.admission(None) { self.phase = SessionPhase::Idle; reply(fx, id, Err(error)); }
                else { self.idle_call(operation, body, id, fx); }
            }
            (SessionPhase::Idle, SessionInput::Mutate { author, owner, req_id, epoch, path, body, purpose }) => {
                self.phase = SessionPhase::Idle;
                if self.awaits_publication() {
                    self.queue_mutate(Queued::Mutate { author, owner, req_id, epoch, path, body, purpose });
                    return;
                }
                self.begin(Queued::Mutate { author, owner, req_id, epoch, path, body, purpose }, fx);
            }
            (SessionPhase::SecretMismatch, SessionInput::SessionCall { operation, body, reply: id }) => self.held_call(SessionPhase::SecretMismatch, operation, body, id, SessionError::StateLost, fx),
            (SessionPhase::SecretMismatch, SessionInput::Mutate { author, req_id, .. }) => {
                self.phase = SessionPhase::SecretMismatch;
                author_error(fx, author, req_id, ErrorClass::Permanent, SessionError::StateLost);
            }
            (phase @ SessionPhase::SecretUnavailable { .. }, SessionInput::SessionCall { operation, body, reply: id }) => {
                let error = SessionError::Storage("the secret is unavailable".into());
                self.held_call(phase, operation, body, id, error, fx);
            }
            (SessionPhase::SecretUnavailable { error, read }, SessionInput::Mutate { author, req_id, .. }) => {
                self.phase = SessionPhase::SecretUnavailable { error, read };
                author_error(fx, author, req_id, ErrorClass::Network, SessionError::Storage("the secret is unavailable".into()));
            }
            (SessionPhase::SecretUnavailable { error, .. }, SessionInput::Foreground { .. }) => {
                let op = self.secret_op();
                fx.push(SessionEffect::ReadSecret { secret_op_id: op });
                self.phase = SessionPhase::SecretUnavailable { error, read: Some(op) };
            }
            (SessionPhase::SecretUnavailable { error, read }, SessionInput::SecretRead { secret_op_id, read: found }) if read == Some(secret_op_id) => match found {
                SecretState::Present(digest) if self.credential.as_ref().is_some_and(|credential| credential.digest == digest) => self.idle(fx),
                SecretState::Present(_) => self.phase = SessionPhase::SecretMismatch,
                SecretState::Missing => {
                    self.phase = SessionPhase::Idle;
                    self.missing_secret(fx);
                }
                SecretState::Error(_) => self.phase = SessionPhase::SecretUnavailable { error, read: None },
            },
            (SessionPhase::Generating { call, op }, SessionInput::CredentialGenerated { secret_op_id, credential }) if secret_op_id == op => {
                let next = self.secret_op();
                fx.push(SessionEffect::StoreSecret { credential, secret_op_id: next });
                self.phase = SessionPhase::Storing { call, credential_new: credential, op: next, after: AfterStore::Persist };
            }
            (SessionPhase::Storing { call, credential_new, op, after }, SessionInput::SecretStored { secret_op_id, ok }) if secret_op_id == op => match (after, ok) {
                (AfterStore::Persist, true) => {
                    let LoginCall { operation, body, reply: id } = call;
                    let request_id = self.request();
                    let epoch = self.epoch;
                    self.persist(PendingCall { request_id, sequence: 1, class: OperationClass::Login, operation: operation.name, path: operation.path, body, document: None, credential: Some(credential_new), purpose: 0, author: None, owner: None, epoch, reply: Some(id) }, Some(credential_new), fx);
                }
                (AfterStore::Persist, false) => self.clearing(Some(call.into()), Some(Err(SessionError::Storage("the secret could not be stored".into()))), AfterClear::Idle, fx),
                (AfterStore::Clear(reason), _) => self.clearing(Some(call.into()), Some(Err(reason)), AfterClear::Idle, fx),
                (AfterStore::Drain, _) => {
                    refuse(fx, call.into(), SessionError::Closed);
                    self.drained(fx);
                }
            },
            (SessionPhase::ClearingSecret { call, reason, op, after }, SessionInput::SecretCleared { secret_op_id, .. }) if secret_op_id == op => {
                match after {
                    AfterClear::Idle => {
                        if let (Some(call), Some(reason)) = (call, reason) {
                            match (call, reason) {
                                (Queued::Call { reply: id, .. }, answer) => reply(fx, id, answer),
                                (call, Err(error)) => refuse(fx, call, error),
                                (call, Ok(_)) => refuse(fx, call, SessionError::Invalid),
                            }
                        }
                        if let Some(answer) = self.resume_clearing.take() { self.deliver_resume(answer, fx); }
                        self.report_failed(fx);
                        self.idle(fx);
                    }
                    AfterClear::Drain => {
                        if let Some(call) = call { refuse(fx, call, SessionError::Closed); }
                        self.report_failed(fx);
                        self.drained(fx);
                    }
                }
            }
            (phase @ (SessionPhase::Generating { .. } | SessionPhase::Storing { .. } | SessionPhase::ClearingSecret { .. }), SessionInput::SessionCall { operation, body, reply: id }) => {
                self.phase = phase;
                self.queue.push_back(Queued::Call { operation, body, reply: id });
            }
            (phase @ (SessionPhase::Generating { .. } | SessionPhase::Storing { .. } | SessionPhase::ClearingSecret { .. }), SessionInput::Mutate { author, owner, req_id, epoch, path, body, purpose }) => {
                self.phase = phase;
                self.queue.push_back(Queued::Mutate { author, owner, req_id, epoch, path, body, purpose });
            }
            (SessionPhase::PendingHeld { pending }, SessionInput::Mutate { author, req_id, .. }) if req_id == pending.request_id => self.start_calling(PendingCall { author: Some(author), ..pending }, fx),
            (SessionPhase::PendingHeld { pending }, SessionInput::SessionCall { operation, body, reply: id }) => {
                self.queue.push_back(Queued::Call { operation, body, reply: id });
                self.phase = SessionPhase::PendingHeld { pending };
            }
            (SessionPhase::PendingHeld { pending }, SessionInput::Mutate { author, owner, req_id, epoch, path, body, purpose }) => {
                self.phase = SessionPhase::PendingHeld { pending };
                self.queue_mutate(Queued::Mutate { author, owner, req_id, epoch, path, body, purpose });
            }
            (SessionPhase::Blocked { pending, reason }, SessionInput::SessionCall { reply: id, .. }) => {
                self.phase = SessionPhase::Blocked { pending, reason };
                reply(fx, id, Err(blocked_error(reason)));
            }
            (SessionPhase::Blocked { pending, reason }, SessionInput::Mutate { author, req_id, .. }) => {
                let same = pending.as_ref().is_some_and(|pending| pending.request_id == req_id);
                match (same, pending.is_some()) {
                    (true, _) => author_error_retained(fx, author, req_id, ErrorClass::Permanent, blocked_error(reason), true),
                    (false, true) => author_error(fx, author, req_id, ErrorClass::Permanent, blocked_error(reason)),
                    (false, false) => author_error(fx, author, req_id, ErrorClass::Permanent, SessionError::StateLost),
                }
                self.phase = SessionPhase::Blocked { pending, reason };
            }
            (SessionPhase::Persisting { pending, batch }, SessionInput::Committed(Committed { batch: committed, .. })) if committed == batch => {
                self.write_settled();
                if let Some(error) = self.detach_error.take() {
                    self.resume_finish_retained(&pending, Err(error), true, fx);
                    fx.push(SessionEffect::ScheduleTick { attempts: 0 });
                    self.phase = SessionPhase::Calling { pending: PendingCall { reply: None, ..pending }, attempts: 0, phase: AttemptPhase::WaitingRetry };
                } else { self.start_calling(pending, fx); }
            }
            (SessionPhase::Persisting { pending, batch }, SessionInput::Rejected(Rejected { batch: rejected, reason })) if rejected == batch => match reason {
                RejectReason::Invalid(_) => {
                    self.write_settled();
                    self.detach_error = None;
                    self.resume_finish(&pending, Err(SessionError::Invalid), fx);
                    self.idle(fx);
                }
                RejectReason::Storage(_) => {
                    self.phase = SessionPhase::Persisting { pending, batch };
                    self.write_refused(fx);
                }
                RejectReason::NoSession | RejectReason::Closed => self.phase = SessionPhase::Persisting { pending, batch },
                RejectReason::StaleGeneration { .. } => { self.phase = SessionPhase::Persisting { pending, batch }; fx.push(SessionEffect::ExecutorFailed); }
            },
            (SessionPhase::Persisting { pending, batch }, SessionInput::Tick) if !self.write_in_flight => {
                self.write_in_flight = true;
                fx.push(SessionEffect::CommitPending { batch, epoch: pending.epoch, pending: pending.clone(), session: self.write_session });
                self.phase = SessionPhase::Persisting { pending, batch };
            }
            (phase @ (SessionPhase::Persisting { .. } | SessionPhase::Calling { .. } | SessionPhase::Landing { .. }), SessionInput::Mutate { author, owner, req_id, epoch, path, body, purpose }) => {
                let current = pending_of(&phase).is_some_and(|pending| pending.request_id == req_id);
                self.phase = phase;
                if !current { self.queue_mutate(Queued::Mutate { author, owner, req_id, epoch, path, body, purpose }); }
            }
            (phase @ (SessionPhase::Persisting { .. } | SessionPhase::Calling { .. } | SessionPhase::Landing { .. }), SessionInput::SessionCall { operation, body, reply: id }) => {
                self.phase = phase;
                self.queue.push_back(Queued::Call { operation, body, reply: id });
            }
            (SessionPhase::Calling { pending, attempts, phase }, input) => self.calling(pending, attempts, phase, input, fx),
            (SessionPhase::Landing { pending, batch, answer, identity, sequence, server_ended }, SessionInput::Committed(Committed { batch: committed, .. })) if committed == batch => {
                self.write_settled();
                match identity.filter(|_| answer.is_ok()) {
                    Some(digest) => {
                        self.credential = Some(Credential { digest, sequence: pending.sequence });
                        if let (Some(id), Ok(_)) = (pending.reply, &answer) { self.after_publish = Some(AnswerAfterPublish { reply: id, result: answer.clone(), wait: PublishWait::Identity }); }
                        if self.restored_active == Some(pending.request_id) { if let Ok(body) = &answer { self.resume_published = Some(body.clone()); } }
                        fx.push(SessionEffect::CredentialCommitted(CredentialCommitted { identity: digest }));
                    }
                    None if login_failed(&pending, &answer, server_ended) => {
                        self.credential = None;
                        if self.restored_active == Some(pending.request_id) { self.resume_clearing = Some(answer.clone()); }
                        let call = pending.reply.map(|id| Queued::Call { operation: pending.operation.clone(), body: pending.body.clone(), reply: id });
                        let reason = call.as_ref().map(|_| answer);
                        self.clearing(call, reason, AfterClear::Idle, fx);
                        return;
                    }
                    None => {
                        if let (Some(sequence), Some(credential)) = (sequence, &mut self.credential) { credential.sequence = sequence; }
                        let mut pending = pending;
                        if server_ended {
                            fx.push(SessionEffect::ServerEnded(ServerEnded { epoch: pending.epoch }));
                            if let (Some(id), None) = (pending.reply, pending.author) {
                                self.after_publish = Some(AnswerAfterPublish { reply: id, result: answer.clone(), wait: PublishWait::EpochAfter(pending.epoch) });
                                pending.reply = None;
                            }
                            pending.author = None;
                        }
                        if !server_ended || self.restored_active != Some(pending.request_id) { self.resume_finish(&pending, answer, fx); }
                    }
                }
                if self.failed_batch == Some(batch) { self.report_failed(fx); }
                self.idle(fx);
            }
            (SessionPhase::Landing { pending, batch, answer, identity, sequence, server_ended }, SessionInput::Rejected(Rejected { batch: rejected, reason })) if rejected == batch => match reason {
                RejectReason::Invalid(_) => {
                    self.write_settled();
                    let mut delivered = pending.clone();
                    if server_ended {
                        fx.push(SessionEffect::ServerEnded(ServerEnded { epoch: pending.epoch }));
                        if let (Some(id), None) = (delivered.reply, delivered.author) {
                            self.after_publish = Some(AnswerAfterPublish { reply: id, result: answer.clone(), wait: PublishWait::EpochAfter(pending.epoch) });
                            delivered.reply = None;
                        }
                        delivered.author = None;
                    }
                    if !server_ended || self.restored_active != Some(delivered.request_id) { self.resume_finish_retained(&delivered, answer, true, fx); }
                    if self.failed_batch == Some(batch) { self.report_failed(fx); }
                    self.phase = SessionPhase::Blocked { pending: Some(PendingCall { reply: None, ..pending }), reason: Block::Invalid };
                }
                RejectReason::Storage(_) => {
                    self.phase = SessionPhase::Landing { pending, batch, answer, identity, sequence, server_ended };
                    self.write_refused(fx);
                }
                RejectReason::NoSession | RejectReason::Closed => self.phase = SessionPhase::Landing { pending, batch, answer, identity, sequence, server_ended },
                RejectReason::StaleGeneration { .. } => { self.phase = SessionPhase::Landing { pending, batch, answer, identity, sequence, server_ended }; fx.push(SessionEffect::ExecutorFailed); }
            },
            (SessionPhase::Landing { pending, batch, answer, identity, sequence, server_ended }, SessionInput::Tick) if !self.write_in_flight => {
                self.land(pending, answer, identity, sequence, server_ended, fx);
                if let (Some(failed), SessionPhase::Landing { batch: renewed, .. }) = (self.failed_batch, &self.phase) { if failed == batch { self.failed_batch = Some(*renewed); } }
            }
            (phase, _) => self.phase = phase,
        }
    }

    pub(super) fn calling(&mut self, pending: PendingCall, attempts: u64, phase: AttemptPhase, input: SessionInput, fx: &mut Vec<SessionEffect>) {
        let mine = |request_id: RequestId, attempt_id: AttemptId| pending.request_id == request_id && phase == AttemptPhase::InFlight(attempt_id);
        if matches!(&input, SessionInput::HttpFailed { attempt_id } if mine(pending.request_id, *attempt_id)) {
            self.land(pending, Err(SessionError::Http { status: 0 }), None, None, false, fx);
            if let SessionPhase::Landing { batch, .. } = &self.phase { self.failed_batch = Some(*batch); }
            return;
        }
        match input {
            SessionInput::GateClosed { request_id, attempt_id } if mine(request_id, attempt_id) => {
                self.phase = SessionPhase::Calling { pending, attempts, phase: AttemptPhase::WaitingNetwork };
            }
            SessionInput::Response { request_id, attempt_id, body, sequence, refusal, kind } if mine(request_id, attempt_id) => {
                if let crate::transport::AnswerKind::Transient { reason } = &kind {
                    if let Some(author) = pending.author {
                        fx.push(SessionEffect::Answer { author, req_id: pending.request_id, outcome: AuthorOutcome::Response { body, kind, pending_retained: false } });
                        self.phase = SessionPhase::PendingHeld { pending };
                    } else { self.retry_direct(pending, attempts, SessionError::Graphql { code: reason.clone(), body }, fx); }
                    return;
                }
                if matches!(kind, crate::transport::AnswerKind::DocumentMissing) {
                    if let Some(document) = pending.document.clone() {
                        self.persist(PendingCall { body: document, document: None, ..pending }, None, fx);
                        return;
                    }
                }
                self.answer_kind = Some(kind);
                let answer = if pending.author.is_none() {
                    let value = serde_json::from_str::<serde_json::Value>(&body).ok();
                    match value.as_ref().filter(|value| !value.get("data").is_some_and(|data| !data.is_null())).and_then(crate::transport::refusal) {
                        Some(crate::transport::Refusal::Graphql(code)) => Err(SessionError::Graphql { code, body: body.clone() }),
                        _ => Ok(body.clone()),
                    }
                } else { Ok(body.clone()) };
                if let Some(code) = refusal {
                    let confirmed = (pending.class != OperationClass::Sessionless && sequence == Some(pending.sequence)).then_some(pending.sequence);
                    let error = SessionError::Session { code, body: body.clone() };
                    self.land(pending, Err(error), None, confirmed, false, fx);
                } else if pending.class == OperationClass::Sessionless {
                    self.land(pending, answer, None, None, false, fx);
                } else if sequence == Some(pending.sequence) {
                    let identity = pending.credential;
                    let sequence = pending.sequence;
                    self.land(pending, answer, identity, Some(sequence), false, fx);
                } else if sequence.is_none() {
                    self.land(pending, answer, None, None, false, fx);
                } else {
                    self.resume_finish_retained(&pending, Err(SessionError::SequenceConflict), true, fx);
                    self.phase = SessionPhase::Blocked { pending: Some(PendingCall { reply: None, ..pending }), reason: Block::SequenceConflict };
                }
            }
            SessionInput::Revoked { request_id, attempt_id, body, .. } if mine(request_id, attempt_id) => {
                self.answer_kind = Some(crate::transport::AnswerKind::Revoked);
                let answer = if pending.class == OperationClass::Logout { Ok(body) } else { Err(SessionError::Revoked { body }) };
                self.land(pending, answer, None, None, true, fx);
            }
            SessionInput::HttpError { request_id, attempt_id, security: Some((code, detail)), .. } if mine(request_id, attempt_id) => self.land(pending, Err(SessionError::Security { code, detail }), None, None, false, fx),
            SessionInput::HttpError { request_id, attempt_id, temporary: true, status, body, .. } if mine(request_id, attempt_id) => {
                if let Some(author) = pending.author {
                    match body {
                        Some(body) => {
                            let outcome = match serde_json::from_str(&body) {
                                Ok(value) => AuthorOutcome::Response { body, kind: crate::transport::classify_answer(&crate::transport::WireOutcome::Answer { sequence: None, body: value }, false, true), pending_retained: false },
                                Err(_) => AuthorOutcome::Error { class: ErrorClass::Permanent, error: SessionError::Invalid, pending_retained: true },
                            };
                            fx.push(SessionEffect::Answer { author, req_id: pending.request_id, outcome });
                        }
                        None => author_error_retained(fx, author, pending.request_id, ErrorClass::Network, SessionError::Http { status }, true),
                    }
                    self.phase = SessionPhase::PendingHeld { pending };
                } else {
                    let error = if let Some(body) = body {
                        let code = serde_json::from_str(&body).ok().and_then(|value| crate::transport::refusal_name(&value)).unwrap_or_default();
                        SessionError::Graphql { code, body }
                    } else if status == 0 { SessionError::Network } else { SessionError::Http { status } };
                    self.retry_direct(pending, attempts, error, fx);
                }
            }
            SessionInput::AnswerTooLarge { request_id, attempt_id, limit } if mine(request_id, attempt_id) => {
                if let Some(author) = pending.author {
                    author_error_retained(fx, author, pending.request_id, ErrorClass::Network, SessionError::ResponseTooLarge { limit }, true);
                    self.phase = SessionPhase::PendingHeld { pending };
                } else if pending.class == OperationClass::Sessionless { self.land(pending, Err(SessionError::ResponseTooLarge { limit }), None, None, false, fx); }
                else { self.retry_direct(pending, attempts, SessionError::ResponseTooLarge { limit }, fx); }
            }
            SessionInput::HttpError { request_id, attempt_id, auth, status, too_large, .. } if mine(request_id, attempt_id) => {
                let error = match (auth, too_large) {
                    (true, _) => SessionError::Unauthorized,
                    (false, Some((size, limit))) => SessionError::TooLarge { size, limit },
                    (false, None) => SessionError::Http { status },
                };
                self.land(pending, Err(error), None, None, false, fx);
            }
            SessionInput::Dropped { request_id, attempt_id } if mine(request_id, attempt_id) => {
                if let Some(author) = pending.author {
                    fx.push(SessionEffect::Answer { author, req_id: pending.request_id, outcome: AuthorOutcome::Dropped });
                    self.phase = SessionPhase::PendingHeld { pending };
                } else { self.retry_direct(pending, attempts, SessionError::Network, fx); }
            }
            SessionInput::SecretMissing { request_id, attempt_id, lost } if mine(request_id, attempt_id) => {
                if lost {
                    self.resume_finish(&pending, Err(SessionError::StateLost), fx);
                    self.phase = SessionPhase::SecretMismatch;
                } else {
                    self.phase = SessionPhase::Calling { pending, attempts, phase: AttemptPhase::WaitingSecret };
                }
            }
            SessionInput::Tick if phase == AttemptPhase::WaitingRetry && self.is_online() => self.request_http(pending, attempts, fx),
            SessionInput::Foreground { .. } if phase == AttemptPhase::WaitingSecret => {
                if self.is_online() {
                    self.request_http(pending, attempts, fx);
                } else {
                    self.phase = SessionPhase::Calling { pending, attempts, phase: AttemptPhase::WaitingNetwork };
                }
            }
            _ => self.phase = SessionPhase::Calling { pending, attempts, phase },
        }
    }
}
