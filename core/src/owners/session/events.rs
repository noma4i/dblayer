use super::*;

impl SessionState {
    pub(super) fn run(&mut self, input: SessionInput, fx: &mut Vec<SessionEffect>) {
        let phase = std::mem::replace(&mut self.phase, SessionPhase::Closed);
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
        match input {
            SessionInput::Closing => self.closing(phase, fx),
            SessionInput::SessionResume { reply: id } => {
                self.phase = phase;
                if let Some(answer) = self.restored_answer.take() { fx.push(SessionEffect::ResumeReply { id, answer: answer.map(Some) }); }
                else if self.restored_direct.is_some() || self.restored_active.is_some() || self.resume_published.is_some() { self.resume_waiters.push(id); }
                else { fx.push(SessionEffect::ResumeReply { id, answer: Ok(None) }); }
            }
            SessionInput::InterestPlus { stream_id } => {
                self.subscribers.insert(stream_id);
                self.phase = phase;
            }
            SessionInput::InterestMinus { stream_id } => {
                self.subscribers.remove(&stream_id);
                self.phase = phase;
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
            SessionInput::Restored { epoch, pending, .. } => {
                self.phase = phase;
                fx.push(SessionEffect::RestoreClassified { epoch });
                self.restored = true;
                let early = std::mem::take(&mut self.released);
                let released = matches!((&self.phase, &pending), (SessionPhase::PendingHeld { pending: held }, Some(saved)) if held.request_id == saved.request_id && (early.contains(&saved.request_id) || saved.owner == Some(MutateOwner::Detached) && held.owner != saved.owner));
                if let (SessionPhase::PendingHeld { pending: held }, true) = (self.phase.clone(), released) {
                    self.start_calling(PendingCall { author: None, owner: Some(MutateOwner::Detached), ..held }, fx);
                } else if let (Some(pending), true) = (pending, epoch == self.epoch && self.current(epoch)) {
                    match pending.owner {
                        None if pending.author.is_none() => self.restored_direct = Some(PendingCall { epoch, ..pending }),
                        Some(MutateOwner::Detached) => self.restored_detached = Some(PendingCall { epoch, ..pending }),
                        _ => {}
                    }
                }
                if self.restore_ready && matches!(self.phase, SessionPhase::Idle) { self.start_restored(fx); }
            }
            SessionInput::RestoreReady { epoch } => {
                self.phase = phase;
                if epoch == self.epoch && self.current(epoch) { self.restore_ready = true; }
                if self.restore_ready && matches!(self.phase, SessionPhase::Idle) { self.start_restored(fx); }
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
                    if let Some(removed) = self.queue.remove(at) { refuse(fx, removed, SessionError::Interrupted); }
                }
            }
            SessionInput::Release { req_id } => match phase {
                SessionPhase::PendingHeld { pending } if pending.request_id == req_id => self.start_calling(PendingCall { author: None, owner: Some(MutateOwner::Detached), ..pending }, fx),
                SessionPhase::Landing { pending, batch, answer, identity, sequence, server_ended } if pending.request_id == req_id => self.phase = SessionPhase::Landing { pending: PendingCall { author: None, owner: Some(MutateOwner::Detached), ..pending }, batch, answer, identity, sequence, server_ended },
                SessionPhase::Blocked { pending: Some(pending), reason, landing } if pending.request_id == req_id => self.phase = SessionPhase::Blocked { pending: Some(PendingCall { author: None, owner: Some(MutateOwner::Detached), ..pending }), reason, landing },
                SessionPhase::Idle if !self.restored => {
                    self.released.insert(req_id);
                    self.phase = SessionPhase::Idle;
                }
                phase => self.phase = phase,
            },
            SessionInput::EpochEnded(EpochEnded { epoch, cause }) => {
                if epoch != self.epoch { self.phase = phase; return; }
                if cause != Cause::SessionStarted || self.resume_published.is_none() {
                    self.end_resume(SessionError::Interrupted, fx);
                }
                self.restored_direct = None;
                self.restored_detached = None;
                self.restore_ready = false;
                self.restore_floor = epoch + 1;
                self.restore_error = None;
                if is_ended(cause) {
                    if self.after_publish.as_ref().is_some_and(|held| held.wait == PublishWait::Identity) {
                        if let Some(held) = self.after_publish.take() { reply(fx, held.reply, Err(SessionError::Interrupted)); }
                    }
                    self.held = None;
                }
                self.on_epoch_ended(phase, epoch, cause, fx);
                if is_ended(cause) { self.report_failed(fx); }
            }
            input => self.by_phase(phase, input, fx),
        }
    }

    pub(super) fn on_epoch_ended(&mut self, phase: SessionPhase, epoch: Epoch, cause: Cause, fx: &mut Vec<SessionEffect>) {
        match phase {
            SessionPhase::Sealing { pending, .. } => {
                let error = if pending.author.is_some() { SessionError::NoSession } else { SessionError::Interrupted };
                finish(fx, &pending, Err(error));
                self.refuse_queue(fx, SessionError::NoSession);
                if is_ended(cause) { self.credential = None; }
                self.phase = SessionPhase::Idle;
            }
            SessionPhase::Idle => {
                if is_ended(cause) { self.credential = None; }
                self.phase = SessionPhase::Idle;
            }
            SessionPhase::Generating { call, .. } if is_ended(cause) => {
                refuse(fx, call.into(), SessionError::Interrupted);
                self.refuse_queue(fx, SessionError::NoSession);
                self.credential = None;
                self.phase = SessionPhase::Idle;
            }
            SessionPhase::Storing { call, credential_new, op, after } if is_ended(cause) => {
                self.credential = None;
                let after = if matches!(after, AfterStore::Drain) { AfterStore::Drain } else { AfterStore::Clear(SessionError::Interrupted) };
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
                finish(fx, &pending, Err(SessionError::Interrupted));
                if let SessionPhase::Calling { phase: AttemptPhase::InFlight(attempt_id), .. } = phase { fx.push(SessionEffect::HttpCancel { attempt_id }); }
                self.refuse_queue(fx, SessionError::NoSession);
                self.credential = None;
                self.idle(fx);
            }
            other => self.phase = other,
        }
    }

    pub(super) fn closing(&mut self, phase: SessionPhase, fx: &mut Vec<SessionEffect>) {
        self.end_resume(SessionError::Closed, fx);
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
            SessionPhase::Blocked { pending: Some(pending), reason: Block::Storage, landing: Some(landed) } if online && pending.author.is_none() => self.land_again(pending, landed, fx),
            other => self.phase = other,
        }
    }

    pub(super) fn open(&mut self, credential: Option<Credential>, pending: Option<PendingCall>, dropped_pending: bool, secret: SecretState, fx: &mut Vec<SessionEffect>) {
        self.credential = credential.clone();
        if dropped_pending {
            self.phase = SessionPhase::Blocked { pending: None, reason: Block::StateLost, landing: None };
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
            (SessionPhase::Idle, SessionInput::SessionCall { operation, body, reply: id }) => self.idle_call(operation, body, id, fx),
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
                (AfterStore::Persist, false) => self.clearing(Some(call.into()), Some(SessionError::Storage("the secret could not be stored".into())), AfterClear::Idle, fx),
                (AfterStore::Clear(reason), _) => self.clearing(Some(call.into()), Some(reason), AfterClear::Idle, fx),
                (AfterStore::Drain, _) => {
                    refuse(fx, call.into(), SessionError::Closed);
                    self.drained(fx);
                }
            },
            (SessionPhase::ClearingSecret { call, reason, op, after }, SessionInput::SecretCleared { secret_op_id, .. }) if secret_op_id == op => {
                match after {
                    AfterClear::Idle => {
                        if let (Some(call), Some(reason)) = (call, reason) { refuse(fx, call, reason); }
                        if let Some(error) = self.resume_clearing.take() { self.deliver_resume(Err(error), fx); }
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
            (SessionPhase::Blocked { pending, reason, landing }, SessionInput::SessionCall { reply: id, .. }) => {
                self.phase = SessionPhase::Blocked { pending, reason, landing };
                reply(fx, id, Err(SessionError::StateLost));
            }
            (SessionPhase::Blocked { pending, reason, landing }, SessionInput::Mutate { author, req_id, .. }) => {
                let same = pending.as_ref().is_some_and(|pending| pending.request_id == req_id);
                match (same, pending, reason, landing) {
                    (true, Some(pending), Block::Storage, Some(landed)) if pending.author.is_some() => self.land_again(pending, landed, fx),
                    (true, pending, reason, landing) => {
                        author_error_retained(fx, author, req_id, ErrorClass::Permanent, blocked_error(reason), true);
                        self.phase = SessionPhase::Blocked { pending, reason, landing };
                    }
                    (false, Some(pending), reason, landing) => {
                        author_error(fx, author, req_id, ErrorClass::Permanent, blocked_error(reason));
                        self.phase = SessionPhase::Blocked { pending: Some(pending), reason, landing };
                    }
                    (false, None, reason, landing) => {
                        author_error(fx, author, req_id, ErrorClass::Permanent, SessionError::StateLost);
                        self.phase = SessionPhase::Blocked { pending: None, reason, landing };
                    }
                }
            }
            (SessionPhase::Blocked { pending: Some(pending), reason: Block::Storage, landing: Some(landed) }, SessionInput::Tick(_)) if self.is_online() && pending.author.is_none() => self.land_again(pending, landed, fx),
            (SessionPhase::Persisting { pending, batch }, SessionInput::Committed(Committed { batch: committed, .. })) if committed == batch => self.start_calling(pending, fx),
            (SessionPhase::Persisting { pending, batch }, SessionInput::Rejected(Rejected { batch: rejected, reason })) if rejected == batch => {
                self.resume_finish(&pending, Err(SessionError::Rejected(reason)), fx);
                self.idle(fx);
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
                match identity.filter(|_| answer.is_ok()) {
                    Some(digest) => {
                        self.credential = Some(Credential { digest, sequence: pending.sequence });
                        if let (Some(id), Ok(_)) = (pending.reply, &answer) { self.after_publish = Some(AnswerAfterPublish { reply: id, result: answer.clone(), wait: PublishWait::Identity }); }
                        if self.restored_active == Some(pending.request_id) { if let Ok(body) = &answer { self.resume_published = Some(body.clone()); } }
                        fx.push(SessionEffect::CredentialCommitted(CredentialCommitted { identity: digest }));
                    }
                    None if login_failed(&pending, &answer, server_ended) => {
                        self.credential = None;
                        if self.restored_active == Some(pending.request_id) { self.resume_clearing = answer.clone().err(); }
                        let call = pending.reply.map(|id| Queued::Call { operation: pending.operation.clone(), body: pending.body.clone(), reply: id });
                        let reason = call.as_ref().and(answer.err());
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
                        }
                        self.resume_finish(&pending, answer, fx);
                    }
                }
                if self.failed_batch == Some(batch) { self.report_failed(fx); }
                self.idle(fx);
            }
            (SessionPhase::Landing { pending, batch, answer, identity, sequence, server_ended }, SessionInput::Rejected(Rejected { batch: rejected, reason })) if rejected == batch => {
                self.resume_finish_retained(&pending, Err(SessionError::Rejected(reason)), true, fx);
                if self.failed_batch == Some(batch) { self.report_failed(fx); }
                self.phase = SessionPhase::Blocked { pending: Some(PendingCall { reply: None, ..pending }), reason: Block::Storage, landing: Some(Landed { answer, identity, sequence, server_ended }) };
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
                if matches!(kind, crate::transport::AnswerKind::Transient { .. }) {
                    if let Some(author) = pending.author {
                        fx.push(SessionEffect::Answer { author, req_id: pending.request_id, outcome: AuthorOutcome::Response { body, kind } });
                        self.phase = SessionPhase::PendingHeld { pending };
                    } else if self.is_online() {
                        fx.push(SessionEffect::ScheduleTick { attempts: attempts + 1 });
                        self.phase = SessionPhase::Calling { pending, attempts: attempts + 1, phase: AttemptPhase::WaitingRetry };
                    } else { self.phase = SessionPhase::Calling { pending, attempts, phase: AttemptPhase::WaitingNetwork }; }
                    return;
                }
                self.answer_kind = Some(kind);
                if let Some(code) = refusal {
                    let confirmed = (pending.class != OperationClass::Sessionless && sequence == Some(pending.sequence)).then_some(pending.sequence);
                    let error = if code == crate::transport::SessionCode::Unauthenticated { SessionError::Unauthorized } else { SessionError::Session(code) };
                    self.land(pending, Err(error), None, confirmed, false, fx);
                } else if pending.class == OperationClass::Sessionless {
                    self.land(pending, Ok(body), None, None, false, fx);
                } else if sequence == Some(pending.sequence) {
                    let identity = pending.credential;
                    let sequence = pending.sequence;
                    self.land(pending, Ok(body), identity, Some(sequence), false, fx);
                } else if sequence.is_none() && pending.document.is_some() && crate::transport::encrypted::document_missing(&body) {
                    let document = pending.document.clone();
                    self.persist(PendingCall { body: document.unwrap_or_default(), document: None, ..pending }, None, fx);
                } else if sequence.is_none() && pending.class == OperationClass::Login {
                    let code = answer_code(&body);
                    self.land(pending, Err(SessionError::Session(code)), None, None, false, fx);
                } else if sequence.is_none() {
                    self.land(pending, Ok(body), None, None, false, fx);
                } else {
                    self.resume_finish_retained(&pending, Err(SessionError::SequenceConflict), true, fx);
                    self.phase = SessionPhase::Blocked { pending: Some(PendingCall { reply: None, ..pending }), reason: Block::SequenceConflict, landing: None };
                }
            }
            SessionInput::Revoked { request_id, attempt_id, body } if mine(request_id, attempt_id) => {
                self.answer_kind = Some(crate::transport::AnswerKind::Revoked);
                let answer = if pending.class == OperationClass::Logout { Ok(body) } else { Err(SessionError::Revoked) };
                self.land(pending, answer, None, None, true, fx);
            }
            SessionInput::HttpError { request_id, attempt_id, security: Some((code, detail)), .. } if mine(request_id, attempt_id) => self.land(pending, Err(SessionError::Security { code, detail }), None, None, false, fx),
            SessionInput::HttpError { request_id, attempt_id, temporary: true, status, body, .. } if mine(request_id, attempt_id) => {
                if let Some(author) = pending.author {
                    match body {
                        Some(body) => {
                            let outcome = match serde_json::from_str(&body) {
                                Ok(value) => AuthorOutcome::Response { body, kind: crate::transport::classify_answer(&crate::transport::WireOutcome::Answer { sequence: None, body: value }, false, true) },
                                Err(_) => AuthorOutcome::Error { class: ErrorClass::Permanent, error: SessionError::Invalid, pending_retained: true },
                            };
                            fx.push(SessionEffect::Answer { author, req_id: pending.request_id, outcome });
                        }
                        None => author_error_retained(fx, author, pending.request_id, ErrorClass::Network, SessionError::Http { status }, true),
                    }
                    self.phase = SessionPhase::PendingHeld { pending };
                } else if self.is_online() {
                    fx.push(SessionEffect::ScheduleTick { attempts: attempts + 1 });
                    self.phase = SessionPhase::Calling { pending, attempts: attempts + 1, phase: AttemptPhase::WaitingRetry };
                } else {
                    self.phase = SessionPhase::Calling { pending, attempts, phase: AttemptPhase::WaitingNetwork };
                }
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
                } else if self.is_online() {
                    fx.push(SessionEffect::ScheduleTick { attempts: attempts + 1 });
                    self.phase = SessionPhase::Calling { pending, attempts: attempts + 1, phase: AttemptPhase::WaitingRetry };
                } else {
                    self.phase = SessionPhase::Calling { pending, attempts, phase: AttemptPhase::WaitingNetwork };
                }
            }
            SessionInput::SecretMissing { request_id, attempt_id, lost } if mine(request_id, attempt_id) => {
                if lost {
                    self.resume_finish(&pending, Err(SessionError::StateLost), fx);
                    self.phase = SessionPhase::SecretMismatch;
                } else {
                    self.phase = SessionPhase::Calling { pending, attempts, phase: AttemptPhase::WaitingSecret };
                }
            }
            SessionInput::Tick(_) if phase == AttemptPhase::WaitingRetry && self.is_online() => self.request_http(pending, attempts, fx),
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
