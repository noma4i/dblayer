use super::*;

impl Session {
    pub(super) fn run(&mut self, input: Input, out: &mut Out) {
        let state = std::mem::replace(&mut self.state, State::Closed);
        if matches!(state, State::Closed) {
            match input {
                Input::SessionCall { reply, .. } => out.replies.push((reply, Err(Fail::Closed))),
                Input::Mutate { author, req_id, .. } => out.effects.push(author_error(author, req_id, Kind::Permanent, Fail::Closed)),
                _ => {}
            }
            return;
        }
        if self.restore_error.is_some() {
            match &input {
                Input::SessionCall { reply, .. } => {
                    self.state = state;
                    out.replies.push((*reply, Err(Fail::Storage)));
                    return;
                }
                Input::Mutate { author, req_id, epoch, .. } if *epoch == self.epoch => {
                    self.state = state;
                    out.effects.push(author_error(*author, *req_id, Kind::Permanent, Fail::Storage));
                    return;
                }
                _ => {}
            }
        }
        match input {
            Input::Closing => self.closing(state, out),
            Input::InterestPlus(stream) => {
                self.subscribers.insert(stream);
                self.state = state;
            }
            Input::InterestMinus(stream) => {
                self.subscribers.remove(&stream);
                self.state = state;
            }
            Input::Online(online) if self.online.is_some() => self.online_changed(state, online, out),
            Input::Online(_) => self.state = state,
            Input::Snapshot { epoch, online } if epoch == self.epoch && self.current(epoch) => self.online_changed(state, online, out),
            Input::Snapshot { .. } => self.state = state,
            Input::Published { epoch, identity, state: published } => {
                if epoch != self.epoch && matches!(published, PubState::Session | PubState::NoSession) {
                    self.online = None;
                    out.effects.push(Effect::SnapshotRequest { epoch });
                }
                self.epoch = epoch;
                self.pub_state = published;
                self.state = state;
                if std::mem::take(&mut self.adopt) { self.adopt_epoch(epoch); }
                self.answer_publication(out, epoch, published, identity);
                if matches!(self.state, State::Idle) { self.drain(out); }
            }
            Input::Restored { epoch, pending } => {
                self.state = state;
                out.effects.push(Effect::RestoreClassified { epoch });
                self.restored = true;
                let early = std::mem::take(&mut self.released);
                let released = matches!((&self.state, &pending), (State::PendingHeld { pending: held }, Some(saved)) if held.request_id == saved.request_id && (early.contains(&saved.request_id) || saved.owner == Some(Owner::Detached) && held.owner != saved.owner));
                if let (State::PendingHeld { pending: held }, true) = (self.state.clone(), released) {
                    self.start_calling(Pending { author: None, owner: Some(Owner::Detached), ..held }, out);
                } else if let (Some(pending), true) = (pending, epoch == self.epoch && self.current(epoch)) {
                    match pending.owner {
                        None => self.restored_direct = Some(Pending { epoch, ..pending }),
                        Some(Owner::Detached) => self.restored_detached = Some(Pending { epoch, ..pending }),
                        Some(_) => {}
                    }
                }
                if self.restore_ready && matches!(self.state, State::Idle) { self.start_restored(out); }
            }
            Input::RestoreReady { epoch } => {
                self.state = state;
                if epoch == self.epoch && self.current(epoch) { self.restore_ready = true; }
                if self.restore_ready && matches!(self.state, State::Idle) { self.start_restored(out); }
            }
            Input::RestoreFailed { epoch, error } => {
                self.state = state;
                if epoch == self.epoch {
                    self.restore_error = Some(error);
                    out.effects.push(Effect::RestoreClassified { epoch });
                }
            }
            Input::Cancel { req_id } => {
                self.state = state;
                if let Some(at) = self.queue.iter().position(|queued| matches!(queued, Queued::Mutate { req_id: queued_id, .. } if *queued_id == req_id)) {
                    if let Some(removed) = self.queue.remove(at) { Self::refuse(out, removed, Fail::Interrupted); }
                }
            }
            Input::Release { req_id } => match state {
                State::PendingHeld { pending } if pending.request_id == req_id => self.start_calling(Pending { author: None, owner: Some(Owner::Detached), ..pending }, out),
                State::Landing { pending, batch, answer, identity, sequence, server_ended } if pending.request_id == req_id => self.state = State::Landing { pending: Pending { author: None, owner: Some(Owner::Detached), ..pending }, batch, answer, identity, sequence, server_ended },
                State::Blocked { pending: Some(pending), reason, landing } if pending.request_id == req_id => self.state = State::Blocked { pending: Some(Pending { author: None, owner: Some(Owner::Detached), ..pending }), reason, landing },
                State::Idle if !self.restored => {
                    self.released.insert(req_id);
                    self.state = State::Idle;
                }
                state => self.state = state,
            },
            Input::EpochEnded { epoch, cause } => {
                if epoch != self.epoch { self.state = state; return; }
                self.restored_direct = None;
                self.restored_detached = None;
                self.restore_ready = false;
                self.restore_floor = epoch + 1;
                self.restore_error = None;
                if is_ended(cause) {
                    if self.after_publish.as_ref().is_some_and(|held| held.2.is_none()) {
                        if let Some((reply, ..)) = self.after_publish.take() { out.replies.push((reply, Err(Fail::Interrupted))); }
                    }
                    self.held = None;
                }
                self.on_epoch_ended(state, epoch, cause, out);
            }
            input => self.by_state(state, input, out),
        }
    }

    pub(super) fn on_epoch_ended(&mut self, state: State, epoch: u64, cause: EndCause, out: &mut Out) {
        match state {
            State::Sealing { pending, .. } => {
                let error = if pending.author.is_some() { Fail::NoSession } else { Fail::Interrupted };
                Self::finish(out, &pending, Err(error));
                self.refuse_queue(out, Fail::NoSession);
                if is_ended(cause) { self.credential = None; }
                self.state = State::Idle;
            }
            State::Idle => {
                if is_ended(cause) { self.credential = None; }
                self.state = State::Idle;
            }
            State::Generating { call, .. } if is_ended(cause) => {
                Self::refuse(out, call, Fail::Interrupted);
                self.refuse_queue(out, Fail::NoSession);
                self.credential = None;
                self.state = State::Idle;
            }
            State::Storing { call, credential_new, op, after } if is_ended(cause) => {
                self.credential = None;
                let after = if matches!(after, AfterStore::Drain) { AfterStore::Drain } else { AfterStore::Clear(Fail::Interrupted) };
                self.state = State::Storing { call, credential_new, op, after };
            }
            State::ClearingSecret { call, reason, op, after } => {
                if is_ended(cause) { self.credential = None; }
                self.state = State::ClearingSecret { call, reason, op, after };
            }
            State::PendingHeld { .. } | State::Blocked { .. } | State::SecretMismatch | State::SecretUnavailable { .. } if is_ended(cause) => {
                self.credential = None;
                self.refuse_queue(out, Fail::NoSession);
                self.idle(out);
            }
            state @ (State::Persisting { .. } | State::Calling { .. } | State::Landing { .. }) => {
                let Some(pending) = pending_of(&state).filter(|pending| epoch == pending.epoch && is_ended(cause)) else {
                    self.state = state;
                    return;
                };
                Self::finish(out, &pending, Err(Fail::Interrupted));
                if let State::Calling { phase: CallPhase::InFlight(attempt_id), .. } = state { out.effects.push(Effect::HttpCancel { attempt_id }); }
                self.refuse_queue(out, Fail::NoSession);
                self.credential = None;
                self.idle(out);
            }
            other => self.state = other,
        }
    }

    pub(super) fn closing(&mut self, state: State, out: &mut Out) {
        if let Some((reply, ..)) = self.after_publish.take() { out.replies.push((reply, Err(Fail::Closed))); }
        match state {
            State::Sealing { pending, .. } => Self::finish(out, &pending, Err(Fail::Closed)),
            State::Persisting { pending, .. } | State::Landing { pending, .. } => Self::finish(out, &pending, Err(Fail::Closed)),
            State::Calling { pending, phase, .. } => {
                Self::finish(out, &pending, Err(Fail::Closed));
                if let CallPhase::InFlight(attempt_id) = phase { out.effects.push(Effect::HttpCancel { attempt_id }); }
            }
            State::Generating { call, .. } => Self::refuse(out, call, Fail::Closed),
            State::Storing { call, credential_new, op, .. } => {
                self.refuse_queue(out, Fail::Closed);
                self.state = State::Storing { call, credential_new, op, after: AfterStore::Drain };
                return;
            }
            State::ClearingSecret { call, reason, op, .. } => {
                self.refuse_queue(out, Fail::Closed);
                self.state = State::ClearingSecret { call, reason, op, after: AfterClear::Drain };
                return;
            }
            _ => {}
        }
        self.drained(out);
    }

    pub(super) fn online_changed(&mut self, state: State, online: bool, out: &mut Out) {
        self.online = Some(online);
        match state {
            State::Calling { pending, attempts, phase: CallPhase::InFlight(attempt_id) } if !online => {
                out.effects.push(Effect::HttpCancel { attempt_id });
                if let Some(author) = pending.author {
                    out.effects.push(Effect::Answer { author, req_id: pending.request_id, outcome: Outcome::Dropped });
                    self.state = State::PendingHeld { pending };
                } else {
                    self.state = State::Calling { pending, attempts, phase: CallPhase::WaitingNetwork };
                }
            }
            State::Calling { pending, attempts, phase: CallPhase::WaitingNetwork } if online => self.send(pending, attempts, out),
            State::Blocked { pending: Some(pending), reason: Block::Storage, landing: Some(landed) } if online && pending.author.is_none() => self.land_again(pending, landed, out),
            other => self.state = other,
        }
    }

    pub(super) fn open(&mut self, credential: Option<Credential>, pending: Option<Pending>, dropped_pending: bool, secret: Secret, out: &mut Out) {
        self.credential = credential;
        if dropped_pending {
            self.state = State::Blocked { pending: None, reason: Block::StateLost, landing: None };
            return;
        }
        let secret = match secret {
            Secret::Error(error) => {
                self.state = State::SecretUnavailable { error, read: None };
                return;
            }
            other => other,
        };
        self.state = State::Idle;
        match (credential, secret) {
            (None, Secret::Missing) => {}
            (None, _) => self.clearing(None, None, AfterClear::Idle, out),
            (Some(_), Secret::Missing) => self.missing_secret(out),
            (Some(credential), Secret::Present(digest)) if digest != credential.digest => self.state = State::SecretMismatch,
            (Some(_), _) => {
                match pending {
                    Some(pending) if pending.owner.as_ref().and_then(author_of).is_some() => {
                        let author = pending.owner.as_ref().and_then(author_of).or(pending.author);
                        self.adopt = true;
                        self.state = State::PendingHeld { pending: Pending { reply: None, author, ..pending } };
                    }
                    _ => {}
                }
            }
        }
    }

    pub(super) fn missing_secret(&mut self, out: &mut Out) {
        self.credential = None;
        out.effects.push(Effect::SessionReset);
    }

    pub(super) fn idle_call(&mut self, operation: String, body: String, reply: ReplyId, out: &mut Out) {
        self.state = State::Idle;
        if self.def(&operation).is_none() {
            out.replies.push((reply, Err(Fail::Invalid)));
            return;
        }
        match self.pub_state {
            PubState::EndingFailed | PubState::StorageFailed => out.replies.push((reply, Err(Fail::Storage))),
            PubState::Ending => self.queue.push_back(Queued::Call { operation, body, reply }),
            _ if self.after_publish.is_some() => self.queue.push_back(Queued::Call { operation, body, reply }),
            _ => self.begin(Queued::Call { operation, body, reply }, out),
        }
    }

    pub(super) fn held_call(&mut self, held: State, operation: String, body: String, reply: ReplyId, fail: Fail, out: &mut Out) {
        match self.def(&operation) {
            None => {
                out.replies.push((reply, Err(Fail::Invalid)));
                self.state = held;
            }
            Some(def) if def.class != Class::Sessionless => {
                out.replies.push((reply, Err(fail)));
                self.state = held;
            }
            Some(_) => {
                self.held = Some(held);
                self.state = State::Idle;
                self.begin(Queued::Call { operation, body, reply }, out);
            }
        }
    }

    pub(super) fn by_state(&mut self, state: State, input: Input, out: &mut Out) {
        match (state, input) {
            (State::Sealing { pending, seal_id }, Input::Sealed { seal_id: id, size }) if id == seal_id => {
                if size > 262144 { Self::finish(out, &pending, Err(Fail::TooLarge { size, limit: 262144 })); self.idle(out); }
                else { self.persist(pending, None, out); }
            }
            (State::Sealing { pending, seal_id }, Input::SealFailed { seal_id: id, error }) if id == seal_id => {
                Self::finish(out, &pending, Err(error));
                self.idle(out);
            }
            (state @ State::Sealing { .. }, Input::SessionCall { operation, body, reply }) => {
                self.state = state;
                self.queue.push_back(Queued::Call { operation, body, reply });
            }
            (state @ State::Sealing { .. }, Input::Mutate { author, owner, req_id, epoch, path, body, purpose }) => {
                self.state = state;
                self.queue_mutate(Queued::Mutate { author, owner, req_id, epoch, path, body, purpose });
            }
            (State::Idle, Input::Open { credential, pending, dropped_pending, secret }) => self.open(credential, pending, dropped_pending, secret, out),
            (state, Input::Open { .. }) => self.state = state,
            (State::Idle, Input::SessionCall { operation, body, reply }) => self.idle_call(operation, body, reply, out),
            (State::Idle, Input::Mutate { author, owner, req_id, epoch, path, body, purpose }) => {
                self.state = State::Idle;
                if self.after_publish.is_some() {
                    self.queue_mutate(Queued::Mutate { author, owner, req_id, epoch, path, body, purpose });
                    return;
                }
                self.begin(Queued::Mutate { author, owner, req_id, epoch, path, body, purpose }, out);
            }
            (State::SecretMismatch, Input::SessionCall { operation, body, reply }) => self.held_call(State::SecretMismatch, operation, body, reply, Fail::StateLost, out),
            (State::SecretMismatch, Input::Mutate { author, req_id, .. }) => {
                self.state = State::SecretMismatch;
                out.effects.push(author_error(author, req_id, Kind::Permanent, Fail::StateLost));
            }
            (state @ State::SecretUnavailable { .. }, Input::SessionCall { operation, body, reply }) => self.held_call(state, operation, body, reply, Fail::Storage, out),
            (State::SecretUnavailable { error, read }, Input::Mutate { author, req_id, .. }) => {
                self.state = State::SecretUnavailable { error, read };
                out.effects.push(author_error(author, req_id, Kind::Temporary, Fail::Storage));
            }
            (State::SecretUnavailable { error, .. }, Input::Foreground) => {
                let op = self.secret_op();
                out.effects.push(Effect::ReadSecret { op });
                self.state = State::SecretUnavailable { error, read: Some(op) };
            }
            (State::SecretUnavailable { error, read }, Input::SecretRead { op, read: found }) if read == Some(op) => match found {
                Secret::Present(digest) if self.credential.as_ref().is_some_and(|credential| credential.digest == digest) => self.idle(out),
                Secret::Present(_) => self.state = State::SecretMismatch,
                Secret::Missing => {
                    self.state = State::Idle;
                    self.missing_secret(out);
                }
                Secret::Error(_) => self.state = State::SecretUnavailable { error, read: None },
            },
            (State::Generating { call, op }, Input::CredentialGenerated { op: seen, credential }) if seen == op => {
                let next = self.secret_op();
                out.effects.push(Effect::StoreSecret { credential, op: next });
                self.state = State::Storing { call, credential_new: credential, op: next, after: AfterStore::Persist };
            }
            (State::Storing { call, credential_new, op, after }, Input::SecretStored { op: seen, ok }) if seen == op => match (after, ok) {
                (AfterStore::Persist, true) => {
                    let Queued::Call { operation, body, reply } = call else { unreachable!("a generating call is a call") };
                    let def = self.def(&operation).expect("a generating call is declared");
                    let request_id = self.request();
                    let epoch = self.epoch;
                    self.persist(Pending { request_id, sequence: 1, class: Class::Login, operation, path: def.path, body, document: None, credential: Some(credential_new), purpose: 0, author: None, owner: None, epoch, reply: Some(reply) }, Some(credential_new), out);
                }
                (AfterStore::Persist, false) => self.clearing(Some(call), Some(Fail::Storage), AfterClear::Idle, out),
                (AfterStore::Clear(reason), _) => self.clearing(Some(call), Some(reason), AfterClear::Idle, out),
                (AfterStore::Drain, _) => {
                    Self::refuse(out, call, Fail::Closed);
                    self.drained(out);
                }
            },
            (State::ClearingSecret { call, reason, op, after }, Input::SecretCleared { op: seen, .. }) if seen == op => {
                match after {
                    AfterClear::Idle => {
                        if let (Some(call), Some(reason)) = (call, reason) { Self::refuse(out, call, reason); }
                        self.idle(out);
                    }
                    AfterClear::Drain => {
                        if let Some(call) = call { Self::refuse(out, call, Fail::Closed); }
                        self.drained(out);
                    }
                }
            }
            (state @ (State::Generating { .. } | State::Storing { .. } | State::ClearingSecret { .. }), Input::SessionCall { operation, body, reply }) => {
                self.state = state;
                self.queue.push_back(Queued::Call { operation, body, reply });
            }
            (state @ (State::Generating { .. } | State::Storing { .. } | State::ClearingSecret { .. }), Input::Mutate { author, owner, req_id, epoch, path, body, purpose }) => {
                self.state = state;
                self.queue.push_back(Queued::Mutate { author, owner, req_id, epoch, path, body, purpose });
            }
            (State::PendingHeld { pending }, Input::Mutate { author, req_id, .. }) if req_id == pending.request_id => self.start_calling(Pending { author: Some(author), ..pending }, out),
            (State::PendingHeld { pending }, Input::SessionCall { operation, body, reply }) => {
                self.queue.push_back(Queued::Call { operation, body, reply });
                self.state = State::PendingHeld { pending };
            }
            (State::PendingHeld { pending }, Input::Mutate { author, owner, req_id, epoch, path, body, purpose }) => {
                self.state = State::PendingHeld { pending };
                self.queue_mutate(Queued::Mutate { author, owner, req_id, epoch, path, body, purpose });
            }
            (State::Blocked { pending, reason, landing }, Input::SessionCall { reply, .. }) => {
                self.state = State::Blocked { pending, reason, landing };
                out.replies.push((reply, Err(Fail::StateLost)));
            }
            (State::Blocked { pending, reason, landing }, Input::Mutate { author, req_id, .. }) => {
                let same = pending.as_ref().is_some_and(|pending| pending.request_id == req_id);
                match (same, pending, reason, landing) {
                    (true, Some(pending), Block::Storage, Some(landed)) if pending.author.is_some() => self.land_again(pending, landed, out),
                    (true, pending, reason, landing) => {
                        out.effects.push(author_error_retained(author, req_id, Kind::Permanent, blocked_fail(reason), true));
                        self.state = State::Blocked { pending, reason, landing };
                    }
                    (false, Some(pending), reason, landing) => {
                        out.effects.push(author_error(author, req_id, Kind::Permanent, blocked_fail(reason)));
                        self.state = State::Blocked { pending: Some(pending), reason, landing };
                    }
                    (false, None, reason, landing) => {
                        out.effects.push(author_error(author, req_id, Kind::Permanent, Fail::StateLost));
                        self.state = State::Blocked { pending: None, reason, landing };
                    }
                }
            }
            (State::Blocked { pending: Some(pending), reason: Block::Storage, landing: Some(landed) }, Input::Tick) if self.is_online() && pending.author.is_none() => self.land_again(pending, landed, out),
            (State::Persisting { pending, batch }, Input::Committed(committed)) if committed == batch => self.start_calling(pending, out),
            (State::Persisting { pending, batch }, Input::Rejected(rejected, reason)) if rejected == batch => {
                Self::finish(out, &pending, Err(Fail::Reason(reason)));
                self.idle(out);
            }
            (state @ (State::Persisting { .. } | State::Calling { .. } | State::Landing { .. }), Input::Mutate { author, owner, req_id, epoch, path, body, purpose }) => {
                let current = pending_of(&state).is_some_and(|pending| pending.request_id == req_id);
                self.state = state;
                if !current { self.queue_mutate(Queued::Mutate { author, owner, req_id, epoch, path, body, purpose }); }
            }
            (state @ (State::Persisting { .. } | State::Calling { .. } | State::Landing { .. }), Input::SessionCall { operation, body, reply }) => {
                self.state = state;
                self.queue.push_back(Queued::Call { operation, body, reply });
            }
            (State::Calling { pending, attempts, phase }, input) => self.calling(pending, attempts, phase, input, out),
            (State::Landing { pending, batch, answer, identity, sequence, server_ended }, Input::Committed(committed)) if committed == batch => {
                match identity.filter(|_| answer.is_ok()) {
                    Some(digest) => {
                        self.credential = Some(Credential { digest, sequence: pending.sequence });
                        if let (Some(reply), Ok(_)) = (pending.reply, &answer) { self.after_publish = Some((reply, answer.clone(), None)); }
                        out.effects.push(Effect::CredentialCommitted(digest));
                    }
                    None if pending.class == Class::Login && pending.credential.is_some() && answer.is_err() && !server_ended => {
                        self.credential = None;
                        let call = pending.reply.map(|reply| Queued::Call { operation: pending.operation.clone(), body: pending.body.clone(), reply });
                        let reason = if call.is_some() { answer.err() } else { None };
                        self.clearing(call, reason, AfterClear::Idle, out);
                        return;
                    }
                    None => {
                        if let (Some(sequence), Some(credential)) = (sequence, &mut self.credential) { credential.sequence = sequence; }
                        let mut pending = pending;
                        if server_ended {
                            out.effects.push(Effect::ServerEnded { epoch: pending.epoch });
                            if let (Some(reply), None) = (pending.reply, pending.author) {
                                self.after_publish = Some((reply, answer.clone(), Some(pending.epoch)));
                                pending.reply = None;
                            }
                        }
                        Self::finish(out, &pending, answer);
                    }
                }
                self.idle(out);
            }
            (State::Landing { pending, batch, answer, identity, sequence, server_ended }, Input::Rejected(rejected, reason)) if rejected == batch => {
                Self::finish_retained(out, &pending, Err(Fail::Reason(reason)), true);
                self.state = State::Blocked { pending: Some(Pending { reply: None, ..pending }), reason: Block::Storage, landing: Some(Landed { answer, identity, sequence, server_ended }) };
            }
            (state, _) => self.state = state,
        }
    }

    pub(super) fn current(&self, epoch: u64) -> bool { epoch >= self.epoch && epoch >= self.restore_floor }

    pub(super) fn queue_mutate(&mut self, queued: Queued) {
        let Queued::Mutate { req_id, .. } = &queued else { return };
        if !self.queue.iter().any(|entry| matches!(entry, Queued::Mutate { req_id: seen, .. } if seen == req_id)) { self.queue.push_back(queued); }
    }

    pub(super) fn calling(&mut self, pending: Pending, attempts: u64, phase: CallPhase, input: Input, out: &mut Out) {
        let mine = |request_id: RequestId, attempt_id: AttemptId| pending.request_id == request_id && phase == CallPhase::InFlight(attempt_id);
        match input {
            Input::GateClosed { request_id, attempt_id } if mine(request_id, attempt_id) => {
                self.state = State::Calling { pending, attempts, phase: CallPhase::WaitingNetwork };
            }
            Input::Response { request_id, attempt_id, body, sequence, refusal } if mine(request_id, attempt_id) => {
                if let Some(code) = refusal {
                    let confirmed = (pending.class != Class::Sessionless && sequence == Some(pending.sequence)).then_some(pending.sequence);
                    self.land(pending, Err(Fail::Session(code)), None, confirmed, false, out);
                } else if pending.class == Class::Sessionless {
                    self.land(pending, Ok(body), None, None, false, out);
                } else if sequence == Some(pending.sequence) {
                    let identity = pending.credential;
                    let sequence = pending.sequence;
                    self.land(pending, Ok(body), identity, Some(sequence), false, out);
                } else if sequence.is_none() && pending.class == Class::Login {
                    let code = super::code_of_answer(&body);
                    self.land(pending, Err(Fail::Session(code)), None, None, false, out);
                } else if sequence.is_none() && pending.document.is_some() && super::names_missing_document(&body) {
                    let full = pending.document.clone().unwrap_or_default();
                    self.persist(Pending { body: full, document: None, ..pending }, None, out);
                } else if sequence.is_none() {
                    self.land(pending, Ok(body), None, None, false, out);
                } else {
                    Self::finish_retained(out, &pending, Err(Fail::SequenceConflict), true);
                    self.state = State::Blocked { pending: Some(Pending { reply: None, ..pending }), reason: Block::SequenceConflict, landing: None };
                }
            }
            Input::Revoked { request_id, attempt_id, body } if mine(request_id, attempt_id) => {
                let answer = if pending.class == Class::Logout { Ok(body) } else { Err(Fail::Revoked) };
                self.land(pending, answer, None, None, true, out);
            }
            Input::HttpError { request_id, attempt_id, security: Some((code, detail)), .. } if mine(request_id, attempt_id) => self.land(pending, Err(Fail::Security { code, detail }), None, None, false, out),
            Input::HttpError { request_id, attempt_id, temporary: true, body, .. } if mine(request_id, attempt_id) => {
                if let Some(author) = pending.author {
                    out.effects.push(match body {
                        Some(body) => Effect::Answer { author, req_id: pending.request_id, outcome: Outcome::Response(body) },
                        None => author_error_retained(author, pending.request_id, Kind::Temporary, Fail::Http, true),
                    });
                    self.state = State::PendingHeld { pending };
                } else if self.is_online() {
                    out.effects.push(Effect::ScheduleTick { attempts: attempts + 1 });
                    self.state = State::Calling { pending, attempts: attempts + 1, phase: CallPhase::WaitingRetry };
                } else {
                    self.state = State::Calling { pending, attempts, phase: CallPhase::WaitingNetwork };
                }
            }
            Input::HttpError { request_id, attempt_id, auth, .. } if mine(request_id, attempt_id) => {
                let fail = if auth { Fail::Unauthorized } else { Fail::Http };
                self.land(pending, Err(fail), None, None, false, out);
            }
            Input::Dropped { request_id, attempt_id } if mine(request_id, attempt_id) => {
                if let Some(author) = pending.author {
                    out.effects.push(Effect::Answer { author, req_id: pending.request_id, outcome: Outcome::Dropped });
                    self.state = State::PendingHeld { pending };
                } else if self.is_online() {
                    out.effects.push(Effect::ScheduleTick { attempts: attempts + 1 });
                    self.state = State::Calling { pending, attempts: attempts + 1, phase: CallPhase::WaitingRetry };
                } else {
                    self.state = State::Calling { pending, attempts, phase: CallPhase::WaitingNetwork };
                }
            }
            Input::SecretMissing { request_id, attempt_id, lost } if mine(request_id, attempt_id) => {
                if lost {
                    Self::finish(out, &pending, Err(Fail::StateLost));
                    self.state = State::SecretMismatch;
                } else {
                    self.state = State::Calling { pending, attempts, phase: CallPhase::WaitingSecret };
                }
            }
            Input::Tick if phase == CallPhase::WaitingRetry && self.is_online() => self.send(pending, attempts, out),
            Input::Foreground if phase == CallPhase::WaitingSecret => {
                if self.is_online() {
                    self.send(pending, attempts, out);
                } else {
                    self.state = State::Calling { pending, attempts, phase: CallPhase::WaitingNetwork };
                }
            }
            _ => self.state = State::Calling { pending, attempts, phase },
        }
    }
}
