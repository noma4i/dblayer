use super::*;

impl SessionState {
    pub fn new(declared: &[&str], online: bool) -> Self {
        let operations = declared.iter().map(|name| SessionOperation { name: name.to_string(), path: format!("/{name}"), class: OperationClass::Authenticated }).collect();
        Self::with_operations(operations, Some(online))
    }

    pub fn with_operations(operations: Vec<SessionOperation>, online: Option<bool>) -> Self {
        Self { phase: SessionPhase::Idle, credential: None, answer_kind: None, queue: VecDeque::new(), online, epoch: 0, published: PublishedState::Session, after_publish: None, external_revoked: Default::default(), restored_direct: None, restored_detached: None, restored_transfer: None, released: BTreeSet::new(), restored: false, restored_active: None, restored_answer: None, resume_taking: None, resume_saving: None, resume_landing_saved: false, restore_error: None, resume_waiters: Vec::new(), resume_published: None, resume_clearing: None, failed_batch: None, detach_error: None, operations, next_request: 1, next_batch: 1, next_attempt: 1, next_secret: 1, restore_ready: false, restore_floor: 0, adopt: false, held: None }
    }

    pub fn phase(&self) -> &SessionPhase { &self.phase }

    pub fn credential(&self) -> Option<&Credential> { self.credential.as_ref() }

    pub fn queued(&self) -> usize { self.queue.len() }


    #[cfg(test)]
    pub fn awaiting_publication(&self) -> Option<(ReplyId, PublishWait)> { self.after_publish.as_ref().map(|held| (held.reply, held.wait)) }

    pub fn restored_direct(&self) -> Option<&PendingCall> { self.restored_direct.as_ref() }

    pub fn restored_detached(&self) -> Option<&PendingCall> { self.restored_detached.as_ref() }

    pub fn released(&self) -> usize { self.released.len() }

    pub(super) fn batch(&mut self) -> BatchId {
        self.next_batch += 1;
        self.next_batch - 1
    }

    pub(super) fn request(&mut self) -> RequestId {
        self.next_request += 1;
        self.next_request - 1
    }

    pub(super) fn attempt(&mut self) -> AttemptId {
        self.next_attempt += 1;
        self.next_attempt - 1
    }

    pub(super) fn secret_op(&mut self) -> SecretOpId {
        self.next_secret += 1;
        self.next_secret - 1
    }

    pub(super) fn is_online(&self) -> bool { self.online == Some(true) }

    pub(super) fn admission(&self, epoch: Option<Epoch>) -> Option<SessionError> {
        if self.published == PublishedState::Closing { return Some(SessionError::Closed); }
        if epoch.is_some_and(|epoch| epoch != self.epoch) { return Some(SessionError::NoSession); }
        None
    }

    pub(super) fn def(&self, operation: &str) -> Option<SessionOperation> { self.operations.iter().find(|def| def.name == operation).cloned() }

    pub(super) fn refuse_queue(&mut self, fx: &mut Vec<SessionEffect>, error: SessionError) {
        for queued in self.queue.drain(..) { refuse(fx, queued, error.clone()); }
    }

    pub(super) fn idle(&mut self, fx: &mut Vec<SessionEffect>) {
        self.phase = SessionPhase::Idle;
        if let Some(held) = self.held.take() {
            let error = if matches!(held, SessionPhase::SecretMismatch) { SessionError::StateLost } else { SessionError::Storage("the secret is unavailable".into()) };
            self.refuse_queue(fx, error);
            self.phase = held;
            return;
        }
        if self.restore_ready && self.start_restored(fx) { return; }
        self.drain(fx);
    }

    pub(super) fn drain(&mut self, fx: &mut Vec<SessionEffect>) {
        while matches!(self.phase, SessionPhase::Idle) {
            match self.published {
                PublishedState::EndingFailed | PublishedState::StorageFailed => {
                    self.refuse_queue(fx, SessionError::Storage("the session could not be saved".into()));
                    return;
                }
                PublishedState::Ending | PublishedState::Closing => return,
                _ => {}
            }
            if self.awaits_publication() { return; }
            let Some(queued) = self.queue.pop_front() else { return };
            self.begin(queued, fx);
        }
    }

    pub(super) fn begin(&mut self, queued: Queued, fx: &mut Vec<SessionEffect>) {
        match queued {
            Queued::Call { operation, body, reply: id } => {
                if let Some(error) = self.admission(None) { reply(fx, id, Err(error)); return; }
                let Some(def) = self.def(&operation) else {
                    reply(fx, id, Err(SessionError::Invalid));
                    return;
                };
                let epoch = self.epoch;
                let sequence = self.credential.as_ref().map_or(1, |credential| credential.sequence + 1);
                match def.class {
                    OperationClass::Login if self.credential.is_some() => reply(fx, id, Err(SessionError::SessionExists)),
                    OperationClass::Login => self.generate(LoginCall { operation: def, body, reply: id }, fx),
                    OperationClass::Authenticated if self.credential.is_none() => self.generate(LoginCall { operation: def, body, reply: id }, fx),
                    OperationClass::Logout if self.credential.is_none() => reply(fx, id, Err(SessionError::NoCredential)),
                    OperationClass::Sessionless => {
                        let request_id = self.request();
                        self.seal(PendingCall { request_id, sequence: 0, class: def.class, operation, path: def.path, body, document: None, credential: None, purpose: 0, author: None, owner: None, epoch, reply: Some(id) }, fx);
                    }
                    OperationClass::Authenticated | OperationClass::Logout => {
                        let request_id = self.request();
                        let (body, document) = pair_bodies(body);
                        self.seal(PendingCall { request_id, sequence, class: def.class, operation, path: def.path, body, document, credential: None, purpose: 0, author: None, owner: None, epoch, reply: Some(id) }, fx);
                    }
                }
            }
            Queued::Mutate { author, owner, req_id, epoch, path, body, purpose } => {
                if let Some(error) = self.admission(Some(epoch)).or_else(|| self.credential.is_none().then_some(SessionError::NoSession)) {
                    author_error(fx, author, req_id, ErrorClass::Permanent, error);
                    return;
                }
                let sequence = self.credential.as_ref().map_or(1, |credential| credential.sequence + 1);
                let (body, document) = pair_bodies(body);
                self.seal(PendingCall { request_id: req_id, sequence, class: OperationClass::Authenticated, operation: String::new(), path, body, document, credential: None, purpose, author: Some(author), owner, epoch, reply: None }, fx);
            }
        }
    }

    pub(super) fn seal(&mut self, pending: PendingCall, fx: &mut Vec<SessionEffect>) {
        let seal_id = self.attempt();
        fx.push(SessionEffect::SealProbe { seal_id, pending: pending.clone(), credential: self.credential.as_ref().map(|credential| credential.digest) });
        self.phase = SessionPhase::Sealing { pending, seal_id };
    }

    pub(super) fn generate(&mut self, call: LoginCall, fx: &mut Vec<SessionEffect>) {
        let op = self.secret_op();
        fx.push(SessionEffect::GenerateCredential { secret_op_id: op });
        self.phase = SessionPhase::Generating { call, op };
    }

    pub(super) fn persist(&mut self, pending: PendingCall, session: Option<Digest>, fx: &mut Vec<SessionEffect>) {
        let batch = self.batch();
        fx.push(SessionEffect::CommitPending { batch, epoch: pending.epoch, pending: pending.clone(), session });
        self.phase = SessionPhase::Persisting { pending, batch };
    }

    pub(super) fn request_http(&mut self, pending: PendingCall, attempts: u64, fx: &mut Vec<SessionEffect>) {
        let attempt_id = self.attempt();
        fx.push(SessionEffect::HttpRequest { pending: pending.clone(), attempt_id });
        self.phase = SessionPhase::Calling { pending, attempts, phase: AttemptPhase::InFlight(attempt_id) };
    }

    pub(super) fn start_calling(&mut self, pending: PendingCall, fx: &mut Vec<SessionEffect>) {
        if self.is_online() {
            self.request_http(pending, 0, fx);
        } else {
            self.phase = SessionPhase::Calling { pending, attempts: 0, phase: AttemptPhase::WaitingNetwork };
        }
    }

    /// The pair went out once before the restart: the send counts.
    fn start_calling_again(&mut self, pending: PendingCall, fx: &mut Vec<SessionEffect>) {
        if self.is_online() {
            self.request_http(pending, 1, fx);
        } else {
            self.phase = SessionPhase::Calling { pending, attempts: 1, phase: AttemptPhase::WaitingNetwork };
        }
    }

    pub(super) fn start_restored(&mut self, fx: &mut Vec<SessionEffect>) -> bool {
        if let Some(pending) = self.restored_direct.take() {
            self.restored_active = Some(pending.request_id);
            self.start_calling(pending, fx);
            return true;
        }
        if let Some(pending) = self.restored_transfer.take() {
            self.start_calling_again(pending, fx);
            return true;
        }
        let Some(pending) = self.restored_detached.take() else { return false };
        self.start_calling(pending, fx);
        true
    }

    /// A pair held for the transfer that owned it before the restart: Session sends it itself and the answer goes to that transfer.
    pub(super) fn drive_held_transfer(&mut self, fx: &mut Vec<SessionEffect>) {
        let SessionPhase::PendingHeld { pending } = self.phase.clone() else { return };
        if !matches!(pending.owner, Some(MutateOwner::Transfer { .. } | MutateOwner::Orphan { .. })) || pending.reply.is_some() { return; }
        self.restored_transfer = None;
        self.start_calling_again(PendingCall { author: Some(TRANSFER_AUTHOR), ..pending }, fx);
    }

    pub(super) fn resume_finish(&mut self, pending: &PendingCall, result: SessionAnswer, fx: &mut Vec<SessionEffect>) { self.resume_finish_retained(pending, result, false, fx); }

    pub(super) fn resume_finish_retained(&mut self, pending: &PendingCall, result: SessionAnswer, pending_retained: bool, fx: &mut Vec<SessionEffect>) {
        let kind = if pending_retained && result.is_err() { None } else { self.answer_kind.take() };
        if self.restored_active == Some(pending.request_id) {
            self.deliver_resume(result, fx);
        } else { finish_retained(fx, pending, result, pending_retained, kind); }
    }

    pub(super) fn deliver_resume(&mut self, result: SessionAnswer, fx: &mut Vec<SessionEffect>) {
        self.restored_active = None;
        let saved = std::mem::take(&mut self.resume_landing_saved);
        if self.resume_waiters.is_empty() {
            if saved { self.restored_answer = Some(RestoredAnswer::Saved(result)); }
            else {
                let batch = self.batch();
                fx.push(SessionEffect::CommitResumeAnswer { batch, epoch: self.epoch, answer: Some(result.clone()) });
                self.resume_saving = Some((batch, result));
            }
        }
        else if saved {
            self.restored_answer = Some(RestoredAnswer::Saved(result));
            let waiters = std::mem::take(&mut self.resume_waiters);
            self.take_resume(waiters, fx);
        }
        else {
            for (index, id) in self.resume_waiters.drain(..).enumerate() {
                fx.push(SessionEffect::ResumeReply { id, answer: if index == 0 { result.clone().map(Some) } else { Ok(None) } });
            }
        }
    }

    fn deliver_local_resume(&mut self, error: SessionError, fx: &mut Vec<SessionEffect>) {
        self.restored_active = None;
        self.resume_landing_saved = false;
        if self.resume_waiters.is_empty() { self.restored_answer = Some(RestoredAnswer::Local(error)); }
        else {
            for (index, id) in self.resume_waiters.drain(..).enumerate() {
                fx.push(SessionEffect::ResumeReply { id, answer: if index == 0 { Err(error.clone()) } else { Ok(None) } });
            }
        }
    }

    pub(super) fn take_resume(&mut self, waiters: Vec<ReplyId>, fx: &mut Vec<SessionEffect>) {
        let batch = self.batch();
        self.resume_taking = Some((batch, waiters));
        fx.push(SessionEffect::CommitResumeAnswer { batch, epoch: self.epoch, answer: None });
    }

    pub(super) fn end_resume(&mut self, error: SessionError, fx: &mut Vec<SessionEffect>) {
        if let Some((_, waiters)) = self.resume_taking.take() {
            for id in waiters { fx.push(SessionEffect::ResumeReply { id, answer: Err(error.clone()) }); }
        }
        self.resume_saving = None;
        self.resume_landing_saved = false;
        self.restored_answer = None;
        self.restored_active = None;
        self.resume_published = None;
        self.resume_clearing = None;
        for id in self.resume_waiters.drain(..) { fx.push(SessionEffect::ResumeReply { id, answer: Err(error.clone()) }); }
    }

    pub(super) fn report_failed(&mut self, fx: &mut Vec<SessionEffect>) {
        if self.failed_batch.take().is_some() { fx.push(SessionEffect::ExecutorFailed); }
    }

    pub(super) fn land(&mut self, pending: PendingCall, answer: SessionAnswer, identity: Option<Digest>, sequence: Option<u64>, server_ended: bool, fx: &mut Vec<SessionEffect>) {
        let batch = self.batch();
        let record = pending.author.map(|_| (pending.request_id, answer.clone()));
        let erase_session = server_ended || login_failed(&pending, &answer, server_ended);
        self.resume_landing_saved = self.restored_active == Some(pending.request_id) && (self.resume_waiters.is_empty() || pending.class == OperationClass::Login) && !server_ended;
        let resume_answer = self.resume_landing_saved.then(|| answer.clone());
        fx.push(SessionEffect::CommitLanding { batch, epoch: pending.epoch, digest: identity, sequence, answer: record, resume_answer, erase_session, end_journal: server_ended && pending.class != OperationClass::Login });
        self.phase = SessionPhase::Landing { pending, batch, answer, identity, sequence, server_ended };
    }

    pub(super) fn land_again(&mut self, pending: PendingCall, landed: Landed, fx: &mut Vec<SessionEffect>) {
        self.land(pending, landed.answer, landed.identity, landed.sequence, landed.server_ended, fx);
    }

    pub(super) fn clearing(&mut self, call: Option<Queued>, reason: Option<SessionAnswer>, after: AfterClear, fx: &mut Vec<SessionEffect>) {
        let op = self.secret_op();
        fx.push(SessionEffect::ClearSecret { secret_op_id: op });
        self.phase = SessionPhase::ClearingSecret { call, reason, op, after };
    }

    pub(super) fn drained(&mut self, fx: &mut Vec<SessionEffect>) {
        self.refuse_queue(fx, SessionError::Closed);
        self.held = None;
        self.restored_direct = None;
        self.restored_detached = None;
        self.restored_transfer = None;
        self.phase = SessionPhase::Closed;
        if self.resume_taking.is_none() { fx.push(SessionEffect::Drained(Drained { owner: "session" })); }
    }

    pub(super) fn answer_publication(&mut self, fx: &mut Vec<SessionEffect>, epoch: Epoch, state: PublishedState, identity: Option<Digest>) {
        if let Some(body) = self.resume_published.take() {
            let digest = self.credential.as_ref().map(|credential| credential.digest);
            if state == PublishedState::Session && identity.is_some() && identity == digest { self.deliver_resume(Ok(body), fx); }
            else if matches!(state, PublishedState::StorageFailed | PublishedState::EndingFailed) { self.deliver_local_resume(SessionError::Storage("the session could not be saved".into()), fx); }
            else if state == PublishedState::Ending { self.deliver_local_resume(SessionError::NoSession, fx); }
            else { self.resume_published = Some(body); }
        }
        let Some(held) = self.after_publish.take() else { return };
        let digest = self.credential.as_ref().map(|credential| credential.digest);
        let failed = matches!(state, PublishedState::StorageFailed | PublishedState::EndingFailed);
        match held.wait {
            _ if failed => reply(fx, held.reply, Err(SessionError::Storage("the session could not be saved".into()))),
            PublishWait::Identity if state == PublishedState::Session && identity.is_some() && identity == digest => reply(fx, held.reply, held.result),
            PublishWait::Identity if state == PublishedState::Ending => reply(fx, held.reply, Err(SessionError::NoSession)),
            PublishWait::EpochAfter(ended) if matches!(state, PublishedState::Session | PublishedState::NoSession) && epoch > ended => reply(fx, held.reply, held.result),
            _ => self.after_publish = Some(held),
        }
    }

    pub(super) fn adopt_epoch(&mut self, epoch: Epoch) {
        if let Some(pending) = self.restored_direct.as_mut().or(self.restored_detached.as_mut()).or(self.restored_transfer.as_mut()) { pending.epoch = epoch; }
        if let SessionPhase::PendingHeld { pending } | SessionPhase::Calling { pending, .. } = &mut self.phase { pending.epoch = epoch; }
    }

    pub(super) fn current(&self, epoch: Epoch) -> bool { epoch >= self.epoch && epoch >= self.restore_floor }

    /// A signed-in call waits for the publication that confirms its credential.
    pub(super) fn awaits_publication(&self) -> bool { self.after_publish.is_some() || self.resume_published.is_some() }

    pub(super) fn queue_mutate(&mut self, queued: Queued) {
        let Queued::Mutate { req_id, .. } = &queued else { return };
        if !self.queue.iter().any(|entry| matches!(entry, Queued::Mutate { req_id: seen, .. } if seen == req_id)) { self.queue.push_back(queued); }
    }

}

pub(super) fn pair_bodies(body: String) -> (String, Option<String>) {
    match crate::transport::encrypted::hash_only(&body) {
        Some(hash_only) => (hash_only, Some(body)),
        None => (body, None),
    }
}
