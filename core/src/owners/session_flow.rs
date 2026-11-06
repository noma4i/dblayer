use super::*;

impl SessionState {
    pub fn new(declared: &[&str], online: bool) -> Self {
        let operations = declared.iter().map(|name| SessionOperation { name: name.to_string(), path: format!("/{name}"), class: OperationClass::Authenticated }).collect();
        Self::with_operations(operations, Some(online))
    }

    pub fn with_operations(operations: Vec<SessionOperation>, online: Option<bool>) -> Self {
        Self { phase: SessionPhase::Idle, credential: None, queue: VecDeque::new(), online, epoch: 0, published: PublishedState::Session, after_publish: None, subscribers: BTreeSet::new(), restored_direct: None, restored_active: None, restored_answer: None, resume_waiters: Vec::new(), resume_published: None, resume_clearing: None, failed_batch: None, operations, next_request: 1, next_batch: 1, next_attempt: 1, next_secret: 1, restore_ready: false, restore_floor: 0, adopt: false, held: None, reported: (0, RetryPhase::Idle) }
    }

    pub fn phase(&self) -> &SessionPhase { &self.phase }

    pub fn credential(&self) -> Option<&Credential> { self.credential.as_ref() }

    pub fn queued(&self) -> usize { self.queue.len() }

    pub fn subscribers(&self) -> usize { self.subscribers.len() }

    pub fn awaiting_publication(&self) -> Option<&(ReplyId, String)> { self.after_publish.as_ref() }

    pub fn restored_direct(&self) -> Option<&PendingCall> { self.restored_direct.as_ref() }

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

    pub(super) fn def(&self, operation: &str) -> Option<SessionOperation> { self.operations.iter().find(|def| def.name == operation).cloned() }

    pub(super) fn refuse_queue(&mut self, fx: &mut Vec<SessionEffect>, error: SessionError) {
        for queued in self.queue.drain(..) { refuse(fx, queued, error.clone()); }
    }

    pub(super) fn view(&self) -> (u64, RetryPhase) {
        match &self.phase {
            SessionPhase::Calling { attempts, phase, .. } => (*attempts, match phase {
                AttemptPhase::InFlight(_) => RetryPhase::InFlight,
                AttemptPhase::WaitingRetry => RetryPhase::WaitingRetry,
                AttemptPhase::WaitingNetwork => RetryPhase::WaitingNetwork,
                AttemptPhase::WaitingSecret => RetryPhase::WaitingSecret,
            }),
            _ => (0, RetryPhase::Idle),
        }
    }

    pub(super) fn idle(&mut self, fx: &mut Vec<SessionEffect>) {
        self.phase = SessionPhase::Idle;
        if let Some(held) = self.held.take() {
            let error = if matches!(held, SessionPhase::SecretMismatch) { SessionError::StateLost } else { SessionError::Storage("the secret is unavailable".into()) };
            self.refuse_queue(fx, error);
            self.phase = held;
            return;
        }
        if self.restore_ready {
            if let Some(pending) = self.restored_direct.take() {
                self.start_restored(pending, fx);
                return;
            }
        }
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
                        self.seal(PendingCall { request_id, sequence: 0, class: def.class, operation, path: def.path, body, credential: None, purpose: 0, author: None, owner: None, epoch, reply: Some(id) }, fx);
                    }
                    OperationClass::Authenticated | OperationClass::Logout => {
                        let request_id = self.request();
                        self.seal(PendingCall { request_id, sequence, class: def.class, operation, path: def.path, body, credential: None, purpose: 0, author: None, owner: None, epoch, reply: Some(id) }, fx);
                    }
                }
            }
            Queued::Mutate { author, owner, req_id, epoch, path, body, purpose } => {
                if self.published == PublishedState::Closing || epoch != self.epoch || self.credential.is_none() {
                    author_error(fx, author, req_id, ErrorClass::Permanent, SessionError::NoSession);
                    return;
                }
                let sequence = self.credential.as_ref().map_or(1, |credential| credential.sequence + 1);
                self.seal(PendingCall { request_id: req_id, sequence, class: OperationClass::Authenticated, operation: String::new(), path, body, credential: None, purpose, author: Some(author), owner, epoch, reply: None }, fx);
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

    pub(super) fn request_http(&mut self, pending: PendingCall, attempts: u64, document_sent: bool, fx: &mut Vec<SessionEffect>) {
        let attempt_id = self.attempt();
        fx.push(SessionEffect::HttpRequest { pending: pending.clone(), attempt_id, full_document: document_sent });
        self.phase = SessionPhase::Calling { pending, attempts, phase: AttemptPhase::InFlight(attempt_id), document_sent };
    }

    pub(super) fn start_calling(&mut self, pending: PendingCall, fx: &mut Vec<SessionEffect>) {
        if self.is_online() {
            self.request_http(pending, 0, false, fx);
        } else {
            self.phase = SessionPhase::Calling { pending, attempts: 0, phase: AttemptPhase::WaitingNetwork, document_sent: false };
        }
    }

    pub(super) fn start_restored(&mut self, pending: PendingCall, fx: &mut Vec<SessionEffect>) {
        self.restored_active = Some(pending.request_id);
        self.start_calling(pending, fx);
    }

    pub(super) fn resume_finish(&mut self, pending: &PendingCall, result: SessionAnswer, fx: &mut Vec<SessionEffect>) {
        if self.restored_active == Some(pending.request_id) {
            self.deliver_resume(result, fx);
        } else { finish(fx, pending, result); }
    }

    pub(super) fn deliver_resume(&mut self, result: SessionAnswer, fx: &mut Vec<SessionEffect>) {
        self.restored_active = None;
        if self.resume_waiters.is_empty() { self.restored_answer = Some(result); }
        else { for id in self.resume_waiters.drain(..) { fx.push(SessionEffect::ResumeReply { id, answer: result.clone().map(Some) }); } }
    }

    pub(super) fn end_resume(&mut self, error: SessionError, fx: &mut Vec<SessionEffect>) {
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
        let erase_session = login_failed(&pending, &answer, server_ended);
        fx.push(SessionEffect::CommitLanding { batch, epoch: pending.epoch, digest: identity, sequence, answer: record, erase_session });
        self.phase = SessionPhase::Landing { pending, batch, answer, identity, sequence, server_ended };
    }

    pub(super) fn clearing(&mut self, call: Option<Queued>, reason: Option<SessionError>, after: AfterClear, fx: &mut Vec<SessionEffect>) {
        let op = self.secret_op();
        fx.push(SessionEffect::ClearSecret { secret_op_id: op });
        self.phase = SessionPhase::ClearingSecret { call, reason, op, after };
    }

    pub(super) fn drained(&mut self, fx: &mut Vec<SessionEffect>) {
        self.refuse_queue(fx, SessionError::Closed);
        self.held = None;
        self.restored_direct = None;
        self.phase = SessionPhase::Closed;
        fx.push(SessionEffect::Drained(Drained { owner: "session" }));
    }

    pub(super) fn answer_publication(&mut self, fx: &mut Vec<SessionEffect>, state: PublishedState, identity: Option<Digest>) {
        if let Some(body) = self.resume_published.take() {
            let digest = self.credential.as_ref().map(|credential| credential.digest);
            if state == PublishedState::Session && identity.is_some() && identity == digest { self.deliver_resume(Ok(body), fx); }
            else if matches!(state, PublishedState::StorageFailed | PublishedState::EndingFailed) { self.deliver_resume(Err(SessionError::Storage("the session could not be saved".into())), fx); }
            else if state == PublishedState::Ending { self.deliver_resume(Err(SessionError::Interrupted), fx); }
            else { self.resume_published = Some(body); }
        }
        let Some((id, body)) = self.after_publish.take() else { return };
        let digest = self.credential.as_ref().map(|credential| credential.digest);
        if state == PublishedState::Session && identity.is_some() && identity == digest {
            reply(fx, id, Ok(body));
        } else if matches!(state, PublishedState::StorageFailed | PublishedState::EndingFailed) {
            reply(fx, id, Err(SessionError::Storage("the session could not be saved".into())));
        } else if state == PublishedState::Ending {
            reply(fx, id, Err(SessionError::Interrupted));
        } else {
            self.after_publish = Some((id, body));
        }
    }

    pub(super) fn adopt_epoch(&mut self, epoch: Epoch) {
        if let Some(pending) = &mut self.restored_direct { pending.epoch = epoch; }
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
