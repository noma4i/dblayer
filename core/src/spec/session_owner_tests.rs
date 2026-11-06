//! Owner-level tests of the owner `Session` in the `lifecycle` machine: the owner on `DetScheduler` against `ref_session.rs`. Names are the `Test` column.

use super::ref_session::{AfterClear, AfterStore, Block, CallPhase, Class, Credential as RefCredential, Effect as RefEffect, EndCause, Fail, Input, Kind, Operation, Outcome, Pending as RefPending, PubState, Queued as RefQueued, Reason, RetryPhase, Secret, Session as Ref, State as RefState};
use crate::owners::contract::{Committed, EpochEnded, ErrorClass, Online, Published, PublishedState, RejectReason, Rejected};
use crate::owners::session::{self as owner, AuthorOutcome, Credential, OperationClass, PendingCall, Queued, SecretState, SessionCalls, SessionEffect, SessionError, SessionInput, SessionPhase, SessionState};
use crate::owners::support::{new_log, take, Log, Recorder};
use crate::runtime::{Address, Cause, DetScheduler, EpochStamp, Time};
use crate::transport::SessionCode;

const EPOCH: u64 = 3;
const CRED: u64 = 4;
const OTP: &str = "/graphql";

fn operations() -> Vec<Operation> {
    let def = |name: &str, class| Operation { name: name.into(), path: format!("/{name}"), class };
    vec![def("social", Class::Authenticated), def("logout", Class::Logout), def("login", Class::Login), def("visit", Class::Sessionless)]
}

fn class_of(class: Class) -> OperationClass {
    match class {
        Class::Login => OperationClass::Login,
        Class::Authenticated => OperationClass::Authenticated,
        Class::Sessionless => OperationClass::Sessionless,
        Class::Logout => OperationClass::Logout,
    }
}

fn ref_class(class: OperationClass) -> Class {
    match class {
        OperationClass::Login => Class::Login,
        OperationClass::Authenticated => Class::Authenticated,
        OperationClass::Sessionless => Class::Sessionless,
        OperationClass::Logout => Class::Logout,
    }
}

fn owner_operations() -> Vec<owner::SessionOperation> {
    operations().into_iter().map(|def| owner::SessionOperation { name: def.name, path: def.path, class: class_of(def.class) }).collect()
}

fn reject(reason: Reason) -> RejectReason {
    match reason {
        Reason::NoSession => RejectReason::NoSession,
        Reason::Storage => RejectReason::Storage("disk".into()),
        Reason::Closed => RejectReason::Closed,
    }
}

fn fail_of(error: &SessionError) -> Fail {
    match error {
        SessionError::Invalid => Fail::Invalid,
        SessionError::Rejected(RejectReason::NoSession) => Fail::Reason(Reason::NoSession),
        SessionError::Rejected(RejectReason::Storage(_)) => Fail::Reason(Reason::Storage),
        SessionError::Rejected(RejectReason::Closed) => Fail::Reason(Reason::Closed),
        SessionError::Rejected(other) => panic!("the Session table has no {other:?}"),
        SessionError::Session(code) => Fail::Session(*code),
        SessionError::Storage(_) => Fail::Storage,
        SessionError::SessionExists => Fail::SessionExists,
        SessionError::NoCredential => Fail::NoCredential,
        SessionError::Revoked => Fail::Revoked,
        SessionError::StateLost => Fail::StateLost,
        SessionError::SequenceConflict => Fail::SequenceConflict,
        SessionError::Protocol => Fail::Protocol,
        SessionError::Unauthorized => Fail::Unauthorized,
        SessionError::Http { .. } => Fail::Http,
        SessionError::TooLarge { size, limit } => Fail::TooLarge { size: *size, limit: *limit },
        SessionError::Interrupted => Fail::Interrupted,
        SessionError::NoSession => Fail::NoSession,
        SessionError::Closed => Fail::Closed,
    }
}


fn cause(cause: EndCause) -> Cause {
    match cause {
        EndCause::SessionReset => Cause::SessionReset,
        EndCause::ServerEnded => Cause::ServerEnded,
        EndCause::Other => Cause::IdentityChanged,
    }
}

fn published_state(state: PubState) -> PublishedState {
    match state {
        PubState::NoSession => PublishedState::NoSession,
        PubState::Session => PublishedState::Session,
        PubState::Ending => PublishedState::Ending,
        PubState::EndingFailed => PublishedState::EndingFailed,
        PubState::StorageFailed => PublishedState::StorageFailed,
        PubState::Closing => PublishedState::Closing,
    }
}

fn secret_state(secret: &Secret) -> SecretState {
    match secret {
        Secret::Present(digest) => SecretState::Present(*digest),
        Secret::Missing => SecretState::Missing,
        Secret::Error(error) => SecretState::Error(error.clone()),
    }
}

fn pending_of(pending: &PendingCall) -> RefPending {
    RefPending { request_id: pending.request_id, sequence: pending.sequence, class: ref_class(pending.class), operation: pending.operation.clone(), path: pending.path.clone(), body: pending.body.clone(), credential: pending.credential, purpose: pending.purpose, author: pending.author, owner: pending.owner.clone(), epoch: pending.epoch, reply: pending.reply }
}

fn owner_pending(pending: RefPending) -> PendingCall {
    PendingCall { request_id: pending.request_id, sequence: pending.sequence, class: class_of(pending.class), operation: pending.operation, path: pending.path, body: pending.body, credential: pending.credential, purpose: pending.purpose, author: pending.author, owner: pending.owner, epoch: pending.epoch, reply: pending.reply }
}

fn queued_of(queued: &Queued) -> RefQueued {
    match queued {
        Queued::Call { operation, body, reply } => RefQueued::Call { operation: operation.clone(), body: body.clone(), reply: *reply },
        Queued::Mutate { author, owner, req_id, epoch, path, body, purpose } => RefQueued::Mutate { author: *author, owner: owner.clone(), req_id: *req_id, epoch: *epoch, path: path.clone(), body: body.clone(), purpose: *purpose },
    }
}


fn outcome_of(outcome: &AuthorOutcome) -> Outcome {
    match outcome {
        AuthorOutcome::Response(body) => Outcome::Response(body.clone()),
        AuthorOutcome::Error { class, error } => Outcome::Error(if *class == ErrorClass::Permanent { Kind::Permanent } else { Kind::Temporary }, fail_of(error)),
        AuthorOutcome::Dropped => Outcome::Dropped,
    }
}

fn phase_of(phase: &SessionPhase) -> RefState {
    let call_phase = |phase: &owner::AttemptPhase| match phase {
        owner::AttemptPhase::InFlight(attempt) => CallPhase::InFlight(*attempt),
        owner::AttemptPhase::WaitingRetry => CallPhase::WaitingRetry,
        owner::AttemptPhase::WaitingNetwork => CallPhase::WaitingNetwork,
        owner::AttemptPhase::WaitingSecret => CallPhase::WaitingSecret,
    };
    match phase {
        SessionPhase::Idle => RefState::Idle,
        SessionPhase::Sealing { pending, seal_id } => RefState::Sealing { pending: pending_of(pending), seal_id: *seal_id },
        SessionPhase::Generating { call, op } => RefState::Generating { call: queued_of(&call.clone().into()), op: *op },
        SessionPhase::Storing { call, credential_new, op, after } => RefState::Storing { call: queued_of(&call.clone().into()), credential_new: *credential_new, op: *op, after: match after {
            owner::AfterStore::Persist => AfterStore::Persist,
            owner::AfterStore::Clear(error) => AfterStore::Clear(fail_of(error)),
            owner::AfterStore::Drain => AfterStore::Drain,
        } },
        SessionPhase::ClearingSecret { call, reason, op, after } => RefState::ClearingSecret { call: call.as_ref().map(queued_of), reason: reason.as_ref().map(fail_of), op: *op, after: match after {
            owner::AfterClear::Idle => AfterClear::Idle,
            owner::AfterClear::Drain => AfterClear::Drain,
        } },
        SessionPhase::PendingHeld { pending } => RefState::PendingHeld { pending: pending_of(pending) },
        SessionPhase::SecretMismatch => RefState::SecretMismatch,
        SessionPhase::SecretUnavailable { error, read } => RefState::SecretUnavailable { error: error.clone(), read: *read },
        SessionPhase::Persisting { pending, batch } => RefState::Persisting { pending: pending_of(pending), batch: *batch },
        SessionPhase::Calling { pending, attempts, phase, document_sent } => RefState::Calling { pending: pending_of(pending), attempts: *attempts, phase: call_phase(phase), document_sent: *document_sent },
        SessionPhase::Landing { pending, batch, answer, identity, sequence, server_ended } => RefState::Landing { pending: pending_of(pending), batch: *batch, answer: answer.clone().map_err(|error| fail_of(&error)), identity: *identity, sequence: *sequence, server_ended: *server_ended },
        SessionPhase::Blocked { pending, reason } => RefState::Blocked { pending: pending.as_ref().map(pending_of), reason: match reason {
            owner::Block::Storage => Block::Storage,
            owner::Block::SequenceConflict => Block::SequenceConflict,
            owner::Block::Protocol => Block::Protocol,
            owner::Block::StateLost => Block::StateLost,
        } },
        SessionPhase::Closed => RefState::Closed,
    }
}

fn retry_phase(phase: owner::RetryPhase) -> RetryPhase {
    match phase {
        owner::RetryPhase::Idle => RetryPhase::Idle,
        owner::RetryPhase::InFlight => RetryPhase::InFlight,
        owner::RetryPhase::WaitingRetry => RetryPhase::WaitingRetry,
        owner::RetryPhase::WaitingNetwork => RetryPhase::WaitingNetwork,
        owner::RetryPhase::WaitingSecret => RetryPhase::WaitingSecret,
    }
}

fn effect_of(effect: &SessionEffect) -> Option<RefEffect> {
    Some(match effect {
        SessionEffect::SealProbe { seal_id, pending, credential } => RefEffect::SealProbe { seal_id: *seal_id, pending: pending_of(pending), credential: *credential },
        SessionEffect::CommitPending { batch, epoch, pending, session } => RefEffect::CommitPending { batch: *batch, epoch: *epoch, pending: pending_of(pending), session: *session },
        SessionEffect::CommitLanding { batch, epoch, digest, sequence, answer, erase_session } => RefEffect::CommitLanding { batch: *batch, epoch: *epoch, digest: *digest, sequence: *sequence, answer: answer.as_ref().map(|(id, answer)| (*id, answer.clone().map_err(|error| fail_of(&error)))), erase_session: *erase_session },
        SessionEffect::HttpRequest { pending, attempt_id, full_document } => RefEffect::HttpRequest { pending: pending_of(pending), attempt_id: *attempt_id, full_document: *full_document },
        SessionEffect::HttpCancel { attempt_id } => RefEffect::HttpCancel { attempt_id: *attempt_id },
        SessionEffect::ScheduleTick { attempts } => RefEffect::ScheduleTick { attempts: *attempts },
        SessionEffect::GenerateCredential { secret_op_id } => RefEffect::GenerateCredential { op: *secret_op_id },
        SessionEffect::StoreSecret { credential, secret_op_id } => RefEffect::StoreSecret { credential: *credential, op: *secret_op_id },
        SessionEffect::ClearSecret { secret_op_id } => RefEffect::ClearSecret { op: *secret_op_id },
        SessionEffect::ReadSecret { secret_op_id } => RefEffect::ReadSecret { op: *secret_op_id },
        SessionEffect::CredentialCommitted(committed) => RefEffect::CredentialCommitted(committed.identity),
        SessionEffect::ServerEnded(ended) => RefEffect::ServerEnded { epoch: ended.epoch },
        SessionEffect::SessionReset => RefEffect::SessionReset,
        SessionEffect::RestoreClassified { epoch } => RefEffect::RestoreClassified { epoch: *epoch },
        SessionEffect::Answer { author, req_id, outcome } => RefEffect::Answer { author: *author, req_id: *req_id, outcome: outcome_of(outcome) },
        SessionEffect::RetryState { stream_id, attempts, phase } => RefEffect::RetryState { stream: *stream_id, attempts: *attempts, phase: retry_phase(*phase) },
        SessionEffect::Drained(_) => RefEffect::Drained,
        SessionEffect::Reply { .. } | SessionEffect::ResumeReply { .. } | SessionEffect::ExecutorFailed => return None,
    })
}

struct Twin {
    det: DetScheduler,
    addr: Address<SessionCalls>,
    log: Log<SessionEffect>,
    reference: Ref,
    history: Vec<RefEffect>,
    replies: Vec<(u64, Result<String, Fail>)>,
}

impl Twin {
    /// Online, signed in (`credential` 4 at sequence 0), `Published.epoch` = `EPOCH`.
    fn new() -> Self { Self::start(Some(true), Some(RefCredential { digest: CRED, sequence: 0 })) }

    fn offline() -> Self { Self::start(Some(false), Some(RefCredential { digest: CRED, sequence: 0 })) }

    /// No credential: a call that carries the session signs in.
    fn bare() -> Self { Self::start(Some(true), None) }

    fn start(online: Option<bool>, credential: Option<RefCredential>) -> Self {
        let mut det = DetScheduler::new(2);
        let log = new_log();
        let addr = det.add::<SessionCalls>("session", SessionState::with_operations(owner_operations(), online), Recorder::<SessionCalls>::new(&log));
        let mut twin = Self { det, addr, log, reference: Ref::new(operations(), online), history: Vec::new(), replies: Vec::new() };
        twin.feed(Input::Published { epoch: EPOCH, identity: None, state: PubState::Session });
        let secret = credential.as_ref().map_or(Secret::Missing, |credential| Secret::Present(credential.digest));
        twin.feed(Input::Open { credential, pending: None, dropped_pending: false, secret });
        twin
    }

    fn feed(&mut self, input: Input) -> Vec<RefEffect> {
        if let RefState::Sealing { seal_id, .. } = self.reference.state {
            if !matches!(input, Input::Sealed { .. } | Input::SealFailed { .. }) {
                self.feed(Input::Sealed { seal_id, size: 1 });
            }
        }
        let batch_of = |state: &RefState| match state {
            RefState::Persisting { batch, .. } | RefState::Landing { batch, .. } => *batch,
            _ => 9_999,
        };
        let current = batch_of(&self.reference.state);
        let resolve = |batch: u64| if batch == 0 { current } else { batch };
        let owner_input = match input.clone() {
            Input::Sealed { seal_id, size } => SessionInput::Sealed { seal_id, size },
            Input::SealFailed { seal_id, error } => SessionInput::SealFailed { seal_id, error: match error { Fail::StateLost => SessionError::StateLost, _ => SessionError::Storage("seal failed".into()) } },
            Input::Open { credential, pending, dropped_pending, secret } => SessionInput::Open { credential: credential.map(|c| Credential { digest: c.digest, sequence: c.sequence }), pending: pending.map(owner_pending), dropped_pending, secret: secret_state(&secret) },
            Input::SessionCall { operation, body, reply } => SessionInput::SessionCall { operation, body, reply },
            Input::Mutate { author, owner, req_id, epoch, path, body, purpose } => SessionInput::Mutate { author, owner, req_id, epoch, path, body, purpose },
            Input::Cancel { req_id } => SessionInput::Cancel { req_id },
            Input::Release { req_id } => SessionInput::Release { req_id },
            Input::CredentialGenerated { op, credential } => SessionInput::CredentialGenerated { secret_op_id: op, credential },
            Input::SecretStored { op, ok } => SessionInput::SecretStored { secret_op_id: op, ok },
            Input::SecretCleared { op, ok } => SessionInput::SecretCleared { secret_op_id: op, ok },
            Input::SecretRead { op, read } => SessionInput::SecretRead { secret_op_id: op, read: secret_state(&read) },
            Input::SecretMissing { request_id, attempt_id, lost } => SessionInput::SecretMissing { request_id, attempt_id, lost },
            Input::Foreground => SessionInput::Foreground { seq: 1 },
            Input::InterestPlus(stream_id) => SessionInput::InterestPlus { stream_id },
            Input::InterestMinus(stream_id) => SessionInput::InterestMinus { stream_id },
            Input::RestoreReady { epoch } => SessionInput::RestoreReady { epoch },
            Input::Restored { epoch, pending } => SessionInput::Restored { epoch, pending: pending.map(owner_pending), answers: Default::default(), pending_lost: false },
            Input::Response { request_id, attempt_id, body, sequence, refusal } => SessionInput::Response { request_id, attempt_id, body, sequence, refusal },
            Input::Revoked { request_id, attempt_id, body } => SessionInput::Revoked { request_id, attempt_id, body },
            Input::HttpError { request_id, attempt_id, temporary, auth, persisted_missing } => SessionInput::HttpError { request_id, attempt_id, temporary, auth, persisted_missing, status: 0, too_large: None },
            Input::Dropped { request_id, attempt_id } => SessionInput::Dropped { request_id, attempt_id },
            Input::Committed(batch) => SessionInput::Committed(Committed { batch: resolve(batch), revision: 1, tables: vec![], models: Default::default() }),
            Input::Rejected(batch, reason) => SessionInput::Rejected(Rejected { batch: resolve(batch), reason: reject(reason) }),
            Input::Tick => SessionInput::Tick(Time(0)),
            Input::Online(online) => SessionInput::Online(Online(online)),
            Input::Snapshot { epoch, online } => SessionInput::Snapshot { epoch, online },
            Input::Published { epoch, identity, state } => SessionInput::Published(Published { epoch, identity, generation: 0, state: published_state(state), closed: false }),
            Input::EpochEnded { epoch, cause: c } => SessionInput::EpochEnded(EpochEnded { epoch, cause: cause(c) }),
            Input::Closing => SessionInput::Closing,
        };
        let reference_input = match input {
            Input::Committed(0) => Input::Committed(current),
            Input::Rejected(0, reason) => Input::Rejected(current, reason),
            other => other,
        };
        let traced = self.det.trace().len();
        self.det.post(self.addr, EpochStamp::Control, owner_input);
        self.det.run_until_quiet(100).expect("the owner is quiet");
        let scheduled = self.det.trace()[traced..].iter().filter(|entry| entry.effects.contains("Schedule {")).count();
        let raw = take(&self.log);
        let want = self.reference.step(reference_input);
        let mut effects: Vec<RefEffect> = raw.iter().filter_map(effect_of).collect();
        if scheduled > 0 {
            let Some(SessionPhase::Calling { attempts, .. }) = self.det.state(self.addr).map(SessionState::phase) else { panic!("a retry timer is scheduled in Calling only") };
            effects.extend(std::iter::repeat_n(RefEffect::ScheduleTick { attempts: *attempts }, scheduled));
        }
        let mut sorted_have = effects.clone();
        let mut sorted_want = want.effects.clone();
        sorted_have.sort_by_key(|effect| format!("{effect:?}"));
        sorted_want.sort_by_key(|effect| format!("{effect:?}"));
        assert_eq!(sorted_have, sorted_want, "the same effects");
        let replies: Vec<(u64, Result<String, Fail>)> = raw.iter().filter_map(|effect| match effect {
            SessionEffect::Reply { id, answer } => Some((*id, answer.clone().map_err(|error| fail_of(&error)))),
            _ => None,
        }).collect();
        assert_eq!(replies, want.replies, "the same replies");
        self.replies.extend(replies);
        self.history.extend(want.effects.iter().cloned());
        let state = self.det.state(self.addr).expect("the owner state");
        assert_eq!(phase_of(state.phase()), self.reference.state, "the same state");
        assert_eq!(state.credential().map(|c| RefCredential { digest: c.digest, sequence: c.sequence }), self.reference.credential, "the same credential");
        assert_eq!(state.queued(), self.reference.queue.len(), "the same queue");
        assert_eq!(state.subscribers(), self.reference.subscribers.len(), "the same subscribers");
        assert_eq!(state.awaiting_publication().cloned(), self.reference.after_publish, "the same answer after publish");
        assert_eq!(state.restored_direct().map(pending_of), self.reference.restored_direct, "the same restored direct pending");
        want.effects
    }

    fn feed_all(&mut self, inputs: impl IntoIterator<Item = Input>) { inputs.into_iter().for_each(|input| { self.feed(input); }); }

    fn pending(&self) -> RefPending {
        match &self.reference.state {
            RefState::Sealing { pending, .. } | RefState::Persisting { pending, .. } | RefState::Calling { pending, .. } | RefState::Landing { pending, .. } | RefState::PendingHeld { pending } | RefState::Blocked { pending: Some(pending), .. } => pending.clone(),
            other => panic!("no pending request in {other:?}"),
        }
    }

    fn rid(&self) -> u64 { self.pending().request_id }

    fn aid(&self) -> u64 {
        match &self.reference.state {
            RefState::Calling { phase: CallPhase::InFlight(attempt), .. } => *attempt,
            other => panic!("no attempt in {other:?}"),
        }
    }

    fn op(&self) -> u64 {
        match &self.reference.state {
            RefState::Generating { op, .. } | RefState::Storing { op, .. } | RefState::ClearingSecret { op, .. } => *op,
            RefState::SecretUnavailable { read: Some(op), .. } => *op,
            other => panic!("no secret operation in {other:?}"),
        }
    }

    fn response(&self, sequence: u64) -> Input { Input::Response { request_id: self.rid(), attempt_id: self.aid(), body: "{}".into(), sequence: Some(sequence), refusal: None } }

    fn http_error(&self, temporary: bool, auth: bool) -> Input { Input::HttpError { request_id: self.rid(), attempt_id: self.aid(), temporary, auth, persisted_missing: false } }

    fn dropped(&self) -> Input { Input::Dropped { request_id: self.rid(), attempt_id: self.aid() } }

    fn in_phase(&self, want: CallPhase) -> bool { matches!(&self.reference.state, RefState::Calling { phase, .. } if *phase == want) }
}

fn call(reply: u64) -> Input { Input::SessionCall { operation: "social".into(), body: "{}".into(), reply } }
fn call_of(operation: &str, reply: u64) -> Input { Input::SessionCall { operation: operation.into(), body: "{}".into(), reply } }
fn mutate(author: u64, req_id: u64) -> Input { Input::Mutate { author, owner: None, req_id, epoch: EPOCH, path: OTP.into(), body: "{}".into(), purpose: 0 } }
fn committed() -> Input { Input::Committed(0) }
fn rejected(reason: Reason) -> Input { Input::Rejected(0, reason) }
fn published(state: PubState) -> Input { Input::Published { epoch: EPOCH, identity: None, state } }
fn ended(cause: EndCause) -> Input { Input::EpochEnded { epoch: EPOCH, cause } }
fn answer(author: u64, req_id: u64, outcome: Outcome) -> RefEffect { RefEffect::Answer { author, req_id, outcome } }
fn permanent(fail: Fail) -> Outcome { Outcome::Error(Kind::Permanent, fail) }

fn persisting() -> Twin {
    let mut twin = Twin::new();
    twin.feed(call(1));
    if let RefState::Sealing { seal_id, .. } = twin.reference.state { twin.feed(Input::Sealed { seal_id, size: 1 }); }
    twin
}

fn finish_seal(twin: &mut Twin) {
    if let RefState::Sealing { seal_id, .. } = twin.reference.state { twin.feed(Input::Sealed { seal_id, size: 1 }); }
}

fn calling() -> Twin {
    let mut twin = persisting();
    twin.feed(committed());
    twin
}

fn landing(sequence: u64) -> Twin {
    let mut twin = calling();
    let input = twin.response(sequence);
    twin.feed(input);
    twin
}

fn calling_mutate() -> Twin {
    let mut twin = Twin::new();
    twin.feed_all([mutate(7, 11), committed()]);
    twin
}

fn held() -> Twin {
    let mut twin = calling_mutate();
    let input = twin.dropped();
    twin.feed(input);
    assert!(matches!(twin.reference.state, RefState::PendingHeld { .. }));
    twin
}

fn generating() -> Twin {
    let mut twin = Twin::bare();
    twin.feed(call_of("login", 1));
    twin
}

fn storing() -> Twin {
    let mut twin = generating();
    let op = twin.op();
    twin.feed(Input::CredentialGenerated { op, credential: 42 });
    twin
}

fn clearing() -> Twin {
    let mut twin = storing();
    twin.feed(Input::EpochEnded { epoch: EPOCH, cause: EndCause::SessionReset });
    let op = twin.op();
    twin.feed(Input::SecretStored { op, ok: true });
    twin
}

fn blocked_storage() -> Twin {
    let mut twin = landing(1);
    twin.feed(rejected(Reason::Storage));
    twin
}

fn stored(owner: Option<crate::transport::RequestOwner>, request_id: u64, sequence: u64) -> RefPending {
    let author = owner.as_ref().map(super::ref_session::author_of);
    RefPending { request_id, sequence, class: Class::Authenticated, operation: String::new(), path: OTP.into(), body: "{}".into(), credential: None, purpose: 0, author, owner, epoch: 0, reply: None }
}

fn open(credential: Option<(u64, u64)>, pending: Option<RefPending>, dropped_pending: bool, secret: Secret) -> Input {
    Input::Open { credential: credential.map(|(digest, sequence)| RefCredential { digest, sequence }), pending, dropped_pending, secret }
}

fn opened(input: Input) -> Twin {
    let mut twin = Twin::start(Some(true), None);
    twin.feed(input);
    twin
}

include!("session_owner_calls_tests.rs");
include!("session_owner_http_tests.rs");
include!("session_owner_restore_tests.rs");
include!("session_owner_props_tests.rs");
