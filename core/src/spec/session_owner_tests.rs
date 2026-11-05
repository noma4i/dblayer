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
        SessionError::Http => Fail::Http,
        SessionError::Interrupted => Fail::Interrupted,
        SessionError::NoSession => Fail::NoSession,
        SessionError::Closed => Fail::Closed,
    }
}

fn owner_error(fail: &Fail) -> SessionError {
    match fail {
        Fail::Invalid => SessionError::Invalid,
        Fail::Reason(reason) => SessionError::Rejected(reject(*reason)),
        Fail::Session(code) => SessionError::Session(*code),
        Fail::Storage => SessionError::Storage("disk".into()),
        Fail::SessionExists => SessionError::SessionExists,
        Fail::NoCredential => SessionError::NoCredential,
        Fail::Revoked => SessionError::Revoked,
        Fail::StateLost => SessionError::StateLost,
        Fail::SequenceConflict => SessionError::SequenceConflict,
        Fail::Protocol => SessionError::Protocol,
        Fail::Unauthorized => SessionError::Unauthorized,
        Fail::Http => SessionError::Http,
        Fail::Interrupted => SessionError::Interrupted,
        Fail::NoSession => SessionError::NoSession,
        Fail::Closed => SessionError::Closed,
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
        PubState::Resetting => PublishedState::Resetting,
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

fn owner_queued(queued: RefQueued) -> Queued {
    match queued {
        RefQueued::Call { operation, body, reply } => Queued::Call { operation, body, reply },
        RefQueued::Mutate { author, owner, req_id, epoch, path, body, purpose } => Queued::Mutate { author, owner, req_id, epoch, path, body, purpose },
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
        SessionEffect::Reply { .. } => return None,
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
        let batch_of = |state: &RefState| match state {
            RefState::Persisting { batch, .. } | RefState::Landing { batch, .. } => *batch,
            _ => 9_999,
        };
        let current = batch_of(&self.reference.state);
        let resolve = |batch: u64| if batch == 0 { current } else { batch };
        let owner_input = match input.clone() {
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
            Input::HttpError { request_id, attempt_id, temporary, auth, persisted_missing } => SessionInput::HttpError { request_id, attempt_id, temporary, auth, persisted_missing },
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
            RefState::Persisting { pending, .. } | RefState::Calling { pending, .. } | RefState::Landing { pending, .. } | RefState::PendingHeld { pending } | RefState::Blocked { pending: Some(pending), .. } => pending.clone(),
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
    twin
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

#[test]
fn sess_idle_unknown_call() {
    let mut twin = Twin::new();
    twin.feed(call_of("nope", 1));
    assert_eq!(twin.replies, vec![(1, Err(Fail::Invalid))]);
}

#[test]
fn sess_idle_call_ending_failed() {
    for state in [PubState::EndingFailed, PubState::StorageFailed] {
        let mut twin = Twin::new();
        twin.feed(published(state));
        twin.feed(call(1));
        assert_eq!(twin.replies, vec![(1, Err(Fail::Storage))]);
    }
}

#[test]
fn sess_idle_call_during_ending() {
    let mut twin = Twin::new();
    twin.feed(published(PubState::Ending));
    assert!(twin.feed(call(1)).is_empty());
    assert_eq!(twin.reference.queue.len(), 1, "the call waits for Published");
}

#[test]
fn sess_idle_login_with_session() {
    let mut twin = Twin::new();
    twin.feed(call_of("login", 1));
    assert_eq!(twin.replies, vec![(1, Err(Fail::SessionExists))]);
    assert!(matches!(twin.reference.state, RefState::Idle));
}

#[test]
fn sess_idle_login_generate() {
    let mut twin = Twin::bare();
    assert!(matches!(&twin.feed(call_of("login", 1))[..], [RefEffect::GenerateCredential { .. }]));
    let mut twin = Twin::bare();
    assert!(matches!(&twin.feed(call(1))[..], [RefEffect::GenerateCredential { .. }]), "an authenticated call at credential None signs in");
}

#[test]
fn sess_idle_call() {
    let mut twin = Twin::new();
    let effects = twin.feed(call(1));
    assert!(matches!(&effects[..], [RefEffect::CommitPending { pending, session: None, .. }] if pending.sequence == 1 && pending.class == Class::Authenticated && pending.epoch == EPOCH));
}

#[test]
fn sess_idle_call_sessionless() {
    let mut twin = Twin::new();
    let effects = twin.feed(call_of("visit", 1));
    assert!(matches!(&effects[..], [RefEffect::CommitPending { pending, .. }] if pending.sequence == 0 && pending.class == Class::Sessionless));
}

#[test]
fn sess_idle_call_no_credential() {
    let mut twin = Twin::bare();
    assert!(twin.feed(call_of("logout", 1)).is_empty());
    assert_eq!(twin.replies, vec![(1, Err(Fail::NoCredential))]);
}

#[test]
fn sess_idle_logout() {
    let mut twin = Twin::new();
    let effects = twin.feed(call_of("logout", 1));
    assert!(matches!(&effects[..], [RefEffect::CommitPending { pending, .. }] if pending.class == Class::Logout && pending.sequence == 1));
}

#[test]
fn sess_secret_op_stray() {
    let mut twin = generating();
    let op = twin.op();
    twin.feed_all([Input::CredentialGenerated { op: op + 50, credential: 9 }, Input::SecretStored { op, ok: true }, Input::SecretCleared { op, ok: true }]);
    assert!(matches!(twin.reference.state, RefState::Generating { .. }), "a foreign id changes nothing");
    let mut idle = Twin::new();
    idle.feed_all([Input::CredentialGenerated { op: 1, credential: 9 }, Input::SecretStored { op: 1, ok: true }, Input::SecretCleared { op: 1, ok: true }]);
    assert!(idle.history.is_empty());
}

#[test]
fn sess_generated_store_secret() {
    let mut twin = generating();
    let op = twin.op();
    let effects = twin.feed(Input::CredentialGenerated { op, credential: 42 });
    assert!(matches!(&effects[..], [RefEffect::StoreSecret { credential: 42, .. }]), "the secret goes before the digest");
}

#[test]
fn sess_secret_stored_persist() {
    let mut twin = storing();
    let op = twin.op();
    let effects = twin.feed(Input::SecretStored { op, ok: true });
    assert!(matches!(&effects[..], [RefEffect::CommitPending { pending, session: Some(42), .. }] if pending.sequence == 1 && pending.class == Class::Login && pending.credential == Some(42)));
}

#[test]
fn sess_secret_store_failed() {
    let mut twin = storing();
    let op = twin.op();
    assert!(matches!(&twin.feed(Input::SecretStored { op, ok: false })[..], [RefEffect::ClearSecret { .. }]), "no orphan stays");
    let op = twin.op();
    twin.feed(Input::SecretCleared { op, ok: true });
    assert_eq!(twin.replies, vec![(1, Err(Fail::Storage))]);
}

#[test]
fn sess_secret_stored_then_clear() {
    let mut twin = clearing();
    let op = twin.op();
    twin.feed(Input::SecretCleared { op, ok: true });
    assert_eq!(twin.replies, vec![(1, Err(Fail::Interrupted))]);
    assert!(twin.reference.credential.is_none());
}

#[test]
fn sess_secret_cleared() {
    let mut twin = storing();
    twin.feed_all([call(2), mutate(7, 11)]);
    let op = twin.op();
    twin.feed(Input::SecretStored { op, ok: false });
    let op = twin.op();
    let effects = twin.feed(Input::SecretCleared { op, ok: false });
    assert!(matches!(&effects[..], [RefEffect::GenerateCredential { .. }]), "the next call of the queue starts after the secret is cleared: {effects:?}");
}

#[test]
fn sess_secret_stored_drain() {
    let mut twin = storing();
    twin.feed_all([call(2), Input::Closing]);
    assert_eq!(twin.replies, vec![(2, Err(Fail::Closed))]);
    let op = twin.op();
    let effects = twin.feed(Input::SecretStored { op, ok: true });
    assert_eq!(effects, vec![RefEffect::Drained]);
    assert!(twin.replies.contains(&(1, Err(Fail::Closed))));
}

#[test]
fn sess_secret_cleared_drain() {
    let mut twin = clearing();
    twin.feed(Input::Closing);
    let op = twin.op();
    assert_eq!(twin.feed(Input::SecretCleared { op, ok: true }), vec![RefEffect::Drained]);
    assert!(matches!(twin.reference.state, RefState::Closed));
    assert_eq!(twin.replies, vec![(1, Err(Fail::Closed))], "the call hears Closed, not the reason of the clearing");
}

#[test]
fn sess_clearing_queue() {
    let mut twin = clearing();
    assert!(twin.feed(call(2)).is_empty());
    assert!(twin.feed(mutate(7, 11)).is_empty());
    assert_eq!(twin.reference.queue.len(), 2);
}

#[test]
fn sess_clearing_other() {
    let mut twin = clearing();
    twin.feed_all([Input::Online(false), published(PubState::Session), Input::Tick, ended(EndCause::ServerEnded)]);
    assert!(matches!(twin.reference.state, RefState::ClearingSecret { .. }));
    assert!(twin.reference.credential.is_none());
}

#[test]
fn sess_clearing_closing() {
    let mut twin = clearing();
    twin.feed_all([call(2), Input::Closing]);
    assert_eq!(twin.replies, vec![(2, Err(Fail::Closed))]);
    assert!(matches!(twin.reference.state, RefState::ClearingSecret { after: AfterClear::Drain, .. }));
}

#[test]
fn sess_generating_queue() {
    let mut twin = generating();
    twin.feed_all([call(2), mutate(7, 1)]);
    assert_eq!(twin.reference.queue.len(), 2);
}

#[test]
fn sess_generating_epoch_ended() {
    let mut twin = generating();
    let op = twin.op();
    twin.feed_all([call(2), ended(EndCause::SessionReset)]);
    assert_eq!(twin.replies, vec![(1, Err(Fail::Interrupted)), (2, Err(Fail::NoSession))]);
    twin.feed(Input::CredentialGenerated { op, credential: 5 });
    assert!(matches!(twin.reference.state, RefState::Idle), "a late generated credential is a foreign input");
}

#[test]
fn sess_storing_epoch_ended() {
    let mut twin = storing();
    twin.feed(ended(EndCause::ServerEnded));
    assert!(matches!(&twin.reference.state, RefState::Storing { after: AfterStore::Clear(Fail::Interrupted), .. }));
    assert!(twin.reference.credential.is_none());
}

#[test]
fn sess_generating_closing() {
    let mut twin = generating();
    twin.feed_all([call(2), mutate(7, 1)]);
    let effects = twin.feed(Input::Closing);
    assert!(effects.contains(&RefEffect::Drained) && effects.contains(&answer(7, 1, permanent(Fail::Closed))));
    assert_eq!(twin.replies, vec![(1, Err(Fail::Closed)), (2, Err(Fail::Closed))]);
}

#[test]
fn sess_storing_closing() {
    let mut twin = storing();
    twin.feed(call(2));
    let effects = twin.feed(Input::Closing);
    assert!(effects.is_empty(), "Drained follows SecretStored");
    assert!(matches!(twin.reference.state, RefState::Storing { after: AfterStore::Drain, .. }));
}

#[test]
fn sess_idle_mutate_stale() {
    let mut twin = Twin::new();
    let stale = Input::Mutate { author: 7, owner: None, req_id: 11, epoch: EPOCH + 1, path: OTP.into(), body: "{}".into(), purpose: 0 };
    assert_eq!(twin.feed(stale), vec![answer(7, 11, permanent(Fail::NoSession))]);
    twin.feed(published(PubState::Closing));
    assert_eq!(twin.feed(mutate(7, 12)), vec![answer(7, 12, permanent(Fail::NoSession))]);
}

#[test]
fn sess_idle_mutate_no_session() {
    let mut twin = Twin::bare();
    assert_eq!(twin.feed(mutate(7, 11)), vec![answer(7, 11, permanent(Fail::NoSession))]);
}

#[test]
fn sess_idle_mutate() {
    let mut twin = Twin::new();
    let effects = twin.feed(mutate(7, 11));
    assert!(matches!(&effects[..], [RefEffect::CommitPending { pending, .. }] if pending.request_id == 11 && pending.sequence == 1 && pending.author == Some(7) && pending.reply.is_none() && pending.epoch == EPOCH));
}

#[test]
fn sess_mutate_attempt_pending() {
    let mut twin = held();
    let pending = twin.pending();
    let effects = twin.feed(mutate(7, 11));
    assert!(matches!(&effects[..], [RefEffect::HttpRequest { pending: sent, .. }] if sent.request_id == pending.request_id && sent.sequence == pending.sequence), "the stored pair goes again: {effects:?}");
    assert!(matches!(twin.reference.state, RefState::Calling { attempts: 0, .. }));
}

#[test]
fn sess_mutate_duplicate() {
    for mut twin in [calling_mutate(), { let mut twin = Twin::new(); twin.feed(mutate(7, 11)); twin }] {
        assert!(twin.feed(mutate(7, 11)).is_empty());
        assert_eq!(twin.reference.queue.len(), 0);
    }
    let mut twin = persisting();
    twin.feed_all([mutate(7, 12), mutate(7, 12)]);
    assert_eq!(twin.reference.queue.len(), 1, "a repeat of a queued request is not queued twice");
}

#[test]
fn sess_mutate_queued() {
    let mut twin = persisting();
    assert!(twin.feed(mutate(7, 11)).is_empty());
    assert_eq!(twin.reference.queue.len(), 1);
}

#[test]
fn sess_dequeue_stale_mutate() {
    let mut twin = persisting();
    twin.feed(mutate(7, 11));
    twin.feed(Input::Published { epoch: EPOCH + 1, identity: None, state: PubState::Session });
    twin.feed(committed());
    let input = twin.response(1);
    twin.feed(input);
    let effects = twin.feed(committed());
    assert!(effects.contains(&answer(7, 11, permanent(Fail::NoSession))), "a queued Mutate keeps its epoch: {effects:?}");
    assert!(matches!(twin.reference.state, RefState::Idle));
}

#[test]
fn sess_cancel_queued() {
    let mut twin = persisting();
    twin.feed(mutate(7, 12));
    let effects = twin.feed(Input::Cancel { req_id: 12 });
    assert_eq!(effects, vec![answer(7, 12, permanent(Fail::Interrupted))]);
    assert!(matches!(twin.reference.state, RefState::Persisting { .. }), "the started request goes on");
}

#[test]
fn sess_cancel_started() {
    let mut twin = Twin::new();
    twin.feed(mutate(7, 11));
    assert!(twin.feed(Input::Cancel { req_id: 11 }).is_empty());
    twin.feed(committed());
    assert!(twin.feed(Input::Cancel { req_id: 11 }).is_empty());
    assert!(matches!(twin.reference.state, RefState::Calling { phase: CallPhase::InFlight(_), .. }));
    let input = twin.response(1);
    twin.feed(input);
    assert!(twin.feed(Input::Cancel { req_id: 11 }).is_empty());
    let done = twin.feed(committed());
    assert_eq!(done.iter().filter(|effect| matches!(effect, RefEffect::Answer { .. })).count(), 1, "one final answer: {done:?}");
    assert!(twin.feed(Input::Cancel { req_id: 99 }).is_empty());
}

#[test]
fn sess_persisting_committed() {
    let mut twin = persisting();
    let effects = twin.feed(committed());
    assert!(matches!(&effects[..], [RefEffect::HttpRequest { full_document: false, .. }]));
    assert!(matches!(twin.reference.state, RefState::Calling { attempts: 0, phase: CallPhase::InFlight(_), .. }));
}

#[test]
fn sess_persisting_committed_offline() {
    let mut twin = Twin::offline();
    twin.feed(call(1));
    assert!(twin.feed(committed()).is_empty());
    assert!(twin.in_phase(CallPhase::WaitingNetwork));
}

#[test]
fn sess_persisting_rejected() {
    let mut twin = Twin::new();
    twin.feed_all([mutate(7, 11), call(2)]);
    let effects = twin.feed(rejected(Reason::Storage));
    assert!(effects.contains(&answer(7, 11, permanent(Fail::Reason(Reason::Storage)))));
    assert!(matches!(twin.reference.state, RefState::Persisting { .. }), "the next entry of the queue starts");
}

#[test]
fn sess_call_queued() {
    for mut twin in [persisting(), calling(), landing(1)] {
        assert!(twin.feed(call(2)).is_empty());
        assert_eq!(twin.reference.queue.len(), 1);
    }
}

#[test]
fn sess_calling_response() {
    let mut twin = calling();
    let input = twin.response(1);
    let effects = twin.feed(input);
    assert!(matches!(&effects[..], [RefEffect::CommitLanding { digest: None, sequence: Some(1), answer: None, .. }]));
    assert!(twin.replies.is_empty(), "nothing is answered before the commit");
}

#[test]
fn sess_calling_response_sessionless() {
    let mut twin = Twin::new();
    twin.feed_all([call_of("visit", 1), committed()]);
    let input = Input::Response { request_id: twin.rid(), attempt_id: twin.aid(), body: "{}".into(), sequence: None, refusal: None };
    let effects = twin.feed(input);
    assert!(matches!(&effects[..], [RefEffect::CommitLanding { digest: None, sequence: None, .. }]));
    twin.feed(committed());
    assert_eq!(twin.reference.credential, Some(RefCredential { digest: CRED, sequence: 0 }), "a call without a session leaves the credential");
    assert_eq!(twin.replies, vec![(1, Ok("{}".into()))]);
}

#[test]
fn sess_calling_revoked() {
    let mut twin = calling();
    let input = Input::Revoked { request_id: twin.rid(), attempt_id: twin.aid(), body: "{}".into() };
    let effects = twin.feed(input);
    assert!(matches!(&effects[..], [RefEffect::CommitLanding { digest: None, sequence: None, .. }]));
    let done = twin.feed(committed());
    assert!(done.contains(&RefEffect::ServerEnded { epoch: EPOCH }));
    assert_eq!(twin.replies, vec![(1, Err(Fail::Revoked))]);
}

#[test]
fn sess_calling_revoked_logout() {
    let mut twin = Twin::new();
    twin.feed_all([call_of("logout", 1), committed()]);
    let input = Input::Revoked { request_id: twin.rid(), attempt_id: twin.aid(), body: r#"{"revoked":true}"#.into() };
    twin.feed(input);
    let done = twin.feed(committed());
    assert!(done.contains(&RefEffect::ServerEnded { epoch: EPOCH }));
    assert_eq!(twin.replies, vec![(1, Ok(r#"{"revoked":true}"#.into()))], "a revoked answer to logout confirms the logout");
}

#[test]
fn sess_calling_sequence_conflict() {
    let mut twin = Twin::new();
    twin.feed_all([mutate(7, 11), committed()]);
    let input = twin.response(9);
    let effects = twin.feed(input);
    assert_eq!(effects, vec![answer(7, 11, permanent(Fail::SequenceConflict))]);
    assert!(matches!(twin.reference.state, RefState::Blocked { reason: Block::SequenceConflict, .. }));
    assert_eq!(twin.reference.credential, Some(RefCredential { digest: CRED, sequence: 0 }));
}

#[test]
fn sess_calling_no_sequence() {
    let mut twin = calling();
    let input = Input::Response { request_id: twin.rid(), attempt_id: twin.aid(), body: "{}".into(), sequence: None, refusal: None };
    twin.feed(input);
    assert_eq!(twin.replies, vec![(1, Err(Fail::Protocol))]);
    assert!(matches!(twin.reference.state, RefState::Blocked { reason: Block::Protocol, .. }));
}

#[test]
fn sess_calling_server_refusal_code() {
    for (author, code) in [(false, SessionCode::AccountUnavailable), (true, SessionCode::Unauthenticated)] {
        let mut twin = if author { calling_mutate() } else { calling() };
        let input = Input::Response { request_id: twin.rid(), attempt_id: twin.aid(), body: "{}".into(), sequence: Some(1), refusal: Some(code) };
        let before = twin.feed(input);
        assert!(matches!(&before[..], [RefEffect::CommitLanding { sequence: Some(1), .. }]));
        assert!(twin.replies.is_empty() && !before.iter().any(|effect| matches!(effect, RefEffect::Answer { .. })), "answered after the commit");
        let after = twin.feed(committed());
        if author {
            assert!(after.contains(&answer(7, 11, permanent(Fail::Session(code)))), "{after:?}");
        } else {
            assert_eq!(twin.replies, vec![(1, Err(Fail::Session(code)))]);
        }
        assert_eq!(twin.reference.credential.map(|credential| credential.digest), Some(CRED), "a refusal keeps the credential");
        assert_eq!(twin.reference.credential.map(|credential| credential.sequence), Some(1), "the confirmed sequence is spent");
    }
    let mut twin = calling();
    let input = Input::Response { request_id: twin.rid(), attempt_id: twin.aid(), body: "{}".into(), sequence: None, refusal: Some(SessionCode::AccountUnavailable) };
    twin.feed_all([input, committed()]);
    assert_eq!(twin.reference.credential.map(|credential| credential.sequence), Some(0), "an unconfirmed refusal spends no sequence");
}

#[test]
fn sess_calling_stray() {
    let mut twin = calling();
    let (rid, aid) = (twin.rid(), twin.aid());
    twin.feed_all([
        Input::Response { request_id: rid + 50, attempt_id: aid, body: "{}".into(), sequence: Some(1), refusal: None },
        Input::Response { request_id: rid, attempt_id: aid + 50, body: "{}".into(), sequence: Some(1), refusal: None },
        Input::Revoked { request_id: rid, attempt_id: aid + 50, body: "{}".into() },
        Input::HttpError { request_id: rid, attempt_id: aid + 50, temporary: false, auth: true, persisted_missing: false },
        Input::Dropped { request_id: rid + 50, attempt_id: aid },
    ]);
    assert!(matches!(twin.reference.state, RefState::Calling { phase: CallPhase::InFlight(a), .. } if a == aid));
}

#[test]
fn sess_calling_dropped() {
    let mut twin = calling();
    let input = twin.dropped();
    assert_eq!(twin.feed(input), vec![RefEffect::ScheduleTick { attempts: 1 }]);
    assert!(twin.in_phase(CallPhase::WaitingRetry));
}

#[test]
fn sess_calling_dropped_mutate() {
    let mut twin = calling_mutate();
    let input = twin.dropped();
    assert_eq!(twin.feed(input), vec![answer(7, 11, Outcome::Dropped)]);
    assert!(matches!(twin.reference.state, RefState::PendingHeld { .. }), "the slot stays with the author");
}

#[test]
fn sess_calling_dropped_offline() {
    let mut twin = calling();
    let (rid, aid) = (twin.rid(), twin.aid());
    twin.feed(Input::Online(false));
    assert!(twin.feed(Input::Dropped { request_id: rid, attempt_id: aid }).is_empty(), "the cancelled attempt gives no outcome");
    assert!(twin.in_phase(CallPhase::WaitingNetwork), "the request waits for the network");
}

#[test]
fn sess_calling_persisted_query_missing() {
    let mut twin = calling();
    let input = Input::HttpError { request_id: twin.rid(), attempt_id: twin.aid(), temporary: false, auth: false, persisted_missing: true };
    let effects = twin.feed(input);
    assert!(matches!(&effects[..], [RefEffect::HttpRequest { full_document: true, .. }]), "the same pair goes with the document: {effects:?}");
    assert!(matches!(twin.reference.state, RefState::Calling { attempts: 0, document_sent: true, .. }));
}

#[test]
fn sess_calling_persisted_query_refused() {
    let mut twin = calling();
    let missing = |twin: &Twin| Input::HttpError { request_id: twin.rid(), attempt_id: twin.aid(), temporary: false, auth: false, persisted_missing: true };
    let first = missing(&twin);
    twin.feed(first);
    let second = missing(&twin);
    twin.feed(second);
    twin.feed(committed());
    assert_eq!(twin.replies, vec![(1, Err(Fail::Http))], "a second miss is a permanent refusal");
}

#[test]
fn sess_calling_offline_cancel() {
    let mut twin = calling();
    let aid = twin.aid();
    assert_eq!(twin.feed(Input::Online(false)), vec![RefEffect::HttpCancel { attempt_id: aid }]);
    assert!(twin.in_phase(CallPhase::WaitingNetwork));
    assert!(matches!(&twin.feed(Input::Online(true))[..], [RefEffect::HttpRequest { .. }]), "the same request goes again");
}

#[test]
fn sess_calling_offline_mutate() {
    let mut twin = calling_mutate();
    let aid = twin.aid();
    let effects = twin.feed(Input::Online(false));
    assert_eq!(effects, vec![RefEffect::HttpCancel { attempt_id: aid }, answer(7, 11, Outcome::Dropped)]);
    assert!(twin.feed(Input::Online(true)).is_empty() && twin.feed(Input::Tick).is_empty(), "only a new Mutate of the author sends it again");
}

#[test]
fn sess_calling_offline_idle() {
    let mut twin = Twin::offline();
    twin.feed_all([call(1), committed()]);
    assert!(twin.feed(Input::Online(false)).is_empty());
}

#[test]
fn sess_calling_temporary() {
    let mut twin = calling();
    let input = twin.http_error(true, false);
    assert_eq!(twin.feed(input), vec![RefEffect::ScheduleTick { attempts: 1 }]);
    assert!(twin.in_phase(CallPhase::WaitingRetry));
}

#[test]
fn sess_calling_temporary_mutate() {
    let mut twin = calling_mutate();
    let input = twin.http_error(true, false);
    assert_eq!(twin.feed(input), vec![answer(7, 11, Outcome::Error(Kind::Temporary, Fail::Http))]);
    assert!(matches!(twin.reference.state, RefState::PendingHeld { .. }));
}

#[test]
fn sess_calling_temporary_offline() {
    let mut twin = calling();
    let (rid, aid) = (twin.rid(), twin.aid());
    twin.feed(Input::Online(false));
    let late = Input::HttpError { request_id: rid, attempt_id: aid, temporary: true, auth: false, persisted_missing: false };
    assert!(twin.feed(late).is_empty(), "no timer while the network is away");
    assert!(twin.in_phase(CallPhase::WaitingNetwork));
    assert!(matches!(&twin.feed(Input::Online(true))[..], [RefEffect::HttpRequest { .. }]));
}

#[test]
fn sess_calling_unauthorized() {
    let mut twin = calling();
    let input = twin.http_error(false, true);
    twin.feed(input);
    let effects = twin.feed(committed());
    assert!(!effects.iter().any(|effect| matches!(effect, RefEffect::ServerEnded { .. } | RefEffect::CredentialCommitted(_))), "401 gives Lifecycle no input");
    assert_eq!(twin.replies, vec![(1, Err(Fail::Unauthorized))]);
}

#[test]
fn sess_calling_permanent() {
    let mut twin = calling();
    let input = twin.http_error(false, false);
    let effects = twin.feed(input);
    assert!(matches!(&effects[..], [RefEffect::CommitLanding { digest: None, sequence: None, .. }]));
    twin.feed(committed());
    assert_eq!(twin.replies, vec![(1, Err(Fail::Http))]);
    assert_eq!(twin.reference.credential.map(|credential| credential.sequence), Some(0), "a failed call spends no sequence");
}

#[test]
fn sess_calling_tick() {
    let mut twin = calling();
    let input = twin.http_error(true, false);
    twin.feed(input);
    let effects = twin.feed(Input::Tick);
    assert!(matches!(&effects[..], [RefEffect::HttpRequest { .. }]), "{effects:?}");
    assert!(twin.feed(Input::Tick).is_empty(), "the second Tick starts nothing");
}

#[test]
fn sess_calling_tick_other() {
    let mut twin = calling();
    assert!(twin.feed(Input::Tick).is_empty());
    let mut waiting = Twin::offline();
    waiting.feed_all([call(1), committed()]);
    assert!(waiting.feed(Input::Tick).is_empty());
}

#[test]
fn sess_calling_online() {
    let mut twin = Twin::offline();
    twin.feed_all([call(1), committed()]);
    assert!(matches!(&twin.feed(Input::Online(true))[..], [RefEffect::HttpRequest { .. }]));
    assert!(twin.in_phase(CallPhase::InFlight(twin.aid())));
}

#[test]
fn sess_calling_online_in_flight() {
    let mut twin = calling();
    assert!(twin.feed(Input::Online(true)).is_empty(), "a request in the network is not sent twice");
}

#[test]
fn sess_calling_secret_locked() {
    let mut twin = calling();
    let input = Input::SecretMissing { request_id: twin.rid(), attempt_id: twin.aid(), lost: false };
    assert!(twin.feed(input).is_empty());
    assert!(twin.in_phase(CallPhase::WaitingSecret));
    assert_eq!(twin.reference.credential.map(|credential| credential.sequence), Some(0), "the sequence is not touched");
    assert!(twin.feed(Input::Tick).is_empty() && twin.feed(Input::Online(true)).is_empty(), "only Foreground repeats");
}

#[test]
fn sess_calling_secret_foreground() {
    let mut twin = calling();
    let input = Input::SecretMissing { request_id: twin.rid(), attempt_id: twin.aid(), lost: false };
    twin.feed(input);
    let rid = twin.rid();
    let effects = twin.feed(Input::Foreground);
    assert!(matches!(&effects[..], [RefEffect::HttpRequest { pending, .. }] if pending.request_id == rid));
}

#[test]
fn sess_calling_secret_foreground_offline() {
    let mut twin = calling();
    let input = Input::SecretMissing { request_id: twin.rid(), attempt_id: twin.aid(), lost: false };
    twin.feed_all([input, Input::Online(false)]);
    assert!(twin.feed(Input::Foreground).is_empty());
    assert!(twin.in_phase(CallPhase::WaitingNetwork));
}

#[test]
fn sess_calling_secret_lost() {
    let mut twin = calling_mutate();
    let input = Input::SecretMissing { request_id: twin.rid(), attempt_id: twin.aid(), lost: true };
    assert_eq!(twin.feed(input), vec![answer(7, 11, permanent(Fail::StateLost))]);
    assert!(matches!(twin.reference.state, RefState::SecretMismatch));
    let mut call_twin = calling();
    let input = Input::SecretMissing { request_id: call_twin.rid(), attempt_id: call_twin.aid(), lost: true };
    call_twin.feed(input);
    assert_eq!(call_twin.replies, vec![(1, Err(Fail::StateLost))]);
}

#[test]
fn sess_landing_committed_credential() {
    let mut twin = generating();
    let op = twin.op();
    twin.feed(Input::CredentialGenerated { op, credential: 42 });
    let op = twin.op();
    twin.feed_all([Input::SecretStored { op, ok: true }, committed()]);
    let input = twin.response(1);
    twin.feed(input);
    assert_eq!(twin.feed(committed()), vec![RefEffect::CredentialCommitted(42)], "the identity is the digest of the generated credential");
    assert!(twin.replies.is_empty(), "the answer waits for the publication");
    assert_eq!(twin.reference.credential, Some(RefCredential { digest: 42, sequence: 1 }));
    assert_eq!(twin.reference.after_publish, Some((1, "{}".into())));
}

#[test]
fn sess_landing_mutate() {
    let mut twin = calling_mutate();
    let input = twin.response(1);
    let effects = twin.feed(input);
    assert!(matches!(&effects[..], [RefEffect::CommitLanding { answer: Some((11, Ok(_))), .. }]), "the answer is stored with the landing: {effects:?}");
    assert_eq!(twin.feed(committed()), vec![answer(7, 11, Outcome::Response("{}".into()))]);
    assert_eq!(twin.reference.credential, Some(RefCredential { digest: CRED, sequence: 1 }));
}

#[test]
fn sess_landing_committed() {
    let mut twin = landing(1);
    twin.feed(committed());
    assert_eq!(twin.replies, vec![(1, Ok("{}".into()))]);
    assert_eq!(twin.reference.credential.map(|credential| credential.sequence), Some(1));
}

#[test]
fn sess_landing_committed_error() {
    let mut twin = calling_mutate();
    let input = twin.http_error(false, false);
    let before = twin.feed(input);
    assert!(matches!(&before[..], [RefEffect::CommitLanding { answer: Some((11, Err(Fail::Http))), .. }]));
    assert_eq!(twin.feed(committed()), vec![answer(7, 11, permanent(Fail::Http))]);
}

#[test]
fn sess_landing_rejected() {
    let mut twin = calling_mutate();
    let input = twin.response(1);
    twin.feed(input);
    let effects = twin.feed(rejected(Reason::Storage));
    assert_eq!(effects, vec![answer(7, 11, permanent(Fail::Reason(Reason::Storage)))]);
    assert!(matches!(twin.reference.state, RefState::Blocked { reason: Block::Storage, pending: Some(_) }), "the stored pending stays");
}

#[test]
fn sess_blocked_call() {
    let mut twin = blocked_storage();
    twin.feed(call(2));
    assert_eq!(twin.replies.last(), Some(&(2, Err(Fail::StateLost))), "no new pending replaces the stored one");
}

#[test]
fn sess_blocked_mutate_lost() {
    let mut twin = opened(open(Some((CRED, 0)), None, true, Secret::Present(CRED)));
    assert_eq!(twin.feed(mutate(7, 11)), vec![answer(7, 11, permanent(Fail::StateLost))]);
}

#[test]
fn sess_blocked_mutate() {
    let mut twin = Twin::new();
    twin.feed_all([mutate(7, 11), committed()]);
    let input = twin.response(5);
    twin.feed(input);
    assert_eq!(twin.feed(mutate(7, 12)), vec![answer(7, 12, permanent(Fail::StateLost))]);
}

#[test]
fn sess_blocked_attempt_pending() {
    let mut twin = Twin::new();
    twin.feed_all([mutate(7, 11), committed()]);
    let input = twin.response(1);
    twin.feed_all([input, rejected(Reason::Storage)]);
    let effects = twin.feed(mutate(7, 11));
    assert!(matches!(&effects[..], [RefEffect::HttpRequest { pending, .. }] if pending.request_id == 11 && pending.sequence == 1));
}

#[test]
fn sess_blocked_mutate_refused() {
    let mut twin = Twin::new();
    twin.feed_all([mutate(7, 11), committed()]);
    let input = twin.response(5);
    twin.feed(input);
    assert_eq!(twin.feed(mutate(7, 11)), vec![answer(7, 11, permanent(Fail::SequenceConflict))], "the request does not leave");
}

#[test]
fn sess_blocked_retry() {
    let mut twin = blocked_storage();
    let effects = twin.feed(Input::Tick);
    assert!(matches!(&effects[..], [RefEffect::HttpRequest { pending, .. }] if pending.sequence == 1), "the same pair goes again: {effects:?}");
    let mut again = blocked_storage();
    let effects = again.feed(Input::Online(true));
    assert!(matches!(&effects[..], [RefEffect::HttpRequest { .. }]));
}

#[test]
fn sess_blocked_stays() {
    let mut twin = Twin::new();
    twin.feed_all([mutate(7, 11), committed()]);
    let input = twin.response(5);
    twin.feed(input);
    assert!(twin.feed(Input::Tick).is_empty() && twin.feed(Input::Online(false)).is_empty() && twin.feed(Input::Online(true)).is_empty());
    assert!(matches!(twin.reference.state, RefState::Blocked { .. }));
}

#[test]
fn sess_blocked_epoch_ended() {
    let mut twin = blocked_storage();
    twin.feed(call(2));
    twin.feed(ended(EndCause::SessionReset));
    assert!(matches!(twin.reference.state, RefState::Idle) && twin.reference.credential.is_none());
}

#[test]
fn sess_blocked_closing() {
    let mut twin = blocked_storage();
    assert_eq!(twin.feed(Input::Closing), vec![RefEffect::Drained]);
}

#[test]
fn sess_idle_stray() {
    let mut twin = Twin::new();
    twin.feed_all([
        Input::Response { request_id: 1, attempt_id: 1, body: "{}".into(), sequence: Some(1), refusal: None },
        Input::HttpError { request_id: 1, attempt_id: 1, temporary: true, auth: false, persisted_missing: false },
        Input::Dropped { request_id: 1, attempt_id: 1 },
        Input::Revoked { request_id: 1, attempt_id: 1, body: "{}".into() },
        committed(), rejected(Reason::Storage), Input::Tick,
    ]);
    assert!(twin.history.is_empty());
}

#[test]
fn sess_online() {
    for mut twin in [Twin::new(), persisting(), calling(), landing(1)] {
        twin.feed_all([Input::Online(false), Input::Online(true)]);
        assert_eq!(twin.reference.online, Some(true));
    }
}

#[test]
fn sess_published() {
    for mut twin in [Twin::new(), persisting(), calling(), landing(1), held(), blocked_storage()] {
        twin.feed(Input::Published { epoch: 8, identity: None, state: PubState::Session });
        assert_eq!(twin.reference.epoch, 8);
    }
}

#[test]
fn sess_published_answers_login() {
    let mut twin = generating();
    let op = twin.op();
    twin.feed(Input::CredentialGenerated { op, credential: 42 });
    let op = twin.op();
    twin.feed_all([Input::SecretStored { op, ok: true }, committed()]);
    let input = twin.response(1);
    twin.feed_all([input, committed(), call(2)]);
    assert!(twin.replies.is_empty());
    twin.feed(Input::Published { epoch: EPOCH + 2, identity: Some(42), state: PubState::Session });
    assert_eq!(twin.replies, vec![(1, Ok("{}".into()))], "the answer follows the publication of the identity");
    assert!(twin.history.iter().filter(|effect| matches!(effect, RefEffect::CommitPending { .. })).count() >= 2, "the queue goes on after the answer");
}

#[test]
fn sess_published_login_failed() {
    for (state, want) in [(PubState::StorageFailed, Fail::Storage), (PubState::EndingFailed, Fail::Storage), (PubState::Ending, Fail::Interrupted)] {
        let mut twin = generating();
        let op = twin.op();
        twin.feed(Input::CredentialGenerated { op, credential: 42 });
        let op = twin.op();
        twin.feed_all([Input::SecretStored { op, ok: true }, committed()]);
        let input = twin.response(1);
        twin.feed_all([input, committed()]);
        twin.feed(Input::Published { epoch: EPOCH, identity: None, state });
        assert_eq!(twin.replies, vec![(1, Err(want))]);
        assert!(twin.reference.after_publish.is_none());
    }
}

#[test]
fn sess_landing_login_failed_clears() {
    let mut twin = generating();
    let op = twin.op();
    twin.feed(Input::CredentialGenerated { op, credential: 42 });
    let op = twin.op();
    twin.feed_all([Input::SecretStored { op, ok: true }, committed()]);
    let input = twin.http_error(false, false);
    let landing = twin.feed(input);
    assert!(matches!(&landing[..], [RefEffect::CommitLanding { erase_session: true, answer: None, .. }]), "{landing:?}");
    let cleared = twin.feed(committed());
    assert!(matches!(&cleared[..], [RefEffect::ClearSecret { .. }]), "{cleared:?}");
    assert!(twin.replies.is_empty(), "the call hears the error after the secret is gone");
    let op = twin.op();
    twin.feed(Input::SecretCleared { op, ok: true });
    assert_eq!(twin.replies, vec![(1, Err(Fail::Http))]);
    assert_eq!(twin.reference.credential, None);
    assert!(matches!(&twin.feed(call_of("login", 2))[..], [RefEffect::GenerateCredential { .. }]), "the next sign-in goes as the first");
}

#[test]
fn sess_published_ending_failed() {
    let mut twin = Twin::new();
    twin.feed(published(PubState::Ending));
    twin.feed(call(1));
    twin.feed(published(PubState::EndingFailed));
    assert_eq!(twin.replies, vec![(1, Err(Fail::Storage))]);
    assert_eq!(twin.reference.queue.len(), 0);
}

#[test]
fn sess_published_drains_queue() {
    let mut twin = Twin::new();
    twin.feed(published(PubState::Ending));
    twin.feed(call(1));
    let effects = twin.feed(published(PubState::Session));
    assert!(matches!(&effects[..], [RefEffect::CommitPending { .. }]), "the call waited for the publication: {effects:?}");
}

#[test]
fn sess_epoch_ended_cleared() {
    for cause in [EndCause::SessionReset, EndCause::ServerEnded] {
        let mut twin = calling_mutate();
        twin.feed(call(2));
        let aid = twin.aid();
        let effects = twin.feed(ended(cause));
        assert!(effects.contains(&RefEffect::HttpCancel { attempt_id: aid }) && effects.contains(&answer(7, 11, permanent(Fail::Interrupted))));
        assert_eq!(twin.replies, vec![(2, Err(Fail::NoSession))]);
        assert!(twin.reference.credential.is_none());
    }
    let mut twin = persisting();
    twin.feed(ended(EndCause::SessionReset));
    assert_eq!(twin.replies, vec![(1, Err(Fail::Interrupted))]);
    let mut twin = landing(1);
    twin.feed(ended(EndCause::SessionReset));
    assert_eq!(twin.replies, vec![(1, Err(Fail::Interrupted))]);
}

#[test]
fn sess_epoch_ended_other() {
    for mut twin in [persisting(), calling(), landing(1)] {
        twin.feed(ended(EndCause::Other));
        twin.feed(Input::EpochEnded { epoch: EPOCH + 5, cause: EndCause::SessionReset });
        assert!(twin.replies.is_empty());
    }
}

#[test]
fn sess_idle_epoch_ended() {
    let mut twin = Twin::new();
    twin.feed(ended(EndCause::ServerEnded));
    assert!(twin.reference.credential.is_none());
}

#[test]
fn sess_idle_epoch_ended_other() {
    let mut twin = Twin::new();
    twin.feed(ended(EndCause::Other));
    assert!(twin.reference.credential.is_some());
}

#[test]
fn sess_closing() {
    for mut twin in [Twin::new(), persisting(), calling(), landing(1), held(), blocked_storage()] {
        twin.feed(call(7));
        twin.feed(Input::Closing);
        assert!(matches!(twin.reference.state, RefState::Closed) && twin.history.contains(&RefEffect::Drained));
    }
}

#[test]
fn sess_closed() {
    let mut twin = Twin::new();
    twin.feed_all([Input::Closing, call(1), mutate(7, 11), Input::Tick, Input::Online(true), Input::Closing]);
    assert_eq!(twin.replies, vec![(1, Err(Fail::Closed))]);
}

#[test]
fn sess_open_empty() {
    let twin = opened(open(None, None, false, Secret::Missing));
    assert!(matches!(twin.reference.state, RefState::Idle) && twin.history.is_empty());
}

#[test]
fn sess_open_restored() {
    let mut twin = opened(open(Some((CRED, 6)), None, false, Secret::Present(CRED)));
    assert!(matches!(&twin.feed(call(1))[..], [RefEffect::CommitPending { pending, .. }] if pending.sequence == 7));
}

#[test]
fn sess_open_pending() {
    let owner = crate::transport::RequestOwner::Operation { seq: 3 };
    let twin = opened(open(Some((CRED, 6)), Some(stored(Some(owner), 3, 7)), false, Secret::Present(CRED)));
    assert!(matches!(&twin.reference.state, RefState::PendingHeld { pending } if pending.author == Some(super::ref_session::OUTBOX_AUTHOR)), "the author hears the stored pair from Restored and repeats it");
    assert!(twin.history.is_empty(), "Session sends nothing itself");
}

#[test]
fn sess_open_direct_pending() {
    let twin = opened(open(Some((CRED, 6)), Some(stored(None, 5, 7)), false, Secret::Present(CRED)));
    assert!(matches!(twin.reference.state, RefState::Idle) && twin.history.is_empty());
}

#[test]
fn sess_open_direct_login_pending() {
    let login = RefPending { class: Class::Login, credential: Some(CRED), sequence: 1, ..stored(None, 5, 1) };
    let twin = opened(open(Some((CRED, 0)), Some(login), false, Secret::Present(CRED)));
    assert!(matches!(&twin.history[..], [RefEffect::HttpRequest { pending, .. }] if pending.request_id == 5 && pending.credential == Some(CRED)), "a sign-in stored before a crash goes again at once: {:?}", twin.history);
}

#[test]
fn sess_open_pending_lost() {
    let mut twin = opened(open(Some((CRED, 6)), None, true, Secret::Present(CRED)));
    assert!(matches!(twin.reference.state, RefState::Blocked { pending: None, reason: Block::StateLost }));
    twin.feed(call(1));
    assert_eq!(twin.replies, vec![(1, Err(Fail::StateLost))]);
}

#[test]
fn sess_open_secret_unavailable() {
    let mut twin = opened(open(Some((CRED, 6)), None, false, Secret::Error("locked".into())));
    assert!(matches!(&twin.reference.state, RefState::SecretUnavailable { error, read: None } if error == "locked"));
    twin.feed(call(1));
    assert_eq!(twin.replies, vec![(1, Err(Fail::Storage))]);
}

#[test]
fn sess_open_orphan_secret() {
    let twin = opened(open(None, None, false, Secret::Present(9)));
    assert!(matches!(twin.history[..], [RefEffect::ClearSecret { .. }]), "the orphan secret is erased: {:?}", twin.history);
}

#[test]
fn sess_open_digest_without_secret() {
    let twin = opened(open(Some((CRED, 6)), None, false, Secret::Missing));
    assert_eq!(twin.history, vec![RefEffect::SessionReset]);
    assert!(twin.reference.credential.is_none());
}

#[test]
fn sess_open_digest_mismatch() {
    let mut twin = opened(open(Some((CRED, 6)), None, false, Secret::Present(9)));
    assert!(matches!(twin.reference.state, RefState::SecretMismatch));
    twin.feed_all([call(1), mutate(7, 11)]);
    assert_eq!(twin.replies, vec![(1, Err(Fail::StateLost))]);
    assert_eq!(twin.reference.credential.map(|credential| credential.digest), Some(CRED), "the credential is not replaced");
}

#[test]
fn sess_restored_direct_pending() {
    let mut twin = Twin::new();
    let effects = twin.feed(Input::Restored { epoch: EPOCH, pending: Some(stored(None, 5, 1)) });
    assert_eq!(effects, vec![RefEffect::RestoreClassified { epoch: EPOCH }]);
    assert!(twin.reference.restored_direct.is_some());
}

#[test]
fn sess_restore_ready_direct() {
    let mut twin = Twin::new();
    twin.feed_all([Input::Restored { epoch: EPOCH, pending: Some(stored(None, 5, 1)) }]);
    let effects = twin.feed(Input::RestoreReady { epoch: EPOCH });
    assert!(matches!(&effects[..], [RefEffect::HttpRequest { pending, .. }] if pending.request_id == 5 && pending.reply.is_none()));
    let input = twin.response(1);
    twin.feed_all([input, committed()]);
    assert!(twin.replies.is_empty(), "an answer without a caller shows nothing");
    assert_eq!(twin.reference.credential.map(|credential| credential.sequence), Some(1));
}

#[test]
fn sess_restore_ready_other() {
    let mut twin = persisting();
    twin.feed(Input::Restored { epoch: EPOCH, pending: Some(stored(None, 5, 1)) });
    assert!(twin.feed(Input::RestoreReady { epoch: EPOCH }).is_empty());
    let mut idle = Twin::new();
    assert!(idle.feed(Input::RestoreReady { epoch: EPOCH }).is_empty());
}

#[test]
fn sess_restored_direct_on_idle() {
    let mut twin = Twin::new();
    twin.feed_all([call(1), Input::Restored { epoch: EPOCH, pending: None }]);
    twin.feed(Input::RestoreReady { epoch: EPOCH });
    assert!(twin.reference.restored_direct.is_none());
    let mut busy = persisting();
    busy.feed_all([Input::RestoreReady { epoch: EPOCH }, Input::Restored { epoch: EPOCH, pending: None }]);
    busy.feed(committed());
    let input = busy.response(1);
    busy.feed_all([input, committed()]);
    assert!(matches!(busy.reference.state, RefState::Idle));
}

#[test]
fn sess_restored_direct_dropped() {
    let mut twin = Twin::new();
    twin.feed(Input::Restored { epoch: EPOCH, pending: Some(stored(None, 5, 1)) });
    twin.feed(ended(EndCause::Other));
    assert!(twin.reference.restored_direct.is_none());
    let mut closing = Twin::new();
    closing.feed_all([Input::Restored { epoch: EPOCH, pending: Some(stored(None, 5, 1)) }, Input::Closing]);
    assert!(closing.reference.restored_direct.is_none());
}

#[test]
fn sess_restored_late_epoch() {
    let mut twin = Twin::new();
    twin.feed_all([Input::Restored { epoch: EPOCH, pending: Some(stored(None, 5, 1)) }, Input::RestoreReady { epoch: EPOCH }]);
    twin.feed(ended(EndCause::SessionReset));
    twin.feed(Input::Published { epoch: EPOCH + 1, identity: None, state: PubState::NoSession });
    let late = twin.feed(Input::Restored { epoch: EPOCH, pending: Some(stored(None, 5, 1)) });
    assert_eq!(late, vec![RefEffect::RestoreClassified { epoch: EPOCH }], "the pending of the ended epoch starts no request");
    assert!(twin.reference.restored_direct.is_none());
    assert!(twin.feed(Input::RestoreReady { epoch: EPOCH }).is_empty());
    assert!(!twin.reference.restore_ready, "the permission of the ended epoch is dropped");
}

#[test]
fn sess_snapshot_late_epoch() {
    let mut twin = Twin::offline();
    twin.feed(ended(EndCause::SessionReset));
    twin.feed(Input::Published { epoch: EPOCH + 1, identity: None, state: PubState::NoSession });
    assert!(twin.feed(Input::Snapshot { epoch: EPOCH, online: true }).is_empty());
    assert_eq!(twin.reference.online, Some(false), "the snapshot of the ended epoch changes nothing");
    twin.feed(Input::Snapshot { epoch: EPOCH + 1, online: true });
    assert_eq!(twin.reference.online, Some(true));
}

#[test]
fn sess_idle_call_awaiting_publication() {
    let mut twin = generating();
    let op = twin.op();
    twin.feed(Input::CredentialGenerated { op, credential: 42 });
    let op = twin.op();
    twin.feed_all([Input::SecretStored { op, ok: true }, committed()]);
    let input = twin.response(1);
    twin.feed_all([input, committed()]);
    assert!(twin.reference.after_publish.is_some());
    assert!(twin.feed(call(2)).is_empty(), "a new call waits for the publication of the credential");
    assert!(twin.feed(mutate(7, 11)).is_empty(), "so does a Mutate");
    assert_eq!(twin.reference.queue.len(), 2);
    assert_eq!(twin.history.iter().filter(|effect| matches!(effect, RefEffect::CommitPending { .. })).count(), 1);
}

#[test]
fn sess_restored_other() {
    for mut twin in [persisting(), generating(), held(), blocked_storage(), opened(open(Some((CRED, 6)), None, false, Secret::Present(9)))] {
        let effects = twin.feed(Input::Restored { epoch: EPOCH, pending: Some(stored(None, 5, 1)) });
        assert_eq!(effects, vec![RefEffect::RestoreClassified { epoch: EPOCH }]);
        assert!(twin.reference.restored_direct.is_none());
    }
}

#[test]
fn sess_interest() {
    let mut twin = Twin::new();
    let effects = twin.feed(Input::InterestPlus(5));
    assert_eq!(effects, vec![RefEffect::RetryState { stream: 5, attempts: 0, phase: RetryPhase::Idle }]);
    twin.feed_all([call(1), committed()]);
    let input = twin.http_error(true, false);
    let effects = twin.feed(input);
    assert!(effects.contains(&RefEffect::RetryState { stream: 5, attempts: 1, phase: RetryPhase::WaitingRetry }), "{effects:?}");
}

#[test]
fn sess_interest_minus() {
    let mut twin = Twin::new();
    twin.feed_all([Input::InterestPlus(5), Input::InterestPlus(6), Input::InterestMinus(5)]);
    twin.feed_all([call(1), committed()]);
    assert!(twin.history.iter().rev().take(1).all(|effect| !matches!(effect, RefEffect::RetryState { stream: 5, .. })));
    assert_eq!(twin.reference.subscribers.len(), 1);
}

#[test]
fn sess_interest_minus_unknown() {
    let mut twin = Twin::new();
    twin.feed(Input::InterestPlus(5));
    assert!(twin.feed(Input::InterestMinus(9)).is_empty());
    assert_eq!(twin.reference.subscribers.len(), 1);
}

#[test]
fn sess_foreground_other() {
    for mut twin in [Twin::new(), persisting(), calling(), landing(1), held(), blocked_storage(), generating()] {
        assert!(twin.feed(Input::Foreground).is_empty());
    }
}

#[test]
fn sess_mismatch_call() {
    let mut twin = opened(open(Some((CRED, 6)), None, false, Secret::Present(9)));
    for operation in ["social", "logout", "login"] { twin.feed(call_of(operation, 1)); }
    assert!(twin.replies.iter().all(|(_, answer)| *answer == Err(Fail::StateLost)));
}

#[test]
fn sess_mismatch_sessionless() {
    for secret in [Secret::Present(9), Secret::Error("locked".into())] {
        let mut twin = opened(open(Some((CRED, 6)), None, false, secret));
        let before = twin.reference.state.clone();
        let effects = twin.feed(call_of("visit", 1));
        assert!(matches!(&effects[..], [RefEffect::CommitPending { pending, .. }] if pending.class == Class::Sessionless));
        twin.feed(committed());
        let input = Input::Response { request_id: twin.rid(), attempt_id: twin.aid(), body: "{}".into(), sequence: None, refusal: None };
        twin.feed_all([input, committed()]);
        assert_eq!(twin.reference.state, before, "the state returns to where the call began");
        assert_eq!(twin.replies, vec![(1, Ok("{}".into()))]);
    }
}

#[test]
fn sess_mismatch_mutate() {
    let mut twin = opened(open(Some((CRED, 6)), None, false, Secret::Present(9)));
    assert_eq!(twin.feed(mutate(7, 11)), vec![answer(7, 11, permanent(Fail::StateLost))]);
}

#[test]
fn sess_mismatch_epoch_ended() {
    for secret in [Secret::Present(9), Secret::Error("locked".into())] {
        let mut twin = opened(open(Some((CRED, 6)), None, false, secret));
        twin.feed(ended(EndCause::SessionReset));
        assert!(matches!(twin.reference.state, RefState::Idle) && twin.reference.credential.is_none());
    }
}

#[test]
fn sess_mismatch_closing() {
    for secret in [Secret::Present(9), Secret::Error("locked".into())] {
        let mut twin = opened(open(Some((CRED, 6)), None, false, secret));
        assert_eq!(twin.feed(Input::Closing), vec![RefEffect::Drained]);
    }
}

#[test]
fn sess_mismatch_other() {
    for secret in [Secret::Present(9), Secret::Error("locked".into())] {
        let mut twin = opened(open(Some((CRED, 6)), None, false, secret));
        twin.feed_all([Input::Online(false), Input::Tick, published(PubState::Session), Input::Cancel { req_id: 3 }, ended(EndCause::Other)]);
        assert_eq!(twin.reference.online, Some(false));
        assert!(matches!(twin.reference.state, RefState::SecretMismatch | RefState::SecretUnavailable { .. }));
    }
}

#[test]
fn sess_unavailable_call() {
    let mut twin = opened(open(Some((CRED, 6)), None, false, Secret::Error("locked".into())));
    for operation in ["social", "logout", "login"] { twin.feed(call_of(operation, 1)); }
    assert!(twin.replies.iter().all(|(_, answer)| *answer == Err(Fail::Storage)));
}

#[test]
fn sess_unavailable_mutate() {
    let mut twin = opened(open(Some((CRED, 6)), None, false, Secret::Error("locked".into())));
    assert_eq!(twin.feed(mutate(7, 11)), vec![answer(7, 11, Outcome::Error(Kind::Temporary, Fail::Storage))]);
}

fn unavailable_reading() -> Twin {
    let mut twin = opened(open(Some((CRED, 6)), None, false, Secret::Error("locked".into())));
    assert!(matches!(&twin.feed(Input::Foreground)[..], [RefEffect::ReadSecret { .. }]));
    twin
}

#[test]
fn sess_unavailable_foreground() {
    let twin = unavailable_reading();
    assert!(matches!(twin.reference.state, RefState::SecretUnavailable { read: Some(_), .. }));
}

#[test]
fn sess_unavailable_read_present() {
    let mut twin = unavailable_reading();
    twin.feed(call(2));
    let op = twin.op();
    twin.feed(Input::SecretRead { op, read: Secret::Present(CRED) });
    assert!(matches!(twin.reference.state, RefState::Idle), "calls with the session work again");
    let mut queued = unavailable_reading();
    queued.feed(Input::SecretStored { op: 1, ok: true });
}

#[test]
fn sess_unavailable_read_mismatch() {
    let mut twin = unavailable_reading();
    let op = twin.op();
    twin.feed(Input::SecretRead { op, read: Secret::Present(9) });
    assert!(matches!(twin.reference.state, RefState::SecretMismatch));
}

#[test]
fn sess_unavailable_read_missing() {
    let mut twin = unavailable_reading();
    let op = twin.op();
    let effects = twin.feed(Input::SecretRead { op, read: Secret::Missing });
    assert_eq!(effects, vec![RefEffect::SessionReset], "the same cell as the open without a secret");
    assert!(twin.reference.credential.is_none());
}

#[test]
fn sess_unavailable_read_error() {
    let mut twin = unavailable_reading();
    let op = twin.op();
    assert!(twin.feed(Input::SecretRead { op, read: Secret::Error("still".into()) }).is_empty());
    assert!(matches!(twin.reference.state, RefState::SecretUnavailable { read: None, .. }), "the next Foreground reads again");
    assert!(matches!(&twin.feed(Input::Foreground)[..], [RefEffect::ReadSecret { .. }]));
}

#[test]
fn sess_secret_read_stray() {
    let mut twin = unavailable_reading();
    let op = twin.op();
    twin.feed(Input::SecretRead { op: op + 9, read: Secret::Present(CRED) });
    assert!(matches!(twin.reference.state, RefState::SecretUnavailable { read: Some(_), .. }));
    let mut calling = calling();
    let (rid, aid) = (calling.rid(), calling.aid());
    calling.feed_all([Input::SecretRead { op: 1, read: Secret::Missing }, Input::SecretMissing { request_id: rid + 9, attempt_id: aid, lost: true }, Input::SecretMissing { request_id: rid, attempt_id: aid + 9, lost: true }]);
    assert!(matches!(calling.reference.state, RefState::Calling { phase: CallPhase::InFlight(_), .. }));
}

#[test]
fn sess_held_epoch_ended() {
    let mut twin = held();
    twin.feed(call(2));
    twin.feed(ended(EndCause::ServerEnded));
    assert_eq!(twin.replies, vec![(2, Err(Fail::NoSession))]);
    assert!(twin.reference.credential.is_none());
}

#[test]
fn sess_held_closing() {
    let mut twin = held();
    twin.feed(call(2));
    assert!(twin.feed(Input::Closing).contains(&RefEffect::Drained));
    assert_eq!(twin.replies, vec![(2, Err(Fail::Closed))]);
}

#[test]
fn sess_held_queue() {
    let mut twin = held();
    assert!(twin.feed(call(2)).is_empty() && twin.feed(mutate(7, 12)).is_empty());
    assert_eq!(twin.reference.queue.len(), 2, "the slot belongs to the pair of the author");
}

#[test]
fn sess_held_cancel() {
    let mut twin = held();
    assert!(twin.feed(Input::Cancel { req_id: 11 }).is_empty());
    assert!(matches!(twin.reference.state, RefState::PendingHeld { .. }), "the pair is kept for the next Mutate");
}

#[test]
fn sess_held_release() {
    let mut twin = held();
    let rid = twin.rid();
    let effects = twin.feed(Input::Release { req_id: rid });
    assert!(matches!(twin.reference.state, RefState::Calling { .. }), "the pair goes on after the release");
    assert!(matches!(effects.as_slice(), [RefEffect::HttpRequest { pending, .. }] if pending.request_id == rid && pending.author.is_none()), "the same pair goes out with no author: {effects:?}");
}

#[test]
fn sess_release_other() {
    let mut twin = held();
    assert!(twin.feed(Input::Release { req_id: 99 }).is_empty());
    assert!(matches!(twin.reference.state, RefState::PendingHeld { .. }), "another request does not release the pair");
    let mut idle = Twin::new();
    assert!(idle.feed(Input::Release { req_id: 1 }).is_empty());
}

#[test]
fn sess_held_other() {
    let mut twin = held();
    let (rid, aid) = (twin.rid(), 1);
    twin.feed_all([Input::Online(false), Input::Online(true), Input::Tick, published(PubState::Session), committed(), rejected(Reason::Storage),
        Input::Response { request_id: rid, attempt_id: aid, body: "{}".into(), sequence: Some(1), refusal: None },
        Input::Revoked { request_id: rid, attempt_id: aid, body: "{}".into() },
        Input::HttpError { request_id: rid, attempt_id: aid, temporary: false, auth: true, persisted_missing: false },
        Input::Dropped { request_id: rid, attempt_id: aid }]);
    assert!(matches!(twin.reference.state, RefState::PendingHeld { .. }));
    assert_eq!(twin.reference.online, Some(true));
}

#[test]
fn sess_wait_call() {
    let mut twin = generating();
    let op = twin.op();
    twin.feed(Input::CredentialGenerated { op, credential: 42 });
    let op = twin.op();
    twin.feed_all([Input::SecretStored { op, ok: true }, committed()]);
    let input = twin.response(1);
    twin.feed_all([input, committed(), Input::Published { epoch: EPOCH + 2, identity: Some(42), state: PubState::Session }]);
    assert_eq!(twin.replies, vec![(1, Ok("{}".into()))]);
    for mut ending in [calling(), persisting()] {
        ending.feed(ended(EndCause::SessionReset));
        assert_eq!(ending.replies, vec![(1, Err(Fail::Interrupted))]);
    }
    let mut closed = persisting();
    closed.feed(Input::Closing);
    assert_eq!(closed.replies, vec![(1, Err(Fail::Closed))]);
}

fn next(seed: &mut u64) -> u64 {
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    *seed >> 33
}

/// A random trace; requests, attempts and batches are read from the reference state so the inputs are mostly the right ones.
fn trace(seed: u64, steps: usize, mut check: impl FnMut(&Twin, &Input, &[RefEffect])) -> Twin {
    let mut s = seed;
    let mut twin = Twin::bare();
    let mut reply = 0;
    for _ in 0..steps {
        let (request, sequence) = match &twin.reference.state {
            RefState::Persisting { pending, .. } | RefState::Calling { pending, .. } | RefState::Landing { pending, .. } | RefState::PendingHeld { pending } | RefState::Blocked { pending: Some(pending), .. } => (pending.request_id, pending.sequence),
            _ => (1, 1),
        };
        let attempt = match &twin.reference.state {
            RefState::Calling { phase: CallPhase::InFlight(attempt), .. } => *attempt,
            _ => 1,
        };
        let op = match &twin.reference.state {
            RefState::Generating { op, .. } | RefState::Storing { op, .. } | RefState::ClearingSecret { op, .. } => *op,
            RefState::SecretUnavailable { read: Some(op), .. } => *op,
            _ => 1,
        };
        let input = match next(&mut s) % 30 {
            0..=2 => { reply += 1; call(reply) }
            3 | 4 => committed(),
            5 => rejected(Reason::Storage),
            6 | 7 => Input::Response { request_id: request, attempt_id: attempt, body: "{}".into(), sequence: Some(if next(&mut s) % 6 == 0 { sequence + 1 } else { sequence }), refusal: None },
            8 => Input::Revoked { request_id: request, attempt_id: attempt, body: String::new() },
            9 => Input::HttpError { request_id: request, attempt_id: attempt, temporary: next(&mut s) % 2 == 0, auth: next(&mut s) % 2 == 0, persisted_missing: next(&mut s) % 5 == 0 },
            10 => Input::Tick,
            11 => Input::Online(next(&mut s) % 2 == 0),
            12 => Input::Published { epoch: EPOCH + next(&mut s) % 3, identity: None, state: [PubState::Session, PubState::Ending, PubState::EndingFailed, PubState::NoSession][(next(&mut s) % 4) as usize] },
            13 => Input::EpochEnded { epoch: EPOCH + next(&mut s) % 3, cause: [EndCause::SessionReset, EndCause::ServerEnded, EndCause::Other][(next(&mut s) % 3) as usize] },
            14 => Input::Response { request_id: request + 50, attempt_id: attempt, body: "{}".into(), sequence: Some(sequence), refusal: None },
            15 => if next(&mut s) % 6 == 0 { Input::Closing } else { committed() },
            16 | 17 => { reply += 1; mutate(100 + reply, 1_000 + reply % 3) }
            18 => Input::Dropped { request_id: request, attempt_id: attempt },
            19 => { reply += 1; call_of(["login", "visit", "logout"][(next(&mut s) % 3) as usize], reply) }
            20 => Input::CredentialGenerated { op, credential: 20 + next(&mut s) % 3 },
            21 => Input::SecretStored { op, ok: next(&mut s) % 4 != 0 },
            22 => Input::SecretCleared { op, ok: true },
            23 => Input::SecretMissing { request_id: request, attempt_id: attempt, lost: next(&mut s) % 2 == 0 },
            24 => Input::Foreground,
            25 => Input::Cancel { req_id: 1_000 + next(&mut s) % 3 },
            26 => Input::SecretRead { op, read: [Secret::Present(CRED), Secret::Missing, Secret::Error("x".into())][(next(&mut s) % 3) as usize].clone() },
            27 => Input::InterestPlus(next(&mut s) % 3),
            28 => Input::InterestMinus(next(&mut s) % 3),
            _ => Input::HttpError { request_id: request, attempt_id: attempt, temporary: false, auth: false, persisted_missing: true },
        };
        let effects = twin.feed(input.clone());
        check(&twin, &input, &effects);
    }
    twin
}

#[test]
fn sess_prop_sequence() {
    let mut twin = Twin::new();
    for reply in 1..=3 {
        twin.feed(call(reply));
        twin.feed(committed());
        let input = twin.response(reply);
        twin.feed_all([input, committed()]);
    }
    let sequences: Vec<u64> = twin.history.iter().filter_map(|e| match e { RefEffect::CommitPending { pending, .. } => Some(pending.sequence), _ => None }).collect();
    assert_eq!(sequences, vec![1, 2, 3], "the sequence grows by one without a gap");
    assert_eq!(twin.history.iter().filter(|e| matches!(e, RefEffect::HttpRequest { .. })).count(), 3, "one request in the network per call");
}

#[test]
fn sess_prop_credential_only_committed() {
    for seed in 1..=60 {
        let twin = trace(seed, 60, |_, _, _| {});
        let committed_identities: Vec<u64> = twin.history.iter().filter_map(|e| match e { RefEffect::CredentialCommitted(c) => Some(*c), _ => None }).collect();
        if let Some(credential) = twin.reference.credential {
            assert!(credential.digest == CRED || committed_identities.contains(&credential.digest) || twin.history.is_empty(), "seed {seed}: the credential {credential:?} was never committed");
        }
    }
}

#[test]
fn sess_prop_reply_once() {
    for seed in 1..=60 {
        let mut twin = trace(seed, 60, |_, _, _| {});
        twin.feed(Input::Closing);
        let mut seen = Vec::new();
        for (id, _) in &twin.replies {
            assert!(!seen.contains(id), "seed {seed}: call {id} is answered twice");
            seen.push(*id);
        }
    }
}

#[test]
fn sess_prop_idle() {
    let mut twin = Twin::new();
    twin.feed_all([Input::Tick, Input::Online(true), Input::Foreground]);
    assert!(twin.history.is_empty(), "no pending, no request, no Tick");
}

#[test]
fn sess_prop_unauthorized_keeps_session() {
    let mut twin = landing(1);
    twin.feed(committed());
    let before = twin.reference.credential.clone();
    twin.feed_all([call(2), committed()]);
    let input = twin.http_error(false, true);
    twin.feed_all([input, committed()]);
    assert_eq!(twin.reference.credential, before.map(|credential| credential), "the credential is the same");
    assert!(!twin.history.iter().any(|e| matches!(e, RefEffect::ServerEnded { .. } | RefEffect::SessionReset)));
}

#[test]
fn sess_prop_single_flight() {
    for seed in 1..=60 {
        trace(seed, 80, |twin, input, effects| {
            let requests: Vec<_> = effects.iter().filter(|effect| matches!(effect, RefEffect::HttpRequest { .. })).collect();
            assert!(requests.len() <= 1, "seed {seed}: {input:?} starts two requests");
            if let Some(RefEffect::HttpRequest { attempt_id, .. }) = requests.first() {
                assert!(matches!(&twin.reference.state, RefState::Calling { phase: CallPhase::InFlight(now), .. } if now == attempt_id), "seed {seed}: the new attempt is the one in flight");
            }
        });
    }
    let mut twin = calling();
    let (rid, aid) = (twin.rid(), twin.aid());
    twin.feed(Input::Online(false));
    twin.feed(Input::Online(true));
    twin.feed(Input::Response { request_id: rid, attempt_id: aid, body: "{}".into(), sequence: Some(1), refusal: None });
    assert!(matches!(twin.reference.state, RefState::Calling { phase: CallPhase::InFlight(now), .. } if now != aid), "a late input of a cancelled attempt changes nothing");
}

#[test]
fn sess_prop_mutate_stamp() {
    let mut twin = persisting();
    twin.feed(mutate(7, 11));
    twin.feed(Input::Published { epoch: EPOCH + 1, identity: None, state: PubState::Session });
    assert!(matches!(twin.reference.queue.front(), Some(RefQueued::Mutate { epoch, .. }) if *epoch == EPOCH), "Published does not rebind a queued input");
}

#[test]
fn sess_prop_durable_answer() {
    let outcomes: [(Input, Fail); 3] = [
        (Input::HttpError { request_id: 11, attempt_id: 0, temporary: false, auth: false, persisted_missing: false }, Fail::Http),
        (Input::HttpError { request_id: 11, attempt_id: 0, temporary: false, auth: true, persisted_missing: false }, Fail::Unauthorized),
        (Input::Revoked { request_id: 11, attempt_id: 0, body: "{}".into() }, Fail::Revoked),
    ];
    for (input, fail) in outcomes {
        let mut twin = calling_mutate();
        let aid = twin.aid();
        let input = match input {
            Input::HttpError { request_id, temporary, auth, persisted_missing, .. } => Input::HttpError { request_id, attempt_id: aid, temporary, auth, persisted_missing },
            Input::Revoked { request_id, body, .. } => Input::Revoked { request_id, attempt_id: aid, body },
            other => other,
        };
        let before = twin.feed(input);
        assert!(matches!(&before[..], [RefEffect::CommitLanding { answer: Some((11, Err(stored))), .. }] if *stored == fail), "{fail:?}: the outcome is stored with the removal: {before:?}");
        assert!(!before.iter().any(|effect| matches!(effect, RefEffect::Answer { .. })), "{fail:?}: the author hears it after the commit");
        assert!(twin.feed(committed()).contains(&answer(7, 11, permanent(fail))));
    }
}

#[test]
fn sess_prop_blocked_pending() {
    for seed in 1..=60 {
        trace(seed, 80, |twin, input, effects| {
            if matches!(twin.reference.state, RefState::Blocked { .. } | RefState::PendingHeld { .. }) {
                assert!(!effects.iter().any(|effect| matches!(effect, RefEffect::CommitPending { .. })), "seed {seed}: {input:?} writes a second pending beside a stored one");
            }
        });
    }
}

#[test]
fn sess_prop_retry_owner() {
    for seed in 1..=60 {
        trace(seed, 80, |twin, input, effects| {
            let author_pending = matches!(&twin.reference.state, RefState::Calling { pending, .. } | RefState::PendingHeld { pending } if pending.author.is_some());
            if author_pending {
                assert!(!effects.iter().any(|effect| matches!(effect, RefEffect::ScheduleTick { .. })), "seed {seed}: {input:?} schedules a retry for an author");
                if matches!(input, Input::Tick) { assert!(effects.is_empty(), "seed {seed}: a Tick repeats a request of an author"); }
            }
        });
    }
}

#[test]
fn sess_prop_held_slot() {
    let mut twin = held();
    twin.feed_all([call(2), mutate(7, 12), Input::Tick, Input::Online(false), Input::Online(true)]);
    assert!(!twin.history.iter().rev().take(1).any(|effect| matches!(effect, RefEffect::CommitPending { .. })));
    assert_eq!(twin.history.iter().filter(|effect| matches!(effect, RefEffect::CommitPending { .. })).count(), 1, "no other request is written while the pair of the author is stored");
    twin.feed(mutate(7, 11));
    let input = twin.response(1);
    twin.feed_all([input, committed()]);
    assert!(twin.history.iter().filter(|effect| matches!(effect, RefEffect::CommitPending { .. })).count() >= 2, "the slot goes on after the pair is resolved");
}

#[test]
fn sess_prop_secret_sqlite_consistent() {
    let mut twin = generating();
    let op = twin.op();
    twin.feed(Input::CredentialGenerated { op, credential: 42 });
    let op = twin.op();
    twin.feed(Input::SecretStored { op, ok: true });
    let kinds: Vec<&str> = twin.history.iter().filter_map(|effect| match effect {
        RefEffect::GenerateCredential { .. } => Some("generate"),
        RefEffect::StoreSecret { .. } => Some("store"),
        RefEffect::CommitPending { session: Some(_), .. } => Some("digest"),
        RefEffect::ClearSecret { .. } => Some("clear"),
        _ => None,
    }).collect();
    assert_eq!(kinds, ["generate", "store", "digest"], "the secret is written before the digest");
    let mut erased = clearing();
    let op = erased.op();
    assert!(erased.feed(call(5)).is_empty(), "no new secret is written before SecretCleared");
    erased.feed(Input::SecretCleared { op, ok: true });
    let kinds: Vec<&str> = erased.history.iter().filter_map(|effect| match effect {
        RefEffect::StoreSecret { .. } => Some("store"),
        RefEffect::ClearSecret { .. } => Some("clear"),
        RefEffect::GenerateCredential { .. } => Some("generate"),
        _ => None,
    }).collect();
    let clear_at = kinds.iter().position(|kind| *kind == "clear").expect("a clear");
    assert!(kinds[clear_at + 1..].iter().all(|kind| *kind != "store"), "{kinds:?}: SecretCleared is awaited before the next StoreSecret");
}
