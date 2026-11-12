//! Owner-level tests of the owner `Session` in the `lifecycle` machine: the owner on `DetScheduler`, read through its effects and replies.

mod vocab;
pub use vocab::{author_of, Answer, Class, Credential as RefCredential, Effect as RefEffect, EndCause, Fail, Input, Kind, Operation, Outcome, Pending as RefPending, PubState, Reason, Secret, OUTBOX_AUTHOR, TRANSFER_AUTHOR};
use crate::owners::contract::{Committed, EpochEnded, ErrorClass, Online, Published, PublishedState, RejectReason, Rejected};
use crate::owners::session::{self as owner, AuthorOutcome, Credential, OperationClass, PendingCall, SecretState, SessionCalls, SessionEffect, SessionError, SessionInput, SessionState};
use crate::spec::support::{new_log, take, Log, Recorder};
use crate::runtime::{Address, Cause, DetScheduler, EpochStamp};
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
        Reason::Invalid => RejectReason::Invalid("bad".into()),
        Reason::Closed => RejectReason::Closed,
    }
}

fn fail_of(error: &SessionError) -> Fail {
    match error {
        SessionError::Invalid => Fail::Invalid,
        SessionError::NotSent => Fail::NotSent,
        SessionError::Rejected(other) => panic!("no row of the Session table gives an author Rejected({other:?}): a refused write repeats, ends with the epoch or answers Invalid"),
        SessionError::Session { code: crate::transport::SessionCode::Unauthenticated, .. } => Fail::Unauthorized,
        SessionError::Session { code, .. } => Fail::Session(*code),
        SessionError::Graphql { code, .. } => Fail::Graphql(code.clone()),
        SessionError::Storage(_) => Fail::Storage,
        SessionError::SessionExists => Fail::SessionExists,
        SessionError::NoCredential => Fail::NoCredential,
        SessionError::Revoked { .. } => Fail::Revoked,
        SessionError::StateLost => Fail::StateLost,
        SessionError::SequenceConflict => Fail::SequenceConflict,
        SessionError::Unauthorized => Fail::Unauthorized,
        SessionError::ResponseTooLarge { limit } => Fail::ResponseTooLarge { limit: *limit },
        SessionError::Security { code, detail } => Fail::Security { code: *code, detail: detail.clone() },
        SessionError::Http { .. } => Fail::Http,
        SessionError::Network => Fail::Network,
        SessionError::TooLarge { size, limit } => Fail::TooLarge { size: *size, limit: *limit },
        SessionError::Interrupted => Fail::Interrupted,
        SessionError::NoSession => Fail::NoSession,
        SessionError::Closed => Fail::Closed,
    }
}

fn answer_of(answer: &owner::SessionAnswer) -> Result<String, Fail> {
    match answer {
        Err(error) => Err(fail_of(error)),
        Ok(body) => match serde_json::from_str(body).ok().and_then(|value| crate::transport::refusal(&value)) {
            Some(crate::transport::Refusal::Session(crate::transport::SessionCode::Unauthenticated)) => Err(Fail::Unauthorized),
            Some(crate::transport::Refusal::Session(code)) => Err(Fail::Session(code)),
            Some(crate::transport::Refusal::Graphql(name)) => Err(Fail::Graphql(name)),
            None => Ok(body.clone()),
        },
    }
}


fn cause(cause: EndCause) -> Cause {
    match cause {
        EndCause::SessionReset => Cause::SessionReset,
        EndCause::ServerEnded => Cause::ServerEnded,
        EndCause::Close => Cause::Close,
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
    RefPending { request_id: pending.request_id, sequence: pending.sequence, class: ref_class(pending.class), operation: pending.operation.clone(), path: pending.path.clone(), body: pending.body.clone(), document: pending.document.clone(), credential: pending.credential, purpose: pending.purpose, author: pending.author, owner: pending.owner.clone(), epoch: pending.epoch, reply: pending.reply }
}

fn owner_pending(pending: RefPending) -> PendingCall {
    PendingCall { request_id: pending.request_id, sequence: pending.sequence, class: class_of(pending.class), operation: pending.operation, path: pending.path, body: pending.body, document: pending.document, credential: pending.credential, purpose: pending.purpose, author: pending.author, owner: pending.owner, epoch: pending.epoch, reply: pending.reply }
}

fn outcome_of(outcome: &AuthorOutcome) -> Outcome {
    match outcome {
        AuthorOutcome::Response { body, pending_retained, .. } => Outcome::Response(body.clone(), *pending_retained),
        AuthorOutcome::Error { class, error, pending_retained } => Outcome::Error(if *class == ErrorClass::Permanent { Kind::Permanent } else { Kind::Temporary }, fail_of(error), *pending_retained),
        AuthorOutcome::Dropped => Outcome::Dropped,
    }
}

fn effect_of(effect: &SessionEffect) -> Option<RefEffect> {
    Some(match effect {
        SessionEffect::SealProbe { seal_id, pending, credential } => RefEffect::SealProbe { seal_id: *seal_id, pending: pending_of(pending), credential: *credential },
        SessionEffect::CommitPending { batch, epoch, pending, session } => RefEffect::CommitPending { batch: *batch, epoch: *epoch, pending: pending_of(pending), session: *session },
        SessionEffect::CommitLanding { batch, epoch, digest, sequence, answer, resume_answer, erase_session, end_journal } => RefEffect::CommitLanding { batch: *batch, epoch: *epoch, digest: *digest, sequence: *sequence, answer: answer.as_ref().map(|(id, answer)| (*id, answer.clone().map_err(|error| fail_of(&error)))), resume_answer: resume_answer.as_ref().map(|answer| answer.clone().map_err(|error| fail_of(&error))), erase_session: *erase_session, end_journal: *end_journal },
        SessionEffect::CommitRevoked { batch, epoch } => RefEffect::CommitLanding { batch: *batch, epoch: *epoch, digest: None, sequence: None, answer: None, resume_answer: None, erase_session: true, end_journal: true },
        SessionEffect::CommitResumeAnswer { .. } => return None,
        SessionEffect::HttpRequest { pending, attempt_id } => RefEffect::HttpRequest { pending: pending_of(pending), attempt_id: *attempt_id },
        SessionEffect::HttpCancel { attempt_id } => RefEffect::HttpCancel { attempt_id: *attempt_id },
        SessionEffect::ScheduleTick { .. } => RefEffect::ScheduleTick,
        SessionEffect::GenerateCredential { secret_op_id } => RefEffect::GenerateCredential { op: *secret_op_id },
        SessionEffect::StoreSecret { credential, secret_op_id } => RefEffect::StoreSecret { credential: *credential, op: *secret_op_id },
        SessionEffect::ClearSecret { secret_op_id } => RefEffect::ClearSecret { op: *secret_op_id },
        SessionEffect::ReadSecret { secret_op_id } => RefEffect::ReadSecret { op: *secret_op_id },
        SessionEffect::CredentialCommitted(committed) => RefEffect::CredentialCommitted(committed.identity),
        SessionEffect::ServerEnded(ended) => RefEffect::ServerEnded { epoch: ended.epoch },
        SessionEffect::SessionReset => RefEffect::SessionReset,
        SessionEffect::RestoreClassified { epoch } => RefEffect::RestoreClassified { epoch: *epoch },
        SessionEffect::SnapshotRequest { epoch } => RefEffect::SnapshotRequest { epoch: *epoch },
        SessionEffect::Answer { author, req_id, outcome } => RefEffect::Answer { author: *author, req_id: *req_id, outcome: outcome_of(outcome) },
        SessionEffect::Drained(_) => RefEffect::Drained,
        SessionEffect::Reply { .. } | SessionEffect::ResumeReply { .. } | SessionEffect::ExecutorFailed => return None,
    })
}

struct Twin {
    det: DetScheduler,
    addr: Address<SessionCalls>,
    log: Log<SessionEffect>,
    history: Vec<RefEffect>,
    replies: Vec<(u64, Result<String, Fail>)>,
    raw_text: String,
    seal: Option<u64>,
    pending: Option<RefPending>,
    attempt: Option<u64>,
    op: Option<u64>,
    batch: Option<u64>,
}

impl Twin {
    fn new() -> Self { Self::start(Some(true), Some(RefCredential { digest: CRED, sequence: 0 })) }

    fn offline() -> Self { Self::start(Some(false), Some(RefCredential { digest: CRED, sequence: 0 })) }
    fn bare() -> Self { Self::start(Some(true), None) }

    fn start(online: Option<bool>, credential: Option<RefCredential>) -> Self {
        let mut det = DetScheduler::new(2);
        let log = new_log();
        let addr = det.add::<SessionCalls>("session", SessionState::with_operations(owner_operations(), online), Recorder::<SessionCalls>::new(&log));
        let mut twin = Self { det, addr, log, history: Vec::new(), replies: Vec::new(), raw_text: String::new(), seal: None, pending: None, attempt: None, op: None, batch: None };
        twin.feed(Input::Published { epoch: EPOCH, identity: None, state: PubState::Session });
        if let Some(online) = online { twin.feed(Input::Snapshot { epoch: EPOCH, online }); }
        let secret = credential.as_ref().map_or(Secret::Missing, |credential| Secret::Present(credential.digest));
        twin.feed(Input::Open { credential, pending: None, dropped_pending: false, secret });
        twin.history.retain(|effect| !matches!(effect, RefEffect::SnapshotRequest { .. }));
        twin
    }

    fn feed(&mut self, input: Input) -> Vec<RefEffect> {
        if let Some(seal_id) = self.seal {
            if !matches!(input, Input::Sealed { .. } | Input::SealFailed { .. } | Input::AnswerTooLarge { .. } | Input::EpochEnded { cause: EndCause::Close, .. }) {
                self.feed(Input::Sealed { seal_id, size: 1 });
            }
        }
        let current = self.batch.unwrap_or(9_999);
        let resolve = |batch: u64| if batch == 0 { current } else { batch };
        match &input {
            Input::Sealed { .. } | Input::SealFailed { .. } => self.seal = None,
            Input::Committed(_) | Input::Rejected(..) => self.batch = None,
            Input::Open { pending: Some(pending), .. } | Input::Restored { pending: Some(pending), .. } => self.pending = Some(pending.clone()),
            _ => {}
        }
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
            Input::GateClosed { request_id, attempt_id } => SessionInput::GateClosed { request_id, attempt_id },
            Input::Foreground => SessionInput::Foreground { seq: 1 },
            Input::RestoreReady { epoch } => SessionInput::RestoreReady { epoch },
            Input::RestoreFailed { epoch, error } => SessionInput::RestoreFailed { epoch, error },
            Input::Restored { epoch, pending } => SessionInput::Restored { epoch, pending: pending.map(owner_pending), answers: Default::default(), pending_lost: false, resume_answer: None },
            Input::Response { request_id, attempt_id, body, sequence, refusal } => { let kind = kind_of(&self.history, request_id, &body, sequence); SessionInput::Response { request_id, attempt_id, body, sequence, refusal, kind } }
            Input::Revoked { request_id, attempt_id, body, author_ref } => SessionInput::Revoked { request_id, attempt_id, body, author_ref, epoch: EPOCH },
            Input::HttpError { request_id, attempt_id, temporary, auth, security, body } => SessionInput::HttpError { request_id, attempt_id, temporary, auth, status: 0, too_large: None, security, body },
            Input::AnswerTooLarge { request_id, attempt_id, limit } => SessionInput::AnswerTooLarge { request_id, attempt_id, limit },
            Input::Dropped { request_id, attempt_id } => SessionInput::Dropped { request_id, attempt_id },
            Input::Unknown { request_id, attempt_id } => SessionInput::Dropped { request_id, attempt_id },
            Input::Committed(batch) => SessionInput::Committed(Committed { batch: resolve(batch), revision: 1, tables: vec![], rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0, refused: Vec::new() }),
            Input::Rejected(batch, reason) => SessionInput::Rejected(Rejected { batch: resolve(batch), reason: reject(reason) }),
            Input::Tick => SessionInput::Tick,
            Input::Online(online) => SessionInput::Online(Online(online)),
            Input::Snapshot { epoch, online } => SessionInput::Snapshot { epoch, online },
            Input::Published { epoch, identity, state } => SessionInput::Published(Published { epoch, identity, generation: 0, state: published_state(state), closed: false }),
            Input::EpochEnded { epoch, cause: c } => SessionInput::EpochEnded(EpochEnded { epoch, cause: cause(c) }),
            Input::Closing => SessionInput::Closing,
        };
        let cleared = matches!(&owner_input, SessionInput::SecretCleared { .. });
        let traced = self.det.trace().len();
        self.det.post(self.addr, EpochStamp::Control, owner_input);
        self.det.run_until_quiet(100).expect("the owner is quiet");
        let scheduled = self.det.trace()[traced..].iter().filter(|entry| entry.effects.contains("Schedule {") || entry.effects.contains("Backoff {")).count();
        let raw = take(&self.log);
        self.raw_text = format!("{raw:?}");
        let mut effects: Vec<RefEffect> = raw.iter().filter_map(effect_of).collect();
        effects.extend(std::iter::repeat_n(RefEffect::ScheduleTick, scheduled));
        let replies: Vec<(u64, Result<String, Fail>)> = raw.iter().filter_map(|effect| match effect {
            SessionEffect::Reply { id, answer } => Some((*id, if cleared { answer_of(answer) } else { answer.clone().map_err(|error| fail_of(&error)) })),
            _ => None,
        }).collect();
        for effect in &effects {
            match effect {
                RefEffect::SealProbe { seal_id, pending, .. } => { self.seal = Some(*seal_id); self.pending = Some(pending.clone()); }
                RefEffect::CommitPending { batch, pending, .. } => { self.batch = Some(*batch); self.pending = Some(pending.clone()); }
                RefEffect::CommitLanding { batch, .. } => self.batch = Some(*batch),
                RefEffect::HttpRequest { pending, attempt_id } => { self.attempt = Some(*attempt_id); self.pending = Some(pending.clone()); }
                RefEffect::GenerateCredential { op } | RefEffect::StoreSecret { op, .. } | RefEffect::ClearSecret { op } | RefEffect::ReadSecret { op } => self.op = Some(*op),
                _ => {}
            }
        }
        self.replies.extend(replies);
        self.history.extend(effects.iter().cloned());
        effects
    }

    fn feed_all(&mut self, inputs: impl IntoIterator<Item = Input>) { inputs.into_iter().for_each(|input| { self.feed(input); }); }

    fn credential(&self) -> Option<RefCredential> { self.det.state(self.addr).expect("the owner state").credential().map(|c| RefCredential { digest: c.digest, sequence: c.sequence }) }

    fn pending(&self) -> RefPending { self.pending.clone().expect("the owner has sent or stored a request") }

    fn rid(&self) -> u64 { self.pending().request_id }

    fn aid(&self) -> u64 { self.attempt.expect("the owner has sent a request") }

    fn op(&self) -> u64 { self.op.expect("the owner asked for a secret operation") }

    fn response(&self, sequence: u64) -> Input { Input::Response { request_id: self.rid(), attempt_id: self.aid(), body: "{}".into(), sequence: Some(sequence), refusal: None } }

    fn http_error(&self, temporary: bool, auth: bool) -> Input { Input::HttpError { request_id: self.rid(), attempt_id: self.aid(), temporary, auth, security: None, body: None } }

    fn dropped(&self) -> Input { Input::Dropped { request_id: self.rid(), attempt_id: self.aid() } }
}

fn call(reply: u64) -> Input { Input::SessionCall { operation: "social".into(), body: "{}".into(), reply } }
fn call_of(operation: &str, reply: u64) -> Input { Input::SessionCall { operation: operation.into(), body: "{}".into(), reply } }
fn mutate(author: u64, req_id: u64) -> Input { Input::Mutate { author, owner: None, req_id, epoch: EPOCH, path: OTP.into(), body: "{}".into(), purpose: 0 } }
fn committed() -> Input { Input::Committed(0) }
fn rejected(reason: Reason) -> Input { Input::Rejected(0, reason) }
fn published(state: PubState) -> Input { Input::Published { epoch: EPOCH, identity: None, state } }
fn ended(cause: EndCause) -> Input { Input::EpochEnded { epoch: EPOCH, cause } }
fn answer(author: u64, req_id: u64, outcome: Outcome) -> RefEffect { RefEffect::Answer { author, req_id, outcome } }
fn permanent(fail: Fail) -> Outcome { Outcome::Error(Kind::Permanent, fail, false) }
fn retained(fail: Fail) -> Outcome { Outcome::Error(Kind::Permanent, fail, true) }

fn persisting() -> Twin {
    let mut twin = Twin::new();
    twin.feed(call(1));
    if let Some(seal_id) = twin.seal { twin.feed(Input::Sealed { seal_id, size: 1 }); }
    twin
}

fn finish_seal(twin: &mut Twin) {
    if let Some(seal_id) = twin.seal { twin.feed(Input::Sealed { seal_id, size: 1 }); }
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
fn blocked_invalid() -> Twin {
    let mut twin = calling_mutate();
    let input = twin.response(1);
    twin.feed(input);
    twin.feed(rejected(Reason::Invalid));
    twin
}

fn stored(owner: Option<crate::transport::RequestOwner>, request_id: u64, sequence: u64) -> RefPending {
    let author = owner.as_ref().and_then(author_of);
    RefPending { request_id, sequence, class: Class::Authenticated, operation: String::new(), path: OTP.into(), document: None, body: "{}".into(), credential: None, purpose: 0, author, owner, epoch: 0, reply: None }
}

fn open(credential: Option<(u64, u64)>, pending: Option<RefPending>, dropped_pending: bool, secret: Secret) -> Input {
    Input::Open { credential: credential.map(|(digest, sequence)| RefCredential { digest, sequence }), pending, dropped_pending, secret }
}

fn opened(input: Input) -> Twin {
    let mut twin = Twin::start(Some(true), None);
    twin.feed(input);
    twin
}

mod session_owner_calls_tests;
mod session_owner_http_tests;
mod session_owner_restore_tests;
use session_owner_restore_tests::*;
mod session_owner_props_tests;
mod session_owner_early_release_tests;
mod session_owner_release_tests;
mod session_owner_write_failure_tests;
mod session_owner_detached_tests;

fn kind_of(history: &[RefEffect], request_id: u64, body: &str, sequence: Option<u64>) -> crate::transport::AnswerKind {
    let pending = history.iter().rev().find_map(|effect| match effect { RefEffect::HttpRequest { pending, .. } if pending.request_id == request_id => Some(pending), _ => None });
    let (sequenced, has_document) = pending.map_or((sequence.is_some(), false), |pending| (pending.class != Class::Sessionless, pending.document.is_none()));
    let body: serde_json::Value = serde_json::from_str(body).unwrap_or(serde_json::Value::Null);
    crate::transport::classify_answer(&crate::transport::WireOutcome::Answer { sequence: sequence.and_then(|value| i64::try_from(value).ok()), body }, sequenced, has_document)
}
