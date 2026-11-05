//! Owner-level tests of the owner `Session` in the `lifecycle` machine: the owner on `DetScheduler` against `ref_session.rs`. Names are the `Test` column.

use super::ref_session::{Credential as RefCredential, Effect as RefEffect, EndCause, Fail, Input, Operation, Outcome, Pending as RefPending, Queued as RefQueued, Reason, Session as Ref, State as RefState};
use crate::owners::contract::{Committed, EpochEnded, Online, Published, RejectReason, Rejected};
use crate::owners::session::{AuthorOutcome, Credential, SessionOperation, PendingCall, Queued, SessionCalls, SessionEffect, SessionError, SessionInput, SessionPhase, SessionState};
use crate::owners::support::{new_log, take, Log, Recorder};
use crate::runtime::{Address, Cause, DetScheduler, EpochStamp, Time};

const EPOCH: u64 = 3;
/// `social`, `logout` and `login` carry the session (a call at `credential = None` is the sign-in), `visit` carries no session.
fn operations() -> Vec<Operation> {
    let def = |name: &str, sessionless| Operation { name: name.into(), path: format!("/{name}"), sessionless };
    vec![def("social", false), def("logout", false), def("login", false), def("visit", true)]
}

fn owner_operations() -> Vec<SessionOperation> {
    operations().into_iter().map(|def| SessionOperation { name: def.name, path: def.path, sessionless: def.sessionless }).collect()
}

fn reject(reason: Reason) -> RejectReason {
    match reason {
        Reason::NoSession => RejectReason::NoSession,
        Reason::Storage => RejectReason::Storage("disk".into()),
        Reason::Closed => RejectReason::Closed,
    }
}

fn reason_of(reason: &RejectReason) -> Reason {
    match reason {
        RejectReason::NoSession => Reason::NoSession,
        RejectReason::Storage(_) => Reason::Storage,
        RejectReason::Closed => Reason::Closed,
        RejectReason::StaleGeneration { .. } => panic!("the Session table has no generation"),
        RejectReason::Invalid => panic!("the Session batch carries no rows"),
    }
}

fn fail_of(error: &SessionError) -> Fail {
    match error {
        SessionError::Invalid => Fail::Invalid,
        SessionError::Rejected(reason) => Fail::Reason(reason_of(reason)),
        SessionError::Revoked => Fail::Revoked,
        SessionError::StateLost => Fail::StateLost,
        SessionError::NoCredential => panic!("the reference has no NoCredential, the cell is tested without it"),
        SessionError::SequenceConflict => Fail::SequenceConflict,
        SessionError::Protocol => Fail::Protocol,
        SessionError::Unauthorized => Fail::Unauthorized,
        SessionError::Http => Fail::Http,
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

fn pending_of(pending: &PendingCall) -> RefPending {
    RefPending {
        request_id: pending.request_id,
        sequence: pending.sequence,
        operation: pending.operation.clone(),
        path: pending.path.clone(),
        body: pending.body.clone(),
        sessionless: pending.sessionless,
        credential: pending.credential,
        purpose: pending.purpose,
        author: pending.author,
        owner: pending.owner.clone(),
        reply: pending.reply,
    }
}

fn owner_pending(pending: RefPending) -> PendingCall {
    PendingCall { request_id: pending.request_id, sequence: pending.sequence, operation: pending.operation, path: pending.path, body: pending.body, sessionless: pending.sessionless, credential: pending.credential, purpose: pending.purpose, author: pending.author, owner: pending.owner, reply: pending.reply }
}

fn queued_of(queued: &Queued) -> RefQueued {
    match queued {
        Queued::Call { operation, body, reply } => RefQueued::Call { operation: operation.clone(), body: body.clone(), reply: *reply },
        Queued::Mutate { author, owner, req_id, path, body, purpose } => RefQueued::Mutate { author: *author, owner: owner.clone(), req_id: *req_id, path: path.clone(), body: body.clone(), purpose: *purpose },
    }
}

fn outcome_of(outcome: &AuthorOutcome) -> Outcome {
    match outcome {
        AuthorOutcome::Response(body) => Outcome::Response(body.clone()),
        AuthorOutcome::Error(error) => Outcome::Error(fail_of(error)),
    }
}

fn phase_of(phase: &SessionPhase) -> RefState {
    match phase {
        SessionPhase::Idle => RefState::Idle,
        SessionPhase::Generating { call } => RefState::Generating { call: queued_of(call) },
        SessionPhase::Persisting { pending, batch } => RefState::Persisting { pending: pending_of(pending), batch: *batch },
        SessionPhase::Calling { pending, attempts, sent } => RefState::Calling { pending: pending_of(pending), attempts: *attempts, sent: *sent },
        SessionPhase::Landing { pending, batch, answer, identity, ended } => RefState::Landing { pending: pending_of(pending), batch: *batch, answer: answer.clone().map_err(|error| fail_of(&error)), identity: *identity, ended: *ended },
        SessionPhase::Blocked { pending, attempts, retry } => RefState::Blocked { pending: pending_of(pending), attempts: *attempts, retry: *retry },
        SessionPhase::Closed => RefState::Closed,
    }
}

fn effect_of(effect: &SessionEffect) -> Option<RefEffect> {
    Some(match effect {
        SessionEffect::CommitPending { batch, epoch, pending } => RefEffect::CommitPending { batch: *batch, epoch: *epoch, pending: pending_of(pending) },
        SessionEffect::CommitLanding { batch, epoch, digest, sequence } => RefEffect::CommitLanding { batch: *batch, epoch: *epoch, digest: *digest, sequence: *sequence },
        SessionEffect::RemovePending { batch, epoch } => RefEffect::RemovePending { batch: *batch, epoch: *epoch },
        SessionEffect::HttpRequest { pending } => RefEffect::HttpRequest { pending: pending_of(pending) },
        SessionEffect::GenerateCredential => RefEffect::GenerateCredential,
        SessionEffect::Answer { author, req_id, outcome } => RefEffect::Answer { author: *author, req_id: *req_id, outcome: outcome_of(outcome) },
        SessionEffect::HttpCancel { request_id } => RefEffect::HttpCancel { request_id: *request_id },
        SessionEffect::ScheduleTick { attempts } => RefEffect::ScheduleTick { attempts: *attempts },
        SessionEffect::CredentialCommitted(committed) => RefEffect::CredentialCommitted(committed.identity),
        SessionEffect::ServerEnded(ended) => RefEffect::ServerEnded { epoch: ended.epoch },
        SessionEffect::Drained(_) => RefEffect::Drained,
        SessionEffect::Reply { .. } => return None,
    })
}

struct Twin {
    det: DetScheduler,
    addr: Address<SessionCalls>,
    log: Log<SessionEffect>,
    reference: Ref,
    history: Vec<SessionEffect>,
    replies: Vec<(u64, Result<String, Fail>)>,
}

impl Twin {
    /// Idle, online, `Published.epoch` = `EPOCH`.
    fn new() -> Self { Self::with(true) }

    fn with(online: bool) -> Self { Self::start(online, Some(RefCredential { digest: 4, sequence: 0 })) }

    /// No credential: a call that carries the session signs in.
    fn bare() -> Self { Self::start(true, None) }

    fn start(online: bool, credential: Option<RefCredential>) -> Self {
        let mut det = DetScheduler::new(2);
        let log = new_log();
        let addr = det.add::<SessionCalls>("session", SessionState::with_operations(owner_operations(), "/logout".into(), online), Recorder::<SessionCalls>::new(&log));
        let mut twin = Self { det, addr, log, reference: Ref::new(operations(), online), history: Vec::new(), replies: Vec::new() };
        twin.feed(Input::Published { epoch: EPOCH });
        twin.feed(Input::Open { credential, pending: None });
        twin
    }

    fn feed(&mut self, input: Input) -> Vec<RefEffect> {
        let batch_of = |state: &RefState| match state {
            RefState::Persisting { batch, .. } | RefState::Landing { batch, .. } => *batch,
            _ => 9_999,
        };
        let current = batch_of(&self.reference.state);
        let owner_input = match input.clone() {
            Input::Open { credential, pending } => SessionInput::Open { credential: credential.map(|c| Credential { digest: c.digest, sequence: c.sequence }), pending: pending.map(owner_pending) },
            Input::SessionCall { operation, body, reply } => SessionInput::SessionCall { operation, body, reply },
            Input::Mutate { author, owner, req_id, path, body, purpose } => SessionInput::Mutate { author, owner, req_id, path, body, purpose },
            Input::CredentialGenerated(credential) => SessionInput::CredentialGenerated { credential },
            Input::Dropped { request_id } => SessionInput::Dropped { request_id },
            Input::Cancel { req_id } => SessionInput::Cancel { req_id },
            Input::Response { request_id, body, sequence, credential } => SessionInput::Response { request_id, body, sequence, credential },
            Input::Revoked { request_id, body } => SessionInput::Revoked { request_id, body },
            Input::HttpError { request_id, temporary, auth } => SessionInput::HttpError { request_id, temporary, auth },
            Input::Committed(batch) => SessionInput::Committed(Committed { batch: if batch == 0 { current } else { batch }, revision: 1, tables: vec![], models: Default::default() }),
            Input::Rejected(batch, reason) => SessionInput::Rejected(Rejected { batch: if batch == 0 { current } else { batch }, reason: reject(reason) }),
            Input::Tick => SessionInput::Tick(Time(0)),
            Input::Online(online) => SessionInput::Online(Online(online)),
            Input::Published { epoch } => SessionInput::Published(Published { epoch, identity: None, generation: 0, state: crate::owners::contract::PublishedState::Session, closed: false }),
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
        self.history.extend(raw.iter().cloned());
        let want = self.reference.step(reference_input);
        let mut effects: Vec<RefEffect> = raw.iter().filter_map(effect_of).collect();
        if scheduled > 0 {
            let Some(SessionPhase::Calling { attempts, .. } | SessionPhase::Blocked { attempts, .. }) = self.det.state(self.addr).map(SessionState::phase) else { panic!("a retry timer is scheduled in Calling or Blocked only") };
            effects.extend(std::iter::repeat_n(RefEffect::ScheduleTick { attempts: *attempts }, scheduled));
        }
        let replies: Vec<(u64, Result<String, Fail>)> = raw.iter().filter_map(|effect| match effect {
            SessionEffect::Reply { id, answer } => Some((*id, answer.clone().map_err(|error| fail_of(&error)))),
            _ => None,
        }).collect();
        assert_eq!(effects, want.effects, "the same effects");
        assert_eq!(replies, want.replies, "the same replies");
        self.replies.extend(replies);
        let state = self.det.state(self.addr).expect("the owner state");
        assert_eq!(phase_of(state.phase()), self.reference.state, "the same state");
        assert_eq!(state.credential().map(|c| RefCredential { digest: c.digest, sequence: c.sequence }), self.reference.credential, "the same credential");
        assert_eq!(state.queued(), self.reference.queue.len(), "the same queue");
        want.effects
    }

    fn feed_all(&mut self, inputs: impl IntoIterator<Item = Input>) { inputs.into_iter().for_each(|input| { self.feed(input); }); }

    fn request_id(&self) -> u64 {
        match &self.reference.state {
            RefState::Persisting { pending, .. } | RefState::Calling { pending, .. } | RefState::Landing { pending, .. } => pending.request_id,
            other => panic!("no request in {other:?}"),
        }
    }
}

fn call(reply: u64) -> Input { Input::SessionCall { operation: "social".into(), body: "{}".into(), reply } }
fn call_of(operation: &str, reply: u64) -> Input { Input::SessionCall { operation: operation.into(), body: "{}".into(), reply } }
fn mutate(author: u64, req_id: u64) -> Input { Input::Mutate { author, owner: None, req_id, path: "/graphql".into(), body: "{}".into(), purpose: 0 } }
fn committed() -> Input { Input::Committed(0) }
fn response(request_id: u64, sequence: u64, credential: Option<u64>) -> Input { Input::Response { request_id, body: "{}".into(), sequence: Some(sequence), credential } }
fn error(request_id: u64, temporary: bool, auth: bool) -> Input { Input::HttpError { request_id, temporary, auth } }

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

fn landing(credential: Option<u64>) -> Twin {
    let mut twin = calling();
    let id = twin.request_id();
    twin.feed(response(id, 1, credential));
    twin
}

#[test]
fn sess_idle_call() { persisting(); }

#[test]
fn sess_idle_unknown_call() { Twin::new().feed(call_of("nope", 1)); }

#[test]
fn sess_idle_stray() {
    let mut twin = Twin::new();
    twin.feed_all([response(1, 1, None), error(1, true, false), committed(), Input::Rejected(0, Reason::Storage), Input::Tick]);
}

#[test]
fn sess_persisting_committed() {
    let mut twin = persisting();
    assert_eq!(twin.feed(committed()).len(), 1, "one Http request");
}

#[test]
fn sess_persisting_committed_offline() {
    let mut twin = Twin::with(false);
    twin.feed(call(1));
    assert!(twin.feed(committed()).is_empty());
}

#[test]
fn sess_persisting_rejected() {
    let mut twin = persisting();
    twin.feed(call(2));
    twin.feed(Input::Rejected(0, Reason::Storage));
}

#[test]
fn sess_call_queued() {
    let mut twin = persisting();
    twin.feed(call(2));
    twin.feed(committed());
    twin.feed(call(3));
    let id = twin.request_id();
    twin.feed(response(id, 1, None));
    twin.feed(call(4));
    assert!(twin.reference.queue.len() >= 2);
}

#[test]
fn sess_calling_response() { landing(None); }

#[test]
fn sess_calling_revoked() {
    let mut twin = calling();
    twin.feed(call(2));
    let id = twin.request_id();
    twin.feed(Input::Revoked { request_id: id, body: String::new() });
}

#[test]
fn sess_calling_sequence_conflict() {
    let mut twin = calling();
    let id = twin.request_id();
    twin.feed(response(id, 9, None));
    twin.feed(Input::Response { request_id: 99, body: String::new(), sequence: None, credential: None });
}

#[test]
fn sess_calling_stray() {
    let mut twin = calling();
    twin.feed_all([response(99, 1, None), error(99, false, true)]);
}

#[test]
fn sess_calling_temporary() {
    let mut twin = calling();
    let id = twin.request_id();
    twin.feed_all([error(id, true, false), error(id, true, true)]);
}

#[test]
fn sess_calling_temporary_offline() {
    let mut twin = calling();
    let id = twin.request_id();
    twin.feed_all([Input::Online(false), error(id, true, false)]);
}

#[test]
fn sess_calling_unauthorized() {
    let mut twin = calling();
    let id = twin.request_id();
    twin.feed(error(id, false, true));
    assert!(!twin.history.iter().any(|e| matches!(e, SessionEffect::ServerEnded(_) | SessionEffect::CredentialCommitted(_))), "401 gives Lifecycle no input");
}

#[test]
fn sess_calling_permanent() {
    let mut twin = calling();
    let id = twin.request_id();
    twin.feed(error(id, false, false));
}

#[test]
fn sess_calling_tick() {
    let mut twin = calling();
    let id = twin.request_id();
    twin.feed(error(id, true, false));
    let effects = twin.feed(Input::Tick);
    assert!(matches!(&effects[..], [RefEffect::HttpRequest { .. }]), "the timer starts the repeat: {effects:?}");
}

#[test]
fn sess_calling_tick_offline() {
    let mut twin = calling();
    let id = twin.request_id();
    twin.feed(error(id, true, false));
    twin.feed_all([Input::Online(false), Input::Tick]);
}

#[test]
fn sess_calling_online() {
    let mut twin = Twin::with(false);
    twin.feed_all([call(1), committed(), Input::Online(true)]);
    let before = twin.history.len();
    twin.feed(Input::Online(true));
    assert_eq!(twin.history.len(), before, "a request in the network is not sent twice");
}

#[test]
fn sess_landing_committed_credential() {
    let mut twin = landing(Some(77));
    assert_eq!(twin.feed(committed()), vec![RefEffect::CredentialCommitted(77)]);
}

#[test]
fn sess_landing_committed() { landing(None).feed(committed()); }

#[test]
fn sess_landing_rejected() { landing(Some(5)).feed(Input::Rejected(0, Reason::Closed)); }

#[test]
fn sess_online() {
    for mut twin in [Twin::new(), persisting(), calling(), landing(None)] {
        twin.feed_all([Input::Online(false), Input::Online(true)]);
    }
}

#[test]
fn sess_published() {
    for mut twin in [Twin::new(), persisting(), calling(), landing(None)] {
        twin.feed(Input::Published { epoch: 8 });
    }
}

#[test]
fn sess_epoch_ended_cleared() {
    for cause in [EndCause::SessionReset, EndCause::ServerEnded] {
        let mut twin = calling();
        twin.feed(call(2));
        twin.feed(Input::EpochEnded { epoch: EPOCH, cause });
        assert_eq!(twin.reference.credential, None);
    }
    let mut twin = persisting();
    twin.feed(Input::EpochEnded { epoch: EPOCH, cause: EndCause::SessionReset });
    let mut twin = landing(Some(5));
    twin.feed(Input::EpochEnded { epoch: EPOCH, cause: EndCause::SessionReset });
}

#[test]
fn sess_epoch_ended_other() {
    for mut twin in [persisting(), calling(), landing(None)] {
        twin.feed(Input::EpochEnded { epoch: EPOCH, cause: EndCause::Other });
        twin.feed(Input::EpochEnded { epoch: EPOCH + 5, cause: EndCause::SessionReset });
    }
}

#[test]
fn sess_idle_epoch_ended() {
    let mut twin = landing(Some(5));
    twin.feed(committed());
    assert!(twin.reference.credential.is_some());
    twin.feed(Input::EpochEnded { epoch: EPOCH, cause: EndCause::ServerEnded });
    assert!(twin.reference.credential.is_none());
}

#[test]
fn sess_idle_epoch_ended_other() {
    let mut twin = landing(Some(5));
    twin.feed(committed());
    twin.feed(Input::EpochEnded { epoch: EPOCH, cause: EndCause::Other });
    assert!(twin.reference.credential.is_some());
}

#[test]
fn sess_closing() {
    for mut twin in [Twin::new(), persisting(), calling(), landing(None)] {
        twin.feed(call(7));
        twin.feed(Input::Closing);
    }
}

#[test]
fn sess_closed() {
    let mut twin = Twin::new();
    twin.feed_all([Input::Closing, call(1), Input::Tick, Input::Online(true), Input::Closing]);
}

#[test]
fn sess_open_restored() {
    let mut twin = Twin::new();
    twin.feed(Input::Open { credential: Some(RefCredential { digest: 4, sequence: 6 }), pending: None });
    twin.feed(call(1));
}

#[test]
fn sess_open_pending() {
    let mut twin = Twin::new();
    let effects = twin.feed(Input::Open { credential: Some(RefCredential { digest: 4, sequence: 6 }), pending: Some(RefPending { request_id: 5, sequence: 7, operation: "social".into(), path: "/social".into(), body: "{}".into(), sessionless: false, credential: None, purpose: 0, author: None, owner: None, reply: Some(99) }) });
    assert_eq!(effects.len(), 1, "one Http request of the restored pending");
    assert!(matches!(&effects[0], RefEffect::HttpRequest { pending } if pending.request_id == 5 && pending.reply.is_none()));
    twin.feed(response(5, 7, None));
    twin.feed(committed());
    assert!(twin.replies.is_empty(), "an answer without a caller shows nothing");
}

#[test]
fn sess_open_pending_owner() {
    let mut twin = Twin::bare();
    let owner = crate::transport::RequestOwner::Operation { seq: 3 };
    let pending = RefPending { request_id: 5, sequence: 7, operation: String::new(), path: "/graphql".into(), body: "{}".into(), sessionless: false, credential: None, purpose: 0, author: None, owner: Some(owner), reply: None };
    let effects = twin.feed(Input::Open { credential: Some(RefCredential { digest: 4, sequence: 6 }), pending: Some(pending) });
    assert!(matches!(&effects[..], [RefEffect::HttpRequest { pending }] if pending.author == Some(super::ref_session::OUTBOX_AUTHOR)), "a restored pending with an owner goes to the author the owner names");
    twin.feed(response(5, 7, None));
    let answered = twin.feed(committed());
    assert!(matches!(&answered[..], [RefEffect::Answer { author, req_id: 5, outcome: Outcome::Response(_) }] if *author == super::ref_session::OUTBOX_AUTHOR));
    let transfer = RefPending { request_id: 6, sequence: 8, operation: String::new(), path: "/graphql".into(), body: "{}".into(), sessionless: false, credential: None, purpose: 1, author: None, owner: Some(crate::transport::RequestOwner::Transfer { operation_id: 1, item_id: "a".into(), step: crate::upload::UploadStage::Prepare }), reply: None };
    let mut twin = Twin::bare();
    let effects = twin.feed(Input::Open { credential: Some(RefCredential { digest: 4, sequence: 7 }), pending: Some(transfer) });
    assert!(matches!(&effects[..], [RefEffect::HttpRequest { pending }] if pending.author == Some(super::ref_session::TRANSFER_AUTHOR)));
}

#[test]
fn sess_open_empty() {
    let mut twin = Twin::new();
    twin.feed(Input::Open { credential: None, pending: None });
}

#[test]
fn sess_wait_call() {
    let mut twin = landing(None);
    twin.feed(committed());
    twin.feed(call(2));
    twin.feed(Input::EpochEnded { epoch: EPOCH, cause: EndCause::SessionReset });
    let mut closed = persisting();
    closed.feed(Input::Closing);
    assert!(closed.replies.iter().all(|(_, answer)| *answer == Err(Fail::Closed)));
}

fn signed_in() -> Twin {
    let mut twin = Twin::new();
    twin.feed(Input::Open { credential: Some(RefCredential { digest: 4, sequence: 6 }), pending: None });
    twin
}

fn generating() -> Twin {
    let mut twin = Twin::bare();
    assert_eq!(twin.feed(call_of("login", 1)), vec![RefEffect::GenerateCredential]);
    twin
}

#[test]
fn sess_idle_login_generate() {
    let twin = generating();
    assert!(matches!(twin.reference.state, RefState::Generating { .. }));
}

#[test]
fn sess_generated_persist() {
    let mut twin = generating();
    let effects = twin.feed(Input::CredentialGenerated(42));
    assert!(matches!(&effects[..], [RefEffect::CommitPending { pending, .. }] if pending.sequence == 1 && pending.credential == Some(42) && !pending.sessionless && pending.author.is_none()));
}

#[test]
fn sess_generating_queue() {
    let mut twin = generating();
    twin.feed_all([call(2), mutate(7, 1)]);
    assert_eq!(twin.reference.queue.len(), 2);
}

#[test]
fn sess_generating_closing_and_epoch_ended() {
    let mut twin = generating();
    twin.feed_all([call(2), mutate(7, 1), Input::Closing]);
    assert_eq!(twin.replies, vec![(1, Err(Fail::Closed)), (2, Err(Fail::Closed))]);
    let mut twin = generating();
    twin.feed_all([call(2), Input::EpochEnded { epoch: EPOCH, cause: EndCause::SessionReset }]);
    assert_eq!(twin.replies, vec![(1, Err(Fail::Interrupted)), (2, Err(Fail::NoSession))]);
    assert!(matches!(twin.reference.state, RefState::Idle));
}

#[test]
fn sess_login_lands_credential() {
    let mut twin = generating();
    twin.feed_all([Input::CredentialGenerated(42), committed()]);
    let id = twin.request_id();
    twin.feed(response(id, 1, Some(9)));
    assert_eq!(twin.feed(committed()), vec![RefEffect::CredentialCommitted(42)], "the identity is the digest of the generated credential, not the answer field");
    assert_eq!(twin.reference.credential, Some(RefCredential { digest: 42, sequence: 1 }));
}

#[test]
fn sess_idle_mutate() {
    let mut twin = signed_in();
    let effects = twin.feed(mutate(7, 11));
    assert!(matches!(&effects[..], [RefEffect::CommitPending { pending, .. }] if pending.request_id == 11 && pending.sequence == 7 && pending.author == Some(7) && pending.reply.is_none()));
}

#[test]
fn sess_idle_mutate_no_session() {
    let mut twin = Twin::bare();
    let effects = twin.feed(mutate(7, 11));
    assert_eq!(effects, vec![RefEffect::Answer { author: 7, req_id: 11, outcome: Outcome::Error(Fail::NoSession) }]);
}

#[test]
fn sess_mutate_queued() {
    let mut twin = signed_in();
    twin.feed_all([call(1), mutate(7, 11)]);
    assert_eq!(twin.reference.queue.len(), 1);
    twin.feed(committed());
    let id = twin.request_id();
    twin.feed(response(id, 7, None));
    twin.feed(committed());
    assert!(matches!(twin.reference.state, RefState::Persisting { .. }), "the queued Mutate follows the call");
}

#[test]
fn sess_calling_dropped() {
    let mut twin = signed_in();
    twin.feed_all([call(1), committed()]);
    let id = twin.request_id();
    assert_eq!(twin.feed(Input::Dropped { request_id: id }), vec![RefEffect::ScheduleTick { attempts: 1 }]);
    twin.feed(Input::Tick);
    let mut offline = Twin::with(false);
    offline.feed_all([call(1), committed()]);
    let id = offline.request_id();
    assert!(offline.feed(Input::Dropped { request_id: id }).is_empty());
    twin.feed(Input::Dropped { request_id: 99 });
}

#[test]
fn sess_landing_mutate() {
    let mut twin = signed_in();
    twin.feed_all([mutate(7, 11), committed()]);
    twin.feed(response(11, 7, None));
    let effects = twin.feed(committed());
    assert_eq!(effects, vec![RefEffect::Answer { author: 7, req_id: 11, outcome: Outcome::Response("{}".into()) }]);
    assert_eq!(twin.reference.credential, Some(RefCredential { digest: 4, sequence: 7 }));
}

#[test]
fn sess_mutate_error_reaches_author() {
    let mut twin = signed_in();
    twin.feed_all([mutate(7, 11), committed()]);
    let before = twin.feed(error(11, false, false));
    assert!(!before.iter().any(|effect| matches!(effect, RefEffect::Answer { .. })), "the author hears the error after the commit");
    let effects = twin.feed(committed());
    assert!(effects.contains(&RefEffect::Answer { author: 7, req_id: 11, outcome: Outcome::Error(Fail::Http) }));
    let mut twin = signed_in();
    twin.feed_all([mutate(7, 11), Input::Closing]);
}

#[test]
fn sess_calling_no_sequence() {
    let mut twin = calling();
    let id = twin.request_id();
    twin.feed(Input::Response { request_id: id, body: "{}".into(), sequence: None, credential: None });
    assert_eq!(twin.replies, vec![(1, Err(Fail::Protocol))]);
}

#[test]
fn sess_calling_offline_cancel() {
    let mut twin = calling();
    let id = twin.request_id();
    assert_eq!(twin.feed(Input::Online(false)), vec![RefEffect::HttpCancel { request_id: id }]);
    assert!(matches!(twin.feed(Input::Online(true))[..], [RefEffect::HttpRequest { .. }]), "the same request goes again");
}

#[test]
fn sess_calling_offline_idle() {
    let mut twin = Twin::with(false);
    twin.feed_all([call(1), committed()]);
    assert!(twin.feed(Input::Online(false)).is_empty());
}

#[test]
fn sess_landing_committed_sessionless() {
    let mut twin = signed_in();
    twin.feed_all([call_of("visit", 1), committed()]);
    let id = twin.request_id();
    twin.feed(Input::Response { request_id: id, body: "{}".into(), sequence: None, credential: None });
    twin.feed(committed());
    assert_eq!(twin.reference.credential, Some(RefCredential { digest: 4, sequence: 6 }), "a call without a session leaves the credential");
    assert_eq!(twin.replies, vec![(1, Ok("{}".into()))]);
}

fn next(seed: &mut u64) -> u64 {
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    *seed >> 33
}

/// A random trace; requests and batches are read from the reference state so the inputs are mostly the right ones.
fn trace(seed: u64) -> Twin {
    let mut s = seed;
    let mut twin = Twin::bare();
    let mut reply = 0;
    for _ in 0..40 {
        let request = match &twin.reference.state {
            RefState::Persisting { pending, .. } | RefState::Calling { pending, .. } | RefState::Landing { pending, .. } => pending.request_id,
            _ => 1,
        };
        let sequence = match &twin.reference.state {
            RefState::Persisting { pending, .. } | RefState::Calling { pending, .. } | RefState::Landing { pending, .. } => pending.sequence,
            _ => 1,
        };
        let input = match next(&mut s) % 20 {
            0..=2 => { reply += 1; call(reply) }
            3 | 4 => committed(),
            5 => Input::Rejected(0, Reason::Storage),
            6 | 7 => response(request, if next(&mut s) % 6 == 0 { sequence + 1 } else { sequence }, if next(&mut s) % 2 == 0 { Some(10 + next(&mut s) % 3) } else { None }),
            8 => Input::Revoked { request_id: request, body: String::new() },
            9 => error(request, next(&mut s) % 2 == 0, next(&mut s) % 2 == 0),
            10 => Input::Tick,
            11 => Input::Online(next(&mut s) % 2 == 0),
            12 => Input::Published { epoch: EPOCH + next(&mut s) % 3 },
            13 => Input::EpochEnded { epoch: EPOCH + next(&mut s) % 3, cause: [EndCause::SessionReset, EndCause::ServerEnded, EndCause::Other][(next(&mut s) % 3) as usize] },
            14 => response(request + 50, sequence, None),
            15 => if next(&mut s) % 5 == 0 { Input::Closing } else { committed() },
            16 => { reply += 1; mutate(100 + reply, 1_000 + reply) }
            17 => Input::Dropped { request_id: request },
            18 => { reply += 1; call_of(["login", "visit"][(next(&mut s) % 2) as usize], reply) }
            _ => Input::CredentialGenerated(20 + next(&mut s) % 3),
        };
        twin.feed(input);
    }
    twin
}

#[test]
fn sess_prop_sequence() {
    let mut twin = Twin::new();
    for reply in 1..=3 {
        twin.feed(call(reply));
        twin.feed(committed());
        let id = twin.request_id();
        twin.feed(response(id, reply, Some(5)));
        twin.feed(committed());
    }
    let sequences: Vec<u64> = twin.history.iter().filter_map(|e| match e { SessionEffect::CommitPending { pending, .. } => Some(pending.sequence), _ => None }).collect();
    assert_eq!(sequences, vec![1, 2, 3], "the sequence grows by one without a gap");
    let requests = twin.history.iter().filter(|e| matches!(e, SessionEffect::HttpRequest { .. })).count();
    assert_eq!(requests, 3, "one request in the network per call");
}

#[test]
fn sess_prop_credential_only_committed() {
    for seed in 1..=40 {
        let twin = trace(seed);
        let committed_identities: Vec<u64> = twin.history.iter().filter_map(|e| match e { SessionEffect::CredentialCommitted(c) => Some(c.identity), _ => None }).collect();
        if let Some(credential) = twin.reference.credential {
            assert!(committed_identities.contains(&credential.digest), "the credential {credential:?} was never committed");
        }
    }
}

#[test]
fn sess_prop_reply_once() {
    for seed in 1..=40 {
        let mut twin = trace(seed);
        twin.feed(Input::Closing);
        let mut seen = Vec::new();
        for (id, _) in &twin.replies {
            assert!(!seen.contains(id), "call {id} is answered twice");
            seen.push(*id);
        }
    }
}

#[test]
fn sess_prop_idle() {
    let mut twin = Twin::new();
    twin.feed(Input::Tick);
    assert!(twin.history.is_empty(), "no pending, no request, no Tick");
}

#[test]
fn sess_prop_unauthorized_keeps_session() {
    let mut twin = landing(Some(5));
    twin.feed(committed());
    let before = twin.reference.credential.clone();
    twin.feed(call(2));
    twin.feed(committed());
    let id = twin.request_id();
    twin.feed(error(id, false, true));
    assert_eq!(twin.reference.credential, before, "the credential is the same");
    assert!(!twin.history.iter().any(|e| matches!(e, SessionEffect::ServerEnded(_))));
}

/// The owner alone, for the cells that `ref_session.rs` does not hold (D-08, D-11, D-17).
struct Solo {
    det: DetScheduler,
    addr: Address<SessionCalls>,
    log: Log<SessionEffect>,
}

impl Solo {
    fn new(credential: Option<Credential>) -> Self { Self::with(SessionState::with_operations(owner_operations(), "/logout".into(), true), credential) }

    fn with(state: SessionState, credential: Option<Credential>) -> Self {
        let mut det = DetScheduler::new(2);
        let log = new_log();
        let addr = det.add::<SessionCalls>("session", state, Recorder::<SessionCalls>::new(&log));
        let mut solo = Self { det, addr, log };
        solo.feed(SessionInput::Published(Published { epoch: EPOCH, identity: None, generation: 0, state: crate::owners::contract::PublishedState::Session, closed: false }));
        solo.feed(SessionInput::Open { credential, pending: None });
        solo
    }

    fn feed(&mut self, input: SessionInput) -> Vec<SessionEffect> {
        self.det.post(self.addr, EpochStamp::Control, input);
        self.det.run_until_quiet(100).expect("the owner is quiet");
        take(&self.log)
    }

    fn phase(&self) -> SessionPhase { self.det.state(self.addr).expect("the owner state").phase().clone() }

    fn commit(&mut self) -> Vec<SessionEffect> {
        let batch = match self.phase() {
            SessionPhase::Persisting { batch, .. } | SessionPhase::Landing { batch, .. } => batch,
            other => panic!("no batch in {other:?}"),
        };
        self.feed(SessionInput::Committed(Committed { batch, revision: 1, tables: vec![], models: Default::default() }))
    }

    fn calling(&mut self, operation: &str) -> u64 {
        self.feed(SessionInput::SessionCall { operation: operation.into(), body: "{}".into(), reply: 1 });
        self.commit();
        match self.phase() {
            SessionPhase::Calling { pending, .. } => pending.request_id,
            other => panic!("not calling: {other:?}"),
        }
    }
}

fn stored_credential() -> Option<Credential> { Some(Credential { digest: 4, sequence: 0 }) }

#[test]
fn sess_idle_call_no_credential() {
    let mut solo = Solo::new(None);
    let effects = solo.feed(SessionInput::SessionCall { operation: "logout".into(), body: "{}".into(), reply: 1 });
    assert_eq!(effects, vec![SessionEffect::Reply { id: 1, answer: Err(SessionError::NoCredential) }], "no request, no commit, the caller hears NoCredential");
    assert_eq!(solo.phase(), SessionPhase::Idle);
}

#[test]
fn sess_calling_revoked_logout() {
    let mut solo = Solo::new(stored_credential());
    let id = solo.calling("logout");
    let mut effects = solo.feed(SessionInput::Revoked { request_id: id, body: r#"{"revoked":true}"#.into() });
    effects.extend(solo.commit());
    assert!(effects.contains(&SessionEffect::Reply { id: 1, answer: Ok(r#"{"revoked":true}"#.into()) }), "a revoked answer to logout confirms the logout: {effects:?}");
    assert!(effects.iter().any(|effect| matches!(effect, SessionEffect::ServerEnded(_))), "and the server ended the session: {effects:?}");
    assert_eq!(solo.phase(), SessionPhase::Idle);
}

#[test]
fn sess_cancel_queued() {
    let mut solo = Solo::new(stored_credential());
    solo.feed(SessionInput::Mutate { author: 7, owner: None, req_id: 11, path: "/graphql".into(), body: "{}".into(), purpose: 0 });
    solo.commit();
    solo.feed(SessionInput::Mutate { author: 7, owner: None, req_id: 12, path: "/graphql".into(), body: "{}".into(), purpose: 0 });
    let effects = solo.feed(SessionInput::Cancel { req_id: 12 });
    assert_eq!(effects, vec![SessionEffect::Answer { author: 7, req_id: 12, outcome: AuthorOutcome::Error(SessionError::Interrupted) }], "the author of a queued Mutate hears Interrupted");
    assert!(matches!(solo.phase(), SessionPhase::Calling { .. }), "the started request goes on");
    assert_eq!(solo.det.state(solo.addr).expect("the owner state").queued(), 0);
}

#[test]
fn sess_cancel_started() {
    let mut solo = Solo::new(stored_credential());
    solo.feed(SessionInput::Mutate { author: 7, owner: None, req_id: 11, path: "/graphql".into(), body: "{}".into(), purpose: 0 });
    let persisting = solo.feed(SessionInput::Cancel { req_id: 11 });
    assert!(persisting.is_empty() && matches!(solo.phase(), SessionPhase::Persisting { .. }), "Persisting: {persisting:?}");
    solo.commit();
    let calling = solo.feed(SessionInput::Cancel { req_id: 11 });
    assert!(calling.is_empty(), "Calling: no HttpCancel, no reply: {calling:?}");
    assert!(matches!(solo.phase(), SessionPhase::Calling { sent: true, .. }), "the request stays in the network");
    solo.feed(SessionInput::Response { request_id: 11, body: "{}".into(), sequence: Some(1), credential: None });
    let landing = solo.feed(SessionInput::Cancel { req_id: 11 });
    assert!(landing.is_empty() && matches!(solo.phase(), SessionPhase::Landing { .. }), "Landing: {landing:?}");
    let done = solo.commit();
    assert_eq!(done.iter().filter(|effect| matches!(effect, SessionEffect::Answer { .. })).count(), 1, "one final answer: {done:?}");
    assert!(done.contains(&SessionEffect::Answer { author: 7, req_id: 11, outcome: AuthorOutcome::Response("{}".into()) }));
}

#[test]
fn sess_calling_terminal_error_answers_after_the_commit() {
    for (input, answer, ended) in [
        (SessionInput::HttpError { request_id: 0, temporary: false, auth: false }, SessionError::Http, false),
        (SessionInput::HttpError { request_id: 0, temporary: false, auth: true }, SessionError::Unauthorized, false),
        (SessionInput::Revoked { request_id: 0, body: "{}".into() }, SessionError::Revoked, true),
    ] {
        let mut solo = Solo::new(stored_credential());
        let id = solo.calling("social");
        let input = match input {
            SessionInput::HttpError { temporary, auth, .. } => SessionInput::HttpError { request_id: id, temporary, auth },
            SessionInput::Revoked { body, .. } => SessionInput::Revoked { request_id: id, body },
            other => other,
        };
        let before = solo.feed(input);
        assert!(matches!(&before[..], [SessionEffect::RemovePending { .. }]), "{answer:?}: nothing is answered before the commit: {before:?}");
        let after = solo.commit();
        assert!(after.contains(&SessionEffect::Reply { id: 1, answer: Err(answer.clone()) }), "{answer:?}: {after:?}");
        assert_eq!(after.iter().any(|effect| matches!(effect, SessionEffect::ServerEnded(_))), ended, "{answer:?}");
        assert_eq!(solo.det.state(solo.addr).expect("the owner state").credential().map(|credential| credential.sequence), Some(0), "a failed call spends no sequence");
    }
}

#[test]
fn sess_calling_tick_starts_one_request() {
    let mut solo = Solo::new(stored_credential());
    let id = solo.calling("social");
    solo.feed(SessionInput::HttpError { request_id: id, temporary: true, auth: false });
    let first = solo.feed(SessionInput::Tick(Time(0)));
    let second = solo.feed(SessionInput::Tick(Time(0)));
    assert_eq!(first.iter().filter(|effect| matches!(effect, SessionEffect::HttpRequest { .. })).count(), 1, "{first:?}");
    assert!(second.is_empty(), "the second Tick starts nothing: {second:?}");
    assert!(solo.feed(SessionInput::Idle).is_empty());
}

#[test]
fn sess_landing_rejected_keeps_the_pending() {
    let mut solo = Solo::new(stored_credential());
    let id = solo.calling("social");
    solo.feed(SessionInput::Response { request_id: id, body: "{}".into(), sequence: Some(1), credential: None });
    let batch = match solo.phase() { SessionPhase::Landing { batch, .. } => batch, other => panic!("{other:?}") };
    let rejected = solo.feed(SessionInput::Rejected(Rejected { batch, reason: RejectReason::Storage("disk".into()) }));
    assert!(rejected.contains(&SessionEffect::Reply { id: 1, answer: Err(SessionError::Rejected(RejectReason::Storage("disk".into()))) }));
    assert!(matches!(solo.phase(), SessionPhase::Blocked { retry: true, .. }));
    let next = solo.feed(SessionInput::SessionCall { operation: "social".into(), body: "{}".into(), reply: 2 });
    assert_eq!(next, vec![SessionEffect::Reply { id: 2, answer: Err(SessionError::StateLost) }], "no new pending replaces the stored one");
    let retry = solo.feed(SessionInput::Tick(Time(0)));
    assert!(matches!(&retry[..], [SessionEffect::HttpRequest { pending }] if pending.request_id == id && pending.sequence == 1), "the same pair goes again: {retry:?}");
}

#[test]
fn sess_generating_secret_failed() {
    let mut solo = Solo::new(None);
    solo.feed(SessionInput::SessionCall { operation: "social".into(), body: "{}".into(), reply: 1 });
    assert!(matches!(solo.phase(), SessionPhase::Generating { .. }));
    let effects = solo.feed(SessionInput::CredentialFailed { detail: "keychain".into() });
    assert_eq!(effects, vec![SessionEffect::Reply { id: 1, answer: Err(SessionError::Rejected(RejectReason::Storage("keychain".into()))) }]);
    assert_eq!(solo.phase(), SessionPhase::Idle);
}

#[test]
fn sess_calling_secret_unavailable() {
    let mut solo = Solo::new(stored_credential());
    let id = solo.calling("social");
    let lost = solo.feed(SessionInput::CredentialUnavailable { request_id: id, lost: true, detail: String::new() });
    assert_eq!(lost, vec![SessionEffect::Reply { id: 1, answer: Err(SessionError::StateLost) }], "no schedule for a lost secret");
    assert!(matches!(solo.phase(), SessionPhase::Blocked { retry: false, .. }));
    assert!(solo.feed(SessionInput::Online(Online(true))).is_empty() && solo.feed(SessionInput::Tick(Time(0))).is_empty(), "StateLost is not repeated");
    let mut solo = Solo::new(stored_credential());
    let id = solo.calling("social");
    let failed = solo.feed(SessionInput::CredentialUnavailable { request_id: id, lost: false, detail: "locked".into() });
    assert!(failed.contains(&SessionEffect::Reply { id: 1, answer: Err(SessionError::Rejected(RejectReason::Storage("locked".into()))) }));
    assert!(matches!(solo.phase(), SessionPhase::Blocked { retry: true, .. }), "a store error may be repeated");
    let repeat = solo.feed(SessionInput::Tick(Time(0)));
    assert!(matches!(&repeat[..], [SessionEffect::HttpRequest { pending }] if pending.request_id == id), "{repeat:?}");
}

/// The logout of the client is a path: the operation named `logout` is an ordinary one, the operation on the path of the logout is the logout.
fn renamed_logout() -> SessionState {
    let def = |name: &str, path: &str| SessionOperation { name: name.into(), path: path.into(), sessionless: false };
    SessionState::with_operations(vec![def("logout", "/signin"), def("leave", "/exit")], "/exit".into(), true)
}

#[test]
fn sess_idle_call_no_credential_by_path() {
    let mut solo = Solo::with(renamed_logout(), None);
    let refused = solo.feed(SessionInput::SessionCall { operation: "leave".into(), body: "{}".into(), reply: 1 });
    assert_eq!(refused, vec![SessionEffect::Reply { id: 1, answer: Err(SessionError::NoCredential) }], "the operation on the logout path of the client is refused without a request");
    assert_eq!(solo.phase(), SessionPhase::Idle);
}

#[test]
fn sess_idle_login_generate_by_path() {
    let mut solo = Solo::with(renamed_logout(), None);
    let effects = solo.feed(SessionInput::SessionCall { operation: "logout".into(), body: "{}".into(), reply: 1 });
    assert_eq!(effects, vec![SessionEffect::GenerateCredential], "an operation named logout on another path is a sign-in");
    assert!(matches!(solo.phase(), SessionPhase::Generating { .. }));
}

#[test]
fn sess_calling_revoked_logout_by_path() {
    let mut solo = Solo::with(renamed_logout(), stored_credential());
    let id = solo.calling("leave");
    let mut effects = solo.feed(SessionInput::Revoked { request_id: id, body: "{}".into() });
    effects.extend(solo.commit());
    assert!(effects.contains(&SessionEffect::Reply { id: 1, answer: Ok("{}".into()) }), "revoked on the logout path of the client confirms the logout: {effects:?}");
    let mut solo = Solo::with(renamed_logout(), stored_credential());
    let id = solo.calling("logout");
    let mut effects = solo.feed(SessionInput::Revoked { request_id: id, body: "{}".into() });
    effects.extend(solo.commit());
    assert!(effects.contains(&SessionEffect::Reply { id: 1, answer: Err(SessionError::Revoked) }), "revoked on another path is Revoked: {effects:?}");
}
