//! Owner-level tests of the owner `Session` in `specs/machines/lifecycle.md`: the owner on `DetScheduler` against `ref_session.rs`. Names are the `Test` column.

use super::ref_session::{Credential as RefCredential, Effect as RefEffect, EndCause, Fail, Input, Pending as RefPending, Reason, Session as Ref, State as RefState};
use crate::owners::contract::{Committed, EpochEnded, Online, Published, RejectReason, Rejected};
use crate::owners::session::{Credential, PendingCall, SessionCalls, SessionEffect, SessionError, SessionInput, SessionPhase, SessionState};
use crate::owners::support::{new_log, take, Log, Recorder};
use crate::runtime::{Address, Cause, DetScheduler, EpochStamp, Time};

const EPOCH: u64 = 3;
const DECLARED: [&str; 2] = ["social", "logout"];

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
    }
}

fn fail_of(error: &SessionError) -> Fail {
    match error {
        SessionError::Invalid => Fail::Invalid,
        SessionError::Rejected(reason) => Fail::Reason(reason_of(reason)),
        SessionError::Revoked => Fail::Revoked,
        SessionError::SequenceConflict => Fail::SequenceConflict,
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

fn pending_of(pending: &PendingCall) -> RefPending { RefPending { request_id: pending.request_id, sequence: pending.sequence, operation: pending.operation.clone(), reply: pending.reply } }

fn phase_of(phase: &SessionPhase) -> RefState {
    match phase {
        SessionPhase::Idle => RefState::Idle,
        SessionPhase::Persisting { pending, batch } => RefState::Persisting { pending: pending_of(pending), batch: *batch },
        SessionPhase::Calling { pending, attempts, sent } => RefState::Calling { pending: pending_of(pending), attempts: *attempts, sent: *sent },
        SessionPhase::Landing { pending, batch, answer, identity } => RefState::Landing { pending: pending_of(pending), batch: *batch, answer: answer.clone(), identity: *identity },
        SessionPhase::Closed => RefState::Closed,
    }
}

fn effect_of(effect: &SessionEffect) -> Option<RefEffect> {
    Some(match effect {
        SessionEffect::CommitPending { batch, request_id, sequence } => RefEffect::CommitPending { batch: *batch, request_id: *request_id, sequence: *sequence },
        SessionEffect::CommitLanding { batch, digest, sequence } => RefEffect::CommitLanding { batch: *batch, digest: *digest, sequence: *sequence },
        SessionEffect::RemovePending { batch } => RefEffect::RemovePending { batch: *batch },
        SessionEffect::HttpRequest { request_id } => RefEffect::HttpRequest { request_id: *request_id },
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

    fn with(online: bool) -> Self {
        let mut det = DetScheduler::new(2);
        let log = new_log();
        let addr = det.add::<SessionCalls>("session", SessionState::new(&DECLARED, online), Recorder::<SessionCalls>::new(&log));
        let mut twin = Self { det, addr, log, reference: Ref::new(&DECLARED, online), history: Vec::new(), replies: Vec::new() };
        twin.feed(Input::Published { epoch: EPOCH });
        twin
    }

    fn feed(&mut self, input: Input) -> Vec<RefEffect> {
        let batch_of = |state: &RefState| match state {
            RefState::Persisting { batch, .. } | RefState::Landing { batch, .. } => *batch,
            _ => 9_999,
        };
        let current = batch_of(&self.reference.state);
        let owner_input = match input.clone() {
            Input::Open { credential, pending } => SessionInput::Open { credential: credential.map(|c| Credential { digest: c.digest, sequence: c.sequence }), pending: pending.map(|p| PendingCall { request_id: p.request_id, sequence: p.sequence, operation: p.operation, reply: p.reply }) },
            Input::SessionCall { operation, reply } => SessionInput::SessionCall { operation, reply },
            Input::Response { request_id, body, sequence, credential, revoked } => SessionInput::Response { request_id, body, sequence, credential, revoked },
            Input::HttpError { request_id, temporary, auth } => SessionInput::HttpError { request_id, temporary, auth },
            Input::Committed(batch) => SessionInput::Committed(Committed { batch: if batch == 0 { current } else { batch }, revision: 1, tables: vec![], models: vec![] }),
            Input::Rejected(batch, reason) => SessionInput::Rejected(Rejected { batch: if batch == 0 { current } else { batch }, reason: reject(reason) }),
            Input::Tick => SessionInput::Tick(Time(0)),
            Input::Online(online) => SessionInput::Online(Online(online)),
            Input::Published { epoch } => SessionInput::Published(Published { epoch, identity: None, generation: 0, closed: false }),
            Input::EpochEnded { epoch, cause: c } => SessionInput::EpochEnded(EpochEnded { epoch, cause: cause(c) }),
            Input::Closing => SessionInput::Closing,
        };
        let reference_input = match input {
            Input::Committed(0) => Input::Committed(current),
            Input::Rejected(0, reason) => Input::Rejected(current, reason),
            other => other,
        };
        self.det.post(self.addr, EpochStamp::Control, owner_input);
        self.det.run_until_quiet(100).expect("the owner is quiet");
        let raw = take(&self.log);
        self.history.extend(raw.iter().cloned());
        let want = self.reference.step(reference_input);
        let effects: Vec<RefEffect> = raw.iter().filter_map(effect_of).collect();
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

fn call(reply: u64) -> Input { Input::SessionCall { operation: "social".into(), reply } }
fn committed() -> Input { Input::Committed(0) }
fn response(request_id: u64, sequence: u64, credential: Option<u64>) -> Input { Input::Response { request_id, body: "{}".into(), sequence: Some(sequence), credential, revoked: false } }
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
fn sess_idle_unknown_call() { Twin::new().feed(Input::SessionCall { operation: "nope".into(), reply: 1 }); }

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
    twin.feed(Input::Response { request_id: id, body: String::new(), sequence: None, credential: None, revoked: true });
}

#[test]
fn sess_calling_sequence_conflict() {
    let mut twin = calling();
    let id = twin.request_id();
    twin.feed(response(id, 9, None));
    twin.feed(Input::Response { request_id: 99, body: String::new(), sequence: None, credential: None, revoked: false });
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
fn sess_calling_tick() { calling().feed(Input::Tick); }

#[test]
fn sess_calling_tick_offline() {
    let mut twin = calling();
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
    let effects = twin.feed(Input::Open { credential: Some(RefCredential { digest: 4, sequence: 6 }), pending: Some(RefPending { request_id: 5, sequence: 7, operation: "social".into(), reply: Some(99) }) });
    assert_eq!(effects, vec![RefEffect::HttpRequest { request_id: 5 }]);
    twin.feed(response(5, 7, None));
    twin.feed(committed());
    assert!(twin.replies.is_empty(), "an answer without a caller shows nothing");
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

fn next(seed: &mut u64) -> u64 {
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    *seed >> 33
}

/// A random trace; requests and batches are read from the reference state so the inputs are mostly the right ones.
fn trace(seed: u64) -> Twin {
    let mut s = seed;
    let mut twin = Twin::new();
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
        let input = match next(&mut s) % 16 {
            0..=2 => { reply += 1; call(reply) }
            3 | 4 => committed(),
            5 => Input::Rejected(0, Reason::Storage),
            6 | 7 => response(request, if next(&mut s) % 6 == 0 { sequence + 1 } else { sequence }, if next(&mut s) % 2 == 0 { Some(10 + next(&mut s) % 3) } else { None }),
            8 => Input::Response { request_id: request, body: String::new(), sequence: None, credential: None, revoked: true },
            9 => error(request, next(&mut s) % 2 == 0, next(&mut s) % 2 == 0),
            10 => Input::Tick,
            11 => Input::Online(next(&mut s) % 2 == 0),
            12 => Input::Published { epoch: EPOCH + next(&mut s) % 3 },
            13 => Input::EpochEnded { epoch: EPOCH + next(&mut s) % 3, cause: [EndCause::SessionReset, EndCause::ServerEnded, EndCause::Other][(next(&mut s) % 3) as usize] },
            14 => response(request + 50, sequence, None),
            _ => if next(&mut s) % 5 == 0 { Input::Closing } else { committed() },
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
    let sequences: Vec<u64> = twin.history.iter().filter_map(|e| match e { SessionEffect::CommitPending { sequence, .. } => Some(*sequence), _ => None }).collect();
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
