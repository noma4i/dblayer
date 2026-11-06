use crate::owners::contract::{Committed, EpochEnded, Published, PublishedState};
use crate::owners::session::{Credential, OperationClass, PendingCall, SecretState, SessionCalls, SessionEffect, SessionError, SessionInput, SessionState};
use crate::owners::support::{new_log, take, Log, Recorder};
use crate::runtime::{Address, Cause, DetScheduler, EpochStamp};

struct Rig { det: DetScheduler, address: Address<SessionCalls>, log: Log<SessionEffect> }

impl Rig {
    fn new() -> Self {
        let mut det = DetScheduler::new(3);
        let log = new_log();
        let address = det.add::<SessionCalls>("session", SessionState::new(&[], true), Recorder::<SessionCalls>::new(&log));
        let mut rig = Self { det, address, log };
        rig.send(SessionInput::Published(Published { epoch: 3, identity: Some(4), generation: 0, state: PublishedState::Session, closed: false }));
        rig.send(SessionInput::Open { credential: Some(Credential { digest: 4, sequence: 0 }), pending: None, dropped_pending: false, secret: SecretState::Present(4) });
        rig
    }

    fn send(&mut self, input: SessionInput) -> Vec<SessionEffect> {
        self.det.post(self.address, EpochStamp::Control, input);
        self.det.run_until_quiet(100).unwrap();
        take(&self.log)
    }

    fn restore(&mut self) {
        let pending = PendingCall { request_id: 11, sequence: 1, class: OperationClass::Authenticated, operation: "read".into(), path: "/read".into(), body: "{}".into(), credential: Some(4), purpose: 0, author: None, owner: None, epoch: 3, reply: None };
        self.send(SessionInput::Restored { epoch: 3, pending: Some(pending), answers: Default::default(), pending_lost: false });
    }

    fn start(&mut self) -> u64 {
        self.send(SessionInput::RestoreReady { epoch: 3 }).iter().find_map(|effect| match effect { SessionEffect::HttpRequest { attempt_id, .. } => Some(*attempt_id), _ => None }).unwrap()
    }

    fn refuse(&mut self, attempt_id: u64) -> Vec<SessionEffect> {
        let effects = self.send(SessionInput::HttpError { request_id: 11, attempt_id, temporary: false, auth: false, persisted_missing: false, status: 0, too_large: Some((262145, 262144)) });
        let batch = effects.iter().find_map(|effect| match effect { SessionEffect::CommitLanding { batch, .. } => Some(*batch), _ => None }).unwrap();
        self.send(SessionInput::Committed(Committed { batch, revision: 1, tables: vec![], models: Default::default() }))
    }
}

fn resume(id: u64) -> SessionInput { SessionInput::SessionResume { reply: id } }
fn too_large() -> SessionError { SessionError::TooLarge { size: 262145, limit: 262144 } }
fn answer(id: u64, result: Result<Option<String>, SessionError>) -> SessionEffect { SessionEffect::ResumeReply { id, answer: result } }

#[test]
fn sess_resume_none() { assert_eq!(Rig::new().send(resume(1)), [answer(1, Ok(None))]); }

#[test]
fn sess_resume_waits() {
    let mut rig = Rig::new();
    rig.restore();
    assert!(rig.send(resume(1)).is_empty());
    rig.start();
    assert!(rig.send(resume(2)).is_empty());
}

#[test]
fn sess_resume_waiter_answered() {
    let mut rig = Rig::new();
    rig.restore();
    let attempt = rig.start();
    rig.send(resume(1));
    rig.send(resume(2));
    assert_eq!(rig.refuse(attempt), [answer(1, Err(too_large())), answer(2, Err(too_large()))]);
    assert_eq!(rig.send(resume(3)), [answer(3, Ok(None))]);
}

#[test]
fn sess_restored_answer_kept() {
    let mut rig = Rig::new();
    rig.restore();
    let attempt = rig.start();
    assert!(rig.refuse(attempt).is_empty());
    assert_eq!(rig.send(resume(1)), [answer(1, Err(too_large()))]);
}

#[test]
fn sess_resume_takes_answer() {
    let mut rig = Rig::new();
    rig.restore();
    let attempt = rig.start();
    rig.refuse(attempt);
    assert_eq!(rig.send(resume(1)), [answer(1, Err(too_large()))]);
    assert_eq!(rig.send(resume(2)), [answer(2, Ok(None))]);
}

#[test]
fn sess_resume_end() {
    let mut rig = Rig::new();
    rig.restore();
    rig.start();
    rig.send(resume(1));
    assert!(rig.send(SessionInput::EpochEnded(EpochEnded { epoch: 2, cause: Cause::SessionReset })).is_empty());
    let effects = rig.send(SessionInput::EpochEnded(EpochEnded { epoch: 3, cause: Cause::SessionReset }));
    assert!(effects.contains(&answer(1, Err(SessionError::Interrupted))));
    assert_eq!(rig.send(resume(2)), [answer(2, Ok(None))]);
    let mut closed = Rig::new();
    closed.restore();
    closed.send(resume(3));
    assert!(closed.send(SessionInput::Closing).contains(&answer(3, Err(SessionError::Closed))));
    assert_eq!(closed.send(resume(4)), [answer(4, Err(SessionError::Closed))]);
}

#[test]
fn sess_epoch_ended_stale() {
    let mut rig = Rig::new();
    rig.restore();
    let attempt = rig.start();
    rig.send(resume(1));
    assert!(rig.send(SessionInput::EpochEnded(EpochEnded { epoch: 2, cause: Cause::SessionReset })).is_empty());
    assert_eq!(rig.refuse(attempt), [answer(1, Err(too_large()))]);
    assert_eq!(rig.det.state(rig.address).unwrap().credential().unwrap().digest, 4);
}

#[test]
fn sess_prop_resume_once() {
    let mut rig = Rig::new();
    rig.restore();
    let attempt = rig.start();
    let response = rig.send(SessionInput::Response { request_id: 11, attempt_id: attempt, body: "{\"ok\":true}".into(), sequence: Some(1), refusal: None });
    let batch = response.iter().find_map(|effect| match effect { SessionEffect::CommitLanding { batch, sequence: Some(1), .. } => Some(*batch), _ => None }).unwrap();
    let committed = rig.send(SessionInput::Committed(Committed { batch, revision: 1, tables: vec![], models: Default::default() }));
    assert!(!committed.iter().any(|effect| matches!(effect, SessionEffect::HttpRequest { .. } | SessionEffect::Reply { .. })));
    let published = rig.send(SessionInput::Published(Published { epoch: 3, identity: Some(4), generation: 0, state: PublishedState::Session, closed: false }));
    assert!(!published.iter().any(|effect| matches!(effect, SessionEffect::HttpRequest { .. } | SessionEffect::Reply { .. })));
    assert_eq!(rig.send(resume(1)), [answer(1, Ok(Some("{\"ok\":true}".into())))]);
    assert_eq!(rig.send(resume(2)), [answer(2, Ok(None))]);
    assert_eq!(rig.det.state(rig.address).unwrap().credential().unwrap().sequence, 1);
}
