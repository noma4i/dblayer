use crate::store::Store;
use crate::owners::contract::{Committed, EpochEnded, Online, Published, PublishedState, RejectReason, Rejected};
use crate::owners::session::{AuthorOutcome, Credential, OperationClass, SecretState, SessionCalls, SessionEffect, SessionError, SessionInput, SessionOperation, SessionPhase, SessionState};
use crate::spec::support::{new_log, take, Log, Recorder};
use crate::runtime::{Address, Cause, DetScheduler, EpochStamp};

fn send(det: &mut DetScheduler, address: Address<SessionCalls>, log: &Log<SessionEffect>, input: SessionInput) -> Vec<SessionEffect> {
    det.post(address, EpochStamp::Control, input);
    det.run_until_quiet(100).unwrap();
    let effects = take(log);
    if let [SessionEffect::SealProbe { seal_id, .. }] = effects.as_slice() {
        det.post(address, EpochStamp::Control, SessionInput::Sealed { seal_id: *seal_id, size: 1 });
        det.run_until_quiet(100).unwrap();
        return take(log);
    }
    effects
}

#[test]
fn sess_epoch_ended_kept() {
    for phase in 0..4 {
        for cause in [Cause::IdentityChanged, Cause::SessionStarted, Cause::Close] {
            let mut det = DetScheduler::new(2);
            let log = new_log();
            let operations = vec![SessionOperation { name: "login".into(), path: "/login".into(), class: OperationClass::Login }];
            let address = det.add::<SessionCalls>("session", SessionState::with_operations(operations, Some(true)), Recorder::<SessionCalls>::new(&log));
            send(&mut det, address, &log, SessionInput::Published(Published { epoch: 3, identity: None, generation: 0, state: PublishedState::Session, closed: false }));
            send(&mut det, address, &log, SessionInput::Snapshot { epoch: 3, online: true });
            let credential = (phase >= 2).then_some(Credential { digest: 4, sequence: 0 });
            let secret = if credential.is_some() { SecretState::Present(4) } else { SecretState::Missing };
            send(&mut det, address, &log, SessionInput::Open { credential, pending: None, dropped_pending: false, secret });
            if phase < 2 {
                let effects = send(&mut det, address, &log, SessionInput::SessionCall { operation: "login".into(), body: "{}".into(), reply: 1 });
                let op = match effects.as_slice() { [SessionEffect::GenerateCredential { secret_op_id }] => *secret_op_id, other => panic!("{other:?}") };
                if phase == 1 { send(&mut det, address, &log, SessionInput::CredentialGenerated { secret_op_id: op, credential: 42 }); }
            } else {
                let effects = send(&mut det, address, &log, SessionInput::Mutate { author: 7, owner: None, req_id: 11, epoch: 3, path: "/graphql".into(), body: "{}".into(), purpose: 0 });
                let batch = effects.iter().find_map(|effect| match effect { SessionEffect::CommitPending { batch, .. } => Some(*batch), _ => None }).unwrap();
                let effects = send(&mut det, address, &log, SessionInput::Committed(Committed { batch, revision: 1, tables: vec![], rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0, refused: Vec::new() }));
                let attempt = effects.iter().find_map(|effect| match effect { SessionEffect::HttpRequest { attempt_id, .. } => Some(*attempt_id), _ => None }).unwrap();
                let input = if phase == 2 { SessionInput::Dropped { request_id: 11, attempt_id: attempt } } else { SessionInput::Response { request_id: 11, attempt_id: attempt, body: "{}".into(), sequence: Some(5), refusal: None, kind: crate::transport::AnswerKind::Answered { sequence: Some(5) } } };
                send(&mut det, address, &log, input);
            }
            let before = det.state(address).unwrap().phase().clone();
            assert!(match phase { 0 => matches!(before, SessionPhase::Generating { .. }), 1 => matches!(before, SessionPhase::Storing { .. }), 2 => matches!(before, SessionPhase::PendingHeld { .. }), _ => matches!(before, SessionPhase::Blocked { .. }) });
            assert!(send(&mut det, address, &log, SessionInput::EpochEnded(EpochEnded { epoch: 3, cause })).is_empty());
            assert_eq!(det.state(address).unwrap().phase(), &before);
        }
    }
}

#[test]
fn sess_generating_other() {
    for storing in [false, true] {
        let mut det = DetScheduler::new(2);
        let log = new_log();
        let operations = vec![SessionOperation { name: "login".into(), path: "/login".into(), class: OperationClass::Login }];
        let address = det.add::<SessionCalls>("session", SessionState::with_operations(operations, Some(true)), Recorder::<SessionCalls>::new(&log));
        let published = Published { epoch: 3, identity: None, generation: 0, state: PublishedState::Session, closed: false };
        send(&mut det, address, &log, SessionInput::Published(published.clone()));
        send(&mut det, address, &log, SessionInput::Snapshot { epoch: 3, online: true });
        send(&mut det, address, &log, SessionInput::Open { credential: None, pending: None, dropped_pending: false, secret: SecretState::Missing });
        let effects = send(&mut det, address, &log, SessionInput::SessionCall { operation: "login".into(), body: "{}".into(), reply: 1 });
        let op = match effects.as_slice() { [SessionEffect::GenerateCredential { secret_op_id }] => *secret_op_id, other => panic!("{other:?}") };
        if storing { send(&mut det, address, &log, SessionInput::CredentialGenerated { secret_op_id: op, credential: 42 }); }
        let before = det.state(address).unwrap().phase().clone();
        assert!(matches!(before, SessionPhase::Generating { .. } | SessionPhase::Storing { .. }));
        let inputs = [
            SessionInput::Online(Online(false)), SessionInput::Published(published), SessionInput::Tick,
            SessionInput::Response { request_id: 9, attempt_id: 9, body: "{}".into(), sequence: None, refusal: None, kind: crate::transport::AnswerKind::Answered { sequence: None } },
            SessionInput::Revoked { request_id: 9, attempt_id: 9, body: "{}".into(), author_ref: None, epoch: 0 },
            SessionInput::HttpError { request_id: 9, attempt_id: 9, temporary: false, auth: false, status: 0, too_large: None, security: None, body: None },
            SessionInput::Dropped { request_id: 9, attempt_id: 9 },
            SessionInput::Committed(Committed { batch: 9, revision: 1, tables: vec![], rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0, refused: Vec::new() }),
            SessionInput::Rejected(Rejected { batch: 9, reason: RejectReason::NoSession }),
        ];
        for input in inputs {
            assert!(send(&mut det, address, &log, input).is_empty());
            assert_eq!(det.state(address).unwrap().phase(), &before);
        }
    }
}

fn calling_error(too_large: Option<(u64, u64)>, security: Option<(crate::transport::SecurityCode, String)>, expected: SessionError) {
    for author in [false, true] {
        let mut det = DetScheduler::new(2);
        let log = new_log();
        let operations = vec![SessionOperation { name: "read".into(), path: "/read".into(), class: OperationClass::Authenticated }];
        let address = det.add::<SessionCalls>("session", SessionState::with_operations(operations, Some(true)), Recorder::<SessionCalls>::new(&log));
        send(&mut det, address, &log, SessionInput::Published(Published { epoch: 3, identity: Some(4), generation: 0, state: PublishedState::Session, closed: false }));
        send(&mut det, address, &log, SessionInput::Snapshot { epoch: 3, online: true });
        send(&mut det, address, &log, SessionInput::Open { credential: Some(Credential { digest: 4, sequence: 0 }), pending: None, dropped_pending: false, secret: SecretState::Present(4) });
        let input = if author { SessionInput::Mutate { author: 7, owner: None, req_id: 11, epoch: 3, path: "/graphql".into(), body: "{}".into(), purpose: 0 } } else { SessionInput::SessionCall { operation: "read".into(), body: "{}".into(), reply: 1 } };
        let effects = send(&mut det, address, &log, input);
        let (batch, request) = effects.iter().find_map(|effect| match effect { SessionEffect::CommitPending { batch, pending, .. } => Some((*batch, pending.request_id)), _ => None }).unwrap();
        let effects = send(&mut det, address, &log, SessionInput::Committed(Committed { batch, revision: 1, tables: vec![], rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0, refused: Vec::new() }));
        let attempt = effects.iter().find_map(|effect| match effect { SessionEffect::HttpRequest { attempt_id, .. } => Some(*attempt_id), _ => None }).unwrap();
        let effects = send(&mut det, address, &log, SessionInput::HttpError { request_id: request, attempt_id: attempt, temporary: false, auth: false, status: 0, too_large, security: security.clone(), body: None });
        let (batch, answer) = effects.iter().find_map(|effect| match effect { SessionEffect::CommitLanding { batch, answer, .. } => Some((*batch, answer.clone())), _ => None }).unwrap();
        assert_eq!(answer, author.then(|| (request, Err(expected.clone()))));
        assert!(!effects.iter().any(|effect| matches!(effect, SessionEffect::Reply { .. } | SessionEffect::Answer { .. })));
        let effects = send(&mut det, address, &log, SessionInput::Committed(Committed { batch, revision: 2, tables: vec![], rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0, refused: Vec::new() }));
        if author {
            assert!(effects.iter().any(|effect| matches!(effect, SessionEffect::Answer { author: 7, req_id: 11, outcome: AuthorOutcome::Error { error, .. } } if error == &expected)));
        } else {
            assert!(effects.contains(&SessionEffect::Reply { id: 1, answer: Err(expected.clone()) }));
        }
    }
}

#[test]
fn sess_calling_status_zero() { calling_error(None, None, SessionError::Http { status: 0 }); }

#[test]
fn sess_calling_security() {
    let code = crate::transport::SecurityCode::TlsTrust;
    let detail = "platform trust refused".to_string();
    calling_error(None, Some((code, detail.clone())), SessionError::Security { code, detail });
}

#[test]
fn sess_calling_too_large() {
    use crate::store::session_state;
    calling_error(Some((262145, 262144)), None, SessionError::TooLarge { size: 262145, limit: 262144 });
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    let open = || Store::open_models(Some(&path), vec![], None, std::sync::Arc::new(crate::runtime::SystemClock)).map(std::sync::Arc::new).unwrap();
    let db = open();
    db.lock().connection.execute_batch("DROP TABLE encrypted_answer; CREATE TABLE encrypted_answer (request_id INTEGER PRIMARY KEY, outcome TEXT NOT NULL CHECK (outcome IN ('ok', 'http', 'unauthorized', 'revoked', 'session')), detail TEXT, operation_id INTEGER) STRICT; INSERT INTO encrypted_answer VALUES (10, 'http', '422', 6); UPDATE manifest SET value = 'old' WHERE key = 'format_version';").unwrap();
    drop(db);
    let db = open();
    assert_eq!(session_state::answers(&db.lock().connection).unwrap().get(&10), Some(&session_state::StoredAnswer::Http { status: 422 }));
    let answer = crate::owners::session::stored_answer(&Err(SessionError::TooLarge { size: 262145, limit: 262144 })).unwrap();
    session_state::store_answer(&db.lock().connection, 11, Some(7), &answer).unwrap();
    drop(db);
    let db = open();
    assert_eq!(session_state::answers(&db.lock().connection).unwrap().get(&11), Some(&answer));
}

#[test]
fn http_failure_reports_after_its_answer_is_committed() {
    let mut det = DetScheduler::new(2);
    let log = new_log();
    let address = det.add::<SessionCalls>("session", SessionState::new(&[], true), Recorder::<SessionCalls>::new(&log));
    send(&mut det, address, &log, SessionInput::Published(Published { epoch: 3, identity: Some(4), generation: 0, state: PublishedState::Session, closed: false }));
    send(&mut det, address, &log, SessionInput::Snapshot { epoch: 3, online: true });
    send(&mut det, address, &log, SessionInput::Open { credential: Some(Credential { digest: 4, sequence: 0 }), pending: None, dropped_pending: false, secret: SecretState::Present(4) });
    let effects = send(&mut det, address, &log, SessionInput::Mutate { author: 7, owner: None, req_id: 11, epoch: 3, path: "/graphql".into(), body: "{}".into(), purpose: 0 });
    let batch = effects.iter().find_map(|effect| match effect { SessionEffect::CommitPending { batch, .. } => Some(*batch), _ => None }).unwrap();
    let effects = send(&mut det, address, &log, SessionInput::Committed(Committed { batch, revision: 1, tables: vec![], rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0, refused: Vec::new() }));
    let attempt = effects.iter().find_map(|effect| match effect { SessionEffect::HttpRequest { attempt_id, .. } => Some(*attempt_id), _ => None }).unwrap();
    let effects = send(&mut det, address, &log, SessionInput::HttpFailed { attempt_id: attempt });
    assert!(!effects.iter().any(|effect| matches!(effect, SessionEffect::Answer { .. } | SessionEffect::ExecutorFailed)));
    let batch = effects.iter().find_map(|effect| match effect { SessionEffect::CommitLanding { batch, .. } => Some(*batch), _ => None }).unwrap();
    let effects = send(&mut det, address, &log, SessionInput::Committed(Committed { batch, revision: 2, tables: vec![], rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0, refused: Vec::new() }));
    assert!(matches!(&effects[..], [SessionEffect::Answer { author: 7, req_id: 11, outcome: AuthorOutcome::Error { error: SessionError::Http { status: 0 }, .. } }, SessionEffect::ExecutorFailed]));
}

#[test]
fn sess_prop_login_and_lifecycle_ending_orders() {
    use crate::owners::lifecycle::{Lifecycle, LifecycleEffect, LifecycleInput, LifecyclePhase, LifecycleState};
    let orders = [[0, 1, 2], [0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0]];
    for failed in [false, true] {
        for order in orders {
            let mut det = DetScheduler::new(2);
            let session_log = new_log();
            let life_log = new_log();
            let ops = vec![SessionOperation { name: "login".into(), path: "/login".into(), class: OperationClass::Login }];
            let session = det.add::<SessionCalls>("session", SessionState::with_operations(ops, Some(true)), Recorder::<SessionCalls>::new(&session_log));
            let life = det.add::<Lifecycle>("lifecycle", LifecycleState::new(), Recorder::<Lifecycle>::new(&life_log));
            det.post(life, EpochStamp::Control, LifecycleInput::Open { saved_epoch: 2, saved_generation: 0, saved_identity: Some(4), saved_unowned_pending: false, reply: None });
            det.run_until_quiet(100).unwrap();
            take(&life_log);
            send(&mut det, session, &session_log, SessionInput::Published(Published { epoch: 3, identity: Some(4), generation: 0, state: PublishedState::Session, closed: false }));
            send(&mut det, session, &session_log, SessionInput::Snapshot { epoch: 3, online: true });
            send(&mut det, session, &session_log, SessionInput::Open { credential: Some(Credential { digest: 4, sequence: 0 }), pending: None, dropped_pending: false, secret: SecretState::Present(4) });
            det.post(life, EpochStamp::Control, LifecycleInput::SessionReset { reply: Some(9) });
            det.run_until_quiet(100).unwrap();
            let effects = take(&life_log);
            let erase_id = effects.iter().find_map(|effect| match effect { LifecycleEffect::ClearAccount { erase_id, .. } => Some(*erase_id), _ => None }).unwrap();
            assert!(matches!(det.state(life).unwrap().phase(), LifecyclePhase::Ending { .. }));
            let mut outputs = Vec::new();
            for action in order {
                let input = match action {
                    0 => SessionInput::EpochEnded(EpochEnded { epoch: 3, cause: Cause::SessionReset }),
                    1 => SessionInput::Published(Published { epoch: 3, identity: Some(4), generation: 0, state: PublishedState::Ending, closed: false }),
                    _ => SessionInput::SessionCall { operation: "login".into(), body: "{}".into(), reply: 1 },
                };
                outputs.extend(send(&mut det, session, &session_log, input));
            }
            if let SessionPhase::Generating { op, .. } = det.state(session).unwrap().phase().clone() {
                outputs.extend(send(&mut det, session, &session_log, SessionInput::CredentialGenerated { secret_op_id: op, credential: 42 }));
                let next = send(&mut det, session, &session_log, SessionInput::SecretStored { secret_op_id: op, ok: true });
                let batch = next.iter().find_map(|effect| match effect { SessionEffect::CommitPending { batch, .. } => Some(*batch), _ => None });
                outputs.extend(next);
                if let Some(batch) = batch {
                    outputs.extend(send(&mut det, session, &session_log, SessionInput::Rejected(Rejected { batch, reason: RejectReason::NoSession })));
                }
            }
            let completion = if failed { LifecycleInput::ClearFailed { erase_id, error: "storage".into() } } else { LifecycleInput::Cleared { erase_id } };
            det.post(life, EpochStamp::Control, completion);
            det.run_until_quiet(100).unwrap();
            take(&life_log);
            if failed {
                outputs.extend(send(&mut det, session, &session_log, SessionInput::Published(Published { epoch: 3, identity: Some(4), generation: 0, state: PublishedState::EndingFailed, closed: false })));
            }
            assert!(!outputs.iter().any(|effect| matches!(effect, SessionEffect::CredentialCommitted(_))), "{order:?}, failed={failed}: {outputs:?}");
        }
    }
}

#[test]
fn sess_prop_login_between_epoch_end_and_ending() {
    for order in [[0, 1, 2], [0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0]] {
        let mut det = DetScheduler::new(2);
        let log = new_log();
        let ops = vec![SessionOperation { name: "login".into(), path: "/login".into(), class: OperationClass::Login }];
        let session = det.add::<SessionCalls>("session", SessionState::with_operations(ops, Some(true)), Recorder::<SessionCalls>::new(&log));
        let published = Published { epoch: 3, identity: Some(4), generation: 0, state: PublishedState::Session, closed: false };
        send(&mut det, session, &log, SessionInput::Published(published.clone()));
        send(&mut det, session, &log, SessionInput::Snapshot { epoch: 3, online: true });
        send(&mut det, session, &log, SessionInput::Open { credential: Some(Credential { digest: 4, sequence: 0 }), pending: None, dropped_pending: false, secret: SecretState::Present(4) });
        let mut observed = Vec::new();
        for action in order {
            let input = match action {
                0 => SessionInput::EpochEnded(EpochEnded { epoch: 3, cause: Cause::SessionReset }),
                1 => SessionInput::SessionCall { operation: "login".into(), body: "{}".into(), reply: 1 },
                _ => SessionInput::Published(Published { state: PublishedState::Ending, ..published.clone() }),
            };
            let effects = send(&mut det, session, &log, input);
            if action == 1 {
                let kind = if effects.iter().any(|effect| matches!(effect, SessionEffect::Reply { answer: Err(_), .. })) { "refused" }
                    else if effects.iter().any(|effect| matches!(effect, SessionEffect::GenerateCredential { .. })) { "started" }
                    else { "queued" };
                let expected = if order[0] == 1 { "refused" } else if order == [0, 1, 2] { "started" } else { "queued" };
                assert_eq!(kind, expected, "{order:?}: {effects:?}");
            }
            observed.extend(effects);
            if action == 1 {
                if let Some(op) = observed.iter().find_map(|effect| match effect { SessionEffect::GenerateCredential { secret_op_id } => Some(*secret_op_id), _ => None }) {
                    let effects = send(&mut det, session, &log, SessionInput::CredentialGenerated { secret_op_id: op, credential: 42 });
                    let secret_op_id = effects.iter().find_map(|effect| match effect { SessionEffect::StoreSecret { secret_op_id, .. } => Some(*secret_op_id), _ => None }).unwrap();
                    observed.extend(effects);
                    let effects = send(&mut det, session, &log, SessionInput::SecretStored { secret_op_id, ok: true });
                    let batch = effects.iter().find_map(|effect| match effect { SessionEffect::CommitPending { batch, .. } => Some(*batch), _ => None });
                    observed.extend(effects);
                    if let Some(batch) = batch {
                        observed.extend(send(&mut det, session, &log, SessionInput::Committed(Committed { batch, revision: 1, tables: vec![], rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0, refused: Vec::new() })));
                    }
                }
            }
        }
        assert_eq!(observed.iter().any(|effect| matches!(effect, SessionEffect::HttpRequest { .. })), order == [0, 1, 2], "{order:?}: {observed:?}");
    }
}

#[test]
fn sess_login_after_epoch_end_rejected_pending() {
    let mut det = DetScheduler::new(2);
    let log = new_log();
    let ops = vec![SessionOperation { name: "login".into(), path: "/login".into(), class: OperationClass::Login }];
    let session = det.add::<SessionCalls>("session", SessionState::with_operations(ops, Some(true)), Recorder::<SessionCalls>::new(&log));
    send(&mut det, session, &log, SessionInput::Published(Published { epoch: 3, identity: Some(4), generation: 0, state: PublishedState::Session, closed: false }));
    send(&mut det, session, &log, SessionInput::Snapshot { epoch: 3, online: true });
    send(&mut det, session, &log, SessionInput::Open { credential: Some(Credential { digest: 4, sequence: 0 }), pending: None, dropped_pending: false, secret: SecretState::Present(4) });
    send(&mut det, session, &log, SessionInput::EpochEnded(EpochEnded { epoch: 3, cause: Cause::SessionReset }));
    let effects = send(&mut det, session, &log, SessionInput::SessionCall { operation: "login".into(), body: "{}".into(), reply: 1 });
    let op = effects.iter().find_map(|effect| match effect { SessionEffect::GenerateCredential { secret_op_id } => Some(*secret_op_id), _ => None }).unwrap();
    let effects = send(&mut det, session, &log, SessionInput::CredentialGenerated { secret_op_id: op, credential: 42 });
    let op = effects.iter().find_map(|effect| match effect { SessionEffect::StoreSecret { secret_op_id, .. } => Some(*secret_op_id), _ => None }).unwrap();
    let effects = send(&mut det, session, &log, SessionInput::SecretStored { secret_op_id: op, ok: true });
    let batch = effects.iter().find_map(|effect| match effect { SessionEffect::CommitPending { batch, .. } => Some(*batch), _ => None }).unwrap();
    let effects = send(&mut det, session, &log, SessionInput::Rejected(Rejected { batch, reason: RejectReason::NoSession }));
    assert!(effects.is_empty(), "Persisting x Rejected NoSession: no answer and no effect: {effects:?}");
    assert!(matches!(det.state(session).map(SessionState::phase), Some(SessionPhase::Persisting { .. })), "the phase does not change");
}
