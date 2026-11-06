use crate::owners::contract::{Committed, EpochEnded, Online, Published, PublishedState, RejectReason, Rejected};
use crate::owners::session::{Credential, OperationClass, SecretState, SessionCalls, SessionEffect, SessionInput, SessionOperation, SessionPhase, SessionState};
use crate::owners::support::{new_log, take, Log, Recorder};
use crate::runtime::{Address, Cause, DetScheduler, EpochStamp, Time};

fn send(det: &mut DetScheduler, address: Address<SessionCalls>, log: &Log<SessionEffect>, input: SessionInput) -> Vec<SessionEffect> {
    det.post(address, EpochStamp::Control, input);
    det.run_until_quiet(100).unwrap();
    take(log)
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
                let effects = send(&mut det, address, &log, SessionInput::Committed(Committed { batch, revision: 1, tables: vec![], models: Default::default() }));
                let attempt = effects.iter().find_map(|effect| match effect { SessionEffect::HttpRequest { attempt_id, .. } => Some(*attempt_id), _ => None }).unwrap();
                let input = if phase == 2 { SessionInput::Dropped { request_id: 11, attempt_id: attempt } } else { SessionInput::Response { request_id: 11, attempt_id: attempt, body: "{}".into(), sequence: Some(5), refusal: None } };
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
        send(&mut det, address, &log, SessionInput::Open { credential: None, pending: None, dropped_pending: false, secret: SecretState::Missing });
        let effects = send(&mut det, address, &log, SessionInput::SessionCall { operation: "login".into(), body: "{}".into(), reply: 1 });
        let op = match effects.as_slice() { [SessionEffect::GenerateCredential { secret_op_id }] => *secret_op_id, other => panic!("{other:?}") };
        if storing { send(&mut det, address, &log, SessionInput::CredentialGenerated { secret_op_id: op, credential: 42 }); }
        let before = det.state(address).unwrap().phase().clone();
        assert!(matches!(before, SessionPhase::Generating { .. } | SessionPhase::Storing { .. }));
        let inputs = [
            SessionInput::Online(Online(false)), SessionInput::Published(published), SessionInput::Tick(Time(0)),
            SessionInput::Response { request_id: 9, attempt_id: 9, body: "{}".into(), sequence: None, refusal: None },
            SessionInput::Revoked { request_id: 9, attempt_id: 9, body: "{}".into() },
            SessionInput::HttpError { request_id: 9, attempt_id: 9, temporary: false, auth: false, persisted_missing: false, status: 0, too_large: None },
            SessionInput::Dropped { request_id: 9, attempt_id: 9 },
            SessionInput::Committed(Committed { batch: 9, revision: 1, tables: vec![], models: Default::default() }),
            SessionInput::Rejected(Rejected { batch: 9, reason: RejectReason::NoSession }),
        ];
        for input in inputs {
            assert!(send(&mut det, address, &log, input).is_empty());
            assert_eq!(det.state(address).unwrap().phase(), &before);
        }
    }
}
