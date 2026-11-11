use super::*;

pub(super) fn restored_login(rig: &mut Rig) -> u64 {
    let pending = PendingCall { request_id: 11, sequence: 1, class: OperationClass::Login, operation: "login".into(), path: "/login".into(), body: "{}".into(), document: None, credential: Some(4), purpose: 0, author: None, owner: None, epoch: 3, reply: None };
    rig.send(SessionInput::Restored { epoch: 3, pending: Some(pending), answers: Default::default(), pending_lost: false, resume_answer: None });
    rig.start()
}

pub(super) fn commit_landing(rig: &mut Rig, effects: &[SessionEffect]) -> Vec<SessionEffect> {
    let batch = effects.iter().find_map(|effect| match effect { SessionEffect::CommitLanding { batch, .. } => Some(*batch), _ => None }).unwrap();
    rig.send(SessionInput::Committed(Committed { batch, revision: 1, tables: vec![], rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0, refused: Vec::new() }))
}

pub(super) fn login_response(rig: &mut Rig, attempt: u64) -> Vec<SessionEffect> {
    let landing = rig.send(SessionInput::Response { request_id: 11, attempt_id: attempt, body: "{}".into(), sequence: Some(1), refusal: None, kind: crate::transport::AnswerKind::Answered { sequence: Some(1) } });
    commit_landing(rig, &landing)
}

pub(super) fn login_refused(rig: &mut Rig, attempt: u64) -> Vec<SessionEffect> {
    let landing = rig.send(SessionInput::HttpError { request_id: 11, attempt_id: attempt, temporary: false, auth: false, status: 0, too_large: None, security: None, body: None });
    commit_landing(rig, &landing)
}

pub(super) fn publish(rig: &mut Rig, state: PublishedState) -> Vec<SessionEffect> {
    rig.send(SessionInput::Published(Published { epoch: 3, identity: Some(4), generation: 0, state, closed: false }))
}

pub(super) fn http_failure() -> SessionError { SessionError::Http { status: 0 } }

pub(super) fn clearing_op(effects: &[SessionEffect]) -> u64 {
    let [SessionEffect::ClearSecret { secret_op_id }] = effects else { panic!("only the secret is cleared: {effects:?}") };
    *secret_op_id
}

fn finish_take(rig: &mut Rig, effects: Vec<SessionEffect>) -> Vec<SessionEffect> {
    let [SessionEffect::CommitResumeAnswer { batch, answer: None, .. }] = effects[..] else { panic!("the outcome must be removed: {effects:?}") };
    rig.send(SessionInput::Committed(Committed { batch, revision: 1, tables: vec![], rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0, refused: Vec::new() }))
}

#[test]
fn sess_landing_restored_login() {
    let mut rig = Rig::new();
    let attempt = restored_login(&mut rig);
    rig.send(ask_answer(1));
    let landing = rig.send(SessionInput::Response { request_id: 11, attempt_id: attempt, body: "{}".into(), sequence: Some(1), refusal: None, kind: crate::transport::AnswerKind::Answered { sequence: Some(1) } });
    assert!(landing.iter().any(|effect| matches!(effect, SessionEffect::CommitLanding { resume_answer: Some(Ok(body)), .. } if body == "{}")), "{landing:?}");
    let effects = commit_landing(&mut rig, &landing);
    assert_eq!(effects, [SessionEffect::CredentialCommitted(crate::owners::contract::CredentialCommitted { identity: 4 })], "no answer before the publication");
    let state = rig.det.state(rig.address).unwrap();
    assert_eq!((state.credential().map(|credential| (credential.digest, credential.sequence)), state.awaiting_publication()), (Some((4, 1)), None));
    let effects = publish(&mut rig, PublishedState::Session);
    rig.send(SessionInput::Restored { epoch: 3, pending: None, answers: Default::default(), pending_lost: false, resume_answer: None });
    assert_eq!(finish_take(&mut rig, effects), [answer(1, Ok(Some("{}".into())))]);
}

#[test]
fn sess_landing_restored_login_failed() {
    let mut rig = Rig::new();
    let attempt = restored_login(&mut rig);
    rig.send(ask_answer(1));
    let landing = rig.send(SessionInput::HttpError { request_id: 11, attempt_id: attempt, temporary: false, auth: false, status: 0, too_large: None, security: None, body: None });
    assert!(landing.iter().any(|effect| matches!(effect, SessionEffect::CommitLanding { resume_answer: Some(Err(error)), erase_session: true, .. } if *error == http_failure())), "{landing:?}");
    let op = clearing_op(&commit_landing(&mut rig, &landing));
    assert!(rig.det.state(rig.address).unwrap().credential().is_none(), "digest and sequence of the new credential are erased");
    let effects = rig.send(SessionInput::SecretCleared { secret_op_id: op, ok: true });
    assert_eq!(finish_take(&mut rig, effects), [answer(1, Err(http_failure()))]);
}

#[test]
fn sess_restored_secret_cleared() {
    let mut rig = Rig::new();
    let attempt = restored_login(&mut rig);
    rig.send(ask_answer(1));
    rig.send(ask_answer(2));
    let op = clearing_op(&login_refused(&mut rig, attempt));
    let effects = rig.send(SessionInput::SecretCleared { secret_op_id: op, ok: true });
    assert_eq!(finish_take(&mut rig, effects), [answer(1, Err(http_failure())), answer(2, Ok(None))]);
    let mut kept = Rig::new();
    let attempt = restored_login(&mut kept);
    let op = clearing_op(&login_refused(&mut kept, attempt));
    let effects = kept.send(SessionInput::SecretCleared { secret_op_id: op, ok: true });
    assert!(effects.is_empty(), "{effects:?}");
    assert_eq!(kept.take_answer(3), [answer(3, Err(http_failure()))]);
    assert_eq!(kept.send(ask_answer(4)), [answer(4, Ok(None))]);
}

#[test]
fn sess_published_restored_login() {
    let mut rig = Rig::new();
    let attempt = restored_login(&mut rig);
    rig.send(ask_answer(1));
    rig.send(ask_answer(2));
    login_response(&mut rig, attempt);
    let effects = publish(&mut rig, PublishedState::Session);
    assert_eq!(finish_take(&mut rig, effects), [answer(1, Ok(Some("{}".into()))), answer(2, Ok(None))]);
    let mut kept = Rig::new();
    let attempt = restored_login(&mut kept);
    login_response(&mut kept, attempt);
    let effects = publish(&mut kept, PublishedState::Session);
    assert!(effects.is_empty(), "{effects:?}");
    assert_eq!(kept.take_answer(3), [answer(3, Ok(Some("{}".into())))]);
    assert_eq!(kept.send(ask_answer(4)), [answer(4, Ok(None))]);
}

#[test]
fn sess_published_restored_login_failed() {
    let not_saved = || SessionError::Storage("the session could not be saved".into());
    for (state, expected) in [(PublishedState::StorageFailed, not_saved()), (PublishedState::EndingFailed, not_saved()), (PublishedState::Ending, SessionError::NoSession)] {
        let mut rig = Rig::new();
        let attempt = restored_login(&mut rig);
        rig.send(ask_answer(1));
        rig.send(ask_answer(2));
        login_response(&mut rig, attempt);
        assert_eq!(publish(&mut rig, state), [answer(1, Err(expected.clone())), answer(2, Ok(None))]);
        let mut kept = Rig::new();
        let attempt = restored_login(&mut kept);
        login_response(&mut kept, attempt);
        let effects = publish(&mut kept, state);
        assert!(effects.is_empty(), "{effects:?}");
        assert_eq!(kept.send(ask_answer(3)), [answer(3, Err(expected))]);
    }
}
