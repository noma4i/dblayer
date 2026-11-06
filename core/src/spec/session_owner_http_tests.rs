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
    finish_seal(&mut twin);
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
    assert!(matches!(&effects[..], [RefEffect::SealProbe { .. }]), "the call waited for the publication: {effects:?}");
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

