use super::*;

#[test]
fn sess_sealing_failed() {
    let mut twin = Twin::new();
    let effects = twin.feed(call(1));
    let [RefEffect::SealProbe { seal_id, .. }] = effects.as_slice() else { panic!("{effects:?}") };
    twin.feed(Input::SealFailed { seal_id: *seal_id, error: Fail::StateLost });
    assert_eq!(twin.replies, [(1, Err(Fail::StateLost))]);
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
    assert!(matches!(&effects[..], [RefEffect::SealProbe { pending, .. }] if pending.sequence == 1 && pending.class == Class::Authenticated && pending.epoch == EPOCH));
}

#[test]
fn sess_idle_call_sessionless() {
    let mut twin = Twin::new();
    let effects = twin.feed(call_of("visit", 1));
    assert!(matches!(&effects[..], [RefEffect::SealProbe { pending, .. }] if pending.sequence == 0 && pending.class == Class::Sessionless));
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
    assert!(matches!(&effects[..], [RefEffect::SealProbe { pending, .. }] if pending.class == Class::Logout && pending.sequence == 1));
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
fn sess_idle_mutate_closed() {
    for closed in [false, true] {
        let mut twin = Twin::new();
        let published = Published { epoch: EPOCH, identity: None, generation: 0, state: PublishedState::Closing, closed };
        twin.det.post(twin.addr, EpochStamp::Control, SessionInput::Published(published));
        twin.det.post(twin.addr, EpochStamp::Control, SessionInput::Mutate { author: 7, owner: None, req_id: 11, epoch: EPOCH + 1, path: OTP.into(), body: "{}".into(), purpose: 0 });
        twin.det.run_until_quiet(100).unwrap();
        let effects = take(&twin.log);
        assert!(matches!(effects.as_slice(), [SessionEffect::Answer { author: 7, req_id: 11, outcome: AuthorOutcome::Error { class: ErrorClass::Permanent, error: SessionError::Closed, .. } }]));
    }
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
    assert!(matches!(&effects[..], [RefEffect::SealProbe { pending, .. }] if pending.request_id == 11 && pending.sequence == 1 && pending.author == Some(7) && pending.reply.is_none() && pending.epoch == EPOCH));
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
    twin.feed(Input::Snapshot { epoch: EPOCH + 1, online: true });
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
    assert!(matches!(&effects[..], [RefEffect::HttpRequest { .. }]));
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
    assert!(matches!(twin.reference.state, RefState::Sealing { .. }), "the next entry of the queue starts");
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
    assert!(twin.replies.is_empty(), "the answer waits for the epoch after the one that ended");
    assert_eq!(twin.reference.awaiting(), Some((1, Some(EPOCH))));
    twin.feed(Input::Published { epoch: EPOCH + 1, identity: None, state: PubState::Session });
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
    assert!(twin.replies.is_empty(), "the logout answer waits for the new epoch");
    twin.feed(Input::Published { epoch: EPOCH + 1, identity: None, state: PubState::Session });
    assert_eq!(twin.replies, vec![(1, Ok(r#"{"revoked":true}"#.into()))], "a revoked answer to logout confirms the logout");
}

#[test]
fn sess_landing_server_ended_held() {
    let mut twin = calling();
    let input = Input::Revoked { request_id: twin.rid(), attempt_id: twin.aid(), body: "{}".into() };
    twin.feed(input);
    let done = twin.feed(committed());
    assert!(done.contains(&RefEffect::ServerEnded { epoch: EPOCH }));
    assert!(twin.replies.is_empty());
    assert_eq!(twin.reference.awaiting(), Some((1, Some(EPOCH))), "the session call holds its answer until the epoch passes");
}

#[test]
fn sess_landing_server_ended_mutate_answers() {
    let mut twin = Twin::new();
    twin.feed_all([mutate(7, 11), committed()]);
    let input = Input::Revoked { request_id: twin.rid(), attempt_id: twin.aid(), body: "{}".into() };
    twin.feed(input);
    let done = twin.feed(committed());
    assert!(done.contains(&RefEffect::ServerEnded { epoch: EPOCH }));
    assert!(twin.reference.awaiting().is_none(), "a mutate answers at once");
}

#[test]
fn sess_calling_sequence_conflict() {
    let mut twin = Twin::new();
    twin.feed_all([mutate(7, 11), committed()]);
    let input = twin.response(9);
    let effects = twin.feed(input);
    assert_eq!(effects, vec![answer(7, 11, retained(Fail::SequenceConflict))], "the author learns the pair stays saved");
    assert!(matches!(twin.reference.state, RefState::Blocked { reason: Block::SequenceConflict, .. }));
    assert_eq!(twin.reference.credential, Some(RefCredential { digest: CRED, sequence: 0 }));
}

#[test]
fn sess_calling_no_sequence() {
    let mut twin = calling();
    let credential = twin.reference.credential;
    let input = Input::Response { request_id: twin.rid(), attempt_id: twin.aid(), body: "{}".into(), sequence: None, refusal: None };
    let effects = twin.feed(input);
    assert!(matches!(&effects[..], [RefEffect::CommitLanding { digest: None, sequence: None, .. }]), "the pending goes, the number stays: {effects:?}");
    twin.feed(committed());
    assert_eq!(twin.replies, vec![(1, Ok("{}".into()))], "the answer goes to the caller");
    assert_eq!(twin.reference.credential, credential, "credential.sequence is not spent");
    let mut author = calling_mutate();
    let input = Input::Response { request_id: author.rid(), attempt_id: author.aid(), body: "{}".into(), sequence: None, refusal: None };
    author.feed(input);
    let done = author.feed(committed());
    assert!(done.contains(&answer(7, 11, Outcome::Response("{}".into()))), "the author gets the answer: {done:?}");
    assert!(matches!(author.reference.state, RefState::Idle), "no pending is held: {:?}", author.reference.state);
}

#[test]
fn sess_calling_document_missing() {
    let document = "mutation Send { send { id } }";
    let full = crate::transport::encrypted::graphql_body(&crate::transport::GraphQLRequest::new(document, "Send", serde_json::json!({})), true).to_string();
    let missing = serde_json::json!({ "errors": [{ "message": "PERSISTED_QUERY_NOT_FOUND", "extensions": { "code": "PERSISTED_QUERY_NOT_FOUND" } }] }).to_string();
    let mut twin = Twin::new();
    let input = Input::Mutate { author: 7, owner: None, req_id: 11, epoch: EPOCH, path: OTP.into(), body: full.clone(), purpose: 0 };
    twin.feed_all([input, committed()]);
    let first = twin.pending();
    assert!(!first.body.contains("\"query\""), "the first pair is hash-only: {}", first.body);
    let credential = twin.reference.credential;
    let input = Input::Response { request_id: twin.rid(), attempt_id: twin.aid(), body: missing.clone(), sequence: None, refusal: None };
    let effects = twin.feed(input);
    let [RefEffect::CommitPending { pending, session: None, .. }] = effects.as_slice() else { panic!("one Commit, no answer to the caller: {effects:?}") };
    assert_eq!((pending.sequence, pending.body.as_str()), (first.sequence, full.as_str()), "the same number, the full document");
    assert!(twin.replies.is_empty());
    let sent = twin.feed(committed());
    assert!(matches!(&sent[..], [RefEffect::HttpRequest { pending, .. }] if pending.body == full), "{sent:?}");
    assert_eq!(twin.reference.credential, credential, "the number is not spent");
    let input = Input::Response { request_id: twin.rid(), attempt_id: twin.aid(), body: missing, sequence: None, refusal: None };
    let effects = twin.feed(input);
    assert!(matches!(&effects[..], [RefEffect::CommitLanding { digest: None, sequence: None, .. }]), "a full pair that still misses its document is final: {effects:?}");
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
            assert!(after.contains(&answer(7, 11, permanent(Fail::Unauthorized))), "{after:?}");
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
        Input::HttpError { request_id: rid, attempt_id: aid + 50, temporary: false, auth: true, security: None, body: None },
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

