use super::*;

#[test]
fn sess_stray_unknown_idle() {
    let mut twin = Twin::new();
    let effects = twin.feed(Input::Unknown { request_id: 73, attempt_id: 19 });
    assert!(effects.is_empty(), "a foreign answer emits no effects: {effects:?}");
    assert!(twin.replies.is_empty());
}

#[test]
fn sess_direct_call_exhausted() {
    for operation in ["social", "visit"] {
        let mut twin = Twin::new();
        twin.feed(call_of(operation, 1));
        finish_seal(&mut twin);
        twin.feed(committed());
        for attempt in 1..=40 {
            let input = twin.http_error(true, false);
            let effects = twin.feed(input);
            if attempt < 40 {
                assert!(twin.replies.is_empty(), "attempt {attempt}: {:?}", twin.replies);
                assert_eq!(effects, vec![RefEffect::ScheduleTick]);
                assert!(matches!(&twin.feed(Input::Tick)[..], [RefEffect::HttpRequest { .. }]));
            } else if operation == "visit" {
                assert!(matches!(&effects[..], [RefEffect::CommitLanding { .. }]));
                twin.feed(committed());
            } else {
                assert!(matches!(&effects[..], [RefEffect::CommitPending { pending, .. }] if pending.owner == Some(crate::transport::RequestOwner::Detached)));
                assert!(twin.replies.is_empty());
                assert_eq!(twin.feed(committed()), vec![RefEffect::ScheduleTick]);
                assert!(matches!(&twin.feed(Input::Tick)[..], [RefEffect::HttpRequest { pending, .. }] if pending.owner == Some(crate::transport::RequestOwner::Detached) && pending.reply.is_none()));
            }
        }
        assert_eq!(twin.replies, vec![(1, Err(Fail::Network))]);
    }
    for (last, expected) in [("dropped", Fail::Network), ("unknown", Fail::Network), ("transient", Fail::Graphql("TIMEOUT".into())), ("large", Fail::ResponseTooLarge { limit: 64 })] {
        let mut twin = calling();
        for attempt in 1..=40 {
            let input = if attempt < 40 { twin.http_error(true, false) } else {
                match last {
                    "dropped" => twin.dropped(),
                    "unknown" => Input::Unknown { request_id: twin.rid(), attempt_id: twin.aid() },
                    "transient" => Input::Response { request_id: twin.rid(), attempt_id: twin.aid(), body: r#"{"errors":[{"message":"timeout","extensions":{"code":"TIMEOUT"}}]}"#.into(), sequence: None, refusal: None },
                    _ => Input::AnswerTooLarge { request_id: twin.rid(), attempt_id: twin.aid(), limit: 64 },
                }
            };
            let effects = twin.feed(input);
            if attempt < 40 {
            assert_eq!(effects, vec![RefEffect::ScheduleTick]);
            assert!(matches!(&twin.feed(Input::Tick)[..], [RefEffect::HttpRequest { .. }]));
            } else {
            assert!(matches!(&effects[..], [RefEffect::CommitPending { pending, .. }] if pending.owner == Some(crate::transport::RequestOwner::Detached)));
            assert!(twin.replies.is_empty());
            assert_eq!(twin.feed(committed()), vec![RefEffect::ScheduleTick]);
            assert!(matches!(&twin.feed(Input::Tick)[..], [RefEffect::HttpRequest { pending, .. }] if pending.owner == Some(crate::transport::RequestOwner::Detached) && pending.reply.is_none()));
            }
        }
        assert_eq!(twin.replies, vec![(1, Err(expected))], "{last}");
    }
}

#[test]
fn sess_answer_too_large_other() {
    let mut sealing = Twin::new();
    sealing.feed(call(1));
    let mismatch = opened(open(Some((CRED, 6)), None, false, Secret::Present(9)));
    let unavailable = opened(open(Some((CRED, 6)), None, false, Secret::Error("locked".into())));
    let blocked = opened(open(Some((CRED, 6)), None, true, Secret::Present(CRED)));
    let mut closed = Twin::new();
    closed.feed(Input::Closing);
    for (name, mut twin) in [
        ("Idle", Twin::new()), ("Sealing", sealing), ("Generating", generating()),
        ("Storing", storing()), ("ClearingSecret", clearing()), ("SecretMismatch", mismatch),
        ("SecretUnavailable", unavailable), ("Persisting", persisting()), ("Landing", landing(1)),
        ("BlockedPending", blocked), ("Closed", closed),
    ] {
        assert!(twin.feed(Input::AnswerTooLarge { request_id: 11, attempt_id: 1, limit: 64 }).is_empty(), "{name}");
    }
}

#[test]
fn sess_calling_answer_too_large_after_offline() {
    let mut twin = calling();
    let (request_id, attempt_id) = (twin.rid(), twin.aid());
    assert_eq!(twin.feed(Input::Online(false)), vec![RefEffect::HttpCancel { attempt_id }]);
    assert!(twin.feed(Input::AnswerTooLarge { request_id, attempt_id, limit: 64 }).is_empty());
    assert!(matches!(&twin.feed(Input::Online(true))[..], [RefEffect::HttpRequest { .. }]), "the request waits for the network");
}

#[test]
fn sess_answer_too_large_table() {
    let mut read = Twin::new();
    read.feed(call_of("visit", 1));
    finish_seal(&mut read);
    read.feed(committed());
    let input = Input::AnswerTooLarge { request_id: read.rid(), attempt_id: read.aid(), limit: 64 };
    assert!(matches!(&read.feed(input)[..], [RefEffect::CommitLanding { .. }]));
    assert!(read.replies.is_empty());
    read.feed(committed());
    assert_eq!(read.replies, vec![(1, Err(Fail::ResponseTooLarge { limit: 64 }))]);

    let mut author = calling_mutate();
    let input = Input::AnswerTooLarge { request_id: author.rid(), attempt_id: author.aid(), limit: 64 };
    assert_eq!(author.feed(input), vec![answer(7, 11, Outcome::Error(Kind::Temporary, Fail::ResponseTooLarge { limit: 64 }, true))]);
    assert!(author.feed(Input::AnswerTooLarge { request_id: 11, attempt_id: 1, limit: 64 }).is_empty());
    assert!(matches!(&author.feed(mutate(7, 11))[..], [RefEffect::HttpRequest { .. }]), "the stored pair is kept for the author");

    let mut direct = calling();
    let (request_id, attempt_id) = (direct.rid(), direct.aid());
    for (rid, aid) in [(request_id + 1, attempt_id), (request_id, attempt_id + 1)] {
        assert!(direct.feed(Input::AnswerTooLarge { request_id: rid, attempt_id: aid, limit: 64 }).is_empty());
    }
    assert_eq!(direct.feed(Input::AnswerTooLarge { request_id, attempt_id, limit: 64 }), vec![RefEffect::ScheduleTick]);
    assert!(direct.feed(Input::AnswerTooLarge { request_id, attempt_id, limit: 64 }).is_empty());

    let mut secret = calling();
    let (request_id, attempt_id) = (secret.rid(), secret.aid());
    secret.feed(Input::SecretMissing { request_id, attempt_id, lost: false });
    assert!(secret.feed(Input::AnswerTooLarge { request_id, attempt_id, limit: 64 }).is_empty());
    assert!(matches!(&secret.feed(Input::Foreground)[..], [RefEffect::HttpRequest { .. }]), "the request waits for the secret");

    let mut offline = calling();
    let (request_id, attempt_id) = (offline.rid(), offline.aid());
    offline.feed(Input::Online(false));
    assert!(offline.feed(Input::AnswerTooLarge { request_id, attempt_id, limit: 64 }).is_empty());
    assert!(matches!(&offline.feed(Input::Online(true))[..], [RefEffect::HttpRequest { .. }]), "the request waits for the network");
    let mut idle = Twin::new();
    assert!(idle.feed(Input::AnswerTooLarge { request_id: 1, attempt_id: 1, limit: 64 }).is_empty());
}

#[test]
fn sess_calling_dropped_offline() {
    let mut twin = calling();
    let (rid, aid) = (twin.rid(), twin.aid());
    twin.feed(Input::Online(false));
    assert!(twin.feed(Input::Dropped { request_id: rid, attempt_id: aid }).is_empty(), "the cancelled attempt gives no outcome");
    assert!(matches!(&twin.feed(Input::Online(true))[..], [RefEffect::HttpRequest { .. }]), "the request waits for the network");
}

#[test]
fn sess_calling_offline_cancel() {
    let mut twin = calling();
    let aid = twin.aid();
    assert_eq!(twin.feed(Input::Online(false)), vec![RefEffect::HttpCancel { attempt_id: aid }]);
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
    assert_eq!(twin.feed(input), vec![RefEffect::ScheduleTick]);
}

#[test]
fn sess_calling_temporary_mutate() {
    let mut twin = calling_mutate();
    let input = twin.http_error(true, false);
    assert_eq!(twin.feed(input), vec![answer(7, 11, Outcome::Error(Kind::Temporary, Fail::Http, true))]);
    assert!(matches!(&twin.feed(mutate(7, 11))[..], [RefEffect::HttpRequest { .. }]), "the stored pair waits for the author");
}

#[test]
fn sess_calling_temporary_mutate_body() {
    let mut twin = calling_mutate();
    let body = r#"{"errors":[{"message":"Busy","extensions":{"code":"SERVICE_UNAVAILABLE"}}]}"#.to_string();
    let input = Input::HttpError { request_id: twin.rid(), attempt_id: twin.aid(), temporary: true, auth: false, security: None, body: Some(body.clone()) };
    assert_eq!(twin.feed(input), vec![answer(7, 11, Outcome::Response(body, false))]);
    assert!(matches!(&twin.feed(mutate(7, 11))[..], [RefEffect::HttpRequest { .. }]), "the stored pair waits for the author");
}

#[test]
fn sess_calling_temporary_offline() {
    let mut twin = calling();
    let (rid, aid) = (twin.rid(), twin.aid());
    twin.feed(Input::Online(false));
    let late = Input::HttpError { request_id: rid, attempt_id: aid, temporary: true, auth: false, security: None, body: None };
    assert!(twin.feed(late).is_empty(), "no timer while the network is away");
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
    assert_eq!(twin.credential().map(|credential| credential.sequence), Some(0), "a failed call spends no sequence");
}

#[test]
fn sess_calling_unknown_answer() {
    let mut online = calling();
    let (rid, aid) = (online.rid(), online.aid());
    assert_eq!(online.feed(Input::Unknown { request_id: rid, attempt_id: aid }), vec![RefEffect::ScheduleTick], "an unreadable 200 retries the same pair like Dropped");
    assert_eq!(online.credential().map(|credential| credential.sequence), Some(0), "the unknown answer spends no sequence");
    let mut author = calling_mutate();
    let (rid, aid) = (author.rid(), author.aid());
    assert_eq!(author.feed(Input::Unknown { request_id: rid, attempt_id: aid }), vec![answer(7, 11, Outcome::Dropped)], "the author hears Dropped, the pair is held");
    assert!(matches!(&author.feed(mutate(7, 11))[..], [RefEffect::HttpRequest { .. }]), "the stored pair is kept for the author to retry");
    let mut offline = calling();
    let (rid, aid) = (offline.rid(), offline.aid());
    offline.feed(Input::Online(false));
    assert!(offline.feed(Input::Unknown { request_id: rid, attempt_id: aid }).is_empty(), "offline the cancelled attempt gives no outcome");
    assert!(matches!(&offline.feed(Input::Online(true))[..], [RefEffect::HttpRequest { .. }]), "the request waits for the network");
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
    assert_eq!(twin.credential().map(|credential| credential.sequence), Some(0), "the sequence is not touched");
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
    assert!(matches!(&twin.feed(Input::Online(true))[..], [RefEffect::HttpRequest { .. }]), "the request waits for the network");
}

#[test]
fn sess_calling_secret_lost() {
    let mut twin = calling_mutate();
    let input = Input::SecretMissing { request_id: twin.rid(), attempt_id: twin.aid(), lost: true };
    assert_eq!(twin.feed(input), vec![answer(7, 11, permanent(Fail::StateLost))]);
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
    assert_eq!(twin.credential(), Some(RefCredential { digest: 42, sequence: 1 }));
}

#[test]
fn sess_landing_mutate() {
    let mut twin = calling_mutate();
    let input = twin.response(1);
    let effects = twin.feed(input);
    assert!(matches!(&effects[..], [RefEffect::CommitLanding { answer: Some((11, Ok(_))), .. }]), "the answer is stored with the landing: {effects:?}");
    assert_eq!(twin.feed(committed()), vec![answer(7, 11, Outcome::Response("{}".into(), false))]);
    assert_eq!(twin.credential(), Some(RefCredential { digest: CRED, sequence: 1 }));
}

#[test]
fn sess_landing_committed() {
    let mut twin = landing(1);
    twin.feed(committed());
    assert_eq!(twin.replies, vec![(1, Ok("{}".into()))]);
    assert_eq!(twin.credential().map(|credential| credential.sequence), Some(1));
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
fn sess_blocked_call() {
    let mut twin = blocked_invalid();
    twin.feed(call(2));
    assert_eq!(twin.replies.last(), Some(&(2, Err(Fail::Invalid))), "the reason of the block, no new pending replaces the stored one");
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
    assert_eq!(twin.feed(mutate(7, 12)), vec![answer(7, 12, permanent(Fail::SequenceConflict))], "another pair: the blocking reason, nothing retained for it");
}

#[test]
fn sess_blocked_mutate_refused() {
    let mut conflict = Twin::new();
    conflict.feed_all([mutate(7, 11), committed()]);
    let input = conflict.response(5);
    conflict.feed(input);
    assert_eq!(conflict.feed(mutate(7, 11)), vec![answer(7, 11, retained(Fail::SequenceConflict))], "the request does not leave, the pair stays saved");
    let mut invalid = Twin::new();
    invalid.feed_all([mutate(7, 11), committed()]);
    let input = invalid.response(1);
    invalid.feed_all([input, rejected(Reason::Invalid)]);
    assert_eq!(invalid.feed(mutate(7, 11)), vec![answer(7, 11, retained(Fail::Invalid))], "the request does not leave, the pair stays saved");
}

#[test]
fn sess_blocked_stays() {
    for mut twin in [{ let mut conflict = Twin::new(); conflict.feed_all([mutate(7, 11), committed()]); let input = conflict.response(5); conflict.feed(input); conflict }, blocked_invalid()] {
        assert!(twin.feed(Input::Tick).is_empty() && twin.feed(Input::Online(false)).is_empty() && twin.feed(Input::Online(true)).is_empty());
    }
}

#[test]
fn sess_blocked_epoch_ended() {
    let mut twin = blocked_invalid();
    twin.feed(call(2));
    twin.feed(ended(EndCause::SessionReset));
    assert!(twin.credential().is_none());
}

#[test]
fn sess_blocked_closing() {
    let mut twin = blocked_invalid();
    assert_eq!(twin.feed(Input::Closing), vec![RefEffect::Drained]);
}

#[test]
fn sess_idle_stray() {
    let mut twin = Twin::new();
    twin.feed_all([
        Input::Response { request_id: 1, attempt_id: 1, body: "{}".into(), sequence: Some(1), refusal: None },
        Input::HttpError { request_id: 1, attempt_id: 1, temporary: true, auth: false, security: None, body: None },
        Input::Dropped { request_id: 1, attempt_id: 1 },
        Input::Revoked { request_id: 1, attempt_id: 1, body: "{}".into(), author_ref: None },
        committed(), rejected(Reason::Storage), Input::Tick,
    ]);
    assert!(twin.history.is_empty());
}

#[test]
fn sess_revoked_other_author() {
    for mut twin in [Twin::new(), calling()] {
        let effects = twin.feed(Input::Revoked { request_id: 77, attempt_id: 88, body: "{}".into(), author_ref: Some(31) });
        let [RefEffect::CommitLanding { batch, erase_session: true, end_journal: true, answer: None, resume_answer: None, digest: None, sequence: None, .. }] = effects[..] else { panic!("the revoked answer of another author commits the session erase and end journal: {effects:?}") };
        let committed = twin.feed(Input::Committed(batch));
        assert!(matches!(&committed[..], [RefEffect::ServerEnded { .. }]), "after Committed only the server end is published, the author learns the end from its epoch (D-278): {committed:?}");
    }
}

#[test]
fn sess_published_epoch_is_taken_by_the_next_entry() {
    let mut twin = Twin::new();
    twin.feed(Input::Published { epoch: 8, identity: None, state: PubState::Session });
    let effects = twin.feed(Input::Mutate { author: 7, owner: None, req_id: 11, epoch: 8, path: OTP.into(), body: "{}".into(), purpose: 0 });
    assert!(matches!(&effects[..], [RefEffect::SealProbe { pending, .. }] if pending.epoch == 8), "{effects:?}");
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
    for (state, want) in [(PubState::StorageFailed, Fail::Storage), (PubState::EndingFailed, Fail::Storage), (PubState::Ending, Fail::NoSession)] {
        let mut twin = generating();
        let op = twin.op();
        twin.feed(Input::CredentialGenerated { op, credential: 42 });
        let op = twin.op();
        twin.feed_all([Input::SecretStored { op, ok: true }, committed()]);
        let input = twin.response(1);
        twin.feed_all([input, committed()]);
        twin.feed(Input::Published { epoch: EPOCH, identity: None, state });
        assert_eq!(twin.replies, vec![(1, Err(want))]);
    }
}

pub(super) fn revoked_and_landed() -> Twin {
    let mut twin = calling();
    let input = Input::Revoked { request_id: twin.rid(), attempt_id: twin.aid(), body: "{}".into(), author_ref: None };
    twin.feed_all([input, committed()]);
    twin
}

#[test]
fn sess_published_answers_session_end() {
    for (epoch, state, want) in [(EPOCH, PubState::Session, None), (EPOCH + 1, PubState::Ending, None), (EPOCH + 1, PubState::Session, Some(Err(Fail::Revoked))), (EPOCH + 1, PubState::NoSession, Some(Err(Fail::Revoked)))] {
        let mut twin = revoked_and_landed();
        twin.feed(Input::Published { epoch, identity: None, state });
        assert_eq!(twin.replies, want.into_iter().map(|answer| (1, answer)).collect::<Vec<_>>(), "epoch {epoch} {state:?}");
    }
}

#[test]
fn sess_published_session_end_failed() {
    for state in [PubState::StorageFailed, PubState::EndingFailed] {
        let mut twin = revoked_and_landed();
        twin.feed(Input::Published { epoch: EPOCH, identity: None, state });
        assert_eq!(twin.replies, vec![(1, Err(Fail::Storage))]);
    }
}

#[test]
fn sess_epoch_ended_keeps_session_end_answer() {
    let mut twin = revoked_and_landed();
    twin.feed(ended(EndCause::ServerEnded));
    assert!(twin.replies.is_empty(), "the answer of the ended epoch stays until the next epoch");
    twin.feed(Input::Published { epoch: EPOCH + 1, identity: None, state: PubState::Session });
    assert_eq!(twin.replies, vec![(1, Err(Fail::Revoked))]);
}

#[test]
fn sess_closing_answers_session_end() {
    let mut twin = revoked_and_landed();
    twin.feed(Input::Closing);
    assert_eq!(twin.replies, vec![(1, Err(Fail::Closed))]);
}

#[test]
fn sess_calling_no_sequence_login() {
    let mut twin = generating();
    let op = twin.op();
    twin.feed(Input::CredentialGenerated { op, credential: 42 });
    let op = twin.op();
    twin.feed_all([Input::SecretStored { op, ok: true }, committed()]);
    let refusal = r#"{"errors":[{"message":"bad","extensions":{"code":"INVALID_INPUT"}}]}"#;
    let input = Input::Response { request_id: twin.rid(), attempt_id: twin.aid(), body: refusal.into(), sequence: None, refusal: None };
    let landing = twin.feed(input);
    assert!(matches!(&landing[..], [RefEffect::CommitLanding { erase_session: true, end_journal: false, digest: None, sequence: None, .. }]), "the provisional credential is erased: {landing:?}");
    let cleared = twin.feed(committed());
    assert!(matches!(&cleared[..], [RefEffect::ClearSecret { .. }]), "{cleared:?}");
    let op = twin.op();
    twin.feed(Input::SecretCleared { op, ok: true });
    assert_eq!(twin.replies, vec![(1, Err(Fail::Graphql("INVALID_INPUT".into())))], "the caller gets the GraphQL error of the body, not Ok (wire.md: an unknown name is Graphql{{code}})");
    assert_eq!(twin.credential(), None);
    assert!(matches!(&twin.feed(call_of("login", 2))[..], [RefEffect::GenerateCredential { .. }]), "the next sign-in goes as the first");
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
    assert!(matches!(&landing[..], [RefEffect::CommitLanding { erase_session: true, end_journal: false, answer: None, .. }]), "{landing:?}");
    let cleared = twin.feed(committed());
    assert!(matches!(&cleared[..], [RefEffect::ClearSecret { .. }]), "{cleared:?}");
    assert!(twin.replies.is_empty(), "the call hears the error after the secret is gone");
    let op = twin.op();
    twin.feed(Input::SecretCleared { op, ok: true });
    assert_eq!(twin.replies, vec![(1, Err(Fail::Http))]);
    assert_eq!(twin.credential(), None);
    assert!(matches!(&twin.feed(call_of("login", 2))[..], [RefEffect::GenerateCredential { .. }]), "the next sign-in goes as the first");
}

#[test]
fn sess_published_ending_failed() {
    let mut twin = Twin::new();
    twin.feed(published(PubState::Ending));
    twin.feed(call(1));
    twin.feed(published(PubState::EndingFailed));
    assert_eq!(twin.replies, vec![(1, Err(Fail::Storage))]);
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
        assert!(effects.contains(&RefEffect::HttpCancel { attempt_id: aid }) && effects.contains(&answer(7, 11, permanent(Fail::NoSession))));
        assert_eq!(twin.replies, vec![(2, Err(Fail::NoSession))]);
        assert!(twin.credential().is_none());
    }
    let mut twin = persisting();
    twin.feed(ended(EndCause::SessionReset));
    assert_eq!(twin.replies, vec![(1, Err(Fail::NoSession))]);
    let mut twin = landing(1);
    twin.feed(ended(EndCause::SessionReset));
    assert_eq!(twin.replies, vec![(1, Err(Fail::NoSession))]);
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
    assert!(twin.credential().is_none());
    let mut other = Twin::new();
    other.feed(ended(EndCause::Other));
    assert!(other.credential().is_some());
}

#[test]
fn sess_closing() {
    for mut twin in [Twin::new(), persisting(), calling(), landing(1), held(), blocked_invalid()] {
        twin.feed(call(7));
        twin.feed(Input::Closing);
        assert!(twin.history.contains(&RefEffect::Drained));
    }
}

#[test]
fn sess_prop_answer_kinds_table() {
    let mut answered = landing(1);
    answered.feed(committed());
    assert_eq!((answered.replies.clone(), answered.credential().map(|credential| credential.sequence)), (vec![(1, Ok("{}".into()))], Some(1)), "answered: Ok, the client sequence is the server number");
    assert!(matches!(&answered.feed(call(2))[..], [RefEffect::SealProbe { .. }]), "answered: the queue is not blocked");
    let mut not_consumed = calling();
    let input = Input::Response { request_id: not_consumed.rid(), attempt_id: not_consumed.aid(), body: "{}".into(), sequence: None, refusal: None };
    not_consumed.feed_all([input, committed()]);
    assert_eq!((not_consumed.replies.clone(), not_consumed.credential().map(|credential| credential.sequence)), (vec![(1, Ok("{}".into()))], Some(0)), "not_consumed: the number is not spent");
    for (code, fail) in [(SessionCode::RateLimited, Fail::Session(SessionCode::RateLimited)), (SessionCode::Unauthenticated, Fail::Unauthorized), (SessionCode::AccountUnavailable, Fail::Session(SessionCode::AccountUnavailable))] {
        let mut twin = calling();
        let input = Input::Response { request_id: twin.rid(), attempt_id: twin.aid(), body: "{}".into(), sequence: Some(1), refusal: Some(code) };
        twin.feed_all([input, committed()]);
        assert_eq!((twin.replies.clone(), twin.credential().map(|credential| credential.sequence)), (vec![(1, Err(fail))], Some(1)), "refused {code:?}: the confirmed refusal spends the number");
        assert!(matches!(&twin.feed(call(2))[..], [RefEffect::SealProbe { .. }]), "refused {code:?}: not blocked");
    }
    let mut revoked = calling();
    let input = Input::Revoked { request_id: revoked.rid(), attempt_id: revoked.aid(), body: "{}".into(), author_ref: None };
    revoked.feed_all([input, committed()]);
    revoked.feed(Input::Published { epoch: EPOCH + 1, identity: None, state: PubState::NoSession });
    assert_eq!((revoked.replies.clone(), revoked.credential().map(|credential| credential.sequence)), (vec![(1, Err(Fail::Revoked))], Some(0)), "revoked: no number spent");
    let mut transport = calling();
    let dropped = transport.dropped();
    assert_eq!(transport.feed(dropped), vec![RefEffect::ScheduleTick], "transport: the lost attempt retries the pair");
    assert_eq!(transport.credential().map(|credential| credential.sequence), Some(0), "transport: nothing is spent");
    let mut conflict = calling();
    let input = conflict.response(5);
    conflict.feed(input);
    assert_eq!(conflict.replies.last(), Some(&(1, Err(Fail::SequenceConflict))));
    assert!(conflict.feed(call(2)).is_empty(), "a mismatched sequence is the only outcome that blocks the queue");
    let mut login = generating();
    let op = login.op();
    login.feed(Input::CredentialGenerated { op, credential: 42 });
    let op = login.op();
    login.feed_all([Input::SecretStored { op, ok: true }, committed()]);
    let input = login.response(1);
    login.feed(input);
    assert_eq!(login.feed(committed()), vec![RefEffect::CredentialCommitted(42)], "login answered: the credential commits at the server number");
    assert_eq!(login.credential(), Some(RefCredential { digest: 42, sequence: 1 }));
    let mut sessionless = Twin::new();
    sessionless.feed(call_of("visit", 9));
    finish_seal(&mut sessionless);
    sessionless.feed(committed());
    let input = Input::Response { request_id: sessionless.rid(), attempt_id: sessionless.aid(), body: "{}".into(), sequence: None, refusal: None };
    sessionless.feed_all([input, committed()]);
    assert_eq!((sessionless.replies.clone(), sessionless.credential().map(|credential| credential.sequence)), (vec![(9, Ok("{}".into()))], Some(0)), "sessionless: Ok, the number is never spent");
    let mut applied = calling_mutate();
    let input = applied.response(1);
    applied.feed(input);
    assert_eq!(applied.feed(committed()), vec![answer(7, 11, Outcome::Response("{}".into(), false))], "an applied mutation is answered, not rolled back");
    assert_eq!(applied.credential().map(|credential| credential.sequence), Some(1), "the applied mutation spent the number");
}

