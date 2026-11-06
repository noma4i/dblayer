use super::*;

#[test]
fn sess_prop_sequence() {
    let mut twin = Twin::new();
    for reply in 1..=3 {
        twin.feed(call(reply));
        twin.feed(committed());
        let input = twin.response(reply);
        twin.feed_all([input, committed()]);
    }
    let sequences: Vec<u64> = twin.history.iter().filter_map(|e| match e { RefEffect::CommitPending { pending, .. } => Some(pending.sequence), _ => None }).collect();
    assert_eq!(sequences, vec![1, 2, 3], "the sequence grows by one without a gap");
    assert_eq!(twin.history.iter().filter(|e| matches!(e, RefEffect::HttpRequest { .. })).count(), 3, "one request in the network per call");
}

#[test]
fn sess_prop_credential_only_committed() {
    for seed in 1..=60 {
        let twin = trace(seed, 60, |_, _, _| {});
        let committed_identities: Vec<u64> = twin.history.iter().filter_map(|e| match e { RefEffect::CredentialCommitted(c) => Some(*c), _ => None }).collect();
        if let Some(credential) = twin.reference.credential {
            assert!(credential.digest == CRED || committed_identities.contains(&credential.digest) || twin.history.is_empty(), "seed {seed}: the credential {credential:?} was never committed");
        }
    }
}

#[test]
fn sess_prop_reply_once() {
    for seed in 1..=60 {
        let mut twin = trace(seed, 60, |_, _, _| {});
        twin.feed(Input::Closing);
        let mut seen = Vec::new();
        for (id, _) in &twin.replies {
            assert!(!seen.contains(id), "seed {seed}: call {id} is answered twice");
            seen.push(*id);
        }
    }
}

#[test]
fn sess_prop_idle() {
    let mut twin = Twin::new();
    twin.feed_all([Input::Tick, Input::Online(true), Input::Foreground]);
    assert!(twin.history.is_empty(), "no pending, no request, no Tick");
}

#[test]
fn sess_prop_unauthorized_keeps_session() {
    let mut twin = landing(1);
    twin.feed(committed());
    let before = twin.reference.credential;
    twin.feed_all([call(2), committed()]);
    let input = twin.http_error(false, true);
    twin.feed_all([input, committed()]);
    assert_eq!(twin.reference.credential, before, "the credential is the same");
    assert!(!twin.history.iter().any(|e| matches!(e, RefEffect::ServerEnded { .. } | RefEffect::SessionReset)));
}

#[test]
fn sess_prop_single_flight() {
    for seed in 1..=60 {
        trace(seed, 80, |twin, input, effects| {
            let requests: Vec<_> = effects.iter().filter(|effect| matches!(effect, RefEffect::HttpRequest { .. })).collect();
            assert!(requests.len() <= 1, "seed {seed}: {input:?} starts two requests");
            if let Some(RefEffect::HttpRequest { attempt_id, .. }) = requests.first() {
                assert!(matches!(&twin.reference.state, RefState::Calling { phase: CallPhase::InFlight(now), .. } if now == attempt_id), "seed {seed}: the new attempt is the one in flight");
            }
        });
    }
    let mut twin = calling();
    let (rid, aid) = (twin.rid(), twin.aid());
    twin.feed(Input::Online(false));
    twin.feed(Input::Online(true));
    twin.feed(Input::Response { request_id: rid, attempt_id: aid, body: "{}".into(), sequence: Some(1), refusal: None });
    assert!(matches!(twin.reference.state, RefState::Calling { phase: CallPhase::InFlight(now), .. } if now != aid), "a late input of a cancelled attempt changes nothing");
}

#[test]
fn sess_prop_mutate_stamp() {
    let mut twin = persisting();
    twin.feed(mutate(7, 11));
    twin.feed(Input::Published { epoch: EPOCH + 1, identity: None, state: PubState::Session });
    assert!(matches!(twin.reference.queue.front(), Some(RefQueued::Mutate { epoch, .. }) if *epoch == EPOCH), "Published does not rebind a queued input");
}

#[test]
fn sess_prop_durable_answer() {
    let outcomes: [(Input, Fail); 4] = [
        (Input::HttpError { request_id: 11, attempt_id: 0, temporary: false, auth: false, persisted_missing: false, security: None }, Fail::Http),
        (Input::HttpError { request_id: 11, attempt_id: 0, temporary: false, auth: true, persisted_missing: false, security: None }, Fail::Unauthorized),
        (Input::HttpError { request_id: 11, attempt_id: 0, temporary: false, auth: false, persisted_missing: false, security: Some((crate::transport::SecurityCode::TlsTrust, "trust".into())) }, Fail::Security { code: crate::transport::SecurityCode::TlsTrust, detail: "trust".into() }),
        (Input::Revoked { request_id: 11, attempt_id: 0, body: "{}".into() }, Fail::Revoked),
    ];
    for (input, fail) in outcomes {
        let mut twin = calling_mutate();
        let aid = twin.aid();
        let input = match input {
            Input::HttpError { request_id, temporary, auth, persisted_missing, security, .. } => Input::HttpError { request_id, attempt_id: aid, temporary, auth, persisted_missing, security },
            Input::Revoked { request_id, body, .. } => Input::Revoked { request_id, attempt_id: aid, body },
            other => other,
        };
        let before = twin.feed(input);
        assert!(matches!(&before[..], [RefEffect::CommitLanding { answer: Some((11, Err(stored))), .. }] if *stored == fail), "{fail:?}: the outcome is stored with the removal: {before:?}");
        assert!(!before.iter().any(|effect| matches!(effect, RefEffect::Answer { .. })), "{fail:?}: the author hears it after the commit");
        assert!(twin.feed(committed()).contains(&answer(7, 11, permanent(fail))));
    }
}

#[test]
fn sess_prop_blocked_pending() {
    for seed in 1..=60 {
        trace(seed, 80, |twin, input, effects| {
            if matches!(twin.reference.state, RefState::Blocked { .. } | RefState::PendingHeld { .. }) {
                assert!(!effects.iter().any(|effect| matches!(effect, RefEffect::CommitPending { .. })), "seed {seed}: {input:?} writes a second pending beside a stored one");
            }
        });
    }
}

#[test]
fn sess_prop_retry_owner() {
    for seed in 1..=60 {
        trace(seed, 80, |twin, input, effects| {
            let author_pending = matches!(&twin.reference.state, RefState::Calling { pending, .. } | RefState::PendingHeld { pending } if pending.author.is_some());
            if author_pending {
                assert!(!effects.iter().any(|effect| matches!(effect, RefEffect::ScheduleTick { .. })), "seed {seed}: {input:?} schedules a retry for an author");
                if matches!(input, Input::Tick) { assert!(effects.is_empty(), "seed {seed}: a Tick repeats a request of an author"); }
            }
        });
    }
}

#[test]
fn sess_prop_held_slot() {
    let mut twin = held();
    twin.feed_all([call(2), mutate(7, 12), Input::Tick, Input::Online(false), Input::Online(true)]);
    assert!(!twin.history.iter().rev().take(1).any(|effect| matches!(effect, RefEffect::CommitPending { .. })));
    assert_eq!(twin.history.iter().filter(|effect| matches!(effect, RefEffect::CommitPending { .. })).count(), 1, "no other request is written while the pair of the author is stored");
    twin.feed(mutate(7, 11));
    let input = twin.response(1);
    twin.feed_all([input, committed()]);
    finish_seal(&mut twin);
    assert!(twin.history.iter().filter(|effect| matches!(effect, RefEffect::CommitPending { .. })).count() >= 2, "the slot goes on after the pair is resolved");
}

#[test]
fn sess_prop_secret_sqlite_consistent() {
    let mut twin = generating();
    let op = twin.op();
    twin.feed(Input::CredentialGenerated { op, credential: 42 });
    let op = twin.op();
    twin.feed(Input::SecretStored { op, ok: true });
    let kinds: Vec<&str> = twin.history.iter().filter_map(|effect| match effect {
        RefEffect::GenerateCredential { .. } => Some("generate"),
        RefEffect::StoreSecret { .. } => Some("store"),
        RefEffect::CommitPending { session: Some(_), .. } => Some("digest"),
        RefEffect::ClearSecret { .. } => Some("clear"),
        _ => None,
    }).collect();
    assert_eq!(kinds, ["generate", "store", "digest"], "the secret is written before the digest");
    let mut erased = clearing();
    let op = erased.op();
    assert!(erased.feed(call(5)).is_empty(), "no new secret is written before SecretCleared");
    erased.feed(Input::SecretCleared { op, ok: true });
    let kinds: Vec<&str> = erased.history.iter().filter_map(|effect| match effect {
        RefEffect::StoreSecret { .. } => Some("store"),
        RefEffect::ClearSecret { .. } => Some("clear"),
        RefEffect::GenerateCredential { .. } => Some("generate"),
        _ => None,
    }).collect();
    let clear_at = kinds.iter().position(|kind| *kind == "clear").expect("a clear");
    assert!(kinds[clear_at + 1..].iter().all(|kind| *kind != "store"), "{kinds:?}: SecretCleared is awaited before the next StoreSecret");
}
