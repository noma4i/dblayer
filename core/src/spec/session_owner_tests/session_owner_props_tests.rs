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
    let mut offline = calling();
    let sent = offline.pending();
    offline.feed_all([Input::Online(false), Input::Online(true)]);
    let mut dropped = held();
    let kept = dropped.pending();
    dropped.feed(mutate(7, 11));
    assert_eq!(http_pairs(&offline.history), vec![(sent.request_id, sent.sequence); 2], "a repeat after the network went away is the same pair");
    assert_eq!(http_pairs(&dropped.history), vec![(kept.request_id, kept.sequence); 2], "a repeat after Dropped is the same pair");
}

/// lifecycle.md BlockedPending: a crash with the pair saved. Observables: the HTTP requests and the `Commit{encrypted_pending}` of the reopened Session.
#[test]
fn sess_blocked_crash_same_pair() {
    let owner = crate::transport::RequestOwner::Operation { seq: 11 };
    let own = |req_id| Input::Mutate { author: 7, owner: Some(owner.clone()), req_id, epoch: EPOCH, path: OTP.into(), body: "{}".into(), purpose: 0 };
    for (reason, server_sequence, rejected_landing) in [("Invalid", 1, true), ("SequenceConflict", 5, false)] {
        let mut twin = Twin::new();
        twin.feed_all([own(11), committed()]);
        let saved = twin.pending();
        let response = twin.response(server_sequence);
        twin.feed(response);
        if rejected_landing { twin.feed(rejected(Reason::Invalid)); }

        let mut again = opened(open(Some((CRED, saved.sequence - 1)), Some(saved.clone()), false, Secret::Present(CRED)));
        again.feed(own(11));
        assert_eq!(http_pairs(&again.history), vec![(saved.request_id, saved.sequence)], "{reason}: the reopened Session repeats only the saved pair");
        assert!(!again.history.iter().any(|effect| matches!(effect, RefEffect::CommitPending { .. })), "{reason}: no new pair is written");
        let response = again.response(server_sequence);
        again.feed(response);
        if rejected_landing {
            assert_eq!(again.credential().map(|credential| credential.sequence), Some(saved.sequence - 1), "{reason}: the landing is not applied before its commit");
            again.feed(committed());
            assert_eq!(again.credential().map(|credential| credential.sequence), Some(saved.sequence), "{reason}: the same sequence lands");
        } else {
            let refused = again.feed(own(12));
            assert_eq!(refused, vec![answer(7, 12, permanent(Fail::SequenceConflict))], "{reason}: no new pair while blocked");
        }
        assert_eq!(http_pairs(&again.history), vec![(saved.request_id, saved.sequence)], "{reason}: one request in all");
    }
    let mut lost = opened(open(Some((CRED, 0)), None, true, Secret::Present(CRED)));
    assert_eq!(lost.feed(own(12)), vec![answer(7, 12, permanent(Fail::StateLost))], "a lost pair stays StateLost");
    assert!(http_pairs(&lost.history).is_empty());
}

fn http_pairs(history: &[RefEffect]) -> Vec<(u64, u64)> {
    history.iter().filter_map(|effect| match effect { RefEffect::HttpRequest { pending, .. } => Some((pending.request_id, pending.sequence)), _ => None }).collect()
}

fn at_attempt(input: Input, attempt: u64) -> Input {
    match input {
        Input::HttpError { request_id, temporary, auth, security, body, .. } => Input::HttpError { request_id, attempt_id: attempt, temporary, auth, security, body },
        Input::AnswerTooLarge { request_id, limit, .. } => Input::AnswerTooLarge { request_id, attempt_id: attempt, limit },
        Input::Revoked { request_id, body, author_ref, .. } => Input::Revoked { request_id, attempt_id: attempt, body, author_ref },
        other => other,
    }
}

#[test]
fn sess_prop_credential_only_committed() {
    for seed in 1..=60 {
        let twin = trace(seed, 60, |_, _, _| {});
        let committed_identities: Vec<u64> = twin.history.iter().filter_map(|e| match e { RefEffect::CredentialCommitted(c) => Some(*c), _ => None }).collect();
        if let Some(credential) = twin.credential() {
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
    let before = twin.credential();
    twin.feed_all([call(2), committed()]);
    let input = twin.http_error(false, true);
    twin.feed_all([input, committed()]);
    assert_eq!(twin.credential(), before, "the credential is the same");
    assert!(!twin.history.iter().any(|e| matches!(e, RefEffect::ServerEnded { .. } | RefEffect::SessionReset)));
}

#[test]
fn sess_prop_single_flight() {
    for seed in 1..=60 {
        trace(seed, 80, |_, input, effects| {
            let requests: Vec<_> = effects.iter().filter(|effect| matches!(effect, RefEffect::HttpRequest { .. })).collect();
            assert!(requests.len() <= 1, "seed {seed}: {input:?} starts two requests");
        });
    }
    let mut twin = calling();
    let (rid, aid) = (twin.rid(), twin.aid());
    twin.feed(Input::Online(false));
    twin.feed(Input::Online(true));
    let late = Input::Response { request_id: rid, attempt_id: aid, body: "{}".into(), sequence: Some(1), refusal: None };
    assert!(twin.feed(late).is_empty(), "a late input of a cancelled attempt changes nothing");
}

#[test]
fn sess_prop_durable_answer() {
    let outcomes: [(Input, Fail); 4] = [
        (Input::HttpError { request_id: 11, attempt_id: 0, temporary: false, auth: false, security: None, body: None }, Fail::Http),
        (Input::HttpError { request_id: 11, attempt_id: 0, temporary: false, auth: true, security: None, body: None }, Fail::Unauthorized),
        (Input::HttpError { request_id: 11, attempt_id: 0, temporary: false, auth: false, security: Some((crate::transport::SecurityCode::TlsTrust, "trust".into())), body: None }, Fail::Security { code: crate::transport::SecurityCode::TlsTrust, detail: "trust".into() }),
        (Input::Revoked { request_id: 11, attempt_id: 0, body: "{}".into(), author_ref: None }, Fail::Revoked),
    ];
    for (input, fail) in outcomes {
        let mut twin = calling_mutate();
        let before = twin.feed(at_attempt(input.clone(), twin.aid()));
        assert!(matches!(&before[..], [RefEffect::CommitLanding { answer: Some((11, Err(stored))), .. }] if *stored == fail), "{fail:?}: the outcome is stored with the removal: {before:?}");
        assert!(!before.iter().any(|effect| matches!(effect, RefEffect::Answer { .. })), "{fail:?}: the author hears it after the commit");
        let after_commit = twin.feed(committed());
        if fail == Fail::Revoked {
            assert!(after_commit.iter().any(|effect| matches!(effect, RefEffect::ServerEnded { .. })) && !after_commit.iter().any(|effect| matches!(effect, RefEffect::Answer { .. })), "Revoked: the author learns the end through the epoch, no live answer: {after_commit:?}");
        } else {
            assert!(after_commit.contains(&answer(7, 11, permanent(fail.clone()))), "{fail:?}: the author hears the stored outcome: {after_commit:?}");
        }
        let landed = twin.credential().expect("the credential");
        let mut reopened = opened(open(Some((landed.digest, landed.sequence)), None, false, Secret::Present(landed.digest)));
        reopened.feed_all([Input::Restored { epoch: EPOCH, pending: None }, Input::RestoreReady { epoch: EPOCH }, Input::Online(true), Input::Tick, Input::Foreground]);
        assert!(http_pairs(&reopened.history).is_empty() && !reopened.history.iter().any(|effect| matches!(effect, RefEffect::CommitPending { .. })), "{fail:?}: a crash after the commit leaves the stored outcome and no second request: {:?}", reopened.history);
        assert!(matches!(&reopened.feed(call(1))[..], [RefEffect::SealProbe { pending, .. }] if pending.sequence == landed.sequence + 1), "{fail:?}: the next pair is the next sequence");

        let owner = crate::transport::RequestOwner::Operation { seq: 11 };
        let own = || Input::Mutate { author: 7, owner: Some(owner.clone()), req_id: 11, epoch: EPOCH, path: OTP.into(), body: "{}".into(), purpose: 0 };
        let mut crashed = Twin::new();
        crashed.feed_all([own(), committed()]);
        let saved = crashed.pending();
        crashed.feed(at_attempt(input, crashed.aid()));
        let mut again = opened(open(Some((CRED, saved.sequence - 1)), Some(saved.clone()), false, Secret::Present(CRED)));
        again.feed(own());
        assert_eq!(http_pairs(&again.history), vec![(saved.request_id, saved.sequence)], "{fail:?}: a crash before the commit repeats the saved pair");
        assert!(!again.history.iter().any(|effect| matches!(effect, RefEffect::CommitPending { .. })), "{fail:?}: no new pair");
    }
}

#[test]
fn sess_prop_server_end_atomic() {
    for body in [serde_json::json!({"revoked": true}), serde_json::json!({"errors": [{"extensions": {"code": "SESSION_ENDED"}}]})] {
        assert_eq!(crate::transport::classify_answer(&crate::transport::WireOutcome::Answer { sequence: None, body: body.clone() }, true, true), crate::transport::AnswerKind::Revoked, "both server-end forms are a Revoked answer");
        let mut twin = calling_mutate();
        let (rid, aid) = (twin.rid(), twin.aid());
        let landing = twin.feed(Input::Revoked { request_id: rid, attempt_id: aid, body: body.to_string(), author_ref: None });
        assert!(matches!(&landing[..], [RefEffect::CommitLanding { erase_session: true, answer: Some((11, Err(Fail::Revoked))), .. }]), "{body}: the credential removal lands in the same batch as the answer: {landing:?}");
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
    every_crash_boundary_gives_one_open_outcome();
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

fn every_crash_boundary_gives_one_open_outcome() {
    #[derive(Debug, PartialEq)]
    enum Outcome { Empty, Orphan, Kept, Reset, Mismatch }
    let boundaries = [
        ("before StoreSecret", None, Secret::Missing, Outcome::Empty),
        ("after StoreSecret, before Commit{digest}", None, Secret::Present(42), Outcome::Orphan),
        ("after Commit{digest}", Some((42, 0)), Secret::Present(42), Outcome::Kept),
        ("after EraseAccount, before ClearSecret", None, Secret::Present(42), Outcome::Orphan),
        ("after ClearSecret", None, Secret::Missing, Outcome::Empty),
        ("digest without secret", Some((42, 0)), Secret::Missing, Outcome::Reset),
        ("digest differs from secret", Some((42, 0)), Secret::Present(9), Outcome::Mismatch),
    ];
    for (boundary, credential, secret, want) in boundaries {
        let mut twin = opened(open(credential, None, false, secret));
        let has = |test: fn(&RefEffect) -> bool| twin.history.iter().filter(|effect| test(effect)).count();
        let (clears, resets) = (has(|effect| matches!(effect, RefEffect::ClearSecret { .. })), has(|effect| matches!(effect, RefEffect::SessionReset)));
        let credential = twin.credential();
        let have = match (clears, resets, credential) {
            (0, 0, None) => Outcome::Empty,
            (1, 0, None) => Outcome::Orphan,
            (0, 1, None) => Outcome::Reset,
            (0, 0, Some(_)) if matches!(&twin.feed(call(1))[..], [RefEffect::SealProbe { .. }]) => Outcome::Kept,
            (0, 0, Some(credential)) if credential.digest == 42 && twin.replies.len() == 1 => Outcome::Mismatch,
            _ => panic!("{boundary}: no single outcome of the Open rows: {:?}", twin.history),
        };
        assert_eq!(have, want, "{boundary}");
        if want == Outcome::Orphan {
            let op = twin.op();
            twin.feed(Input::SecretCleared { op, ok: true });
            assert!(twin.credential().is_none(), "{boundary}: the orphan lives for one open");
            assert_eq!(twin.history.iter().filter(|effect| matches!(effect, RefEffect::ClearSecret { .. })).count(), 1, "{boundary}");
        }
    }
}
