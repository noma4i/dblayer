use super::*;

#[test]
fn sess_closed() {
    let mut twin = Twin::new();
    twin.feed_all([Input::Closing, call(1), mutate(7, 11), Input::Tick, Input::Online(true), Input::Closing]);
    assert_eq!(twin.replies, vec![(1, Err(Fail::Closed))]);
}

#[test]
fn sess_open_sends_nothing_without_a_pair_for_an_author() {
    let login = RefPending { class: Class::Login, credential: Some(CRED), sequence: 1, ..stored(None, 5, 1) };
    for (credential, pending) in [(None, None), (Some((CRED, 6)), Some(stored(None, 5, 7))), (Some((CRED, 0)), Some(login))] {
        let secret = if credential.is_some() { Secret::Present(CRED) } else { Secret::Missing };
        let twin = opened(open(credential, pending, false, secret));
        assert!(twin.history.is_empty(), "Session sends nothing before Restored: {:?}", twin.history);
    }
}

#[test]
fn sess_open_restored() {
    let mut twin = opened(open(Some((CRED, 6)), None, false, Secret::Present(CRED)));
    assert!(matches!(&twin.feed(call(1))[..], [RefEffect::SealProbe { pending, .. }] if pending.sequence == 7));
}

#[test]
fn sess_open_pending() {
    let owner = crate::transport::RequestOwner::Operation { seq: 3 };
    let mut twin = opened(open(Some((CRED, 6)), Some(stored(Some(owner.clone()), 3, 7)), false, Secret::Present(CRED)));
    assert!(twin.history.is_empty(), "Session sends nothing itself");
    let repeated = twin.feed(Input::Mutate { author: OUTBOX_AUTHOR, owner: Some(owner), req_id: 3, epoch: EPOCH, path: OTP.into(), body: "{}".into(), purpose: 0 });
    assert!(matches!(&repeated[..], [RefEffect::HttpRequest { pending, .. }] if pending.request_id == 3 && pending.sequence == 7 && pending.author == Some(OUTBOX_AUTHOR)), "the author repeats the stored pair: {repeated:?}");
}

#[test]
fn sess_open_pending_transfer_replays_its_pair_for_the_same_step() {
    let owner = crate::transport::RequestOwner::Transfer { operation_id: 4, item_id: "key-1".into(), step: crate::owners::write::upload::UploadStage::Create };
    let mut twin = opened(open(Some((CRED, 6)), Some(stored(Some(owner.clone()), 3, 7)), false, Secret::Present(CRED)));
    let effects = twin.feed(Input::Mutate { author: 9, owner: Some(owner), req_id: 3, epoch: EPOCH, path: OTP.into(), body: "{\"other\":1}".into(), purpose: 0 });
    assert!(matches!(&effects[..], [RefEffect::HttpRequest { pending, .. }] if pending.author == Some(9) && pending.request_id == 3 && pending.body == "{}"), "the saved pair goes out again for the new asker, the new body does not: {effects:?}");
}

#[test]
fn sess_open_pending_lost() {
    let mut twin = opened(open(Some((CRED, 6)), None, true, Secret::Present(CRED)));
    twin.feed(call(1));
    assert_eq!(twin.replies, vec![(1, Err(Fail::StateLost))]);
}

#[test]
fn sess_open_secret_unavailable() {
    let mut twin = opened(open(Some((CRED, 6)), None, false, Secret::Error("locked".into())));
    twin.feed(call(1));
    assert_eq!(twin.replies, vec![(1, Err(Fail::Storage))]);
}

#[test]
fn sess_open_orphan_secret() {
    let twin = opened(open(None, None, false, Secret::Present(9)));
    assert!(matches!(twin.history[..], [RefEffect::ClearSecret { .. }]), "the orphan secret is erased: {:?}", twin.history);
}

#[test]
fn sess_open_digest_without_secret() {
    let twin = opened(open(Some((CRED, 6)), None, false, Secret::Missing));
    assert_eq!(twin.history, vec![RefEffect::SessionReset]);
    assert!(twin.credential().is_none());
}

#[test]
fn sess_open_digest_mismatch() {
    let mut twin = opened(open(Some((CRED, 6)), None, false, Secret::Present(9)));
    twin.feed_all([call(1), mutate(7, 11)]);
    assert_eq!(twin.replies, vec![(1, Err(Fail::StateLost))]);
    assert_eq!(twin.credential().map(|credential| credential.digest), Some(CRED), "the credential is not replaced");
}

#[test]
fn sess_restore_failed() {
    let mut twin = Twin::new();
    assert_eq!(twin.feed(Input::RestoreFailed { epoch: EPOCH, error: "disk".into() }), vec![RefEffect::RestoreClassified { epoch: EPOCH }]);
    twin.feed(call(1));
    assert_eq!(twin.replies, vec![(1, Err(Fail::Storage))]);
}

#[test]
fn sess_restore_failed_refuses() {
    let mut twin = Twin::new();
    twin.feed(Input::RestoreFailed { epoch: EPOCH, error: "disk".into() });
    assert_eq!(twin.feed(mutate(7, 11)), vec![answer(7, 11, permanent(Fail::Storage))]);
    assert_eq!(twin.credential().map(|credential| credential.sequence), Some(0));
}

#[test]
fn sess_restore_failed_stale() {
    let mut twin = Twin::new();
    assert!(twin.feed(Input::RestoreFailed { epoch: EPOCH - 1, error: "disk".into() }).is_empty());
    assert!(twin.feed(Input::RestoreFailed { epoch: EPOCH + 1, error: "disk".into() }).is_empty());
    assert!(matches!(twin.feed(call(1)).as_slice(), [RefEffect::SealProbe { .. }]));
}

#[test]
fn sess_restore_failed_reset() {
    let mut twin = Twin::new();
    twin.feed(Input::RestoreFailed { epoch: EPOCH, error: "disk".into() });
    twin.feed(ended(EndCause::SessionReset));
    twin.feed(Input::Published { epoch: EPOCH + 1, identity: None, state: PubState::NoSession });
    assert!(matches!(twin.feed(call(1)).as_slice(), [RefEffect::GenerateCredential { .. }]));
}

#[test]
fn sess_restored_direct_pending() {
    let mut twin = Twin::new();
    let effects = twin.feed(Input::Restored { epoch: EPOCH, pending: Some(stored(None, 5, 1)) });
    assert_eq!(effects, vec![RefEffect::RestoreClassified { epoch: EPOCH }]);
}

#[test]
fn sess_restore_ready_direct() {
    let mut twin = Twin::new();
    twin.feed_all([Input::Restored { epoch: EPOCH, pending: Some(stored(None, 5, 1)) }]);
    let effects = twin.feed(Input::RestoreReady { epoch: EPOCH });
    assert!(matches!(&effects[..], [RefEffect::HttpRequest { pending, .. }] if pending.request_id == 5 && pending.reply.is_none()));
    let input = twin.response(1);
    twin.feed_all([input, committed()]);
    assert!(twin.replies.is_empty(), "an answer without a caller shows nothing");
    assert_eq!(twin.credential().map(|credential| credential.sequence), Some(1));
}

fn transfer_owner() -> crate::transport::RequestOwner {
    crate::transport::RequestOwner::Transfer { operation_id: 4, item_id: "key-1".into(), step: crate::owners::write::upload::UploadStage::Create }
}

fn orphan_owner() -> crate::transport::RequestOwner { crate::transport::RequestOwner::Orphan { upload_id: "u1".into() } }

#[test]
fn sess_restore_ready_transfer_pending() {
    let mut twin = Twin::new();
    assert_eq!(twin.feed(Input::Restored { epoch: EPOCH, pending: Some(stored(Some(transfer_owner()), 5, 1)) }), vec![RefEffect::RestoreClassified { epoch: EPOCH }]);
    let effects = twin.feed(Input::RestoreReady { epoch: EPOCH });
    assert!(matches!(&effects[..], [RefEffect::HttpRequest { pending, .. }] if pending.request_id == 5 && pending.sequence == 1 && pending.reply.is_none() && pending.author == Some(TRANSFER_AUTHOR)), "the stored pair goes out byte for byte: {effects:?}");
    let input = twin.response(1);
    let landing = twin.feed(input);
    assert!(matches!(&landing[..], [RefEffect::CommitLanding { answer: Some((5, Ok(_))), .. }]), "the pair answer is stored for the Transfer author: {landing:?}");
    let committed = twin.feed(committed());
    assert_eq!(committed, vec![answer(TRANSFER_AUTHOR, 5, Outcome::Response("{}".into(), false))], "the pair answer reaches the Transfer author, not a call reply");
    assert!(twin.replies.is_empty(), "no call reply");
    assert_eq!(twin.credential().map(|credential| credential.sequence), Some(1));
}

#[test]
fn sess_restore_ready_other() {
    let mut twin = persisting();
    twin.feed(Input::Restored { epoch: EPOCH, pending: Some(stored(None, 5, 1)) });
    assert!(twin.feed(Input::RestoreReady { epoch: EPOCH }).is_empty());
    let mut idle = Twin::new();
    assert!(idle.feed(Input::RestoreReady { epoch: EPOCH }).is_empty());
    let mut stale = Twin::new();
    stale.feed(Input::Restored { epoch: EPOCH, pending: Some(stored(None, 5, 1)) });
    assert!(stale.feed(Input::RestoreReady { epoch: EPOCH - 1 }).is_empty());
    assert!(stale.feed(Input::RestoreReady { epoch: EPOCH + 1 }).is_empty());
    assert!(matches!(stale.feed(Input::RestoreReady { epoch: EPOCH }).as_slice(), [RefEffect::HttpRequest { .. }]));
}

#[test]
fn sess_restored_direct_on_idle() {
    let mut busy = persisting();
    busy.feed_all([Input::RestoreReady { epoch: EPOCH }, Input::Restored { epoch: EPOCH, pending: Some(stored(None, 5, 2)) }]);
    busy.feed(committed());
    let input = busy.response(1);
    busy.feed(input);
    let effects = busy.feed(committed());
    assert_eq!(effects.iter().filter(|effect| matches!(effect, RefEffect::HttpRequest { pending, .. } if pending.request_id == 5 && pending.sequence == 2)).count(), 1);
}

#[test]
fn sess_restored_direct_dropped() {
    let mut twin = Twin::new();
    twin.feed(Input::Restored { epoch: EPOCH, pending: Some(stored(None, 5, 1)) });
    twin.feed(ended(EndCause::Other));
    assert!(twin.feed(Input::RestoreReady { epoch: EPOCH }).is_empty(), "an ended epoch drops the stored pair");
    let mut closing = Twin::new();
    closing.feed_all([Input::Restored { epoch: EPOCH, pending: Some(stored(None, 5, 1)) }, Input::Closing]);
    assert!(closing.feed(Input::RestoreReady { epoch: EPOCH }).is_empty(), "closing drops the stored pair");
}

#[test]
fn sess_restored_late_epoch() {
    let mut twin = Twin::new();
    twin.feed_all([Input::Restored { epoch: EPOCH, pending: Some(stored(None, 5, 1)) }, Input::RestoreReady { epoch: EPOCH }]);
    twin.feed(ended(EndCause::SessionReset));
    twin.feed(Input::Published { epoch: EPOCH + 1, identity: None, state: PubState::NoSession });
    let late = twin.feed(Input::Restored { epoch: EPOCH, pending: Some(stored(None, 5, 1)) });
    assert_eq!(late, vec![RefEffect::RestoreClassified { epoch: EPOCH }], "the pending of the ended epoch starts no request");
    assert!(twin.feed(Input::RestoreReady { epoch: EPOCH }).is_empty(), "the permission of the ended epoch is dropped");
}

#[test]
fn sess_snapshot_late_epoch() {
    let mut twin = Twin::offline();
    twin.feed(ended(EndCause::SessionReset));
    twin.feed(Input::Published { epoch: EPOCH + 1, identity: None, state: PubState::NoSession });
    assert!(twin.feed(Input::Snapshot { epoch: EPOCH, online: true }).is_empty());
    twin.feed(call_of("visit", 1));
    finish_seal(&mut twin);
    assert!(twin.feed(committed()).is_empty(), "the snapshot of the ended epoch changes nothing");
    assert!(matches!(&twin.feed(Input::Snapshot { epoch: EPOCH + 1, online: true })[..], [RefEffect::HttpRequest { .. }]));
}

#[test]
fn sess_idle_call_awaiting_publication() {
    let mut twin = generating();
    let op = twin.op();
    twin.feed(Input::CredentialGenerated { op, credential: 42 });
    let op = twin.op();
    twin.feed_all([Input::SecretStored { op, ok: true }, committed()]);
    let input = twin.response(1);
    twin.feed_all([input, committed()]);
    assert!(twin.feed(call(2)).is_empty(), "a new call waits for the publication of the credential");
    assert!(twin.feed(mutate(7, 11)).is_empty(), "so does a Mutate");
    assert_eq!(twin.history.iter().filter(|effect| matches!(effect, RefEffect::CommitPending { .. })).count(), 1);
}

#[test]
fn sess_restored_other() {
    for mut twin in [persisting(), generating(), held(), blocked_invalid(), opened(open(Some((CRED, 6)), None, false, Secret::Present(9)))] {
        let effects = twin.feed(Input::Restored { epoch: EPOCH, pending: Some(stored(None, 5, 1)) });
        assert_eq!(effects, vec![RefEffect::RestoreClassified { epoch: EPOCH }]);
    }
}

#[test]
fn sess_foreground_other() {
    for mut twin in [Twin::new(), persisting(), calling(), landing(1), held(), blocked_invalid(), generating()] {
        assert!(twin.feed(Input::Foreground).is_empty());
    }
}

#[test]
fn sess_mismatch_call() {
    let mut twin = opened(open(Some((CRED, 6)), None, false, Secret::Present(9)));
    for operation in ["social", "logout", "login"] { twin.feed(call_of(operation, 1)); }
    assert!(twin.replies.iter().all(|(_, answer)| *answer == Err(Fail::StateLost)));
}

#[test]
fn sess_mismatch_sessionless() {
    for secret in [Secret::Present(9), Secret::Error("locked".into())] {
        let mut twin = opened(open(Some((CRED, 6)), None, false, secret));
        let effects = twin.feed(call_of("visit", 1));
        assert!(matches!(&effects[..], [RefEffect::SealProbe { pending, .. }] if pending.class == Class::Sessionless));
        twin.feed(committed());
        let input = Input::Response { request_id: twin.rid(), attempt_id: twin.aid(), body: "{}".into(), sequence: None, refusal: None };
        twin.feed_all([input, committed()]);
        assert_eq!(twin.replies, vec![(1, Ok("{}".into()))]);
    }
}

#[test]
fn sess_mismatch_mutate() {
    let mut twin = opened(open(Some((CRED, 6)), None, false, Secret::Present(9)));
    assert_eq!(twin.feed(mutate(7, 11)), vec![answer(7, 11, permanent(Fail::StateLost))]);
}

#[test]
fn sess_mismatch_epoch_ended() {
    for secret in [Secret::Present(9), Secret::Error("locked".into())] {
        let mut twin = opened(open(Some((CRED, 6)), None, false, secret));
        twin.feed(ended(EndCause::SessionReset));
        assert!(twin.credential().is_none());
    }
}

#[test]
fn sess_mismatch_closing() {
    for secret in [Secret::Present(9), Secret::Error("locked".into())] {
        let mut twin = opened(open(Some((CRED, 6)), None, false, secret));
        assert_eq!(twin.feed(Input::Closing), vec![RefEffect::Drained]);
    }
}

#[test]
fn sess_mismatch_other() {
    for secret in [Secret::Present(9), Secret::Error("locked".into())] {
        let mut twin = opened(open(Some((CRED, 6)), None, false, secret));
        for input in [Input::Online(false), Input::Tick, published(PubState::Session), Input::Cancel { req_id: 3 }, ended(EndCause::Other)] {
            assert!(twin.feed(input).is_empty());
        }
        twin.feed(call(1));
        assert_eq!(twin.replies.len(), 1, "the call is still refused");
    }
}

/// sess_stored_pair_of_a_transfer_gets_the_state_outcome: a state that keeps the stored pair of a transfer without driving it answers the transfer once with the outcome of its `Mutate` row, the pair stays saved.
#[test]
fn sess_stored_pair_of_a_transfer_gets_the_state_outcome() {
    let restore = |twin: &mut Twin| {
        let mut effects = twin.feed(Input::Restored { epoch: EPOCH, pending: Some(stored(Some(transfer_owner()), 5, 7)) });
        effects.extend(twin.feed(Input::RestoreReady { epoch: EPOCH }));
        effects
    };
    let expected = vec![answer(TRANSFER_AUTHOR, 5, retained(Fail::StateLost))];
    let answers = |effects: &[RefEffect]| effects.iter().filter(|effect| matches!(effect, RefEffect::Answer { .. })).cloned().collect::<Vec<_>>();
    let mut at_open = opened(open(Some((CRED, 6)), None, false, Secret::Present(9)));
    assert_eq!(answers(&restore(&mut at_open)), expected, "SecretMismatch at the open");
    let mut after_read = opened(open(Some((CRED, 6)), None, false, Secret::Error("locked".into())));
    assert!(answers(&restore(&mut after_read)).is_empty(), "SecretUnavailable waits for the read");
    after_read.feed(Input::Foreground);
    let op = after_read.op();
    assert_eq!(answers(&after_read.feed(Input::SecretRead { op, read: Secret::Present(9) })), expected, "SecretMismatch after the read");
    assert!(answers(&after_read.feed(Input::Foreground)).is_empty(), "once");
    let mut blocked = opened(open(Some((CRED, 6)), None, true, Secret::Present(CRED)));
    assert_eq!(answers(&restore(&mut blocked)), expected, "BlockedPending");
}

#[test]
fn sess_unavailable_call() {
    let mut twin = opened(open(Some((CRED, 6)), None, false, Secret::Error("locked".into())));
    for operation in ["social", "logout", "login"] { twin.feed(call_of(operation, 1)); }
    assert!(twin.replies.iter().all(|(_, answer)| *answer == Err(Fail::Storage)));
}

#[test]
fn sess_unavailable_mutate() {
    let mut twin = opened(open(Some((CRED, 6)), None, false, Secret::Error("locked".into())));
    assert_eq!(twin.feed(mutate(7, 11)), vec![answer(7, 11, Outcome::Error(Kind::Temporary, Fail::Storage, false))]);
}

pub(super) fn unavailable_reading() -> Twin {
    let mut twin = opened(open(Some((CRED, 6)), None, false, Secret::Error("locked".into())));
    assert!(matches!(&twin.feed(Input::Foreground)[..], [RefEffect::ReadSecret { .. }]));
    twin
}

#[test]
fn sess_unavailable_read_present() {
    let mut twin = unavailable_reading();
    twin.feed(call(2));
    let op = twin.op();
    twin.feed(Input::SecretRead { op, read: Secret::Present(CRED) });
    assert!(matches!(&twin.feed(call(3))[..], [RefEffect::SealProbe { .. }]), "calls with the session work again");
}

#[test]
fn sess_unavailable_read_mismatch() {
    let mut twin = unavailable_reading();
    let op = twin.op();
    twin.feed(Input::SecretRead { op, read: Secret::Present(9) });
    twin.feed(call(3));
    assert_eq!(twin.replies.last(), Some(&(3, Err(Fail::StateLost))));
}

#[test]
fn sess_unavailable_read_missing() {
    let mut twin = unavailable_reading();
    let op = twin.op();
    let effects = twin.feed(Input::SecretRead { op, read: Secret::Missing });
    assert_eq!(effects, vec![RefEffect::SessionReset], "the same cell as the open without a secret");
    assert!(twin.credential().is_none());
}

#[test]
fn sess_unavailable_read_error() {
    let mut twin = unavailable_reading();
    let op = twin.op();
    assert!(twin.feed(Input::SecretRead { op, read: Secret::Error("still".into()) }).is_empty());
    assert!(matches!(&twin.feed(Input::Foreground)[..], [RefEffect::ReadSecret { .. }]));
}

#[test]
fn sess_secret_read_stray() {
    let mut twin = unavailable_reading();
    let op = twin.op();
    assert!(twin.feed(Input::SecretRead { op: op + 9, read: Secret::Present(CRED) }).is_empty());
    twin.feed(Input::SecretRead { op, read: Secret::Present(CRED) });
    assert!(matches!(&twin.feed(call(3))[..], [RefEffect::SealProbe { .. }]), "the stray read changed nothing, the real one counts");
    let mut calling = calling();
    let (rid, aid) = (calling.rid(), calling.aid());
    for stray in [Input::SecretRead { op: 1, read: Secret::Missing }, Input::SecretMissing { request_id: rid + 9, attempt_id: aid, lost: true }, Input::SecretMissing { request_id: rid, attempt_id: aid + 9, lost: true }] {
        assert!(calling.feed(stray).is_empty());
    }
    let input = calling.response(1);
    assert!(matches!(&calling.feed(input)[..], [RefEffect::CommitLanding { .. }]), "the attempt is still in flight");
}

#[test]
fn sess_held_epoch_ended() {
    let mut twin = held();
    twin.feed(call(2));
    twin.feed(ended(EndCause::ServerEnded));
    assert_eq!(twin.replies, vec![(2, Err(Fail::NoSession))]);
    assert!(twin.credential().is_none());
}

#[test]
fn sess_held_closing() {
    let mut twin = held();
    twin.feed(call(2));
    assert!(twin.feed(Input::Closing).contains(&RefEffect::Drained));
    assert_eq!(twin.replies, vec![(2, Err(Fail::Closed))]);
}

#[test]
fn sess_held_queue() {
    let mut twin = held();
    assert!(twin.feed(call(2)).is_empty() && twin.feed(mutate(7, 12)).is_empty());
}

#[test]
fn sess_held_cancel() {
    let mut twin = held();
    assert!(twin.feed(Input::Cancel { req_id: 11 }).is_empty());
    assert!(matches!(&twin.feed(mutate(7, 11))[..], [RefEffect::HttpRequest { .. }]), "the pair is kept for the next Mutate");
}

#[test]
fn sess_held_release() {
    let mut twin = opened(open(Some((CRED, 0)), Some(stored(Some(super::session_owner_release_tests::outbox_owner()), 5, 1)), false, Secret::Present(CRED)));
    let effects = twin.feed(Input::Release { req_id: 5 });
    assert!(matches!(effects.as_slice(), [RefEffect::Answer { author: OUTBOX_AUTHOR, req_id: 5, outcome: Outcome::Dropped }, RefEffect::HttpRequest { pending, .. }] if pending.request_id == 5 && pending.owner == Some(crate::transport::RequestOwner::Detached)), "the author that heard nothing hears Dropped once and the same pair goes out detached: {effects:?}");
}

#[test]
fn sess_release_other() {
    let mut twin = held();
    assert!(twin.feed(Input::Release { req_id: 99 }).is_empty());
    assert!(matches!(&twin.feed(mutate(7, 11))[..], [RefEffect::HttpRequest { .. }]), "another request does not release the pair");
    let mut idle = Twin::new();
    assert!(idle.feed(Input::Release { req_id: 1 }).is_empty());
}

#[test]
fn sess_held_other() {
    let mut twin = held();
    let (rid, aid) = (twin.rid(), 1);
    twin.feed_all([Input::Online(false), Input::Online(true), Input::Tick, published(PubState::Session), committed(), rejected(Reason::Storage),
        Input::Response { request_id: rid, attempt_id: aid, body: "{}".into(), sequence: Some(1), refusal: None },
        Input::Revoked { request_id: rid, attempt_id: aid, body: "{}".into(), author_ref: None },
        Input::HttpError { request_id: rid, attempt_id: aid, temporary: false, auth: true, security: None, body: None },
        Input::Dropped { request_id: rid, attempt_id: aid }]);
    assert!(matches!(&twin.feed(mutate(7, 11))[..], [RefEffect::HttpRequest { .. }]), "the pair is still held");
}

#[test]
fn sess_wait_call() {
    let mut twin = generating();
    let op = twin.op();
    twin.feed(Input::CredentialGenerated { op, credential: 42 });
    let op = twin.op();
    twin.feed_all([Input::SecretStored { op, ok: true }, committed()]);
    let input = twin.response(1);
    twin.feed_all([input, committed(), Input::Published { epoch: EPOCH + 2, identity: Some(42), state: PubState::Session }]);
    assert_eq!(twin.replies, vec![(1, Ok("{}".into()))]);
    for mut ending in [calling(), persisting()] {
        ending.feed(ended(EndCause::SessionReset));
        assert_eq!(ending.replies, vec![(1, Err(Fail::NoSession))]);
    }
    let mut closed = persisting();
    closed.feed(Input::Closing);
    assert_eq!(closed.replies, vec![(1, Err(Fail::Closed))]);
}

pub(super) fn next(seed: &mut u64) -> u64 {
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    *seed >> 33
}

/// A random trace; requests, attempts and secret operations are the last ones the owner announced so the inputs are mostly the right ones.
pub(super) fn trace(seed: u64, steps: usize, mut check: impl FnMut(&Twin, &Input, &[RefEffect])) -> Twin {
    let mut s = seed;
    let mut twin = Twin::bare();
    let mut reply = 0;
    for _ in 0..steps {
        let (request, sequence) = twin.pending.as_ref().map_or((1, 1), |pending| (pending.request_id, pending.sequence));
        let attempt = twin.attempt.unwrap_or(1);
        let op = twin.op.unwrap_or(1);
        let input = match next(&mut s) % 31 {
            0..=2 => { reply += 1; call(reply) }
            3 | 4 => committed(),
            5 => rejected(Reason::Storage),
            6 | 7 => Input::Response { request_id: request, attempt_id: attempt, body: "{}".into(), sequence: Some(if next(&mut s).is_multiple_of(6) { sequence + 1 } else { sequence }), refusal: None },
            8 => Input::Revoked { request_id: request, attempt_id: attempt, body: String::new(), author_ref: None },
            9 => Input::HttpError { request_id: request, attempt_id: attempt, temporary: next(&mut s).is_multiple_of(2), auth: next(&mut s).is_multiple_of(2), security: None, body: None },
            10 => Input::Tick,
            11 => Input::Online(next(&mut s).is_multiple_of(2)),
            12 => Input::Published { epoch: EPOCH + next(&mut s) % 3, identity: None, state: [PubState::Session, PubState::Ending, PubState::EndingFailed, PubState::NoSession][(next(&mut s) % 4) as usize] },
            13 => Input::EpochEnded { epoch: EPOCH + next(&mut s) % 3, cause: [EndCause::SessionReset, EndCause::ServerEnded, EndCause::Other][(next(&mut s) % 3) as usize] },
            14 => Input::Response { request_id: request + 50, attempt_id: attempt, body: "{}".into(), sequence: Some(sequence), refusal: None },
            15 => if next(&mut s).is_multiple_of(6) { Input::Closing } else { committed() },
            16 | 17 => { reply += 1; mutate(100 + reply, 1_000 + reply % 3) }
            18 => Input::Dropped { request_id: request, attempt_id: attempt },
            19 => { reply += 1; call_of(["login", "visit", "logout"][(next(&mut s) % 3) as usize], reply) }
            20 => Input::CredentialGenerated { op, credential: 20 + next(&mut s) % 3 },
            21 => Input::SecretStored { op, ok: !next(&mut s).is_multiple_of(4) },
            22 => Input::SecretCleared { op, ok: true },
            23 => Input::SecretMissing { request_id: request, attempt_id: attempt, lost: next(&mut s).is_multiple_of(2) },
            24 => Input::Foreground,
            25 => Input::Cancel { req_id: 1_000 + next(&mut s) % 3 },
            26 => Input::SecretRead { op, read: [Secret::Present(CRED), Secret::Missing, Secret::Error("x".into())][(next(&mut s) % 3) as usize].clone() },
            29 => Input::AnswerTooLarge { request_id: request + if next(&mut s).is_multiple_of(4) { 50 } else { 0 }, attempt_id: attempt + if next(&mut s).is_multiple_of(4) { 50 } else { 0 }, limit: 64 },
            _ => Input::HttpError { request_id: request, attempt_id: attempt, temporary: false, auth: false, security: None, body: None },
        };
        let effects = twin.feed(input.clone());
        check(&twin, &input, &effects);
    }
    twin
}

/// lifecycle.md:308, 524-526 (D-280, D-269): the held abort pair of a multipart debt goes out once, byte for byte, its answer reaches the Transfer author once after Committed, and a late repeat of the answer does nothing.
#[test]
fn sess_restored_orphan() {
    let mut twin = Twin::new();
    let pair = stored(Some(orphan_owner()), 5, 1);
    assert_eq!(twin.feed(Input::Restored { epoch: EPOCH, pending: Some(pair.clone()) }), vec![RefEffect::RestoreClassified { epoch: EPOCH }]);
    let sent = twin.feed(Input::RestoreReady { epoch: EPOCH });
    let [RefEffect::HttpRequest { pending, .. }] = &sent[..] else { panic!("one request for the held pair: {sent:?}") };
    assert_eq!((pending.request_id, pending.sequence, &pending.path, &pending.body, &pending.owner), (pair.request_id, pair.sequence, &pair.path, &pair.body, &pair.owner), "the pair goes out as stored");
    assert!(twin.feed(Input::Online(true)).is_empty(), "Online after RestoreReady sends nothing again");

    let (request, attempt) = (twin.rid(), twin.aid());
    let first = twin.response(1);
    twin.feed(first);
    let delivered = twin.feed(committed());
    assert_eq!(delivered, vec![answer(TRANSFER_AUTHOR, 5, Outcome::Response("{}".into(), false))], "one answer to the Transfer author after Committed");

    let late = Input::Response { request_id: request, attempt_id: attempt, body: "{}".into(), sequence: Some(1), refusal: None };
    assert!(twin.feed(late).is_empty(), "a late repeat of the answer does nothing");
    assert!(twin.feed(committed()).is_empty(), "a repeated Committed gives no second answer");
    assert!(twin.replies.is_empty(), "no call reply");
    assert_eq!(twin.credential().map(|credential| credential.sequence), Some(1), "the sequence moved once");
}
