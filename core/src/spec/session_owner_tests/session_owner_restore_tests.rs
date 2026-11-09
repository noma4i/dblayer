use super::*;

#[test]
fn sess_closed() {
    let mut twin = Twin::new();
    twin.feed_all([Input::Closing, call(1), mutate(7, 11), Input::Tick, Input::Online(true), Input::Closing]);
    assert_eq!(twin.replies, vec![(1, Err(Fail::Closed))]);
}

#[test]
fn sess_open_empty() {
    let twin = opened(open(None, None, false, Secret::Missing));
    assert!(matches!(twin.reference.state, RefState::Idle) && twin.history.is_empty());
}

#[test]
fn sess_open_restored() {
    let mut twin = opened(open(Some((CRED, 6)), None, false, Secret::Present(CRED)));
    assert!(matches!(&twin.feed(call(1))[..], [RefEffect::SealProbe { pending, .. }] if pending.sequence == 7));
}

#[test]
fn sess_open_pending() {
    let owner = crate::transport::RequestOwner::Operation { seq: 3 };
    let twin = opened(open(Some((CRED, 6)), Some(stored(Some(owner), 3, 7)), false, Secret::Present(CRED)));
    assert!(matches!(&twin.reference.state, RefState::PendingHeld { pending } if pending.author == Some(super::super::ref_session::OUTBOX_AUTHOR)), "the author hears the stored pair from Restored and repeats it");
    assert!(twin.history.is_empty(), "Session sends nothing itself");
}

#[test]
fn sess_open_pending_transfer_replays_its_pair_for_the_same_step() {
    let owner = crate::transport::RequestOwner::Transfer { operation_id: 4, item_id: "key-1".into(), step: crate::owners::write::upload::UploadStage::Create };
    let mut twin = opened(open(Some((CRED, 6)), Some(stored(Some(owner.clone()), 3, 7)), false, Secret::Present(CRED)));
    twin.feed(Input::Mutate { author: 9, owner: Some(owner), req_id: 3, epoch: EPOCH, path: OTP.into(), body: "{\"other\":1}".into(), purpose: 0 });
    assert!(matches!(&twin.reference.state, RefState::Calling { pending, .. } if pending.author == Some(9) && pending.request_id == 3 && pending.body == "{}"), "the saved pair goes out again for the new asker, the new body does not");
}

#[test]
fn sess_open_direct_pending() {
    let twin = opened(open(Some((CRED, 6)), Some(stored(None, 5, 7)), false, Secret::Present(CRED)));
    assert!(matches!(twin.reference.state, RefState::Idle) && twin.history.is_empty());
}

#[test]
fn sess_open_direct_login_pending() {
    let login = RefPending { class: Class::Login, credential: Some(CRED), sequence: 1, ..stored(None, 5, 1) };
    let twin = opened(open(Some((CRED, 0)), Some(login), false, Secret::Present(CRED)));
    assert!(matches!(twin.reference.state, RefState::Idle) && twin.history.is_empty());
}

#[test]
fn sess_open_pending_lost() {
    let mut twin = opened(open(Some((CRED, 6)), None, true, Secret::Present(CRED)));
    assert!(matches!(twin.reference.state, RefState::Blocked { pending: None, reason: Block::StateLost, landing: None }));
    twin.feed(call(1));
    assert_eq!(twin.replies, vec![(1, Err(Fail::StateLost))]);
}

#[test]
fn sess_open_secret_unavailable() {
    let mut twin = opened(open(Some((CRED, 6)), None, false, Secret::Error("locked".into())));
    assert!(matches!(&twin.reference.state, RefState::SecretUnavailable { error, read: None } if error == "locked"));
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
    assert!(twin.reference.credential.is_none());
}

#[test]
fn sess_open_digest_mismatch() {
    let mut twin = opened(open(Some((CRED, 6)), None, false, Secret::Present(9)));
    assert!(matches!(twin.reference.state, RefState::SecretMismatch));
    twin.feed_all([call(1), mutate(7, 11)]);
    assert_eq!(twin.replies, vec![(1, Err(Fail::StateLost))]);
    assert_eq!(twin.reference.credential.map(|credential| credential.digest), Some(CRED), "the credential is not replaced");
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
    assert_eq!(twin.reference.credential.map(|credential| credential.sequence), Some(0));
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
    assert!(twin.reference.restored_direct.is_some());
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
    assert_eq!(twin.reference.credential.map(|credential| credential.sequence), Some(1));
}

fn transfer_owner() -> crate::transport::RequestOwner {
    crate::transport::RequestOwner::Transfer { operation_id: 4, item_id: "key-1".into(), step: crate::owners::write::upload::UploadStage::Create }
}

#[test]
fn sess_restored_transfer_pending() {
    let mut twin = Twin::new();
    let effects = twin.feed(Input::Restored { epoch: EPOCH, pending: Some(stored(Some(transfer_owner()), 5, 1)) });
    assert_eq!(effects, vec![RefEffect::RestoreClassified { epoch: EPOCH }]);
    assert!(twin.reference.restored_transfer.as_ref().is_some_and(|pending| pending.request_id == 5 && pending.author == Some(super::super::ref_session::TRANSFER_AUTHOR) && pending.reply.is_none()));
    assert!(twin.reference.restored_direct.is_none() && twin.reference.restored_detached.is_none(), "the pending is held for the Transfer owner, not a direct or detached call");
    let effects = twin.feed(Input::RestoreReady { epoch: EPOCH });
    assert!(matches!(&effects[..], [RefEffect::HttpRequest { pending, .. }] if pending.request_id == 5 && pending.sequence == 1), "Session drives the held transfer pair itself: {effects:?}");
}

#[test]
fn sess_restore_ready_transfer_pending() {
    let mut twin = Twin::new();
    twin.feed(Input::Restored { epoch: EPOCH, pending: Some(stored(Some(transfer_owner()), 5, 1)) });
    let effects = twin.feed(Input::RestoreReady { epoch: EPOCH });
    assert!(matches!(&effects[..], [RefEffect::HttpRequest { pending, .. }] if pending.request_id == 5 && pending.sequence == 1 && pending.reply.is_none()), "the stored pair goes out byte for byte: {effects:?}");
    assert!(matches!(&twin.reference.state, RefState::Calling { pending, attempts: 1, .. } if pending.request_id == 5));
    assert!(twin.reference.restored_transfer.is_none());
    let input = twin.response(1);
    let landing = twin.feed(input);
    assert!(matches!(&landing[..], [RefEffect::CommitLanding { answer: Some((5, Ok(_))), .. }]), "the pair answer is stored for the Transfer author: {landing:?}");
    let committed = twin.feed(committed());
    assert_eq!(committed, vec![answer(super::super::ref_session::TRANSFER_AUTHOR, 5, Outcome::Response("{}".into()))], "the pair answer reaches the Transfer author, not a call reply");
    assert!(twin.replies.is_empty(), "no call reply");
    assert_eq!(twin.reference.credential.map(|credential| credential.sequence), Some(1));
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
    assert!(stale.reference.restored_direct.is_some());
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
    assert!(twin.reference.restored_direct.is_none());
    let mut closing = Twin::new();
    closing.feed_all([Input::Restored { epoch: EPOCH, pending: Some(stored(None, 5, 1)) }, Input::Closing]);
    assert!(closing.reference.restored_direct.is_none());
}

#[test]
fn sess_restored_late_epoch() {
    let mut twin = Twin::new();
    twin.feed_all([Input::Restored { epoch: EPOCH, pending: Some(stored(None, 5, 1)) }, Input::RestoreReady { epoch: EPOCH }]);
    twin.feed(ended(EndCause::SessionReset));
    twin.feed(Input::Published { epoch: EPOCH + 1, identity: None, state: PubState::NoSession });
    let late = twin.feed(Input::Restored { epoch: EPOCH, pending: Some(stored(None, 5, 1)) });
    assert_eq!(late, vec![RefEffect::RestoreClassified { epoch: EPOCH }], "the pending of the ended epoch starts no request");
    assert!(twin.reference.restored_direct.is_none());
    assert!(twin.feed(Input::RestoreReady { epoch: EPOCH }).is_empty());
    assert!(!twin.reference.restore_ready, "the permission of the ended epoch is dropped");
}

#[test]
fn sess_snapshot_late_epoch() {
    let mut twin = Twin::offline();
    twin.feed(ended(EndCause::SessionReset));
    twin.feed(Input::Published { epoch: EPOCH + 1, identity: None, state: PubState::NoSession });
    assert!(twin.feed(Input::Snapshot { epoch: EPOCH, online: true }).is_empty());
    assert_eq!(twin.reference.online, None, "the snapshot of the ended epoch changes nothing");
    twin.feed(Input::Snapshot { epoch: EPOCH + 1, online: true });
    assert_eq!(twin.reference.online, Some(true));
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
    assert!(twin.reference.after_publish.is_some());
    assert!(twin.feed(call(2)).is_empty(), "a new call waits for the publication of the credential");
    assert!(twin.feed(mutate(7, 11)).is_empty(), "so does a Mutate");
    assert_eq!(twin.reference.queue.len(), 2);
    assert_eq!(twin.history.iter().filter(|effect| matches!(effect, RefEffect::CommitPending { .. })).count(), 1);
}

#[test]
fn sess_restored_other() {
    for mut twin in [persisting(), generating(), held(), blocked_storage(), opened(open(Some((CRED, 6)), None, false, Secret::Present(9)))] {
        let effects = twin.feed(Input::Restored { epoch: EPOCH, pending: Some(stored(None, 5, 1)) });
        assert_eq!(effects, vec![RefEffect::RestoreClassified { epoch: EPOCH }]);
        assert_eq!(twin.reference.restored_direct.as_ref().map(|pending| pending.request_id), Some(5));
    }
}

#[test]
fn sess_interest() {
    let mut twin = Twin::new();
    let effects = twin.feed(Input::InterestPlus(5));
    assert_eq!(effects, vec![RefEffect::RetryState { stream: 5, attempts: 0, phase: RetryPhase::Idle }]);
    twin.feed_all([call(1), committed()]);
    let input = twin.http_error(true, false);
    let effects = twin.feed(input);
    assert!(effects.contains(&RefEffect::RetryState { stream: 5, attempts: 1, phase: RetryPhase::WaitingRetry }), "{effects:?}");
}

#[test]
fn sess_interest_minus() {
    let mut twin = Twin::new();
    twin.feed_all([Input::InterestPlus(5), Input::InterestPlus(6), Input::InterestMinus(5)]);
    twin.feed_all([call(1), committed()]);
    assert!(twin.history.iter().rev().take(1).all(|effect| !matches!(effect, RefEffect::RetryState { stream: 5, .. })));
    assert_eq!(twin.reference.subscribers.len(), 1);
}

#[test]
fn sess_interest_minus_unknown() {
    let mut twin = Twin::new();
    twin.feed(Input::InterestPlus(5));
    assert!(twin.feed(Input::InterestMinus(9)).is_empty());
    assert_eq!(twin.reference.subscribers.len(), 1);
}

#[test]
fn sess_foreground_other() {
    for mut twin in [Twin::new(), persisting(), calling(), landing(1), held(), blocked_storage(), generating()] {
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
        let before = twin.reference.state.clone();
        let effects = twin.feed(call_of("visit", 1));
        assert!(matches!(&effects[..], [RefEffect::SealProbe { pending, .. }] if pending.class == Class::Sessionless));
        twin.feed(committed());
        let input = Input::Response { request_id: twin.rid(), attempt_id: twin.aid(), body: "{}".into(), sequence: None, refusal: None };
        twin.feed_all([input, committed()]);
        assert_eq!(twin.reference.state, before, "the state returns to where the call began");
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
        assert!(matches!(twin.reference.state, RefState::Idle) && twin.reference.credential.is_none());
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
        twin.feed_all([Input::Online(false), Input::Tick, published(PubState::Session), Input::Cancel { req_id: 3 }, ended(EndCause::Other)]);
        assert_eq!(twin.reference.online, Some(false));
        assert!(matches!(twin.reference.state, RefState::SecretMismatch | RefState::SecretUnavailable { .. }));
    }
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
fn sess_unavailable_foreground() {
    let twin = unavailable_reading();
    assert!(matches!(twin.reference.state, RefState::SecretUnavailable { read: Some(_), .. }));
}

#[test]
fn sess_unavailable_read_present() {
    let mut twin = unavailable_reading();
    twin.feed(call(2));
    let op = twin.op();
    twin.feed(Input::SecretRead { op, read: Secret::Present(CRED) });
    assert!(matches!(twin.reference.state, RefState::Idle), "calls with the session work again");
    let mut queued = unavailable_reading();
    queued.feed(Input::SecretStored { op: 1, ok: true });
}

#[test]
fn sess_unavailable_read_mismatch() {
    let mut twin = unavailable_reading();
    let op = twin.op();
    twin.feed(Input::SecretRead { op, read: Secret::Present(9) });
    assert!(matches!(twin.reference.state, RefState::SecretMismatch));
}

#[test]
fn sess_unavailable_read_missing() {
    let mut twin = unavailable_reading();
    let op = twin.op();
    let effects = twin.feed(Input::SecretRead { op, read: Secret::Missing });
    assert_eq!(effects, vec![RefEffect::SessionReset], "the same cell as the open without a secret");
    assert!(twin.reference.credential.is_none());
}

#[test]
fn sess_unavailable_read_error() {
    let mut twin = unavailable_reading();
    let op = twin.op();
    assert!(twin.feed(Input::SecretRead { op, read: Secret::Error("still".into()) }).is_empty());
    assert!(matches!(twin.reference.state, RefState::SecretUnavailable { read: None, .. }), "the next Foreground reads again");
    assert!(matches!(&twin.feed(Input::Foreground)[..], [RefEffect::ReadSecret { .. }]));
}

#[test]
fn sess_secret_read_stray() {
    let mut twin = unavailable_reading();
    let op = twin.op();
    twin.feed(Input::SecretRead { op: op + 9, read: Secret::Present(CRED) });
    assert!(matches!(twin.reference.state, RefState::SecretUnavailable { read: Some(_), .. }));
    let mut calling = calling();
    let (rid, aid) = (calling.rid(), calling.aid());
    calling.feed_all([Input::SecretRead { op: 1, read: Secret::Missing }, Input::SecretMissing { request_id: rid + 9, attempt_id: aid, lost: true }, Input::SecretMissing { request_id: rid, attempt_id: aid + 9, lost: true }]);
    assert!(matches!(calling.reference.state, RefState::Calling { phase: CallPhase::InFlight(_), .. }));
}

#[test]
fn sess_held_epoch_ended() {
    let mut twin = held();
    twin.feed(call(2));
    twin.feed(ended(EndCause::ServerEnded));
    assert_eq!(twin.replies, vec![(2, Err(Fail::NoSession))]);
    assert!(twin.reference.credential.is_none());
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
    assert_eq!(twin.reference.queue.len(), 2, "the slot belongs to the pair of the author");
}

#[test]
fn sess_held_cancel() {
    let mut twin = held();
    assert!(twin.feed(Input::Cancel { req_id: 11 }).is_empty());
    assert!(matches!(twin.reference.state, RefState::PendingHeld { .. }), "the pair is kept for the next Mutate");
}

#[test]
fn sess_held_release() {
    let mut twin = held();
    let rid = twin.rid();
    let effects = twin.feed(Input::Release { req_id: rid });
    assert!(matches!(twin.reference.state, RefState::Calling { .. }), "the pair goes on after the release");
    assert!(matches!(effects.as_slice(), [RefEffect::HttpRequest { pending, .. }] if pending.request_id == rid && pending.author.is_none() && pending.owner == Some(crate::transport::RequestOwner::Detached)), "the same pair goes out with no author: {effects:?}");
}

#[test]
fn sess_release_other() {
    let mut twin = held();
    assert!(twin.feed(Input::Release { req_id: 99 }).is_empty());
    assert!(matches!(twin.reference.state, RefState::PendingHeld { .. }), "another request does not release the pair");
    let mut idle = Twin::new();
    assert!(idle.feed(Input::Release { req_id: 1 }).is_empty());
}

#[test]
fn sess_held_other() {
    let mut twin = held();
    let (rid, aid) = (twin.rid(), 1);
    twin.feed_all([Input::Online(false), Input::Online(true), Input::Tick, published(PubState::Session), committed(), rejected(Reason::Storage),
        Input::Response { request_id: rid, attempt_id: aid, body: "{}".into(), sequence: Some(1), refusal: None },
        Input::Revoked { request_id: rid, attempt_id: aid, body: "{}".into() },
        Input::HttpError { request_id: rid, attempt_id: aid, temporary: false, auth: true, security: None, body: None },
        Input::Dropped { request_id: rid, attempt_id: aid }]);
    assert!(matches!(twin.reference.state, RefState::PendingHeld { .. }));
    assert_eq!(twin.reference.online, Some(true));
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
        assert_eq!(ending.replies, vec![(1, Err(Fail::Interrupted))]);
    }
    let mut closed = persisting();
    closed.feed(Input::Closing);
    assert_eq!(closed.replies, vec![(1, Err(Fail::Closed))]);
}

pub(super) fn next(seed: &mut u64) -> u64 {
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    *seed >> 33
}

/// A random trace; requests, attempts and batches are read from the reference state so the inputs are mostly the right ones.
pub(super) fn trace(seed: u64, steps: usize, mut check: impl FnMut(&Twin, &Input, &[RefEffect])) -> Twin {
    let mut s = seed;
    let mut twin = Twin::bare();
    let mut reply = 0;
    for _ in 0..steps {
        let (request, sequence) = match &twin.reference.state {
            RefState::Sealing { pending, .. } | RefState::Persisting { pending, .. } | RefState::Calling { pending, .. } | RefState::Landing { pending, .. } | RefState::PendingHeld { pending } | RefState::Blocked { pending: Some(pending), .. } => (pending.request_id, pending.sequence),
            _ => (1, 1),
        };
        let attempt = match &twin.reference.state {
            RefState::Calling { phase: CallPhase::InFlight(attempt), .. } => *attempt,
            _ => 1,
        };
        let op = match &twin.reference.state {
            RefState::Generating { op, .. } | RefState::Storing { op, .. } | RefState::ClearingSecret { op, .. } => *op,
            RefState::SecretUnavailable { read: Some(op), .. } => *op,
            _ => 1,
        };
        let input = match next(&mut s) % 30 {
            0..=2 => { reply += 1; call(reply) }
            3 | 4 => committed(),
            5 => rejected(Reason::Storage),
            6 | 7 => Input::Response { request_id: request, attempt_id: attempt, body: "{}".into(), sequence: Some(if next(&mut s).is_multiple_of(6) { sequence + 1 } else { sequence }), refusal: None },
            8 => Input::Revoked { request_id: request, attempt_id: attempt, body: String::new() },
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
            27 => Input::InterestPlus(next(&mut s) % 3),
            28 => Input::InterestMinus(next(&mut s) % 3),
            _ => Input::HttpError { request_id: request, attempt_id: attempt, temporary: false, auth: false, security: None, body: None },
        };
        let effects = twin.feed(input.clone());
        check(&twin, &input, &effects);
    }
    twin
}

