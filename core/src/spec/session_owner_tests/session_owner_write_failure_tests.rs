//! Owner-level tests of the `session` machine: a refused write of the pair or of the landing (R14, R17), `Release` and `Cancel` of a queued call (R16) and the retry wait cut by the network (R18).

use super::*;

fn post(twin: &mut Twin, input: SessionInput) -> Vec<SessionEffect> {
    twin.det.post(twin.addr, EpochStamp::Control, input);
    twin.det.run_until_quiet(100).expect("the owner is quiet");
    take(&twin.log)
}

fn timers_set(twin: &mut Twin, input: Input) -> (usize, Vec<RefEffect>) {
    let before = twin.det.trace().len();
    let effects = twin.feed(input);
    (twin.det.trace()[before..].iter().filter(|entry| entry.effects.contains("Schedule {") || entry.effects.contains("Backoff {")).count(), effects)
}

fn last_batch(twin: &Twin) -> u64 {
    twin.history.iter().rev().find_map(|effect| match effect { RefEffect::CommitPending { batch, .. } | RefEffect::CommitLanding { batch, .. } => Some(*batch), _ => None }).expect("the owner wrote a batch")
}

fn mutate_persisting() -> Twin {
    let mut twin = Twin::new();
    twin.feed(mutate(7, 11));
    finish_seal(&mut twin);
    twin
}

#[test]
fn sess_persisting_storage_retried() {
    for refusals in 1..=5 {
        let mut twin = mutate_persisting();
        let (pair, batch) = (twin.rid(), last_batch(&twin));
        for _ in 0..refusals {
            let (timers, effects) = timers_set(&mut twin, rejected(Reason::Storage));
            assert_eq!((timers, effects.is_empty()), (1, true), "refusals={refusals}: one timer, no Answer");
            let again = twin.feed(Input::Tick);
            assert!(matches!(&again[..], [RefEffect::CommitPending { batch: again_batch, pending, .. }] if *again_batch == batch && pending.request_id == pair), "the same batch of the same pair is written again: {again:?}");
        }
        let sent = twin.feed(committed());
        assert!(matches!(&sent[..], [RefEffect::HttpRequest { pending, .. }] if pending.request_id == pair), "refusals={refusals}: {sent:?}");
    }
}

#[test]
fn sess_landing_storage_retried() {
    for refusals in 1..=5 {
        let mut twin = super::session_owner_release_tests::rejected_landing(1);
        for _ in 0..refusals {
            let before = last_batch(&twin);
            let (timers, effects) = timers_set(&mut twin, rejected(Reason::Storage));
            assert_eq!((timers, effects.is_empty()), (1, true), "refusals={refusals}: one timer, no Answer");
            let again = twin.feed(Input::Tick);
            assert!(matches!(&again[..], [RefEffect::CommitLanding { batch, .. }] if *batch != before), "the landing goes again with a new batch: {again:?}");
        }
        assert_eq!(twin.feed(committed()), vec![answer(7, 11, Outcome::Response("{}".into(), false))], "the author hears the outcome once");
        assert_eq!(twin.credential().map(|credential| credential.sequence), Some(1));
    }
}

#[test]
fn sess_write_refused_by_an_ended_epoch_or_closing_writes_nothing() {
    for reason in [Reason::NoSession, Reason::Closed] {
        for mut twin in [mutate_persisting(), super::session_owner_release_tests::rejected_landing(1)] {
            assert!(twin.feed(rejected(reason)).is_empty(), "{reason:?}: no effect, no write, no Answer");
        }
    }
}

#[test]
fn sess_tick_during_a_write_is_foreign() {
    for mut twin in [mutate_persisting(), super::session_owner_release_tests::rejected_landing(1)] {
        assert!(twin.feed(Input::Tick).is_empty(), "a batch is in flight: the Tick is foreign");
    }
}

fn assert_blocked_invalid(twin: &mut Twin, context: &str) {
    assert_eq!(twin.credential().map(|credential| credential.sequence), Some(0), "{context}: the landing is not applied");
    assert!(twin.feed(Input::Tick).is_empty(), "{context}: no repeat of the write");
}

#[test]
fn sess_landing_invalid() {
    let mut twin = super::session_owner_release_tests::rejected_landing(1);
    assert_eq!(twin.feed(rejected(Reason::Invalid)), vec![answer(7, 11, Outcome::Response("{}".into(), true))], "the server outcome reaches the author as received, the pair is retained (R14)");
    assert_blocked_invalid(&mut twin, "live author");

    let mut ended = calling_mutate();
    let input = Input::Revoked { request_id: ended.rid(), attempt_id: ended.aid(), body: "{}".into(), author_ref: None };
    ended.feed(input);
    assert_eq!(ended.feed(rejected(Reason::Invalid)), vec![RefEffect::ServerEnded { epoch: EPOCH }], "server_ended: no live answer to the author, only the end of the epoch (D-278)");
    assert_blocked_invalid(&mut ended, "server_ended");

    let mut detached = super::session_owner_detached_tests::landing_detached(false);
    assert!(detached.feed(rejected(Reason::Invalid)).is_empty(), "a Detached pair has no author to hear the outcome");
    assert_blocked_invalid(&mut detached, "Detached");

    for (author, owner) in [(OUTBOX_AUTHOR, super::session_owner_release_tests::outbox_owner()), (TRANSFER_AUTHOR, super::session_owner_release_tests::transfer_init_owner())] {
        let mut kept = Twin::new();
        kept.feed_all([super::session_owner_release_tests::owned_mutate(author, owner, 11), committed(), Input::Release { req_id: 11 }]);
        let input = kept.response(1);
        kept.feed(input);
        assert_eq!(kept.feed(rejected(Reason::Invalid)), vec![answer(author, 11, Outcome::Response("{}".into(), true))], "author {author} kept by Release hears the outcome as sess_landing_detached_committed with the pair retained");
        assert_blocked_invalid(&mut kept, "Detached with the author kept");
    }
}

#[test]
fn sess_release_queued() {
    let mut twin = persisting();
    twin.feed(mutate(7, 12));
    assert_eq!(twin.feed(Input::Release { req_id: 12 }), vec![answer(7, 12, permanent(Fail::NotSent))], "no pair was created for the queued entry");
    assert!(matches!(&twin.feed(committed())[..], [RefEffect::HttpRequest { .. }]), "the started request goes on");
}

#[test]
fn sess_release_sealing() {
    let mut twin = Twin::new();
    twin.feed(mutate(7, 11));
    let seal_id = twin.seal.expect("the pair is sealing");
    let released = post(&mut twin, SessionInput::Release { req_id: 11 });
    assert!(matches!(released.as_slice(), [SessionEffect::Answer { author: 7, req_id: 11, outcome: AuthorOutcome::Error { class: ErrorClass::Permanent, error: SessionError::NotSent, pending_retained: false } }]), "no pair was created: {released:?}");
    assert!(post(&mut twin, SessionInput::Sealed { seal_id, size: 1 }).is_empty(), "the late Sealed is foreign");
}

#[test]
fn sess_release_pair_started() {
    for (mut twin, stored) in [(mutate_persisting(), true), (calling_mutate(), false)] {
        assert_eq!(twin.feed(Input::Release { req_id: 11 }), vec![answer(7, 11, Outcome::Dropped)], "the author hears: unknown outcome");
        if stored {
            let sent = twin.feed(committed());
            assert!(matches!(&sent[..], [RefEffect::HttpRequest { pending, .. }] if pending.author.is_none() && pending.owner == Some(crate::transport::RequestOwner::Detached)), "the pair goes on without the author: {sent:?}");
        }
        let input = twin.response(1);
        twin.feed(input);
        assert!(twin.feed(committed()).is_empty(), "the author hears nothing more");
    }
}

#[test]
fn sess_release_started_stray() {
    for mut twin in [mutate_persisting(), calling_mutate()] {
        assert!(twin.feed(Input::Release { req_id: 99 }).is_empty());
    }
    let mut sealing = Twin::new();
    sealing.feed(mutate(7, 11));
    assert!(post(&mut sealing, SessionInput::Release { req_id: 99 }).is_empty());
}

#[test]
fn sess_prop_retry_survives_offline() {
    let orders: [&[Input]; 4] = [
        &[Input::Online(false), Input::Online(true)],
        &[Input::Online(false), Input::Tick, Input::Online(true)],
        &[Input::Online(false), Input::Online(false), Input::Tick, Input::Online(true)],
        &[Input::Online(false), Input::Online(true), Input::Tick],
    ];
    for (at, order) in orders.iter().enumerate() {
        let mut twin = calling();
        let input = twin.http_error(true, false);
        twin.feed(input);
        let (mut online, mut requests) = (true, 0);
        for input in *order {
            if let Input::Online(value) = input { online = *value; }
            let effects = twin.feed(input.clone());
            let sent = effects.iter().filter(|effect| matches!(effect, RefEffect::HttpRequest { .. })).count();
            assert!(online || sent == 0, "order {at}: nothing is sent while the network is away: {effects:?}");
            requests += sent;
        }
        assert_eq!(requests, 1, "order {at}: {order:?}");
    }
}
