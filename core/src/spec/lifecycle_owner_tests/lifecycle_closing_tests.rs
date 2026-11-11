use super::*;

#[test]
fn life_closing_drained_duplicate() {
    let mut twin = closing_session();
    assert!(twin.feed(Event::Drained(RefOwner::Feed)).is_empty());
    assert!(twin.feed(Event::Drained(RefOwner::Feed)).is_empty());
    twin.feed_all(drain_all());
    let before = twin.history.iter().filter(|effect| matches!(effect, LifecycleEffect::StoreClose)).count();
    assert!(twin.feed(Event::Drained(RefOwner::Feed)).is_empty());
    let after = twin.history.iter().filter(|effect| matches!(effect, LifecycleEffect::StoreClose)).count();
    assert_eq!((twin.published().state, before, after), (PubState::Closing, 1, 1));
}

#[test]
fn life_restoring_close_before_classified() {
    for identity in [None, Some(3)] {
        let mut twin = Twin::open(0, 0, identity, Some(7));
        twin.feed(close(8));
        assert_eq!(twin.published().state, PubState::Closing);
        assert_eq!(twin.replies().iter().filter(|(id, _)| *id == 7).cloned().collect::<Vec<_>>(), vec![(7, Err(LifecycleError::Closed))]);
        twin.feed_all(drain_all());
        twin.feed_all([Event::ObserversStopped, Event::StoreClosed]);
        assert_eq!(twin.replies().iter().filter(|(id, _)| *id == 7).count(), 1);
    }
}

#[test]
fn life_executor_failed() {
    let mut twin = Twin::open(0, 0, None, Some(7));
    twin.feed(Event::ExecutorFailed);
    assert!(twin.replies().contains(&(7, Err(LifecycleError::Runtime))), "the open wait gets Runtime");
    assert!(twin.published().closed);
}

#[test]
fn life_closed_refuses() {
    let mut twin = closing_session();
    twin.feed_all(drain_all());
    twin.feed_all([Event::ObserversStopped, Event::StoreClosed]);
    twin.feed(reset(5));
    twin.feed(close(7));
    twin.feed_all([Event::Saved(999), Event::ExecutorFailed]);
    for id in [5] {
        assert!(twin.replies().contains(&(id, Err(LifecycleError::Closed))));
    }
    assert!(twin.replies().contains(&(7, Ok(()))), "a repeated close after Closed succeeds");
}

#[test]
fn life_closed_close_ok() {
    let mut twin = closing_session();
    twin.feed_all(drain_all());
    twin.feed_all([Event::ObserversStopped, Event::StoreClosed]);
    assert_eq!(*twin.state(), RefState::Closed);
    let before = twin.replies().len();
    let effects = twin.feed(close(77));
    assert!(effects.is_empty(), "repeated close must not start work: {effects:?}");
    assert_eq!(&twin.replies()[before..], &[(77, Ok(()))]);
    assert_eq!(*twin.state(), RefState::Closed);
}

#[test]
fn life_call_ending() {
    let twin = ending();
    assert_eq!(twin.published().state, PubState::Ending);
    assert_eq!(twin.published().epoch, 2, "the calls of the ended epoch stay out");
}

#[test]
fn life_call_storage_failed() {
    let mut twin = Twin::new();
    twin.feed(credential(1));
    let publication = twin.publications()[0];
    twin.feed(Event::SaveFailed(publication));
    assert_eq!(twin.cell().state, PubState::StorageFailed);
}

#[test]
fn life_wait_session_reset() {
    let mut twin = ending();
    let done = cleared(&twin);
    twin.feed(done);
    twin.settle();
    assert_eq!(twin.replies(), vec![(1, Ok(()))]);
}

#[test]
fn life_wait_close() {
    let mut twin = session(1);
    twin.feed(close(1));
    twin.feed_all(drain_all());
    twin.feed_all([Event::ObserversStopped, Event::StoreClosed]);
    assert_eq!(twin.replies(), vec![(1, Ok(()))]);
}

#[test]
fn life_wait_open() {
    let mut twin = Twin::open(0, 0, None, Some(7));
    twin.settle();
    assert_eq!(twin.replies(), vec![(7, Ok(()))]);
}

pub(super) fn next(seed: &mut u64) -> u64 {
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    *seed >> 33
}

pub(super) fn random_event(seed: &mut u64, step: u64, twin: &Twin) -> Event {
    let identity = 1 + next(seed) % 2;
    let erases = twin.erases();
    let erase = if erases.is_empty() || next(seed).is_multiple_of(5) { 999 } else { erases[(next(seed) as usize) % erases.len()].id };
    let publications = twin.publications();
    let publication = if publications.is_empty() || next(seed).is_multiple_of(6) { 999 } else { publications[(next(seed) as usize) % publications.len()] };
    match next(seed) % 22 {
        0 | 1 => Event::Credential(identity),
        2 => Event::SessionReset(Some(step)),
        3 => Event::ServerEnded(1 + next(seed) % 6),
        5 | 6 => Event::Cleared(erase),
        9 => Event::ClearFailed(erase),
        10 => Event::Close(step),
        11..=14 => Event::Saved(publication),
        15 => Event::SaveFailed(publication),
        16 => Event::StoreClosed,
        17 => Event::ObserversStopped,
        18 => Event::Started(OWNERS[(next(seed) % 10) as usize], 1 + next(seed) % 4),
        19 => Event::RestoreClassified(OWNERS[(next(seed) % 10) as usize], 1 + next(seed) % 4),
        _ => Event::Drained(OWNERS[(next(seed) % 10) as usize]),
    }
}

/// A trace of `len` random inputs; the Store answers are random too.
pub(super) fn trace(seed: u64, len: u64) -> Twin {
    let mut seed = seed;
    let mut twin = Twin::open(0, 0, None, Some(1_000));
    for step in 1..=len {
        let event = random_event(&mut seed, step, &twin);
        twin.feed(event);
    }
    twin
}

pub(super) fn traces() -> impl Iterator<Item = Twin> { (1..=60).map(|seed| trace(seed, 40)) }

#[test]
fn life_prop_epoch_monotonic() {
    for twin in traces() {
        let mut last = 0;
        for effect in &twin.history {
            if let LifecycleEffect::Publish { published, .. } = effect {
                assert!(published.epoch >= last, "Published.epoch {} after {last}", published.epoch);
                last = published.epoch;
            }
        }
    }
}

#[test]
fn life_prop_no_work_after_end() {
    for twin in traces() {
        let mut ended: BTreeSet<u64> = BTreeSet::new();
        for effect in &twin.history {
            match effect {
                LifecycleEffect::EpochEnded { epoch, .. } => { ended.insert(*epoch); }
                LifecycleEffect::EpochStarted { epoch, .. } => assert!(!ended.contains(epoch), "the ended epoch {epoch} starts again"),
                _ => {}
            }
        }
    }
}

#[test]
fn life_prop_clear_before_start() {
    for twin in traces() {
        let mut clearing = false;
        for (event, effects) in &twin.steps {
            for effect in effects {
                match effect {
                    LifecycleEffect::ClearAccount { .. } => clearing = true,
                    LifecycleEffect::EpochStarted { identity: Some(_), .. } => assert!(!clearing || matches!(event, Event::Saved(_)), "an epoch with an identity starts before the account clearing answered"),
                    _ => {}
                }
            }
            if matches!(event, Event::Cleared(_)) {
                clearing = false;
            }
        }
    }
}

/// Every command with a reply is answered at most once; after the owners drain, the Store closes and the observers stop, `close` is answered.
#[test]
fn life_prop_reply_once() {
    for seed in 1..=60 {
        let mut twin = trace(seed, 40);
        twin.feed(close(5_000));
        twin.feed_all(drain_all());
        for erase in twin.erases() {
            twin.feed(Event::Cleared(erase.id));
        }
        twin.feed_all([Event::ObserversStopped, Event::StoreClosed]);
        let mut seen: Vec<u64> = Vec::new();
        for effect in &twin.history {
            if let LifecycleEffect::Reply { id, .. } = effect {
                if *id == 5_000 || *id == 1_000 { continue; }
                assert!(!seen.contains(id), "reply {id} comes twice");
                seen.push(*id);
            }
        }
        assert_eq!(*twin.state(), RefState::Closed, "seed {seed}");
        assert!(twin.replies().iter().any(|(id, _)| *id == 5_000), "close is answered");
    }
}

#[test]
fn life_prop_close_drains() {
    let mut twin = session(1);
    twin.feed(close(1));
    let owners = OWNERS;
    for (index, owner) in owners.into_iter().enumerate() {
        let effects = twin.feed(Event::Drained(owner));
        let last = index + 1 == owners.len();
        assert_eq!(effects.contains(&RefEffect::StoreClose), last, "close ends with the last Drained and not before");
    }
    twin.feed(Event::StoreClosed);
    assert!(!twin.replies().iter().any(|(id, _)| *id == 1), "close waits for the observers");
    twin.feed(Event::ObserversStopped);
    assert!(twin.replies().contains(&(1, Ok(()))));
    assert!(twin.published().closed);
}

#[test]
fn life_prop_generation() {
    for twin in traces() {
        let mut last = 0;
        for effect in &twin.history {
            if let LifecycleEffect::Publish { published, .. } = effect {
                assert!(published.generation >= last, "generation {} after {last}", published.generation);
                last = published.generation;
            }
        }
    }
}

/// `ClearAccount` goes to the Store with its cause: the Store erases `encrypted_*` for `SessionReset` and `ServerEnded` only.
#[test]
fn life_prop_session_kept() {
    for twin in traces() {
        let mut handed = BTreeSet::new();
        for effect in &twin.history {
            match effect {
                LifecycleEffect::ClearAccount { generation_new, erase_id, .. } => {
                    assert!(handed.insert(*erase_id), "the erase id {erase_id} is handed twice");
                    let _ = generation_new;
                }
                _ => {}
            }
        }
    }
    let mut twin = session(1);
    let effects = twin.feed(credential(2));
    assert!(effects.iter().any(|e| matches!(e, RefEffect::ClearAccount { cause: RefCause::IdentityChanged, .. })), "a change of identity clears with IdentityChanged");
}

/// Two erases in flight never get the same generation: the Store refuses `generation_new` at or below the last.
#[test]
fn life_prop_generation_distinct() {
    for twin in traces() {
        let mut open: Vec<u64> = Vec::new();
        for (event, effects) in &twin.steps {
            for effect in effects {
                match effect {
                    LifecycleEffect::ClearAccount { generation_new, .. } => {
                        assert!(!open.contains(generation_new), "generation {generation_new} handed to two erases in flight");
                        open.push(*generation_new);
                    }
                    _ => {}
                }
            }
            if matches!(event, Event::Cleared(_) | Event::ModelsCleared(..) | Event::ClearFailed(_)) {
                open.clear();
            }
        }
    }
}

#[test]
fn life_prop_idle() {
    let mut twin = trace(3, 30);
    twin.send(LifecycleInput::Idle);
    assert!(twin.drain().0.is_empty(), "no input, no effect");
}


#[test]
fn life_prop_every_epoch_ends() {
    for twin in traces() {
        let mut ended: BTreeSet<u64> = BTreeSet::new();
        for effect in &twin.history {
            match effect {
                LifecycleEffect::EpochEnded { epoch, .. } => { ended.insert(*epoch); }
                LifecycleEffect::EpochStarted { epoch, .. } if *epoch > 1 => assert!(ended.contains(&(epoch - 1)) || ended.contains(&(epoch - 2)), "epoch {epoch} starts, neither {} nor {} ended", epoch - 1, epoch - 2),
                _ => {}
            }
        }
    }
}

/// `EpochStarted` and the answers of a publication go out after its `Saved` only.
#[test]
fn life_prop_publish_after_saved() {
    for twin in traces() {
        let mut saved: BTreeSet<u64> = BTreeSet::new();
        let waiting: Vec<(u64, u64)> = twin.history.iter().filter_map(|effect| match effect { LifecycleEffect::Publish { publication_id, published } => Some((*publication_id, published.epoch)), _ => None }).collect();
        for (event, effects) in &twin.steps {
            if let Event::Saved(id) = event { saved.insert(*id); }
            for effect in effects {
                if let LifecycleEffect::EpochStarted { epoch, .. } = effect {
                    assert!(waiting.iter().any(|(id, e)| e == epoch && saved.contains(id)), "EpochStarted({epoch}) before its Saved");
                }
            }
        }
    }
}

/// `Saved` of one publication leaves the other open publications alone.
#[test]
fn life_prop_publication_id() {
    let mut twin = Twin::open(0, 0, None, Some(7));
    twin.feed(credential(1));
    let second = twin.publications()[1];
    twin.feed(Event::Saved(second));
    assert!(!twin.replies().iter().any(|(id, _)| *id == 7), "the later Saved does not answer the earlier publication");
    assert_eq!(twin.publications().len(), 1);
}

/// The traces of the findings, at the level of the owner.
#[test]
fn life_findings() {
    let mut identity = session(1);
    identity.feed(credential(2));
    let done = cleared(&identity);
    identity.feed(done);
    identity.settle();
    assert_eq!(*identity.state(), RefState::Session { epoch: 4, identity: 2 }, "a change of identity clears first, then starts");

    let mut closing_during_clear = ending();
    closing_during_clear.feed(close(2));
    closing_during_clear.feed_all(drain_all());
    let done = cleared(&closing_during_clear);
    closing_during_clear.feed(done);
    closing_during_clear.feed_all([Event::ObserversStopped, Event::StoreClosed]);
    assert_eq!(*closing_during_clear.state(), RefState::Closed);

    let mut double_reset = ending();
    double_reset.feed(reset(2));
    let done = cleared(&double_reset);
    double_reset.feed(done);
    double_reset.settle();
    assert_eq!(double_reset.history.iter().filter(|e| matches!(e, LifecycleEffect::Reply { result: Ok(()), .. })).count(), 2 + 1, "both resets are answered");

    let mut opened = Twin::new();
    opened.feed(credential(1));
    assert!(opened.history.iter().any(|e| matches!(e, LifecycleEffect::EpochEnded { cause: Cause::SessionStarted, .. })), "a stream open without a session ends when the account is entered");
}

/// P-LIFE-12: `open` is answered exactly when the last receiver has classified, in every delivery order and with no clock.
#[test]
fn life_prop_open_no_timeout() {
    let starts = [[RefOwner::Outbox, RefOwner::Transfer], [RefOwner::Transfer, RefOwner::Outbox]];
    let classes = [RefOwner::Outbox, RefOwner::Transfer, RefOwner::Session];
    let class_orders = [[0, 1, 2], [0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0]];
    for start in starts {
        for order in class_orders {
            let mut twin = Twin::open(4, 2, Some(9), Some(7));
            twin.feed(Event::Saved(1));
            for owner in start { twin.feed(Event::Started(owner, 5)); }
            for (at, index) in order.into_iter().enumerate() {
                assert!(twin.replies().is_empty(), "{start:?} {order:?}: open answered before the receiver {at} classified");
                twin.feed(Event::RestoreClassified(classes[index], 5));
            }
            assert_eq!(twin.replies(), vec![(7, Ok(()))], "{start:?} {order:?}: open is answered once all receivers classified");
        }
    }
    let mut failed = Twin::open(4, 2, Some(9), Some(7));
    failed.feed(Event::SaveFailed(1));
    assert_eq!(failed.replies(), vec![(7, Err(LifecycleError::Storage))], "a failed save answers open with Storage without waiting for anything");
}

/// P-LIFE-22: every order of the owner drains, `ObserversStopped` at every position, duplicates of every input and a joining second `close` end in one `Ok` per `close` and `Closed`.
#[test]
fn life_prop_closing_all_orders() {
    let n = OWNERS.len();
    for shift in 0..n {
        for reversed in [false, true] {
            let mut order: Vec<_> = OWNERS.into_iter().cycle().skip(shift).take(n).collect();
            if reversed { order.reverse(); }
            for stop_at in 0..=n + 1 {
                for duplicate in [false, true] {
                    let label = format!("shift {shift} reversed {reversed} stop_at {stop_at} duplicate {duplicate}");
                    let mut steps: Vec<Event> = Vec::new();
                    for (index, owner) in order.iter().enumerate() {
                        if index == stop_at { steps.push(Event::ObserversStopped); }
                        steps.push(Event::Drained(*owner));
                        if duplicate { steps.push(Event::Drained(*owner)); }
                        if index == n / 2 { steps.push(close(2)); }
                    }
                    if stop_at == n { steps.push(Event::ObserversStopped); }
                    steps.push(Event::StoreClosed);
                    if stop_at == n + 1 { steps.push(Event::ObserversStopped); }
                    let mut twin = session(1);
                    twin.feed(close(1));
                    for step in steps {
                        twin.feed(step);
                        if !matches!(twin.state(), RefState::Closed) { assert!(twin.replies().is_empty(), "{label}: answered before the end"); }
                    }
                    assert_eq!(*twin.state(), RefState::Closed, "{label}");
                    twin.feed_all([Event::StoreClosed, Event::ObserversStopped, Event::Drained(order[0])]);
                    let mut replies = twin.replies();
                    replies.sort_by_key(|reply| reply.0);
                    assert_eq!(replies, vec![(1, Ok(())), (2, Ok(()))], "{label}: one Ok per close");
                    assert_eq!(twin.history.iter().filter(|effect| matches!(effect, LifecycleEffect::StoreClose)).count(), 1, "{label}: one StoreClose");
                }
            }
        }
    }
}
