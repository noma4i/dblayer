use std::collections::BTreeSet;

use super::*;

/// A close during an open clearing waits for the erase, answers the reset by its own outcome, then closes the Store.
#[test]
fn life_close_during_the_clearing() {
    let mut twin = ending();
    twin.feed(close(2));
    twin.feed_all(drain_all());
    let done = cleared(&twin);
    let effects = twin.feed(done);
    assert!(effects.contains(&LifecycleEffect::StoreClose), "the store closes after the last erase");
    assert!(twin.replies().contains(&(1, Ok(()))), "the wait of the erase is answered by its own outcome");
    twin.feed_all([LifecycleInput::ObserversStopped, LifecycleInput::StoreClosed]);
    assert_eq!(*twin.phase(), LifecyclePhase::Closed);
}

#[test]
fn life_closing_refuses_new_work() {
    let mut twin = closing_session();
    twin.feed(reset(5));
    twin.feed_all([credential(2), server_ended(2)]);
    assert!(twin.replies().contains(&(5, Err(LifecycleError::Closed))));
}

#[test]
fn life_closed_refuses_and_a_repeated_close_succeeds() {
    let mut twin = closing_session();
    twin.feed_all(drain_all());
    twin.feed_all([LifecycleInput::ObserversStopped, LifecycleInput::StoreClosed]);
    assert_eq!(*twin.phase(), LifecyclePhase::Closed);
    twin.feed(reset(5));
    assert!(twin.replies().contains(&(5, Err(LifecycleError::Closed))));
    let effects = twin.feed(close(7));
    assert!(effects.is_empty(), "a repeated close must not start work: {effects:?}");
    assert!(twin.replies().contains(&(7, Ok(()))), "a repeated close after Closed succeeds");
    twin.feed_all([saved(999), LifecycleInput::ExecutorFailed { executor: "store".into() }]);
    assert_eq!(*twin.phase(), LifecyclePhase::Closed);
}

/// A close before the restore ended answers `open` with `Closed` once.
#[test]
fn life_close_before_the_restore_answers_open_once() {
    for identity in [None, Some(3)] {
        let mut twin = Twin::open(0, 0, identity, Some(7));
        twin.feed(close(8));
        assert_eq!(twin.published().state, PublishedState::Closing);
        assert_eq!(twin.replies().iter().filter(|(id, _)| *id == 7).cloned().collect::<Vec<_>>(), vec![(7, Err(LifecycleError::Closed))]);
        twin.feed_all(drain_all());
        twin.feed_all([LifecycleInput::ObserversStopped, LifecycleInput::StoreClosed]);
        assert_eq!(twin.replies().iter().filter(|(id, _)| *id == 7).count(), 1);
    }
}

#[test]
fn life_executor_failure_answers_open_with_runtime() {
    let mut twin = Twin::open(0, 0, None, Some(7));
    twin.feed(LifecycleInput::ExecutorFailed { executor: "store".into() });
    assert!(twin.replies().contains(&(7, Err(LifecycleError::Runtime))), "the open wait gets Runtime");
    assert!(twin.published().closed);
}

/// Every order of the owner drains, `ObserversStopped` at every position, duplicates of every input and a joining second `close` end in one `Ok` per `close`, one `StoreClose` after the last drain, and `Closed`.
#[test]
fn life_closing_all_orders() {
    let n = ALL_OWNERS.len();
    for shift in 0..n {
        for reversed in [false, true] {
            let mut order: Vec<_> = ALL_OWNERS.into_iter().cycle().skip(shift).take(n).collect();
            if reversed { order.reverse(); }
            for stop_at in 0..=n + 1 {
                for duplicate in [false, true] {
                    let label = format!("shift {shift} reversed {reversed} stop_at {stop_at} duplicate {duplicate}");
                    let mut steps: Vec<LifecycleInput> = Vec::new();
                    for (index, owner) in order.iter().enumerate() {
                        if index == stop_at { steps.push(LifecycleInput::ObserversStopped); }
                        steps.push(drained(*owner));
                        if duplicate { steps.push(drained(*owner)); }
                        if index == n / 2 { steps.push(close(2)); }
                    }
                    if stop_at == n { steps.push(LifecycleInput::ObserversStopped); }
                    steps.push(LifecycleInput::StoreClosed);
                    if stop_at == n + 1 { steps.push(LifecycleInput::ObserversStopped); }
                    let mut twin = session(1);
                    twin.feed(close(1));
                    let mut drained_owners: BTreeSet<Peer> = BTreeSet::new();
                    for step in steps {
                        if let LifecycleInput::Drained(Drained { owner }) = &step { drained_owners.extend(ALL_OWNERS.into_iter().filter(|peer| peer.name() == *owner)); }
                        let effects = twin.feed(step);
                        if effects.contains(&LifecycleEffect::StoreClose) { assert_eq!(drained_owners.len(), n, "{label}: the store closes before the last drain"); }
                        if !matches!(twin.phase(), LifecyclePhase::Closed) { assert!(twin.replies().is_empty(), "{label}: answered before the end"); }
                    }
                    assert_eq!(*twin.phase(), LifecyclePhase::Closed, "{label}");
                    assert!(twin.published().closed, "{label}");
                    twin.feed_all([LifecycleInput::StoreClosed, LifecycleInput::ObserversStopped, drained(order[0])]);
                    let mut replies = twin.replies();
                    replies.sort_by_key(|reply| reply.0);
                    assert_eq!(replies, vec![(1, Ok(())), (2, Ok(()))], "{label}: one Ok per close");
                    assert_eq!(twin.history.iter().filter(|effect| matches!(effect, LifecycleEffect::StoreClose)).count(), 1, "{label}: one StoreClose");
                }
            }
        }
    }
}

fn next(seed: &mut u64) -> u64 {
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    *seed >> 33
}

fn random_input(seed: &mut u64, step: u64, twin: &Twin) -> LifecycleInput {
    let identity = 1 + next(seed) % 2;
    let erases = twin.erases();
    let erase_id = if erases.is_empty() || next(seed).is_multiple_of(5) { 999 } else { erases[(next(seed) as usize) % erases.len()] };
    let publications = twin.publications();
    let publication = if publications.is_empty() || next(seed).is_multiple_of(6) { 999 } else { publications[(next(seed) as usize) % publications.len()] };
    match next(seed) % 22 {
        0 | 1 => credential(identity),
        2 => reset(step),
        3 => server_ended(1 + next(seed) % 6),
        5 | 6 => LifecycleInput::Cleared { erase_id },
        9 => LifecycleInput::ClearFailed { erase_id, error: SQL.into() },
        10 => close(step),
        11..=14 => saved(publication),
        15 => save_failed(publication),
        16 => LifecycleInput::StoreClosed,
        17 => LifecycleInput::ObserversStopped,
        18 => started(ALL_OWNERS[(next(seed) % 10) as usize], 1 + next(seed) % 4),
        19 => classified(ALL_OWNERS[(next(seed) % 10) as usize], 1 + next(seed) % 4),
        _ => drained(ALL_OWNERS[(next(seed) % 10) as usize]),
    }
}

/// A trace of `len` random inputs; the Store answers are random too.
fn trace(seed: u64, len: u64) -> Twin {
    let mut seed = seed;
    let mut twin = Twin::open(0, 0, None, Some(1_000));
    for step in 1..=len {
        let input = random_input(&mut seed, step, &twin);
        twin.feed(input);
    }
    twin
}

fn traces() -> impl Iterator<Item = Twin> { (1..=60).map(|seed| trace(seed, 40)) }

/// In any trace: the epoch and the generation of the publications never go down, an ended epoch never starts, every started epoch follows the end of its predecessor.
#[test]
fn life_traces_epoch_and_generation_only_grow() {
    for twin in traces() {
        let (mut last_epoch, mut last_generation) = (0, 0);
        let mut ended: BTreeSet<u64> = BTreeSet::new();
        for effect in &twin.history {
            match effect {
                LifecycleEffect::Publish { published, .. } => {
                    assert!(published.epoch >= last_epoch, "Published.epoch {} after {last_epoch}", published.epoch);
                    assert!(published.generation >= last_generation, "generation {} after {last_generation}", published.generation);
                    (last_epoch, last_generation) = (published.epoch, published.generation);
                }
                LifecycleEffect::EpochEnded { epoch, .. } => { ended.insert(*epoch); }
                LifecycleEffect::EpochStarted { epoch, .. } => {
                    assert!(!ended.contains(epoch), "the ended epoch {epoch} starts again");
                    assert!(*epoch <= 1 || ended.contains(&(epoch - 1)) || ended.contains(&(epoch - 2)), "epoch {epoch} starts, neither {} nor {} ended", epoch - 1, epoch - 2);
                }
                _ => {}
            }
        }
    }
}

/// An epoch with an identity starts only after the account clearing was answered.
#[test]
fn life_traces_clear_before_start() {
    for twin in traces() {
        let mut clearing = false;
        for (input, effects) in &twin.steps {
            for effect in effects {
                match effect {
                    LifecycleEffect::ClearAccount { .. } => clearing = true,
                    LifecycleEffect::EpochStarted { identity: Some(_), .. } => assert!(!clearing || matches!(input, LifecycleInput::Saved { .. }), "an epoch with an identity starts before the account clearing answered"),
                    _ => {}
                }
            }
            if matches!(input, LifecycleInput::Cleared { .. }) {
                clearing = false;
            }
        }
    }
}

/// `EpochStarted` goes out after the `Saved` of its publication only.
#[test]
fn life_traces_start_after_saved() {
    for twin in traces() {
        let mut saved_ids: BTreeSet<u64> = BTreeSet::new();
        let waiting: Vec<(u64, u64)> = twin.history.iter().filter_map(|effect| match effect { LifecycleEffect::Publish { publication_id, published } => Some((*publication_id, published.epoch)), _ => None }).collect();
        for (input, effects) in &twin.steps {
            if let LifecycleInput::Saved { publication_id } = input { saved_ids.insert(*publication_id); }
            for effect in effects {
                if let LifecycleEffect::EpochStarted { epoch, .. } = effect {
                    assert!(waiting.iter().any(|(id, e)| e == epoch && saved_ids.contains(id)), "EpochStarted({epoch}) before its Saved");
                }
            }
        }
    }
}

/// An erase id and the generation of an erase in flight are handed once.
#[test]
fn life_traces_hand_each_erase_once() {
    for twin in traces() {
        let mut handed = BTreeSet::new();
        let mut open: Vec<u64> = Vec::new();
        for (input, effects) in &twin.steps {
            for effect in effects {
                if let LifecycleEffect::ClearAccount { generation_new, erase_id, .. } = effect {
                    assert!(handed.insert(*erase_id), "the erase id {erase_id} is handed twice");
                    assert!(!open.contains(generation_new), "generation {generation_new} handed to two erases in flight");
                    open.push(*generation_new);
                }
            }
            if matches!(input, LifecycleInput::Cleared { .. } | LifecycleInput::ClearFailed { .. }) {
                open.clear();
            }
        }
    }
}

/// Every command with a reply is answered at most once; after the owners drain, `close` is answered and the owner is `Closed`.
#[test]
fn life_traces_answer_each_command_once() {
    for seed in 1..=60 {
        let mut twin = trace(seed, 40);
        twin.feed(close(5_000));
        twin.feed_all(drain_all());
        for erase_id in twin.erases() {
            twin.feed(LifecycleInput::Cleared { erase_id });
        }
        twin.feed_all([LifecycleInput::ObserversStopped, LifecycleInput::StoreClosed]);
        let mut seen: Vec<u64> = Vec::new();
        for effect in &twin.history {
            if let LifecycleEffect::Reply { id, .. } = effect {
                if *id == 5_000 || *id == 1_000 { continue; }
                assert!(!seen.contains(id), "reply {id} comes twice");
                seen.push(*id);
            }
        }
        assert_eq!(*twin.phase(), LifecyclePhase::Closed, "seed {seed}");
        assert!(twin.replies().iter().any(|(id, _)| *id == 5_000), "close is answered");
    }
}
