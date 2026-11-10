use super::*;

#[test]
fn store_epoch_ended_deferred() {
    let mut twin = Twin::live();
    let mut batch = Batch::rows(1, vec![("Moment".into(), "a".into())]);
    batch.basis.insert("Moment".into(), 0);
    twin.feed(Request::Commit { id: 1, batch });
    twin.feed(Request::EpochEnded { epoch: 1 });
    assert_eq!(twin.state().epoch(), 1, "EpochEnded waits for Written");
    twin.feed(Request::Written { id: 1, result: Ok(1) });
    assert_eq!(twin.state().epoch(), 2);
}

#[test]
fn store_epoch_ended() {
    let mut twin = Twin::live();
    twin.feed(Request::EpochEnded { epoch: 1 });
    assert_eq!(Twin::rejected(&twin.commit(1, &["a"])), Some(Reason::NoSession), "the commits of the ended epoch are refused");
}

#[test]
fn store_epoch_ended_late() {
    let mut twin = Twin::live();
    twin.feed(Request::EpochEnded { epoch: 1 });
    twin.feed(Request::EpochEnded { epoch: 0 });
    assert_eq!(twin.state().epoch(), 2);
}

#[test]
fn store_read_intents() {
    let mut twin = Twin::live();
    assert_eq!(twin.feed(Request::ReadIntents { epoch: 1 }), vec![Action::ReadIntents { epoch: 1, direct: false }]);
}

#[test]
fn store_reads_counted() {
    let mut twin = Twin::live();
    twin.feed(Request::Close);
    twin.feed(Request::ReadsClosed(vec![4, 5]));
    twin.feed(Request::ReadFinished(4));
    assert_eq!(twin.state().mode(), StoreMode::Open, "a read is still open");
    let done = twin.feed(Request::ReadFinished(5));
    assert!(done.contains(&Action::StoreClosed));
}

#[test]
fn store_read_finished_stray() {
    let mut twin = Twin::live();
    assert!(twin.feed(Request::ReadFinished(3)).is_empty());
}

#[test]
fn store_close_waits() {
    let mut twin = Twin::live();
    assert_eq!(twin.feed(Request::Close), vec![Action::CloseReads]);
    assert_eq!(twin.state().mode(), StoreMode::Open);
}

#[test]
fn store_close() {
    let mut twin = Twin::live();
    twin.feed(Request::Close);
    let actions = twin.feed(Request::ReadsClosed(vec![]));
    assert_eq!(actions, vec![Action::CloseConnections, Action::StoreClosed]);
    assert_eq!(twin.state().mode(), StoreMode::Closed);
}

#[test]
fn store_close_after_io() {
    let mut twin = Twin::live();
    let mut batch = Batch::rows(1, vec![("Moment".into(), "a".into())]);
    batch.basis.insert("Moment".into(), 0);
    twin.feed(Request::Commit { id: 1, batch });
    assert!(twin.feed(Request::Close).is_empty(), "Close waits for the write");
    let done = twin.feed(Request::Written { id: 1, result: Ok(1) });
    assert!(done.contains(&Action::CloseReads));
    let end = twin.feed(Request::ReadsClosed(vec![]));
    assert!(end.contains(&Action::StoreClosed));
}

#[test]
fn store_closing_refuses() {
    let mut twin = Twin::live();
    twin.feed(Request::Close);
    assert_eq!(Twin::rejected(&twin.commit(1, &["a"])), Some(Reason::Closed));
    assert_eq!(twin.feed(Request::ClearAccount { epoch_old: 1, cause: RefCause::SessionReset, generation_new: 1, erase_id: 1 }), vec![Action::ClearFailed { erase_id: 1, closed: true }]);
    assert_eq!(twin.feed(Request::Published { publication_id: 5, epoch: 2, generation: 0, active: true }), vec![Action::SaveFailed { publication_id: 5, closed: true }]);
}

#[test]
fn store_closed() {
    let mut twin = Twin::live();
    twin.feed(Request::Close);
    twin.feed(Request::ReadsClosed(vec![]));
    assert_eq!(Twin::rejected(&twin.commit(1, &["a"])), Some(Reason::Closed));
}

#[test]
fn store_closed_clear() {
    let mut twin = Twin::live();
    twin.feed(Request::Close);
    twin.feed(Request::ReadsClosed(vec![]));
    assert_eq!(twin.feed(Request::ClearModels { epoch: 1, generation_new: 1, models: names(&["Moment"]), erase_id: 3 }), vec![Action::ClearFailed { erase_id: 3, closed: true }]);
}

#[test]
fn store_closed_other() {
    let mut twin = Twin::live();
    twin.feed(Request::Close);
    twin.feed(Request::ReadsClosed(vec![]));
    for request in [Request::Published { publication_id: 4, epoch: 3, generation: 0, active: true }, Request::EpochEnded { epoch: 1 }, Request::Written { id: 5, result: Ok(1) }, Request::Erased { io_id: 5, result: Ok(1) }, Request::ReadIntents { epoch: 1 }, Request::ReadsClosed(vec![]), Request::ReadFinished(2)] {
        assert!(twin.feed(request).is_empty());
    }
}

pub(super) fn next(seed: &mut u64) -> u64 {
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    *seed >> 33
}

pub(super) fn random_trace(seed: u64) -> Twin {
    let mut seed = seed;
    let mut twin = Twin::live();
    for step in 0..40u64 {
        match next(&mut seed) % 9 {
            0..=2 => {
                let epoch = if next(&mut seed).is_multiple_of(5) { 7 } else { twin.reference.epoch };
                let mut batch = Batch::rows(epoch, vec![("Moment".into(), format!("r{step}"))]);
                batch.basis.insert("Moment".into(), next(&mut seed) % 3);
                twin.commit_batch(batch, !next(&mut seed).is_multiple_of(6));
            }
            3 => { twin.clear_account(RefCause::IdentityChanged, twin.reference.generation("Moment") + 1 + next(&mut seed) % 2, !next(&mut seed).is_multiple_of(4)); }
            4 => { twin.clear_models(twin.reference.generation("Moment") + next(&mut seed) % 3, &["Moment"], !next(&mut seed).is_multiple_of(4)); }
            5 => { let epoch = twin.reference.epoch; twin.publish(epoch + 1, twin.reference.saved_generation); }
            6 => { let epoch = twin.reference.epoch; twin.feed(Request::EpochEnded { epoch }); }
            7 => { twin.feed(Request::ReadIntents { epoch: twin.reference.epoch }); }
            _ => { twin.feed(Request::ReadFinished(next(&mut seed) % 3)); }
        }
    }
    twin
}

#[test]
fn store_prop_revision() {
    for seed in 1..=40 {
        let twin = random_trace(seed);
        let mut last = 0;
        for (_, actions) in &twin.steps {
            for action in actions {
                if let Action::BroadcastCommitted { revision, .. } = action {
                    assert_eq!(*revision, last + 1, "seed {seed}: the revision grows by 1 per commit");
                    last = *revision;
                }
            }
        }
    }
}

#[test]
fn store_prop_no_stale_rows() {
    for seed in 1..=40 {
        assert_eq!(random_trace(seed).stale_writes, 0, "a row with a basis below the generation reached SQLite");
    }
}

#[test]
fn store_prop_no_stale_epoch() {
    for seed in 1..=40 {
        let twin = random_trace(seed);
        assert!(twin.written.iter().all(|(batch, store)| batch == store), "a batch of another epoch reached SQLite: {:?}", twin.written);
    }
}

#[test]
fn store_prop_atomic() {
    let mut twin = Twin::live();
    let mut batch = Batch::rows(1, vec![("Moment".into(), "a".into()), ("Moment".into(), "b".into())]);
    batch.basis.insert("Moment".into(), 0);
    twin.commit_batch(batch, true);
    assert_eq!(twin.state().revision(), 1, "two rows, one transaction, one revision");
}

/// Every batch id is answered once.
#[test]
fn store_prop_reply_once() {
    for seed in 1..=40 {
        let twin = random_trace(seed);
        let mut seen = BTreeSet::new();
        for effect in &twin.history {
            if let StoreEffect::Reply { batch, .. } = effect {
                assert!(seen.insert(*batch), "seed {seed}: batch {batch} is answered twice");
            }
        }
    }
}

/// Store batch: the id is unique within the author, the answer goes to the pair `(author, id)`.
#[test]
fn store_commit_author_pair() {
    let mut twin = Twin::live();
    let batch = |author| StoreBatch { author, id: 1, stamp: crate::runtime::EpochStamp::Epoch(1), basis: BTreeMap::new(), rows: Vec::new(), intents: 0, credential: None, ops: Vec::new() };
    twin.send(StoreInput::Commit(batch(BatchAuthor::Session)));
    twin.send(StoreInput::Commit(batch(BatchAuthor::Outbox)));
    let written = twin.send(StoreInput::Written { author: BatchAuthor::Session, batch: 1, result: Ok(WriteDone { revision: 1, ..WriteDone::default() }) });
    assert!(written.iter().any(|effect| matches!(effect, StoreEffect::Reply { author: BatchAuthor::Session, batch: 1, stamp: crate::runtime::EpochStamp::Epoch(1), reply: StoreReply::Committed { .. } })), "the Session batch is answered");
    assert!(!written.iter().any(|effect| matches!(effect, StoreEffect::Reply { author: BatchAuthor::Outbox, .. })), "the Outbox batch with the same id still waits");
    let written = twin.send(StoreInput::Written { author: BatchAuthor::Outbox, batch: 1, result: Ok(WriteDone { revision: 2, ..WriteDone::default() }) });
    assert!(written.iter().any(|effect| matches!(effect, StoreEffect::Reply { author: BatchAuthor::Outbox, batch: 1, stamp: crate::runtime::EpochStamp::Epoch(1), reply: StoreReply::Committed { .. } })), "the Outbox batch is answered with the epoch of the batch");
}

#[test]
fn store_prop_reads_not_blocked() {
    let mut twin = Twin::live();
    let before = twin.history.len();
    twin.send(StoreInput::Idle);
    assert_eq!(twin.history.len(), before, "a read gives no input and no effect of the writer");
    assert_eq!(twin.state().revision(), 0);
}

#[test]
fn store_prop_intents_survive_format() {
    let mut twin = Twin::new();
    let mut batch = Batch::rows(0, vec![]);
    batch.intents = 3;
    twin.commit_batch(batch, true);
    let before = twin.reference.intents;
    twin.boot(Boot::FormatChange, (0, 0, None));
    twin.clear_models(1, &["Moment"], true);
    assert_eq!(twin.reference.intents, before, "the intents survive a format change and ClearModels");
    assert!(!twin.history.iter().any(|effect| matches!(effect, StoreEffect::EraseAccount { .. })), "no account erase touched them");
}

#[test]
fn store_prop_generation_monotonic() {
    for seed in 1..=40 {
        let twin = random_trace(seed);
        assert!(twin.generations.windows(2).all(|pair| pair[0] <= pair[1]), "model_generation went down: {:?}", twin.generations);
    }
    let mut twin = Twin::live();
    twin.clear_account(RefCause::SessionReset, 4, true);
    assert_eq!(twin.state().generation("Moment"), 4);
    assert_eq!(twin.state().generation("Other"), 4, "after ClearAccount every model has generation_new");
}

/// P-STORE-10: between `Write` and `Written` the Store takes no step by `Commit`, `ClearAccount`, `ClearModels`, `EpochEnded`, `Close`.
#[test]
fn store_prop_write_atomic_step() {
    for seed in 1..=40 {
        let twin = random_trace(seed);
        let mut writing = false;
        for (request, actions) in &twin.steps {
            if matches!(request, Request::Written { .. }) { writing = false; }
            if writing && matches!(request, Request::Commit { .. } | Request::ClearAccount { .. } | Request::ClearModels { .. } | Request::Close) {
                assert!(!actions.iter().any(|action| matches!(action, Action::Write(_) | Action::EraseAccount { .. } | Action::EraseModels { .. } | Action::CloseReads)), "seed {seed}: a step by {request:?} while writing");
            }
            if actions.iter().any(|action| matches!(action, Action::Write(_))) { writing = true; }
        }
    }
}

/// P-STORE-15: `Saved` only after the SQLite answer, `SaveFailed` only without it.
#[test]
fn store_prop_publish_saved() {
    let mut twin = Twin::new();
    twin.feed(Request::Published { publication_id: 1, epoch: 3, generation: 0, active: true });
    twin.feed(Request::Published { publication_id: 2, epoch: 4, generation: 0, active: true });
    assert_eq!(twin.state().epoch(), 0, "nothing is saved before the answer");
    twin.feed(Request::Erased { io_id: 1, result: Ok(0) });
    twin.feed(Request::Erased { io_id: 2, result: Err(()) });
    assert_eq!(twin.state().epoch(), 3, "the failed save leaves the previous epoch");
}

/// P-STORE-17: a `Published` taken during `Save(p1)` is answered once, the heads start in order.
#[test]
fn store_prop_io_head_once() {
    let mut twin = Twin::new();
    twin.feed(Request::Published { publication_id: 1, epoch: 3, generation: 0, active: true });
    twin.feed(Request::Published { publication_id: 2, epoch: 4, generation: 0, active: true });
    twin.feed(Request::Published { publication_id: 3, epoch: 5, generation: 0, active: true });
    twin.feed(Request::Erased { io_id: 1, result: Ok(0) });
    twin.feed(Request::Erased { io_id: 2, result: Err(()) });
    twin.feed(Request::Erased { io_id: 3, result: Ok(0) });
    let starts = twin.history.iter().filter(|e| matches!(e, StoreEffect::SavePublished { .. })).count();
    let answers = twin.history.iter().filter(|e| matches!(e, StoreEffect::Saved { .. } | StoreEffect::SaveFailed { .. })).count();
    assert_eq!((starts, answers), (3, 3));
}

/// P-STORE-16: `StoreClosed` once, after the reads, the write and the IO.
#[test]
fn store_prop_close_once() {
    let mut twin = Twin::live();
    twin.feed(Request::Close);
    twin.feed(Request::ReadsClosed(vec![1]));
    twin.feed(Request::ReadFinished(1));
    twin.feed(Request::ReadFinished(1));
    twin.feed(Request::Close);
    assert_eq!(twin.history.iter().filter(|effect| matches!(effect, StoreEffect::StoreClosed)).count(), 1);
}

/// P-STORE-18: saving a restricting publication keeps the epoch, the commit of the ended epoch stays refused.
#[test]
fn store_prop_restricting_keeps_epoch() {
    for epoch in 1..=5 {
        let mut twin = Twin::new();
        twin.publish(epoch, 0);
        twin.feed(Request::EpochEnded { epoch });
        twin.publication += 1;
        let publication_id = twin.publication;
        twin.feed(Request::Published { publication_id, epoch, generation: 0, active: false });
        let io_id = twin.head_io().expect("the save of the publication starts");
        twin.feed(Request::Erased { io_id, result: Ok(0) });
        assert!(twin.state().epoch() >= epoch, "epoch {epoch}: the restricting save lowered the epoch to {}", twin.state().epoch());
        assert_eq!(Twin::rejected(&twin.commit(epoch, &["a"])), Some(Reason::NoSession), "epoch {epoch}: the commit of the ended epoch is refused");
    }
}
