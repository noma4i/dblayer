use super::*;

fn next(seed: &mut u64) -> u64 {
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    *seed >> 33
}

/// 40 random requests from a live Store: commits (some of another epoch, some failing in SQLite), clearings with every cause, publications, epoch ends, reads.
fn random_trace(seed: u64) -> Twin {
    let mut seed = seed;
    let mut twin = Twin::live();
    for step in 0..40u64 {
        match next(&mut seed) % 9 {
            0..=2 => {
                let epoch = if next(&mut seed).is_multiple_of(5) { 7 } else { twin.state().epoch() };
                let mut batch = Batch::rows(epoch, vec![("Moment".into(), format!("r{step}"))]);
                batch.basis.insert("Moment".into(), next(&mut seed) % 3);
                twin.commit_batch(batch, !next(&mut seed).is_multiple_of(6));
            }
            3 => { twin.clear_account(Cause::IdentityChanged, twin.state().generation("Moment") + 1 + next(&mut seed) % 2, !next(&mut seed).is_multiple_of(4)); }
            4 => {
                let causes = [Cause::SessionReset, Cause::ServerEnded, Cause::IdentityChanged, Cause::SessionStarted, Cause::Close];
                let cause = causes[(next(&mut seed) % causes.len() as u64) as usize];
                twin.clear_account(cause, twin.state().generation("Moment") + 1, !next(&mut seed).is_multiple_of(4));
            }
            5 => { let (epoch, generation) = (twin.state().epoch(), twin.state().generation("Moment")); twin.publish(epoch + 1, generation); }
            6 => { let epoch = twin.state().epoch(); twin.feed(Request::EpochEnded { epoch }); }
            7 => { twin.feed(Request::ReadIntents { epoch: twin.state().epoch() }); }
            _ => { twin.feed(Request::ReadFinished(next(&mut seed) % 3)); }
        }
    }
    twin
}

/// In any trace: the revision grows by 1 per commit, no batch of another epoch and no row of a stale generation reaches SQLite,
/// the generation never goes down, every batch is answered once, and between `Write` and `Written` no commit, clearing or close takes a step.
#[test]
fn store_traces_keep_the_invariants() {
    for seed in 1..=40 {
        let twin = random_trace(seed);
        let mut last = 0;
        let mut writing = false;
        let mut answered = BTreeSet::new();
        for (request, actions) in &twin.steps {
            for action in actions {
                if let Action::BroadcastCommitted { revision, .. } = action {
                    assert_eq!(*revision, last + 1, "seed {seed}: the revision grows by 1 per commit");
                    last = *revision;
                }
            }
            if matches!(request, Request::Written { .. }) { writing = false; }
            if writing && matches!(request, Request::Commit { .. } | Request::ClearAccount { .. } | Request::Close) {
                assert!(!actions.iter().any(|action| matches!(action, Action::Write(_) | Action::EraseAccount { .. } | Action::CloseReads)), "seed {seed}: a step by {request:?} while writing");
            }
            if actions.iter().any(|action| matches!(action, Action::Write(_))) { writing = true; }
        }
        for effect in &twin.history {
            if let StoreEffect::Reply { batch, .. } = effect {
                assert!(answered.insert(*batch), "seed {seed}: batch {batch} is answered twice");
            }
        }
        assert!(twin.written.iter().all(|(batch, store)| batch == store), "seed {seed}: a batch of another epoch reached SQLite: {:?}", twin.written);
        assert_eq!(twin.stale_writes, 0, "seed {seed}: a row with a basis below the generation reached SQLite");
        assert!(twin.generations.windows(2).all(|pair| pair[0] <= pair[1]), "seed {seed}: model_generation went down: {:?}", twin.generations);
    }
}

/// After a clearing of any cause, for every model, with a batch in flight before it or without, a batch with the old basis is refused and never reaches SQLite.
#[test]
fn store_clear_account_makes_old_basis_stale() {
    for cause in [Cause::SessionReset, Cause::ServerEnded, Cause::IdentityChanged, Cause::SessionStarted, Cause::Close] {
        for (commit_first, model) in [false, true].into_iter().flat_map(|first| ["Moment", "Other"].into_iter().map(move |model| (first, model))) {
            let mut twin = Twin::live();
            let batch = |id: &str| {
                let mut batch = Batch::rows(1, vec![(model.into(), id.into())]);
                batch.basis.insert(model.into(), 0);
                batch
            };
            if commit_first {
                assert_eq!(twin.feed(Request::Commit { id: 10, batch: batch("before") }), vec![Action::Write(10)]);
                assert!(twin.feed(Request::ClearAccount { epoch_old: 1, cause, generation_new: 1, erase_id: 10 }).is_empty());
                let actions = twin.feed(Request::Written { id: 10, result: Ok(1) });
                let io_id = actions.iter().find_map(|action| match action { Action::EraseAccount { io_id, .. } => Some(*io_id), _ => None }).expect("the deferred clear starts after the write");
                let actions = twin.feed(Request::Erased { io_id, result: Ok(2) });
                if let Some(io_id) = actions.iter().find_map(|action| match action { Action::ClearSecret { io_id } => Some(*io_id), _ => None }) {
                    twin.feed(Request::Erased { io_id, result: Ok(2) });
                }
            } else {
                twin.clear_account(cause, 1, true);
            }
            assert_eq!(twin.state().generation(model), 1, "{cause:?}, commit_first={commit_first}");
            let actions = twin.commit_batch(batch("after"), true);
            assert_eq!(Twin::rejected(&actions), Some(Reason::StaleGeneration(vec![model.into()])), "{cause:?}, commit_first={commit_first}");
            assert!(!actions.iter().any(|action| matches!(action, Action::Write(_))), "stale rows must not reach SQLite: {actions:?}");
            assert_eq!(twin.stale_writes, 0);
        }
    }
}
