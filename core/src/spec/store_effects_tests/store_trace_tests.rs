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
                twin.commit_batch(Batch::rows(epoch, vec![("Moment".into(), format!("r{step}"))]), !next(&mut seed).is_multiple_of(6));
            }
            3 => { twin.clear_account(Cause::IdentityChanged, twin.state().generation() + 1 + next(&mut seed) % 2, !next(&mut seed).is_multiple_of(4)); }
            4 => {
                let causes = [Cause::SessionReset, Cause::ServerEnded, Cause::IdentityChanged, Cause::SessionStarted, Cause::Close];
                let cause = causes[(next(&mut seed) % causes.len() as u64) as usize];
                twin.clear_account(cause, twin.state().generation() + 1, !next(&mut seed).is_multiple_of(4));
            }
            5 => { let (epoch, generation) = (twin.state().epoch(), twin.state().generation()); twin.publish(epoch + 1, generation); }
            6 => { let epoch = twin.state().epoch(); twin.feed(Request::EpochEnded { epoch }); }
            7 => { twin.feed(Request::ReadIntents { epoch: twin.state().epoch() }); }
            _ => { twin.feed(Request::ReadFinished(next(&mut seed) % 3)); }
        }
    }
    twin
}

/// In any trace: the revision grows by 1 per commit, no batch of another epoch reaches SQLite,
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
        assert!(twin.generation_history.windows(2).all(|pair| pair[0] <= pair[1]), "seed {seed}: generation went down: {:?}", twin.generation_history);
    }
}
