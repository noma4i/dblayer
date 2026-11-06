//! Property of the owner `Transfer`: the work of an input grows with the open transfers, not with the finished ones.

use std::sync::atomic::Ordering;

use super::transfer_harness::*;
use crate::owners::write::outbox::OutboxOutcome;

fn examined(twin: &Twin) -> u64 { twin.state().examined.load(Ordering::Relaxed) }

/// The work of the same trace (accept, upload, mutation, outcome) after `finished` transfers ended in the epoch.
fn work_after(finished: usize) -> (u64, usize) {
    let mut twin = Twin::new();
    for round in 0..finished {
        let key = format!("Moment:done{round}:upload");
        twin.mutating(&key);
        twin.mutation_outcome(&key, OutboxOutcome::Confirmed { revision: 1, result: None });
        twin.commit();
    }
    let open = twin.state().transfers.len();
    let before = examined(&twin);
    let key = "Moment:fresh:upload";
    let id = twin.mutating(key);
    twin.mutation_outcome(key, OutboxOutcome::Confirmed { revision: 2, result: None });
    twin.commit();
    assert_eq!(twin.stage(id), crate::owners::write::transfer::TransferStage::Done);
    (examined(&twin) - before, open)
}

#[test]
fn xfer_prop_work_by_open() {
    let (small, open_small) = work_after(3);
    let (large, open_large) = work_after(6);
    assert_eq!((open_small, open_large), (0, 0), "a finished transfer leaves the map of the owner");
    assert!(small > 0, "the counter sees the scans");
    assert_eq!(small, large, "the work of a trace does not depend on the number of finished transfers");
}
