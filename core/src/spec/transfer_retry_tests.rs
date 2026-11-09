//! Owner-level tests of the owner `Transfer`: `Failed x Retry` of a mutation whose intent is gone or alive, and `RetryOutcome` of Outbox.

use super::transfer_harness::*;
use crate::owners::store::StoreOp;
use crate::owners::write::outbox::{Fault, OpSource, OutboxOutcome};
use crate::owners::write::transfer::{TransferAnswer, TransferId, TransferInput, TransferStage};

const KEY: &str = "Moment:r1:upload";
const OTHER: &str = "Moment:r2:upload";

/// A single transfer whose mutation was rejected by the server: the intent is gone, the file is on the server.
fn rejected_single() -> (Twin, TransferId) {
    let mut twin = Twin::new();
    let id = twin.mutating(KEY);
    twin.mutation_outcome(KEY, OutboxOutcome::Rejected { error: "no".into() });
    twin.commit();
    assert_eq!(twin.stage(id), TransferStage::Failed);
    (twin, id)
}

/// A single transfer whose mutation failed with the intent alive: Retry goes through Outbox.
fn failed_single() -> (Twin, TransferId) {
    let mut twin = Twin::new();
    let id = twin.mutating(KEY);
    twin.mutation_outcome(KEY, OutboxOutcome::Failed { error: "boom".into() });
    twin.commit();
    assert_eq!(twin.stage(id), TransferStage::Failed);
    (twin, id)
}

fn intent_ops(out: &Out) -> Vec<u64> {
    out.batches.iter().flat_map(|batch| batch.ops.iter()).filter_map(|op| if let StoreOp::Intent { op_id, .. } = op { Some(*op_id) } else { None }).collect()
}

fn intent_cycles(out: &Out) -> Vec<(u32, u64)> {
    out.batches.iter().flat_map(|batch| batch.ops.iter()).filter_map(|op| if let StoreOp::Intent { attempts, cycle_id, .. } = op { Some((*attempts, *cycle_id)) } else { None }).collect()
}

fn recreated(twin: &mut Twin, id: TransferId, keyed: &str) {
    let (reply, out) = twin.retry(keyed, None);
    assert_eq!((twin.stage(id), out.writes(), out.outbox_retries()), (TransferStage::PersistingRetry, 1, 0));
    assert!(out.replies(reply).is_empty(), "nothing before Committed");
    assert_eq!(out.kinds(), vec![crate::owners::store::UploadKind::Retrying]);
    let out = twin.commit();
    assert!(matches!(out.replies(reply).as_slice(), [TransferAnswer::Receipt(receipt)] if receipt.cycle_id == 2), "{:?}", out.replies(reply));
    assert_eq!((twin.stage(id), twin.t(id).cycle_id), (TransferStage::PersistingBatchIntent, 2));
    assert_eq!(out.writes(), 1);
    assert_eq!(intent_ops(&out), vec![id], "the reserved op_id of the element");
    assert_eq!(intent_cycles(&out), vec![(1, 2)], "the first attempt of the repeated cycle");
    assert!(out.submit_ops().is_empty());
    let out = twin.commit();
    assert_eq!(twin.stage(id), TransferStage::Mutating);
    assert_eq!(out.submit_ops().len(), 1);
    assert_eq!(out.submit_ops()[0].origin, OpSource::Transfer { transfer_id: id, cycle_id: 2 });
    assert_eq!(out.outbox_retries(), 0, "no Retry of the removed intent");
}

#[test]
fn xfer_retry_removed_intent() {
    let (mut twin, id) = rejected_single();
    recreated(&mut twin, id, KEY);
}

#[test]
fn xfer_batch_retry_missing() {
    let mut twin = Twin::with_slots(4);
    let ids = twin.batch_mutating(&[KEY, OTHER]);
    twin.mutation_outcome("batch:1", OutboxOutcome::Rejected { error: "no".into() });
    twin.commit();
    assert!(ids.iter().all(|id| twin.stage(*id) == TransferStage::Failed));
    recreated(&mut twin, ids[0], KEY);
    assert_eq!(twin.stage(ids[1]), TransferStage::Failed, "the other element waits for its own retry");
}

#[test]
fn xfer_retry_mutation_outcome() {
    let (mut twin, id) = failed_single();
    let (reply, out) = twin.retry(KEY, None);
    assert_eq!((twin.stage(id), out.outbox_retries(), out.writes()), (TransferStage::PersistingRetry, 1, 0));
    let out = twin.post(TransferInput::RetryOutcome { cycles: vec![(id, 2)], result: Ok(4) });
    assert_eq!((twin.stage(id), twin.t(id).cycle_id), (TransferStage::Mutating, 2));
    assert!(matches!(out.replies(reply).as_slice(), [TransferAnswer::Receipt(receipt)] if receipt.cycle_id == 2));
    assert_eq!((out.submits(), out.writes()), (0, 0));
}

#[test]
fn xfer_retry_mutation_rejected() {
    let (mut twin, id) = failed_single();
    let (reply, _) = twin.retry(KEY, None);
    let out = twin.post(TransferInput::RetryOutcome { cycles: vec![(id, 2)], result: Err(Fault::Invalid) });
    assert_eq!((twin.stage(id), twin.t(id).cycle_id), (TransferStage::Failed, 1));
    assert_eq!(Twin::fault(&out, reply), Some(Fault::Invalid));
    let (reply, out) = twin.outcome(id, 1);
    assert_eq!(out.replies(reply), vec![TransferAnswer::Outcome(OutboxOutcome::Failed { error: "boom".into() }, None)], "the old cycle keeps its outcome");
}

#[test]
fn xfer_retry_mutation_stale() {
    let (mut twin, id) = failed_single();
    let strays = [vec![(id, 2)], vec![(id + 50, 2)]];
    for cycles in strays {
        let out = twin.post(TransferInput::RetryOutcome { cycles, result: Ok(4) });
        assert!(Twin::quiet(&out), "{out:?}");
        assert_eq!(twin.stage(id), TransferStage::Failed);
    }
    twin.retry(KEY, None);
    for cycles in [vec![(id, 5)], vec![(id + 50, 2)], vec![(id, 2), (id + 50, 2)]] {
        let out = twin.post(TransferInput::RetryOutcome { cycles, result: Ok(4) });
        assert!(Twin::quiet(&out), "{out:?}");
        assert_eq!(twin.stage(id), TransferStage::PersistingRetry);
    }
    let (mut twin, id) = rejected_single();
    twin.retry(KEY, None);
    let out = twin.post(TransferInput::RetryOutcome { cycles: vec![(id, 2)], result: Ok(4) });
    assert!(Twin::quiet(&out), "no wait for Outbox: {out:?}");
    assert_eq!(twin.stage(id), TransferStage::PersistingRetry);
}
