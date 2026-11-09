//! Owner-level tests of the owner `Transfer`: `Mutating`, the final mutation in Outbox and its outcomes.

use super::transfer_harness::*;
use crate::owners::contract::Online;
use crate::owners::write::outbox::{Fault, OutboxOutcome};
use crate::owners::write::transfer::{Shown, TransferAnswer, TransferId, TransferInput, TransferStage};

const KEY: &str = "Moment:r1:upload";
const OTHER: &str = "Moment:r2:upload";

fn confirmed(revision: i64) -> OutboxOutcome { OutboxOutcome::Confirmed { revision, result: None, row_id: None } }

fn failed(error: &str) -> OutboxOutcome { OutboxOutcome::Failed { error: error.into() } }

#[test]
fn xfer_mutation_confirmed() {
    let mut twin = Twin::new();
    let id = twin.mutating(KEY);
    let out = twin.mutation_outcome(KEY, confirmed(3));
    assert_eq!((twin.stage(id), out.removed(), out.all_replies()), (TransferStage::Removing, vec![KEY.to_string()], 0));
    let out = twin.commit();
    assert_eq!((twin.stage(id), out.deletes(), out.shows(), out.round_idle()), (TransferStage::Done, 1, vec![Shown::Gone], vec![id]));
    let (reply, out) = twin.outcome(id, 1);
    assert_eq!(out.replies(reply), vec![TransferAnswer::Outcome(confirmed(3), None)]);
}

#[test]
fn xfer_mutation_failed() {
    let mut twin = Twin::new();
    let id = twin.mutating(KEY);
    let out = twin.mutation_outcome(KEY, failed("e"));
    assert_eq!((twin.stage(id), out.kinds()), (TransferStage::PersistingFailure, vec![crate::owners::store::UploadKind::Failed]));
    let out = twin.commit();
    assert_eq!((twin.stage(id), out.shows()), (TransferStage::Failed, vec![Shown::Failed { error: "e".into() }]));
    let (reply, out) = twin.outcome(id, 1);
    assert_eq!(out.replies(reply), vec![TransferAnswer::Outcome(failed("e"), None)]);
}

#[test]
fn xfer_mutation_rejected_discard_waiting() {
    let mut twin = Twin::new();
    let id = twin.mutating(KEY);
    let (reply, out) = twin.discard(KEY);
    assert_eq!((out.outbox_discards(), out.all_replies()), (1, 0));
    let out = twin.mutation_outcome(KEY, OutboxOutcome::Rejected { error: "e".into() });
    assert_eq!((twin.stage(id), out.removed(), out.all_replies()), (TransferStage::Removing, vec![KEY.to_string()], 0), "the rollback is done by Outbox: nothing is left to undo");
    let out = twin.commit();
    assert_eq!((out.replies(reply), twin.stage(id)), (vec![TransferAnswer::Ok], TransferStage::Done));
}

#[test]
fn xfer_mutation_failed_discard_waiting() {
    let mut twin = Twin::new();
    let id = twin.mutating(KEY);
    twin.discard(KEY);
    let out = twin.mutation_outcome(KEY, failed("e"));
    assert!(Twin::quiet(&out), "the discard sent to Outbox decides: {out:?}");
    assert_eq!(twin.stage(id), TransferStage::Mutating);
}

fn failed_mutation(twin: &mut Twin) -> TransferId {
    let id = twin.mutating(KEY);
    twin.mutation_outcome(KEY, failed("e"));
    twin.commit();
    assert_eq!(twin.stage(id), TransferStage::Failed);
    id
}

#[test]
fn xfer_failed_discard_live_intent() {
    let mut twin = Twin::new();
    let id = failed_mutation(&mut twin);
    let (reply, out) = twin.discard(KEY);
    assert_eq!((out.outbox_discards(), out.all_replies(), out.removed(), twin.stage(id)), (1, 0, Vec::<String>::new(), TransferStage::Failed), "the live intent is discarded by Outbox, Transfer does not remove it behind its back");
    let out = twin.post(TransferInput::DiscardOutcome { key: KEY.into(), result: Ok(()) });
    assert!(Twin::quiet(&out), "{out:?}");
    let out = twin.mutation_outcome(KEY, OutboxOutcome::Discarded);
    assert_eq!((twin.stage(id), out.removed()), (TransferStage::Removing, vec![KEY.to_string()]));
    assert_eq!((twin.commit().replies(reply), twin.stage(id)), (vec![TransferAnswer::Ok], TransferStage::Done));
}

#[test]
fn xfer_failed_cancel_live_intent() {
    let mut twin = Twin::new();
    let id = failed_mutation(&mut twin);
    let (reply, out) = twin.cancel(KEY);
    assert_eq!((out.outbox_discards(), out.all_replies(), out.removed(), twin.stage(id)), (1, 0, Vec::<String>::new(), TransferStage::Failed), "the live intent is discarded by Outbox, Transfer does not remove it behind its back");
    let out = twin.mutation_outcome(KEY, OutboxOutcome::Discarded);
    assert_eq!((twin.stage(id), out.removed()), (TransferStage::Removing, vec![KEY.to_string()]));
    assert_eq!((twin.commit().replies(reply), twin.stage(id)), (vec![TransferAnswer::Cancelled(true)], TransferStage::Done));
}

#[test]
fn xfer_failed_discard_live_intent_refused() {
    let mut twin = Twin::new();
    let id = failed_mutation(&mut twin);
    let (reply, _) = twin.discard(KEY);
    let out = twin.post(TransferInput::DiscardOutcome { key: KEY.into(), result: Err(Fault::InFlight) });
    assert_eq!((out.replies(reply), twin.stage(id)), (vec![TransferAnswer::Err(Fault::InFlight)], TransferStage::Failed));
    let (_, out) = twin.discard(KEY);
    assert_eq!(out.outbox_discards(), 1, "a new discard goes to Outbox again");
}

#[test]
fn xfer_mutation_discarded() {
    let mut twin = Twin::new();
    let id = twin.mutating(KEY);
    let (reply, _) = twin.discard(KEY);
    let out = twin.mutation_outcome(KEY, OutboxOutcome::Discarded);
    assert_eq!((twin.stage(id), out.removed()), (TransferStage::Removing, vec![KEY.to_string()]));
    let out = twin.commit();
    assert_eq!((out.replies(reply), twin.stage(id)), (vec![TransferAnswer::Ok], TransferStage::Done));
    let (reply, out) = twin.outcome(id, 1);
    assert_eq!(out.replies(reply), vec![TransferAnswer::Outcome(OutboxOutcome::Discarded, None)]);
}

#[test]
fn xfer_mutating_discard_refused() {
    let mut twin = Twin::new();
    let id = twin.mutating(KEY);
    let (reply, _) = twin.discard(KEY);
    let out = twin.post(TransferInput::DiscardOutcome { key: KEY.into(), result: Err(Fault::InFlight) });
    assert_eq!(out.replies(reply), vec![TransferAnswer::Err(Fault::InFlight)], "Transfer answers the call itself");
    assert_eq!(twin.stage(id), TransferStage::Mutating);
    let (_, out) = twin.discard(KEY);
    assert_eq!(out.outbox_discards(), 1, "a new discard goes to Outbox again");
}

#[test]
fn xfer_mutating_discard_accepted() {
    let mut twin = Twin::new();
    let id = twin.mutating(KEY);
    let (reply, _) = twin.discard(KEY);
    let out = twin.post(TransferInput::DiscardOutcome { key: KEY.into(), result: Ok(()) });
    assert!(Twin::quiet(&out), "{out:?}");
    assert_eq!(twin.stage(id), TransferStage::Mutating);
    twin.mutation_outcome(KEY, OutboxOutcome::Discarded);
    assert_eq!(twin.commit().replies(reply), vec![TransferAnswer::Ok]);
}

#[test]
fn xfer_mutation_interrupted() {
    let mut twin = Twin::new();
    let id = twin.mutating(KEY);
    let out = twin.mutation_outcome(KEY, OutboxOutcome::Ended(crate::ffi::EndCause::SessionEnded));
    assert_eq!((twin.stage(id), out.writes(), out.shows()), (TransferStage::Done, 0, vec![Shown::Gone]), "the row stays in the base until the account is cleared or the next open");
    let (reply, out) = twin.outcome(id, 1);
    assert_eq!(out.replies(reply), vec![TransferAnswer::Outcome(OutboxOutcome::Ended(crate::ffi::EndCause::SessionEnded), None)]);
}

#[test]
fn xfer_mutating_cancel() {
    let mut twin = Twin::new();
    let id = twin.mutating(KEY);
    let (reply, out) = twin.cancel(KEY);
    assert_eq!((out.replies(reply), twin.stage(id)), (vec![TransferAnswer::Cancelled(false)], TransferStage::Mutating), "the mutation is already on its way");
    let mut batch = Twin::with_slots(2);
    let ids = batch.batch_mutating(&[KEY, OTHER]);
    let (reply, out) = batch.cancel(KEY);
    assert_eq!((Twin::fault(&out, reply), batch.stage(ids[0])), (Some(Fault::InFlight), TransferStage::Mutating));
}

#[test]
fn xfer_mutating_discard() {
    let mut twin = Twin::new();
    let id = twin.mutating(KEY);
    let (_, out) = twin.discard(KEY);
    assert_eq!((out.outbox_discards(), out.all_replies()), (1, 0));
    let (_, out) = twin.discard(KEY);
    assert_eq!((out.outbox_discards(), out.all_replies(), twin.t(id).discard_waiters.len()), (0, 0, 2), "one discard goes to Outbox, both calls wait");
}

#[test]
fn xfer_mutating_retry() {
    let mut twin = Twin::new();
    let id = twin.mutating(KEY);
    let (reply, out) = twin.retry(KEY, None);
    assert!(matches!(out.replies(reply).as_slice(), [TransferAnswer::Receipt(receipt)] if receipt.op_id == id));
    assert_eq!(twin.stage(id), TransferStage::Mutating);
}

#[test]
fn xfer_mutating_other() {
    let mut twin = Twin::new();
    let id = twin.mutating(KEY);
    let attempt_id = twin.attempt(id);
    let mut inputs = twin.stray_results(id);
    inputs.extend([
        TransferInput::StepOk { transfer_id: id, attempt_id, part: Some(1), upload_id: None, etag: None },
        TransferInput::Completed { transfer_id: id, attempt_id, signed_id: None },
        TransferInput::Online(Online(true)),
        TransferInput::PowerSaving(true),
        TransferInput::SlotFreed,
    ]);
    twin.ignores(id, inputs);
}

#[test]
fn xfer_mutation_outcome_stale_cycle() {
    let mut twin = Twin::new();
    let id = twin.mutating(KEY);
    let current = twin.t(id).cycle_id;
    let out = twin.post(TransferInput::MutationOutcome { transfer_id: id, cycle_id: current + 1, intent_removed: true, outcome: confirmed(3) });
    assert_eq!((twin.stage(id), out.batches.len(), out.all_replies()), (TransferStage::Mutating, 0, 0), "an outcome of another cycle is a foreign input (R2)");
    let out = twin.mutation_outcome(KEY, confirmed(3));
    assert_eq!((twin.stage(id), out.removed()), (TransferStage::Removing, vec![KEY.to_string()]));
}
