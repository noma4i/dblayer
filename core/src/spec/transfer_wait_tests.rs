//! Owner-level tests of the owner `Transfer`: the waits of the calls. Each call gets exactly one answer: success, refusal, end of the epoch or close.

use super::transfer_harness::*;
use crate::owners::write::outbox::{Fault, OutboxOutcome};
use crate::owners::write::transfer::{TransferAnswer, TransferInput, TransferStage};
use crate::runtime::Time;

const KEY: &str = "Moment:r1:upload";
const OTHER: &str = "Moment:r2:upload";

fn interrupted() -> Vec<TransferAnswer> { vec![TransferAnswer::Err(Fault::Interrupted)] }

#[test]
fn xfer_wait_accept() {
    let mut twin = Twin::offline();
    let (id, reply, _) = twin.accepting(KEY, 2);
    twin.commit();
    twin.copy_done(id);
    assert_eq!(twin.commit().replies(reply).len(), 1, "success: the receipt");
    let mut ended = Twin::new();
    let (_, reply, _) = ended.accepting(KEY, 2);
    assert_eq!(ended.end_epoch().replies(reply), interrupted(), "EpochEnded");
    let mut closing = Twin::new();
    let (_, reply, _) = closing.accepting(KEY, 2);
    closing.commit();
    assert_eq!(closing.close().replies(reply), interrupted(), "Closing");
    let mut refused = Twin::new();
    let out = refused.post(TransferInput::Accept { epoch: EPOCH + 1, item: Box::new(item(KEY, 2)), reply: 9, now: Time(0) });
    assert_eq!(out.replies(9), vec![TransferAnswer::Err(Fault::NoSession)], "refusal");
}

#[test]
fn xfer_wait_accept_batch() {
    let batch = |twin: &mut Twin| {
        twin.post(TransferInput::AcceptBatch { joint: Box::new(joint()), epoch: EPOCH, items: vec![item(KEY, 2), item(OTHER, 2)], reply: 5, now: Time(0) });
    };
    let mut ok = Twin::offline();
    batch(&mut ok);
    ok.commit();
    for key in [KEY, OTHER] {
        let id = ok.state().open_id(key).expect("an element");
        ok.copy_done(id);
    }
    let out = ok.commit();
    assert!(matches!(out.replies(5).as_slice(), [TransferAnswer::Receipts(receipts)] if receipts.len() == 2), "N receipts in one answer");
    let mut ended = Twin::new();
    batch(&mut ended);
    assert_eq!(ended.end_epoch().replies(5), interrupted(), "one answer for the batch");
    let mut closing = Twin::new();
    batch(&mut closing);
    closing.commit();
    assert_eq!(closing.close().replies(5), interrupted());
}

#[test]
fn xfer_wait_cancel() {
    let mut twin = Twin::offline();
    twin.accepted(KEY, 2);
    let (reply, _) = twin.cancel(KEY);
    assert_eq!(twin.commit().replies(reply), vec![TransferAnswer::Cancelled(true)], "success");
    let mut busy = Twin::new();
    busy.persisting_copy(KEY, 2);
    let (reply, out) = busy.cancel(KEY);
    assert_eq!(Twin::fault(&out, reply), Some(Fault::InFlight), "refusal");
    let mut ended = Twin::offline();
    ended.accepted(KEY, 2);
    let (reply, _) = ended.cancel(KEY);
    assert_eq!(ended.end_epoch().replies(reply), interrupted(), "EpochEnded while the removal waits");
    let mut closing = Twin::offline();
    closing.accepted(KEY, 2);
    let (reply, _) = closing.cancel(KEY);
    assert_eq!(closing.close().replies(reply), interrupted(), "Closing while the removal waits");
}

#[test]
fn xfer_wait_retry() {
    let mut twin = Twin::new();
    twin.failed(KEY);
    let (reply, _) = twin.retry(KEY, None);
    assert!(matches!(twin.commit().replies(reply).as_slice(), [TransferAnswer::Receipt(_)]), "success");
    let (reply, out) = twin.retry("Moment:none:upload", None);
    assert_eq!(Twin::fault(&out, reply), Some(Fault::NotFound), "refusal");
    let mut ended = Twin::new();
    ended.failed(KEY);
    let (reply, _) = ended.retry(KEY, None);
    assert_eq!(ended.end_epoch().replies(reply), interrupted(), "EpochEnded");
    let mut closing = Twin::new();
    closing.failed(KEY);
    let (reply, _) = closing.retry(KEY, None);
    assert_eq!(closing.close().replies(reply), interrupted(), "Closing");
}

#[test]
fn xfer_wait_discard() {
    let mut twin = Twin::offline();
    twin.accepted(KEY, 2);
    let (reply, _) = twin.discard(KEY);
    assert_eq!(twin.commit().replies(reply), vec![TransferAnswer::Ok], "success");
    let mut mutating = Twin::new();
    mutating.mutating(KEY);
    let (reply, out) = mutating.discard(KEY);
    assert!(out.replies(reply).is_empty(), "in Mutating the answer waits for the outcome of Outbox");
    mutating.mutation_outcome(KEY, OutboxOutcome::Discarded);
    assert_eq!(mutating.commit().replies(reply), vec![TransferAnswer::Ok]);
    let (reply, out) = mutating.discard("Moment:none:upload");
    assert_eq!(Twin::fault(&out, reply), Some(Fault::NotFound), "refusal");
    let mut ended = Twin::new();
    ended.mutating(KEY);
    let (reply, _) = ended.discard(KEY);
    assert_eq!(ended.end_epoch().replies(reply), interrupted(), "EpochEnded");
    let mut closing = Twin::new();
    closing.mutating(KEY);
    let (reply, _) = closing.discard(KEY);
    assert_eq!(closing.close().replies(reply), interrupted(), "Closing");
}

#[test]
fn xfer_wait_outcome() {
    let mut twin = Twin::new();
    let id = twin.mutating(KEY);
    let (reply, _) = twin.outcome(id, 1);
    twin.mutation_outcome(KEY, OutboxOutcome::Confirmed { revision: 2, result: None });
    assert_eq!(twin.commit().replies(reply), vec![TransferAnswer::Outcome(OutboxOutcome::Confirmed { revision: 2, result: None })], "Confirmed at Done");
    let mut failed = Twin::new();
    let id = failed.uploading(KEY, 2);
    let (reply, _) = failed.outcome(id, 1);
    failed.step_error(id, false);
    assert_eq!(failed.commit().replies(reply), vec![TransferAnswer::Outcome(OutboxOutcome::Failed { error: "failed".into() })], "Failed in Failed");
    let mut ended = Twin::new();
    let id = ended.uploading(KEY, 2);
    let (reply, _) = ended.outcome(id, 1);
    assert_eq!(ended.end_epoch().replies(reply), vec![TransferAnswer::Outcome(OutboxOutcome::Interrupted)], "EpochEnded");
    let mut closing = Twin::new();
    let id = closing.uploading(KEY, 2);
    let (reply, _) = closing.outcome(id, 1);
    assert_eq!(closing.close().replies(reply), vec![TransferAnswer::Outcome(OutboxOutcome::Interrupted)], "Closing");
    assert_eq!(closing.stage(id), TransferStage::Done);
}
