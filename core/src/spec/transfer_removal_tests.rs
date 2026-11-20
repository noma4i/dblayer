//! Owner-level tests of the owner `Transfer`: the removal commands in the stages of the internal steps (D-392).

use super::transfer_harness::*;
use crate::owners::contract::ReplyId;
use crate::owners::write::outbox::{Fault, OutboxInput, ReserveTarget};
use crate::owners::write::transfer::{TransferAnswer, TransferId, TransferInput, TransferStage};

const KEY: &str = "Moment:r1:upload";
const OTHER: &str = "Message:m2:send";

#[test]
fn xfer_retry_and_discard_keep_their_order_while_failure_commits() {
    for retry_first in [true, false] {
        let mut twin = Twin::new();
        twin.persisting_failure(KEY);
        let ((retry, first), (discard, second)) = if retry_first {
            let retry = twin.retry(KEY, None);
            (retry, twin.discard(KEY))
        } else {
            let discard = twin.discard(KEY);
            (twin.retry(KEY, None), discard)
        };
        let mut outs = vec![first, second];
        outs.push(twin.commit());
        if !retry_first { assert!(answers(&outs, retry).is_empty(), "the earlier discard has not committed"); }
        for _ in 0..8 { if !twin.has_pending() { break; } outs.push(twin.commit()); }
        assert_eq!(answers(&outs, discard), vec![TransferAnswer::Ok]);
        let retried = answers(&outs, retry);
        if retry_first { assert!(matches!(retried.as_slice(), [TransferAnswer::Receipt(receipt)] if receipt.cycle_id == 2), "{retried:?}"); }
        else { assert_eq!(retried, vec![TransferAnswer::Err(Fault::NotFound)]); }
    }
}

fn copying_batch(twin: &mut Twin) -> TransferId {
    twin.post(TransferInput::AcceptBatch { joint: Box::new(joint()), epoch: EPOCH, items: vec![item(KEY, 2), item(OTHER, 2)], reply: 80 });
    let ids = [twin.state().open_id(KEY).unwrap(), twin.state().open_id(OTHER).unwrap()];
    twin.reserve_batch(&ids);
    twin.commit();
    ids[0]
}

fn persisting_batch_intent(twin: &mut Twin) -> TransferId {
    let ids = twin.accepted_batch(&[KEY, OTHER], 1);
    for _ in &ids { twin.commit(); }
    for id in &ids {
        twin.step_ok(*id, 1, None);
        twin.commit();
        twin.commit();
        twin.completed(*id);
        twin.commit();
    }
    ids[0]
}

fn at(stage: TransferStage) -> (Twin, TransferId) {
    let mut twin = Twin::with_slots(4);
    let id = match stage {
        TransferStage::Reserving => {
            let reply = twin.reply();
            twin.post(TransferInput::Accept { epoch: EPOCH, item: Box::new(item(KEY, 2)), reply });
            twin.state().open_id(KEY).unwrap()
        }
        TransferStage::Accepting => twin.accepting(KEY, 2).0,
        TransferStage::Copying => copying_batch(&mut twin),
        TransferStage::CopyReady => {
            let id = copying_batch(&mut twin);
            twin.copy_done(id);
            id
        }
        TransferStage::PersistingCopy => twin.persisting_copy(KEY, 2),
        TransferStage::PersistingAttempt => twin.accepted(KEY, 2),
        TransferStage::PersistingPart => {
            let id = twin.uploading(KEY, 2);
            twin.step_ok(id, 1, Some("u1"));
            id
        }
        TransferStage::PersistingComplete => twin.persisting_complete(KEY),
        TransferStage::PersistingBatchIntent => persisting_batch_intent(&mut twin),
        TransferStage::PersistingRetry => {
            let id = twin.failed(KEY);
            twin.retry(KEY, None);
            id
        }
        TransferStage::PersistingFailure => twin.persisting_failure(KEY),
        other => panic!("{other:?}"),
    };
    assert_eq!(twin.stage(id), stage);
    (twin, id)
}

/// The input that moves the transfer on: the end of a copy, the answer of a reservation, a part, a write of the Store. `None` once the file is on the server.
fn natural(twin: &mut Twin, id: TransferId) -> Option<Out> {
    let stage = twin.stage(id);
    let copying = twin.state().transfers.values().find(|transfer| transfer.stage == TransferStage::Copying).map(|transfer| transfer.transfer_id);
    if let Some(copy) = copying { return Some(twin.copy_done(copy)); }
    match stage {
        TransferStage::Reserving => Some(twin.post(TransferInput::OpReserved { target: ReserveTarget::Transfer(id), result: Ok(id) })),
        TransferStage::Uploading => {
            let part = twin.t(id).confirmed_parts + 1;
            Some(twin.step_ok(id, part, Some("u1")))
        }
        _ if twin.has_pending() => Some(twin.commit()),
        _ => None,
    }
}

fn answers(outs: &[Out], reply: ReplyId) -> Vec<TransferAnswer> { outs.iter().flat_map(|out| out.replies(reply)).collect() }

/// The command of the removal waits in every stage of an internal step without an answer and without `InFlight`, the exit of the stage leads to `Removing`, the commit of the removal answers it once with `true` / `Ok`, the copy is deleted and an upload that exists is owed 1 abort.
/// Observable: the answers, the batches, the effects. Catches: a command refused by the stage it met, a command lost at the exit, an answer twice.
#[test]
fn xfer_removal_waits_for_the_stage() {
    use TransferStage::*;
    for (stage, copy, debts) in [
        (Reserving, false, 0), (Accepting, false, 0), (Copying, false, 0), (CopyReady, false, 0), (PersistingCopy, true, 0), (PersistingAttempt, true, 0),
        (PersistingPart, true, 1), (PersistingComplete, true, 0), (PersistingBatchIntent, true, 0), (PersistingRetry, true, 0), (PersistingFailure, true, 0),
    ] {
        for cancel in [true, false] {
            let (mut twin, id) = at(stage);
            let (reply, first) = if cancel { twin.cancel(KEY) } else { twin.discard(KEY) };
            assert!(first.replies(reply).is_empty() && first.writes() == 0, "{stage:?} cancel {cancel}: the command waits");
            let mut outs = vec![first];
            for _ in 0..16 {
                if matches!(twin.stage(id), Removing | Mutating) { break; }
                outs.push(natural(&mut twin, id).unwrap_or_else(|| panic!("{stage:?}: no way out of {:?}", twin.stage(id))));
            }
            if twin.stage(id) == Mutating {
                let sent = outs.iter().flat_map(|out| out.outbox.iter()).filter(|input| matches!(input, OutboxInput::DiscardTransfer { .. })).count();
                let batched = stage == PersistingBatchIntent;
                let expected = match (batched, cancel) { (true, _) => vec![TransferAnswer::Err(Fault::InFlight)], (false, true) => vec![TransferAnswer::Cancelled(false)], (false, false) => Vec::new() };
                assert_eq!((answers(&outs, reply), sent), (expected, usize::from(!cancel && !batched)), "{stage:?} cancel {cancel}: the mutation left, the command meets the row of Mutating");
                continue;
            }
            assert_eq!(twin.stage(id), Removing, "{stage:?} cancel {cancel}");
            assert!(answers(&outs, reply).is_empty(), "{stage:?} cancel {cancel}: no answer before the commit of the removal");
            for _ in 0..8 {
                if !twin.has_pending() { break; }
                outs.push(twin.commit());
            }
            let answer = if cancel { TransferAnswer::Cancelled(true) } else { TransferAnswer::Ok };
            assert_eq!(answers(&outs, reply), vec![answer], "{stage:?} cancel {cancel}");
            assert_eq!(twin.stage(id), Done, "{stage:?} cancel {cancel}");
            assert_eq!(outs.iter().map(|out| out.orphans_written().len()).sum::<usize>(), debts, "{stage:?} cancel {cancel}: 1 debt of the multipart that exists");
            if copy { assert!(outs.iter().any(|out| out.deletes() > 0), "{stage:?} cancel {cancel}: the copy is deleted"); }
        }
    }
}

/// Two commands wait in a stage and a third meets the removal in flight: all three get the outcome of the one removal, the batch of the removal is 1.
/// Observable: the answers and the batches that remove the row. Catches: a second command that gets `InFlight`, a second removal batch, a lost answer.
#[test]
fn xfer_removal_joins_the_removal_in_flight() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 2);
    twin.step_ok(id, 1, Some("u1"));
    let ((cancel, first), (discard, second)) = (twin.cancel(KEY), twin.discard(KEY));
    let mut outs = vec![first, second];
    while twin.stage(id) != TransferStage::Removing { outs.push(twin.commit()); }
    let (late, third) = twin.cancel(KEY);
    outs.push(third);
    assert!([cancel, discard, late].iter().all(|reply| answers(&outs, *reply).is_empty()), "no answer before the commit of the removal");
    outs.push(twin.commit());
    assert_eq!((answers(&outs, cancel), answers(&outs, discard), answers(&outs, late)), (vec![TransferAnswer::Cancelled(true)], vec![TransferAnswer::Ok], vec![TransferAnswer::Cancelled(true)]));
    assert_eq!(outs.iter().map(|out| out.removed().len()).sum::<usize>(), 1, "one batch removes the row");
}

/// P-XFER-78: a command of the removal at every point of the trace of a transfer from the acceptance to the last part gets `true` / `Ok` once, never `InFlight`, the transfer ends and an upload that exists is owed 1 abort.
#[test]
fn xfer_prop_removal_at_any_point() {
    for cancel in [true, false] {
        let mut point = 0;
        loop {
            let mut twin = Twin::with_slots(4);
            let reply = twin.reply();
            let mut outs = vec![twin.post(TransferInput::Accept { epoch: EPOCH, item: Box::new(item(KEY, 3)), reply })];
            let id = twin.state().open_id(KEY).unwrap();
            let mut ended = false;
            for _ in 0..point {
                match natural(&mut twin, id) {
                    Some(out) => outs.push(out),
                    None => { ended = true; break; }
                }
            }
            if ended { break; }
            let owed = twin.state().transfers.get(&id).is_some_and(|transfer| transfer.upload_id.is_some());
            let (command, out) = if cancel { twin.cancel(KEY) } else { twin.discard(KEY) };
            outs.push(out);
            for _ in 0..16 {
                if twin.stage(id) == TransferStage::Done { break; }
                match natural(&mut twin, id) {
                    Some(out) => outs.push(out),
                    None => break,
                }
            }
            let answer = if cancel { TransferAnswer::Cancelled(true) } else { TransferAnswer::Ok };
            assert_eq!(answers(&outs, command), vec![answer], "point {point} cancel {cancel}");
            assert_eq!(twin.stage(id), TransferStage::Done, "point {point} cancel {cancel}");
            assert_eq!(outs.iter().map(|out| out.orphans_written().len()).sum::<usize>(), usize::from(owed), "point {point} cancel {cancel}: 1 debt of the multipart that exists");
            point += 1;
        }
        assert!(point > 8, "the trace has points in many stages");
    }
}
