//! Owner-level tests of the owner `Transfer` for a transfer whose target row does not exist at the acceptance (D-287): the transfer is addressed by `transfer_id` and `cycle_id`, never by the row.

use std::sync::Arc;

use super::transfer_harness::*;
use crate::owners::store::StoreOp;
use crate::owners::write::outbox::{Fault, OutboxInput, OutboxOutcome, ReserveTarget};
use crate::owners::write::transfer::{Shown, TransferAnswer, TransferId, TransferInput, TransferStage};

const KEY: &str = "CurrentUser:me:upload_photo";
const OTHER: &str = "CurrentUser:other:upload_photo";

fn item(key: &str, parts: u32) -> crate::owners::write::transfer::BatchItem {
    let mut item = super::transfer_harness::item(key, parts);
    item.mutation.effects = Default::default();
    item
}

fn accept(twin: &mut Twin, key: &str, parts: u32) -> (TransferId, u64, Out) {
    let reply = twin.reply();
    let out = twin.post(TransferInput::Accept { epoch: EPOCH, item: Box::new(item(key, parts)), reply });
    let id = twin.state().open_id(key).expect("a transfer with no target row is accepted");
    (id, reply, out)
}

fn reserve(twin: &mut Twin, id: TransferId) -> Out { twin.post(TransferInput::OpReserved { target: ReserveTarget::Transfer(id), result: Ok(id) }) }

fn uploading(twin: &mut Twin, key: &str, parts: u32) -> (TransferId, u64) {
    let (id, reply, _) = accept(twin, key, parts);
    reserve(twin, id);
    twin.commit();
    twin.copy_done(id);
    twin.commit();
    if twin.stage(id) == TransferStage::PersistingAttempt { twin.commit(); }
    assert_eq!(twin.stage(id), TransferStage::Uploading);
    (id, reply)
}

fn mutating(twin: &mut Twin, key: &str) -> (TransferId, Out) {
    let (id, _) = uploading(twin, key, 1);
    twin.step_ok(id, 1, None);
    twin.commit();
    twin.commit();
    twin.completed(id);
    let out = twin.commit();
    assert_eq!(twin.stage(id), TransferStage::Mutating);
    (id, out)
}

fn failed(twin: &mut Twin, key: &str) -> TransferId {
    let (id, _) = uploading(twin, key, 2);
    twin.step_error(id, false);
    twin.commit();
    assert_eq!(twin.stage(id), TransferStage::Failed);
    id
}

fn landed(id: &str) -> crate::store::Row {
    Arc::new(crate::store::Record { id: crate::identity::RowId::new(id).unwrap(), revision: 3, data: serde_json::Map::from_iter([("id".to_string(), serde_json::json!(id))]), sync: Default::default() })
}

fn committed_with(twin: &mut Twin, shown: Vec<Option<crate::store::Row>>) -> Out {
    twin.answer(|batch| TransferInput::Committed(crate::owners::contract::Committed { batch, revision: 4, tables: Vec::new(), rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown, files_removed: 0, refused: Vec::new() }))
}

#[test]
fn xfer_copy_rowless() {
    let mut twin = Twin::new();
    let (id, reply, _) = accept(&mut twin, KEY, 1);
    reserve(&mut twin, id);
    let copy = twin.commit();
    assert_eq!((twin.stage(id), copy.copies()), (TransferStage::Copying, 1));
    let stored = twin.copy_done(id);
    assert_eq!(twin.stage(id), TransferStage::PersistingCopy);
    assert!(stored.batches.iter().flat_map(|batch| &batch.ops).all(|op| !matches!(op, StoreOp::Effects { .. })));
    let accepted = twin.commit();
    assert!(matches!(accepted.replies(reply).as_slice(), [TransferAnswer::Receipt(receipt)] if receipt.optimistic.is_none()));
}

#[test]
fn xfer_rowless_mutation_lands() {
    for has_row in [false, true] {
        let mut twin = Twin::new();
        let (id, _) = mutating(&mut twin, KEY);
        let row_id = has_row.then(|| "u1".to_string());
        let outcome = OutboxOutcome::Confirmed { revision: 3, result: Some("{}".into()), row_id };
        twin.mutation_outcome(KEY, outcome.clone());
        committed_with(&mut twin, has_row.then(|| Some(landed("u1"))).into_iter().collect());
        let (reply, out) = twin.outcome(id, 1);
        assert_eq!(out.replies(reply), vec![TransferAnswer::Outcome(outcome, has_row.then(|| landed("u1")))]);
    }
}

#[test]
fn xfer_rowless_restore() {
    let mut row = stored(1, KEY, 1);
    row.completed = true;
    row.signed_id = Some("signed".into());
    row.has_intent = true;
    row.has_operation = false;
    row.mutation.effects = Default::default();
    let (mut twin, _) = Twin::opened(vec![row], Default::default());
    twin.post(TransferInput::RestoreReady { epoch: EPOCH });
    assert_eq!(twin.stage(1), TransferStage::Mutating);
    let outcome = OutboxOutcome::Confirmed { revision: 3, result: None, row_id: None };
    twin.mutation_outcome(KEY, outcome.clone());
    twin.commit();
    let (reply, out) = twin.outcome(1, 1);
    assert_eq!(out.replies(reply), vec![TransferAnswer::Outcome(outcome, None)]);
}

#[test]
fn xfer_accept_check_order_without_row() {
    let invalid = || { let mut broken = item(KEY, 2); broken.source.as_mut().unwrap().path.clear(); Box::new(broken) };
    let mut closing = Twin::new();
    closing.close();
    let out = closing.post(TransferInput::Accept { epoch: EPOCH + 1, item: invalid(), reply: 7 });
    assert_eq!(Twin::fault(&out, 7), Some(Fault::Ended(crate::ffi::EndCause::Closed)), "Closed first among Closed, NoSession and Invalid");
    let mut twin = Twin::new();
    let out = twin.post(TransferInput::Accept { epoch: EPOCH + 1, item: invalid(), reply: 8 });
    assert_eq!(Twin::fault(&out, 8), Some(Fault::NoSession), "NoSession before Invalid");
    let out = twin.post(TransferInput::Accept { epoch: EPOCH, item: invalid(), reply: 9 });
    assert_eq!((Twin::fault(&out, 9), out.writes(), twin.state().open_id(KEY)), (Some(Fault::Invalid), 0, None), "Invalid, never a missing row");
}

#[test]
fn xfer_rowless_confirmed() {
    let mut twin = Twin::new();
    let (id, _) = mutating(&mut twin, KEY);
    let confirmed = OutboxOutcome::Confirmed { revision: 3, result: Some(r#"{"status":"GOOD"}"#.into()), row_id: None };
    let out = twin.mutation_outcome(KEY, confirmed.clone());
    assert_eq!((twin.stage(id), out.removed()), (TransferStage::Removing, vec![KEY.to_string()]));
    let out = twin.commit();
    assert_eq!((twin.stage(id), out.shows()), (TransferStage::Done, vec![Shown::Gone]));
    let (reply, out) = twin.outcome(id, 1);
    assert_eq!(out.replies(reply), vec![TransferAnswer::Outcome(confirmed, None)], "no landed row: the answer alone");
}

#[test]
fn xfer_rowless_cancel() {
    let mut twin = Twin::new();
    let (id, _, _) = accept(&mut twin, KEY, 2);
    reserve(&mut twin, id);
    twin.commit();
    let (reply, out) = twin.cancel(KEY);
    assert_eq!((Twin::fault(&out, reply), out.copy_cancels()), (None, 1), "a missing row does not make a cancel NotFound, the copy stops");
    let mut second = Twin::new();
    let id = failed(&mut second, KEY);
    let (reply, _) = second.cancel(KEY);
    assert_eq!((second.commit().replies(reply), second.stage(id)), (vec![TransferAnswer::Cancelled(true)], TransferStage::Done));
}

#[test]
fn xfer_rowless_discard() {
    let mut twin = Twin::new();
    let id = failed(&mut twin, KEY);
    let (reply, out) = twin.discard(KEY);
    assert_eq!((twin.stage(id), out.removed(), out.outbox_discards()), (TransferStage::Removing, vec![KEY.to_string()], 0), "no intent: Outbox is not asked, the transfer alone goes");
    assert_eq!(twin.commit().replies(reply), vec![TransferAnswer::Ok]);
    let mut live = Twin::new();
    let (id, _) = mutating(&mut live, KEY);
    live.mutation_outcome(KEY, OutboxOutcome::Failed { error: "boom".into() });
    live.commit();
    let (_, out) = live.discard(KEY);
    assert!(matches!(out.outbox.as_slice(), [OutboxInput::DiscardTransfer { transfer_id, cycle_id: 1, .. }] if *transfer_id == id), "a live intent is discarded by the identity of the transfer and its cycle: {:?}", out.outbox);
}

#[test]
fn xfer_rowless_retry() {
    let mut twin = Twin::new();
    let id = failed(&mut twin, KEY);
    let (reply, out) = twin.retry(KEY, None);
    assert_eq!(Twin::fault(&out, reply), None, "a retry never reads the missing row");
    let out = twin.commit();
    assert!(matches!(out.replies(reply).as_slice(), [TransferAnswer::Receipt(receipt)] if receipt.op_id == id && receipt.cycle_id == 2 && receipt.optimistic.is_none()), "{:?}", out.replies(reply));
}

#[test]
fn xfer_rowless_group() {
    let mut twin = Twin::new();
    let reply = twin.reply();
    let keys = ["Moment:r1:upload", OTHER];
    let out = twin.post(TransferInput::AcceptBatch { joint: Box::new(joint()), epoch: EPOCH, items: keys.iter().map(|key| item(key, 2)).collect(), reply });
    assert_eq!(Twin::fault(&out, reply), None, "a group whose targets do not exist is accepted");
    let ids: Vec<TransferId> = keys.iter().map(|key| twin.state().open_id(key).expect("an element")).collect();
    assert_ne!(ids[0], ids[1], "separate transfers");
    let out = twin.reserve_batch(&ids);
    assert_eq!(out.rows().len(), 2, "one commit for both");
    assert!(out.batches.iter().flat_map(|batch| batch.ops.iter()).all(|op| !matches!(op, StoreOp::Effects { .. })), "no synthetic row of the model");
}

#[test]
fn xfer_rowless_row_appears_removed() {
    let mut twin = Twin::with_slots(2);
    let (first, _) = mutating(&mut twin, KEY);
    let (second, _) = uploading(&mut twin, OTHER, 2);
    let confirmed = OutboxOutcome::Confirmed { revision: 3, result: None, row_id: Some("u1".into()) };
    twin.mutation_outcome(KEY, confirmed.clone());
    committed_with(&mut twin, vec![Some(landed("u1"))]);
    assert_eq!((twin.stage(first), twin.stage(second)), (TransferStage::Done, TransferStage::Uploading), "the row appeared with the answer of the first transfer, the second one goes on");
    let (reply, out) = twin.outcome(first, 1);
    assert_eq!(out.replies(reply), vec![TransferAnswer::Outcome(confirmed, Some(landed("u1")))]);
    let (third, _) = mutating(&mut twin, "CurrentUser:u1:upload_photo");
    let out = twin.mutation_outcome("CurrentUser:u1:upload_photo", OutboxOutcome::Discarded);
    assert_eq!((twin.stage(third), out.removed(), twin.stage(second)), (TransferStage::Removing, vec!["CurrentUser:u1:upload_photo".to_string()], TransferStage::Uploading), "the removal of the landed row ends the transfer on it alone");
}

#[test]
fn xfer_prop_rowless_epoch_close() {
    for close in [false, true] {
        for stage in 0..5 {
            let mut twin = Twin::new();
            let (id, reply, first) = accept(&mut twin, KEY, 1);
            let mut answers = first.replies(reply);
            if stage > 0 { answers.extend(reserve(&mut twin, id).replies(reply)); }
            if stage > 1 { answers.extend(twin.commit().replies(reply)); }
            if stage > 2 { answers.extend(twin.copy_done(id).replies(reply)); }
            if stage > 3 { answers.extend(twin.commit().replies(reply)); }
            let ended = if close { twin.close() } else { twin.end_epoch() };
            answers.extend(ended.replies(reply));
            while answers.is_empty() && twin.has_pending() { answers.extend(twin.commit().replies(reply)); }
            let expected = if close { Fault::Ended(crate::ffi::EndCause::Closed) } else { Fault::Ended(crate::ffi::EndCause::SessionEnded) };
            let label = format!("close {close}, stage {stage}");
            assert_eq!(answers.len(), 1, "exactly one outcome of the call ({label}): {answers:?}");
            assert!(matches!(&answers[0], TransferAnswer::Receipt(_)) && stage > 3 || answers[0] == TransferAnswer::Err(expected.clone()), "the receipt that was given, or the exact cause {expected:?} ({label}): {answers:?}");
            let late = reserve(&mut twin, id);
            assert_eq!((late.writes(), late.all_replies(), late.outbox.len()), (0, 0, 0), "a late answer is foreign ({label})");
        }
    }
}
