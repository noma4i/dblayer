//! Owner-level tests of the owner `Transfer` for a transfer whose target row does not exist at the acceptance (D-287): the transfer is addressed by `transfer_id` and `cycle_id`, never by the row.

use super::transfer_harness::*;
use crate::owners::store::UploadKind;
use crate::owners::write::outbox::{Fault, OutboxInput, OutboxOutcome, ReserveTarget};
use crate::owners::write::transfer::{BatchItem, Shown, TransferAnswer, TransferId, TransferInput, TransferStage, TransferTick};
use crate::runtime::Time;

const KEY: &str = "CurrentUser:me:upload_photo";
const OTHER: &str = "CurrentUser:other:upload_photo";

fn rowless(key: &str, parts: u32) -> BatchItem { BatchItem { row_exists: false, ..item(key, parts) } }

fn accept_rowless(twin: &mut Twin, key: &str, parts: u32) -> (TransferId, u64, Out) {
    let reply = twin.reply();
    let out = twin.post(TransferInput::Accept { epoch: EPOCH, item: Box::new(rowless(key, parts)), reply, now: Time(0) });
    let id = twin.state().open_id(key).expect("a rowless transfer is accepted");
    (id, reply, out)
}

fn accepting_rowless(twin: &mut Twin, key: &str, parts: u32) -> (TransferId, u64, Out) {
    let (id, reply, _) = accept_rowless(twin, key, parts);
    let out = twin.post(TransferInput::OpReserved { target: ReserveTarget::Transfer(id), result: Ok(id) });
    (id, reply, out)
}

fn uploading_rowless(twin: &mut Twin, key: &str, parts: u32) -> TransferId {
    let (id, ..) = accepting_rowless(twin, key, parts);
    twin.commit();
    twin.copy_done(id);
    twin.commit();
    if twin.stage(id) == TransferStage::PersistingAttempt { twin.commit(); }
    assert_eq!(twin.stage(id), TransferStage::Uploading);
    id
}

fn mutating_rowless(twin: &mut Twin, key: &str) -> (TransferId, Out) {
    let id = uploading_rowless(twin, key, 1);
    twin.step_ok(id, 1, None);
    twin.commit();
    twin.commit();
    twin.completed(id);
    let out = twin.commit();
    assert_eq!(twin.stage(id), TransferStage::Mutating);
    (id, out)
}

fn failed_rowless(twin: &mut Twin, key: &str) -> TransferId {
    let id = uploading_rowless(twin, key, 2);
    twin.step_error(id, false);
    twin.commit();
    assert_eq!(twin.stage(id), TransferStage::Failed);
    id
}

#[test]
fn xfer_accept_rowless() {
    let mut twin = Twin::new();
    let (id, reply, out) = accept_rowless(&mut twin, KEY, 2);
    assert_eq!(Twin::fault(&out, reply), None, "a missing row is not a refusal");
    assert_eq!(twin.stage(id), TransferStage::Reserving);
    assert!(matches!(out.outbox.as_slice(), [OutboxInput::ReserveOp { target: ReserveTarget::Transfer(found), .. }] if *found == id), "the same reservation as a transfer with a row");
    assert_eq!((out.writes(), out.all_replies()), (0, 0), "nothing before Committed");
}

#[test]
fn xfer_accept_check_order_without_row() {
    let mut twin = Twin::new();
    let mut broken = rowless(KEY, 2);
    broken.source.as_mut().unwrap().path.clear();
    let out = twin.post(TransferInput::Accept { epoch: EPOCH, item: Box::new(broken), reply: 9, now: Time(0) });
    assert_eq!(Twin::fault(&out, 9), Some(Fault::Invalid), "an invalid source is refused as Invalid, never as a missing row");
    let out = twin.post(TransferInput::Accept { epoch: EPOCH + 1, item: Box::new(rowless(KEY, 2)), reply: 10, now: Time(0) });
    assert_eq!(Twin::fault(&out, 10), Some(Fault::NoSession));
}

#[test]
fn xfer_copy_rowless() {
    let mut twin = Twin::new();
    let (id, _, out) = accepting_rowless(&mut twin, KEY, 2);
    assert_eq!((out.writes(), out.kinds()), (1, vec![UploadKind::SyncUpload]), "the copy is recorded as a row of sync_upload, no optimistic row is written");
    assert!(out.batches.iter().flat_map(|batch| batch.ops.iter()).all(|op| !matches!(op, crate::owners::store::StoreOp::Effects { .. })), "no effects of a row that does not exist");
    let out = twin.commit();
    assert_eq!((twin.stage(id), out.copies()), (TransferStage::Copying, 1));
}

#[test]
fn xfer_rowless_steps() {
    let mut twin = Twin::new();
    let id = uploading_rowless(&mut twin, KEY, 2);
    let out = twin.step_ok(id, 1, Some("u1"));
    assert_eq!(out.writes(), 1, "the part is stored by the key of the transfer");
    let out = twin.commit();
    assert_eq!(out.steps().len(), 1, "the next part goes out");
}

#[test]
fn xfer_rowless_mutation_lands() {
    let mut twin = Twin::new();
    let (id, out) = mutating_rowless(&mut twin, KEY);
    let ops = out.submit_ops();
    assert_eq!(ops.len(), 1, "the final mutation goes through Outbox once");
    assert_eq!((ops[0].row_id.as_str(), ops[0].effects.rows.len()), ("me", 0), "no effects of a row that does not exist");
    assert_eq!(twin.t(id).cycle_id, 1);
}

#[test]
fn xfer_rowless_confirmed() {
    let mut twin = Twin::new();
    let (id, _) = mutating_rowless(&mut twin, KEY);
    let confirmed = OutboxOutcome::Confirmed { revision: 3, result: Some(r#"{"status":"GOOD"}"#.into()), row_id: Some("u1".into()) };
    let out = twin.mutation_outcome(KEY, confirmed.clone());
    assert_eq!((twin.stage(id), out.removed()), (TransferStage::Removing, vec![KEY.to_string()]));
    let out = twin.commit();
    assert_eq!((twin.stage(id), out.shows()), (TransferStage::Done, vec![Shown::Gone]));
    let (reply, out) = twin.outcome(id, 1);
    assert_eq!(out.replies(reply), vec![TransferAnswer::Outcome(confirmed, None)], "the outcome carries the answer of the final mutation and the landed row id");
}

#[test]
fn xfer_rowless_cancel() {
    let mut twin = Twin::new();
    let (_, _, _) = accepting_rowless(&mut twin, KEY, 2);
    twin.commit();
    let (reply, out) = twin.cancel(KEY);
    assert_eq!(Twin::fault(&out, reply), None, "a missing row does not make a cancel NotFound");
    assert_eq!(out.copy_cancels(), 1, "the copy of the file stops");
    let mut second = Twin::new();
    let id = failed_rowless(&mut second, KEY);
    let (reply, _) = second.cancel(KEY);
    assert_eq!((second.commit().replies(reply), second.stage(id)), (vec![TransferAnswer::Cancelled(true)], TransferStage::Done));
}

#[test]
fn xfer_rowless_discard() {
    let mut twin = Twin::new();
    let id = failed_rowless(&mut twin, KEY);
    let (reply, out) = twin.discard(KEY);
    assert_eq!((twin.stage(id), out.removed()), (TransferStage::Removing, vec![KEY.to_string()]), "an empty rollback: the transfer alone goes");
    assert_eq!(twin.commit().replies(reply), vec![TransferAnswer::Ok]);
}

#[test]
fn xfer_rowless_retry() {
    let mut twin = Twin::new();
    let id = failed_rowless(&mut twin, KEY);
    let (reply, out) = twin.retry(KEY, None);
    assert_eq!(Twin::fault(&out, reply), None, "a retry never reads the missing row");
    let out = twin.commit();
    assert!(matches!(out.replies(reply).as_slice(), [TransferAnswer::Receipt(receipt)] if receipt.op_id == id && receipt.cycle_id == 2 && receipt.optimistic.is_none()), "{:?}", out.replies(reply));
}

#[test]
fn xfer_rowless_restore() {
    let mut twin = Twin::new();
    let id = uploading_rowless(&mut twin, KEY, 2);
    let (mut reopened, out) = Twin::opened(vec![stored(id, KEY, 2)], Default::default());
    assert_eq!((reopened.stage(id), out.classified()), (TransferStage::Ready, 1), "restored by the record of the transfer, no row is read");
    reopened.online(true);
    reopened.post(TransferInput::RestoreReady { epoch: EPOCH });
    assert_eq!(reopened.commit().steps().len(), 1, "the steps go on");
}

#[test]
fn xfer_rowless_orphan() {
    let mut twin = Twin::new();
    let id = uploading_rowless(&mut twin, KEY, 3);
    twin.step_ok(id, 1, Some("u1"));
    twin.commit();
    twin.commit();
    let (reply, out) = twin.discard(KEY);
    assert_eq!((out.orphans_written(), out.removed()), (vec![("u1".to_string(), KEY.to_string())], vec![KEY.to_string()]), "the debt of the multipart upload is written by the id of the upload");
    let out = twin.commit();
    assert_eq!((out.orphan_aborts(), out.replies(reply)), (vec!["u1".to_string()], vec![TransferAnswer::Ok]));
}

#[test]
fn xfer_rowless_failed_expiry() {
    let mut twin = Twin::new();
    let id = failed_rowless(&mut twin, KEY);
    let out = twin.post(TransferInput::Tick { now: Time(8 * DAY), kind: TransferTick::Expire });
    assert_eq!((twin.stage(id), out.removed()), (TransferStage::Removing, vec![KEY.to_string()]));
    let out = twin.commit();
    assert_eq!((twin.stage(id), out.deletes()), (TransferStage::Done, 1), "the copy goes with the transfer");
}

#[test]
fn xfer_rowless_group() {
    let mut twin = Twin::new();
    let reply = twin.reply();
    let items = vec![item("Moment:r1:upload", 2), rowless(OTHER, 2)];
    let out = twin.post(TransferInput::AcceptBatch { joint: Box::new(joint()), epoch: EPOCH, items, reply, now: Time(0) });
    assert_eq!(Twin::fault(&out, reply), None, "a group with a missing row is accepted");
    let ids: Vec<TransferId> = ["Moment:r1:upload", OTHER].iter().map(|key| twin.state().open_id(key).expect("an element")).collect();
    assert_ne!(ids[0], ids[1], "separate transfers");
    let out = twin.reserve_batch(&ids);
    assert_eq!(out.rows().len(), 2, "one commit for both, no synthetic row");
}

#[test]
fn xfer_rowless_row_appears_removed() {
    let mut twin = Twin::new();
    let (id, _) = mutating_rowless(&mut twin, KEY);
    let out = twin.mutation_outcome(KEY, OutboxOutcome::Rejected { error: "row removed".into() });
    assert_eq!((twin.stage(id), out.removed()), (TransferStage::Removing, vec![KEY.to_string()]), "the removal of the landed row ends this transfer alone");
    let mut other = Twin::new();
    let kept = uploading_rowless(&mut other, OTHER, 2);
    assert_eq!(other.stage(kept), TransferStage::Uploading, "a transfer of another key is untouched");
}

#[test]
fn xfer_prop_rowless_epoch_close() {
    for close in [false, true] {
        for stage in 0..5 {
            let mut twin = Twin::new();
            let (id, reply, _) = accept_rowless(&mut twin, KEY, 1);
            if stage > 0 { twin.post(TransferInput::OpReserved { target: ReserveTarget::Transfer(id), result: Ok(id) }); }
            if stage > 1 { twin.commit(); }
            if stage > 2 { twin.copy_done(id); }
            if stage > 3 { twin.commit(); }
            let out = if close { twin.close() } else { twin.end_epoch() };
            let faults = [Twin::fault(&out, reply)].into_iter().flatten().count();
            assert!(faults <= 1, "one outcome of the call (close {close}, stage {stage})");
            twin.online(true);
            let late = twin.post(TransferInput::OpReserved { target: ReserveTarget::Transfer(id), result: Ok(id) });
            assert_eq!(late.writes(), 0, "a late answer is foreign (close {close}, stage {stage})");
        }
    }
}
