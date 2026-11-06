//! Owner-level tests of the owner `Transfer`: the elements of a batch (`BatchReady`, the mutation of the batch, the single mutations, the removal of an element and `renumber`).

use serde_json::{json, Value};

use super::transfer_harness::*;
use crate::owners::contract::Online;
use crate::owners::store::{EffectRow, IntentEffects};
use crate::owners::write::outbox::{Fault, OpSource, OutboxOutcome};
use crate::owners::write::transfer::{BatchItem, Joint, Renumber, Shown, TransferAnswer, TransferId, TransferInput, TransferStage, SIGNED_ID_SENTINEL};
use crate::runtime::Time;

const KEY: &str = "Moment:r1:upload";
const OTHER: &str = "Moment:r2:upload";
const THIRD: &str = "Moment:r3:upload";
const FOURTH: &str = "Moment:r4:upload";

fn fielded(key: &str) -> BatchItem {
    let mut element = item(key, 1);
    element.fields = json!({"name": key, "position": 0, "size": 0, "signedId": SIGNED_ID_SENTINEL}).as_object().unwrap().clone();
    element.mutation.body = json!({"variables": {"name": key, "position": 0, "size": 0, "signedId": SIGNED_ID_SENTINEL}}).to_string();
    element
}

fn rules(minimum: usize, renumber: bool) -> Joint {
    let renumber = renumber.then(|| Renumber { position: "position".into(), size: "size".into(), position_target: "position".into(), size_target: "size".into(), start: 1 });
    Joint { minimum, renumber, ..joint() }
}

fn batch(keys: &[&str], minimum: usize, renumber: bool) -> (Twin, Vec<TransferId>) {
    let mut twin = Twin::with_slots(4);
    let ids = twin.accepted_items(keys.iter().map(|key| fielded(key)).collect(), rules(minimum, renumber));
    (twin, ids)
}

/// The file of the element is on the server and the write of `completed` is answered.
fn file_done(twin: &mut Twin, id: TransferId) -> Out {
    twin.step_ok(id, 1, None);
    twin.commit();
    twin.completed(id);
    twin.commit()
}

fn variables(op: &crate::owners::write::outbox::NewOp) -> Value { serde_json::from_str::<Value>(&op.body).unwrap()["variables"].clone() }

#[test]
fn xfer_batch_item_ready() {
    let (mut twin, ids) = batch(&[KEY, OTHER], 2, false);
    let out = file_done(&mut twin, ids[0]);
    assert_eq!((twin.stage(ids[0]), twin.stage(ids[1])), (TransferStage::BatchReady, TransferStage::Uploading));
    assert_eq!((out.shows(), out.submit_ops().len()), (vec![Shown::Uploading { progress: 100 }], 0), "the element waits, nothing goes to the server");
}

#[test]
fn xfer_batch_mutation() {
    let (mut twin, ids) = batch(&[KEY, OTHER], 2, false);
    file_done(&mut twin, ids[0]);
    let out = file_done(&mut twin, ids[1]);
    assert!(ids.iter().all(|id| twin.stage(*id) == TransferStage::Mutating));
    let ops = out.submit_ops();
    assert_eq!(ops.len(), 1, "one request for the batch");
    assert_eq!(ops[0].origin, OpSource::Transfer("batch:1".into()));
    assert_eq!(variables(ops[0])["items"], json!([{"name": KEY, "position": 0, "size": 0, "signedId": "signed"}, {"name": OTHER, "position": 0, "size": 0, "signedId": "signed"}]), "the elements in the original order with the signed ids");
}

#[test]
fn xfer_batch_singles() {
    let (mut twin, ids) = batch(&[KEY, OTHER], 2, false);
    twin.cancel(OTHER);
    twin.commit();
    assert_eq!(twin.stage(ids[1]), TransferStage::Done);
    let out = file_done(&mut twin, ids[0]);
    let ops = out.submit_ops();
    assert_eq!((twin.stage(ids[0]), ops.len()), (TransferStage::Mutating, 1), "fewer than the minimum remain: the element goes alone");
    assert_eq!(ops[0].origin, OpSource::Transfer(KEY.into()));
}

#[test]
fn xfer_batch_singles_next() {
    let (mut twin, ids) = batch(&[KEY, OTHER, THIRD], 3, false);
    twin.cancel(THIRD);
    twin.commit();
    file_done(&mut twin, ids[0]);
    let out = file_done(&mut twin, ids[1]);
    assert_eq!((twin.stage(ids[0]), twin.stage(ids[1])), (TransferStage::Mutating, TransferStage::BatchReady));
    assert_eq!(out.submit_ops().iter().map(|op| op.origin.clone()).collect::<Vec<_>>(), vec![OpSource::Transfer(KEY.into())], "the first element goes alone");
    let out = twin.mutation_outcome(KEY, OutboxOutcome::Confirmed { revision: 5, result: None });
    let ops = out.submit_ops();
    assert_eq!((twin.stage(ids[1]), ops.len()), (TransferStage::Mutating, 1), "the next element goes when the previous one ended");
    assert_eq!(ops[0].origin, OpSource::Transfer(OTHER.into()));
}

#[test]
fn xfer_batch_member_in_flight() {
    let (mut twin, ids) = batch(&[KEY, OTHER], 2, false);
    file_done(&mut twin, ids[0]);
    file_done(&mut twin, ids[1]);
    let ((cancel, out), (discard, other)) = (twin.cancel(KEY), twin.discard(OTHER));
    assert_eq!((Twin::fault(&out, cancel), Twin::fault(&other, discard)), (Some(Fault::InFlight), Some(Fault::InFlight)));
    assert!(ids.iter().all(|id| twin.stage(*id) == TransferStage::Mutating) && out.writes() + other.writes() == 0);
}

#[test]
fn xfer_batch_member_cancel() {
    let (mut twin, ids) = batch(&[KEY, OTHER, THIRD], 2, true);
    let (reply, out) = twin.cancel(OTHER);
    assert_eq!((out.cancel_steps(), out.removed(), out.writes()), (1, vec![OTHER.to_string()], 1), "the file in the network is stopped, the element goes in one write");
    assert!(out.replies(reply).is_empty(), "nothing is answered before the commit");
    let out = twin.commit();
    assert_eq!((out.replies(reply), twin.stage(ids[1])), (vec![TransferAnswer::Cancelled(true)], TransferStage::Done));
    assert!(ids.iter().filter(|id| **id != ids[1]).all(|id| twin.t(*id).fields["size"] == json!(2)), "the others learn the new size");
}

#[test]
fn xfer_batch_member_discard() {
    let mut twin = Twin::with_slots(4);
    let effects = |row: &str| IntentEffects { rows: vec![EffectRow { model: "Moment".into(), row_id: row.into(), values: "{}".into() }], ..Default::default() };
    let items: Vec<BatchItem> = [(KEY, "r1"), (OTHER, "r2")].iter().map(|(key, row)| BatchItem { mutation: crate::owners::write::outbox::NewOp { effects: effects(row), ..fielded(key).mutation }, ..fielded(key) }).collect();
    let reply = twin.reply();
    twin.post(TransferInput::AcceptBatch { joint: rules(2, false), epoch: EPOCH, items, reply, now: Time(0) });
    let ids: Vec<TransferId> = [KEY, OTHER].iter().map(|key| twin.state().open_id(key).unwrap()).collect();
    for (op, id) in [(11, ids[0]), (12, ids[1])] {
        twin.post(TransferInput::OpReserved { transfer_id: id, result: Ok(op) });
    }
    twin.commit();
    ids.iter().for_each(|id| drop(twin.copy_done(*id)));
    twin.commit();
    let (reply, out) = twin.discard(KEY);
    assert_eq!((out.rolled_back(), out.removed()), (vec![11], vec![KEY.to_string()]), "the optimistic row of the element goes, the other stays");
    let out = twin.commit();
    assert_eq!(out.replies(reply), vec![TransferAnswer::Ok]);
}

#[test]
fn xfer_batch_singles_member_cancel() {
    let (mut twin, ids) = batch(&[KEY, OTHER, THIRD], 3, false);
    twin.cancel(THIRD);
    twin.commit();
    file_done(&mut twin, ids[0]);
    file_done(&mut twin, ids[1]);
    assert_eq!((twin.stage(ids[0]), twin.stage(ids[1])), (TransferStage::Mutating, TransferStage::BatchReady));
    let (reply, out) = twin.cancel(OTHER);
    assert_eq!((out.removed(), out.writes()), (vec![OTHER.to_string()], 1));
    let out = twin.commit();
    assert_eq!((out.replies(reply), twin.stage(ids[1]), twin.stage(ids[0])), (vec![TransferAnswer::Cancelled(true)], TransferStage::Done, TransferStage::Mutating), "the single of the first element is not touched");
}

#[test]
fn xfer_batch_ready_retry() {
    let (mut twin, ids) = batch(&[KEY, OTHER], 2, false);
    file_done(&mut twin, ids[0]);
    let (reply, out) = twin.retry(KEY, None);
    assert!(matches!(out.replies(reply).as_slice(), [TransferAnswer::Receipt(receipt)] if receipt.op_id == ids[0]));
    assert_eq!(twin.stage(ids[0]), TransferStage::BatchReady);
}

#[test]
fn xfer_batch_ready_other() {
    let (mut twin, ids) = batch(&[KEY, OTHER], 2, false);
    file_done(&mut twin, ids[0]);
    let mut inputs = twin.stray_results(ids[0]);
    inputs.extend([TransferInput::Online(Online(true)), TransferInput::SlotFreed, TransferInput::MutationOutcome { key: OTHER.into(), outcome: OutboxOutcome::Confirmed { revision: 1, result: None } }]);
    twin.ignores(ids[0], inputs);
}

#[test]
fn xfer_batch_confirmed() {
    let (mut twin, ids) = batch(&[KEY, OTHER], 2, false);
    file_done(&mut twin, ids[0]);
    file_done(&mut twin, ids[1]);
    let out = twin.mutation_outcome("batch:1", OutboxOutcome::Confirmed { revision: 9, result: None });
    assert_eq!((out.removed(), out.writes()), (vec![KEY.to_string(), OTHER.to_string()], 1), "every element leaves in one write");
    twin.commit();
    for id in ids {
        let (reply, out) = twin.outcome(id, 1);
        assert_eq!((out.replies(reply), twin.stage(id)), (vec![TransferAnswer::Outcome(OutboxOutcome::Confirmed { revision: 9, result: None })], TransferStage::Done));
    }
}

#[test]
fn xfer_batch_failed() {
    let (mut twin, ids) = batch(&[KEY, OTHER], 2, false);
    file_done(&mut twin, ids[0]);
    file_done(&mut twin, ids[1]);
    let out = twin.mutation_outcome("batch:1", OutboxOutcome::Failed { error: "boom".into() });
    assert_eq!((out.writes(), out.rows().len(), twin.stage(ids[0])), (1, 2, TransferStage::PersistingFailure));
    let out = twin.commit();
    assert!(ids.iter().all(|id| twin.stage(*id) == TransferStage::Failed));
    assert_eq!(out.shows(), vec![Shown::Failed { error: "boom".into() }, Shown::Failed { error: "boom".into() }]);
}

#[test]
fn xfer_batch_retry_mutation() {
    let (mut twin, ids) = batch(&[KEY, OTHER], 2, false);
    file_done(&mut twin, ids[0]);
    file_done(&mut twin, ids[1]);
    twin.mutation_outcome("batch:1", OutboxOutcome::Failed { error: "boom".into() });
    twin.commit();
    let (reply, out) = twin.retry(KEY, None);
    assert!(ids.iter().all(|id| twin.stage(*id) == TransferStage::PersistingRetry) && out.writes() == 1 && out.outbox_retries() == 0, "the write comes first");
    let out = twin.commit();
    assert!(ids.iter().all(|id| twin.stage(*id) == TransferStage::Mutating));
    assert_eq!(out.outbox_retries(), 1, "the saved mutation of the batch goes once");
    assert!(matches!(out.replies(reply).as_slice(), [TransferAnswer::Receipt(receipt)] if receipt.cycle_id == 2));
}

#[test]
fn xfer_batch_retry_file() {
    let (mut twin, ids) = batch(&[KEY, OTHER], 2, false);
    twin.step_error(ids[0], false);
    twin.commit();
    assert_eq!(twin.stage(ids[0]), TransferStage::Failed);
    let (reply, out) = twin.retry(KEY, None);
    assert_eq!((twin.stage(ids[0]), twin.stage(ids[1]), out.writes()), (TransferStage::PersistingRetry, TransferStage::Uploading, 1), "only the element of the failed file is written");
    let out = twin.commit();
    assert!(matches!(out.replies(reply).as_slice(), [TransferAnswer::Receipt(receipt)] if receipt.cycle_id == 2), "{:?}", out.replies(reply));
    assert_eq!((twin.t(ids[0]).cycle_id, twin.stage(ids[0])), (2, TransferStage::Uploading), "a slot is free: the file goes at once");
}

#[test]
fn xfer_batch_removal_releases_mutation() {
    let (mut twin, ids) = batch(&[KEY, OTHER], 2, false);
    file_done(&mut twin, ids[0]);
    twin.cancel(OTHER);
    let out = twin.commit();
    let ops = out.submit_ops();
    assert_eq!((twin.stage(ids[0]), ops.len()), (TransferStage::Mutating, 1), "the element that waited goes alone when the other leaves");
    assert_eq!(ops[0].origin, OpSource::Transfer(KEY.into()));
}

#[test]
fn xfer_accept_batch_length() {
    let mut twin = Twin::new();
    for keys in [vec![KEY], vec![KEY; 1].into_iter().chain((0..10).map(|_| "Moment:x:upload")).collect::<Vec<_>>()] {
        let items = keys.iter().enumerate().map(|(index, _)| item(&format!("Moment:r{index}:upload"), 1)).collect();
        let reply = twin.reply();
        let out = twin.post(TransferInput::AcceptBatch { joint: joint(), epoch: EPOCH, items, reply, now: Time(0) });
        assert_eq!((Twin::fault(&out, reply), out.writes(), out.all_replies()), (Some(Fault::Invalid), 0, 1), "{} elements", keys.len());
    }
    assert!(twin.state().transfers.is_empty());
}

/// P-XFER-31: a batch gives one mutation with the remaining elements in order, or one single mutation per element in order, and an element that leaves does not touch the files of the others.
#[test]
fn xfer_prop_batch_one_mutation() {
    let (mut whole, ids) = batch(&[KEY, OTHER, THIRD], 2, false);
    let outs: Vec<Out> = ids.iter().map(|id| file_done(&mut whole, *id)).collect();
    assert_eq!(outs.iter().map(|out| out.submit_ops().len()).collect::<Vec<_>>(), vec![0, 0, 1]);
    assert_eq!(variables(outs[2].submit_ops()[0])["items"].as_array().unwrap().iter().map(|item| item["name"].clone()).collect::<Vec<_>>(), vec![json!(KEY), json!(OTHER), json!(THIRD)]);

    let (mut alone, ids) = batch(&[KEY, OTHER, THIRD], 3, false);
    let parts: Vec<u32> = [1, 2].iter().map(|index| alone.t(ids[*index]).confirmed_parts).collect();
    alone.cancel(KEY);
    let out = alone.commit();
    assert_eq!(out.deletes(), 1, "only the copy of the element that left goes");
    assert_eq!([1, 2].iter().map(|index| alone.t(ids[*index]).confirmed_parts).collect::<Vec<_>>(), parts);
    file_done(&mut alone, ids[1]);
    let first = file_done(&mut alone, ids[2]);
    assert_eq!(first.submit_ops().iter().map(|op| op.origin.clone()).collect::<Vec<_>>(), vec![OpSource::Transfer(OTHER.into())], "the first remaining element goes first");
    let next = alone.mutation_outcome(OTHER, OutboxOutcome::Confirmed { revision: 3, result: None });
    assert_eq!(next.submit_ops().iter().map(|op| op.origin.clone()).collect::<Vec<_>>(), vec![OpSource::Transfer(THIRD.into())]);
}

/// P-XFER-32: after an element leaves, the remaining ones get positions `start..` and the size in the row and in the saved variables, below the minimum nothing changes, without `renumber` nothing changes.
#[test]
fn xfer_prop_batch_renumber() {
    let (mut twin, ids) = batch(&[KEY, OTHER, THIRD, FOURTH], 2, true);
    let (_, out) = twin.cancel(OTHER);
    assert_eq!(out.numbers(), vec![("r1".to_string(), vec![("position".to_string(), 1), ("size".to_string(), 3)]), ("r3".to_string(), vec![("position".to_string(), 2), ("size".to_string(), 3)]), ("r4".to_string(), vec![("position".to_string(), 3), ("size".to_string(), 3)])]);
    let saved = out.rows().into_iter().find(|(row, _)| row.key == THIRD).map(|(row, _)| row.body.clone()).unwrap();
    assert_eq!(serde_json::from_str::<Value>(&saved).unwrap()["variables"], json!({"name": THIRD, "position": 2, "size": 3, "signedId": SIGNED_ID_SENTINEL}), "the saved variables follow");
    twin.commit();
    twin.cancel(KEY);
    twin.commit();
    let (_, out) = twin.cancel(THIRD);
    assert!(out.numbers().is_empty(), "one element is left, below the minimum: the numbers stay");
    assert_eq!(twin.t(ids[3]).fields["position"], json!(2));

    let (mut plain, _) = batch(&[KEY, OTHER, THIRD], 2, false);
    let (_, out) = plain.cancel(OTHER);
    assert!(out.numbers().is_empty(), "no renumber declared");
}
