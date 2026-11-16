//! Owner-level tests of the owner `Transfer`: the elements of a batch (`BatchReady`, the mutation of the batch, the single mutations, the removal of an element and `renumber`).

use serde_json::{json, Value};

use super::transfer_harness::*;
use crate::owners::contract::Online;
use crate::owners::store::{EffectRow, IntentEffects};
use crate::owners::write::outbox::{Fault, OpSource, OutboxOutcome, ReserveTarget};
use crate::owners::write::transfer::{BatchItem, Joint, Renumber, Shown, TransferAnswer, TransferId, TransferInput, TransferStage, SIGNED_ID_SENTINEL};

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

fn single(id: TransferId) -> OpSource { OpSource::Transfer { transfer_id: id, cycle_id: 1 } }

fn batch(keys: &[&str], minimum: usize, renumber: bool) -> (Twin, Vec<TransferId>) {
    let mut twin = Twin::with_slots(4);
    let ids = twin.accepted_items(keys.iter().map(|key| fielded(key)).collect(), rules(minimum, renumber));
    for _ in &ids {
        assert_eq!(twin.commit().steps().len(), 1);
    }
    (twin, ids)
}

/// The file of the element is on the server and the write of `completed` is answered.
fn file_done(twin: &mut Twin, id: TransferId) -> Out {
    twin.step_ok(id, 1, None);
    twin.commit();
    twin.commit();
    twin.completed(id);
    let out = twin.commit();
    if twin.state().transfers.values().any(|transfer| transfer.stage == TransferStage::PersistingBatchIntent) {
        assert_eq!(out.writes(), 1);
        assert!(out.submit_ops().is_empty());
        twin.commit()
    } else { out }
}

#[test]
fn xfer_batch_intent_committed() {
    let (mut twin, ids) = batch(&[KEY, OTHER], 2, false);
    file_done(&mut twin, ids[0]);
    twin.step_ok(ids[1], 1, None);
    twin.commit();
    twin.commit();
    twin.completed(ids[1]);
    let before = twin.commit();
    assert!(ids.iter().all(|id| twin.stage(*id) == TransferStage::PersistingBatchIntent));
    assert_eq!(before.writes(), 1);
    assert!(before.submit_ops().is_empty());
    let after = twin.commit();
    assert!(ids.iter().all(|id| twin.stage(*id) == TransferStage::Mutating));
    assert_eq!(after.submit_ops().len(), 1);
}

fn persisting_joint() -> (Twin, Vec<TransferId>) {
    let (mut twin, ids) = batch(&[KEY, OTHER], 2, false);
    file_done(&mut twin, ids[0]);
    twin.step_ok(ids[1], 1, None);
    twin.commit();
    twin.commit();
    twin.completed(ids[1]);
    twin.commit();
    (twin, ids)
}

#[test]
fn xfer_batch_intent_invalid() {
    let (mut twin, ids) = persisting_joint();
    let out = twin.reject(crate::owners::contract::RejectReason::Invalid(String::new()));
    assert_eq!((out.writes(), out.rows().len()), (1, 2));
    assert!(ids.iter().all(|id| twin.stage(*id) == TransferStage::PersistingFailure));
    assert!(out.submit_ops().is_empty());
}

#[test]
fn xfer_completed_batch() {
    let (mut twin, ids) = batch(&[KEY, OTHER], 2, false);
    twin.step_ok(ids[0], 1, None);
    twin.commit();
    twin.commit();
    let out = twin.completed(ids[0]);
    assert_eq!(twin.stage(ids[0]), TransferStage::PersistingComplete);
    assert_eq!(out.rows().len(), 1);
    assert!(out.rows()[0].0.completed);
    assert!(out.batches[0].ops.iter().all(|op| !matches!(op, crate::owners::store::StoreOp::Intent { .. })));
    assert!(out.submit_ops().is_empty());
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
    twin.step_ok(ids[1], 1, None);
    twin.commit();
    twin.commit();
    twin.completed(ids[1]);
    let intent = twin.commit();
    assert_eq!(intent.batches.len(), 1);
    assert!(intent.batches[0].ops.iter().any(|op| matches!(op, crate::owners::store::StoreOp::Intent { op_id: 1_000_001, origin: crate::owners::store::IntentOrigin::TransferGroup { batch_id: 1, transfer_ids }, .. } if *transfer_ids == ids)), "the group intent and member ids share a batch: {:?}", intent.batches);
    let out = twin.commit();
    assert!(ids.iter().all(|id| twin.stage(*id) == TransferStage::Mutating));
    let ops = out.submit_ops();
    assert_eq!(ops.len(), 1, "one request for the batch");
    assert!(matches!(&ops[0].origin, OpSource::TransferGroup { batch_id: 1, members } if members.iter().map(|member| member.transfer_id).collect::<Vec<_>>() == ids));
    assert_eq!(variables(ops[0])["items"], json!([{"name": KEY, "position": 0, "size": 0, "signedId": "signed"}, {"name": OTHER, "position": 0, "size": 0, "signedId": "signed"}]), "the elements in the original order with the signed ids");
}

#[test]
fn xfer_batch_singles() {
    let (mut twin, ids) = batch(&[KEY, OTHER], 2, false);
    twin.cancel(OTHER);
    twin.commit();
    assert_eq!(twin.stage(ids[1]), TransferStage::Done);
    twin.step_ok(ids[0], 1, None);
    twin.commit();
    twin.commit();
    twin.completed(ids[0]);
    let intent = twin.commit();
    assert_eq!(intent.batches.len(), 1);
    assert!(intent.batches[0].ops.iter().any(|op| matches!(op, crate::owners::store::StoreOp::Intent { op_id, origin: crate::owners::store::IntentOrigin::Transfer { transfer_id }, .. } if *op_id == ids[0] && *transfer_id == ids[0])), "the first single intent must name its member: {:?}", intent.batches);
    let out = twin.commit();
    let ops = out.submit_ops();
    assert_eq!((twin.stage(ids[0]), ops.len()), (TransferStage::Mutating, 1), "fewer than the minimum remain: the element goes alone");
    assert_eq!(ops[0].origin, single(ids[0]));
}

#[test]
fn xfer_batch_singles_next() {
    let (mut twin, ids) = batch(&[KEY, OTHER, THIRD], 3, false);
    twin.cancel(THIRD);
    twin.commit();
    file_done(&mut twin, ids[0]);
    let out = file_done(&mut twin, ids[1]);
    assert_eq!((twin.stage(ids[0]), twin.stage(ids[1])), (TransferStage::Mutating, TransferStage::BatchReady));
    assert_eq!(out.submit_ops().iter().map(|op| op.origin.clone()).collect::<Vec<_>>(), vec![single(ids[0])], "the first element goes alone");
    let pending = twin.mutation_outcome(KEY, OutboxOutcome::Confirmed { revision: 5, result: None, row_id: None });
    assert_eq!((twin.stage(ids[1]), pending.writes()), (TransferStage::PersistingBatchIntent, 2));
    assert!(pending.batches.iter().any(|batch| batch.ops.iter().any(|op| matches!(op, crate::owners::store::StoreOp::Intent { op_id, origin: crate::owners::store::IntentOrigin::Transfer { transfer_id }, .. } if *op_id == ids[1] && *transfer_id == ids[1]))), "the next single intent names its member: {:?}", pending.batches);
    assert!(pending.submit_ops().is_empty());
    assert!(twin.commit().submit_ops().is_empty());
    let out = twin.commit();
    let ops = out.submit_ops();
    assert_eq!((twin.stage(ids[1]), ops.len()), (TransferStage::Mutating, 1), "the next element goes when the previous one ended");
    assert_eq!(ops[0].origin, single(ids[1]));
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
    twin.post(TransferInput::AcceptBatch { joint: Box::new(rules(2, false)), epoch: EPOCH, items, reply });
    let ids: Vec<TransferId> = [KEY, OTHER].iter().map(|key| twin.state().open_id(key).unwrap()).collect();
    for (op, id) in [(11, ids[0]), (12, ids[1])] {
        twin.post(TransferInput::OpReserved { target: ReserveTarget::Transfer(id), result: Ok(op) });
    }
    let group = twin.t(ids[0]).batch_id.unwrap();
    twin.post(TransferInput::OpReserved { target: ReserveTarget::Group(group), result: Ok(13) });
    twin.commit();
    ids.iter().for_each(|id| drop(twin.copy_done(*id)));
    twin.commit();
    twin.commit();
    twin.commit();
    let (reply, out) = twin.discard(KEY);
    assert_eq!((out.rolled_back(), out.removed()), (vec![11], vec![KEY.to_string()]), "the optimistic row of the element goes, the other stays");
    let out = twin.commit();
    assert_eq!(out.replies(reply), vec![TransferAnswer::Ok]);
}

#[test]
fn xfer_a_member_of_a_dropped_group_leaves_like_a_single() {
    {
        let (mut twin, ids) = batch(&[KEY, OTHER, THIRD], 3, false);
        twin.cancel(THIRD);
        twin.commit();
        file_done(&mut twin, ids[0]);
        file_done(&mut twin, ids[1]);
        assert_eq!((twin.stage(ids[0]), twin.stage(ids[1])), (TransferStage::Mutating, TransferStage::BatchReady));
        let (reply, out) = twin.cancel(OTHER);
        assert_eq!((out.removed(), out.writes()), (vec![OTHER.to_string()], 1));
        assert!(out.batches[0].ops.iter().any(|op| matches!(op, crate::owners::store::StoreOp::RollBack { op_id } if *op_id == ids[1])), "removal and rollback share the same batch: {:?}", out.batches);
        let out = twin.commit();
        assert_eq!((out.replies(reply), twin.stage(ids[1]), twin.stage(ids[0])), (vec![TransferAnswer::Cancelled(true)], TransferStage::Done, TransferStage::Mutating), "the single of the first element is not touched");
    }
    {
        let (mut twin, ids) = batch(&[KEY, OTHER, THIRD], 3, true);
        twin.cancel(THIRD);
        twin.commit();
        file_done(&mut twin, ids[0]);
        file_done(&mut twin, ids[1]);
        assert_eq!((twin.stage(ids[0]), twin.stage(ids[1])), (TransferStage::Mutating, TransferStage::BatchReady), "the group dropped to singles: one runs, one waits");
        let (reply, out) = twin.discard(OTHER);
        assert_eq!((out.removed(), out.rolled_back()), (vec![OTHER.to_string()], vec![ids[1]]), "it goes like a single: its row and its op roll back");
        assert!(out.numbers().is_empty(), "the dropped group is not renumbered");
        let out = twin.commit();
        assert_eq!((out.replies(reply), twin.stage(ids[1]), twin.stage(ids[0])), (vec![TransferAnswer::Ok], TransferStage::Done, TransferStage::Mutating), "the single of the first element keeps running");
    }
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
    inputs.extend([TransferInput::Online(Online(true)), TransferInput::SlotFreed, TransferInput::MutationOutcome { transfer_id: ids[1], cycle_id: 1, intent_removed: true, outcome: OutboxOutcome::Confirmed { revision: 1, result: None, row_id: None }, row_never_created: false }]);
    twin.ignores(ids[0], inputs);
}

#[test]
fn xfer_batch_confirmed() {
    let (mut twin, ids) = batch(&[KEY, OTHER], 2, false);
    file_done(&mut twin, ids[0]);
    file_done(&mut twin, ids[1]);
    let out = twin.mutation_outcome("batch:1", OutboxOutcome::Confirmed { revision: 9, result: None, row_id: None });
    assert_eq!((out.removed(), out.writes()), (vec![KEY.to_string(), OTHER.to_string()], 1), "every element leaves in one write");
    twin.commit();
    for id in ids {
        let (reply, out) = twin.outcome(id, 1);
        assert_eq!((out.replies(reply), twin.stage(id)), (vec![TransferAnswer::Outcome(OutboxOutcome::Confirmed { revision: 9, result: None, row_id: None }, None)], TransferStage::Done));
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
    assert!(ids.iter().all(|id| twin.stage(*id) == TransferStage::PersistingRetry));
    assert_eq!((out.writes(), out.outbox_retries()), (0, 1));
    assert!(out.replies(reply).is_empty());
    assert!(out.submit_ops().is_empty());
    let cycles = ids.iter().map(|id| (*id, 2)).collect();
    let out = twin.post(TransferInput::RetryOutcome { cycles, result: Ok(2) });
    assert!(ids.iter().all(|id| twin.stage(*id) == TransferStage::Mutating));
    assert_eq!(out.outbox_retries(), 0);
    assert!(out.submit_ops().is_empty());
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
    assert_eq!((twin.t(ids[0]).cycle_id, twin.stage(ids[0])), (2, TransferStage::PersistingAttempt));
    assert_eq!(twin.commit().steps().len(), 1);
}

#[test]
fn xfer_batch_removal_releases_mutation() {
    let (mut twin, ids) = batch(&[KEY, OTHER], 2, false);
    file_done(&mut twin, ids[0]);
    twin.cancel(OTHER);
    let intent = twin.commit();
    assert_eq!(intent.batches.len(), 1);
    assert!(intent.batches[0].ops.iter().any(|op| matches!(op, crate::owners::store::StoreOp::Intent { op_id, origin: crate::owners::store::IntentOrigin::Transfer { transfer_id }, .. } if *op_id == ids[0] && *transfer_id == ids[0])), "the remaining member intent is recorded: {:?}", intent.batches);
    let out = twin.commit();
    let ops = out.submit_ops();
    assert_eq!((twin.stage(ids[0]), ops.len()), (TransferStage::Mutating, 1), "the element that waited goes alone when the other leaves");
    assert_eq!(ops[0].origin, single(ids[0]));
}

#[test]
fn xfer_accept_batch_length() {
    let mut twin = Twin::new();
    for keys in [vec![KEY], vec![KEY; 1].into_iter().chain((0..10).map(|_| "Moment:x:upload")).collect::<Vec<_>>()] {
        let items = keys.iter().enumerate().map(|(index, _)| item(&format!("Moment:r{index}:upload"), 1)).collect();
        let reply = twin.reply();
        let out = twin.post(TransferInput::AcceptBatch { joint: Box::new(joint()), epoch: EPOCH, items, reply });
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
    assert_eq!(first.submit_ops().iter().map(|op| op.origin.clone()).collect::<Vec<_>>(), vec![single(ids[1])], "the first remaining element goes first");
    let pending = alone.mutation_outcome(OTHER, OutboxOutcome::Confirmed { revision: 3, result: None, row_id: None });
    assert_eq!((alone.stage(ids[2]), pending.writes()), (TransferStage::PersistingBatchIntent, 2));
    assert!(pending.submit_ops().is_empty());
    assert!(alone.commit().submit_ops().is_empty());
    let next = alone.commit();
    assert_eq!(next.submit_ops().iter().map(|op| op.origin.clone()).collect::<Vec<_>>(), vec![single(ids[2])]);
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

#[test]
fn xfer_batch_element_without_a_row_fails_before_the_batch_confirms() {
    let (mut twin, ids) = batch(&[KEY, OTHER], 2, false);
    file_done(&mut twin, ids[0]);
    file_done(&mut twin, ids[1]);
    let members = vec![(ids[0], 1, OutboxOutcome::Confirmed { revision: 9, result: None, row_id: None }), (ids[1], 1, OutboxOutcome::Failed { error: "protocol".into() })];
    twin.post(TransferInput::MutationOutcomes { batch_id: 1, members, intent_removed: true });
    assert_eq!(twin.stage(ids[1]), TransferStage::PersistingFailure, "the element the answer lacks keeps its failure, the confirmation of the batch skips it");
    assert_ne!(twin.stage(ids[0]), TransferStage::PersistingFailure, "the others are confirmed");
}

#[test]
fn xfer_accept_replaces_a_failed_member_of_a_batch() {
    let (mut twin, ids) = batch(&[KEY, OTHER], 2, false);
    twin.step_error(ids[0], false);
    twin.commit();
    assert_eq!(twin.stage(ids[0]), TransferStage::Failed);
    let out = twin.post(TransferInput::Accept { epoch: EPOCH, item: Box::new(item(KEY, 2)), reply: 90 });
    assert!(out.replies(90).is_empty(), "the failed member must not block its key: {:?}", out.replies(90));
    let new = out.outbox.iter().find_map(|input| match input {
        crate::owners::write::outbox::OutboxInput::ReserveReplacement { target: ReserveTarget::Transfer(id), .. } if *id != ids[0] => Some(*id),
        _ => None,
    }).expect("the new upload reserves its operation in place of the failed member");
    let out = twin.post(TransferInput::ReplacementReserved { target: ReserveTarget::Transfer(new), result: Ok((8, None)) });
    assert_eq!((out.removed(), out.outcomes_written()), (vec![KEY.to_string()], vec![(ids[0], 1, OutboxOutcome::Discarded)]), "the failed member goes in the batch of the new upload");
    twin.commit();
    assert_eq!((twin.state().open_id(KEY), twin.stage(ids[1])), (Some(new), TransferStage::Uploading), "the other member keeps uploading");
}

#[test]
fn xfer_batch_member_without_a_row_is_discarded_after_the_batch_confirmed() {
    let (mut twin, ids) = batch(&[KEY, OTHER], 2, false);
    file_done(&mut twin, ids[0]);
    file_done(&mut twin, ids[1]);
    let members = vec![(ids[0], 1, OutboxOutcome::Confirmed { revision: 9, result: None, row_id: None }), (ids[1], 1, OutboxOutcome::Failed { error: "protocol".into() })];
    twin.post(TransferInput::MutationOutcomes { batch_id: 1, members, intent_removed: true });
    twin.commit();
    assert_eq!(twin.stage(ids[1]), TransferStage::Failed);
    let (reply, out) = twin.discard(OTHER);
    assert!(out.replies(reply).is_empty(), "the discard answers after its write: {:?}", out.replies(reply));
    let out = twin.commit();
    assert_eq!((out.replies(reply), twin.stage(ids[1])), (vec![TransferAnswer::Ok], TransferStage::Done), "a failed member is no `InFlight` once the batch ended");
}
