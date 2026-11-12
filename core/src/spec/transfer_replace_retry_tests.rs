//! Owner-level tests of the owner `Transfer`: the refusal of the accept batch of a replacement (D-344) and the retry of a refused removal write (D-345).

use super::transfer_harness::*;
use crate::owners::contract::RejectReason;
use crate::owners::store::{EffectRow, IntentEffects, StoreOp};
use crate::owners::write::outbox::{Fault, OutboxInput, OutboxOutcome, ReserveTarget};
use crate::owners::write::transfer::{TransferAnswer, TransferId, TransferInput, TransferStage};

const KEY: &str = "Message:m1:send";
const OTHER: &str = "Message:m2:send";
const THIRD: &str = "Message:m3:send";
const OP_ID: u64 = 7;

fn accept_with_effects(key: &str, reply: u64) -> TransferInput {
    let mut element = item(key, 2);
    element.mutation.effects = IntentEffects { rows: vec![EffectRow { model: "Message".into(), row_id: "m1".into(), values: "{\"text\":\"x\"}".into() }], ..Default::default() };
    TransferInput::Accept { epoch: EPOCH, item: Box::new(element), reply }
}

fn rolled_back(out: &Out) -> Vec<u64> { out.batches.iter().flat_map(|batch| batch.ops.iter()).filter_map(|op| if let StoreOp::RollBack { op_id } = op { Some(*op_id) } else { None }).collect() }

/// A `Failed` transfer under `KEY` with its optimistic row, and the replacing `upload` after the reservation: the accept batch of the replacement is in the Store.
fn replacing() -> (Twin, TransferId, TransferId, Out) {
    let mut twin = Twin::new();
    let reply = twin.reply();
    twin.post(accept_with_effects(KEY, reply));
    let old = twin.state().open_id(KEY).expect("a reserving transfer");
    twin.post(TransferInput::OpReserved { target: ReserveTarget::Transfer(old), result: Ok(OP_ID) });
    twin.commit();
    twin.copy_done(old);
    twin.commit();
    twin.commit();
    twin.step_error(old, false);
    twin.commit();
    assert_eq!(twin.stage(old), TransferStage::Failed);
    let out = twin.post(accept_with_effects(KEY, 90));
    let new = out.outbox.iter().find_map(|input| match input {
        OutboxInput::ReserveReplacement { target: ReserveTarget::Transfer(id), .. } if *id != old => Some(*id),
        _ => None,
    }).expect("the replacement reserves its operation");
    let accept = twin.post(TransferInput::ReplacementReserved { target: ReserveTarget::Transfer(new), result: Ok((8, None)) });
    assert_eq!((accept.writes(), accept.removed(), rolled_back(&accept)), (1, vec![KEY.to_string()], vec![OP_ID]));
    (twin, old, new, accept)
}

fn finished_replacement(out: &Out, committed: bool) -> usize { out.outbox.iter().filter(|input| matches!(input, OutboxInput::FinishReplacement { committed: found, .. } if *found == committed)).count() }

#[test]
fn xfer_replace_stale_generation() {
    let (mut twin, old, new, accept) = replacing();
    let generations = std::collections::BTreeMap::from([("Message".to_string(), 7)]);
    let out = twin.reject(RejectReason::StaleGeneration { models: vec!["Message".into()], generations: generations.clone() });
    assert_eq!(out.all_replies(), 0, "the call of `upload` waits for the repeated batch");
    assert_eq!(out.batches.len(), 1, "the accept batch goes again");
    assert_eq!(out.batches[0].basis, generations);
    assert_eq!((out.removed(), rolled_back(&out)), (vec![KEY.to_string()], vec![OP_ID]), "the repeated batch carries the prefix of the replacement");
    assert_eq!(out.removed(), accept.removed());
    assert_eq!(finished_replacement(&out, false), 0, "the replacement is not ended by the refusal");
    refuses_calls(&mut twin, KEY);
    let done = twin.commit();
    assert_eq!(finished_replacement(&done, true), 1);
    let (reply, outcome) = twin.outcome(old, 1);
    assert_eq!(outcome.replies(reply), vec![TransferAnswer::Outcome(OutboxOutcome::Discarded, None)]);
    assert_eq!(twin.stage(new), TransferStage::Copying);
}

#[test]
fn xfer_replace_rejected_invalid_keeps_failed() {
    let (mut twin, old, _, _) = replacing();
    let error = twin.t(old).error.clone().expect("the failure has a cause");
    let out = twin.reject(RejectReason::Invalid("bad row".into()));
    assert_eq!(Twin::fault(&out, 90), Some(Fault::Rejected(RejectReason::Invalid("bad row".into()))));
    assert_eq!(finished_replacement(&out, false), 1);
    assert_eq!(twin.stage(old), TransferStage::Failed);
    let (reply, outcome) = twin.outcome(old, 1);
    assert!(matches!(outcome.replies(reply).as_slice(), [TransferAnswer::Outcome(OutboxOutcome::Failed { error: found }, _)] if *found == error));
    let (reply, retried) = twin.retry(KEY, None);
    assert_ne!(Twin::fault(&retried, reply), Some(Fault::InFlight), "the previous failed transfer takes `retry` again");
}

#[test]
fn xfer_replace_rejected_ended() {
    for (reason, cause, fault) in [(RejectReason::NoSession, crate::ffi::EndCause::SessionEnded, Fault::SessionEnded), (RejectReason::Closed, crate::ffi::EndCause::Closed, Fault::Closed)] {
        let (mut twin, old, _, _) = replacing();
        let out = twin.reject(reason.clone());
        assert_eq!(Twin::fault(&out, 90), Some(fault), "{reason:?}: the call ends with the cause");
        assert_eq!(finished_replacement(&out, false), 1, "{reason:?}");
        assert_eq!(twin.stage(old), TransferStage::Done, "{reason:?}");
        let (reply, outcome) = twin.outcome(old, 1);
        assert_eq!(outcome.replies(reply), vec![TransferAnswer::Outcome(OutboxOutcome::Ended(cause), None)], "{reason:?}: the wait of the previous transfer ends with the cause");
    }
}

/// A batch of `keys` after the acceptance write: the files are in the network.
fn uploading_group(keys: &[&str]) -> (Twin, Vec<TransferId>) {
    let mut twin = Twin::with_slots(4);
    let ids = twin.accepted_items(keys.iter().map(|key| item(key, 1)).collect(), joint());
    for _ in &ids {
        assert_eq!(twin.commit().steps().len(), 1);
    }
    (twin, ids)
}

#[test]
fn xfer_group_storage_retry_same_batch() {
    let (mut twin, ids) = uploading_group(&[KEY, OTHER]);
    let (_, first) = twin.cancel(KEY);
    let (_, second) = twin.cancel(OTHER);
    assert_eq!((first.removed(), second.removed()), (vec![KEY.to_string()], vec![OTHER.to_string()]));
    let refused = twin.reject(storage());
    assert_eq!(refused.timers, 1);
    let tick = twin.tick_retry(ids[0]);
    assert_eq!((tick.batches.len(), tick.removed()), (1, vec![KEY.to_string()]), "the retry writes the refused batch, not the elements that wait in `Removing` now");
    let committed = [twin.commit(), twin.commit()];
    let other_writes = [&second, &tick, &committed[0], &committed[1]].iter().flat_map(|out| out.removed()).filter(|key| key == OTHER).count();
    assert_eq!(other_writes, 1, "the element of the batch in flight is written once");
}

#[test]
fn xfer_group_storage_one_timer() {
    let mut twin = Twin::with_slots(4);
    let reply = twin.reply();
    let items = [KEY, OTHER, THIRD].iter().map(|key| item(key, 1)).collect();
    twin.post(TransferInput::AcceptBatch { joint: Box::new(joint()), epoch: EPOCH, items, reply });
    let ids: Vec<TransferId> = [KEY, OTHER, THIRD].iter().map(|key| twin.state().open_id(key).expect("an element")).collect();
    twin.reserve_batch(&ids);
    twin.commit();
    let first = twin.copy_failed(ids[0]);
    assert_eq!((first.batches.len(), first.removed().len()), (1, 3), "the whole batch goes in one write");
    let refused = twin.reject(storage());
    assert_eq!(refused.timers, 1, "one timer for the batch");
    let tick = twin.tick_retry(ids[0]);
    assert_eq!(tick.batches.len(), 1);
    assert_eq!(tick.batches[0].ops, first.batches[0].ops, "the same operations");
}

#[test]
fn xfer_removing_confirmed_epoch_ended() {
    let mut twin = Twin::new();
    let id = twin.mutating(KEY);
    let (waiter, pending) = twin.outcome(id, 1);
    let confirmed = OutboxOutcome::Confirmed { revision: 7, result: None, row_id: None };
    let mut outs = vec![pending, twin.mutation_outcome(KEY, confirmed.clone())];
    outs.push(twin.reject(storage()));
    assert_eq!(twin.stage(id), TransferStage::Removing);
    outs.push(twin.end_epoch());
    let answers: Vec<TransferAnswer> = outs.iter().flat_map(|out| out.replies(waiter)).collect();
    assert_eq!(answers, vec![TransferAnswer::Outcome(confirmed.clone(), None)], "the outcome of the server stays after the end of the epoch");
}

#[test]
fn xfer_removing_invalid_group_continues() {
    let (mut twin, ids) = uploading_group(&[KEY, OTHER, THIRD]);
    for id in &ids[..2] {
        twin.step_ok(*id, 1, None);
        twin.commit();
        twin.commit();
        twin.completed(*id);
        twin.commit();
    }
    assert_eq!((twin.stage(ids[0]), twin.stage(ids[1]), twin.stage(ids[2])), (TransferStage::BatchReady, TransferStage::BatchReady, TransferStage::Uploading));
    let (_, cancel) = twin.cancel(THIRD);
    assert_eq!(cancel.removed(), vec![THIRD.to_string()]);
    let out = twin.reject(RejectReason::Invalid("bad row".into()));
    assert_eq!(out.writes(), 1, "the rest of the batch goes on in the same step: the write of its intent");
    let stages: Vec<TransferStage> = ids[..2].iter().map(|id| twin.stage(*id)).collect();
    assert_eq!(stages, vec![TransferStage::PersistingBatchIntent; 2]);
}
