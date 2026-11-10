//! Owner-level tests of the owner `Transfer`: the reservation of the `op_id` of the optimistic effects (D-79) and their rollback with the removal.

use super::transfer_harness::*;
use crate::owners::contract::{Online, RejectReason};
use crate::owners::store::{EffectRow, IntentEffects, StoreOp};
use crate::owners::write::outbox::{Fault, OutboxInput, OutboxOutcome, ReserveTarget};
use crate::owners::write::transfer::{TransferAnswer, TransferId, TransferInput, TransferStage};
use crate::runtime::Time;

const KEY: &str = "Message:m1:send";
const OP_ID: u64 = 7;

fn effects() -> IntentEffects { IntentEffects { rows: vec![EffectRow { model: "Message".into(), row_id: "m1".into(), values: "{\"text\":\"x\"}".into() }], ..Default::default() } }

fn accept_input(key: &str, reply: u64) -> TransferInput {
    let mut item = item(key, 2);
    item.mutation.effects = effects();
    TransferInput::Accept { epoch: EPOCH, item: Box::new(item), reply, now: Time(0) }
}

fn reserving(twin: &mut Twin) -> (TransferId, u64, Out) {
    let reply = twin.reply();
    let out = twin.post(accept_input(KEY, reply));
    let id = twin.state().open_id(KEY).expect("a reserving transfer");
    (id, reply, out)
}

fn reserved(twin: &mut Twin) -> (TransferId, u64) {
    let (id, reply, _) = reserving(twin);
    twin.post(TransferInput::OpReserved { target: ReserveTarget::Transfer(id), result: Ok(OP_ID) });
    (id, reply)
}

fn ready(twin: &mut Twin) -> TransferId {
    let (id, _) = reserved(twin);
    twin.commit();
    twin.copy_done(id);
    twin.commit();
    assert_eq!(twin.stage(id), TransferStage::PersistingAttempt);
    assert_eq!(twin.commit().steps().len(), 1);
    id
}

fn rolled_back(out: &Out) -> Vec<u64> { out.batches.iter().flat_map(|batch| batch.ops.iter()).filter_map(|op| if let StoreOp::RollBack { op_id } = op { Some(*op_id) } else { None }).collect() }

fn intents(out: &Out) -> Vec<(u64, IntentEffects)> { out.batches.iter().flat_map(|batch| batch.ops.iter()).filter_map(|op| if let StoreOp::Effects { op_id, effects, .. } = op { Some((*op_id, effects.clone())) } else { None }).collect() }

#[test]
fn xfer_accept_reserve_op() {
    let mut twin = Twin::new();
    let (id, reply, out) = reserving(&mut twin);
    assert_eq!(twin.stage(id), TransferStage::Reserving);
    assert!(matches!(out.outbox.as_slice(), [OutboxInput::ReserveOp { target: ReserveTarget::Transfer(found), .. }] if *found == id), "{:?}", out.outbox);
    assert_eq!((out.writes(), out.all_replies(), out.shows().len()), (0, 0, 0), "nothing is written or shown before the op_id");
    assert!(out.replies(reply).is_empty());
}

#[test]
fn xfer_accept_optimistic_row() {
    let mut twin = Twin::new();
    let (id, reply, _) = reserving(&mut twin);
    let out = twin.post(TransferInput::OpReserved { target: ReserveTarget::Transfer(id), result: Ok(OP_ID) });
    assert_eq!(twin.stage(id), TransferStage::Accepting);
    assert_eq!(out.writes(), 1, "the effects and the row of the upload are one transaction");
    assert_eq!(intents(&out), vec![(OP_ID, effects())]);
    assert_eq!(out.rows()[0].0.op_id, Some(OP_ID));
    assert!(out.replies(reply).is_empty(), "the call waits for the copy");
}

#[test]
fn xfer_reserving_batch_wait() {
    let mut twin = Twin::new();
    let mut first = item(KEY, 2);
    first.mutation.effects = effects();
    let mut second = item("Message:m2:send", 2);
    second.mutation.effects = effects();
    let reply = twin.reply();
    twin.post(TransferInput::AcceptBatch { epoch: EPOCH, items: vec![first, second], joint: Box::new(joint()), reply, now: Time(0) });
    let first = twin.state().open_id(KEY).unwrap();
    let second = twin.state().open_id("Message:m2:send").unwrap();
    let out = twin.post(TransferInput::OpReserved { target: ReserveTarget::Transfer(first), result: Ok(7) });
    assert_eq!((twin.stage(first), twin.stage(second), out.writes()), (TransferStage::Reserving, TransferStage::Reserving, 0));
    let out = twin.post(TransferInput::OpReserved { target: ReserveTarget::Transfer(second), result: Ok(8) });
    assert_eq!(out.writes(), 0);
    let group = twin.t(first).batch_id.unwrap();
    let out = twin.post(TransferInput::OpReserved { target: ReserveTarget::Group(group), result: Ok(9) });
    assert_eq!((twin.stage(first), twin.stage(second), out.writes()), (TransferStage::Accepting, TransferStage::Accepting, 1));
    assert_eq!(out.rows().len(), 2);
}

#[test]
fn xfer_reserving_refused() {
    let mut twin = Twin::new();
    let (id, reply, _) = reserving(&mut twin);
    let out = twin.post(TransferInput::OpReserved { target: ReserveTarget::Transfer(id), result: Err(Fault::Ended(crate::ffi::EndCause::SessionEnded)) });
    assert_eq!(twin.stage(id), TransferStage::Done);
    assert_eq!(Twin::fault(&out, reply), Some(Fault::Ended(crate::ffi::EndCause::SessionEnded)));
    assert_eq!(out.writes(), 0);
}

#[test]
fn xfer_reserving_end() {
    for epoch_end in [true, false] {
        let mut twin = Twin::new();
        let (id, reply, _) = reserving(&mut twin);
        let out = if epoch_end { twin.end_epoch() } else { twin.close() };
        assert_eq!(twin.stage(id), TransferStage::Done);
        assert_eq!(Twin::fault(&out, reply), Some(Fault::Ended(if epoch_end { crate::ffi::EndCause::SessionEnded } else { crate::ffi::EndCause::Closed })), "epoch_end {epoch_end}");
        assert_eq!(out.drained(), usize::from(!epoch_end));
        let late = twin.post(TransferInput::OpReserved { target: ReserveTarget::Transfer(id), result: Ok(OP_ID) });
        assert!(Twin::quiet(&late), "a late OpReserved is foreign: {late:?}");
    }
}

#[test]
fn xfer_reserving_refuses() {
    let mut twin = Twin::new();
    reserving(&mut twin);
    for (name, (reply, out)) in [("cancel", twin.cancel(KEY)), ("discard", twin.discard(KEY)), ("retry", twin.retry(KEY, None))] {
        assert_eq!(Twin::fault(&out, reply), Some(Fault::InFlight), "{name}");
        assert_eq!(out.writes(), 0, "{name}");
    }
}

#[test]
fn xfer_reserving_signals() {
    let mut twin = Twin::new();
    let (id, ..) = reserving(&mut twin);
    twin.ignores(id, vec![TransferInput::Online(Online(false)), TransferInput::Online(Online(true)), TransferInput::PowerSaving(true), TransferInput::PowerSaving(false), TransferInput::SlotFreed]);
    assert!(twin.state().online == Some(true) && !twin.state().saving, "the fields follow the signals");
}

#[test]
fn xfer_accept_key_open() {
    let mut twin = Twin::new();
    let id = twin.accepted(KEY, 2);
    let out = twin.post(accept_input(KEY, 90));
    assert_eq!(Twin::fault(&out, 90), Some(Fault::InFlight));
    assert_eq!((out.writes(), out.outbox.len()), (0, 0), "no new transfer, no reservation");
    let out = twin.post(TransferInput::AcceptBatch { joint: Box::new(joint()), epoch: EPOCH, items: vec![item("Message:m2:send", 2), item(KEY, 2)], reply: 91, now: Time(0) });
    assert_eq!(Twin::fault(&out, 91), Some(Fault::InFlight), "one open key refuses the whole batch");
    assert_eq!((out.writes(), twin.state().open_id("Message:m2:send")), (0, None));
    assert_eq!(twin.stage(id), TransferStage::PersistingAttempt);
}

#[test]
fn xfer_accept_replaces_failed() {
    let mut twin = Twin::new();
    let old = ready(&mut twin);
    twin.step_error(old, false);
    twin.commit();
    assert_eq!(twin.stage(old), TransferStage::Failed);

    let out = twin.post(accept_input(KEY, 90));
    assert!(out.replies(90).is_empty(), "the replacement must not be refused: {:?}", out.replies(90));
    let new = out.outbox.iter().find_map(|input| match input {
        OutboxInput::ReserveOp { target: ReserveTarget::Transfer(id), .. } if *id != old => Some(*id),
        _ => None,
    }).expect("the replacement reserves its operation");
    let out = twin.post(TransferInput::OpReserved { target: ReserveTarget::Transfer(new), result: Ok(8) });
    assert_eq!(out.writes(), 1, "removal and acceptance must share a Store batch");
    assert_eq!(out.removed(), vec![KEY.to_string()]);
    assert_eq!(rolled_back(&out), vec![OP_ID]);
    assert_eq!(out.outcomes_written(), vec![(old, 1, OutboxOutcome::Discarded)]);
    assert_eq!(out.rows().len(), 1);
    let committed = twin.commit();
    assert_eq!(twin.stage(old), TransferStage::Done);
    assert_eq!(committed.deletes(), 1);
    let (reply, outcome) = twin.outcome(old, 1);
    assert_eq!(outcome.replies(reply), vec![TransferAnswer::Outcome(OutboxOutcome::Discarded, None)]);
}

#[test]
fn xfer_accept_replaces_failed_without_operation_after_open() {
    let mut row = stored(1, KEY, 2);
    row.failed = Some("refused".into());
    row.op_id = None;
    row.has_intent = false;
    row.has_operation = false;
    let (mut twin, _) = Twin::opened(vec![row], Default::default());
    twin.post(TransferInput::RestoreReady { epoch: EPOCH });
    assert_eq!(twin.stage(1), TransferStage::Failed);
    let out = twin.post(accept_input(KEY, 90));
    assert!(out.replies(90).is_empty(), "failed upload without operation must not block: {:?}", out.replies(90));
    let new = out.outbox.iter().find_map(|input| match input {
        OutboxInput::ReserveOp { target: ReserveTarget::Transfer(id), .. } if *id != 1 => Some(*id),
        _ => None,
    }).expect("the replacement reserves its operation");
    let out = twin.post(TransferInput::OpReserved { target: ReserveTarget::Transfer(new), result: Ok(8) });
    assert_eq!(out.writes(), 1);
    assert_eq!(out.removed(), vec![KEY.to_string()]);
    assert!(rolled_back(&out).is_empty());
    assert_eq!(out.outcomes_written(), vec![(1, 1, OutboxOutcome::Discarded)]);
    assert_eq!(out.rows().len(), 1);
}

#[test]
fn xfer_remove_rolls_back_optimistic() {
    let mut twin = Twin::new();
    let id = ready(&mut twin);
    let (reply, out) = twin.cancel(KEY);
    assert_eq!(rolled_back(&out), vec![OP_ID], "the row goes with the removal in one batch");
    assert_eq!(out.removed(), vec![KEY.to_string()]);
    let out = twin.commit();
    assert_eq!((twin.stage(id), out.replies(reply)), (TransferStage::Done, vec![TransferAnswer::Cancelled(true)]));
}

/// P-XFER-30: the removal rolls the effects back in every cause before the final mutation left, never after it and never for a confirmed one.
#[test]
fn xfer_prop_optimistic_row_once() {
    let mut cancel_copying = Twin::new();
    let (id, _) = reserved(&mut cancel_copying);
    cancel_copying.commit();
    assert_eq!(cancel_copying.stage(id), TransferStage::Copying);
    assert_eq!(rolled_back(&cancel_copying.cancel(KEY).1), vec![OP_ID], "cancel while copying");

    let mut copy_failed = Twin::new();
    let (id, _) = reserved(&mut copy_failed);
    copy_failed.commit();
    assert_eq!(rolled_back(&copy_failed.copy_failed(id)), vec![OP_ID], "the copy failed");

    let mut stored_copy = Twin::new();
    let (id, _) = reserved(&mut stored_copy);
    stored_copy.commit();
    stored_copy.copy_done(id);
    assert_eq!(rolled_back(&stored_copy.reject(RejectReason::Storage("full".into()))), vec![OP_ID], "the copy is not durable");

    let mut failed = Twin::new();
    let id = ready(&mut failed);
    failed.step_error(id, false);
    failed.commit();
    assert_eq!(failed.stage(id), TransferStage::Failed);
    assert_eq!(rolled_back(&failed.discard(KEY).1), vec![OP_ID], "discard of a failed transfer");

    let mut confirmed = Twin::new();
    confirmed.completing_with_effects();
    let out = confirmed.mutation_outcome(KEY, OutboxOutcome::Confirmed { revision: 5, result: None, row_id: None });
    assert!(rolled_back(&out).is_empty(), "a confirmed mutation keeps its row");

    let mut sent = Twin::new();
    sent.completing_with_effects();
    let out = sent.mutation_outcome(KEY, OutboxOutcome::Discarded);
    assert!(rolled_back(&out).is_empty(), "Outbox rolled the operation back, the removal does not repeat it");
}

impl Twin {
    /// `Mutating` of a transfer with effects: the final mutation left under the reserved `op_id`.
    fn completing_with_effects(&mut self) {
        let id = ready(self);
        self.step_ok(id, 1, Some("u"));
        self.commit();
        assert_eq!(self.stage(id), TransferStage::PersistingAttempt);
        assert_eq!(self.commit().steps().len(), 1);
        self.step_ok(id, 2, None);
        self.commit();
        assert_eq!(self.stage(id), TransferStage::PersistingAttempt);
        assert_eq!(self.commit().completes(), 1);
        self.completed(id);
        let out = self.commit();
        assert_eq!(self.stage(id), TransferStage::Mutating);
        assert!(matches!(out.outbox.as_slice(), [OutboxInput::SubmitReserved { op_id: OP_ID, op, .. }] if op.effects.is_empty()), "{:?}", out.outbox);
    }
}
