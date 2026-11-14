//! Owner-level tests of the owner `Transfer`: the reservation of the `op_id` of the optimistic effects (D-79) and their rollback with the removal.

use super::transfer_harness::*;
use crate::owners::contract::{Online, RejectReason};
use crate::owners::store::{EffectRow, IntentEffects, StoreOp};
use crate::owners::write::outbox::{Fault, OutboxInput, OutboxOutcome, ReserveTarget};
use crate::owners::write::transfer::{TransferAnswer, TransferId, TransferInput, TransferStage};

const KEY: &str = "Message:m1:send";
const OP_ID: u64 = 7;

fn effects() -> IntentEffects { IntentEffects { rows: vec![EffectRow { model: "Message".into(), row_id: "m1".into(), values: "{\"text\":\"x\"}".into() }], ..Default::default() } }

fn accept_input(key: &str, reply: u64) -> TransferInput {
    let mut item = item(key, 2);
    item.mutation.effects = effects();
    TransferInput::Accept { epoch: EPOCH, item: Box::new(item), reply }
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


fn intents(out: &Out) -> Vec<(u64, IntentEffects)> { out.batches.iter().flat_map(|batch| batch.ops.iter()).filter_map(|op| if let StoreOp::Effects { op_id, effects, .. } = op { Some((*op_id, effects.clone())) } else { None }).collect() }

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
    twin.post(TransferInput::AcceptBatch { epoch: EPOCH, items: vec![first, second], joint: Box::new(joint()), reply });
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
fn xfer_reserving_signals() {
    let mut twin = Twin::new();
    let (id, ..) = reserving(&mut twin);
    refuses_calls(&mut twin, KEY);
    twin.ignores(id, vec![TransferInput::Online(Online(false)), TransferInput::Online(Online(true)), TransferInput::PowerSaving(true), TransferInput::PowerSaving(false), TransferInput::SlotFreed]);
    assert!(twin.state().online == Some(true) && !twin.state().saving, "the fields follow the signals");
}

/// `AcceptBatch` without a transfer: a key twice in one call is a wrong call, `Invalid` with a text, no element is accepted (D-398).
#[test]
fn xfer_accept_batch_key_twice() {
    let mut twin = Twin::new();
    let out = twin.post(TransferInput::AcceptBatch { joint: Box::new(joint()), epoch: EPOCH, items: vec![item(KEY, 2), item(KEY, 2)], reply: 91 });
    assert!(matches!(Twin::fault(&out, 91), Some(Fault::Rejected(RejectReason::Invalid(text))) if text.contains(KEY)), "{:?}", out.replies(91));
    assert_eq!((twin.state().transfers.len(), out.outbox.len()), (0, 0));
}

#[test]
fn xfer_accept_key_open() {
    for stage in [TransferStage::Reserving, TransferStage::Accepting, TransferStage::Copying, TransferStage::CopyReady, TransferStage::PersistingCopy, TransferStage::Ready, TransferStage::PersistingAttempt, TransferStage::Uploading, TransferStage::Completing, TransferStage::Mutating, TransferStage::Removing, TransferStage::AwaitingSource] {
        let mut twin = if matches!(stage, TransferStage::Ready | TransferStage::AwaitingSource) { Twin::offline() } else { Twin::new() };
        let id = match stage {
            TransferStage::Reserving => reserving(&mut twin).0,
            TransferStage::Accepting => reserved(&mut twin).0,
            TransferStage::Copying => twin.copying(KEY, 2),
            TransferStage::CopyReady => {
                twin.post(TransferInput::AcceptBatch { joint: Box::new(joint()), epoch: EPOCH, items: vec![item(KEY, 2), item("Message:m2:send", 2)], reply: 80 });
                let ids = [twin.state().open_id(KEY).unwrap(), twin.state().open_id("Message:m2:send").unwrap()];
                twin.reserve_batch(&ids);
                twin.commit();
                twin.copy_done(ids[0]);
                ids[0]
            }
            TransferStage::PersistingCopy => twin.persisting_copy(KEY, 2),
            TransferStage::Ready | TransferStage::PersistingAttempt => twin.accepted(KEY, 2),
            TransferStage::Uploading => twin.uploading(KEY, 2),
            TransferStage::Completing => twin.completing(KEY),
            TransferStage::Mutating => twin.mutating(KEY),
            TransferStage::Removing => {
                let id = twin.mutating(KEY);
                twin.mutation_outcome(KEY, OutboxOutcome::Confirmed { revision: 3, result: None, row_id: None });
                id
            }
            TransferStage::AwaitingSource => {
                let mut pending = item(KEY, 1);
                pending.source = None;
                pending.steps = None;
                twin.post(TransferInput::Accept { epoch: EPOCH, item: Box::new(pending), reply: 80 });
                let id = twin.state().open_id(KEY).unwrap();
                twin.post(TransferInput::OpReserved { target: ReserveTarget::Transfer(id), result: Ok(OP_ID) });
                twin.commit();
                id
            }
            _ => unreachable!(),
        };
        assert_eq!(twin.stage(id), stage);
        let count = twin.state().transfers.len();
        let before = format!("{:?}", twin.t(id));
        let out = twin.post(accept_input(KEY, 90));
        assert_eq!(Twin::fault(&out, 90), Some(Fault::InFlight), "{stage:?}");
        assert_eq!((out.writes(), out.outbox.len(), twin.state().transfers.len()), (0, 0, count), "{stage:?}");
        assert_eq!(format!("{:?}", twin.t(id)), before, "{stage:?}");
        if stage != TransferStage::CopyReady {
            let out = twin.post(TransferInput::AcceptBatch { joint: Box::new(joint()), epoch: EPOCH, items: vec![item("Message:m2:send", 2), item(KEY, 2)], reply: 91 });
            assert_eq!(Twin::fault(&out, 91), Some(Fault::InFlight), "{stage:?}");
            assert_eq!((out.writes(), twin.state().open_id("Message:m2:send")), (0, None));
        }
    }
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
        OutboxInput::ReserveReplacement { target: ReserveTarget::Transfer(id), .. } if *id != old => Some(*id),
        _ => None,
    }).expect("the replacement reserves its operation");
    let out = twin.post(TransferInput::ReplacementReserved { target: ReserveTarget::Transfer(new), result: Ok((8, None)) });
    assert_eq!(out.writes(), 1, "removal and acceptance must share a Store batch");
    assert_eq!(out.removed(), vec![KEY.to_string()]);
    assert_eq!(out.rolled_back(), vec![OP_ID]);
    assert_eq!(out.outcomes_written(), vec![(old, 1, OutboxOutcome::Discarded)]);
    assert_eq!(out.rows().len(), 1);
    let committed = twin.commit();
    assert!(!twin.state().transfers.contains_key(&old));
    assert_eq!(committed.deletes(), 1);
    let (reply, outcome) = twin.outcome(old, 1);
    assert_eq!(outcome.replies(reply), vec![TransferAnswer::Outcome(OutboxOutcome::Discarded, None)]);
}

#[test]
fn xfer_replace_keeps_abort_debt() {
    for (complete, online) in [(true, true), (true, false), (false, true), (false, false)] {
        let mut twin = Twin::new();
        let old = ready(&mut twin);
        twin.step_ok(old, 1, Some("u1"));
        twin.commit();
        twin.commit();
        if complete {
            twin.step_ok(old, 2, None);
            twin.commit();
            twin.commit();
            assert_eq!(twin.stage(old), TransferStage::Completing);
        }
        let failed = if complete { twin.complete_error(old, false) } else { twin.step_error(old, false) };
        assert_eq!(failed.orphans_written(), if complete { vec![] } else { vec![("u1".to_string(), KEY.to_string())] });
        if !online { twin.online(false); }
        let failure_committed = twin.commit();
        assert_eq!(twin.stage(old), TransferStage::Failed);
        if complete {
            assert_eq!(twin.t(old).upload_id.as_deref(), Some("u1"));
            assert!(failure_committed.orphan_aborts().is_empty());
        } else {
            assert_eq!(failure_committed.orphan_aborts().len(), usize::from(online));
        }

        let accepted = twin.post(accept_input(KEY, 90));
        let new = accepted.outbox.iter().find_map(|input| match input {
            OutboxInput::ReserveReplacement { target: ReserveTarget::Transfer(id), .. } if *id != old => Some(*id),
            _ => None,
        }).expect("the replacement reserves its operation");
        let written = twin.post(TransferInput::ReplacementReserved { target: ReserveTarget::Transfer(new), result: Ok((8, None)) });
        assert_eq!(written.writes(), 1);
        assert_eq!(written.removed(), vec![KEY.to_string()]);
        assert_eq!(written.orphans_written(), if complete { vec![("u1".to_string(), KEY.to_string())] } else { vec![] });
        assert!(written.orphan_aborts().is_empty());
        let committed = twin.commit();
        assert!(written.orphans_removed().is_empty() && committed.orphans_removed().is_empty());
        if complete && online {
            assert_eq!(committed.orphan_aborts(), vec!["u1".to_string()]);
        } else {
            assert!(committed.orphan_aborts().is_empty());
            if !online { assert_eq!(twin.online(true).orphan_aborts(), vec!["u1".to_string()]); }
        }
    }
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
        OutboxInput::ReserveReplacement { target: ReserveTarget::Transfer(id), .. } if *id != 1 => Some(*id),
        _ => None,
    }).expect("the replacement reserves its operation");
    let out = twin.post(TransferInput::ReplacementReserved { target: ReserveTarget::Transfer(new), result: Ok((8, None)) });
    assert_eq!(out.writes(), 1);
    assert_eq!(out.removed(), vec![KEY.to_string()]);
    assert!(out.rolled_back().is_empty());
    assert_eq!(out.outcomes_written(), vec![(1, 1, OutboxOutcome::Discarded)]);
    assert_eq!(out.rows().len(), 1);
}

#[test]
fn xfer_accept_batch_replaces_failed_single() {
    let mut twin = Twin::new();
    let old = ready(&mut twin);
    twin.step_error(old, false);
    twin.commit();
    let out = twin.post(TransferInput::AcceptBatch { joint: Box::new(joint()), epoch: EPOCH, items: vec![item(KEY, 2), item("Message:m2:send", 2)], reply: 91 });
    assert!(out.replies(91).is_empty(), "the failed key must not reject the batch: {:?}", out.replies(91));
    let ids: Vec<_> = out.outbox.iter().filter_map(|input| match input {
        OutboxInput::ReserveOp { target: ReserveTarget::Transfer(id), .. } | OutboxInput::ReserveReplacement { target: ReserveTarget::Transfer(id), .. } => Some(*id),
        _ => None,
    }).collect();
    let group = out.outbox.iter().find_map(|input| match input {
        OutboxInput::ReserveOp { target: ReserveTarget::Group(id), .. } => Some(*id),
        _ => None,
    }).expect("the batch reserves its operation");
    assert_eq!(ids.len(), 2);
    twin.post(TransferInput::ReplacementReserved { target: ReserveTarget::Transfer(ids[0]), result: Ok((8, None)) });
    twin.post(TransferInput::OpReserved { target: ReserveTarget::Transfer(ids[1]), result: Ok(9) });
    let out = twin.post(TransferInput::OpReserved { target: ReserveTarget::Group(group), result: Ok(10) });
    assert_eq!(out.writes(), 1);
    assert_eq!(out.removed(), vec![KEY.to_string()]);
    assert_eq!(out.rolled_back(), vec![OP_ID]);
    assert_eq!(out.outcomes_written(), vec![(old, 1, OutboxOutcome::Discarded)]);
    assert_eq!(out.rows().len(), 2);
}

fn replacing_failed() -> (Twin, TransferId, TransferId) {
    let mut twin = Twin::new();
    let old = ready(&mut twin);
    twin.step_error(old, false);
    twin.commit();
    let out = twin.post(accept_input(KEY, 90));
    let new = out.outbox.iter().find_map(|input| match input {
        OutboxInput::ReserveReplacement { target: ReserveTarget::Transfer(id), .. } if *id != old => Some(*id),
        _ => None,
    }).expect("the replacement reserves its operation");
    (twin, old, new)
}

/// Failed (replaceable) x Cancel, Discard: the command waits for the end of the interval and runs by the row of the stage the key leaves to. The batch confirmed removes the new transfer, the batch refused removes the restored failed one. `Retry` is refused.
#[test]
fn xfer_replace_interval_refuses() {
    let (mut twin, old, new) = replacing_failed();
    let (reply, out) = twin.retry(KEY, None);
    assert_eq!(Twin::fault(&out, reply), Some(Fault::InFlight), "retry before reservation");
    let (cancel, out) = twin.cancel(KEY);
    assert!(out.replies(cancel).is_empty(), "cancel waits before reservation");
    let (discard, out) = twin.discard(KEY);
    assert!(out.replies(discard).is_empty(), "discard waits before reservation");
    let reserved = twin.post(TransferInput::ReplacementReserved { target: ReserveTarget::Transfer(new), result: Ok((8, None)) });
    let (reply, out) = twin.retry(KEY, None);
    assert_eq!(Twin::fault(&out, reply), Some(Fault::InFlight), "retry before Store answers");
    for (name, command) in [("cancel", cancel), ("discard", discard)] {
        assert!(reserved.replies(command).is_empty() && out.replies(command).is_empty(), "{name} waits before Store answers");
    }
    let out = twin.commit();
    assert!(!twin.state().transfers.contains_key(&old), "the batch confirmed removes the replaced transfer: {out:?}");
    let mut answers = out.replies(cancel);
    answers.extend(out.replies(discard));
    while answers.len() < 2 && twin.has_pending() { let out = twin.commit(); answers.extend(out.replies(cancel)); answers.extend(out.replies(discard)); }
    assert_eq!(twin.state().open_id(KEY), None, "the new transfer of the key is removed");
    assert!(answers.contains(&TransferAnswer::Cancelled(true)) && answers.contains(&TransferAnswer::Ok), "each command is answered once: {answers:?}");
    assert_eq!(answers.len(), 2);
}

#[test]
fn xfer_replace_rejected_keeps_failed() {
    let (mut twin, old, new) = replacing_failed();
    let error = twin.t(old).error.clone().expect("the failure has a cause");
    twin.post(TransferInput::ReplacementReserved { target: ReserveTarget::Transfer(new), result: Ok((8, None)) });
    let out = twin.reject(RejectReason::Storage("disk".into()));
    assert_eq!(Twin::fault(&out, 90), Some(Fault::Rejected(RejectReason::Storage("disk".into()))));
    assert_eq!(twin.stage(old), TransferStage::Failed);
    let (reply, outcome) = twin.outcome(old, 1);
    assert!(matches!(outcome.replies(reply).as_slice(), [TransferAnswer::Outcome(OutboxOutcome::Failed { error: found }, _)] if *found == error));
    let (reply, out) = twin.discard(KEY);
    assert_ne!(Twin::fault(&out, reply), Some(Fault::InFlight));
}

/// P-XFER-30: the removal rolls the effects back in every cause before the final mutation left, never after it and never for a confirmed one.
#[test]
fn xfer_prop_optimistic_row_once() {
    let mut cancel_copying = Twin::new();
    let (id, _) = reserved(&mut cancel_copying);
    cancel_copying.commit();
    assert_eq!(cancel_copying.stage(id), TransferStage::Copying);
    assert_eq!(cancel_copying.cancel(KEY).1.rolled_back(), vec![OP_ID], "cancel while copying");

    let mut copy_failed = Twin::new();
    let (id, _) = reserved(&mut copy_failed);
    copy_failed.commit();
    assert_eq!(copy_failed.copy_failed(id).rolled_back(), vec![OP_ID], "the copy failed");

    let mut stored_copy = Twin::new();
    let (id, _) = reserved(&mut stored_copy);
    stored_copy.commit();
    stored_copy.copy_done(id);
    assert_eq!(stored_copy.reject(RejectReason::Storage("full".into())).rolled_back(), vec![OP_ID], "the copy is not durable");

    let mut failed = Twin::new();
    let id = ready(&mut failed);
    failed.step_error(id, false);
    failed.commit();
    assert_eq!(failed.stage(id), TransferStage::Failed);
    assert_eq!(failed.discard(KEY).1.rolled_back(), vec![OP_ID], "discard of a failed transfer");

    let mut plain = Twin::new();
    let id = ready(&mut plain);
    let (reply, out) = plain.cancel(KEY);
    assert_eq!((out.rolled_back(), out.removed()), (vec![OP_ID], vec![KEY.to_string()]), "cancel of a ready transfer: the row goes with the removal in one batch");
    let out = plain.commit();
    assert_eq!((plain.stage(id), out.replies(reply)), (TransferStage::Done, vec![TransferAnswer::Cancelled(true)]));

    let mut confirmed = Twin::new();
    confirmed.completing_with_effects();
    let out = confirmed.mutation_outcome(KEY, OutboxOutcome::Confirmed { revision: 5, result: None, row_id: None });
    assert!(out.rolled_back().is_empty(), "a confirmed mutation keeps its row");

    let mut sent = Twin::new();
    sent.completing_with_effects();
    let out = sent.mutation_outcome(KEY, OutboxOutcome::Discarded);
    assert!(out.rolled_back().is_empty(), "Outbox rolled the operation back, the removal does not repeat it");
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

/// Failed upload x new upload of the key x `discard` without an action (Outbox input), `discard` with an action and `cancelUpload` (Transfer inputs) in the replace interval, with the real Outbox and Transfer on one scheduler. Both outcomes of the batch: each call is answered once, no transfer and no operation of the key is left.
/// Observable: the answers by call, the open transfer of the key, the open operations of Outbox. Catches: a command lost or answered twice, `Err(InFlight)` in the interval, a command that circles between the owners.
#[test]
fn xfer_replace_interval_two_owners() {
    for (mutated, refused) in [(false, None), (false, Some(RejectReason::Storage("disk".into()))), (true, None), (true, Some(RejectReason::Storage("disk".into())))] {
        let mut twin = Twin::with_outbox();
        twin.post(accept_input(KEY, 90));
        let old = twin.state().open_id(KEY).expect("the accepting transfer");
        twin.answer_both(None);
        twin.copy_done(old);
        twin.answer_both(None);
        twin.answer_both(None);
        if mutated {
            twin.step_ok(old, 1, Some("u"));
            twin.answer_both(None);
            twin.answer_both(None);
            twin.step_ok(old, 2, None);
            twin.answer_both(None);
            twin.answer_both(None);
            twin.completed(old);
            twin.answer_both(None);
            assert_eq!(twin.stage(old), TransferStage::Mutating);
            twin.post_outbox(OutboxInput::TransportError { op_id: old, temporary: false });
            while twin.has_pending() { twin.answer_both(None); }
            assert_eq!(twin.open_operations(), 1, "the failed mutation is an open operation of Outbox");
        } else {
            twin.step_error(old, false);
            twin.answer_both(None);
        }
        assert_eq!(twin.stage(old), TransferStage::Failed);
        twin.post(accept_input(KEY, 91));
        assert!(twin.has_pending(), "the batch of the replacement waits in the Store");
        let row = twin.reply();
        let mut outs = vec![twin.post_outbox(OutboxInput::Discard { model: "Message".into(), row_id: "m1".into(), action: None, reply: row })];
        let (with_action, out) = twin.discard(KEY);
        outs.push(out);
        let (cancel, out) = twin.cancel(KEY);
        outs.push(out);
        let refused_batch = refused.is_some();
        for (name, reply) in [("discard without action", row), ("discard", with_action), ("cancelUpload", cancel)] {
            assert!(outs.iter().all(|out| out.replies(reply).is_empty()) && twin.outbox_replies().iter().all(|(found, _)| *found != reply), "{name} waits in the interval");
        }
        outs.push(twin.answer_both(refused));
        while twin.has_pending() { outs.push(twin.answer_both(None)); }
        let outbox_replies = twin.outbox_replies();
        for (name, reply) in [("discard without action", row), ("discard", with_action), ("cancelUpload", cancel)] {
            let answers: Vec<_> = outs.iter().flat_map(|out| out.replies(reply)).collect();
            let from_outbox: Vec<_> = outbox_replies.iter().filter(|(found, _)| *found == reply).collect();
            assert!(from_outbox.is_empty(), "{name}, mutated {mutated}: Transfer executes the command, Outbox does not answer it: {from_outbox:?}");
            assert_eq!(answers.len(), 1, "{name}, mutated {mutated}, batch refused {refused_batch}: {answers:?}");
            let expected = if reply == cancel { TransferAnswer::Cancelled(true) } else { TransferAnswer::Ok };
            assert_eq!(answers, vec![expected], "{name}: answered once, not InFlight");
        }
        assert_eq!(twin.state().open_id(KEY), None, "batch refused {refused_batch}");
        assert!(twin.state().transfers.values().filter(|transfer| transfer.key == KEY).all(|transfer| transfer.stage == TransferStage::Done), "no open transfer of the key");
        assert_eq!(twin.open_operations(), 0, "no operation of the key");
    }
}
