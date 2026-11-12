//! Owner-level tests of the owner `Transfer`: closing, stray results, rejected writes, orphan aborts and the discard of a failed transfer.
use super::transfer_harness::{Out, Twin};
use crate::owners::write::outbox::OutboxOutcome;
use crate::owners::write::transfer::AbortFault;

const KEY: &str = "Moment:r1:upload";

#[test]
fn xfer_accepting_closing() {
    use crate::owners::write::transfer::TransferStage;
    for committed in [true, false] {
        let mut twin = Twin::new();
        let (id, reply, _) = twin.accepting(KEY, 2);
        let closing = twin.close();
        assert_eq!((twin.stage(id), closing.all_replies(), closing.drained(), closing.writes()), (TransferStage::Accepting, 0, 0, 0));
        let out = if committed { twin.commit() } else { twin.reject(crate::owners::contract::RejectReason::Storage("disk".into())) };
        assert_eq!((twin.stage(id), format!("{:?}", out.replies(reply)), out.drained(), out.copies()), (TransferStage::Done, "[Err(Ended(Closed))]".into(), 1, 0));
    }
}

#[test]
fn xfer_outcome_unknown() {
    use crate::owners::write::outbox::Fault;
    use crate::owners::write::transfer::{TransferAnswer, TransferInput};
    let mut twin = Twin::new();
    let receipt = crate::owners::write::outbox::Receipt { epoch: 1, op_id: 99, cycle_id: 1, revision: 0, optimistic: None };
    let out = twin.post(TransferInput::Outcome { receipt, reply: 20 , discard_on_failure: false});
    assert_eq!((out.replies(20), out.all_replies(), out.writes(), out.runs.len()), (vec![TransferAnswer::Err(Fault::NotFound)], 1, 0, 1));
    assert!(twin.state().transfers.is_empty());
}

#[test]
fn xfer_completing_stray() {
    use crate::owners::write::transfer::{TransferInput, TransferStage, TransferTick};
    use crate::runtime::Time;
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 2);
    twin.step_ok(id, 1, Some("u1"));
    twin.commit();
    twin.commit();
    twin.step_ok(id, 2, None);
    twin.commit();
    twin.commit();
    assert_eq!(twin.stage(id), TransferStage::Completing);
    let attempt_id = twin.attempt(id) + 1;
    twin.det.tick(Time(1));
    twin.ignores(id, vec![
        TransferInput::Completed { transfer_id: id, attempt_id, signed_id: Some("signed".into()) },
        TransferInput::CompleteError { transfer_id: id, attempt_id, temporary: true, error: "error".into() },
        TransferInput::Tick { kind: TransferTick::Retry { transfer_id: id, attempt_id } },
    ]);
}

#[test]
fn xfer_accepting_rejected_interrupted() {
    use super::transfer_harness::{item, joint, EPOCH};
    use crate::owners::contract::RejectReason;
    use crate::owners::write::transfer::{TransferInput, TransferStage};
    let mut stale = Twin::new();
    let (id, reply, _) = stale.accepting(KEY, 2);
    let out = stale.reject(RejectReason::StaleGeneration { models: vec!["Moment".into()], generations: [("Moment".into(), 2)].into() });
    assert_eq!(stale.stage(id), TransferStage::Done);
    assert!(matches!(out.replies(reply).as_slice(), [crate::owners::write::transfer::TransferAnswer::Err(crate::owners::write::outbox::Fault::Ended(crate::ffi::EndCause::SessionEnded))]));
    assert_eq!(out.copies(), 0);
    for reason in [RejectReason::NoSession, RejectReason::Closed] {
        let mut twin = Twin::new();
        let (id, reply, _) = twin.accepting(KEY, 2);
        let out = twin.reject(reason.clone());
        let cause = if reason == RejectReason::Closed { "Closed" } else { "SessionEnded" };
        assert_eq!((twin.stage(id), format!("{:?}", out.replies(reply)), out.copies()), (TransferStage::Done, format!("[Err({cause})]"), 0));

        let mut twin = Twin::new();
        let keys = [KEY, "Moment:r2:upload"];
        twin.post(TransferInput::AcceptBatch { epoch: EPOCH, items: keys.iter().map(|key| item(key, 2)).collect(), joint: Box::new(joint()), reply: 50 });
        let ids: Vec<_> = keys.iter().map(|key| twin.state().open_id(key).unwrap()).collect();
        twin.reserve_batch(&ids);
        let out = twin.reject(reason);
        assert_eq!((format!("{:?}", out.replies(50)), out.all_replies(), out.copies()), (format!("[Err({cause})]"), 1, 0));
        assert!(ids.iter().all(|id| twin.stage(*id) == TransferStage::Done));
    }
}

#[test]
fn xfer_awaiting_source_expires() {
    use super::transfer_harness::{item, DAY, EPOCH};
    use crate::owners::contract::Online;
    use crate::owners::write::outbox::ReserveTarget;
    use crate::owners::write::transfer::{TransferInput, TransferStage, TransferTick};
    use crate::runtime::Time;
    let mut twin = Twin::offline();
    let mut pending = item(KEY, 1);
    pending.source = None;
    pending.steps = None;
    twin.post(TransferInput::Accept { epoch: EPOCH, item: Box::new(pending), reply: 30 });
    let id = twin.state().open_id(KEY).unwrap();
    twin.post(TransferInput::OpReserved { target: ReserveTarget::Transfer(id), result: Ok(id) });
    twin.commit();
    assert_eq!(twin.stage(id), TransferStage::AwaitingSource);
    twin.det.tick(Time(60 * DAY));
    for input in [
        TransferInput::Online(Online(true)),
        TransferInput::RestoreReady { epoch: EPOCH },
        TransferInput::Tick { kind: TransferTick::Expire },
    ] {
        let out = twin.post(input);
        assert_eq!(twin.stage(id), TransferStage::AwaitingSource);
        assert!(Twin::quiet(&out), "{out:?}");
    }
}

#[test]
fn xfer_accept_after_closing() {
    use super::transfer_harness::{item, joint, EPOCH};
    use crate::owners::write::outbox::Fault;
    use crate::owners::write::transfer::{TransferAnswer, TransferInput};
    let mut twin = Twin::new();
    twin.close();
    for input in [
        TransferInput::Accept { epoch: EPOCH, item: Box::new(item(KEY, 1)), reply: 30 },
        TransferInput::AcceptBatch { epoch: EPOCH, items: vec![item(KEY, 1), item("Moment:r2:upload", 1)], joint: Box::new(joint()), reply: 30 },
    ] {
        let out = twin.post(input);
        assert_eq!((out.replies(30), out.all_replies(), out.writes()), (vec![TransferAnswer::Err(Fault::Closed)], 1, 0));
        assert!(out.outbox.is_empty() && twin.state().transfers.is_empty());
    }
}

#[test]
fn xfer_orphan_abort_refused_keeps_debt() {
    use super::transfer_harness::EPOCH;
    use crate::owners::write::transfer::TransferInput;
    for (status, temporary) in [(Some(403u16), false), (Some(503u16), true), (None, true)] {
        let mut twin = Twin::bare(1);
        twin.post(TransferInput::RegisterOperations { declarations: vec![("Moment".into(), "upload".into())] });
        twin.post(TransferInput::Restored { uploads: Vec::new(), outcomes: Default::default(), orphans: vec![("u9".into(), KEY.into())], batches: Default::default(), held_orphan: None });
        twin.post(TransferInput::RestoreReady { epoch: EPOCH });
        assert_eq!(twin.online(true).orphan_aborts(), vec!["u9".to_string()]);
        let out = twin.post(TransferInput::AbortDone { upload_id: "u9".into(), result: Err(crate::owners::write::transfer::AbortFault { temporary, status }) });
        assert!(Twin::quiet(&out), "{status:?}: {out:?}");
        twin.online(false);
        assert_eq!(twin.online(true).orphan_aborts(), vec!["u9".to_string()], "{status:?}: the debt is kept and resent");
    }
}

fn failed_transfer_mutation(twin: &mut Twin) -> u64 {
    let id = twin.mutating(KEY);
    twin.mutation_outcome(KEY, OutboxOutcome::Failed { error: "error".into() });
    twin.commit();
    assert_eq!(twin.stage(id), crate::owners::write::transfer::TransferStage::Failed);
    id
}

#[test]
fn xfer_failed_discard_outcome() {
    use crate::owners::write::outbox::Fault;
    use crate::owners::write::transfer::{TransferAnswer, TransferInput, TransferStage};
    for accepted in [false, true] {
        let mut twin = Twin::new();
        let id = failed_transfer_mutation(&mut twin);
        let (reply, _) = twin.discard(KEY);
        let result = if accepted { Ok(()) } else { Err(Fault::InFlight) };
        let out = twin.post(TransferInput::DiscardOutcome { key: KEY.into(), result });
        assert_eq!(twin.stage(id), TransferStage::Failed);
        if accepted { assert!(Twin::quiet(&out)); }
        else { assert_eq!((out.replies(reply), out.all_replies(), out.writes()), (vec![TransferAnswer::Err(Fault::InFlight)], 1, 0)); }
    }
}

#[test]
fn xfer_failed_discarded() {
    use crate::owners::write::transfer::{TransferAnswer, TransferStage};
    for cancel in [false, true] {
        let mut twin = Twin::new();
        let id = failed_transfer_mutation(&mut twin);
        let (reply, _) = if cancel { twin.cancel(KEY) } else { twin.discard(KEY) };
        let out = twin.mutation_outcome(KEY, OutboxOutcome::Discarded);
        assert_eq!((twin.stage(id), out.removed(), out.rolled_back(), out.all_replies()), (TransferStage::Removing, vec![KEY.to_string()], Vec::<u64>::new(), 0));
        let out = twin.commit();
        let answer = if cancel { TransferAnswer::Cancelled(true) } else { TransferAnswer::Ok };
        assert_eq!((twin.stage(id), out.replies(reply)), (TransferStage::Done, vec![answer]));
    }
}

#[test]
fn xfer_failed_confirmed_before_discard() {
    use crate::owners::write::outbox::Fault;
    use crate::owners::write::transfer::{TransferAnswer, TransferStage};
    for cancel in [false, true] {
        let mut twin = Twin::new();
        let id = failed_transfer_mutation(&mut twin);
        let (reply, _) = if cancel { twin.cancel(KEY) } else { twin.discard(KEY) };
        let out = twin.mutation_outcome(KEY, OutboxOutcome::Confirmed { revision: 7, result: None, row_id: None });
        let answer = if cancel { TransferAnswer::Cancelled(false) } else { TransferAnswer::Err(Fault::NotFound) };
        assert_eq!((twin.stage(id), out.replies(reply), out.removed()), (TransferStage::Removing, vec![answer], vec![KEY.to_string()]));
    }
}

#[test]
fn xfer_failed_outcome_failed_waiting() {
    use crate::owners::write::transfer::TransferStage;
    for outcome in [OutboxOutcome::Failed { error: "error".into() }, OutboxOutcome::Rejected { error: "error".into() }, OutboxOutcome::Ended(crate::owners::read::deliveries::EndCause::SessionEnded)] {
        let mut twin = Twin::new();
        let id = failed_transfer_mutation(&mut twin);
        twin.discard(KEY);
        let out = twin.mutation_outcome(KEY, outcome);
        assert_eq!(twin.stage(id), TransferStage::Failed);
        assert!(Twin::quiet(&out), "{out:?}");
    }
}

#[test]
fn xfer_accept_rowless() {
    use crate::owners::write::transfer::{TransferInput, TransferStage};
    for pending in [false, true] {
        let mut owner = Twin::new();
        let mut item = super::transfer_harness::item(KEY, 1);
        item.mutation.effects = Default::default();
        if pending { item.source = None; item.steps = None; }
        let out = owner.post(TransferInput::Accept { epoch: 1, item: Box::new(item), reply: 7 });
        let id = owner.state().open_id(KEY).expect("rowless upload is accepted");
        assert_eq!(owner.stage(id), TransferStage::Reserving);
        assert_eq!((out.outbox.len(), out.writes(), out.replies(7).len()), (1, 0, 0));
    }
}

#[test]
fn xfer_rowless_accept_batch() {
    use crate::owners::write::transfer::{TransferInput, TransferStage};
    let keys = ["Moment:r1:upload", "Moment:r2:upload"];
    let mut owner = Twin::new();
    let items = keys.iter().map(|name| {
        let mut item = super::transfer_harness::item(name, 1);
        item.mutation.effects = Default::default();
        item
    }).collect();
    let out = owner.post(TransferInput::AcceptBatch { epoch: 1, joint: Box::new(super::transfer_harness::joint()), items, reply: 7 });
    assert_eq!(out.outbox.len(), 3);
    for name in keys {
        let id = owner.state().open_id(name).expect("rowless batch item is accepted");
        assert_eq!(owner.stage(id), TransferStage::Reserving);
    }
}
