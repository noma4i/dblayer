//! Owner-level tests of the owner `Transfer`: closing, stray results, rejected writes and orphan aborts.
use super::transfer_harness::Twin;

const KEY: &str = "Moment:r1:upload";

fn ended(cause: crate::ffi::EndCause) -> crate::owners::write::transfer::TransferAnswer { crate::owners::write::transfer::TransferAnswer::Err(crate::owners::write::outbox::Fault::Ended(cause)) }

#[test]
fn xfer_accepting_closing() {
    use crate::owners::write::transfer::TransferStage;
    for committed in [true, false] {
        let mut twin = Twin::new();
        let (id, reply, _) = twin.accepting(KEY, 2);
        let closing = twin.close();
        assert_eq!((twin.stage(id), closing.all_replies(), closing.drained(), closing.writes()), (TransferStage::Accepting, 0, 0, 0));
        let out = if committed { twin.commit() } else { twin.reject(crate::owners::contract::RejectReason::Storage("disk".into())) };
        assert_eq!((twin.stage(id), out.replies(reply), out.drained(), out.copies()), (TransferStage::Done, vec![ended(crate::ffi::EndCause::Closed)], 1, 0));
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
    for reason in [RejectReason::NoSession, RejectReason::Closed] {
        let mut twin = Twin::new();
        let (id, reply, _) = twin.accepting(KEY, 2);
        let out = twin.reject(reason.clone());
        let cause = if reason == RejectReason::Closed { crate::ffi::EndCause::Closed } else { crate::ffi::EndCause::SessionEnded };
        assert_eq!((twin.stage(id), out.replies(reply), out.copies()), (TransferStage::Done, vec![ended(cause.clone())], 0));

        let mut twin = Twin::new();
        let keys = [KEY, "Moment:r2:upload"];
        twin.post(TransferInput::AcceptBatch { epoch: EPOCH, items: keys.iter().map(|key| item(key, 2)).collect(), joint: Box::new(joint()), reply: 50 });
        let ids: Vec<_> = keys.iter().map(|key| twin.state().open_id(key).unwrap()).collect();
        twin.reserve_batch(&ids);
        let out = twin.reject(reason);
        assert_eq!((out.replies(50), out.all_replies(), out.copies()), (vec![ended(cause)], 1, 0));
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
        assert_eq!((out.replies(30), out.all_replies(), out.writes()), (vec![TransferAnswer::Err(Fault::Ended(crate::ffi::EndCause::Closed))], 1, 0));
        assert!(out.outbox.is_empty() && twin.state().transfers.is_empty());
    }
}

#[test]
fn xfer_orphan_abort_refused_keeps_debt() {
    use super::transfer_harness::EPOCH;
    use crate::owners::write::transfer::TransferInput;
    for (status, temporary) in [(Some(403u16), false), (Some(503u16), true), (None, true)] {
        let mut twin = Twin::bare(1);
        twin.post(TransferInput::RegisterOperations { declarations: vec![("Moment".into(), "upload".into())], complete: false });
        twin.post(TransferInput::Restored { uploads: Vec::new(), outcomes: Default::default(), orphans: vec![("u9".into(), KEY.into())], batches: Default::default(), held_orphan: None });
        twin.post(TransferInput::RestoreReady { epoch: EPOCH });
        assert_eq!(twin.online(true).orphan_aborts(), vec!["u9".to_string()]);
        let out = twin.post(TransferInput::AbortDone { upload_id: "u9".into(), result: Err(crate::owners::write::transfer::AbortFault { temporary, status }) });
        assert!(Twin::quiet(&out), "{status:?}: {out:?}");
        twin.online(false);
        assert_eq!(twin.online(true).orphan_aborts(), vec!["u9".to_string()], "{status:?}: the debt is kept and resent");
    }
}
