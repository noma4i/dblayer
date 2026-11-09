//! Owner-level tests of the owner `Transfer`: `PersistingBatchIntent` (end of the session, stale basis, the close, the repeat of the write, ignored inputs) and mixed outcomes of a batch.

use super::transfer_harness::*;
use crate::owners::contract::{Online, RejectReason, ReplyId};
use crate::owners::write::outbox::OutboxOutcome;
use crate::owners::write::transfer::{TransferAnswer, TransferId, TransferInput, TransferStage, TransferTick};
use crate::runtime::Time;

const KEY: &str = "Moment:r1:upload";
const OTHER: &str = "Moment:r2:upload";

fn persisting_joint() -> (Twin, Vec<TransferId>) {
    let mut twin = Twin::with_slots(4);
    let ids = twin.accepted_batch(&[KEY, OTHER], 1);
    for _ in &ids {
        twin.commit();
    }
    for id in &ids {
        twin.step_ok(*id, 1, None);
        twin.commit();
        twin.commit();
        twin.completed(*id);
        twin.commit();
    }
    assert!(ids.iter().all(|id| twin.stage(*id) == TransferStage::PersistingBatchIntent));
    (twin, ids)
}

fn waiting_outcomes(twin: &mut Twin, ids: &[TransferId]) -> Vec<ReplyId> { ids.iter().map(|id| twin.outcome(*id, 1).0).collect() }

#[test]
fn xfer_batch_intent_ended() {
    for reason in [RejectReason::NoSession, RejectReason::Closed] {
        let (mut twin, ids) = persisting_joint();
        let replies = waiting_outcomes(&mut twin, &ids);
        let cause = if reason == RejectReason::Closed { crate::ffi::EndCause::Closed } else { crate::ffi::EndCause::SessionEnded };
        let out = twin.reject(reason);
        assert!(ids.iter().all(|id| twin.stage(*id) == TransferStage::Done));
        for reply in replies {
            assert_eq!(out.replies(reply), vec![TransferAnswer::Outcome(OutboxOutcome::Ended(cause.clone()), None)]);
        }
        assert_eq!((out.submits(), out.writes(), out.steps().len(), out.completes()), (0, 0, 0, 0));
    }
}

#[test]
fn xfer_batch_intent_stale() {
    let (mut twin, ids) = persisting_joint();
    let out = twin.reject(RejectReason::StaleGeneration { models: vec!["Moment".into()], generation: 7 });
    assert!(ids.iter().all(|id| twin.stage(*id) == TransferStage::PersistingBatchIntent));
    assert_eq!((out.writes(), out.submits()), (1, 0));
    assert_eq!(out.batches[0].basis.get("Moment"), Some(&7));
    assert_eq!(out.batches[0].intents, 1);
    assert_eq!(twin.commit().submit_ops().len(), 1);
}

#[test]
fn xfer_batch_intent_close() {
    for closing in [false, true] {
        let (mut twin, ids) = persisting_joint();
        let replies = waiting_outcomes(&mut twin, &ids);
        let cause = if closing { crate::ffi::EndCause::Closed } else { crate::ffi::EndCause::SessionEnded };
        let out = if closing { twin.close() } else { twin.end_epoch() };
        assert!(ids.iter().all(|id| twin.stage(*id) == TransferStage::Done));
        for reply in replies {
            assert_eq!(out.replies(reply), vec![TransferAnswer::Outcome(OutboxOutcome::Ended(cause.clone()), None)]);
        }
        assert_eq!(out.drained(), usize::from(closing));
        assert_eq!((out.submits(), out.removed().len(), out.deletes()), (0, 0, 0), "the stored group stays for the next open");
    }
}

#[test]
fn xfer_batch_intent_retry_storage() {
    let (mut twin, ids) = persisting_joint();
    let rejected = twin.reject(RejectReason::Storage("disk".into()));
    assert_eq!(rejected.timers, 1);
    let attempt_id = twin.attempt(ids[0]) + 1000;
    let stale = twin.post(TransferInput::Tick { now: Time(1), kind: TransferTick::Retry { transfer_id: ids[0], attempt_id } });
    assert!(Twin::quiet(&stale), "{stale:?}");
    let out = twin.tick_retry(ids[0]);
    assert_eq!((out.writes(), out.submits()), (1, 0));
    assert_eq!(out.batches[0].intents, 1);
    assert!(ids.iter().all(|id| twin.stage(*id) == TransferStage::PersistingBatchIntent));
    assert_eq!(twin.commit().submit_ops().len(), 1);
}

#[test]
fn xfer_batch_intent_other() {
    let (mut twin, ids) = persisting_joint();
    let mut inputs = twin.stray_results(ids[0]);
    inputs.push(TransferInput::MutationOutcome { transfer_id: ids[0], outcome: OutboxOutcome::Confirmed { revision: 1, result: None, row_id: None } });
    twin.ignores(ids[0], inputs);
    let out = twin.online(false);
    assert!(Twin::quiet(&out), "{out:?}");
    assert_eq!(twin.state().online, Some(false));
    twin.post(TransferInput::PowerSaving(true));
    assert!(twin.state().saving);
    let out = twin.post(TransferInput::Online(Online(true)));
    assert!(out.steps().is_empty() && out.completes() == 0 && out.writes() == 0, "{out:?}");
    assert!(ids.iter().all(|id| twin.stage(*id) == TransferStage::PersistingBatchIntent));
}

#[test]
fn xfer_batch_mixed_outcomes() {
    let (mut twin, ids) = persisting_joint();
    twin.commit();
    assert!(ids.iter().all(|id| twin.stage(*id) == TransferStage::Mutating));
    let outcomes = [(ids[0], OutboxOutcome::Confirmed { revision: 5, result: None, row_id: None }), (ids[1], OutboxOutcome::Failed { error: "boom".into() })].into();
    let out = twin.post(TransferInput::MutationOutcomes { batch_id: 1, outcomes, intent_removed: false });
    assert_eq!((twin.stage(ids[0]), twin.stage(ids[1])), (TransferStage::Removing, TransferStage::PersistingFailure));
    assert_eq!(out.writes(), 1, "one transaction");
    assert_eq!(out.removed(), vec![KEY.to_string()]);
    assert_eq!(out.rows().len(), 1);
    assert!(out.shows().is_empty(), "nothing is seen before Committed");
    let out = twin.commit();
    assert_eq!((twin.stage(ids[0]), twin.stage(ids[1])), (TransferStage::Done, TransferStage::Failed));
    assert!(!out.shows().is_empty());
    let (reply, out) = twin.outcome(ids[0], 1);
    assert_eq!(out.replies(reply), vec![TransferAnswer::Outcome(OutboxOutcome::Confirmed { revision: 5, result: None, row_id: None }, None)]);
    let (reply, out) = twin.outcome(ids[1], 1);
    assert_eq!(out.replies(reply), vec![TransferAnswer::Outcome(OutboxOutcome::Failed { error: "boom".into() }, None)]);
}
