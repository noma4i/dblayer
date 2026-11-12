//! Owner-level tests of the owner `Transfer`: `Failed`, `retry`, `Removing`, the expiry, the outcomes of the receipts, the streams of status, the end of the epoch and the close.

use super::transfer_harness::*;
use crate::owners::contract::{Online, RejectReason};
use crate::owners::store::UploadKind;
use crate::owners::write::outbox::{Fault, OutboxOutcome};
use crate::owners::write::transfer::{NextStep, Shown, Source, TransferAnswer, TransferId, TransferInput, TransferStage, TransferTick};
use crate::runtime::{Cause, Time};

const KEY: &str = "Moment:r1:upload";
const OTHER: &str = "Moment:r2:upload";

pub(super) const SILENT: (usize, usize, usize, usize, usize, usize) = (0, 0, 0, 0, 0, 0);

/// The writes to the Outbox, the submits, the discards, `abort`, `complete` and the steps of one step of the owner.
pub(super) fn server_silence(out: &Out) -> (usize, usize, usize, usize, usize, usize) { (out.outbox.len(), out.submits(), out.outbox_discards(), out.aborts(), out.completes(), out.steps().len()) }

fn failed_outcome(error: &str) -> TransferAnswer { TransferAnswer::Outcome(OutboxOutcome::Failed { error: error.into() }, None) }

/// `Failed{missing_file}`: the row came back from the base without its file.
fn missing_file() -> (Twin, TransferId) {
    let mut row = stored(1, KEY, 2);
    row.file_present = false;
    let (mut twin, _) = Twin::opened(vec![row], Default::default());
    twin.commit();
    assert_eq!(twin.stage(1), TransferStage::Failed);
    (twin, 1)
}

fn failed_with_upload() -> (Twin, TransferId) {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    twin.step_ok(id, 1, Some("u1"));
    let failure = twin.reject(RejectReason::Invalid("bad row".into()));
    assert!(!failure.batches.iter().flat_map(|batch| &batch.ops).any(|op| matches!(op, crate::owners::store::StoreOp::UploadShown { shown, .. } if !matches!(shown, Shown::Failed { .. }))), "the failure batch must not restore the previous status");
    twin.commit();
    assert_eq!((twin.stage(id), twin.t(id).aborted), (TransferStage::Failed, false));
    (twin, id)
}

#[test]
fn xfer_retry_new_source() {
    let (mut twin, id) = missing_file();
    let fresh = Source { path: "/new/video".into(), parts: 2 };
    let (reply, out) = twin.retry(KEY, Some(fresh.clone()));
    assert_eq!((twin.stage(id), out.writes(), out.copies(), out.all_replies()), (TransferStage::Copying, 0, 1, 0));
    twin.copy_done(id);
    let out = twin.commit();
    assert!(matches!(out.replies(reply).as_slice(), [TransferAnswer::Receipt(receipt)] if receipt.cycle_id == 2 && receipt.op_id == id), "{:?}", out.replies(reply));
}

#[test]
fn xfer_retry_needs_source() {
    let (mut twin, id) = missing_file();
    let (reply, out) = twin.retry(KEY, None);
    assert_eq!((Twin::fault(&out, reply), twin.stage(id), out.writes()), (Some(Fault::Invalid), TransferStage::Failed, 0));
}

#[test]
fn xfer_retry_complete() {
    let mut twin = Twin::new();
    let id = twin.completing(KEY);
    twin.complete_error(id, false);
    twin.commit();
    let (reply, out) = twin.retry(KEY, None);
    assert_eq!((twin.stage(id), twin.next_step(id), out.kinds(), out.all_replies()), (TransferStage::PersistingRetry, NextStep::Complete, vec![UploadKind::Retrying], 0));
    let out = twin.commit();
    assert_eq!((out.completes(), out.steps().len()), (0, 0));
    assert!(matches!(out.replies(reply).as_slice(), [TransferAnswer::Receipt(receipt)] if receipt.cycle_id == 2));
    assert_eq!(twin.commit().completes(), 1);
}

#[test]
fn xfer_retry_after_abort() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    twin.step_ok(id, 1, Some("u1"));
    twin.commit();
    twin.commit();
    twin.step_error(id, false);
    twin.commit();
    let (_, out) = twin.retry(KEY, None);
    let row = out.rows()[0].0.clone();
    assert_eq!((twin.next_step(id), row.confirmed_parts, row.upload_id, row.etags.len()), (NextStep::Part(1), 0, None, 0), "a new multipart upload starts from the first part");
    let out = twin.commit();
    assert!(out.steps().is_empty());
    assert_eq!(twin.commit().steps()[0].0, Some(1));
}

#[test]
fn xfer_retry() {
    let (mut twin, id) = failed_with_upload();
    let previous = twin.t(id).outcomes.get(&1).cloned().expect("failed cycle has an outcome");
    let (_, out) = twin.retry(KEY, None);
    assert_eq!((twin.stage(id), twin.next_step(id), out.rows()[0].0.confirmed_parts), (TransferStage::PersistingRetry, NextStep::Part(2), 1), "the first part that is not confirmed");
    assert_eq!(out.rows()[0].0.attempts, 0);
    assert_eq!(out.outcomes_written(), vec![(id, 1, previous)]);
    assert_eq!(out.kinds(), vec![UploadKind::Retrying]);
    assert!(!out.batches.iter().flat_map(|batch| &batch.ops).any(|op| matches!(op, crate::owners::store::StoreOp::UploadShown { shown: Shown::Failed { .. }, .. })), "the retry batch must not restore the previous failed status");
    let out = twin.commit();
    assert!(out.steps().is_empty());
    assert_eq!(out.shows(), vec![Shown::Queued]);
    assert!(!out.stored_shows().iter().any(|shown| matches!(shown, Shown::Failed { .. })), "the committed retry must not schedule the old failed status");
    assert_eq!(twin.commit().steps()[0].0, Some(2));
}

#[test]
fn xfer_persisting_retry_committed() {
    let (mut twin, id) = failed_with_upload();
    let (reply, _) = twin.retry(KEY, None);
    let out = twin.commit();
    assert!(matches!(out.replies(reply).as_slice(), [TransferAnswer::Receipt(receipt)] if receipt.cycle_id == 2), "the new cycle starts only after the write");
    assert_eq!((out.round_needed(), twin.t(id).cycle_id), (vec![vec![id]], 2));
    assert_eq!(out.shows().first(), Some(&Shown::Queued));
}

#[test]
fn xfer_persisting_retry_rejected() {
    let (mut twin, id) = failed_with_upload();
    let (reply, _) = twin.retry(KEY, None);
    let out = twin.reject(RejectReason::Storage("disk".into()));
    assert_eq!(Twin::fault(&out, reply), Some(Fault::Rejected(RejectReason::Storage("disk".into()))));
    assert_eq!((twin.stage(id), twin.t(id).cycle_id, out.shows().len()), (TransferStage::Failed, 1, 0));
}

#[test]
fn xfer_persisting_failure_committed() {
    let mut twin = Twin::new();
    let id = twin.persisting_failure(KEY);
    let (waiting, out) = twin.outcome(id, 1);
    assert_eq!(out.all_replies(), 0, "the outcome of the cycle waits for the terminal cell");
    let out = twin.commit();
    assert_eq!((twin.stage(id), out.shows(), out.round_idle()), (TransferStage::Failed, vec![Shown::Failed { error: super::transfer_harness::refusal() }], vec![id]));
    assert_eq!(out.replies(waiting), vec![failed_outcome(&super::transfer_harness::refusal())]);
}

#[test]
fn xfer_persisting_failure_storage() {
    let mut twin = Twin::new();
    let id = twin.persisting_failure(KEY);
    let out = twin.reject(RejectReason::Storage("disk".into()));
    assert_eq!((twin.stage(id), out.timers, out.shows().len()), (TransferStage::PersistingFailure, 1, 0), "the mark is written again after a pause");
}

/// P-DATA-102: every Storage refusal in a row of the writes of the same transfer makes the pause before the next write longer, on each path that repeats a write.
#[test]
fn xfer_storage_retry_pause_grows() {
    use crate::owners::write::transfer::TransferTick;
    type Path = (&'static str, fn(&mut Twin) -> (u64, Option<u64>), fn(&mut Twin, u64) -> Out, fn(u64) -> TransferInput);
    let failure: Path = ("the failure mark", |twin| (twin.persisting_failure(KEY), None), |twin, _| twin.reject(RejectReason::Storage("disk".into())), |id| TransferInput::Tick { kind: TransferTick::Retry { transfer_id: id, attempt_id: 0 } });
    let restore: Path = ("the old row after a failed swap", |twin| {
        let id = twin.failed(KEY);
        twin.retry(KEY, Some(Source { path: "/fresh".into(), parts: 1 }));
        twin.copy_done(id);
        twin.commit();
        twin.post(TransferInput::StagedPromoteFailed { transfer_id: id, detail: "rename".into() });
        (id, None)
    }, |twin, _| twin.reject(RejectReason::Storage("disk".into())), |id| TransferInput::Tick { kind: TransferTick::StagedRestore { transfer_id: id } });
    let shown: Path = ("the shown value", |twin| {
        let id = twin.uploading(KEY, 3);
        twin.hold_shown = true;
        let out = twin.online(false);
        (id, Some(out.shown_batches()[0]))
    }, |twin, batch| twin.reject_shown(batch), |id| TransferInput::Tick { kind: TransferTick::Shown { transfer_id: id } });
    let attempt: Path = ("the mark of the attempt", |twin| (twin.accepted(KEY, 2), None), |twin, _| twin.reject(RejectReason::Storage("disk".into())), |id| TransferInput::Tick { kind: TransferTick::Retry { transfer_id: id, attempt_id: 0 } });
    let part: Path = ("the confirmed part", |twin| {
        let id = twin.uploading(KEY, 3);
        twin.step_ok(id, 1, Some("u1"));
        (id, None)
    }, |twin, _| twin.reject(RejectReason::Storage("disk".into())), |id| TransferInput::Tick { kind: TransferTick::Retry { transfer_id: id, attempt_id: 0 } });
    let complete: Path = ("completed", |twin| {
        let id = twin.completing(KEY);
        twin.completed(id);
        (id, None)
    }, |twin, _| twin.reject(RejectReason::Storage("disk".into())), |id| TransferInput::Tick { kind: TransferTick::Retry { transfer_id: id, attempt_id: 0 } });
    let removal: Path = ("the removal", |twin| {
        let id = twin.uploading(KEY, 2);
        twin.cancel(KEY);
        (id, None)
    }, |twin, _| twin.reject(RejectReason::Storage("disk".into())), |id| TransferInput::Tick { kind: TransferTick::Retry { transfer_id: id, attempt_id: 0 } });
    for (name, arrive, refuse, due) in [failure, restore, shown, attempt, part, complete, removal] {
        let mut twin = Twin::new();
        let (id, mut held) = arrive(&mut twin);
        let mut pauses = Vec::new();
        for _ in 0..3 {
            let refused = refuse(&mut twin, held.unwrap_or(0));
            assert_eq!(refused.schedules.len(), 1, "{name}: one timer per refusal: {refused:?}");
            pauses.push(refused.schedules[0].0);
            let input = match due(id) {
                TransferInput::Tick { kind: TransferTick::Retry { transfer_id, .. } } => TransferInput::Tick { kind: TransferTick::Retry { transfer_id, attempt_id: twin.attempt(transfer_id) } },
                other => other,
            };
            twin.det.tick(crate::runtime::Time(1));
            let again = twin.post(input);
            assert_eq!(again.batches.len() + again.shown.len(), 1, "{name}: the write goes again: {again:?}");
            held = again.shown_batches().first().copied();
        }
        assert!(pauses[0] < pauses[1] && pauses[1] < pauses[2], "{name}: 3 refusals, 3 growing pauses: {pauses:?}");
    }
}

#[test]
fn xfer_persisting_failure_tick() {
    let mut twin = Twin::new();
    let id = twin.persisting_failure(KEY);
    twin.reject(storage());
    let stray = twin.post(TransferInput::Tick { kind: TransferTick::Retry { transfer_id: id, attempt_id: twin.attempt(id) + 1000 } });
    assert!(Twin::quiet(&stray), "a timer of another attempt writes nothing: {stray:?}");
    let out = twin.tick_retry(id);
    assert_eq!((twin.stage(id), out.writes(), out.kinds()), (TransferStage::PersistingFailure, 1, vec![UploadKind::Failed]));
}

#[test]
fn xfer_discard() {
    let (mut twin, id) = failed_with_upload();
    let (reply, out) = twin.discard(KEY);
    assert_eq!((twin.stage(id), out.aborts(), out.orphans_written().len(), out.removed()), (TransferStage::Removing, 0, 1, vec![KEY.to_string()]));
    let committed = twin.commit();
    assert_eq!((committed.replies(reply), committed.orphan_aborts()), (vec![TransferAnswer::Ok], vec!["u1".to_string()]));
    let (mut other, id) = failed_with_upload();
    let (reply, _) = other.cancel(KEY);
    assert_eq!((other.commit().replies(reply), other.stage(id)), (vec![TransferAnswer::Cancelled(true)], TransferStage::Done));
}

#[test]
fn xfer_discard_no_upload() {
    let mut twin = Twin::new();
    let id = twin.failed(KEY);
    let error = twin.t(id).error.clone().unwrap();
    let (reply, out) = twin.discard(KEY);
    assert_eq!((twin.stage(id), out.aborts(), out.removed()), (TransferStage::Removing, 0, vec![KEY.to_string()]));
    assert_eq!(out.outcomes_written(), vec![(id, 1, OutboxOutcome::Failed { error })]);
    assert_eq!(twin.commit().replies(reply), vec![TransferAnswer::Ok]);
}

#[test]
fn xfer_removing_committed_cancel() {
    let mut twin = Twin::offline();
    let (id, accept_reply, _) = twin.accepting(KEY, 2);
    twin.commit();
    twin.copy_done(id);
    twin.commit();
    let (reply, _) = twin.cancel(KEY);
    let out = twin.commit();
    assert_eq!((out.replies(reply), twin.stage(id), out.deletes(), out.shows(), out.round_idle()), (vec![TransferAnswer::Cancelled(true)], TransferStage::Done, 1, vec![Shown::Gone], vec![id]));
    assert!(out.replies(accept_reply).is_empty(), "the call of `upload` was answered with the receipt long ago");
    let (reply, out) = twin.outcome(id, 1);
    assert_eq!(out.replies(reply), vec![TransferAnswer::Outcome(OutboxOutcome::Discarded, None)]);
}

#[test]
fn xfer_removing_committed_confirmed() {
    let mut twin = Twin::new();
    let id = twin.mutating(KEY);
    twin.mutation_outcome(KEY, OutboxOutcome::Confirmed { revision: 7, result: None, row_id: None });
    let out = twin.commit();
    assert_eq!((twin.stage(id), out.deletes(), out.round_idle()), (TransferStage::Done, 1, vec![id]), "the local copy goes after the write");
    let (reply, out) = twin.outcome(id, 1);
    assert_eq!(out.replies(reply), vec![TransferAnswer::Outcome(OutboxOutcome::Confirmed { revision: 7, result: None, row_id: None }, None)]);
}

#[test]
fn xfer_removing_committed_discarded() {
    let mut twin = Twin::new();
    let id = twin.mutating(KEY);
    let (first, _) = twin.discard(KEY);
    let (second, _) = twin.discard(KEY);
    twin.mutation_outcome(KEY, OutboxOutcome::Discarded);
    let out = twin.commit();
    assert_eq!((out.replies(first), out.replies(second), twin.stage(id)), (vec![TransferAnswer::Ok], vec![TransferAnswer::Ok], TransferStage::Done));
}

#[test]
fn xfer_removing_committed_copy_failed() {
    let mut twin = Twin::new();
    let (id, reply, _) = twin.accepting(KEY, 2);
    twin.commit();
    twin.copy_failed(id);
    let out = twin.commit();
    assert_eq!((Twin::fault(&out, reply), twin.stage(id), out.deletes()), (Some(Fault::Copy("unreadable".into())), TransferStage::Done, 1), "the partial file goes");
    assert!(!twin.state().completed.contains_key(&id), "a transfer that was never accepted has no outcome");
}

type Removal = (&'static str, fn() -> (Twin, TransferId, Option<u64>, Out), fn(&mut Twin, TransferId, Option<u64>, &Out));

fn removals() -> Vec<Removal> {
    let cancel: Removal = ("Cancel", || {
        let mut twin = Twin::offline();
        let id = twin.accepted(KEY, 2);
        let (reply, first) = twin.cancel(KEY);
        (twin, id, Some(reply), first)
    }, |_, _, reply, out| assert_eq!(out.replies(reply.unwrap()), vec![TransferAnswer::Cancelled(true)]));
    let expiry: Removal = ("Cancel without a caller (expiry)", || {
        let mut twin = Twin::new();
        let id = twin.failed(KEY);
        twin.det.tick(Time(8 * DAY));
        let first = twin.post(TransferInput::Tick { kind: TransferTick::Expire });
        (twin, id, None, first)
    }, |twin, id, _, _| {
        let (reply, out) = twin.outcome(id, 1);
        assert_eq!(out.replies(reply), vec![failed_outcome(&refusal())], "the outcome of the failed cycle stays");
    });
    let confirmed: Removal = ("Confirmed", || {
        let mut twin = Twin::new();
        let id = twin.mutating(KEY);
        let first = twin.mutation_outcome(KEY, OutboxOutcome::Confirmed { revision: 7, result: None, row_id: None });
        (twin, id, None, first)
    }, |twin, id, _, _| {
        let (reply, out) = twin.outcome(id, 1);
        assert_eq!(out.replies(reply), vec![TransferAnswer::Outcome(OutboxOutcome::Confirmed { revision: 7, result: None, row_id: None }, None)], "the outcome of the server stays");
    });
    let discarded: Removal = ("Discarded", || {
        let mut twin = Twin::new();
        let id = twin.mutating(KEY);
        let (reply, _) = twin.discard(KEY);
        let first = twin.mutation_outcome(KEY, OutboxOutcome::Discarded);
        (twin, id, Some(reply), first)
    }, |_, _, reply, out| assert_eq!(out.replies(reply.unwrap()), vec![TransferAnswer::Ok]));
    let copy_failed: Removal = ("CopyFailed", || {
        let mut twin = Twin::new();
        let (id, reply, _) = twin.accepting(KEY, 2);
        twin.commit();
        let first = twin.copy_failed(id);
        (twin, id, Some(reply), first)
    }, |_, _, reply, out| assert_eq!(Twin::fault(out, reply.unwrap()), Some(Fault::Copy("unreadable".into()))));
    let by_storage: Removal = ("Storage", || {
        let mut twin = Twin::new();
        let (id, reply, _) = twin.accepting(KEY, 2);
        twin.commit();
        twin.copy_done(id);
        let first = twin.reject(storage());
        (twin, id, Some(reply), first)
    }, |_, _, reply, out| assert!(out.replies(reply.unwrap()).is_empty(), "the call was answered by the step of the refusal"));
    let by_invalid: Removal = ("Invalid", || {
        let mut twin = Twin::new();
        let (id, reply, _) = twin.accepting(KEY, 2);
        twin.commit();
        twin.copy_done(id);
        let first = twin.reject(RejectReason::Invalid("bad row".into()));
        (twin, id, Some(reply), first)
    }, |_, _, reply, out| assert_eq!(Twin::fault(out, reply.unwrap()), Some(Fault::Rejected(RejectReason::Invalid("bad row".into())))));
    vec![cancel, expiry, confirmed, discarded, copy_failed, by_storage, by_invalid]
}

#[test]
fn xfer_removing_storage_retried() {
    for (name, build, ends) in removals() {
        let (mut twin, id, reply, first) = build();
        assert_eq!(twin.stage(id), TransferStage::Removing, "{name}");
        let out = twin.reject(storage());
        assert_eq!((twin.stage(id), out.timers, out.writes(), out.all_replies(), out.shows().len(), out.deletes()), (TransferStage::Removing, 1, 0, 0, 0, 0), "{name}: the removal waits for its write");
        assert_eq!(server_silence(&out), SILENT, "{name}: nothing goes to the server again");
        let again = twin.tick_retry(id);
        assert_eq!((again.writes(), again.removed().len()), (1, 1), "{name}: the same removal is written again");
        assert_eq!(again.batches[0].ops, first.batches[0].ops, "{name}: the same batch");
        assert_eq!(server_silence(&again), SILENT, "{name}: nothing goes to the server again");
        let done = twin.commit();
        assert_eq!(server_silence(&done), SILENT, "{name}: nothing goes to the server after the write");
        assert_eq!(twin.stage(id), TransferStage::Done, "{name}");
        ends(&mut twin, id, reply, &done);
    }
}

#[test]
fn xfer_persisting_write_tick() {
    let mut part = Twin::new();
    let id = part.uploading(KEY, 3);
    part.step_ok(id, 1, Some("u1"));
    let mut complete = Twin::new();
    let complete_id = complete.completing(KEY);
    complete.completed(complete_id);
    let mut removing = Twin::offline();
    let removing_id = removing.accepted(KEY, 2);
    removing.cancel(KEY);
    for (name, twin, id, stage) in [("part", &mut part, id, TransferStage::PersistingPart), ("complete", &mut complete, complete_id, TransferStage::PersistingComplete), ("removing", &mut removing, removing_id, TransferStage::Removing)] {
        let first = twin.reject(storage());
        assert_eq!(first.timers, 1, "{name}");
        let stray = twin.post(TransferInput::Tick { kind: TransferTick::Retry { transfer_id: id, attempt_id: twin.attempt(id) + 1000 } });
        assert!(Twin::quiet(&stray), "{name}: a timer of another attempt writes nothing: {stray:?}");
        let again = twin.tick_retry(id);
        assert_eq!((twin.stage(id), again.writes()), (stage, 1), "{name}");
    }
}

#[test]
fn xfer_removing_invalid_cancel() {
    let mut twin = Twin::offline();
    let id = twin.accepted(KEY, 2);
    let (reply, _) = twin.cancel(KEY);
    let out = twin.reject(RejectReason::Invalid("bad row".into()));
    assert_eq!(Twin::fault(&out, reply), Some(Fault::Rejected(RejectReason::Invalid("bad row".into()))));
    assert_eq!((twin.stage(id), out.shows(), out.deletes(), out.timers, out.writes()), (TransferStage::Ready, vec![Shown::Queued], 0, 0, 0), "the file stays, the transfer is as before the cancel");
    let (mut failed, id) = failed_with_upload();
    let (reply, _) = failed.discard(KEY);
    assert_eq!(failed.stage(id), TransferStage::Removing);
    let out = failed.reject(RejectReason::Invalid("bad row".into()));
    assert_eq!(Twin::fault(&out, reply), Some(Fault::Rejected(RejectReason::Invalid("bad row".into()))));
    assert_eq!((failed.stage(id), out.deletes(), out.timers, out.writes()), (TransferStage::Failed, 0, 0, 0), "the file stays, the transfer is failed as before the discard");
}

#[test]
fn xfer_removing_confirmed_invalid_is_not_retried() {
    let mut confirmed = Twin::new();
    let id = confirmed.mutating(KEY);
    let (waiter, _) = confirmed.outcome(id, 1);
    confirmed.mutation_outcome(KEY, OutboxOutcome::Confirmed { revision: 7, result: None, row_id: None });
    let out = confirmed.reject(RejectReason::Invalid("bad row".into()));
    assert_eq!((confirmed.stage(id), out.shows(), out.timers, out.writes(), out.deletes()), (TransferStage::Done, vec![Shown::Gone], 0, 0, 0), "the outcome of the server ends the transfer, the open removes the row");
    assert_eq!(out.replies(waiter), vec![TransferAnswer::Outcome(OutboxOutcome::Confirmed { revision: 7, result: None, row_id: None }, None)]);
    let mut discarded = Twin::new();
    let id = discarded.mutating(KEY);
    let (reply, _) = discarded.discard(KEY);
    discarded.mutation_outcome(KEY, OutboxOutcome::Discarded);
    let out = discarded.reject(RejectReason::Invalid("bad row".into()));
    assert_eq!((discarded.stage(id), out.replies(reply), out.timers, out.writes()), (TransferStage::Done, vec![TransferAnswer::Ok], 0, 0));
}

#[test]
fn xfer_removing_expiry_invalid() {
    let mut twin = Twin::new();
    let id = twin.failed(KEY);
    twin.det.tick(Time(8 * DAY));
    twin.post(TransferInput::Tick { kind: TransferTick::Expire });
    assert_eq!(twin.stage(id), TransferStage::Removing);
    let out = twin.reject(RejectReason::Invalid("bad row".into()));
    assert_eq!((twin.stage(id), out.all_replies(), out.timers, out.writes(), out.deletes()), (TransferStage::Failed, 0, 0, 0, 0), "the transfer stays failed in memory");
}

#[test]
fn xfer_removing_accept_invalid() {
    let mut copy_failed = Twin::new();
    let (id, reply, _) = copy_failed.accepting(KEY, 2);
    copy_failed.commit();
    copy_failed.copy_failed(id);
    let out = copy_failed.reject(RejectReason::Invalid("bad row".into()));
    assert_eq!((Twin::fault(&out, reply), copy_failed.stage(id), out.deletes(), out.timers), (Some(Fault::Copy("unreadable".into())), TransferStage::Done, 0, 0), "as after Committed, the open removes the row and the file");
    let mut invalid = Twin::new();
    let (id, reply, _) = invalid.accepting(KEY, 2);
    invalid.commit();
    invalid.copy_done(id);
    let first = invalid.reject(RejectReason::Invalid("first".into()));
    assert_eq!((invalid.stage(id), first.all_replies()), (TransferStage::Removing, 0), "the first refusal starts the removal, nobody is answered yet");
    let out = invalid.reject(RejectReason::Invalid("second".into()));
    assert_eq!((Twin::fault(&out, reply), invalid.stage(id), out.timers), (Some(Fault::Rejected(RejectReason::Invalid("first".into()))), TransferStage::Done, 0));
}

#[test]
fn xfer_removing_no_session() {
    let mut twin = Twin::offline();
    let id = twin.accepted(KEY, 2);
    let (reply, _) = twin.cancel(KEY);
    let out = twin.reject(RejectReason::NoSession);
    assert_eq!((out.replies(reply), twin.stage(id), out.deletes()), (vec![TransferAnswer::Err(Fault::Ended(crate::ffi::EndCause::SessionEnded))], TransferStage::Done, 0), "the row stays until the account is cleared or the next open");
}

#[test]
fn xfer_failed_online() {
    let mut twin = Twin::new();
    twin.failed(KEY);
    twin.online(false);
    let out = twin.online(true);
    assert_eq!(out.timers, 1, "the check of the expiry is asked for at once");
}

#[test]
fn xfer_expired() {
    let mut row = stored(2, KEY, 2);
    row.restore_failed = Some("prior".into());
    row.created_at = Time(0);
    row.failed_at = Some(Time(2 * DAY));
    let (mut restored, _) = Twin::opened(vec![row], Default::default());
    restored.det.tick(Time(9 * DAY + 1));
    let expired = restored.post(TransferInput::Tick { kind: TransferTick::Expire });
    assert_eq!((restored.stage(2), expired.removed()), (TransferStage::Removing, vec![KEY.to_string()]));
    let mut twin = Twin::new();
    let id = twin.failed(KEY);
    twin.det.tick(Time(8 * DAY));
    let out = twin.post(TransferInput::Tick { kind: TransferTick::Expire });
    assert_eq!((twin.stage(id), out.removed(), out.all_replies()), (TransferStage::Removing, vec![KEY.to_string()], 0));
    twin.commit();
    let (reply, out) = twin.outcome(id, 1);
    assert_eq!(out.replies(reply), vec![failed_outcome(&super::transfer_harness::refusal())], "the outcome of the failed cycle is immutable");
}

#[test]
fn xfer_expiry_exact_week_keeps() {
    let mut row = stored(2, KEY, 2);
    row.restore_failed = Some("prior".into());
    row.created_at = Time(0);
    row.failed_at = Some(Time(2 * DAY));
    let (mut restored, _) = Twin::opened(vec![row], Default::default());
    restored.det.tick(Time(9 * DAY));
    let kept = restored.post(TransferInput::Tick { kind: TransferTick::Expire });
    assert!(Twin::quiet(&kept));
    assert_eq!(restored.stage(2), TransferStage::Failed);
    let mut twin = Twin::new();
    let id = twin.failed(KEY);
    twin.det.tick(Time(7 * DAY));
    let out = twin.post(TransferInput::Tick { kind: TransferTick::Expire });
    assert!(Twin::quiet(&out), "{out:?}");
    assert_eq!(twin.stage(id), TransferStage::Failed);
    twin.det.tick(Time(7 * DAY + 1));
    let out = twin.post(TransferInput::Tick { kind: TransferTick::Expire });
    assert_eq!((twin.stage(id), out.removed()), (TransferStage::Removing, vec![KEY.to_string()]), "one millisecond over the week expires");
}

#[test]
fn xfer_failed_no_failed_at_never_expires() {
    let mut row = stored(1, KEY, 2);
    row.restore_failed = Some("prior".into());
    row.failed_at = None;
    row.created_at = Time(0);
    let (mut twin, _) = Twin::opened(vec![row], Default::default());
    assert_eq!(twin.stage(1), TransferStage::Failed);
    twin.det.tick(Time(8 * DAY));
    let out = twin.post(TransferInput::Tick { kind: TransferTick::Expire });
    assert!(Twin::quiet(&out), "a Failed row that never recorded a failure time does not expire from created_at: {out:?}");
    assert_eq!(twin.stage(1), TransferStage::Failed);
}

#[test]
fn xfer_failed_other() {
    let mut twin = Twin::new();
    let id = twin.failed(KEY);
    let attempt_id = twin.attempt(id);
    let mut inputs = twin.stray_results(id);
    inputs.extend([
        TransferInput::StepOk { transfer_id: id, attempt_id, part: Some(1), upload_id: None, etag: None },
        TransferInput::Completed { transfer_id: id, attempt_id, signed_id: None },
        TransferInput::MutationOutcome { transfer_id: id, cycle_id: 1, intent_removed: true, outcome: OutboxOutcome::Confirmed { revision: 1, result: None, row_id: None }, row_never_created: false },
        TransferInput::Online(Online(false)),
        TransferInput::PowerSaving(true),
        TransferInput::SlotFreed,
    ]);
    twin.ignores(id, inputs);
}

#[test]
fn xfer_epoch_ended() {
    for (cause, ended) in [(Cause::SessionReset, crate::ffi::EndCause::SessionEnded), (Cause::Close, crate::ffi::EndCause::Closed)] {
        let mut twin = Twin::new();
        let (copying, accept_reply, _) = twin.accepting(KEY, 2);
        twin.commit();
        let uploading = twin.uploading(OTHER, 2);
        let (waiting, _) = twin.outcome(uploading, 1);
        let out = twin.end_epoch_by(cause);
        assert_eq!((twin.stage(copying), twin.stage(uploading)), (TransferStage::Done, TransferStage::Done), "{ended:?}");
        assert_eq!(out.replies(accept_reply), vec![TransferAnswer::Err(Fault::Ended(ended.clone()))], "{ended:?}: the accept is answered with the cause");
        assert_eq!(out.replies(waiting), vec![TransferAnswer::Outcome(OutboxOutcome::Ended(ended.clone()), None)], "{ended:?}: the outcome wait carries the cause");
        assert_eq!((out.cancel_steps(), out.all_replies()), (1, 2), "{ended:?}: each wait is answered once");
    }
}

#[test]
fn xfer_closing_copying() {
    let mut twin = Twin::new();
    let (id, reply, _) = twin.accepting(KEY, 2);
    twin.commit();
    let out = twin.close();
    assert_eq!((twin.stage(id), out.copy_cancels(), out.deletes(), out.drained()), (TransferStage::Done, 1, 1, 1));
    assert_eq!(out.replies(reply), vec![TransferAnswer::Err(Fault::Ended(crate::ffi::EndCause::Closed))]);
    let mut batch = Twin::new();
    batch.post(TransferInput::AcceptBatch { joint: Box::new(joint()), epoch: EPOCH, items: vec![item(KEY, 2), item(OTHER, 2)], reply: 5 });
    let ids = [batch.state().open_id(KEY).expect("first"), batch.state().open_id(OTHER).expect("second")];
    batch.reserve_batch(&ids);
    batch.commit();
    batch.copy_done(ids[0]);
    assert_eq!(batch.stage(ids[0]), TransferStage::CopyReady);
    let out = batch.close();
    assert!(ids.iter().all(|id| batch.stage(*id) == TransferStage::Done));
    assert_eq!((out.replies(5), out.drained()), (vec![TransferAnswer::Err(Fault::Ended(crate::ffi::EndCause::Closed))], 1));
}

#[test]
fn xfer_closing() {
    let mut twin = Twin::new();
    let uploading = twin.uploading(KEY, 2);
    let (waiting, _) = twin.outcome(uploading, 1);
    let out = twin.close();
    assert_eq!((twin.stage(uploading), out.cancel_steps(), out.drained()), (TransferStage::Done, 1, 1), "the row of the base stays for the next open");
    assert_eq!(out.replies(waiting), vec![TransferAnswer::Outcome(OutboxOutcome::Ended(crate::ffi::EndCause::Closed), None)]);
    assert_eq!(out.writes(), 0);
    let mut mutating = Twin::new();
    let id = mutating.mutating(OTHER);
    let out = mutating.close();
    assert_eq!((mutating.stage(id), out.drained()), (TransferStage::Done, 1));
    let out = Twin::bare(1).close();
    assert_eq!((out.drained(), out.all_replies()), (1, 0), "also while restoring");
    let out = Twin::new().close();
    assert_eq!(out.drained(), 1, "also with no transfer");
    let mut done = Twin::offline();
    let id = done.accepted(KEY, 2);
    done.cancel(KEY);
    done.commit();
    assert_eq!(done.stage(id), TransferStage::Done);
    assert_eq!(done.close().drained(), 1);
}

#[test]
fn xfer_outcome_old_cycle() {
    let (mut twin, id) = failed_with_upload();
    twin.retry(KEY, None);
    twin.commit();
    let (old, out) = twin.outcome(id, 1);
    assert_eq!(out.replies(old), vec![failed_outcome("Internal(Invalid): bad row")], "the stored outcome of cycle 1, whatever the state is now");
    let (current, out) = twin.outcome(id, 2);
    assert!(out.replies(current).is_empty(), "cycle 2 is still running");
}

#[test]
fn xfer_outcome_wait() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 2);
    let (reply, out) = twin.outcome(id, 1);
    assert!(out.replies(reply).is_empty());
    let (_, _) = twin.discard(KEY);
    let out = twin.commit();
    assert_eq!(out.replies(reply), vec![TransferAnswer::Outcome(OutboxOutcome::Discarded, None)], "the terminal cell answers every waiter of the cycle once");
    let (unknown, out) = twin.outcome(id + 40, 1);
    assert_eq!(Twin::fault(&out, unknown), Some(Fault::NotFound));
}

#[test]
fn xfer_interest() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    let out = twin.post(TransferInput::StatusInterestPlus { key: KEY.into(), stream_id: 5 });
    assert_eq!(out.statuses(), vec![(5, Shown::Uploading { progress: 0 })], "the first value answers the interest");
    twin.step_ok(id, 1, Some("u1"));
    let out = twin.commit();
    assert!(out.statuses().contains(&(5, Shown::Uploading { progress: 33 })));
}

/// xfer_interest_plus_stored_value: while a `Shown` batch is in flight a reader that joins gets the last committed value, not the pending write (D-226, D-273); Committed hands the new value to every subscriber once.
#[test]
fn xfer_interest_plus_stored_value() {
    let mut twin = Twin::new();
    twin.uploading(KEY, 3);
    let out = twin.post(TransferInput::StatusInterestPlus { key: KEY.into(), stream_id: 5 });
    assert_eq!(out.statuses(), vec![(5, Shown::Uploading { progress: 0 })], "the first reader gets the committed value");
    twin.hold_shown = true;
    let out = twin.online(false);
    assert_eq!((out.shown_batches().len(), out.statuses()), (1, Vec::new()), "the queued value waits for the Store, no reader sees it yet");
    let batch = out.shown_batches()[0];
    let mid = twin.post(TransferInput::StatusInterestPlus { key: KEY.into(), stream_id: 6 });
    assert_eq!(mid.statuses(), vec![(6, Shown::Uploading { progress: 0 })], "a reader joining while the value is in flight gets the last committed value, not the pending write");
    let committed = twin.commit_shown(batch);
    assert_eq!(committed.statuses(), vec![(5, Shown::Queued), (6, Shown::Queued)], "Committed hands the new value to every subscriber once");
}

/// xfer_status_by_key: a stream that follows a key before its transfer exists gets `Gone` at once and every later value of the transfer that takes the key.
#[test]
fn xfer_status_by_key() {
    let mut twin = Twin::new();
    let out = twin.post(TransferInput::StatusInterestPlus { key: KEY.into(), stream_id: 5 });
    assert_eq!(out.statuses(), vec![(5, Shown::Gone)]);
    let id = twin.uploading(KEY, 3);
    twin.post(TransferInput::StatusInterestPlus { key: "Moment:other:upload".into(), stream_id: 6 });
    twin.step_ok(id, 1, None);
    let out = twin.commit();
    assert!(out.statuses().contains(&(5, Shown::Uploading { progress: 33 })));
    assert!(out.statuses().iter().all(|(stream, _)| *stream == 5), "a key of another transfer gets no value of this one");
}

#[test]
fn xfer_interest_minus_unknown() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    twin.post(TransferInput::StatusInterestPlus { key: KEY.into(), stream_id: 5 });
    let out = twin.post(TransferInput::StatusInterestMinus { key: KEY.into(), stream_id: 9 });
    assert!(Twin::quiet(&out), "{out:?}");
    twin.step_ok(id, 1, None);
    assert!(!twin.commit().statuses().is_empty(), "the stream 5 still gets its values");
}

#[test]
fn xfer_round_status() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    let refused = TransferInput::RoundStatus { epoch: EPOCH, transfer_ids: vec![id], refused: Some("denied".into()) };
    let out = twin.post(refused.clone());
    assert_eq!(out.shows(), vec![Shown::Failed { error: "refused".into() }]);
    assert_eq!(out.round_errors(), vec![(KEY.to_string(), Some("denied".to_string()))], "the reason goes to the row of the transfer through the Store");
    assert!(Twin::quiet(&twin.post(refused)), "the same refusal is not shown twice");
    let out = twin.post(TransferInput::RoundStatus { epoch: EPOCH, transfer_ids: vec![id], refused: None });
    assert_eq!(out.shows(), vec![Shown::Uploading { progress: 0 }], "the refusal is taken off");
    assert_eq!(out.round_errors(), vec![(KEY.to_string(), None)], "the column is cleared");
    let out = twin.post(TransferInput::RoundStatus { epoch: EPOCH + 1, transfer_ids: vec![id], refused: Some("late".into()) });
    assert!(Twin::quiet(&out), "{out:?}");
    let mut broken = Twin::new();
    let failed = broken.failed(OTHER);
    let out = broken.post(TransferInput::RoundStatus { epoch: EPOCH, transfer_ids: vec![failed], refused: Some("denied".into()) });
    assert!(out.shows().is_empty(), "a failed transfer shows its own error");
    assert_eq!(out.round_errors(), vec![(OTHER.to_string(), Some("denied".to_string()))], "the column still holds the reason: the row shows error_message first");
}

#[test]
fn xfer_done() {
    let mut twin = Twin::offline();
    let id = twin.accepted(KEY, 2);
    twin.cancel(KEY);
    twin.commit();
    for (name, (reply, out)) in [("cancel", twin.cancel(KEY)), ("discard", twin.discard(KEY)), ("retry", twin.retry(KEY, None))] {
        assert_eq!(Twin::fault(&out, reply), Some(Fault::NotFound), "{name}");
    }
    assert_eq!(twin.stage(id), TransferStage::Done);
}

#[test]
fn xfer_expiry_counts_from_failed_at() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 2);
    twin.det.tick(Time(6 * DAY));
    twin.step_error(id, false);
    twin.commit();
    twin.det.tick(Time(8 * DAY));
    let out = twin.post(TransferInput::Tick { kind: TransferTick::Expire });
    assert!(Twin::quiet(&out) && twin.stage(id) == TransferStage::Failed, "two days after the failure the transfer is kept: {out:?}");
    twin.det.tick(Time(13 * DAY + 1));
    let out = twin.post(TransferInput::Tick { kind: TransferTick::Expire });
    assert_eq!((twin.stage(id), out.removed()), (TransferStage::Removing, vec![KEY.to_string()]), "a week and a millisecond after the failure it expires");
}

#[test]
fn xfer_failed_row_carries_failed_at() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 2);
    twin.det.tick(Time(6 * DAY));
    let out = twin.step_error(id, false);
    assert_eq!(out.rows().iter().map(|(row, _)| row.failed_at).collect::<Vec<_>>(), vec![Some(6 * DAY)], "the batch of the transition writes the time of it");
}

#[test]
fn xfer_end_discard_cancel_write_outcome() {
    for cancel in [true, false] {
        let mut twin = Twin::new();
        let (id, _, _) = twin.accepting(KEY, 2);
        twin.commit();
        let cycle = twin.t(id).cycle_id;
        let (_, out) = if cancel { twin.cancel(KEY) } else { twin.discard(KEY) };
        assert_eq!(out.outcomes_written(), vec![(id, cycle, OutboxOutcome::Discarded)], "cancel={cancel}");
    }
}

#[test]
fn a_transfer_with_its_write_in_flight_refuses_cancel_discard_and_retry() {
    let (mut retrying, id) = failed_with_upload();
    retrying.retry(KEY, None);
    refuses_calls(&mut retrying, KEY);
    assert_eq!(retrying.stage(id), TransferStage::PersistingRetry);
    let mut failing = Twin::new();
    let id = failing.persisting_failure(KEY);
    refuses_calls(&mut failing, KEY);
    assert_eq!(failing.stage(id), TransferStage::PersistingFailure);
    let mut removing = Twin::offline();
    let id = removing.accepted(KEY, 2);
    removing.cancel(KEY);
    refuses_calls(&mut removing, KEY);
    assert_eq!(removing.stage(id), TransferStage::Removing);
    let mut copying = Twin::new();
    let id = copying.persisting_copy(KEY, 2);
    refuses_calls(&mut copying, KEY);
    assert_eq!(copying.stage(id), TransferStage::PersistingCopy);
    let mut completing = Twin::new();
    let id = completing.persisting_complete(KEY);
    refuses_calls(&mut completing, KEY);
    assert_eq!(completing.stage(id), TransferStage::PersistingComplete);
}
