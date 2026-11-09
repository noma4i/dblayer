//! Owner-level tests of the owner `Transfer`: `Failed`, `retry`, `Removing`, the expiry, the outcomes of the receipts, the streams of status, the end of the epoch and the close.

use super::transfer_harness::*;
use crate::owners::contract::{Online, RejectReason};
use crate::owners::store::UploadKind;
use crate::owners::write::outbox::{Fault, OutboxOutcome};
use crate::owners::write::transfer::{NextStep, Shown, Source, TransferAnswer, TransferId, TransferInput, TransferStage, TransferTick};
use crate::runtime::Time;

const KEY: &str = "Moment:r1:upload";
const OTHER: &str = "Moment:r2:upload";

fn refuses_calls(twin: &mut Twin, key: &str) {
    for (name, (reply, out)) in [("cancel", twin.cancel(key)), ("discard", twin.discard(key)), ("retry", twin.retry(key, None))] {
        assert_eq!(Twin::fault(&out, reply), Some(Fault::InFlight), "{name}");
    }
}

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

/// `Failed` with one confirmed part of three and an upload id, `abort` not called: the write of the second part was refused by the base.
fn failed_with_upload() -> (Twin, TransferId) {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    twin.step_ok(id, 1, Some("u1"));
    twin.reject(RejectReason::Storage("disk".into()));
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
    let (_, out) = twin.retry(KEY, None);
    assert_eq!((twin.stage(id), twin.next_step(id), out.rows()[0].0.confirmed_parts), (TransferStage::PersistingRetry, NextStep::Part(2), 1), "the first part that is not confirmed");
    assert_eq!(out.rows()[0].0.attempts, 0);
    let out = twin.commit();
    assert!(out.steps().is_empty());
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
fn xfer_persisting_retry_refuses() {
    let (mut twin, id) = failed_with_upload();
    twin.retry(KEY, None);
    refuses_calls(&mut twin, KEY);
    assert_eq!(twin.stage(id), TransferStage::PersistingRetry);
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

#[test]
fn xfer_persisting_failure_tick() {
    let mut twin = Twin::new();
    let id = twin.persisting_failure(KEY);
    twin.reject(RejectReason::Storage("disk".into()));
    let out = twin.tick_retry(id);
    assert_eq!((twin.stage(id), out.kinds()), (TransferStage::PersistingFailure, vec![UploadKind::Failed]));
}

#[test]
fn xfer_persisting_failure_refuses() {
    let mut twin = Twin::new();
    let id = twin.persisting_failure(KEY);
    refuses_calls(&mut twin, KEY);
    assert_eq!(twin.stage(id), TransferStage::PersistingFailure);
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
    let (reply, out) = twin.discard(KEY);
    assert_eq!((twin.stage(id), out.aborts(), out.removed()), (TransferStage::Removing, 0, vec![KEY.to_string()]));
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

#[test]
fn xfer_removing_rejected() {
    let mut twin = Twin::offline();
    let id = twin.accepted(KEY, 2);
    let (reply, _) = twin.cancel(KEY);
    let out = twin.reject(RejectReason::Storage("disk".into()));
    assert_eq!(Twin::fault(&out, reply), Some(Fault::Rejected(RejectReason::Storage("disk".into()))));
    assert_eq!((twin.stage(id), out.shows(), out.deletes()), (TransferStage::Ready, vec![Shown::Queued], 0), "the file stays, durable Failed is not declared");
    let (mut failed, id) = failed_with_upload();
    let (reply, _) = failed.discard(KEY);
    let out = failed.reject(RejectReason::Storage("disk".into()));
    assert_eq!((Twin::fault(&out, reply).is_some(), failed.stage(id)), (true, TransferStage::Failed));
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
fn xfer_removing_refuses() {
    let mut twin = Twin::offline();
    let id = twin.accepted(KEY, 2);
    twin.cancel(KEY);
    refuses_calls(&mut twin, KEY);
    assert_eq!(twin.stage(id), TransferStage::Removing);
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
    let mut twin = Twin::new();
    let id = twin.failed(KEY);
    let out = twin.post(TransferInput::Tick { now: Time(8 * DAY), kind: TransferTick::Expire });
    assert_eq!((twin.stage(id), out.removed(), out.all_replies()), (TransferStage::Removing, vec![KEY.to_string()], 0));
    twin.commit();
    let (reply, out) = twin.outcome(id, 1);
    assert_eq!(out.replies(reply), vec![failed_outcome(&super::transfer_harness::refusal())], "the outcome of the failed cycle is immutable");
}

#[test]
fn xfer_not_expired() {
    let mut twin = Twin::new();
    let id = twin.failed(KEY);
    let out = twin.post(TransferInput::Tick { now: Time(6 * DAY), kind: TransferTick::Expire });
    assert!(Twin::quiet(&out), "{out:?}");
    assert_eq!(twin.stage(id), TransferStage::Failed);
}

#[test]
fn xfer_expiry_exact_week_keeps() {
    let mut twin = Twin::new();
    let id = twin.failed(KEY);
    let out = twin.post(TransferInput::Tick { now: Time(7 * DAY), kind: TransferTick::Expire });
    assert!(Twin::quiet(&out), "{out:?}");
    assert_eq!(twin.stage(id), TransferStage::Failed);
    let out = twin.post(TransferInput::Tick { now: Time(7 * DAY + 1), kind: TransferTick::Expire });
    assert_eq!((twin.stage(id), out.removed()), (TransferStage::Removing, vec![KEY.to_string()]), "one millisecond over the week expires");
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
        TransferInput::MutationOutcome { transfer_id: id, cycle_id: 1, intent_removed: true, outcome: OutboxOutcome::Confirmed { revision: 1, result: None, row_id: None } },
        TransferInput::Online(Online(false)),
        TransferInput::PowerSaving(true),
        TransferInput::SlotFreed,
    ]);
    twin.ignores(id, inputs);
}

#[test]
fn xfer_epoch_ended() {
    let mut twin = Twin::new();
    let (copying, accept_reply, _) = twin.accepting(KEY, 2);
    twin.commit();
    let uploading = twin.uploading(OTHER, 2);
    let (waiting, _) = twin.outcome(uploading, 1);
    let out = twin.end_epoch();
    assert_eq!((twin.stage(copying), twin.stage(uploading)), (TransferStage::Done, TransferStage::Done));
    assert_eq!(out.replies(accept_reply), vec![TransferAnswer::Err(Fault::Ended(crate::ffi::EndCause::SessionEnded))]);
    assert_eq!(out.replies(waiting), vec![TransferAnswer::Outcome(OutboxOutcome::Ended(crate::ffi::EndCause::SessionEnded), None)]);
    assert_eq!((out.cancel_steps(), out.all_replies()), (1, 2), "each wait is answered once");
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
    batch.post(TransferInput::AcceptBatch { joint: Box::new(joint()), epoch: EPOCH, items: vec![item(KEY, 2), item(OTHER, 2)], reply: 5, now: Time(0) });
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
}

#[test]
fn xfer_restoring_closing() {
    let mut twin = Twin::bare(1);
    let out = twin.close();
    assert_eq!((out.drained(), out.all_replies()), (1, 0));
}

#[test]
fn xfer_done_closing() {
    let mut twin = Twin::new();
    let out = twin.close();
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
    assert_eq!(out.replies(old), vec![failed_outcome("disk")], "the stored outcome of cycle 1, whatever the state is now");
    let (current, out) = twin.outcome(id, 2);
    assert!(out.replies(current).is_empty(), "cycle 2 is still running");
}

#[test]
fn xfer_done_outcome() {
    let mut twin = Twin::offline();
    let id = twin.accepted(KEY, 2);
    twin.cancel(KEY);
    twin.commit();
    let (reply, out) = twin.outcome(id, 1);
    assert_eq!(out.replies(reply), vec![TransferAnswer::Outcome(OutboxOutcome::Discarded, None)]);
}

#[test]
fn xfer_failed_outcome() {
    let mut twin = Twin::new();
    let id = twin.failed(KEY);
    let (reply, out) = twin.outcome(id, 1);
    assert_eq!(out.replies(reply), vec![failed_outcome(&super::transfer_harness::refusal())]);
    assert_eq!(twin.stage(id), TransferStage::Failed);
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
fn xfer_interest_minus() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    twin.post(TransferInput::StatusInterestPlus { key: KEY.into(), stream_id: 5 });
    twin.post(TransferInput::StatusInterestPlus { key: KEY.into(), stream_id: 6 });
    twin.post(TransferInput::StatusInterestMinus { key: KEY.into(), stream_id: 5 });
    twin.step_ok(id, 1, None);
    let out = twin.commit();
    let streams: Vec<u64> = out.statuses().into_iter().map(|(stream, _)| stream).collect();
    assert!(!streams.is_empty() && streams.iter().all(|stream| *stream == 6), "only the own id is removed: {streams:?}");
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
    twin.post(TransferInput::Clock(Time(6 * DAY)));
    twin.step_error(id, false);
    twin.commit();
    let out = twin.post(TransferInput::Tick { now: Time(8 * DAY), kind: TransferTick::Expire });
    assert!(Twin::quiet(&out) && twin.stage(id) == TransferStage::Failed, "two days after the failure the transfer is kept: {out:?}");
    let out = twin.post(TransferInput::Tick { now: Time(13 * DAY + 1), kind: TransferTick::Expire });
    assert_eq!((twin.stage(id), out.removed()), (TransferStage::Removing, vec![KEY.to_string()]), "a week and a millisecond after the failure it expires");
}

#[test]
fn xfer_failed_row_carries_failed_at() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 2);
    twin.post(TransferInput::Clock(Time(6 * DAY)));
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
