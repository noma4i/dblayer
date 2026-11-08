//! Owner-level tests of the owner `Transfer`: the steps of a file: `Ready`, `Uploading`, `PersistingPart`, `Completing`, `PersistingComplete`.

use super::transfer_harness::*;
use crate::owners::contract::{Online, RejectReason};
use crate::owners::store::UploadKind;
use crate::owners::write::outbox::{Fault, OutboxOutcome};
use crate::owners::write::upload::UploadConfig;
use crate::owners::write::transfer::{NextStep, Shown, TransferAnswer, TransferEffect, TransferId, TransferInput, TransferStage, TransferTick};
use crate::runtime::Time;
use crate::transport::NetworkPurpose;

const KEY: &str = "Moment:r1:upload";
const OTHER: &str = "Moment:r2:upload";

fn refuses_calls(twin: &mut Twin, key: &str) {
    for (name, (reply, out)) in [("cancel", twin.cancel(key)), ("discard", twin.discard(key)), ("retry", twin.retry(key, None))] {
        assert_eq!(Twin::fault(&out, reply), Some(Fault::InFlight), "{name}");
    }
}

/// `Ready{Part(1)}` of a transfer with `parts` parts: the network is down.
fn ready(parts: u32) -> (Twin, TransferId) {
    let mut twin = Twin::offline();
    let id = twin.accepted(KEY, parts);
    assert_eq!(twin.stage(id), TransferStage::Ready);
    (twin, id)
}

/// `Completing` of a multipart upload: both parts are confirmed and written.
fn completing_multipart(twin: &mut Twin, key: &str) -> TransferId {
    let id = twin.uploading(key, 2);
    twin.step_ok(id, 1, Some("u1"));
    twin.commit();
    twin.commit();
    twin.step_ok(id, 2, None);
    twin.commit();
    twin.commit();
    assert_eq!(twin.stage(id), TransferStage::Completing);
    id
}

fn purpose_of_steps(out: &Out) -> Vec<NetworkPurpose> { out.runs.iter().filter_map(|run| if let TransferEffect::Step { purpose, .. } = run { Some(*purpose) } else { None }).collect() }

#[test]
fn xfer_ready_start() {
    let (mut twin, id) = ready(3);
    let out = twin.online(true);
    assert_eq!(twin.stage(id), TransferStage::PersistingAttempt);
    assert_eq!(out.rows()[0].0.attempts, 1);
    assert!(out.steps().is_empty());
    let out = twin.commit();
    assert_eq!(twin.stage(id), TransferStage::Uploading);
    assert_eq!((out.steps().len(), out.steps()[0].0, out.shows()), (1, Some(1), vec![Shown::Uploading { progress: 0 }]));
    assert_eq!(purpose_of_steps(&out), vec![NetworkPurpose::Transfer]);
    let mut slow = Twin::offline();
    slow.post(TransferInput::PowerSaving(true));
    let other = slow.accepted(OTHER, 2);
    assert!(Twin::quiet(&slow.online(true)), "saving holds the start");
    let out = slow.post(TransferInput::PowerSaving(false));
    assert_eq!((slow.stage(other), out.steps().len()), (TransferStage::PersistingAttempt, 0));
    let out = slow.commit();
    assert_eq!((slow.stage(other), out.steps().len()), (TransferStage::Uploading, 1));
}

#[test]
fn xfer_attempt_committed_wait() {
    let (mut twin, id) = ready(2);
    twin.online(true);
    twin.online(false);
    let out = twin.commit();
    assert_eq!(twin.stage(id), TransferStage::Ready);
    assert!(out.steps().is_empty());
    let out = twin.online(true);
    assert_eq!(out.rows()[0].0.attempts, 2);
    assert!(out.steps().is_empty());
}

#[test]
fn xfer_attempt_busy() {
    let (mut twin, id) = ready(2);
    twin.online(true);
    refuses_calls(&mut twin, KEY);
    assert_eq!(twin.stage(id), TransferStage::PersistingAttempt);
}

#[test]
fn xfer_attempt_rejected() {
    for (reason, error) in [(RejectReason::Storage("disk".into()), "disk"), (RejectReason::Invalid, "Internal(Invalid)")] {
        let (mut twin, id) = ready(2);
        twin.online(true);
        let out = twin.reject(reason);
        assert_eq!(twin.stage(id), TransferStage::PersistingFailure);
        assert!(out.steps().is_empty());
        assert_eq!(out.rows()[0].0.failed.as_deref(), Some(error));
    }
}

#[test]
fn xfer_attempt_committed() {
    let (mut twin, id) = ready(2);
    let before = twin.online(true);
    let attempt = twin.attempt(id);
    assert!(before.steps().is_empty());
    let after = twin.commit();
    assert_eq!(after.steps(), vec![(Some(1), attempt)]);
    assert_eq!(twin.stage(id), TransferStage::Uploading);
}

#[test]
fn xfer_attempt_stale() {
    let (mut twin, id) = ready(2);
    twin.online(true);
    let attempt = twin.attempt(id);
    let out = twin.reject(RejectReason::StaleGeneration { models: vec!["Moment".into()], generation: 7 });
    assert_eq!(twin.stage(id), TransferStage::PersistingAttempt);
    assert_eq!(out.rows()[0].0.attempts, 1);
    assert_eq!(out.batches[0].basis.get("Moment"), Some(&7));
    assert!(out.steps().is_empty());
    assert_eq!(twin.commit().steps(), vec![(Some(1), attempt)]);
}

#[test]
fn xfer_attempt_other() {
    let (mut twin, id) = ready(2);
    twin.online(true);
    let attempt_id = twin.attempt(id);
    let out = twin.post(TransferInput::StepError { transfer_id: id, attempt_id, temporary: false, error: super::transfer_harness::refusal() });
    assert_eq!(twin.stage(id), TransferStage::PersistingAttempt);
    assert!(out.steps().is_empty());
    assert!(out.batches.is_empty());
}

#[test]
fn xfer_ready_start_complete() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 1);
    twin.step_ok(id, 1, None);
    twin.online(false);
    twin.commit();
    assert_eq!((twin.stage(id), twin.next_step(id)), (TransferStage::Ready, NextStep::Complete));
    let out = twin.online(true);
    assert_eq!((twin.stage(id), out.completes()), (TransferStage::PersistingAttempt, 0));
    let out = twin.commit();
    assert_eq!((twin.stage(id), out.completes(), out.steps().len(), out.shows()), (TransferStage::Completing, 1, 0, vec![Shown::Uploading { progress: 100 }]));
}

#[test]
fn xfer_ready_wait() {
    let mut twin = Twin::new();
    let id = twin.ready_behind(KEY);
    let out = twin.post(TransferInput::SlotFreed);
    assert!(Twin::quiet(&out), "the slot is held by the first transfer: {out:?}");
    assert_eq!(twin.stage(id), TransferStage::Ready);
    let first = twin.state().open_id("busy:r:upload").expect("the first transfer");
    let out = twin.step_error(first, false);
    assert_eq!((out.steps().len(), twin.stage(id)), (0, TransferStage::PersistingAttempt));
}

#[test]
fn xfer_ready_offline() {
    let (mut twin, id) = ready(2);
    let out = twin.online(false);
    assert!(Twin::quiet(&out), "{out:?}");
    assert_eq!(twin.stage(id), TransferStage::Ready);
}

#[test]
fn xfer_ready_cancel() {
    for cancel in [true, false] {
        let (mut twin, id) = ready(2);
        let (reply, out) = if cancel { twin.cancel(KEY) } else { twin.discard(KEY) };
        assert_eq!((twin.stage(id), out.removed(), out.all_replies()), (TransferStage::Removing, vec![KEY.to_string()], 0));
        let out = twin.commit();
        assert_eq!(out.replies(reply), vec![if cancel { TransferAnswer::Cancelled(true) } else { TransferAnswer::Ok }]);
        assert_eq!((twin.stage(id), out.deletes()), (TransferStage::Done, 1));
    }
}

#[test]
fn xfer_ready_retry() {
    let (mut twin, id) = ready(2);
    let (reply, out) = twin.retry(KEY, None);
    assert!(matches!(out.replies(reply).as_slice(), [TransferAnswer::Receipt(receipt)] if receipt.op_id == id && receipt.cycle_id == 1));
    assert_eq!((out.writes(), twin.stage(id)), (0, TransferStage::Ready));
}

#[test]
fn xfer_ready_other() {
    let (mut twin, id) = ready(2);
    let mut inputs = twin.stray_results(id);
    inputs.push(TransferInput::MutationOutcome { transfer_id: id, outcome: OutboxOutcome::Confirmed { revision: 1, result: None } });
    twin.ignores(id, inputs);
}

#[test]
fn xfer_step_ok() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    let out = twin.step_ok(id, 1, Some("u1"));
    assert_eq!(twin.stage(id), TransferStage::PersistingPart);
    let rows = out.rows();
    assert_eq!((rows.len(), rows[0].1, rows[0].0.confirmed_parts, rows[0].0.upload_id.as_deref(), rows[0].0.etags.clone()), (1, UploadKind::ConfirmedParts, 1, Some("u1"), vec![Some("e1".to_string())]));
    assert_eq!((out.steps().len(), out.shows().len()), (0, 0), "the next step and the progress wait for the write");
}

#[test]
fn xfer_all_parts() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 1);
    let out = twin.step_ok(id, 1, None);
    assert_eq!((twin.stage(id), twin.t(id).last_part, out.rows()[0].0.confirmed_parts), (TransferStage::PersistingPart, true, 1));
    assert_eq!(out.completes(), 0);
}

#[test]
fn xfer_step_temp() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    let before = twin.attempt(id);
    let out = twin.step_error(id, true);
    assert_eq!((twin.stage(id), twin.t(id).attempts, twin.t(id).waiting_retry, out.timers), (TransferStage::Uploading, 1, true, 1));
    assert_ne!(twin.attempt(id), before, "the retry is a new physical attempt");
    assert_eq!((out.writes(), out.steps().len(), out.all_replies()), (0, 0, 0));
    assert_eq!(twin.state().active(), 0, "the slot is free while the retry waits");
}

#[test]
fn xfer_put_timeout() {
    let (mut twin, id) = ready(3);
    twin.online(true);
    let attempt = twin.attempt(id);
    let armed = twin.commit();
    let now = twin.state().now;
    let expected = TransferInput::Tick { now, kind: TransferTick::PutTimeout { transfer_id: id, attempt_id: attempt } };
    assert_eq!(armed.schedules, vec![(UploadConfig::default().put_timeout, format!("{expected:?}"))], "the step arms one timeout of its attempt");
    let out = twin.put_timeout(id, attempt);
    assert_eq!((twin.stage(id), twin.t(id).attempts, twin.t(id).waiting_retry, out.timers, out.cancel_steps()), (TransferStage::Uploading, 1, true, 1, 1));
    assert_ne!(twin.attempt(id), attempt, "the retry is a new physical attempt");
    assert_eq!((out.writes(), out.steps().len(), out.all_replies()), (0, 0, 0));
    assert_eq!(twin.state().active(), 0, "the slot is free while the retry waits");
    let (mut done, id) = ready(3);
    done.online(true);
    done.commit();
    assert_eq!(done.step_ok(id, 1, Some("u1")).cancels, 1, "the answer of the step disarms its timeout");
    let (mut worn, id) = ready(3);
    worn.online(true);
    worn.commit();
    for _ in 0..49 {
        let attempt = worn.attempt(id);
        assert_eq!(worn.put_timeout(id, attempt).writes(), 0);
        worn.tick_retry(id);
        worn.commit();
    }
    let attempt = worn.attempt(id);
    let out = worn.put_timeout(id, attempt);
    assert_eq!((worn.stage(id), out.kinds()), (TransferStage::PersistingFailure, vec![UploadKind::Failed]), "the 50th attempt is the last");
}

#[test]
fn xfer_put_timeout_other() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    let attempt = twin.attempt(id);
    let wrong = |transfer_id, attempt_id| TransferInput::Tick { now: Time(1), kind: TransferTick::PutTimeout { transfer_id, attempt_id } };
    twin.ignores(id, vec![wrong(id, attempt + 1), wrong(id + 77, attempt)]);
    let mut completing = Twin::new();
    let id = completing_multipart(&mut completing, OTHER);
    let attempt = completing.attempt(id);
    completing.ignores(id, vec![wrong(id, attempt)]);
}

#[test]
fn xfer_put_timeout_cancelled() {
    let in_flight = || {
        let mut twin = Twin::new();
        let id = twin.uploading(KEY, 3);
        (twin, id)
    };
    let deadline = Time(u64::try_from(UploadConfig::default().put_timeout.as_millis()).unwrap() + 1);
    let fired = |twin: &mut Twin| {
        let before = twin.det.trace().len();
        twin.det.tick(deadline);
        twin.det.run_until_quiet(10_000).unwrap();
        twin.det.trace()[before..].iter().filter(|entry| entry.input.contains("PutTimeout")).count()
    };
    let (mut control, _) = in_flight();
    assert_eq!(fired(&mut control), 1, "an armed timeout fires once at its deadline");
    let (mut twin, id) = in_flight();
    assert_eq!(twin.step_ok(id, 1, Some("u1")).cancels, 1, "StepOk");
    assert_eq!(fired(&mut twin), 0);
    let (mut twin, id) = in_flight();
    assert_eq!(twin.step_error(id, true).cancels, 1, "StepError temporary");
    let (mut twin, id) = in_flight();
    assert_eq!(twin.step_error(id, false).cancels, 1, "StepError permanent");
    let (mut twin, _) = in_flight();
    assert_eq!(twin.online(false).cancels, 1, "Online(false)");
    assert_eq!(fired(&mut twin), 0);
    let (mut twin, _) = in_flight();
    assert_eq!(twin.post(TransferInput::PowerSaving(true)).cancels, 1, "PowerSaving");
    assert_eq!(fired(&mut twin), 0);
    let (mut twin, _) = in_flight();
    twin.end_epoch();
    assert_eq!(fired(&mut twin), 0, "EpochEnded");
}

#[test]
fn xfer_step_fail() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    twin.step_ok(id, 1, Some("u1"));
    twin.commit();
    twin.commit();
    let out = twin.step_error(id, false);
    assert_eq!((twin.stage(id), out.aborts(), out.kinds()), (TransferStage::PersistingFailure, 0, vec![UploadKind::Failed]));
    assert_eq!(out.orphans_written(), vec![("u1".into(), KEY.into())]);
    assert_eq!(out.rows()[0].0.confirmed_parts, 0);
    assert_eq!(out.shows().len(), 0, "failed is observable only after the write");
    let mut single = Twin::new();
    let id = single.uploading(OTHER, 2);
    assert_eq!(single.step_error(id, false).aborts(), 0, "no multipart upload, nothing to abort");
    let mut worn = Twin::new();
    let id = worn.uploading(KEY, 2);
    for _ in 0..49 {
        assert_eq!(worn.step_error(id, true).writes(), 0);
        worn.tick_retry(id);
        worn.commit();
    }
    let out = worn.step_error(id, true);
    assert_eq!((worn.stage(id), out.kinds()), (TransferStage::PersistingFailure, vec![UploadKind::Failed]), "the 50th attempt is the last");
}

#[test]
fn xfer_step_tick() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    let first = twin.attempt(id);
    twin.step_error(id, true);
    let waiting = twin.attempt(id);
    let out = twin.tick_retry(id);
    assert!(out.steps().is_empty());
    assert_ne!(waiting, first);
    let out = twin.commit();
    assert_eq!(out.steps()[0].0, Some(1));
    assert_eq!((twin.stage(id), twin.t(id).waiting_retry, twin.state().active()), (TransferStage::Uploading, false, 1));
}

#[test]
fn xfer_step_tick_other() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    twin.ignores(id, vec![TransferInput::Tick { now: Time(1), kind: TransferTick::Retry { transfer_id: id, attempt_id: twin.attempt(id) } }]);
    twin.step_error(id, true);
    let wrong = TransferInput::Tick { now: Time(1), kind: TransferTick::Retry { transfer_id: id, attempt_id: twin.attempt(id) + 1 } };
    twin.ignores(id, vec![wrong, TransferInput::Tick { now: Time(1), kind: TransferTick::Retry { transfer_id: id + 77, attempt_id: twin.attempt(id) } }]);
}

#[test]
fn xfer_step_offline() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    let out = twin.online(false);
    assert_eq!((twin.stage(id), twin.next_step(id), out.cancel_steps(), out.shows()), (TransferStage::Ready, NextStep::Part(1), 1, vec![Shown::Queued]));
    let mut waiting = Twin::new();
    let id = waiting.uploading(KEY, 3);
    waiting.step_error(id, true);
    assert_eq!(waiting.online(false).cancel_steps(), 0, "no step in flight, nothing to cancel");
    assert_eq!(waiting.stage(id), TransferStage::Ready);
}

#[test]
fn xfer_step_power_saving() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    let out = twin.post(TransferInput::PowerSaving(true));
    assert_eq!((twin.stage(id), out.cancel_steps(), out.shows()), (TransferStage::Ready, 1, vec![Shown::Queued]));
    let out = twin.post(TransferInput::PowerSaving(false));
    assert_eq!((twin.stage(id), out.steps().len()), (TransferStage::PersistingAttempt, 0));
    twin.commit();
}

#[test]
fn xfer_step_online() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    twin.ignores(id, vec![TransferInput::Online(Online(true)), TransferInput::PowerSaving(false), TransferInput::SlotFreed]);
}

#[test]
fn xfer_uploading_cancel() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    twin.step_ok(id, 1, Some("u1"));
    twin.commit();
    twin.commit();
    let (reply, out) = twin.cancel(KEY);
    assert_eq!((twin.stage(id), out.cancel_steps(), out.aborts(), out.removed()), (TransferStage::Removing, 1, 0, vec![KEY.to_string()]));
    assert_eq!(out.all_replies(), 0, "the answer waits for the write");
    let out = twin.commit();
    assert_eq!((out.replies(reply), out.orphan_aborts()), (vec![TransferAnswer::Cancelled(true)], vec!["u1".to_string()]));
    assert_eq!(twin.stage(id), TransferStage::Done);
}

#[test]
fn xfer_uploading_discard() {
    let mut twin = Twin::new();
    let ids = twin.accepted_batch(&[KEY, OTHER], 2);
    twin.commit();
    let (reply, out) = twin.discard(KEY);
    assert_eq!((twin.stage(ids[0]), out.cancel_steps(), out.removed()), (TransferStage::Removing, 1, vec![KEY.to_string()]), "a discard works for a batch element");
    let out = twin.commit();
    assert_eq!(out.replies(reply), vec![TransferAnswer::Ok]);
}

#[test]
fn xfer_uploading_retry() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    let (reply, out) = twin.retry(KEY, None);
    assert!(matches!(out.replies(reply).as_slice(), [TransferAnswer::Receipt(_)]));
    assert_eq!((out.writes(), twin.stage(id)), (0, TransferStage::Uploading));
}

#[test]
fn xfer_uploading_stray() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    let attempt_id = twin.attempt(id);
    twin.ignores(
        id,
        vec![
            TransferInput::StepOk { transfer_id: id, attempt_id: attempt_id + 1, part: Some(1), upload_id: None, etag: None },
            TransferInput::StepError { transfer_id: id, attempt_id: attempt_id + 1, temporary: false, error: super::transfer_harness::refusal() },
            TransferInput::StepOk { transfer_id: id + 40, attempt_id, part: Some(1), upload_id: None, etag: None },
            TransferInput::StepOk { transfer_id: id, attempt_id, part: Some(3), upload_id: None, etag: None },
        ],
    );
}

#[test]
fn xfer_uploading_other() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    let attempt_id = twin.attempt(id);
    twin.ignores(
        id,
        vec![
            TransferInput::Completed { transfer_id: id, attempt_id, signed_id: None },
            TransferInput::CompleteError { transfer_id: id, attempt_id, temporary: false, error: super::transfer_harness::refusal() },
            TransferInput::MutationOutcome { transfer_id: id, outcome: OutboxOutcome::Confirmed { revision: 1, result: None } },
        ],
    );
}

#[test]
fn xfer_persisting_part_committed() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    twin.step_ok(id, 1, Some("u1"));
    let out = twin.commit();
    assert_eq!((twin.stage(id), out.steps().len()), (TransferStage::PersistingAttempt, 0));
    assert_eq!(out.shows().first(), Some(&Shown::Uploading { progress: 33 }));
    assert_eq!(twin.commit().steps()[0].0, Some(2));
    let mut offline = Twin::new();
    let id = offline.uploading(KEY, 3);
    offline.step_ok(id, 1, Some("u1"));
    offline.online(false);
    offline.commit();
    assert_eq!((offline.stage(id), offline.next_step(id)), (TransferStage::Ready, NextStep::Part(2)));
}

#[test]
fn xfer_persisting_part_last() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 1);
    twin.step_ok(id, 1, None);
    twin.online(false);
    let out = twin.commit();
    assert_eq!((twin.stage(id), twin.next_step(id), out.shows()), (TransferStage::Ready, NextStep::Complete, vec![Shown::Uploading { progress: 100 }]));
    let mut online = Twin::new();
    let id = online.uploading(KEY, 1);
    online.step_ok(id, 1, None);
    assert_eq!((online.commit().completes(), online.stage(id)), (0, TransferStage::PersistingAttempt));
    assert_eq!((online.commit().completes(), online.stage(id)), (1, TransferStage::Completing));
}

#[test]
fn xfer_persisting_other() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    twin.step_ok(id, 1, None);
    assert!(Twin::quiet(&twin.online(false)));
    assert!(Twin::quiet(&twin.post(TransferInput::PowerSaving(true))));
    assert_eq!((twin.state().online, twin.state().saving, twin.stage(id)), (Some(false), true, TransferStage::PersistingPart), "the fields are updated, the state stays");
    twin.ignores(id, vec![TransferInput::SlotFreed]);
    let mut failing = Twin::new();
    let id = failing.persisting_failure(KEY);
    let mut inputs = failing.stray_results(id);
    inputs.truncate(4);
    failing.ignores(id, inputs);
    let mut copying = Twin::new();
    let id = copying.persisting_copy(KEY, 2);
    copying.ignores(id, vec![TransferInput::SlotFreed, TransferInput::Online(Online(true))]);
}

#[test]
fn xfer_persisting_storage_failure() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    twin.step_ok(id, 1, None);
    let out = twin.reject(RejectReason::Storage("disk".into()));
    assert_eq!((twin.stage(id), out.kinds(), out.shows().len()), (TransferStage::PersistingFailure, vec![UploadKind::Failed], 0));
    assert_eq!(twin.t(id).error.as_deref(), Some("disk"));
    let mut complete = Twin::new();
    let id = complete.persisting_complete(KEY);
    let out = complete.reject(RejectReason::Storage("disk".into()));
    assert_eq!((complete.stage(id), out.kinds()), (TransferStage::PersistingFailure, vec![UploadKind::Failed]));
}

#[test]
fn xfer_persisting_part_refuses() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    twin.step_ok(id, 1, None);
    refuses_calls(&mut twin, KEY);
    assert_eq!(twin.stage(id), TransferStage::PersistingPart);
}

#[test]
fn xfer_completed() {
    let mut twin = Twin::new();
    let id = twin.completing(KEY);
    let out = twin.completed(id);
    let rows = out.rows();
    assert_eq!((twin.stage(id), rows.len(), rows[0].1, rows[0].0.completed, rows[0].0.signed_id.as_deref()), (TransferStage::PersistingComplete, 1, UploadKind::Completed, true, Some("signed")));
    assert_eq!(out.submits(), 0, "the mutation goes to Outbox after the write");
}

#[test]
fn xfer_complete_temp() {
    let mut twin = Twin::new();
    let id = twin.completing(KEY);
    for round in 1..=10 {
        let out = twin.complete_error(id, true);
        assert_eq!((out.timers, out.writes(), twin.stage(id)), (1, 0, TransferStage::Completing), "round {round}");
        assert_eq!(twin.tick_retry(id).completes(), 0);
        assert_eq!(twin.commit().completes(), 1);
    }
}

#[test]
fn xfer_complete_fail() {
    let mut twin = Twin::new();
    let id = twin.completing(KEY);
    let out = twin.complete_error(id, false);
    let rows = out.rows();
    assert_eq!((twin.stage(id), out.aborts(), rows[0].1, rows[0].0.confirmed_parts), (TransferStage::PersistingFailure, 0, UploadKind::Failed, 1), "the parts stay, no abort");
}

#[test]
fn xfer_complete_tick() {
    let mut twin = Twin::new();
    let id = twin.completing(KEY);
    let first = twin.attempt(id);
    twin.complete_error(id, true);
    let out = twin.tick_retry(id);
    assert_eq!(out.completes(), 0);
    assert_eq!(twin.commit().completes(), 1);
    assert_ne!(twin.attempt(id), first);
    twin.ignores(id, vec![TransferInput::Tick { now: Time(1), kind: TransferTick::Retry { transfer_id: id, attempt_id: twin.attempt(id) } }]);
}

#[test]
fn xfer_complete_offline() {
    let mut twin = Twin::new();
    let id = twin.completing(KEY);
    let out = twin.online(false);
    assert_eq!((twin.stage(id), twin.next_step(id), out.cancel_steps(), out.shows()), (TransferStage::Ready, NextStep::Complete, 1, vec![Shown::Queued]));
    let mut saving = Twin::new();
    let id = saving.completing(KEY);
    saving.post(TransferInput::PowerSaving(true));
    assert_eq!(saving.stage(id), TransferStage::Ready);
}

#[test]
fn xfer_completing_cancel() {
    let mut twin = Twin::new();
    let id = completing_multipart(&mut twin, KEY);
    let (reply, out) = twin.cancel(KEY);
    assert_eq!((twin.stage(id), out.cancel_steps(), out.aborts(), out.removed()), (TransferStage::Removing, 1, 0, vec![KEY.to_string()]));
    let committed = twin.commit();
    assert_eq!((committed.replies(reply), committed.orphan_aborts()), (vec![TransferAnswer::Cancelled(true)], vec!["u1".to_string()]));
}

#[test]
fn xfer_completing_discard() {
    let mut twin = Twin::new();
    let id = completing_multipart(&mut twin, KEY);
    let (reply, out) = twin.discard(KEY);
    assert_eq!((twin.stage(id), out.aborts(), out.orphans_written().len()), (TransferStage::Removing, 0, 1));
    assert_eq!(twin.commit().replies(reply), vec![TransferAnswer::Ok]);
}

#[test]
fn xfer_completing_retry() {
    let mut twin = Twin::new();
    let id = twin.completing(KEY);
    let (reply, out) = twin.retry(KEY, None);
    assert!(matches!(out.replies(reply).as_slice(), [TransferAnswer::Receipt(_)]));
    assert_eq!(twin.stage(id), TransferStage::Completing);
}

#[test]
fn xfer_completing_other() {
    let mut twin = Twin::new();
    let id = twin.completing(KEY);
    let attempt_id = twin.attempt(id);
    twin.ignores(
        id,
        vec![
            TransferInput::StepOk { transfer_id: id, attempt_id, part: Some(1), upload_id: None, etag: None },
            TransferInput::StepError { transfer_id: id, attempt_id, temporary: false, error: super::transfer_harness::refusal() },
            TransferInput::MutationOutcome { transfer_id: id, outcome: OutboxOutcome::Confirmed { revision: 1, result: None } },
            TransferInput::SlotFreed,
            TransferInput::Online(Online(true)),
            TransferInput::PowerSaving(false),
        ],
    );
}

#[test]
fn xfer_persisting_complete_committed() {
    let mut twin = Twin::new();
    let id = twin.persisting_complete(KEY);
    let out = twin.commit();
    assert_eq!((twin.stage(id), out.submits(), out.shows()), (TransferStage::Mutating, 1, vec![Shown::Uploading { progress: 100 }]));
    assert!(matches!(&out.outbox[0], crate::owners::write::outbox::OutboxInput::SubmitReserved { op, .. } if op.origin == (crate::owners::write::outbox::OpSource::Transfer { transfer_id: id, cycle_id: 1 })));
}

#[test]
fn xfer_persisting_complete_refuses() {
    let mut twin = Twin::new();
    let id = twin.persisting_complete(KEY);
    refuses_calls(&mut twin, KEY);
    assert_eq!(twin.stage(id), TransferStage::PersistingComplete);
}

#[test]
fn inv_upload_08_a_restarted_part_never_lowers_the_total_inside_a_phase() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 4);
    let levels = |out: Out| -> Vec<u32> { out.shows().into_iter().filter_map(|show| if let Shown::Uploading { progress } = show { Some(progress) } else { None }).collect() };
    let mut committed = 0;
    let commit = |twin: &mut Twin, committed: &mut u32| {
        let shown = levels(twin.commit());
        assert!(shown.iter().all(|level| level >= committed), "a committed part never lowers the value: {shown:?} after {committed}");
        *committed = shown.last().copied().unwrap_or(*committed);
    };
    let before = levels(twin.step_ok(id, 1, None));
    assert!(before.is_empty(), "a step result changes no value: {before:?}");
    commit(&mut twin, &mut committed);
    assert_eq!(committed, 25);
    twin.commit();
    for outs in [levels(twin.step_error(id, true)), levels(twin.tick_retry(id))] {
        assert!(outs.iter().all(|level| *level == committed), "an error and a restart show only the committed value: {outs:?}");
    }
    twin.commit();
    assert!(levels(twin.step_ok(id, 2, None)).is_empty());
    commit(&mut twin, &mut committed);
    assert_eq!(committed, 50);
}
