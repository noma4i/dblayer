//! Properties of the owner `Transfer`: they hold on any trace of the scenarios below, at the level of the owner.

use std::collections::BTreeMap;

use super::transfer_harness::*;
use crate::store::files::Location;
use crate::owners::contract::RejectReason;
use crate::owners::write::outbox::{Fault, OutboxOutcome};
use crate::owners::write::transfer::{Shown, TransferAnswer, TransferEffect, TransferId, TransferInput, TransferStage, TransferTick};
use crate::runtime::Time;
use crate::transport::NetworkPurpose;

const KEY: &str = "Moment:r1:upload";
const OTHER: &str = "Moment:r2:upload";
const THIRD: &str = "Moment:r3:upload";

fn outcome_of(twin: &mut Twin, id: TransferId, cycle: u64) -> Option<TransferAnswer> {
    let (reply, out) = twin.outcome(id, cycle);
    out.replies(reply).into_iter().next()
}

/// The traces that end a cycle: each returns the owner, the transfer and the outcome it must keep.
fn terminal_traces() -> Vec<(&'static str, Twin, TransferId, OutboxOutcome)> {
    let mut confirmed = Twin::new();
    let id = confirmed.mutating(KEY);
    let mut discarded = Twin::offline();
    let discarded_id = discarded.accepted(KEY, 2);
    let mut failed = Twin::new();
    let failed_id = failed.uploading(KEY, 2);
    let mut interrupted = Twin::new();
    let interrupted_id = interrupted.uploading(KEY, 2);
    vec![
        ("confirmed", confirmed, id, OutboxOutcome::Confirmed { revision: 4, result: None, row_id: None }),
        ("discarded", discarded, discarded_id, OutboxOutcome::Discarded),
        ("failed", failed, failed_id, OutboxOutcome::Failed { error: super::transfer_harness::refusal() }),
        ("interrupted", interrupted, interrupted_id, OutboxOutcome::Ended(crate::ffi::EndCause::SessionEnded)),
    ]
}

fn finish(name: &str, twin: &mut Twin) -> Out {
    match name {
        "confirmed" => {
            twin.mutation_outcome(KEY, OutboxOutcome::Confirmed { revision: 4, result: None, row_id: None });
            twin.commit()
        }
        "discarded" => {
            twin.cancel(KEY);
            twin.commit()
        }
        "failed" => {
            let id = twin.state().open_id(KEY).expect("the transfer");
            twin.step_error(id, false);
            twin.commit()
        }
        _ => twin.end_epoch(),
    }
}

#[test]
fn xfer_prop_outcome() {
    for (name, mut twin, id, expected) in terminal_traces() {
        let (waiter, out) = twin.outcome(id, 1);
        assert!(out.replies(waiter).is_empty(), "{name}: the cycle runs");
        let out = finish(name, &mut twin);
        assert_eq!(out.replies(waiter), vec![TransferAnswer::Outcome(expected.clone(), None)], "{name}: one answer to the waiter");
        if name == "interrupted" {
            continue;
        }
        for _ in 0..2 {
            assert_eq!(outcome_of(&mut twin, id, 1), Some(TransferAnswer::Outcome(expected.clone(), None)), "{name}: the stored outcome never changes");
        }
    }
}

#[test]
fn xfer_prop_abort_rule() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    twin.step_ok(id, 1, Some("u1"));
    twin.commit();
    twin.commit();
    assert_eq!(twin.cancel(KEY).1.orphans_written().len(), 1, "cancel after a part leaves the upload to abort");
    let mut complete = Twin::new();
    let id = complete.completing(KEY);
    assert_eq!(complete.complete_error(id, false).aborts(), 0, "no abort after an error of complete");
    complete.commit();
    assert_eq!(complete.retry(KEY, None).1.aborts(), 0, "no abort on retry");
    let mut aborted = Twin::new();
    let id = aborted.uploading(KEY, 3);
    aborted.step_ok(id, 1, Some("u1"));
    aborted.commit();
    aborted.commit();
    assert_eq!(aborted.step_error(id, false).orphans_written().len(), 1);
    assert_eq!(aborted.commit().orphan_aborts().len(), 1);
    assert_eq!(aborted.discard(KEY).1.orphans_written().len(), 0, "an upload that was aborted is not aborted twice");
    let mut expired = Twin::new();
    let id = expired.uploading(KEY, 3);
    expired.step_ok(id, 1, Some("u1"));
    expired.reject(RejectReason::Invalid("bad row".into()));
    expired.commit();
    expired.det.tick(Time(8 * DAY));
    let out = expired.post(TransferInput::Tick { kind: TransferTick::Expire });
    assert_eq!(out.orphans_written().len(), 1, "the expiry of a failed upload");
    assert_eq!(expired.commit().orphan_aborts(), vec!["u1".to_string()]);
}

#[test]
fn xfer_prop_purpose() {
    let mut twin = Twin::new();
    let mut outs = vec![];
    let id = twin.uploading(KEY, 3);
    outs.push(twin.step_error(id, true));
    outs.push(twin.tick_retry(id));
    outs.push(twin.commit());
    outs.push(twin.step_ok(id, 1, Some("u1")));
    outs.push(twin.commit());
    outs.push(twin.commit());
    outs.push(twin.online(false));
    outs.push(twin.online(true));
    outs.push(twin.commit());
    let steps: Vec<NetworkPurpose> = outs.iter().flat_map(|out| out.runs.iter()).filter_map(|run| if let TransferEffect::Step { purpose, .. } = run { Some(*purpose) } else { None }).collect();
    assert!(steps.len() >= 3 && steps.iter().all(|purpose| *purpose == NetworkPurpose::Transfer), "{steps:?}");
}

#[test]
fn xfer_prop_epoch() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    twin.step_error(id, true);
    twin.end_epoch();
    let mut outs = vec![twin.online(false), twin.online(true), twin.post(TransferInput::SlotFreed), twin.post(TransferInput::PowerSaving(false))];
    outs.push(twin.tick_retry(id));
    outs.push(twin.post(TransferInput::Accept { epoch: EPOCH, item: Box::new(item(OTHER, 2)), reply: 7 }));
    for out in outs {
        assert_eq!((out.steps().len(), out.completes(), out.aborts(), out.copies(), out.writes()), (0, 0, 0, 0, 0), "no request after EpochEnded: {out:?}");
    }
}

#[test]
fn xfer_put_timeout_epoch_end() {
    let in_flight = || {
        let mut twin = Twin::new();
        let id = twin.uploading(KEY, 3);
        let attempt = twin.attempt(id);
        (twin, id, attempt)
    };
    let (mut twin, id, attempt) = in_flight();
    twin.end_epoch();
    let late = twin.put_timeout(id, attempt);
    assert_eq!((late.steps().len(), late.cancel_steps(), late.timers, late.writes(), late.all_replies()), (0, 0, 0, 0, 0), "the timeout after EpochEnded is dropped: {late:?}");
    let (mut twin, id, attempt) = in_flight();
    let first = twin.put_timeout(id, attempt);
    assert_eq!((first.cancel_steps(), first.timers, first.writes(), first.steps().len()), (1, 1, 0, 0), "the timeout before EpochEnded is exactly one StepError");
    twin.end_epoch();
    let retry = twin.tick_retry(id);
    assert_eq!((retry.steps().len(), retry.writes(), retry.timers), (0, 0, 0), "EpochEnded drops the retry");
}

#[test]
fn xfer_prop_round_covers() {
    let mut twin = Twin::new();
    let (id, _, _) = twin.accepting(KEY, 1);
    let mut covered = twin.commit().round_needed();
    twin.copy_done(id);
    twin.commit();
    twin.commit();
    twin.step_ok(id, 1, None);
    twin.commit();
    twin.commit();
    twin.completed(id);
    twin.commit();
    assert_eq!(covered.remove(0), vec![id], "the round is asked for at the commit of the acceptance");
    let idle = twin.mutation_outcome(KEY, OutboxOutcome::Confirmed { revision: 1, result: None, row_id: None });
    assert!(idle.round_idle().is_empty(), "a transfer in Mutating keeps its round");
    let out = twin.commit();
    assert_eq!(out.round_idle(), vec![id], "the round is released at Done");
    let (_open, out) = Twin::opened(vec![stored(1, KEY, 2), stored(2, OTHER, 2)], Default::default());
    assert_eq!(out.round_needed(), vec![vec![1], vec![2]], "a restored transfer is covered from Restored");
}

#[test]
fn xfer_prop_order() {
    for seed in 0..12u64 {
        let mut twin = Twin::with_slots(2);
        let keys = [KEY, OTHER, THIRD, "Moment:r4:upload"];
        let ids: Vec<TransferId> = keys.iter().map(|key| {
            let id = twin.accepted(key, 2);
            if twin.stage(id) == TransferStage::PersistingAttempt {
                twin.commit();
            }
            id
        }).collect();
        let mut state = seed + 1;
        for _ in 0..40 {
            if twin.has_pending() {
                twin.commit();
                assert!(twin.state().active() <= 2, "seed {seed}: at most FileSlots attempts in the network");
                continue;
            }
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let pick = ids[(state >> 33) as usize % ids.len()];
            if twin.t(pick).in_flight && twin.stage(pick) == TransferStage::Uploading {
                if (state >> 20) % 2 == 0 {
                    twin.step_ok(pick, twin.t(pick).confirmed_parts + 1, Some("u"));
                    twin.commit();
                } else {
                    twin.step_error(pick, true);
                    twin.tick_retry(pick);
                }
            } else if twin.t(pick).in_flight && twin.stage(pick) == TransferStage::Completing {
                twin.completed(pick);
                twin.commit();
            }
            assert!(twin.state().active() <= 2, "seed {seed}: at most FileSlots attempts in the network");
            let waiting = twin.state().transfers.values().any(|transfer| transfer.stage == TransferStage::Ready);
            assert!(!waiting || twin.state().active() == 2, "seed {seed}: a free slot is not left to a waiting transfer");
        }
    }
}

#[test]
fn xfer_prop_idle() {
    let mut twin = Twin::offline();
    twin.accepted(KEY, 2);
    for out in [twin.online(false), twin.post(TransferInput::SlotFreed), twin.post(TransferInput::PowerSaving(true))] {
        assert!(Twin::quiet(&out), "no network, no request, no timer: {out:?}");
    }
    let mut done = Twin::new();
    let id = done.mutating(KEY);
    done.mutation_outcome(KEY, OutboxOutcome::Confirmed { revision: 1, result: None, row_id: None });
    done.commit();
    assert_eq!(done.stage(id), TransferStage::Done);
    done.det.tick(Time(1));
    for out in [done.post(TransferInput::SlotFreed), done.online(true), done.post(TransferInput::Tick { kind: TransferTick::Expire })] {
        assert!(Twin::quiet(&out), "no transfer, nothing to do: {out:?}");
    }
}

#[test]
fn xfer_prop_file_kept() {
    let mut closing = Twin::new();
    closing.uploading(KEY, 2);
    assert_eq!(closing.close().deletes(), 0, "Closing keeps the copy");
    let mut ended = Twin::new();
    ended.uploading(KEY, 2);
    assert_eq!(ended.end_epoch().deletes(), 0, "EpochEnded keeps the copy");
    let mut refused = Twin::offline();
    refused.accepted(KEY, 2);
    refused.cancel(KEY);
    assert_eq!(refused.reject(RejectReason::Storage("disk".into())).deletes(), 0, "a refused removal keeps the copy");
    let mut removed = Twin::offline();
    removed.accepted(KEY, 2);
    let (_, before) = removed.cancel(KEY);
    assert_eq!(before.deletes(), 0, "the file goes only after the write");
    assert_eq!(removed.commit().deletes(), 1);
}

#[test]
fn xfer_prop_batch_all_or_none() {
    let count = |twin: &Twin| twin.state().transfers.values().filter(|transfer| transfer.batch_id.is_some() && transfer.stage != TransferStage::Done).count();
    let keys = [KEY, OTHER, THIRD];
    let mut ok = Twin::offline();
    let ids = ok.accepted_batch(&keys, 2);
    assert_eq!((count(&ok), ids.iter().all(|id| ok.stage(*id) == TransferStage::Ready)), (3, true));
    let mut rejected = Twin::new();
    rejected.post(TransferInput::AcceptBatch { joint: Box::new(joint()), epoch: EPOCH, items: keys.iter().map(|key| item(key, 2)).collect(), reply: 5 });
    let ids: Vec<_> = keys.iter().map(|key| rejected.state().open_id(key).unwrap()).collect();
    rejected.reserve_batch(&ids);
    rejected.reject(RejectReason::Storage("disk".into()));
    assert_eq!(count(&rejected), 0);
    let mut failed = Twin::new();
    failed.post(TransferInput::AcceptBatch { joint: Box::new(joint()), epoch: EPOCH, items: keys.iter().map(|key| item(key, 2)).collect(), reply: 5 });
    let ids: Vec<_> = keys.iter().map(|key| failed.state().open_id(key).unwrap()).collect();
    failed.reserve_batch(&ids);
    failed.commit();
    let id = failed.state().open_id(KEY).expect("an element");
    failed.copy_failed(id);
    failed.commit();
    assert_eq!(count(&failed), 0);
    let mut running = Twin::with_slots(3);
    let ids = running.batch_mutating(&keys);
    let (reply, out) = running.cancel(KEY);
    assert_eq!((Twin::fault(&out, reply), running.stage(ids[0])), (Some(Fault::InFlight), TransferStage::Mutating), "an element of a batch whose mutation left is not cancelled alone");
}

#[test]
fn xfer_prop_accept_durable() {
    let mut twin = Twin::offline();
    let (id, reply, out) = twin.accepting(KEY, 2);
    assert!(out.replies(reply).is_empty());
    let out = twin.commit();
    assert!(out.replies(reply).is_empty(), "the copy is not durable yet");
    let out = twin.copy_done(id);
    assert!(out.replies(reply).is_empty(), "the write of the copy is in flight");
    let out = twin.commit();
    assert_eq!(out.replies(reply).len(), 1, "the receipt comes with the write of the copy");
    let mut row = stored(1, KEY, 2);
    row.copying = true;
    row.size = 0;
    let (_, out) = Twin::opened(vec![row], Default::default());
    assert_eq!((out.copies(), out.all_replies()), (1, 0), "a row of `copying` goes on without a receipt");
}

#[test]
fn xfer_prop_observed_after_commit() {
    let mut twin = Twin::new();
    let (id, _, out) = twin.accepting(KEY, 3);
    assert_eq!((out.shows().len(), out.all_replies()), (0, 0), "accept");
    twin.commit();
    let out = twin.copy_done(id);
    assert_eq!((out.shows().len(), out.all_replies()), (0, 0), "copy");
    twin.commit();
    twin.commit();
    let out = twin.step_ok(id, 1, Some("u1"));
    assert_eq!(out.shows().len(), 0, "progress");
    let out = twin.commit();
    assert!(out.shows().contains(&Shown::Uploading { progress: 33 }));
    twin.commit();
    let out = twin.step_error(id, false);
    assert_eq!(out.shows().len(), 0, "failed");
    assert_eq!(twin.commit().shows(), vec![Shown::Failed { error: super::transfer_harness::refusal() }]);
    let (cancel, out) = twin.cancel(KEY);
    assert_eq!(out.replies(cancel).len(), 0, "cancel");
    assert_eq!(twin.commit().replies(cancel).len(), 1);
}

#[test]
fn xfer_prop_intent_after_complete() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 1);
    let mut submits = twin.step_ok(id, 1, None).submits();
    submits += twin.commit().submits();
    submits += twin.commit().submits();
    submits += twin.completed(id).submits();
    assert_eq!(submits, 0, "no mutation before the write of `completed`");
    assert_eq!(twin.commit().submits(), 1);
    let (_, out) = Twin::opened(vec![stored(1, KEY, 2), stored(2, OTHER, 2)], Default::default());
    assert_eq!(out.submits(), 0, "after a restart the unfinished file sends no mutation");
}

#[test]
fn xfer_prop_provenance() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    let old_attempt = twin.attempt(id);
    twin.cancel(KEY);
    twin.commit();
    let fresh = twin.uploading(KEY, 3);
    assert_ne!(fresh, id);
    let before = (twin.stage(fresh), twin.attempt(fresh));
    for input in [
        TransferInput::StepOk { transfer_id: id, attempt_id: old_attempt, part: Some(1), upload_id: Some("u9".into()), etag: None },
        TransferInput::StepError { transfer_id: id, attempt_id: old_attempt, temporary: false, error: super::transfer_harness::refusal() },
        TransferInput::StepOk { transfer_id: fresh, attempt_id: old_attempt, part: Some(1), upload_id: None, etag: None },
        TransferInput::Completed { transfer_id: fresh, attempt_id: old_attempt, signed_id: None },
        TransferInput::CopyDone { transfer_id: fresh, attempt_id: old_attempt, size: 1, sha256: "x".into(), place: Location::Directory, cached: crate::owners::write::transfer::CachedBlob::Miss },
    ] {
        let out = twin.post(input);
        assert!(Twin::quiet(&out), "{out:?}");
        assert_eq!((twin.stage(fresh), twin.attempt(fresh)), before);
    }
}

#[test]
fn xfer_prop_batch_after_all_copies() {
    let mut twin = Twin::new();
    let reply = twin.reply();
    twin.post(TransferInput::AcceptBatch { joint: Box::new(joint()), epoch: EPOCH, items: vec![item(KEY, 2), item(OTHER, 2)], reply });
    let ids = [twin.state().open_id(KEY).expect("first"), twin.state().open_id(OTHER).expect("second")];
    twin.reserve_batch(&ids);
    twin.commit();
    let first = twin.copy_done(ids[0]);
    assert_eq!((first.all_replies(), first.steps().len(), first.writes()), (0, 0, 0), "one copy is not enough");
    let second = twin.copy_done(ids[1]);
    assert_eq!((second.all_replies(), second.steps().len(), second.writes()), (0, 0, 1), "one common write, no receipt and no request before it");
    let out = twin.commit();
    assert_eq!((out.all_replies(), out.steps().len()), (1, 0));
    assert_eq!(twin.commit().steps().len(), 1);
    let mut failing = Twin::new();
    failing.post(TransferInput::AcceptBatch { joint: Box::new(joint()), epoch: EPOCH, items: vec![item(KEY, 2), item(OTHER, 2)], reply: 9 });
    let ids = [failing.state().open_id(KEY).unwrap(), failing.state().open_id(OTHER).unwrap()];
    failing.reserve_batch(&ids);
    failing.commit();
    let id = failing.state().open_id(KEY).expect("first");
    let out = failing.copy_failed(id);
    assert_eq!((out.removed().len(), out.all_replies()), (2, 0), "the metadata of the whole batch goes before the answer");
}

#[test]
fn xfer_prop_saved_outcome() {
    let cases: Vec<(OutboxOutcome, TransferStage)> = vec![
        (OutboxOutcome::Confirmed { revision: 2, result: None, row_id: None }, TransferStage::Removing),
        (OutboxOutcome::Discarded, TransferStage::Removing),
        (OutboxOutcome::Rejected { error: "e".into() }, TransferStage::PersistingFailure),
        (OutboxOutcome::Failed { error: "e".into() }, TransferStage::PersistingFailure),
    ];
    for (outcome, stage) in cases {
        let mut row = stored(1, KEY, 2);
        row.confirmed_parts = 2;
        row.completed = true;
        let (twin, out) = Twin::opened(vec![row], BTreeMap::from([(1, outcome.clone())]));
        assert_eq!((twin.stage(1), out.submits()), (stage, 0), "{outcome:?}: no completed transfer without intent and outcome stays in Mutating");
    }
    let mut row = stored(1, KEY, 2);
    row.completed = true;
    let (twin, out) = Twin::opened(vec![row], Default::default());
    assert_eq!((twin.stage(1), out.submits()), (TransferStage::Failed, 0));
}

#[test]
fn xfer_prop_status_subscribers() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    let first = twin.post(TransferInput::StatusInterestPlus { key: KEY.into(), stream_id: 1 });
    let second = twin.post(TransferInput::StatusInterestPlus { key: KEY.into(), stream_id: 2 });
    assert_eq!((first.statuses().len(), second.statuses().len()), (1, 1), "each interest gets the current value");
    twin.post(TransferInput::StatusInterestMinus { key: KEY.into(), stream_id: 1 });
    twin.step_ok(id, 1, None);
    let out = twin.commit();
    assert!(out.statuses().iter().all(|(stream, _)| *stream == 2) && !out.statuses().is_empty());
    twin.commit();
    twin.post(TransferInput::StatusInterestMinus { key: KEY.into(), stream_id: 2 });
    twin.step_ok(id, 2, None);
    assert!(twin.commit().statuses().is_empty(), "no values after the last id is taken off");
}

#[test]
fn xfer_prop_persist_before_step() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 2);
    let out = twin.step_ok(id, 1, Some("u1"));
    assert_eq!((out.steps().len(), out.completes()), (0, 0), "the next step waits for the write of the part");
    assert_eq!(twin.commit().steps().len(), 0);
    assert_eq!(twin.commit().steps().len(), 1);
    twin.step_ok(id, 2, None);
    let out = twin.commit();
    assert_eq!(out.completes(), 0);
    assert_eq!(twin.commit().completes(), 1);
    let out = twin.completed(id);
    assert_eq!(out.submits(), 0);
    assert_eq!(twin.commit().submits(), 1, "the mutation goes after the write of `completed`");
    let mut failed = Twin::new();
    failed.failed(KEY);
    let (_, out) = failed.retry(KEY, None);
    assert_eq!(out.steps().len(), 0, "the repeat starts after the write of `Retrying`");
    assert_eq!(failed.commit().steps().len(), 0);
    assert_eq!(failed.commit().steps().len(), 1);
}

fn storage() -> RejectReason { RejectReason::Storage("disk".into()) }

/// Items 5 and 6 of the release: any number of `Storage` refusals in a row, the network gone and back between them, never fail the transfer, never send a confirmed part or `complete` a second time.
#[test]
fn xfer_prop_storage_refusal_never_fails_an_upload() {
    for refusals in 1..=5 {
        let mut part = Twin::new();
        let id = part.uploading(KEY, 3);
        let mut seen = part.step_ok(id, 1, Some("u1"));
        let mut complete = Twin::new();
        let complete_id = complete.completing(KEY);
        let mut seen_complete = complete.completed(complete_id);
        for (name, twin, id, stage, seen) in [("part", &mut part, id, TransferStage::PersistingPart, &mut seen), ("complete", &mut complete, complete_id, TransferStage::PersistingComplete, &mut seen_complete)] {
            let written = seen.batches[0].ops.clone();
            let mut sent = Vec::new();
            for round in 1..=refusals {
                let refused = twin.reject(storage());
                let flap = [twin.online(false), twin.online(true), twin.tick_retry(id)];
                assert_eq!((twin.stage(id), refused.all_replies(), refused.kinds().len()), (stage, 0, 0), "{name} {round}/{refusals}");
                assert_eq!(flap[2].batches[0].ops, written, "{name} {round}/{refusals}: the same write goes again");
                for out in [&refused, &flap[0], &flap[1], &flap[2]] {
                    sent.extend(out.steps());
                    assert_eq!((out.completes(), out.aborts(), out.submits(), out.shows().len()), (0, 0, 0, 0), "{name} {round}/{refusals}");
                }
            }
            assert!(sent.is_empty(), "{name} {refusals}: the network sees nothing while the write is refused: {sent:?}");
            let committed = twin.commit();
            if name == "part" {
                assert_eq!(twin.stage(id), TransferStage::PersistingAttempt, "{refusals}");
                assert_eq!(twin.commit().steps().iter().map(|(part, _)| *part).collect::<Vec<_>>(), vec![Some(2)], "{refusals}: part 1 is not sent twice");
            } else {
                assert_eq!((twin.stage(id), committed.submits(), committed.completes()), (TransferStage::Mutating, 1, 0), "{refusals}: the mutation goes once");
            }
        }
    }
}

/// Item 7 of the release: the outcome of the server survives any number of `Storage` refusals of the removal, and the server hears nothing again.
#[test]
fn xfer_prop_confirmed_outcome_survives_store_refusal() {
    for refusals in 1..=5 {
        for confirmed in [true, false] {
            let mut twin = Twin::new();
            let id = twin.mutating(KEY);
            let waiter = if confirmed { None } else { Some(twin.discard(KEY).0) };
            let verdict = if confirmed { OutboxOutcome::Confirmed { revision: 7, result: None, row_id: None } } else { OutboxOutcome::Discarded };
            twin.mutation_outcome(KEY, verdict.clone());
            assert_eq!(twin.stage(id), TransferStage::Removing);
            for round in 1..=refusals {
                let refused = twin.reject(storage());
                let flap = [twin.online(false), twin.online(true), twin.tick_retry(id)];
                assert_eq!((twin.stage(id), refused.all_replies(), refused.outbox.len(), refused.deletes(), refused.shows().len()), (TransferStage::Removing, 0, 0, 0, 0), "{confirmed} {round}/{refusals}");
                assert_eq!((flap[2].writes(), flap[2].removed().len(), flap[2].submits(), flap[2].outbox_discards()), (1, 1, 0, 0), "{confirmed} {round}/{refusals}");
            }
            let done = twin.commit();
            assert_eq!(twin.stage(id), TransferStage::Done, "{confirmed} {refusals}");
            let (reply, out) = twin.outcome(id, 1);
            assert_eq!(out.replies(reply), vec![TransferAnswer::Outcome(verdict, None)], "{confirmed} {refusals}: not Failed");
            if let Some(waiter) = waiter { assert_eq!(done.replies(waiter), vec![TransferAnswer::Ok], "{refusals}"); }
        }
    }
}
