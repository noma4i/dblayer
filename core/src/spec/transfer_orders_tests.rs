//! Orders of the inputs of the owner `Transfer`: every ordering of the independent inputs of a trace, the outcome of each trace by the table.

use std::collections::{BTreeMap, BTreeSet};

use super::transfer_harness::*;
use crate::owners::contract::ReplyId;
use crate::owners::write::outbox::OutboxOutcome;
use crate::owners::write::transfer::{AbortFault, StoredTransfer, TransferAnswer, TransferEffect, TransferId, TransferInput, TransferStage, TransferTick};
use crate::runtime::Time;
use crate::test_wait::{orders_of, permutations};
use crate::store::files::Location;
use crate::transport::NetworkPurpose;

const KEY: &str = "Moment:r1:upload";
const OTHER: &str = "Moment:r2:upload";
const THIRD: &str = "Moment:r3:upload";

pub(super) fn commit_if_pending(twin: &mut Twin) -> Out { if twin.has_pending() { twin.commit() } else { Out::default() } }

pub(super) fn drain(twin: &mut Twin) -> Vec<Out> {
    let mut outs = Vec::new();
    while twin.has_pending() {
        outs.push(twin.commit());
    }
    outs
}

fn sequences(width: usize, depth: usize) -> Vec<Vec<usize>> {
    (0..depth).fold(vec![Vec::new()], |sequences, _| sequences.into_iter().flat_map(|sequence| (0..width).map(move |pick| { let mut next = sequence.clone(); next.push(pick); next })).collect())
}

/// P-XFER-01: the waiters of a cycle and the order of the finish, the write and the waiters give each waiter one answer, after the write.
#[test]
fn xfer_orders_outcome() {
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Ev { WaitFirst, WaitSecond, Finish, Commit }
    use Ev::*;
    let orders = orders_of(&[WaitFirst, WaitSecond, Finish, Commit], &[(Finish, Commit)]);
    assert_eq!(orders.len(), 12);
    let confirmed = OutboxOutcome::Confirmed { revision: 4, result: None, row_id: None };
    for name in ["confirmed", "discarded", "failed"] {
        for order in &orders {
            let (mut twin, id, expected) = match name {
                "confirmed" => {
                    let mut twin = Twin::new();
                    let id = twin.mutating(KEY);
                    (twin, id, confirmed.clone())
                }
                "discarded" => {
                    let mut twin = Twin::offline();
                    let id = twin.accepted(KEY, 2);
                    (twin, id, OutboxOutcome::Discarded)
                }
                _ => {
                    let mut twin = Twin::new();
                    let id = twin.uploading(KEY, 2);
                    (twin, id, OutboxOutcome::Failed { error: refusal() })
                }
            };
            let (mut waiters, mut got, mut committed) = (Vec::<ReplyId>::new(), Vec::<ReplyId>::new(), false);
            for event in order {
                let out = match event {
                    WaitFirst | WaitSecond => {
                        let (reply, out) = twin.outcome(id, 1);
                        waiters.push(reply);
                        out
                    }
                    Finish => match name {
                        "confirmed" => twin.mutation_outcome(KEY, confirmed.clone()),
                        "discarded" => twin.cancel(KEY).1,
                        _ => twin.step_error(id, false),
                    },
                    Commit => {
                        committed = true;
                        twin.commit()
                    }
                };
                for waiter in &waiters {
                    for answer in out.replies(*waiter) {
                        assert!(committed, "{name} {order:?}: an answer before the write of the outcome");
                        assert_eq!(answer, TransferAnswer::Outcome(expected.clone(), None), "{name} {order:?}");
                        got.push(*waiter);
                    }
                }
            }
            let mut sorted = got.clone();
            sorted.sort_unstable();
            assert_eq!(sorted, { let mut all = waiters.clone(); all.sort_unstable(); all }, "{name} {order:?}: one answer to each waiter");
            let (reply, out) = twin.outcome(id, 1);
            assert_eq!(out.replies(reply), vec![TransferAnswer::Outcome(expected.clone(), None)], "{name} {order:?}: the stored outcome stays");
        }
    }
}

/// P-XFER-02: the failure of a step, the removal, the network flaps and the write in any order leave one orphan and send its abort once, online only.
#[test]
fn xfer_orders_abort() {
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Ev { StepError, Cancel, Offline, Online, Commit }
    use Ev::*;
    let orders = orders_of(&[StepError, Cancel, Offline, Online, Commit], &[(Offline, Online)]);
    assert_eq!(orders.len(), 60);
    for order in orders {
        let mut twin = Twin::new();
        let id = twin.uploading(KEY, 3);
        twin.step_ok(id, 1, Some("u1"));
        twin.commit();
        twin.commit();
        let (mut online, mut outs) = (true, Vec::new());
        for event in &order {
            let out = match event {
                StepError if twin.state().transfers.contains_key(&id) => twin.step_error(id, false),
                StepError => Out::default(),
                Cancel => twin.cancel(KEY).1,
                Offline => {
                    online = false;
                    twin.online(false)
                }
                Online => {
                    online = true;
                    twin.online(true)
                }
                Commit => commit_if_pending(&mut twin),
            };
            assert!(online || out.orphan_aborts().is_empty(), "{order:?}: no abort offline");
            outs.push(out);
        }
        outs.extend(drain(&mut twin));
        let written: Vec<(String, String)> = outs.iter().flat_map(Out::orphans_written).collect();
        let aborts: Vec<String> = outs.iter().flat_map(Out::orphan_aborts).collect();
        let ended = twin.state().transfers.get(&id).is_none_or(|transfer| transfer.stage == TransferStage::Failed);
        assert_eq!(written.len(), usize::from(ended), "{order:?}: one orphan when the transfer ended, none while it goes on");
        assert!(written.iter().all(|orphan| *orphan == ("u1".to_string(), KEY.to_string())), "{order:?}");
        assert_eq!(aborts.len(), written.len(), "{order:?}: the abort of each orphan goes out once");
        assert_eq!(outs.iter().map(Out::completes).sum::<usize>(), 0, "{order:?}: no complete");
    }
}

/// P-XFER-03 and P-XFER-04: every step carries the purpose Transfer, and no request leaves after the end of the epoch, in any order of the signals.
#[test]
fn xfer_orders_epoch_purpose() {
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Ev { End, Offline, Online, Slot, Retry, Power, Commit }
    use Ev::*;
    let orders = orders_of(&[End, Offline, Online, Slot, Retry, Power, Commit], &[]);
    assert_eq!(orders.len(), 5040);
    let steps = std::sync::atomic::AtomicUsize::new(0);
    crate::test_wait::each_in_parallel(&orders, |order| {
        let mut twin = Twin::new();
        let id = twin.uploading(KEY, 3);
        twin.step_error(id, true);
        let mut ended = false;
        for event in order {
            let out = match event {
                End => {
                    ended = true;
                    twin.end_epoch()
                }
                Offline => twin.online(false),
                Online => twin.online(true),
                Slot => twin.post(TransferInput::SlotFreed),
                Retry => twin.tick_retry(id),
                Power => twin.post(TransferInput::PowerSaving(false)),
                Commit => commit_if_pending(&mut twin),
            };
            if ended && *event != End {
                assert_eq!((out.steps().len(), out.completes(), out.aborts(), out.copies(), out.writes()), (0, 0, 0, 0, 0), "{order:?} at {event:?}: no request after EpochEnded");
            }
            for run in &out.runs {
                if let TransferEffect::Step { purpose, .. } = run {
                    steps.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    assert_eq!(*purpose, NetworkPurpose::Transfer, "{order:?}");
                }
            }
        }
    });
    assert!(steps.into_inner() > 0, "the traces send steps");
}

/// P-XFER-06: the picks of the network (a part done, `complete` done, the write) in any sequence keep at most `FileSlots` attempts, never leave a slot to a waiting transfer, and start the waiting ones by their order.
#[test]
fn xfer_orders_start_order() {
    let sequences = sequences(3, 5);
    assert_eq!(sequences.len(), 243);
    for sequence in sequences {
        let mut twin = Twin::with_slots(2);
        let ids: Vec<TransferId> = [KEY, OTHER, THIRD].iter().map(|key| {
            let id = twin.accepted(key, 2);
            if twin.stage(id) == TransferStage::PersistingAttempt {
                twin.commit();
            }
            id
        }).collect();
        for pick in &sequence {
            let id = ids[*pick];
            let ready: Vec<TransferId> = ids.iter().copied().filter(|found| twin.stage(*found) == TransferStage::Ready).collect();
            let transfer = &twin.state().transfers[&id];
            match (transfer.stage, transfer.in_flight) {
                (TransferStage::Uploading, true) => { twin.step_ok(id, transfer.confirmed_parts + 1, Some("u")); }
                (TransferStage::Completing, true) => { twin.completed(id); }
                _ => {}
            }
            let mut started: Vec<TransferId> = Vec::new();
            loop {
                for found in &ready {
                    if twin.state().transfers[found].in_flight && !started.contains(found) {
                        started.push(*found);
                    }
                }
                assert!(twin.state().active() <= 2, "{sequence:?}: at most FileSlots attempts in the network");
                if !twin.has_pending() {
                    break;
                }
                twin.commit();
            }
            assert_eq!(started, ready[..started.len()].to_vec(), "{sequence:?}: the waiting ones start by their order");
            let waiting = ids.iter().any(|found| twin.stage(*found) == TransferStage::Ready);
            assert!(!waiting || twin.state().active() == 2, "{sequence:?}: a free slot is not left to a waiting transfer");
        }
    }
}

/// P-XFER-07: a quiet owner stays quiet under any order of the signals that need no work.
#[test]
fn xfer_orders_idle() {
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Ev { Offline, Slot, Power, Expire, Retry }
    use Ev::*;
    let orders = orders_of(&[Offline, Slot, Power, Expire, Retry], &[]);
    assert_eq!(orders.len(), 120);
    for order in orders {
        let mut twin = Twin::offline();
        let id = twin.accepted(KEY, 2);
        for event in &order {
            let out = match event {
                Offline => twin.online(false),
                Slot => twin.post(TransferInput::SlotFreed),
                Power => twin.post(TransferInput::PowerSaving(true)),
                Expire => { twin.det.tick(Time(1)); twin.post(TransferInput::Tick { kind: TransferTick::Expire }) },
                Retry => twin.tick_retry(id),
            };
            assert!(Twin::quiet(&out), "{order:?} at {event:?}: no network, no request, no timer: {out:?}");
        }
    }
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Done { Slot, Online, Expire }
    for order in orders_of(&[Done::Slot, Done::Online, Done::Expire], &[]) {
        let mut twin = Twin::new();
        let id = twin.mutating(KEY);
        twin.mutation_outcome(KEY, OutboxOutcome::Confirmed { revision: 1, result: None, row_id: None });
        twin.commit();
        assert_eq!(twin.stage(id), TransferStage::Done);
        for event in &order {
            let out = match event {
                Done::Slot => twin.post(TransferInput::SlotFreed),
                Done::Online => twin.online(true),
                Done::Expire => { twin.det.tick(Time(1)); twin.post(TransferInput::Tick { kind: TransferTick::Expire }) },
            };
            assert!(Twin::quiet(&out), "{order:?} at {event:?}: nothing to do: {out:?}");
        }
    }
}

/// P-XFER-08: the copy goes only with the write of the removal, whatever the order of the removal, the write, the end of the epoch and the close.
#[test]
fn xfer_orders_file_kept() {
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Ev { Cancel, Commit, End, Close }
    use Ev::*;
    let orders = orders_of(&[Cancel, Commit, End, Close], &[(Cancel, Commit)]);
    assert_eq!(orders.len(), 12);
    for order in orders {
        let mut twin = Twin::offline();
        twin.accepted(KEY, 2);
        let mut deletes = 0;
        for event in &order {
            let out = match event {
                Cancel => twin.cancel(KEY).1,
                Commit => commit_if_pending(&mut twin),
                End => twin.end_epoch(),
                Close => twin.close(),
            };
            assert!(*event == Commit || out.deletes() == 0, "{order:?} at {event:?}: the file goes only after the write");
            deletes += out.deletes();
        }
        assert!(deletes <= 1, "{order:?}");
        let at = |wanted: Ev| order.iter().position(|found| *found == wanted).unwrap();
        if at(Commit) < at(End) && at(Commit) < at(Close) {
            assert_eq!(deletes, 1, "{order:?}: the removal was written");
        }
    }
}

/// P-XFER-10: the signals of the network and the result of the copy in any order give the receipt once, with the write of the copy.
#[test]
fn xfer_orders_accept_durable() {
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Ev { CopyDone, Commit, Offline, Online, Slot, Power }
    use Ev::*;
    let orders = orders_of(&[CopyDone, Commit, Offline, Online, Slot, Power], &[(CopyDone, Commit)]);
    assert_eq!(orders.len(), 360);
    for order in orders {
        let mut twin = Twin::offline();
        let (id, reply, _) = twin.accepting(KEY, 2);
        twin.commit();
        let mut receipts = 0;
        let mut written = false;
        for event in &order {
            let out = match event {
                CopyDone => twin.copy_done(id),
                Commit => {
                    written = true;
                    twin.commit()
                }
                Offline => twin.online(false),
                Online => twin.online(true),
                Slot => twin.post(TransferInput::SlotFreed),
                Power => twin.post(TransferInput::PowerSaving(false)),
            };
            let answers = out.replies(reply);
            assert!(written || answers.is_empty(), "{order:?} at {event:?}: the receipt waits for the write of the copy");
            assert!(answers.iter().all(|answer| matches!(answer, TransferAnswer::Receipt(_))), "{order:?}");
            receipts += answers.len();
        }
        assert_eq!(receipts, 1, "{order:?}: one receipt");
    }
}

/// P-XFER-13: the value of a stream and the success of `cancel` appear only in the answer of the write that holds them, in any order of the inputs.
#[test]
fn xfer_orders_observed_after_commit() {
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Ev { Plus, Progress, Cancel, FirstCommit, SecondCommit }
    use Ev::*;
    let orders = orders_of(&[Plus, Progress, Cancel, FirstCommit, SecondCommit], &[(FirstCommit, SecondCommit)]);
    assert_eq!(orders.len(), 60);
    let mut shown = 0;
    for order in orders {
        let mut twin = Twin::new();
        let id = twin.uploading(KEY, 3);
        let cancel_reply = twin.reply();
        for event in &order {
            let out = match event {
                Plus => {
                    let out = twin.post(TransferInput::StatusInterestPlus { key: KEY.into(), stream_id: 1 });
                    assert_eq!(out.statuses().len(), 1, "{order:?}: the current value to a new interest");
                    out
                }
                Progress if twin.state().transfers.contains_key(&id) => {
                    let part = twin.t(id).confirmed_parts + 1;
                    twin.step_ok(id, part, Some("u1"))
                }
                Progress => Out::default(),
                Cancel => twin.post(TransferInput::Cancel { key: KEY.into(), reply: cancel_reply }),
                FirstCommit | SecondCommit => {
                    let out = commit_if_pending(&mut twin);
                    shown += out.shows().len() + out.statuses().len();
                    continue;
                }
            };
            assert!(out.shows().is_empty() && (*event == Plus || out.statuses().is_empty()), "{order:?} at {event:?}: no value before the write");
            assert!(out.replies(cancel_reply).iter().all(|answer| matches!(answer, TransferAnswer::Err(_))), "{order:?} at {event:?}: no success before the write");
        }
    }
    assert!(shown > 0, "the writes show values");
}

/// P-XFER-12 and P-XFER-14: the results of the network and the writes in any order start the next step and the mutation only after the write of the previous result.
#[test]
fn xfer_orders_persist_before_step() {
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Ev { StepOk, Completed, First, Second, Third }
    use Ev::*;
    let orders = orders_of(&[StepOk, Completed, First, Second, Third], &[(First, Second), (Second, Third)]);
    assert_eq!(orders.len(), 20);
    for order in orders {
        let mut twin = Twin::new();
        let id = twin.uploading(KEY, 1);
        let mut accepted_at = None;
        let mut submits = 0;
        for (index, event) in order.iter().enumerate() {
            let out = match event {
                StepOk => twin.step_ok(id, 1, None),
                Completed => {
                    let out = twin.completed(id);
                    if twin.stage(id) == TransferStage::PersistingComplete {
                        accepted_at = Some(index);
                    }
                    out
                }
                First | Second | Third => commit_if_pending(&mut twin),
            };
            if matches!(event, StepOk | Completed) {
                assert_eq!((out.steps().len(), out.completes(), out.submits()), (0, 0, 0), "{order:?} at {event:?}: the next step waits for the write");
            }
            if out.submits() > 0 {
                assert!(accepted_at.is_some_and(|at| at < index), "{order:?}: the mutation only after the write of `completed`");
            }
            submits += out.submits();
        }
        let written = accepted_at.is_some_and(|at| order[at..].iter().any(|event| matches!(event, First | Second | Third)));
        assert_eq!(submits, usize::from(written), "{order:?}");
    }
}

/// P-XFER-11: results with a stale transfer or attempt id change nothing, in any order with the real result of the fresh transfer.
#[test]
fn xfer_orders_provenance() {
    use crate::owners::write::transfer::CachedBlob;
    let orders = permutations(&[0usize, 1, 2, 3, 4]);
    assert_eq!(orders.len(), 120);
    for order in orders {
        let mut twin = Twin::new();
        let id = twin.uploading(KEY, 3);
        let old_attempt = twin.attempt(id);
        twin.cancel(KEY);
        twin.commit();
        let fresh = twin.uploading(KEY, 3);
        assert_ne!(fresh, id);
        for event in &order {
            let input = match event {
                0 => TransferInput::StepOk { transfer_id: id, attempt_id: old_attempt, part: Some(1), upload_id: Some("u9".into()), etag: None },
                1 => TransferInput::StepError { transfer_id: id, attempt_id: old_attempt, temporary: false, error: refusal() },
                2 => TransferInput::StepOk { transfer_id: fresh, attempt_id: old_attempt, part: Some(1), upload_id: None, etag: None },
                3 => TransferInput::Completed { transfer_id: fresh, attempt_id: old_attempt, signed_id: None },
                _ => TransferInput::CopyDone { transfer_id: fresh, attempt_id: old_attempt, size: 1, sha256: "x".into(), place: Location::Directory, cached: CachedBlob::Miss },
            };
            let out = twin.post(input);
            assert!(Twin::quiet(&out), "{order:?} at {event}: {out:?}");
        }
        let real = twin.step_ok(fresh, 1, Some("u1"));
        assert_eq!(real.writes(), 1, "{order:?}: the fresh result is taken after the stale ones");
        twin.commit();
        assert_eq!(twin.t(fresh).confirmed_parts, 1, "{order:?}");
    }
}

/// P-XFER-17: the saved outcome decides the stage of the restored transfer, with no resend, in every order of the signals of the opening.
#[test]
fn xfer_orders_saved_outcome() {
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Ev { Online, Offline, Restored, RestoreReady, Register }
    use Ev::*;
    let orders = orders_of(&[Online, Offline, Restored, RestoreReady, Register], &[(Restored, RestoreReady)]);
    assert_eq!(orders.len(), 60);
    let cases: Vec<(Option<OutboxOutcome>, TransferStage)> = vec![
        (Some(OutboxOutcome::Confirmed { revision: 2, result: None, row_id: None }), TransferStage::Removing),
        (Some(OutboxOutcome::Discarded), TransferStage::Removing),
        (Some(OutboxOutcome::Rejected { error: "e".into() }), TransferStage::PersistingFailure),
        (Some(OutboxOutcome::Failed { error: "e".into() }), TransferStage::PersistingFailure),
        (None, TransferStage::Failed),
    ];
    for (outcome, stage) in cases {
        for order in &orders {
            let mut row: StoredTransfer = stored(1, KEY, 2);
            row.confirmed_parts = 2;
            row.completed = true;
            let outcomes: BTreeMap<TransferId, BTreeMap<u64, OutboxOutcome>> = outcome.iter().map(|found| (1, BTreeMap::from([(1, found.clone())]))).collect();
            let mut twin = Twin::bare(1);
            let mut resent = 0;
            for event in order.iter().copied().chain([Online]) {
                let out = match event {
                    Online => twin.online(true),
                    Offline => twin.online(false),
                    Restored => twin.post(TransferInput::Restored { uploads: vec![row.clone()], outcomes: outcomes.clone(), orphans: Vec::new(), batches: Default::default(), held_orphan: None }),
                    RestoreReady => twin.post(TransferInput::RestoreReady { epoch: EPOCH }),
                    Register => twin.post(TransferInput::RegisterOperations { declarations: vec![("Moment".into(), "upload".into())], complete: false }),
                };
                resent += out.submits() + out.steps().len() + out.completes();
            }
            assert_eq!((twin.stage(1), resent), (stage, 0), "{outcome:?} {order:?}: the saved outcome decides, nothing is sent again");
        }
    }
}

/// P-XFER-18: interests and progress in any order: a value goes to every live id and to none that was taken off.
#[test]
fn xfer_orders_status_subscribers() {
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Ev { PlusFirst, PlusSecond, MinusFirst, MinusSecond, FirstProgress, SecondProgress }
    use Ev::*;
    let orders = orders_of(&[PlusFirst, PlusSecond, MinusFirst, MinusSecond, FirstProgress, SecondProgress], &[(PlusFirst, MinusFirst), (PlusSecond, MinusSecond), (FirstProgress, SecondProgress)]);
    assert_eq!(orders.len(), 90);
    for order in orders {
        let mut twin = Twin::new();
        let id = twin.uploading(KEY, 4);
        let mut live: BTreeSet<u64> = BTreeSet::new();
        for event in &order {
            match event {
                PlusFirst | PlusSecond => {
                    let stream = if *event == PlusFirst { 1 } else { 2 };
                    let out = twin.post(TransferInput::StatusInterestPlus { key: KEY.into(), stream_id: stream });
                    assert_eq!(out.statuses().iter().map(|(found, _)| *found).collect::<Vec<_>>(), vec![stream], "{order:?}: the current value to the new id");
                    live.insert(stream);
                }
                MinusFirst | MinusSecond => {
                    let stream = if *event == MinusFirst { 1 } else { 2 };
                    let out = twin.post(TransferInput::StatusInterestMinus { key: KEY.into(), stream_id: stream });
                    assert!(out.statuses().is_empty(), "{order:?}");
                    live.remove(&stream);
                }
                FirstProgress | SecondProgress => {
                    let part = twin.t(id).confirmed_parts + 1;
                    let mut outs = vec![twin.step_ok(id, part, Some("u1"))];
                    outs.extend(drain(&mut twin));
                    let reached: BTreeSet<u64> = outs.iter().flat_map(|out| out.statuses().into_iter().map(|(stream, _)| stream)).collect();
                    assert_eq!(reached, live, "{order:?} at {event:?}: a value to every live id and to no other");
                }
            }
        }
    }
}

/// P-XFER-33: the removal, the failure of the abort, the network flaps and the answers of the abort in any order keep the orphan until the server answers, and send the abort only online.
#[test]
fn xfer_orders_orphan_abort() {
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Ev { Discard, Offline, Online, Lost, Answered }
    use Ev::*;
    let orders = orders_of(&[Discard, Offline, Online, Lost, Answered], &[(Offline, Online), (Discard, Lost), (Lost, Answered)]);
    assert!(!orders.is_empty());
    for order in orders {
        let mut twin = Twin::new();
        let id = twin.uploading(KEY, 3);
        twin.step_ok(id, 1, Some("u1"));
        twin.commit();
        twin.commit();
        let (mut online, mut sent, mut removed, mut answered) = (true, 0, 0, false);
        for event in &order {
            let out = match event {
                Discard => {
                    let out = twin.discard(KEY).1;
                    assert_eq!(out.orphans_written().len(), 1, "{order:?}: the orphan goes with the removal");
                    let mut all = vec![out];
                    all.extend(drain(&mut twin));
                    sent += all.iter().map(|found| found.orphan_aborts().len()).sum::<usize>();
                    assert!(online || sent == 0, "{order:?}: no abort offline");
                    Out::default()
                }
                Offline => {
                    online = false;
                    twin.online(false)
                }
                Online => {
                    online = true;
                    twin.online(true)
                }
                Lost | Answered if sent == 0 => Out::default(),
                Lost => twin.post(TransferInput::AbortDone { upload_id: "u1".into(), result: Err(AbortFault { temporary: true, status: None }) }),
                Answered => {
                    answered = true;
                    twin.post(TransferInput::AbortDone { upload_id: "u1".into(), result: Ok(()) })
                }
            };
            assert!(online || out.orphan_aborts().is_empty(), "{order:?} at {event:?}: no abort offline");
            assert!(*event == Answered || out.orphans_removed().is_empty(), "{order:?} at {event:?}: the orphan stays until the server answers");
            let before = sent;
            sent += out.orphan_aborts().len();
            assert!(!answered || sent == before, "{order:?} at {event:?}: no abort after the server answered");
            removed += out.orphans_removed().len();
            drain(&mut twin);
        }
        assert!(sent >= 1, "{order:?}: the abort went out");
        assert_eq!(removed, usize::from(answered), "{order:?}");
    }
}

/// D-491 bounce: `Online(false)` and `Online(true)` of one step cancel the step in flight and send the same step again at once, no retry wait, no second step in flight.
#[test]
fn transfer_path_bounce_restarts_step() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    twin.step_ok(id, 1, Some("u1"));
    twin.commit();
    assert_eq!(twin.stage(id), TransferStage::Uploading);
    let part = twin.t(id).confirmed_parts + 1;
    let down = twin.online(false);
    assert_eq!((down.cancel_steps(), twin.stage(id)), (1, TransferStage::Ready), "the step in flight is cancelled");
    let up = twin.online(true);
    assert_eq!(twin.stage(id), TransferStage::PersistingAttempt, "the next attempt is written at once: {up:?}");
    let restarted = twin.commit();
    assert_eq!(restarted.steps().iter().map(|(step, _)| *step).collect::<Vec<_>>(), vec![Some(part)], "the same part goes out once");
    assert_eq!(twin.stage(id), TransferStage::Uploading);
    assert_eq!(twin.t(id).confirmed_parts + 1, part, "the confirmed parts did not move");
}
