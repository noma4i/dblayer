//! Owner-level tests of the `transfer` machine: the waits of the calls and the properties.

use crate::owners::contract::{RejectReason, Rejected};

use super::ref_transfer::{St};
use crate::owners::contract::{Online};
use crate::owners::write::outbox::{Fault, OutboxOutcome};
use crate::files::Location;
use crate::owners::support::{Emitted, Log, Silent};


use crate::owners::write::transfer::{self as t, RoundState, StoreKind, TransferAnswer, TransferEffect as Run, TransferInput, TransferStage, TransferTick, Transfers};
use crate::runtime::{Address, Cause, DetScheduler, EpochStamp, Time};
use crate::transport::NetworkPurpose;
use super::transfer_harness::*;

fn replies_of(effects: &[TransferEffect], reply: u64) -> Vec<TransferAnswer> {
    effects.iter().filter_map(|effect| match effect {
        TransferEffect::Reply { reply: id, answer } if *id == reply => Some(answer.clone()),
        _ => None,
    }).collect()
}

#[test]
fn xfer_wait_accept() {
    let mut twin = Twin::new();
    let effects = twin.post(TransferInput::Accept { epoch: EPOCH, key: KEY.into(), source: source(), batch: false, reply: 1, now: Time(0), mutation: mutation(), steps: steps() });
    assert!(matches!(replies_of(&effects, 1).as_slice(), [TransferAnswer::Receipt(_)]));
    let stale = twin.post(TransferInput::Accept { epoch: EPOCH + 1, key: "x".into(), source: source(), batch: false, reply: 2, now: Time(0), mutation: mutation(), steps: steps() });
    assert_eq!(replies_of(&stale, 2), [TransferAnswer::Err(Fault::NoSession)]);
    assert_eq!(twin.stage("x"), St::Done, "a stale accept leaves no transfer");
}

#[test]
fn xfer_wait_accept_batch() {
    let mut twin = Twin::new();
    let mut answered = Vec::new();
    for (index, key) in ["b1", "b2", "b3"].iter().enumerate() {
        let effects = twin.post(TransferInput::Accept { epoch: EPOCH, key: (*key).into(), source: source(), batch: true, reply: 10 + index as u64, now: Time(0), mutation: mutation(), steps: steps() });
        answered.push(replies_of(&effects, 10 + index as u64));
    }
    assert!(answered.iter().all(|answers| matches!(answers.as_slice(), [TransferAnswer::Receipt(_)])));
    assert!(twin.state().transfers.values().all(|transfer| transfer.batch));
    take_fx(&twin.log);
    twin.det.end_epoch(EPOCH, Cause::SessionReset);
    twin.run();
    assert!(["b1", "b2", "b3"].iter().all(|key| twin.stage(key) == St::Done), "accepted items end Interrupted with the epoch");
}

#[test]
fn xfer_wait_cancel() {
    let mut twin = reach(St::Ready, &ready());
    let ok = twin.post(TransferInput::Cancel { key: KEY.into(), reply: 21 });
    assert_eq!(replies_of(&ok, 21), [TransferAnswer::Cancelled(true)]);
    let mut twin = reach(St::Mutating, &ready());
    let late = twin.post(TransferInput::Cancel { key: KEY.into(), reply: 22 });
    assert_eq!(replies_of(&late, 22), [TransferAnswer::Cancelled(false)]);
    let mut twin = reach(St::Uploading, &batch());
    let blocked = twin.post(TransferInput::Cancel { key: KEY.into(), reply: 23 });
    assert_eq!(replies_of(&blocked, 23), [TransferAnswer::Err(Fault::InFlight)]);
    let none = Twin::new().post(TransferInput::Cancel { key: "none".into(), reply: 24 });
    assert_eq!(replies_of(&none, 24), [TransferAnswer::Err(Fault::NotFound)]);
}

#[test]
fn xfer_wait_retry() {
    let mut twin = reach(St::Failed, &ready());
    let ok = twin.post(TransferInput::Retry { key: KEY.into(), reply: 31 });
    assert!(matches!(replies_of(&ok, 31).as_slice(), [TransferAnswer::Receipt(_)]));
    let none = Twin::new().post(TransferInput::Retry { key: "none".into(), reply: 32 });
    assert_eq!(replies_of(&none, 32), [TransferAnswer::Err(Fault::NotFound)]);
}

#[test]
fn xfer_wait_discard() {
    let mut twin = reach(St::Failed, &ready());
    let ok = twin.post(TransferInput::Discard { key: KEY.into(), reply: 41 });
    assert_eq!(replies_of(&ok, 41), [TransferAnswer::Ok]);
    let none = Twin::new().post(TransferInput::Discard { key: "none".into(), reply: 42 });
    assert_eq!(replies_of(&none, 42), [TransferAnswer::Err(Fault::NotFound)]);
}

#[test]
fn xfer_wait_outcome() {
    let mut twin = reach(St::Mutating, &ready());
    let effects = twin.post(TransferInput::MutationOutcome { key: KEY.into(), outcome: OutboxOutcome::Confirmed { revision: 3 } });
    assert!(effects.iter().any(|effect| matches!(effect, TransferEffect::Shows { shown: t::Shown::Gone, .. })));
    let mut twin = reach(St::Mutating, &ready());
    let effects = twin.post(TransferInput::MutationOutcome { key: KEY.into(), outcome: OutboxOutcome::Failed { error: "e".into() } });
    assert!(effects.iter().any(|effect| matches!(effect, TransferEffect::Shows { shown: t::Shown::Failed { .. }, .. })));
}

// Properties: the explorer walks the orders of the inputs of three sources.

use crate::owners::support::{snapshot, Source as Feeder, SourceInput, SourceState};
use crate::runtime::{explore, Limits};
use std::cell::RefCell;

thread_local! { static LOG: RefCell<Option<Log<Emitted<Run>>>> = const { RefCell::new(None) }; }

fn scripted(sources: Vec<Vec<TransferInput>>, slots: u32) -> DetScheduler {
    let (mut det, log) = world(3, slots);
    LOG.with(|cell| *cell.borrow_mut() = Some(log.clone()));
    let addr = Address::<Transfers>::new(TRANSFER_AT);
    let feeders: Vec<_> = sources.into_iter().map(|script| det.add::<Feeder<Transfers>>("source", SourceState { to: addr, script }, Silent)).collect();
    det.start_epoch(EPOCH);
    for feeder in feeders {
        det.post(feeder, EpochStamp::Control, SourceInput::Go);
    }
    det
}

fn accepts(keys: &[&str]) -> Vec<TransferInput> {
    keys.iter().enumerate().map(|(index, key)| TransferInput::Accept { epoch: EPOCH, key: (*key).into(), source: source(), batch: false, reply: 100 + index as u64, now: Time(0), mutation: mutation(), steps: steps() }).collect()
}

fn copies(keys: &[&str]) -> Vec<TransferInput> { keys.iter().map(|key| TransferInput::CopyDone { key: (*key).into(), size: 1, sha256: "h".into(), place: Location::Directory }).collect() }

fn net(keys: &[&str]) -> Vec<TransferInput> {
    let mut script = Vec::new();
    for key in keys {
        script.push(TransferInput::StepOk { key: (*key).into(), part: Some(1), upload_id: None });
        script.push(TransferInput::StepError { key: (*key).into(), part: Some(1), temporary: false });
        script.push(TransferInput::Completed { key: (*key).into() });
    }
    script.push(TransferInput::Online(Online(false)));
    script.push(TransferInput::Online(Online(true)));
    script
}

fn walk(sources: impl Fn() -> Vec<Vec<TransferInput>>, slots: u32, check: impl FnMut(&DetScheduler) -> Result<(), String>) {
    let report = explore(|| scripted(sources(), slots), Limits { depth: 6, max_traces: 4_000 }, check);
    assert!(report.violations.is_empty(), "{:?}", report.violations.first());
    assert!(report.traces > 0);
}

fn effects_now() -> Vec<TransferEffect> { LOG.with(|cell| cell.borrow().as_ref().map(|log| snapshot(log).into_iter().filter_map(lower).collect()).unwrap_or_default()) }

#[test]
fn xfer_prop_outcome() {
    walk(|| vec![accepts(&["a", "b"]), copies(&["a", "b"]), net(&["a", "b"])], 1, |_| {
        let effects = effects_now();
        let receipts = effects.iter().filter(|effect| matches!(effect, TransferEffect::Reply { answer: TransferAnswer::Receipt(_), .. })).count();
        let gone = effects.iter().filter(|effect| matches!(effect, TransferEffect::Shows { shown: t::Shown::Gone, .. })).count();
        if gone > receipts { Err("a transfer ended that was never accepted".into()) } else { Ok(()) }
    });
}

#[test]
fn xfer_prop_abort_rule() {
    walk(|| vec![accepts(&["a"]), copies(&["a"]), net(&["a"])], 1, |_| {
        let aborts = effects_now().iter().filter(|effect| matches!(effect, TransferEffect::Abort { .. })).count();
        if aborts > 1 { Err("abort was called twice".into()) } else { Ok(()) }
    });
    let mut twin = reach(St::Failed, &all_parts());
    let effects = twin.post(TransferInput::Retry { key: KEY.into(), reply: 1 });
    assert!(effects.iter().all(|effect| !matches!(effect, TransferEffect::Abort { .. })), "retry after CompleteError does not abort");
}

#[test]
fn xfer_prop_purpose() {
    walk(|| vec![accepts(&["a", "b"]), copies(&["a", "b"]), net(&["a"])], 2, |_| {
        match effects_now().iter().find(|effect| matches!(effect, TransferEffect::Step { purpose, .. } if *purpose != NetworkPurpose::Transfer)) {
            Some(effect) => Err(format!("a step without the purpose Transfer: {effect:?}")),
            None => Ok(()),
        }
    });
}

#[test]
fn xfer_prop_epoch() {
    let mut det = scripted(vec![accepts(&["a"]), copies(&["a"])], 1);
    det.run_until_quiet(10_000).unwrap();
    det.end_epoch(EPOCH, Cause::SessionReset);
    det.run_until_quiet(10_000).unwrap();
    crate::owners::support::take(&LOG.with(|cell| cell.borrow().clone().unwrap()));
    for input in [TransferInput::Online(Online(true)), TransferInput::Tick { now: Time(1), kind: TransferTick::Retry { key: "a".into() } }, TransferInput::SlotFreed] {
        det.post(Address::<Transfers>::new(TRANSFER_AT), EpochStamp::Epoch(EPOCH), input);
    }
    det.run_until_quiet(10_000).unwrap();
    let after = effects_now();
    assert!(after.iter().all(|effect| !matches!(effect, TransferEffect::Step { .. } | TransferEffect::Complete { .. } | TransferEffect::Abort { .. })), "{after:?}");
}

#[test]
fn xfer_prop_round_covers() {
    walk(|| vec![accepts(&["a", "b"]), copies(&["a", "b"]), net(&["a"]), vec![TransferInput::RoundStarted { round_id: 7, epoch: EPOCH }, TransferInput::RoundEnded { round_id: 7, epoch: EPOCH }]], 1, |det| {
        let state = state_of(det)?;
        let open = state.transfers.values().any(|transfer| transfer.epoch == EPOCH && matches!(transfer.stage, TransferStage::Ready | TransferStage::Uploading | TransferStage::Completing | TransferStage::Mutating));
        match (open, state.round) {
            (true, RoundState::None) if !effects_now().iter().any(|effect| matches!(effect, TransferEffect::ScheduleJob { epoch: EPOCH })) => Err("an open transfer without a requested round".into()),
            _ => Ok(()),
        }
    });
}

#[test]
fn xfer_prop_order() {
    walk(|| vec![accepts(&["a", "b", "c"]), copies(&["a", "b", "c"]), net(&["a", "b", "c"])], 1, |det| {
        let state = state_of(det)?;
        let active = state.transfers.values().filter(|transfer| matches!(transfer.stage, TransferStage::Uploading | TransferStage::Completing)).count();
        if active > state.slots as usize { return Err(format!("{active} transfers hold {} slots", state.slots)); }
        let mut started: Vec<&str> = Vec::new();
        for effect in effects_now() {
            if let TransferEffect::Step { key, .. } = &effect {
                if !started.contains(&key.as_str()) { started.push(Box::leak(key.clone().into_boxed_str())); }
            }
        }
        let mut sorted = started.clone();
        sorted.sort();
        if started == sorted { Ok(()) } else { Err(format!("starts out of order: {started:?}")) }
    });
}

#[test]
fn xfer_prop_idle() {
    let mut det = scripted(vec![accepts(&["a"]), copies(&["a"])], 1);
    det.run_until_quiet(10_000).unwrap();
    det.post(Address::<Transfers>::new(TRANSFER_AT), EpochStamp::Epoch(EPOCH), TransferInput::Cancel { key: "a".into(), reply: 9 });
    det.run_until_quiet(10_000).unwrap();
    let before = det.trace().len();
    det.tick(Time(u64::MAX / 2));
    assert!(det.run_until_quiet(100).is_ok());
    assert_eq!(det.trace().len(), before, "no transfer waits: a tick wakes nothing");
}

#[test]
fn xfer_prop_file_kept() {
    let mut twin = Twin::new();
    twin.post(TransferInput::Online(Online(false)));
    twin.accept("a", false);
    twin.post(TransferInput::CopyDone { key: "a".into(), size: 1, sha256: "h".into(), place: Location::Directory });
    take_fx(&twin.log);
    twin.det.close();
    twin.run();
    twin.det.end_epoch(EPOCH, Cause::SessionReset);
    twin.run();
    let effects = take_fx(&twin.log);
    assert!(effects.iter().all(|effect| !matches!(effect, TransferEffect::DeleteFile { .. } | TransferEffect::Store { kind: StoreKind::Removed, .. })), "{effects:?}");
}

#[test]
fn xfer_prop_batch_all_or_none() {
    let mut twin = Twin::new();
    for key in ["b1", "b2"] {
        twin.accept(key, true);
    }
    twin.post(TransferInput::Restored(vec![stored("b3", &batch(), false, false, 0)]));
    let batch_keys: Vec<_> = twin.state().transfers.values().filter(|transfer| transfer.batch).map(|transfer| transfer.key.clone()).collect();
    assert_eq!(batch_keys.len(), 3);
    for key in &batch_keys {
        let effects = twin.post(TransferInput::Cancel { key: key.clone(), reply: 7 });
        let in_flight = matches!(twin.state().stage_of(key), Some(TransferStage::Uploading | TransferStage::Completing));
        if in_flight {
            assert_eq!(replies_of(&effects, 7), [TransferAnswer::Err(Fault::InFlight)]);
        }
    }
}

/// The rule of the table for the answer of Store (`transfer.md`, rules of the table): `Rejected(NoSession)` ends the transfer, `Rejected(Storage)` leaves it failed with the error shown and writes nothing again.
#[test]
fn xfer_store_rejected() {
    for (reason, want_stage, shows_error) in [(RejectReason::NoSession, St::Done, false), (RejectReason::Storage("disk".into()), St::Failed, true)] {
        let mut twin = Twin::new();
        twin.accept(KEY, false);
        let effects = twin.post(TransferInput::CopyDone { key: KEY.into(), size: 1, sha256: "h".into(), place: Location::Directory });
        assert!(effects.iter().any(|effect| matches!(effect, TransferEffect::Store { kind: StoreKind::SyncUpload, .. })), "{effects:?}");
        let effects = twin.post(TransferInput::Rejected(Rejected { batch: 1, reason: reason.clone() }));
        assert_eq!(twin.stage(KEY), want_stage, "{reason:?}");
        assert_eq!(effects.iter().any(|effect| matches!(effect, TransferEffect::Shows { shown: t::Shown::Failed { error }, .. } if error == "disk")), shows_error, "{effects:?}");
        assert!(effects.iter().all(|effect| !matches!(effect, TransferEffect::Store { .. })), "a refused write is not written again: {effects:?}");
    }
}
