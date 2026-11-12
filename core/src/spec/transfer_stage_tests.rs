//! Owner-level tests of a file accepted before its row (D-372): the stage `Held`, `Commit`, the ends without `Commit`.

use super::transfer_harness::*;
use crate::owners::write::outbox::Fault;
use crate::owners::write::transfer::{CommitRequest, Shown, TransferAnswer, TransferId, TransferInput, TransferStage, TransferTick};

const KEY: &str = "Moment:r1:upload";

fn held_item(key: &str, parts: u32) -> crate::owners::write::transfer::BatchItem {
    let mut held = item(key, parts);
    held.held = true;
    held.mutation.effects = Default::default();
    held
}

fn commit_input(twin: &mut Twin, key: &str) -> (u64, Out) {
    let reply = twin.reply();
    let request = CommitRequest { effects: mutation(key).effects, variables: Default::default(), wrap: None, refused: false };
    (reply, twin.post(TransferInput::Commit { epoch: EPOCH, key: key.into(), request, reply }))
}

/// The receipt of the held transfer: copy written, `Ready` or `Uploading` after the first write.
fn staged(twin: &mut Twin, key: &str, parts: u32) -> TransferId {
    let reply = twin.reply();
    twin.post(TransferInput::Accept { epoch: EPOCH, item: Box::new(held_item(key, parts)), reply });
    let id = twin.state().open_id(key).expect("a held transfer is accepted");
    twin.post(TransferInput::OpReserved { target: crate::owners::write::outbox::ReserveTarget::Transfer(id), result: Ok(id) });
    twin.commit();
    twin.copy_done(id);
    let out = twin.commit();
    assert!(matches!(out.replies(reply).as_slice(), [TransferAnswer::Receipt(receipt)] if receipt.optimistic.is_none()), "the receipt has no row");
    if twin.stage(id) == TransferStage::PersistingAttempt { twin.commit(); }
    id
}

/// Every part confirmed and written: the transfer waits in `Held`.
fn held_at(twin: &mut Twin, key: &str) -> TransferId {
    let id = staged(twin, key, 1);
    assert_eq!(twin.stage(id), TransferStage::Uploading);
    twin.step_ok(id, 1, Some("u1"));
    twin.commit();
    assert_eq!(twin.stage(id), TransferStage::Held);
    id
}

#[test]
fn xfer_stage_accept() {
    let mut twin = Twin::offline();
    let id = staged(&mut twin, KEY, 2);
    assert_eq!(twin.stage(id), TransferStage::Ready);
    assert!(twin.t(id).held && twin.t(id).mutation.effects.is_empty());
}

#[test]
fn xfer_stage_invalid() {
    let mut twin = Twin::new();
    let reply = twin.reply();
    let mut pending = held_item(KEY, 1);
    pending.source = None;
    pending.steps = None;
    let out = twin.post(TransferInput::Accept { epoch: EPOCH, item: Box::new(pending), reply });
    assert_eq!(Twin::fault(&out, reply), Some(Fault::Invalid));
    let reply = twin.reply();
    let items = vec![held_item(KEY, 1), held_item("Moment:r2:upload", 1)];
    let out = twin.post(TransferInput::AcceptBatch { epoch: EPOCH, items, joint: Box::new(joint()), reply });
    assert_eq!(Twin::fault(&out, reply), Some(Fault::Invalid));
    assert!(twin.state().open_id(KEY).is_none());
}

#[test]
fn xfer_stage_holds_complete() {
    let mut twin = Twin::new();
    let id = staged(&mut twin, KEY, 1);
    twin.step_ok(id, 1, Some("u1"));
    let out = twin.commit();
    assert_eq!(twin.stage(id), TransferStage::Held);
    assert_eq!(out.completes(), 0);
    assert!(out.submit_ops().is_empty());
    assert_eq!(twin.t(id).last_shown, Some(Shown::Uploading { progress: 99 }));
}

#[test]
fn xfer_stage_commit_failed() {
    let mut twin = Twin::new();
    let id = staged(&mut twin, KEY, 2);
    twin.step_error(id, false);
    twin.commit();
    assert_eq!(twin.stage(id), TransferStage::Failed);
    let (reply, _) = commit_input(&mut twin, KEY);
    let out = twin.commit();
    assert!(matches!(out.replies(reply).as_slice(), [TransferAnswer::Receipt(_)]));
    assert_eq!(twin.stage(id), TransferStage::Failed);
    assert!(!twin.t(id).held);
}

#[test]
fn xfer_stage_commit_twice() {
    let mut twin = Twin::with_slots(2);
    held_at(&mut twin, KEY);
    commit_input(&mut twin, KEY);
    while twin.has_pending() { twin.commit(); }
    let plain = twin.uploading("Moment:r9:upload", 1);
    let stale = twin.reply();
    let request = || CommitRequest { effects: Default::default(), variables: Default::default(), wrap: None, refused: false };
    let outs = [
        ("second commit", commit_input(&mut twin, KEY), Fault::Invalid),
        ("transfer without held", commit_input(&mut twin, "Moment:r9:upload"), Fault::Invalid),
        ("no transfer", commit_input(&mut twin, "Moment:none:upload"), Fault::NotFound),
    ];
    for (name, (reply, out), fault) in outs { assert_eq!(Twin::fault(&out, reply), Some(fault), "{name}"); }
    let out = twin.post(TransferInput::Commit { epoch: EPOCH + 1, key: KEY.into(), request: request(), reply: stale });
    assert_eq!(Twin::fault(&out, stale), Some(Fault::NoSession));
    assert!(twin.state().stage_of(plain).is_some());
}

#[test]
fn xfer_stage_commit_rejected() {
    let mut twin = Twin::new();
    let id = held_at(&mut twin, KEY);
    let (reply, _) = commit_input(&mut twin, KEY);
    let out = twin.reject(storage());
    assert_eq!(Twin::fault(&out, reply), Some(Fault::of_reject(storage())));
    assert_eq!(twin.stage(id), TransferStage::Held);
    assert!(twin.t(id).held);
}

#[test]
fn xfer_stage_discard() {
    for stage in ["held", "uploading", "ready"] {
        let mut twin = if stage == "ready" { Twin::offline() } else { Twin::new() };
        let (id, minted) = match stage {
            "held" => (held_at(&mut twin, KEY), true),
            "uploading" => {
                let id = staged(&mut twin, KEY, 2);
                twin.step_ok(id, 1, Some("u1"));
                twin.commit();
                twin.commit();
                (id, true)
            }
            _ => (staged(&mut twin, KEY, 2), false),
        };
        let (reply, out) = twin.cancel(KEY);
        assert_eq!(twin.stage(id), TransferStage::Removing, "{stage}");
        assert_eq!(out.orphans_written().len(), usize::from(minted), "{stage}: abort for the minted upload");
        let out = twin.commit();
        assert_eq!(out.replies(reply), vec![TransferAnswer::Cancelled(true)], "{stage}");
        assert!(out.deleted().contains(&crate::owners::write::transfer::file_id(id)), "{stage}: the copy goes");
        assert!(twin.state().open_id(KEY).is_none(), "{stage}");
    }
}

#[test]
fn xfer_stage_idle() {
    let mut twin = Twin::new();
    let id = held_at(&mut twin, KEY);
    let signals = vec![
        TransferInput::Online(crate::owners::contract::Online(false)),
        TransferInput::Online(crate::owners::contract::Online(true)),
        TransferInput::PowerSaving(true),
        TransferInput::PowerSaving(false),
        TransferInput::SlotFreed,
        TransferInput::Tick { kind: TransferTick::Expire },
        TransferInput::RegisterOperations { declarations: Vec::new(), complete: false },
        TransferInput::RestoreReady { epoch: EPOCH },
    ];
    for signal in signals {
        let out = twin.post(signal.clone());
        assert!(Twin::quiet(&out), "{signal:?}");
        assert_eq!(twin.stage(id), TransferStage::Held);
    }
}

#[test]
fn xfer_stage_end() {
    let mut twin = Twin::new();
    let id = held_at(&mut twin, KEY);
    let (waiter, _) = twin.outcome(id, 1);
    let out = twin.end_epoch();
    assert_eq!(twin.stage(id), TransferStage::Done);
    assert!(matches!(out.replies(waiter).as_slice(), [TransferAnswer::Outcome(crate::owners::write::outbox::OutboxOutcome::Ended(_), _)]));
    assert!(out.orphans_written().iter().any(|(upload_id, _)| upload_id == "u1"), "the upload is aborted");
    let mut twin = Twin::new();
    let id = held_at(&mut twin, KEY);
    let out = twin.close();
    assert_eq!(twin.stage(id), TransferStage::Done);
    assert!(out.removed().is_empty(), "the row stays in the base for the next opening");
}

#[test]
fn xfer_stage_restored() {
    let mut row = stored(1, KEY, 2);
    row.held = true;
    row.upload_id = Some("u1".into());
    row.confirmed_parts = 2;
    let (mut twin, out) = Twin::opened(vec![row], Default::default());
    assert_eq!(twin.stage(1), TransferStage::Removing);
    assert_eq!(out.removed(), vec![KEY.to_string()]);
    assert_eq!(out.orphans_written(), vec![("u1".to_string(), KEY.to_string())]);
    assert_eq!(out.completes(), 0);
    let out = twin.commit();
    assert!(out.deleted().contains(&crate::owners::write::transfer::file_id(1)));
    assert!(twin.state().open_id(KEY).is_none());
}

/// A random order of the inputs of one held transfer never completes it and never submits a mutation without `Commit`, and every end leaves no row and an abort for the upload it minted.
#[test]
fn xfer_stage_prop_closed_lifecycle() {
    for seed in 0..120u64 {
        let mut random = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let mut next = move |bound: u64| { random = random.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407); (random >> 33) % bound };
        let mut twin = if seed % 2 == 0 { Twin::new() } else { Twin::offline() };
        let id = staged(&mut twin, KEY, 2);
        let (mut committed, mut minted, mut completes) = (false, false, 0);
        for _ in 0..24 {
            if twin.state().stage_of(id).is_none_or(|stage| matches!(stage, TransferStage::Done | TransferStage::Removing)) { break; }
            let out = match next(6) {
                0 => if twin.has_pending() { twin.commit() } else { continue },
                1 if twin.stage(id) == TransferStage::Uploading => { let part = twin.t(id).confirmed_parts + 1; minted = true; twin.step_ok(id, part, Some("u1")) }
                2 => twin.online(next(2) == 0),
                3 if !committed => { committed = true; commit_input(&mut twin, KEY).1 }
                4 => twin.post(TransferInput::PowerSaving(next(2) == 0)),
                _ => continue,
            };
            completes += out.completes();
            assert!(committed || (completes == 0 && out.submit_ops().is_empty()), "seed {seed}: no complete and no mutation without Commit");
        }
        if committed { continue; }
        while twin.has_pending() { completes += twin.commit().completes(); }
        let (_, out) = twin.cancel(KEY);
        completes += out.completes();
        let mut written = out.orphans_written().len();
        let mut removed = out.removed().len();
        while twin.has_pending() { let out = twin.commit(); completes += out.completes(); written += out.orphans_written().len(); removed += out.removed().len(); }
        assert_eq!(completes, 0, "seed {seed}");
        assert_eq!((removed, written), (1, usize::from(minted)), "seed {seed}: one removal and an abort for the minted upload");
    }
}

fn replies_of(outs: &[Out], reply: u64) -> Vec<TransferAnswer> { outs.iter().flat_map(|out| out.replies(reply)).collect() }

fn drive(twin: &mut Twin, id: TransferId, outs: &mut Vec<Out>) {
    for _ in 0..40 {
        if twin.has_pending() { outs.push(twin.commit()); }
        else if twin.state().stage_of(id) == Some(TransferStage::Uploading) && twin.t(id).confirmed_parts < 2 { let part = twin.t(id).confirmed_parts + 1; outs.push(twin.step_ok(id, part, Some("u1"))); }
        else { break; }
    }
}

/// `Cancel` and `Discard` after `Commit`, before the Store answers its batch (D-394): no answer until `Committed`, then the removal runs by the row of the stage it leaves to; `Rejected` leaves the transfer held and the removal runs as in `Held`.
/// Observable: the answers, the batches of the removal, the debts of the abort. Catches: `InFlight` for the window of the write, a command lost at the exit, an answer twice.
#[test]
fn xfer_removal_waits_for_the_commit_batch() {
    for case in ["held", "uploading", "rejected"] {
        for cancel in [true, false] {
            let mut twin = Twin::with_slots(4);
            let id = if case == "uploading" {
                let id = staged(&mut twin, KEY, 2);
                twin.step_ok(id, 1, Some("u1"));
                twin.commit();
                if twin.stage(id) == TransferStage::PersistingAttempt { twin.commit(); }
                assert_eq!(twin.stage(id), TransferStage::Uploading);
                id
            } else { held_at(&mut twin, KEY) };
            let (commit, first) = commit_input(&mut twin, KEY);
            let (command, second) = if cancel { twin.cancel(KEY) } else { twin.discard(KEY) };
            let mut outs = vec![first, second];
            assert!(replies_of(&outs, command).is_empty() && replies_of(&outs, commit).is_empty(), "{case} cancel {cancel}: the command waits for the batch");
            if case == "rejected" {
                outs.push(twin.reject(storage()));
                assert_eq!(replies_of(&outs, commit), vec![TransferAnswer::Err(Fault::of_reject(storage()))]);
                assert!(twin.t(id).held, "the transfer stays held");
            }
            while twin.has_pending() { outs.push(twin.commit()); }
            let answer = if cancel { TransferAnswer::Cancelled(true) } else { TransferAnswer::Ok };
            assert_eq!(replies_of(&outs, command), vec![answer], "{case} cancel {cancel}");
            assert!(twin.state().stage_of(id).is_none_or(|stage| stage == TransferStage::Done), "{case} cancel {cancel}");
            assert_eq!(outs.iter().map(|out| out.removed().len()).sum::<usize>(), 1, "{case} cancel {cancel}: one batch removes the row");
            assert_eq!(outs.iter().map(|out| out.orphans_written().len()).sum::<usize>(), 1, "{case} cancel {cancel}: 1 debt of the multipart");
            if case != "rejected" { assert!(matches!(replies_of(&outs, commit).as_slice(), [TransferAnswer::Receipt(_)]), "{case}: the commit is answered once"); }
        }
    }
}

/// `Commit` in a stage of an internal step (D-394) waits for the stage it leaves to: the receipt once it is open, `NotFound` once a removal ahead of it ended the transfer.
/// Observable: the answers to `Commit`. Catches: `InFlight` for a window of the library, a lost command.
#[test]
fn xfer_commit_waits_for_the_stage() {
    for stage in [TransferStage::PersistingRetry, TransferStage::PersistingFailure] {
        for removed in [false, true] {
            let mut twin = Twin::with_slots(4);
            let id = staged(&mut twin, KEY, 2);
            twin.step_error(id, false);
            if stage == TransferStage::PersistingRetry {
                twin.commit();
                twin.retry(KEY, None);
            }
            assert_eq!(twin.stage(id), stage);
            let removal = removed.then(|| twin.cancel(KEY));
            let (commit, first) = commit_input(&mut twin, KEY);
            let (second_commit, second) = commit_input(&mut twin, KEY);
            let mut outs = vec![first, second];
            assert!(replies_of(&outs, commit).is_empty() && removal.as_ref().is_none_or(|(reply, _)| replies_of(&outs, *reply).is_empty()), "{stage:?}: the command waits");
            assert_eq!(replies_of(&outs, second_commit), vec![TransferAnswer::Err(Fault::Invalid)], "{stage:?}: a second commit is refused");
            drive(&mut twin, id, &mut outs);
            if removed {
                assert_eq!(replies_of(&outs, commit), vec![TransferAnswer::Err(Fault::NotFound)], "{stage:?}");
                assert_eq!(replies_of(&outs, removal.unwrap().0), vec![TransferAnswer::Cancelled(true)], "{stage:?}");
            } else {
                assert!(matches!(replies_of(&outs, commit).as_slice(), [TransferAnswer::Receipt(_)]), "{stage:?}");
                assert!(!twin.t(id).held, "{stage:?}: the row is committed");
            }
        }
    }
}

/// P-XFER-78 (D-394): `Commit` and a removal in any order and at any point of the trace of a staged transfer (parts, errors, writes of the Store): `Commit` gets a receipt when it came first and `NotFound` when the removal did, the removal gets `true` / `Ok`, every call exactly one answer, never `InFlight`.
#[test]
fn xfer_prop_commit_and_removal_in_any_order() {
    for seed in 0..120u64 {
        let mut random = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let mut next = move |bound: u64| { random = random.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407); (random >> 33) % bound };
        let cancel = next(2) == 0;
        let mut twin = Twin::with_slots(4);
        let id = staged(&mut twin, KEY, 2);
        let mut outs: Vec<Out> = Vec::new();
        let (mut commit, mut removal, mut order) = (None, None, Vec::new());
        for _ in 0..16 {
            match next(5) {
                0 if twin.has_pending() => outs.push(twin.commit()),
                1 if twin.stage(id) == TransferStage::Uploading && twin.t(id).confirmed_parts < 2 => { let part = twin.t(id).confirmed_parts + 1; outs.push(twin.step_ok(id, part, Some("u1"))); }
                2 if twin.stage(id) == TransferStage::Uploading && commit.is_none() => outs.push(twin.step_error(id, next(2) == 0)),
                3 if commit.is_none() => { let (reply, out) = commit_input(&mut twin, KEY); commit = Some(reply); order.push('c'); outs.push(out); }
                4 if removal.is_none() => { let (reply, out) = if cancel { twin.cancel(KEY) } else { twin.discard(KEY) }; removal = Some(reply); order.push('r'); outs.push(out); }
                _ => {}
            }
        }
        if commit.is_none() { let (reply, out) = commit_input(&mut twin, KEY); commit = Some(reply); order.push('c'); outs.push(out); }
        if removal.is_none() { let (reply, out) = if cancel { twin.cancel(KEY) } else { twin.discard(KEY) }; removal = Some(reply); order.push('r'); outs.push(out); }
        drive(&mut twin, id, &mut outs);
        let (commit, removal) = (commit.unwrap(), removal.unwrap());
        let answer = if cancel { TransferAnswer::Cancelled(true) } else { TransferAnswer::Ok };
        assert_eq!(replies_of(&outs, removal), vec![answer], "seed {seed} {order:?}");
        let commit_answers = replies_of(&outs, commit);
        if order[0] == 'r' { assert_eq!(commit_answers, vec![TransferAnswer::Err(Fault::NotFound)], "seed {seed} {order:?}"); }
        else { assert!(matches!(commit_answers.as_slice(), [TransferAnswer::Receipt(_)]), "seed {seed} {order:?}: {commit_answers:?}"); }
    }
}
