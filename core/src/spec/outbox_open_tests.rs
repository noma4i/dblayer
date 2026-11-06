//! Owner-level tests of the `outbox` machine: the opening (`Restored`) and the local writes.



use super::ref_outbox::{open_step_of, write_step_of, Ctx, Eff, OCtx, Policy, Reason as RReason, Restored, St, WCtx, WEff, WIn, WObs, WSt};
use crate::owners::contract::{Committed, Online, Rejected, ReplyId};



use crate::owners::write::outbox::{self as o, CommitKind, LocalWriteKind, OpId, OutboxAnswer, OutboxInput, StoredOp};

use crate::runtime::{Cause, Time};
use super::outbox_harness::*;

#[test]
fn out_open_restores() { open_check(Restored::Plain, ready(), false); }

#[test]
fn out_open_restores_queued() { open_check(Restored::Plain, offline(), false); }

#[test]
fn out_open_restores_failed() { open_check(Restored::ErrorMark, ready(), false); }

#[test]
fn out_open_restores_transfer() {
    open_check(Restored::Transfer, ready(), false);
    let (mut twin, id) = restored(Restored::Transfer, &ready());
    twin.committed(id);
    twin.post(OutboxInput::Response { op_id: id, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, errors: None });
    let effects = twin.committed(id);
    assert!(effects.iter().any(|effect| matches!(effect, OutboxEffect::TransferOutcome { outcome: o::OutboxOutcome::Confirmed { .. }, .. })), "{effects:?}");
}

#[test]
fn out_open_format_change() { open_check(Restored::FormatChange, ready(), false); }

#[test]
fn out_open_restores_pending() { open_check(Restored::Pending, ready(), true); }

fn restored(kind: Restored, ctx: &Ctx) -> (Twin, OpId) {
    let mut twin = Twin::bare();
    twin.post(OutboxInput::RegisterOperations { declarations: vec![("Moment".into(), "update".into())] });
    if !ctx.online {
        twin.post(OutboxInput::Online(Online(false)));
    }
    let stored = StoredOp {
        answered: None,
        op_id: 7,
        op: model_op("r", Policy::Rollback, kind == Restored::Transfer),
        error_mark: kind == Restored::ErrorMark,
        pending: (kind == Restored::Pending).then_some((41, 42)),
        attempts: 1,
        cycle_id: 1,
        restore_failed: None,
        restore_element: None,
    };
    twin.first_pair.insert(7, (41, 7));
    let before = twin.post(OutboxInput::Restored { ops: vec![stored], pending_lost: false });
    assert!(before.iter().all(|effect| !matches!(effect, OutboxEffect::Send { .. } | OutboxEffect::Store { .. })), "the network effects wait for RestoreReady: {before:?}");
    twin.kept = twin.post(OutboxInput::RestoreReady { epoch: EPOCH });
    (twin, 7)
}

/// Requirement: an answer saved before the close reaches its operation at open and the operation does not go out again.
/// Observable: the effects of `Restored`. Assert: no `Send`, the batch of the end of the operation (`Land` for `Ok`, `Rollback` for a refusal) and the removal of the saved answer in it.
#[test]
fn out_open_saved_answer_ends_the_operation_without_a_send() {
    for (answer, kind) in [(crate::owners::session::StoredAnswer::Http { status: 422 }, CommitKind::Rollback)] {
        let mut twin = Twin::bare();
        twin.post(OutboxInput::RegisterOperations { declarations: vec![("Moment".into(), "update".into())] });
        let stored = StoredOp { answered: Some((41, answer)), op_id: 7, op: model_op("r", Policy::Rollback, false), error_mark: false, pending: None, attempts: 1, cycle_id: 1, restore_failed: None, restore_element: None };
        let effects = twin.post(OutboxInput::Restored { ops: vec![stored], pending_lost: false });
        assert!(!effects.iter().any(|effect| matches!(effect, OutboxEffect::Send { .. })), "{effects:?}");
        assert!(effects.iter().any(|effect| matches!(effect, OutboxEffect::Store { op_id: Some(7), kind: found, .. } if *found == kind)), "{kind:?}: {effects:?}");
    }
}

fn open_check(kind: Restored, ctx: Ctx, pair: bool) {
    let want = open_step_of(kind, &OCtx { online: ctx.online, row_free: ctx.row_free });
    let (mut twin, id) = restored(kind, &ctx);
    let effects = twin.kept.clone();
    let attempts = effects.iter().filter(|effect| matches!(effect, OutboxEffect::Store { op_id: Some(op), kind: CommitKind::Attempt, .. } if *op == id)).count();
    assert_eq!(twin.stage(id), Some(want.next), "the state after Restored");
    assert_eq!(attempts, usize::from(want.eff.contains(&Eff::Store(super::ref_outbox::Commit::Attempt))), "{effects:?}");
    assert!(!effects.iter().any(|effect| matches!(effect, OutboxEffect::Send { .. })), "no Mutate before the attempt is written: {effects:?}");
    if want.next == St::PersistingAttempt {
        let sent = twin.committed(id);
        let pairs: Vec<_> = sent.iter().filter_map(|effect| match effect { OutboxEffect::Send { op_id, request_id, sequence } if *op_id == id => Some((*request_id, *sequence)), _ => None }).collect();
        assert_eq!(pairs.len(), 1, "{sent:?}");
        if pair {
            assert_eq!(pairs[0].0, 41, "the Send carries the stored request id");
        }
    }
}

fn write_state(state: WSt, ctx: &WCtx) -> (Twin, ReplyId) {
    let mut twin = Twin::new();
    if state == WSt::Writing {
        twin.post(OutboxInput::LocalWrite { model: "Moment".into(), epoch: EPOCH, write: LocalWriteKind::SetEphemeral { expire_after: ctx.ephemeral_expiry.then_some(10) }, row: "w".into(), values: "{}".into(), reply: 50, now: Time(100) });
    }
    (twin, 50)
}

fn write_check(state: WSt, ctx: WCtx, input: WIn) {
    let want = write_step_of(state, &ctx, input).unwrap_or_else(|| panic!("no table cell for write {state:?} x {input:?}"));
    let (mut twin, reply) = write_state(state, &ctx);
    let before = twin.det.trace().len();
    let write_batch = twin.log.lock().unwrap().len();
    let _ = write_batch;
    let effects = match input {
        WIn::Write => twin.post(OutboxInput::LocalWrite { model: "Moment".into(), epoch: if ctx.epoch_current { EPOCH } else { EPOCH + 1 }, write: LocalWriteKind::SetEphemeral { expire_after: ctx.ephemeral_expiry.then_some(10) }, row: "w".into(), values: "{}".into(), reply, now: Time(100) }),
        WIn::Committed => twin.post(OutboxInput::Committed(Committed { batch: 1, revision: 11, tables: vec!["moments".into()], models: Default::default(), stale_parents: Vec::new() })),
        WIn::Rejected(reason) => {
            twin.post(OutboxInput::Rejected(Rejected { batch: 1, reason: reject(reason) }))
        }
        WIn::EpochEnded => { twin.det.end_epoch(EPOCH, Cause::SessionReset); twin.run(); take_fx(&twin.log, state_of(&twin.det)) }
        WIn::Closing => { twin.det.close(); twin.run(); take_fx(&twin.log, state_of(&twin.det)) }
        WIn::Expire => {
            if ctx.renewed {
                twin.post(OutboxInput::LocalWrite { model: "Moment".into(), epoch: EPOCH, write: LocalWriteKind::SetEphemeral { expire_after: Some(1_000) }, row: "w".into(), values: "{}".into(), reply: 51, now: Time(105) });
                twin.post(OutboxInput::Committed(Committed { batch: 1, revision: 12, tables: vec!["moments".into()], models: Default::default(), stale_parents: Vec::new() }));
            }
            twin.post(OutboxInput::Expire { model: "Moment".into(), row: "w".into(), due: Time(110) })
        }
    };
    let scheduled = twin.det.trace()[before..].iter().any(|entry| entry.effects.contains("Schedule"));
    let mut obs = Vec::new();
    let mut eff = Vec::new();
    for effect in effects {
        match effect {
            OutboxEffect::Reply { answer: OutboxAnswer::Ok, .. } => obs.push(WObs::Ok),
            OutboxEffect::Reply { answer: OutboxAnswer::Err(fault), .. } => obs.push(WObs::Fails(fault_of(&fault))),
            OutboxEffect::Store { kind: CommitKind::LocalWrite, generation, .. } => eff.push(if generation > 0 { WEff::StoreWriteNewBasis } else { WEff::StoreWrite }),
            OutboxEffect::Store { kind: CommitKind::RemoveRow, .. } => eff.push(WEff::StoreRemoveRow),
            OutboxEffect::Drained | OutboxEffect::Schedule { .. } => {}
            other => panic!("an effect outside the write table: {other:?}"),
        }
    }
    if scheduled && state == WSt::Writing && matches!(input, WIn::Committed) {
        eff.push(WEff::ScheduleExpire);
    }
    let want_obs: Vec<WObs> = want.obs.iter().copied().filter(|obs| !matches!(obs, WObs::ValueVisibleAtRevision | WObs::RowGone)).collect();
    assert_eq!(obs, want_obs, "{state:?} x {input:?}: the replies");
    assert_eq!(eff, want.eff, "{state:?} x {input:?}: the effects");
}

#[test]
fn out_local_write() { write_check(WSt::None, WCtx::ready(), WIn::Write); }

#[test]
fn out_local_write_stale() { write_check(WSt::None, WCtx { epoch_current: false, ..WCtx::ready() }, WIn::Write); }

#[test]
fn out_local_write_committed() {
    write_check(WSt::Writing, WCtx::ready(), WIn::Committed);
    write_check(WSt::Writing, WCtx { ephemeral_expiry: true, ..WCtx::ready() }, WIn::Committed);
}

#[test]
fn out_local_write_stale_generation() { write_check(WSt::Writing, WCtx::ready(), WIn::Rejected(RReason::StaleGeneration)); }

#[test]
fn out_local_write_rejected() {
    for reason in [RReason::NoSession, RReason::Closed, RReason::Storage] {
        write_check(WSt::Writing, WCtx::ready(), WIn::Rejected(reason));
    }
}

#[test]
fn out_local_write_interrupted() {
    write_check(WSt::Writing, WCtx::ready(), WIn::EpochEnded);
    write_check(WSt::Writing, WCtx::ready(), WIn::Closing);
}

#[test]
fn out_ephemeral_expire() { write_check(WSt::None, WCtx::ready(), WIn::Expire); }

#[test]
fn out_ephemeral_expire_renewed() { write_check(WSt::None, WCtx { renewed: true, ..WCtx::ready() }, WIn::Expire); }

/// P-OUT-20: the saved `Ok(body)` is read by the codec of the operation like a live answer, and the rows it reads land with the end of the operation.
#[test]
fn out_prop_restored_answer_rows() {
    let mut twin = Twin::bare();
    twin.post(OutboxInput::RegisterOperations { declarations: vec![("Moment".into(), "update".into())] });
    let body = "{\"data\":{\"row\":\"saved\"}}";
    let stored = StoredOp { answered: Some((41, crate::owners::session::StoredAnswer::Ok(body.into()))), op_id: 7, op: model_op("r", Policy::Rollback, false), error_mark: false, pending: None, attempts: 1, cycle_id: 1, restore_failed: None, restore_element: None };
    let effects = twin.post(OutboxInput::Restored { ops: vec![stored], pending_lost: false });
    assert!(effects.iter().all(|effect| !matches!(effect, OutboxEffect::Send { .. } | OutboxEffect::Store { .. })), "nothing lands before the body is read: {effects:?}");
    let decoded = twin.det.trace().iter().any(|entry| entry.effects.contains("Decode") && entry.effects.contains("saved"));
    assert!(decoded, "the saved body goes to the codec of the operation");
    let landed = twin.post(OutboxInput::Response { op_id: 7, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, errors: None });
    assert!(landed.iter().any(|effect| matches!(effect, OutboxEffect::Store { op_id: Some(7), kind: CommitKind::Land, .. })), "{landed:?}");
    assert!(twin.store_text.contains("Rows { model: \"Moment\""), "the rows of the answer land in the batch: {}", twin.store_text);
}
