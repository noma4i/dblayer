//! Executable reference of the Outbox owner and of the LocalWrites owner, written from the Outbox owner table of the machine spec only. No library code.
//!
//! `step(&State, &Input) -> (State, Vec<Effect>)`: every fired row emits `Effect::Row(<test id>)`. A pair that the spec does not answer emits
//! `Effect::Undefined`. Ids with the prefix `rule_` name a table rule or a general rule (R2, R4, R4b) that has no test id of its own.

#![allow(dead_code, clippy::too_many_arguments, clippy::large_enum_variant, clippy::collapsible_if, clippy::collapsible_else_if)]

mod calls;
mod helpers;
mod local;
mod ops_early;
mod ops_late;
mod restore;
mod types;

pub use helpers::*;
pub use types::*;

pub fn step(s: &State, i: &Input) -> (State, Vec<Effect>) {
    let mut s = s.clone();
    let before = unfinished_per_row(&s);
    let mut e = Vec::new();
    inner(&mut s, i, &mut e);
    bump_changed_rows(&mut s, &before);
    (s, e)
}

fn done_count(s: &State) -> usize { s.ops.values().filter(|o| o.stage == Stage::Done).count() }

fn unfinished_per_row(s: &State) -> std::collections::BTreeMap<(String, String), usize> {
    let mut counts = std::collections::BTreeMap::new();
    for op in s.ops.values() {
        if op.stage != Stage::Done { *counts.entry((op.model.clone(), op.row_id.clone())).or_insert(0) += 1; }
    }
    counts
}

/// A change to the set of unfinished operations of a row (submit, operation to Done, RowRemoved, restore landing) raises its version.
fn bump_changed_rows(s: &mut State, before: &std::collections::BTreeMap<(String, String), usize>) {
    let after = unfinished_per_row(s);
    let mut rows: std::collections::BTreeSet<(String, String)> = before.keys().cloned().collect();
    rows.extend(after.keys().cloned());
    for key in rows {
        if before.get(&key).copied().unwrap_or(0) != after.get(&key).copied().unwrap_or(0) { bump_selection_row(s, &key.0, &key.1); }
    }
}

pub(crate) fn inner(s: &mut State, i: &Input, e: &mut Vec<Effect>) {
    let queued_before: Vec<OpId> = s.ops.values().filter(|o| o.stage == Stage::Queued).map(|o| o.id).collect();
    let (slots, dones, phase) = (slots_taken(s), done_count(s), s.phase);
    route(s, i, e);
    if !matches!(s.phase, Phase::Classified | Phase::Ready) { return; }
    let skip = match i { Input::Committed { batch: Batch::Op(id), .. } => Some(*id), _ => None };
    let _ = slots;
    let trigger = matches!(i, Input::Online(true) | Input::Committed { kind: Kind::MarkFailed, .. })
        || done_count(s) > dones
        || (phase == Phase::Classified && s.phase == Phase::Ready);
    if !trigger { return; }
    for id in queued_before {
        if Some(id) == skip { continue; }
        match s.ops.get(&id) { Some(o) if o.stage == Stage::Queued => {} _ => continue }
        let mut op = s.ops.remove(&id).unwrap();
        ops_early::queued_drain(s, &mut op, e);
        s.ops.insert(id, op);
    }
}

fn expected_kind(st: Stage) -> Option<Kind> {
    match st {
        Stage::Submitting => Some(Kind::Submit),
        Stage::PersistingAttempt => Some(Kind::Attempt),
        Stage::Landing => Some(Kind::Land),
        Stage::RollingBack => Some(Kind::Rollback),
        Stage::MarkingFailed => Some(Kind::MarkFailed),
        Stage::MarkingRetry => Some(Kind::Retry),
        _ => None,
    }
}

/// The row of one operation for an input.
pub(crate) fn op_input(s: &mut State, op: &mut Op, i: &Input, e: &mut Vec<Effect>) {
    match i {
        Input::Outcome { receipt } => return ops_late::outcome(op, receipt, e),
        Input::EpochEnded(_) => return ops_late::epoch_ended(op, e),
        Input::Closing => return ops_late::closing(op, e),
        _ => {}
    }
    match op.stage {
        Stage::WaitingDeclaration => ops_late::waiting_declaration(s, op, i, e),
        Stage::Submitting => ops_early::submitting(s, op, i, e),
        Stage::Queued => ops_early::queued(s, op, i, e),
        Stage::PersistingAttempt => ops_early::persisting(s, op, i, e),
        Stage::Sending => ops_early::sending(s, op, i, e),
        Stage::Landing => ops_late::landing(s, op, i, e),
        Stage::RollingBack => ops_late::rolling_back(s, op, i, e),
        Stage::MarkingFailed => ops_late::marking_failed(s, op, i, e),
        Stage::MarkingRetry => ops_late::marking_retry(s, op, i, e),
        Stage::Failed => ops_late::failed(s, op, i, e),
        Stage::Done => ops_late::done(s, op, i, e),
    }
}

fn broadcast(s: &mut State, i: &Input, e: &mut Vec<Effect>) {
    let ids: Vec<OpId> = s.ops.keys().copied().collect();
    for id in ids {
        let mut op = s.ops.remove(&id).unwrap();
        op_input(s, &mut op, i, e);
        s.ops.insert(id, op);
    }
}

fn epoch_started(s: &mut State, epoch: Epoch, identity: bool, generations: &std::collections::BTreeMap<String, u64>, e: &mut Vec<Effect>) {
    if matches!(s.phase, Phase::Restoring | Phase::Classified | Phase::Ready) {
        row(e, "out_epoch_started_live_ignored");
        return;
    }
    s.ops.clear();
    s.held.clear();
    s.aliases.clear();
    s.pending_requests.clear();
    s.selects.clear();
    s.queue_failed = false;
    s.pending_lost = false;
    s.epoch = Some(epoch);
    s.epoch_live = true;
    s.generations = generations.clone();
    if identity {
        s.phase = Phase::Restoring;
        row(e, "out_epoch_started_tells_lifecycle");
        e.push(Effect::Lifecycle("Started{Outbox,epoch}"));
        e.push(Effect::Network("SnapshotRequest{Outbox,epoch}"));
    } else {
        s.phase = Phase::NoOwner;
        row(e, "rule_epoch_without_identity");
    }
}

fn answer_held(s: &mut State, cause: EndCause, e: &mut Vec<Effect>) {
    let (outcome, reserved) = match cause {
        EndCause::SessionEnded => ("Outcome:Ended(SessionEnded)", "OpReserved{Err(SessionEnded)}"),
        EndCause::Closed => ("Outcome:Ended(Closed)", "OpReserved{Err(Closed)}"),
    };
    for h in std::mem::take(&mut s.held) {
        match h {
            Input::Submit { .. } | Input::Retry { .. } | Input::Discard { .. } => reply(e, Addr::Call, 0, end_err(cause)),
            Input::Outcome { .. } => reply(e, Addr::Call, 0, outcome),
            Input::ReserveOp { .. } => e.push(Effect::Transfer { op: 0, what: reserved }),
            _ => {}
        }
    }
}

fn closing(s: &mut State, e: &mut Vec<Effect>) {
    let was_drained = s.drained;
    s.closing = true;
    match s.phase {
        Phase::Restoring => {
            row(e, "out_restoring_closing");
            answer_held(s, EndCause::Closed, e);
        }
        Phase::Classified | Phase::Ready => {
            if s.ops.is_empty() { row(e, "out_done_closing"); } else { broadcast(s, &Input::Closing, e); }
        }
        Phase::NoOwner | Phase::Ended => {
            if s.ops.is_empty() { row(e, if was_drained { "out_done" } else { "out_done_closing" }); } else { broadcast(s, &Input::Closing, e); }
        }
    }
    s.phase = Phase::Ended;
    if !was_drained {
        s.drained = true;
        e.push(Effect::Drained("Outbox"));
    }
}

fn epoch_ended(s: &mut State, ep: Epoch, e: &mut Vec<Effect>) {
    if !s.epoch_is(ep) { row(e, "rule_R4_foreign"); return; }
    s.epoch_live = false;
    match s.phase {
        Phase::Restoring => {
            row(e, "out_restoring_epoch_ended");
            answer_held(s, EndCause::SessionEnded, e);
            s.phase = Phase::Ended;
        }
        Phase::Classified | Phase::Ready => {
            broadcast(s, &Input::EpochEnded(ep), e);
            s.phase = Phase::Ended;
        }
        Phase::NoOwner | Phase::Ended => {}
    }
    local::broadcast(s, &Input::EpochEnded(ep), e);
}

fn addressed(s: &mut State, op_id: OpId, i: &Input, e: &mut Vec<Effect>) {
    if s.phase == Phase::Restoring { row(e, "out_restoring_foreign"); return; }
    let mut op = match s.ops.remove(&op_id) {
        Some(op) => op,
        None => {
            row(e, "rule_R2_foreign");
            return;
        }
    };
    let (cycle, retained) = match i {
        Input::Response { cycle_id, pending_retained, .. } | Input::TransportError { cycle_id, pending_retained, .. } | Input::Dropped { cycle_id, pending_retained, .. } => (*cycle_id, *pending_retained),
        Input::Tick { cycle_id, .. } => (*cycle_id, None),
        _ => (op.cycle_id, None),
    };
    if cycle == op.cycle_id { if let Some(b) = retained { op.pending_owned = b; } }
    op_input(s, &mut op, i, e);
    s.ops.insert(op_id, op);
}

fn batch_reply(s: &mut State, i: &Input, epoch: Epoch, batch: Batch, e: &mut Vec<Effect>) {
    let id = match batch { Batch::Local(id) => return local::batch(s, id, i, e), Batch::Op(id) => id };
    if !s.epoch_is(epoch) { row(e, "rule_R4_foreign"); return; }
    if s.phase == Phase::Restoring { row(e, "out_restoring_foreign"); return; }
    let kind = match i { Input::Committed { kind, .. } | Input::Rejected { kind, .. } => *kind, _ => return };
    let mut op = match s.ops.remove(&id) { Some(op) => op, None => { row(e, "rule_R2_foreign"); return; } };
    match expected_kind(op.stage) {
        Some(k) if k != kind => row(e, "rule_R2_foreign"),
        _ => op_input(s, &mut op, i, e),
    }
    s.ops.insert(id, op);
}

fn route(s: &mut State, i: &Input, e: &mut Vec<Effect>) {
    match i {
        Input::Now(t) => { s.now = *t; row(e, "rule_clock"); }
        Input::EpochStarted { epoch, identity, generations } => epoch_started(s, *epoch, *identity, generations, e),
        Input::Submit { epoch, op, flags } => calls::submit(s, *epoch, op, flags, e),
        Input::ReserveOp { epoch, target } => calls::reserve(s, *epoch, *target, e),
        Input::Restored { .. } => restore::restored(s, i, e),
        Input::RegisterOperations { .. } => restore::register(s, i, e),
        Input::RestoreReady { epoch } => restore::restore_ready(s, *epoch, e),
        Input::Committed { epoch, batch, .. } | Input::Rejected { epoch, batch, .. } => batch_reply(s, i, *epoch, *batch, e),
        Input::Response { op_id, .. } | Input::TransportError { op_id, .. } | Input::Dropped { op_id, .. } | Input::Tick { op_id, .. } => addressed(s, *op_id, i, e),
        Input::Retry { .. } | Input::Discard { .. } => calls::retry_discard(s, i, e),
        Input::Outcome { receipt } => calls::outcome(s, receipt, e),
        Input::SelectedOutbox { .. } | Input::SelectedNone { .. } | Input::SelectedTransfer { .. } => calls::selected(s, i, e),
        Input::Online(b) => {
            s.online = Some(*b);
            if s.phase == Phase::Restoring { row(e, "out_restoring_online"); } else { broadcast(s, i, e); }
            local::broadcast(s, i, e);
        }
        Input::ModelsReset { generations, .. } => {
            for (m, g) in generations { s.generations.insert(m.clone(), *g); }
            if s.phase == Phase::Restoring { row(e, "out_restoring_models_reset"); } else { broadcast(s, i, e); }
            local::broadcast(s, i, e);
        }
        Input::RowRemoved { model, row_id, .. } => {
            let ids: Vec<OpId> = s.ops.values().filter(|o| &o.model == model && &o.row_id == row_id).map(|o| o.id).collect();
            if ids.is_empty() { row(e, "rule_no_addressee"); }
            for id in ids {
                let mut op = s.ops.remove(&id).unwrap();
                ops_late::row_removed(&mut op, e);
                s.ops.insert(id, op);
            }
        }
        Input::EpochEnded(ep) => epoch_ended(s, *ep, e),
        Input::Closing => {
            closing(s, e);
            local::broadcast(s, i, e);
        }
        Input::LocalWrite { epoch, id, kind, flags } => local::write(s, *epoch, *id, kind, flags, e),
        Input::CopyDone { id } => local::copy_result(s, *id, true, e),
        Input::CopyFailed { id } => local::copy_result(s, *id, false, e),
        Input::ExpireRow { .. } | Input::ResetEphemeral { .. } => local::ephemeral(i, e),
    }
}
