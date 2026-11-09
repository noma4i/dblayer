//! Calls of the platform and of Transfer: `Submit`, `ReserveOp`, `Retry`, `Discard`, `Outcome`, selectors.

use super::helpers::*;
use super::types::*;

fn storage_queue(to: Addr, op: u64, e: &mut Vec<Effect>) {
    reply(e, to, op, if let Addr::Transfer(_) = to { "DiscardOutcome{Err(Storage{queue})}" } else { "Err(Storage{queue})" });
}

pub fn submit(s: &mut State, epoch: Epoch, spec: &OpSpec, flags: &SubmitFlags, e: &mut Vec<Effect>) {
    if !s.epoch_is(epoch) {
        row(e, "out_submit_stale");
        reply(e, Addr::Call, 0, "Err(NoSession)");
        return;
    }
    match s.phase {
        Phase::Ended if s.closing => {
            row(e, "out_submit_after_closing");
            reply(e, Addr::Call, 0, "Err(Closed)");
            return;
        }
        Phase::NoOwner | Phase::Ended => {
            row(e, "rule_no_session_mutation");
            reply(e, Addr::Call, 0, "Err(NoSession)");
            return;
        }
        Phase::Restoring => {
            row(e, "out_submit_while_restoring");
            s.held.push(Input::Submit { epoch, op: spec.clone(), flags: flags.clone() });
            return;
        }
        _ => {}
    }
    if s.queue_failed {
        row(e, "out_queue_failed_refuses");
        reply(e, Addr::Call, 0, "Err(Storage{queue})");
        return;
    }
    if !s.declared.contains(&(spec.model.clone(), spec.action.clone())) {
        row(e, "out_submit_undeclared");
        reply(e, Addr::Call, 0, "Err(Invalid)");
        return;
    }
    if !flags.valid {
        row(e, "out_submit_invalid_value");
        reply(e, Addr::Call, 0, "Err(Invalid{path})");
        return;
    }
    match flags.send {
        SendKind::NoCreateDeclared => { row(e, "out_send_undeclared"); reply(e, Addr::Call, 0, "Err(Invalid{model})"); return; }
        SendKind::UploadActionUndeclared => { row(e, "out_send_unknown_upload_action"); reply(e, Addr::Call, 0, "Err(Invalid{action})"); return; }
        _ => {}
    }
    let row_id = s.aliases.get(&(spec.model.clone(), spec.row_id.clone())).cloned().unwrap_or_else(|| spec.row_id.clone());
    let mut spec = spec.clone();
    spec.row_id = row_id;
    let id = match spec.op_id { Some(id) => { if id >= s.next_seq { s.next_seq = id + 1; } id } None => { let id = s.next_seq; s.next_seq += 1; id } };
    let mut op = blank_op(id, &spec);
    op.request_id = fresh_request(s);

    let unsent_create = s.ops.values().find(|o| o.model == op.model && o.row_id == op.row_id && o.action == "create" && o.stage == Stage::Queued).map(|o| o.id);
    if spec.destroy {
        if let Some(cid) = unsent_create {
            row(e, "out_destroy_unsent_create");
            op.cancels = Some(cid);
            op.stage = Stage::Submitting;
            op.submit_reply = true;
            let chain: Vec<OpId> = s.ops.values().filter(|o| o.model == op.model && o.row_id == op.row_id && o.id >= cid && o.stage != Stage::Done).map(|o| o.id).collect();
            for member in chain {
                let mut other = s.ops.remove(&member).unwrap();
                other.cancels = Some(cid);
                other.cause = Cause::Discard;
                other.stage = Stage::RollingBack;
                other.waiting_retry = false;
                s.ops.insert(member, other);
            }
            e.push(Effect::Store { op: cid, what: "Commit{Rollback,remove intents of create and later operations,no Tombstone}" });
            s.ops.insert(id, op);
            return;
        }
    }
    if flags.removes_receiver || flags.leaves_scope || flags.join_missing_row {
        if flags.removes_receiver {
            row(e, "out_submit_removes_receiver");
            e.push(Effect::Store { op: id, what: "Commit{Intent,Tombstone}" });
        }
        if flags.leaves_scope {
            row(e, "out_submit_leaves_scope");
            e.push(Effect::Store { op: id, what: "Commit{Intent,Leave}" });
        }
        if flags.join_missing_row {
            row(e, "out_submit_join_missing_row");
            e.push(Effect::Store { op: id, what: "Commit{Intent}(no Join)" });
        }
    } else if flags.intent_written {
        row(e, "out_submit_intent_written");
        op.attempts = spec.attempts;
        if goes_out(s, &op) && slot_free(s) { persist_attempt(&mut op, e); } else { op.stage = Stage::Queued; }
        s.ops.insert(id, op);
        return;
    } else {
        row(e, "out_submit");
        e.push(Effect::Store { op: id, what: "Commit{Intent{attempts=1,cycle_id},effects,basis=generation}" });
    }
    op.attempts = 1;
    op.stage = Stage::Submitting;
    op.submit_reply = true;
    s.ops.insert(id, op);
}

pub fn reserve(s: &mut State, epoch: Epoch, target: u64, e: &mut Vec<Effect>) {
    if !s.epoch_is(epoch) { row(e, "rule_R4_foreign"); return; }
    match s.phase {
        Phase::Restoring => { row(e, "out_reserve_op_while_restoring"); s.held.push(Input::ReserveOp { epoch, target }); }
        Phase::Classified | Phase::Ready => {
            if s.queue_failed {
                row(e, "out_reserve_op_queue_failed");
                reply(e, Addr::Transfer(target), 0, "OpReserved{Err(Storage{queue})}");
            } else {
                row(e, "out_reserve_op");
                s.next_seq += 1;
                e.push(Effect::Transfer { op: s.next_seq - 1, what: "OpReserved{Ok(op_id)}" });
            }
        }
        Phase::NoOwner | Phase::Ended => row(e, "rule_R4_foreign"),
    }
}

/// Retry and Discard: phase checks, selector, then the row of the addressed operation.
pub fn retry_discard(s: &mut State, i: &Input, e: &mut Vec<Effect>) {
    let (epoch, sel, to, kind) = match i {
        Input::Retry { epoch, sel, reply_to } => (*epoch, sel.clone(), *reply_to, SelKind::Retry),
        Input::Discard { epoch, sel, reply_to } => (*epoch, sel.clone(), *reply_to, SelKind::Discard),
        _ => return,
    };
    if !s.epoch_is(epoch) {
        row(e, "rule_R4b_no_session");
        reply(e, to, 0, "Err(NoSession)");
        return;
    }
    match s.phase {
        Phase::Restoring => { row(e, "out_restoring_holds_calls"); s.held.push(i.clone()); return; }
        Phase::NoOwner => { row(e, "out_retry_no_identity"); reply(e, to, 0, "Err(NoSession)"); return; }
        _ => {}
    }
    if s.queue_failed {
        row(e, "out_queue_failed_refuses");
        storage_queue(to, 0, e);
        return;
    }
    let target = match &sel {
        Sel::Op(id) => if s.ops.contains_key(id) { Some(*id) } else { None },
        Sel::Row { model, row_id, action } => {
            if action.is_none() {
                row(e, "boundary_selector_without_action");
                let key = s.selects.keys().next_back().copied().unwrap_or(0) + 1;
                s.selects.insert(key, (kind, sel.clone(), to, epoch));
                e.push(Effect::Transfer { op: key, what: "SelectRow{model,row,outbox_min,kind,reply}" });
                return;
            }
            row(e, "boundary_selector_orders");
            address(s, model, row_id, action)
        }
    };
    dispatch_to(s, target, kind, to, i, e);
}

fn dispatch_to(s: &mut State, target: Option<OpId>, kind: SelKind, to: Addr, i: &Input, e: &mut Vec<Effect>) {
    match target {
        None => {
            row(e, "out_unknown_operation");
            let _ = kind;
            reply(e, to, 0, if let Addr::Transfer(_) = to { "DiscardOutcome{Err(NotFound)}" } else { "Err(NotFound)" });
        }
        Some(id) => {
            let mut op = s.ops.remove(&id).unwrap();
            super::op_input(s, &mut op, i, e);
            s.ops.insert(id, op);
        }
    }
}

pub fn selected(s: &mut State, i: &Input, e: &mut Vec<Effect>) {
    match i {
        Input::SelectedOutbox { reply: r, action } => {
            let (kind, sel, to, epoch) = match s.selects.remove(r) { Some(x) => x, None => { row(e, "rule_R2_foreign"); return; } };
            row(e, "boundary_selector_without_action");
            let (model, row_id) = match &sel { Sel::Row { model, row_id, .. } => (model.clone(), row_id.clone()), Sel::Op(_) => { row(e, "rule_R2_foreign"); return; } };
            let target = address(s, &model, &row_id, &Some(action.clone()));
            let sel2 = Sel::Row { model, row_id, action: Some(action.clone()) };
            let input = match kind { SelKind::Retry => Input::Retry { epoch, sel: sel2, reply_to: to }, SelKind::Discard => Input::Discard { epoch, sel: sel2, reply_to: to } };
            dispatch_to(s, target, kind, to, &input, e);
        }
        Input::SelectedNone { reply: r } => match s.selects.remove(r) {
            Some((_, _, to, _)) => { row(e, "boundary_selector_without_action"); reply(e, to, 0, if let Addr::Transfer(_) = to { "DiscardOutcome{Err(NotFound)}" } else { "Err(NotFound)" }); }
            None => row(e, "rule_R2_foreign"),
        },
        _ => {}
    }
}

pub fn outcome(s: &mut State, receipt: &Receipt, e: &mut Vec<Effect>) {
    if !s.epoch_is(receipt.epoch) {
        row(e, "out_outcome_stale");
        reply(e, Addr::Call, receipt.op_id, "Err(NoSession)");
        return;
    }
    if s.phase == Phase::Restoring {
        row(e, "out_restoring_holds_calls");
        s.held.push(Input::Outcome { receipt: *receipt });
        return;
    }
    match s.ops.remove(&receipt.op_id) {
        None => { row(e, "out_unknown_operation"); reply(e, Addr::Call, receipt.op_id, "Err(NotFound)"); }
        Some(mut op) => {
            super::ops_late::outcome(&mut op, receipt, e);
            s.ops.insert(op.id, op);
        }
    }
}
