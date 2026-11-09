//! Opening: `Restored`, `RegisterOperations`, `RestoreReady`, the classes of the opening section.

use super::helpers::*;
use super::types::*;

fn expired_at_restore(s: &State, op: &Op, item: &RestoreItem) -> bool {
    match op.expires_after {
        Some(d) => s.now.saturating_sub(op.created_at) > d && item.attempts == 0 && !item.sent,
        None => false,
    }
}

/// Puts one restored operation into exactly one class of the opening table, top to bottom.
pub fn classify_item(s: &mut State, op: &mut Op, e: &mut Vec<Effect>) {
    let item = match op.item.clone() { Some(i) => i, None => { undefined(e, "WaitingDeclaration x RegisterOperations", "operation without a restored item"); return; } };
    op.restored = true;
    op.deferred = false;
    op.waiting_retry = false;
    op.attempts = item.attempts;
    op.cycle_id = item.cycle_id;
    op.request_id = item.request_id;
    op.pending_owned = s.pending_requests.contains(&item.request_id);
    op.ever_sent = item.attempts >= 1;
    op.cycle_sent = item.attempts >= 1;
    let declared = s.declared.contains(&(op.model.clone(), op.action.clone()));
    let online = s.online_now();
    let free = row_free(s, op);
    if item.restore_failed {
        row(e, "out_open_restores_dropped");
        op.stage = Stage::Failed;
        op.error = Some(ErrKind::RestoreFailed);
    } else if !declared {
        row(e, "out_open_waiting_declaration");
        op.stage = Stage::WaitingDeclaration;
    } else if s.pending_lost {
        row(e, "out_open_restores_state_lost");
        op.stage = Stage::Failed;
        op.error = Some(ErrKind::StateLost);
    } else if item.answer == Some(Saved::Ok) {
        row(e, "out_open_restores_answered");
        op.stage = Stage::Landing;
        e.push(Effect::Store { op: op.id, what: "Commit{rows,RemoveIntent,DropEffects,RemoveAnswer}" });
    } else if item.answer == Some(Saved::Refused) {
        row(e, "out_open_restores_answered_error");
        if op.keep { mark_failed(op, ErrKind::Server, e); } else { roll_back(op, Cause::Refusal, e); }
    } else if item.has_error_mark {
        row(e, "out_open_restores_failed");
        op.stage = Stage::Failed;
        op.error = Some(ErrKind::Server);
    } else if item.attempts >= MAX_ATTEMPTS {
        row(e, "out_open_restores_exhausted");
        mark_failed(op, ErrKind::Exhausted, e);
    } else if s.pending_requests.contains(&item.request_id) {
        if online && slot_free(s) {
            row(e, "out_open_restores_pending");
            op.stage = Stage::PersistingAttempt;
            op.deferred = true;
        } else if online {
            row(e, "out_open_restores_queued");
            op.stage = Stage::Queued;
        } else {
            row(e, "out_open_restores_pending_offline");
            op.stage = Stage::Queued;
        }
    } else if online && free && expired_at_restore(s, op, &item) {
        row(e, "out_queued_expired");
        roll_back(op, Cause::Expired, e);
    } else if online && free && slot_free(s) {
        row(e, "out_open_restores");
        op.stage = Stage::PersistingAttempt;
        op.deferred = true;
    } else {
        row(e, "out_open_restores_queued");
        op.stage = Stage::Queued;
    }
}

pub fn restored(s: &mut State, i: &Input, e: &mut Vec<Effect>) {
    let (epoch, next_seq, queue, pending, lost) = match i { Input::Restored { epoch, next_seq, queue, pending, pending_lost } => (*epoch, *next_seq, queue, pending, *pending_lost), _ => return };
    if s.phase != Phase::Restoring || !s.epoch_is(epoch) {
        let listed = s.ops.values().any(|o| o.stage != Stage::Done);
        if listed && matches!(s.phase, Phase::Classified | Phase::Ready) && s.epoch_is(epoch) { row(e, "out_prop_stray_input_ignored"); } else { row(e, "rule_R4_foreign"); }
        return;
    }
    row(e, "out_restored");
    s.pending_lost = lost;
    for p in pending { s.pending_requests.insert(*p); }
    s.next_seq = next_seq;
    match queue {
        Err(()) => {
            row(e, "out_open_queue_unreadable");
            s.queue_failed = true;
        }
        Ok(items) => {
            let mut items = items.clone();
            items.sort_by_key(|i| i.op_id);
            for item in items {
                let mut op = blank_op(item.op_id, &item.spec);
                op.item = Some(item.clone());
                op.origin = item.spec.origin;
                if item.op_id >= s.next_seq { s.next_seq = item.op_id + 1; }
                classify_item(s, &mut op, e);
                s.ops.insert(op.id, op);
            }
        }
    }
    s.phase = Phase::Classified;
    e.push(Effect::Lifecycle("RestoreClassified{Outbox,epoch}"));
    let held = std::mem::take(&mut s.held);
    for h in held { super::inner(s, &h, e); }
}

pub fn register(s: &mut State, i: &Input, e: &mut Vec<Effect>) {
    if let Input::RegisterOperations { declarations } = i {
        for d in declarations { s.declared.insert(d.clone()); }
    }
    match s.phase {
        Phase::Restoring => row(e, "out_restoring_register"),
        Phase::Classified | Phase::Ready => {
            let ids: Vec<OpId> = s.ops.keys().copied().collect();
            let mut other = false;
            for id in ids {
                let mut op = s.ops.remove(&id).unwrap();
                if op.stage == Stage::WaitingDeclaration { super::op_input(s, &mut op, i, e); } else { other = true; }
                s.ops.insert(id, op);
            }
            if other { row(e, "out_register_other"); }
        }
        Phase::NoOwner | Phase::Ended => row(e, "rule_no_owner"),
    }
}

pub fn restore_ready(s: &mut State, epoch: Epoch, e: &mut Vec<Effect>) {
    if s.phase == Phase::Classified && s.epoch_is(epoch) {
        row(e, "out_restore_ready");
        s.phase = Phase::Ready;
        let ids: Vec<OpId> = s.ops.keys().copied().collect();
        for id in ids {
            let op = s.ops.get_mut(&id).unwrap();
            if op.deferred && op.stage == Stage::PersistingAttempt {
                op.deferred = false;
                e.push(Effect::Store { op: id, what: "Commit{Intent{attempts+1}}" });
            }
        }
    } else if matches!(s.phase, Phase::Restoring | Phase::Ready | Phase::Classified) {
        row(e, "out_restore_ready_other");
    } else {
        row(e, "rule_R4_foreign");
    }
}
