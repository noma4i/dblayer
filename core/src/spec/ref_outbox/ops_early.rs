//! Rows of the operation states Submitting, Queued, PersistingAttempt, Sending.

use super::helpers::*;
use super::types::*;

fn foreign(e: &mut Vec<Effect>) { row(e, "rule_R2_foreign"); }

pub fn submitting(s: &mut State, op: &mut Op, i: &Input, e: &mut Vec<Effect>) {
    match i {
        Input::Committed { .. } if op.cancels.is_some() => foreign(e),
        Input::Committed { .. } => {
            row(e, "out_submitting_committed_revision");
            let out = goes_out(s, op) && slot_free(s);
            if out {
                row(e, "out_submitting_committed");
                send_mutate(op, e);
            } else {
                row(e, "out_submitting_committed_queued");
                op.stage = Stage::Queued;
            }
            op.submit_reply = false;
            reply(e, Addr::Call, op.id, "Receipt");
            for _ in 0..op.retry_waiter { reply(e, Addr::Call, op.id, "Receipt"); }
            op.retry_waiter = 0;
        }
        Input::Rejected { reason: Reason::StaleGeneration(g), .. } => {
            row(e, "out_submitting_stale_generation");
            let _ = g;
            e.push(Effect::Store { op: op.id, what: "Commit{Intent,basis=generations}" });
        }
        Input::Rejected { reason, .. } => {
            row(e, "out_submitting_rejected");
            let what = match reason { Reason::NoSession => "Err(NoSession)", Reason::Closed => "Err(Closed)", Reason::Storage => "Err(Storage)", _ => "Err(Invalid)" };
            op.submit_reply = false;
            op.stage = Stage::Done;
            reply(e, Addr::Call, op.id, what);
        }
        Input::Discard { reply_to, .. } => { row(e, "out_submitting_discard"); in_flight(op, *reply_to, e); }
        Input::Retry { reply_to, .. } => { row(e, "out_submitting_retry"); let _ = reply_to; op.retry_waiter += 1; }
        Input::Online(_) | Input::Response { .. } | Input::TransportError { .. } | Input::Dropped { .. } | Input::Tick { .. } => row(e, "out_submitting_other"),
        _ => foreign(e),
    }
}

pub fn in_flight(op: &Op, to: Addr, e: &mut Vec<Effect>) { refuse(op, to, "Err(InFlight)", "DiscardOutcome{Err(InFlight)}", e); }

pub fn refuse(op: &Op, to: Addr, api: &'static str, transfer: &'static str, e: &mut Vec<Effect>) {
    reply(e, to, op.id, if let Addr::Transfer(_) = to { transfer } else { api });
}

/// One evaluation of a Queued operation on `Online(true)`, `Committed` or `Done` of any operation, a freed slot or a freed row.
pub fn queued_drain(s: &mut State, op: &mut Op, e: &mut Vec<Effect>) {
    if s.phase == Phase::Classified {
        row(e, "rule_restored_effects_wait_restore_ready");
        return;
    }
    let online = s.online_now();
    let free = row_free(s, op);
    let slot = slot_free(s);
    let exp = expired(s, op);
    if online && free && slot && !exp && op.attempts >= 1 && !op.cycle_sent {
        row(e, "out_queued_send");
        send_mutate(op, e);
    } else if online && free && exp {
        row(e, "out_queued_expired");
        roll_back(op, Cause::Expired, e);
    } else if online && free && !slot {
        row(e, "out_queued_drain_limit");
    } else if online && free && slot && op.attempts < MAX_ATTEMPTS {
        row(e, "out_queued_persist_attempt");
        persist_attempt(op, e);
    } else if online && free && slot && op.attempts >= MAX_ATTEMPTS {
        row(e, "out_queued_exhausted");
        mark_failed(op, ErrKind::Exhausted, e);
    } else if behind_failed(s, op) {
        row(e, "out_queued_blocked");
    } else {
        row(e, "out_queued_wait");
    }
}

pub fn queued(_s: &mut State, op: &mut Op, i: &Input, e: &mut Vec<Effect>) {
    match i {
        Input::Online(true) => {}
        Input::Online(false) => row(e, "out_queued_offline"),
        Input::Discard { reply_to, .. } => {
            row(e, "out_queued_discard");
            if let Addr::Transfer(_) = reply_to { reply(e, *reply_to, op.id, "DiscardOutcome{Ok}"); } else { op.discard_waiters.push(*reply_to); }
            roll_back(op, Cause::Discard, e);
        }
        Input::Retry { .. } => { row(e, "out_queued_retry"); reply(e, Addr::Call, op.id, "Receipt"); }
        Input::Response { .. } | Input::TransportError { .. } | Input::Dropped { .. } | Input::Tick { .. } | Input::Rejected { .. } | Input::Committed { .. } => row(e, "out_queued_stray"),
        _ => foreign(e),
    }
}

pub fn persisting(s: &mut State, op: &mut Op, i: &Input, e: &mut Vec<Effect>) {
    match i {
        Input::Committed { .. } => {
            op.attempts += 1;
            if goes_out(s, op) {
                row(e, "out_persisting_attempt_committed");
                send_mutate(op, e);
            } else {
                row(e, "out_persisting_attempt_committed_offline");
                requeue(op, e);
            }
        }
        Input::Rejected { reason: Reason::Storage, .. } => {
            row(e, "out_persisting_attempt_storage");
            e.push(Effect::Schedule { op: op.id, what: "Tick(backoff)" });
        }
        Input::Rejected { reason: Reason::StaleGeneration(_), .. } => {
            row(e, "out_persisting_attempt_stale_generation");
            e.push(Effect::Store { op: op.id, what: "Commit{Intent{attempts+1},basis=generations}" });
        }
        Input::Rejected { reason: Reason::NoSession, .. } => {
            row(e, "out_persisting_attempt_no_session");
            transfer_outcome(op, Outcome::Ended(EndCause::SessionEnded), e);
            finish(op, Outcome::Ended(EndCause::SessionEnded), e);
        }
        Input::Rejected { reason: Reason::Closed, .. } => {
            row(e, "out_persisting_attempt_no_session");
            transfer_outcome(op, Outcome::Ended(EndCause::Closed), e);
            finish(op, Outcome::Ended(EndCause::Closed), e);
        }
        Input::Rejected { reason: Reason::Invalid, .. } => {
            row(e, "out_persisting_attempt_invalid");
            mark_failed(op, ErrKind::Internal, e);
        }
        Input::Tick { cycle_id, .. } if *cycle_id == op.cycle_id => {
            row(e, "out_persisting_attempt_tick");
            e.push(Effect::Store { op: op.id, what: "Commit{Intent{attempts+1}}" });
        }
        Input::Tick { .. } => row(e, "out_persisting_attempt_other"),
        Input::Discard { reply_to, .. } => { row(e, "out_persisting_attempt_discard"); in_flight(op, *reply_to, e); }
        Input::Retry { .. } => { row(e, "out_persisting_attempt_retry"); reply(e, Addr::Call, op.id, "Receipt"); }
        Input::Online(_) | Input::Response { .. } | Input::TransportError { .. } | Input::Dropped { .. } => row(e, "out_persisting_attempt_other"),
        _ => foreign(e),
    }
}

fn permanent(op: &mut Op, e: &mut Vec<Effect>) {
    if !op.keep {
        row(e, "out_sending_reject_rollback");
        roll_back(op, Cause::Refusal, e);
    } else {
        row(e, "out_sending_reject_keep");
        mark_failed(op, ErrKind::Server, e);
    }
}

fn temporary(s: &State, op: &mut Op, e: &mut Vec<Effect>) {
    if op.attempts >= MAX_ATTEMPTS {
        row(e, "out_sending_exhausted_keep");
        mark_failed(op, ErrKind::Exhausted, e);
    } else if s.online_now() {
        row(e, "out_sending_temp");
        op.waiting_retry = true;
        e.push(Effect::Schedule { op: op.id, what: "Tick(backoff)" });
    } else {
        row(e, "out_sending_temp_offline");
        requeue(op, e);
    }
}

fn land(op: &mut Op, f: LandFacts, e: &mut Vec<Effect>) {
    op.stage = Stage::Landing;
    op.land = f;
    let _ = e;
}

pub fn sending(s: &mut State, op: &mut Op, i: &Input, e: &mut Vec<Effect>) {
    match i {
        Input::Response { cycle_id, resp, swap, .. } if *cycle_id == op.cycle_id => match resp {
            Resp::Data(f) => {
                let mut f = *f;
                if swap.is_some() { f.swap = true; }
                op.swap = swap.clone();
                let f = &f;
                if op.response_ignore {
                    row(e, "out_sending_response_ignore");
                    e.push(Effect::Store { op: op.id, what: "Commit{RemoveIntent,DropEffects,RemoveAnswer,upsert}" });
                } else if op.batch {
                    row(e, "out_sending_response_batch_items");
                    e.push(Effect::Store { op: op.id, what: "Commit{items,Swap,RemoveIntent,DropEffects,RemoveAnswer}" });
                } else {
                    row(e, "out_sending_response");
                    e.push(Effect::Store { op: op.id, what: "Commit{rows,RemoveIntent,DropEffects,RemoveAnswer}" });
                }
                land(op, *f, e);
            }
            Resp::Temporary { has_sequence: false } => { row(e, "out_sending_response_temporary"); temporary(s, op, e); }
            Resp::Temporary { has_sequence: true } | Resp::Permanent => {
                if op.effects_empty {
                    row(e, "out_sending_refused_without_effects");
                    if op.keep {
                        mark_failed(op, ErrKind::Server, e);
                    } else {
                        op.stage = Stage::RollingBack;
                        op.cause = Cause::Refusal;
                        e.push(Effect::Store { op: op.id, what: "Commit{RemoveIntent,RemoveAnswer}" });
                    }
                } else {
                    row(e, "out_sending_response_errors");
                    permanent(op, e);
                }
            }
            Resp::NotFound => {
                if op.destroy {
                    row(e, "out_sending_destroy_not_found");
                    e.push(Effect::Store { op: op.id, what: "Commit{delete row,RemoveIntent,DropEffects,RemoveAnswer}" });
                    land(op, LandFacts::default(), e);
                } else if op.effects_empty {
                    row(e, "out_sending_refused_without_effects");
                    if op.keep { mark_failed(op, ErrKind::Server, e); } else { op.stage = Stage::RollingBack; op.cause = Cause::Refusal; e.push(Effect::Store { op: op.id, what: "Commit{RemoveIntent,RemoveAnswer}" }); }
                } else {
                    row(e, "out_sending_response_errors");
                    permanent(op, e);
                }
            }
        },
        Input::TransportError { cycle_id, temporary: t, .. } if *cycle_id == op.cycle_id => {
            if *t { temporary(s, op, e); } else { permanent(op, e); }
        }
        Input::Dropped { cycle_id, .. } if *cycle_id == op.cycle_id => temporary(s, op, e),
        Input::Response { .. } | Input::TransportError { .. } | Input::Dropped { .. } => row(e, "out_sending_stray"),
        Input::Tick { cycle_id, .. } if *cycle_id == op.cycle_id && !op.waiting_retry => row(e, "out_sending_tick_not_waiting"),
        Input::Tick { cycle_id, .. } if *cycle_id == op.cycle_id => {
            if !slot_free(s) {
                row(e, "out_sending_tick_no_slot");
                requeue(op, e);
            } else if s.online_now() {
                row(e, "out_sending_tick");
                persist_attempt(op, e);
            } else {
                row(e, "out_sending_tick_offline");
                op.stage = Stage::Queued;
                op.waiting_retry = false;
            }
        }
        Input::Tick { .. } => foreign(e),
        Input::Online(false) => row(e, "out_sending_offline"),
        Input::Online(true) => row(e, "out_sending_online"),
        Input::Discard { reply_to, .. } => { row(e, "out_sending_discard"); in_flight(op, *reply_to, e); }
        Input::Retry { .. } => { row(e, "out_sending_retry"); reply(e, Addr::Call, op.id, "Receipt"); }
        Input::Committed { .. } | Input::Rejected { .. } => row(e, "out_sending_other"),
        _ => foreign(e),
    }
}
