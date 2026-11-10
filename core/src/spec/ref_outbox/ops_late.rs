//! Rows of the operation states Landing, RollingBack, MarkingFailed, MarkingRetry, Failed, Done, WaitingDeclaration, and the rules for `outcome`, `EpochEnded`, `Closing`.

use super::helpers::*;
use super::ops_early::{in_flight, refuse};
use super::types::*;

fn foreign(e: &mut Vec<Effect>) { row(e, "rule_R2_foreign"); }

pub fn landing(s: &mut State, op: &mut Op, i: &Input, e: &mut Vec<Effect>) {
    match i {
        Input::Committed { .. } => {
            let f = op.land;
            if f.root_present && f.root_other_row && !f.swap { row(e, "out_landing_committed_root_row"); }
            if f.returns && !f.root_present { row(e, "out_landing_committed_result_no_root"); }
            if f.returns && f.root_present { row(e, "out_landing_committed_result"); }
            if f.swap {
                row(e, "out_landing_committed_swap");
                e.push(Effect::Store { op: op.id, what: "Swap{temp,real}+aliases" });
                if let Some((temp, real)) = op.swap.clone() {
                    s.aliases.insert((op.model.clone(), temp.clone()), real.clone());
                    for o in s.ops.values_mut() { if o.model == op.model && o.row_id == temp { o.row_id = real.clone(); } }
                    op.row_id = real;
                }
            }
            if op.origin != OpOrigin::Api { row(e, "out_landing_committed_transfer"); }
            if f.refetch { row(e, "out_landing_committed_stale"); e.push(Effect::Store { op: op.id, what: "Stale{families}" }); }
            row(e, "out_landing_committed");
            transfer_outcome(op, Outcome::Confirmed, e);
            finish(op, Outcome::Confirmed, e);
        }
        Input::Rejected { reason: Reason::StaleGeneration(_), .. } => { row(e, "out_landing_stale_generation"); e.push(Effect::Store { op: op.id, what: "Commit{Land,basis=generations}" }); }
        Input::Rejected { reason: Reason::NoSession, .. } => {
            row(e, "out_landing_no_session");
            transfer_outcome(op, Outcome::Ended(EndCause::SessionEnded), e);
            finish(op, Outcome::Ended(EndCause::SessionEnded), e);
        }
        Input::Rejected { reason: Reason::Closed, .. } => {
            row(e, "out_landing_no_session");
            transfer_outcome(op, Outcome::Ended(EndCause::Closed), e);
            finish(op, Outcome::Ended(EndCause::Closed), e);
        }
        Input::Rejected { reason: Reason::Storage, .. } => { row(e, "out_landing_storage"); e.push(Effect::Schedule { op: op.id, what: "Tick(backoff)" }); }
        Input::Rejected { reason: Reason::Invalid, .. } => { row(e, "out_landing_invalid"); mark_failed(op, ErrKind::Internal, e); }
        Input::Tick { cycle_id, .. } if *cycle_id == op.cycle_id => { row(e, "out_landing_tick"); e.push(Effect::Store { op: op.id, what: "Commit{Land,basis=generation}" }); }
        Input::Tick { .. } => foreign(e),
        Input::Discard { reply_to, .. } => { row(e, "out_landing_discard"); in_flight(op, *reply_to, e); }
        Input::Retry { .. } => { row(e, "out_landing_retry"); reply(e, Addr::Call, op.id, "Receipt"); }
        Input::Online(_) | Input::ModelsReset { .. } | Input::Response { .. } | Input::TransportError { .. } | Input::Dropped { .. } => row(e, "out_landing_other"),
        _ => foreign(e),
    }
}

pub fn rolling_back(s: &mut State, op: &mut Op, i: &Input, e: &mut Vec<Effect>) {
    match i {
        Input::Committed { .. } => {
            let (o, id) = match op.cause {
                Cause::DiscardRestoreFailed => (Outcome::Discarded, "out_discard_restore_failed_committed"),
                Cause::Discard => (Outcome::Discarded, "out_rollback_committed_discard"),
                Cause::Refusal | Cause::Expired => (Outcome::Rejected, "out_rollback_committed"),
            };
            row(e, id);
            release_if_owned(op, e);
            for w in std::mem::take(&mut op.discard_waiters) { reply(e, w, op.id, "Ok"); }
            if op.cause != Cause::DiscardRestoreFailed { transfer_outcome(op, o, e); }
            finish(op, o, e);
            if op.cancels == Some(op.id) { finish_cancelled(s, op.id, e); }
        }
        Input::Rejected { reason: Reason::StaleGeneration(_), .. } => { row(e, "out_rollback_stale_generation"); e.push(Effect::Store { op: op.id, what: "Commit{Rollback,basis=generations}" }); }
        Input::Rejected { reason: Reason::NoSession, .. } => {
            row(e, "out_rollback_no_session");
            interrupt_waiters(op, EndCause::SessionEnded, e);
            transfer_outcome(op, Outcome::Ended(EndCause::SessionEnded), e);
            finish(op, Outcome::Ended(EndCause::SessionEnded), e);
        }
        Input::Rejected { reason: Reason::Closed, .. } => {
            row(e, "out_rollback_no_session");
            interrupt_waiters(op, EndCause::Closed, e);
            transfer_outcome(op, Outcome::Ended(EndCause::Closed), e);
            finish(op, Outcome::Ended(EndCause::Closed), e);
        }
        Input::Rejected { reason: Reason::Storage, .. } => { row(e, "out_rollback_storage"); e.push(Effect::Schedule { op: op.id, what: "Tick(backoff)" }); }
        Input::Rejected { reason: Reason::Invalid, .. } => {
            row(e, "out_rollback_invalid");
            for w in std::mem::take(&mut op.discard_waiters) { reply(e, w, op.id, "Err(Internal)"); }
            mark_failed(op, ErrKind::Internal, e);
        }
        Input::Tick { cycle_id, .. } if *cycle_id == op.cycle_id => { row(e, "out_rollback_tick"); e.push(Effect::Store { op: op.id, what: "Commit{Rollback}" }); }
        Input::Tick { .. } => foreign(e),
        Input::Discard { reply_to, .. } => {
            row(e, "out_rollback_discard");
            if let Addr::Transfer(_) = reply_to { reply(e, *reply_to, op.id, "DiscardOutcome{Ok}"); } else { op.discard_waiters.push(*reply_to); }
        }
        Input::Retry { reply_to, .. } => { row(e, "out_rollback_retry"); refuse(op, *reply_to, "Err(InFlight)", "Err(InFlight)", e); }
        Input::Online(_) | Input::ModelsReset { .. } | Input::Response { .. } | Input::TransportError { .. } | Input::Dropped { .. } => row(e, "out_rollback_other"),
        _ => foreign(e),
    }
}

pub fn marking_failed(_s: &mut State, op: &mut Op, i: &Input, e: &mut Vec<Effect>) {
    match i {
        Input::Committed { .. } => {
            row(e, "out_marking_failed_committed");
            op.stage = Stage::Failed;
            op.outcome = None;
            for _ in 0..op.outcome_waiters { reply(e, Addr::Call, op.id, "Outcome:Failed"); }
            op.outcome_waiters = 0;
            transfer_outcome(op, Outcome::Failed, e);
        }
        Input::Rejected { reason: Reason::Storage, .. } => { row(e, "out_marking_failed_storage"); e.push(Effect::Schedule { op: op.id, what: "Tick(backoff)" }); }
        Input::Rejected { reason: Reason::NoSession, .. } => {
            row(e, "out_marking_failed_no_session");
            transfer_outcome(op, Outcome::Ended(EndCause::SessionEnded), e);
            finish(op, Outcome::Ended(EndCause::SessionEnded), e);
        }
        Input::Rejected { reason: Reason::Closed, .. } => {
            row(e, "out_marking_failed_no_session");
            transfer_outcome(op, Outcome::Ended(EndCause::Closed), e);
            finish(op, Outcome::Ended(EndCause::Closed), e);
        }
        Input::Rejected { reason: Reason::StaleGeneration(_), .. } => { row(e, "out_marking_failed_stale_generation"); e.push(Effect::Store { op: op.id, what: "Commit{MarkFailed,basis=generations}" }); }
        Input::Rejected { reason: Reason::Invalid, .. } => {
            row(e, "out_marking_failed_invalid");
            op.stage = Stage::Failed;
            for _ in 0..op.outcome_waiters { reply(e, Addr::Call, op.id, "Outcome:Failed"); }
            op.outcome_waiters = 0;
            transfer_outcome(op, Outcome::Failed, e);
        }
        Input::Tick { cycle_id, .. } if *cycle_id == op.cycle_id => { row(e, "out_marking_failed_tick"); e.push(Effect::Store { op: op.id, what: "Commit{MarkFailed}" }); }
        Input::Tick { .. } => foreign(e),
        Input::Discard { reply_to, .. } => { row(e, "out_marking_failed_refuses"); in_flight(op, *reply_to, e); }
        Input::Retry { reply_to, .. } => { row(e, "out_marking_failed_refuses"); refuse(op, *reply_to, "Err(InFlight)", "Err(InFlight)", e); }
        Input::Online(_) | Input::ModelsReset { .. } | Input::Response { .. } | Input::TransportError { .. } | Input::Dropped { .. } => row(e, "out_marking_failed_other"),
        _ => foreign(e),
    }
}

pub fn marking_retry(s: &mut State, op: &mut Op, i: &Input, e: &mut Vec<Effect>) {
    match i {
        Input::Committed { .. } => {
            op.cycle_id += 1;
            op.attempts = 1;
            op.cycle_sent = false;
            op.error = None;
            if !op.pending_owned { op.request_id = fresh_request(s); }
            for _ in 0..op.retry_waiter { reply(e, Addr::Call, op.id, "Receipt"); }
            op.retry_waiter = 0;
            if goes_out(s, op) {
                row(e, "out_marking_retry_committed");
                send_mutate(op, e);
            } else {
                row(e, "out_marking_retry_committed_queued");
                requeue(op, e);
            }
        }
        Input::Rejected { reason: Reason::Storage, .. } => { row(e, "out_marking_retry_storage"); op.stage = Stage::Failed; op.retry_waiter = 0; reply(e, Addr::Call, op.id, "Err(Storage)"); }
        Input::Rejected { reason: reason @ (Reason::NoSession | Reason::Closed), .. } => {
            row(e, "out_marking_retry_no_session");
            op.retry_waiter = 0;
            let cause = if matches!(reason, Reason::Closed) { EndCause::Closed } else { EndCause::SessionEnded };
            reply(e, Addr::Call, op.id, end_err(cause));
            op.stage = Stage::Done;
        }
        Input::Rejected { reason: Reason::StaleGeneration(_), .. } => { row(e, "out_marking_retry_stale_generation"); e.push(Effect::Store { op: op.id, what: "Commit{ClearMark,Intent,basis=generations}" }); }
        Input::Rejected { reason: Reason::Invalid, .. } => { row(e, "out_marking_retry_invalid"); op.stage = Stage::Failed; op.retry_waiter = 0; reply(e, Addr::Call, op.id, "Err(Internal)"); }
        Input::Discard { reply_to, .. } => { row(e, "out_marking_retry_refuses"); in_flight(op, *reply_to, e); }
        Input::Retry { reply_to, .. } => { row(e, "out_marking_retry_refuses"); refuse(op, *reply_to, "Err(InFlight)", "Err(InFlight)", e); }
        Input::Online(_) | Input::ModelsReset { .. } | Input::Response { .. } | Input::TransportError { .. } | Input::Dropped { .. } | Input::Tick { .. } => row(e, "out_marking_retry_other"),
        _ => foreign(e),
    }
}

pub fn failed(_s: &mut State, op: &mut Op, i: &Input, e: &mut Vec<Effect>) {
    match i {
        Input::Retry { reply_to, .. } => match op.error {
            Some(ErrKind::RestoreFailed) => { row(e, "out_failed_retry_restore_failed"); reply(e, *reply_to, op.id, "Err(RestoreFailed)"); }
            Some(ErrKind::StateLost) => { row(e, "out_failed_retry_state_lost"); reply(e, *reply_to, op.id, "Err(Session{StateLost})"); }
            _ => {
                row(e, "out_failed_retry");
                op.stage = Stage::MarkingRetry;
                op.retry_waiter = 1;
                e.push(Effect::Store { op: op.id, what: "Commit{ClearMark,Intent{attempts=1,cycle_id+1}}" });
            }
        },
        Input::Discard { reply_to, .. } => {
            if op.error == Some(ErrKind::RestoreFailed) {
                row(e, "out_failed_discard_restore_failed");
                if let Addr::Transfer(_) = reply_to { reply(e, *reply_to, op.id, "DiscardOutcome{Ok}"); } else { op.discard_waiters.push(*reply_to); }
                op.stage = Stage::RollingBack;
                op.cause = Cause::DiscardRestoreFailed;
                e.push(Effect::Store { op: op.id, what: "Commit{RemoveIntent,Rollback(empty),DiscardRestoreFailed}" });
            } else {
                row(e, "out_failed_discard");
                if let Addr::Transfer(_) = reply_to { reply(e, *reply_to, op.id, "DiscardOutcome{Ok}"); } else { op.discard_waiters.push(*reply_to); }
                roll_back(op, Cause::Discard, e);
            }
        }
        Input::Online(_) | Input::ModelsReset { .. } | Input::Response { .. } | Input::TransportError { .. } | Input::Dropped { .. } | Input::Tick { .. } | Input::Committed { .. } | Input::Rejected { .. } => row(e, "out_failed_other"),
        _ => foreign(e),
    }
}

pub fn done(_s: &mut State, op: &mut Op, i: &Input, e: &mut Vec<Effect>) {
    match i {
        Input::Discard { reply_to, .. } => { row(e, "out_done_refuses"); refuse(op, *reply_to, "Err(NotFound)", "DiscardOutcome{Err(NotFound)}", e); }
        Input::Retry { reply_to, .. } => { row(e, "out_done_refuses"); refuse(op, *reply_to, "Err(NotFound)", "Err(NotFound)", e); }
        _ => row(e, "out_done"),
    }
}

pub fn waiting_declaration(s: &mut State, op: &mut Op, i: &Input, e: &mut Vec<Effect>) {
    match i {
        Input::RegisterOperations { .. } => {
            if s.declared.contains(&(op.model.clone(), op.action.clone())) {
                row(e, "out_register_rechecks_waiting");
                super::restore::classify_item(s, op, e);
            } else {
                row(e, "out_register_still_waiting");
            }
        }
        Input::Discard { reply_to, .. } => { row(e, "out_waiting_declaration_refuses"); in_flight(op, *reply_to, e); }
        Input::Retry { reply_to, .. } => { row(e, "out_waiting_declaration_refuses"); refuse(op, *reply_to, "Err(InFlight)", "Err(InFlight)", e); }
        Input::Online(_) | Input::ModelsReset { .. } | Input::Response { .. } | Input::TransportError { .. } | Input::Dropped { .. } | Input::Tick { .. } | Input::Committed { .. } | Input::Rejected { .. } => row(e, "out_waiting_declaration_other"),
        _ => foreign(e),
    }
}

/// `outcome(receipt)`: the epoch check is made by the caller (out_outcome_stale).
pub fn outcome(op: &mut Op, r: &Receipt, e: &mut Vec<Effect>) {
    if r.cycle_id != op.cycle_id {
        row(e, "out_outcome_old_cycle");
        reply(e, Addr::Call, op.id, "Outcome:Failed(previous cycle)");
    } else if op.stage == Stage::Done {
        row(e, "out_outcome_done");
        row(e, "boundary_receipt_orders");
        reply(e, Addr::Call, op.id, "Outcome:stored");
    } else if op.stage == Stage::Failed {
        row(e, "out_outcome_failed");
        row(e, "boundary_receipt_orders");
        reply(e, Addr::Call, op.id, "Outcome:Failed");
    } else {
        row(e, "out_outcome_wait");
        op.outcome_waiters += 1;
    }
}

pub fn epoch_ended(op: &mut Op, cause: EndCause, e: &mut Vec<Effect>) {
    if op.stage == Stage::Done { row(e, "out_done"); return; }
    row(e, "out_epoch_ended");
    if op.stage == Stage::Sending { e.push(Effect::Session { op: op.id, what: "Cancel{req_id}" }); }
    interrupt_waiters(op, cause, e);
    transfer_outcome(op, Outcome::Ended(cause), e);
    finish(op, Outcome::Ended(cause), e);
}

/// Closing for one operation. `Drained` is emitted once by the owner.
pub fn closing(op: &mut Op, e: &mut Vec<Effect>) {
    match op.stage {
        Stage::Done => row(e, "out_done_closing"),
        Stage::WaitingDeclaration | Stage::Queued | Stage::Failed => {
            row(e, "out_closing_queued");
            interrupt_waiters(op, EndCause::Closed, e);
            transfer_outcome(op, Outcome::Ended(EndCause::Closed), e);
            finish(op, Outcome::Ended(EndCause::Closed), e);
        }
        _ => {
            row(e, "out_closing");
            if op.stage == Stage::Sending { e.push(Effect::Session { op: op.id, what: "Cancel{req_id}" }); }
            interrupt_waiters(op, EndCause::Closed, e);
            transfer_outcome(op, Outcome::Ended(EndCause::Closed), e);
            finish(op, Outcome::Ended(EndCause::Closed), e);
        }
    }
}

/// `RowRemoved` for an operation of the removed row.
pub fn row_removed(op: &mut Op, e: &mut Vec<Effect>) {
    match op.stage {
        Stage::Queued | Stage::Sending | Stage::Failed => {
            row(e, "out_server_removed_row");
            e.push(Effect::Store { op: op.id, what: "Commit{RemoveOp}" });
            release_if_owned(op, e);
            transfer_outcome(op, Outcome::Discarded, e);
            finish(op, Outcome::Discarded, e);
        }
        _ => row(e, "out_server_removed_row_other"),
    }
}
