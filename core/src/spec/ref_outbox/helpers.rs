//! Shared conditions and transitions of the Outbox reference.

use super::types::*;

/// The row-queue version: a counter of the changes to the set of unfinished operations of the row (D-259).
pub fn selection_version(s: &State, model: &str, row_id: &str) -> u64 {
    let row_id = s.aliases.get(&(model.to_string(), row_id.to_string())).map(|r| r.as_str()).unwrap_or(row_id);
    s.selection_versions.get(&(model.to_string(), row_id.to_string())).copied().unwrap_or(0)
}

pub fn bump_selection_row(s: &mut State, model: &str, row_id: &str) {
    s.selection_clock += 1;
    s.selection_versions.insert((model.to_string(), row_id.to_string()), s.selection_clock);
}

pub fn row_free(s: &State, op: &Op) -> bool {
    !s.ops.values().any(|o| o.id != op.id && o.model == op.model && o.row_id == op.row_id && o.id < op.id && o.stage != Stage::Done)
}

pub fn holds_slot(o: &Op) -> bool {
    o.stage == Stage::PersistingAttempt || (o.stage == Stage::Sending && !o.waiting_retry)
}

/// Slots held by the other operations (the operation under evaluation is outside `s.ops`).
pub fn slots_taken(s: &State) -> usize { s.ops.values().filter(|o| holds_slot(o)).count() }
pub fn slot_free(s: &State) -> bool { slots_taken(s) < DRAIN_LIMIT }

pub fn expired(s: &State, op: &Op) -> bool {
    match op.expires_after {
        Some(d) => s.now.saturating_sub(op.created_at) > d && op.attempts == 1 && !op.ever_sent,
        None => false,
    }
}

pub fn goes_out(s: &State, op: &Op) -> bool { s.online_now() && row_free(s, op) && s.phase == Phase::Ready }

pub fn mutation_outcome_name(o: Outcome) -> &'static str {
    match o {
        Outcome::Confirmed => "MutationOutcome(Confirmed)",
        Outcome::Rejected => "MutationOutcome(Rejected)",
        Outcome::Failed => "MutationOutcome(Failed)",
        Outcome::Discarded => "MutationOutcome(Discarded)",
        Outcome::Ended(EndCause::SessionEnded) => "MutationOutcome(Ended(SessionEnded))",
        Outcome::Ended(EndCause::Closed) => "MutationOutcome(Ended(Closed))",
    }
}

fn outcome_reply(o: Outcome) -> &'static str {
    match o {
        Outcome::Confirmed => "Outcome:Confirmed",
        Outcome::Rejected => "Outcome:Rejected",
        Outcome::Failed => "Outcome:Failed",
        Outcome::Discarded => "Outcome:Discarded",
        Outcome::Ended(EndCause::SessionEnded) => "Outcome:Ended(SessionEnded)",
        Outcome::Ended(EndCause::Closed) => "Outcome:Ended(Closed)",
    }
}

pub fn transfer_outcome(op: &Op, o: Outcome, e: &mut Vec<Effect>) {
    if op.origin != OpOrigin::Api { e.push(Effect::Transfer { op: op.id, what: mutation_outcome_name(o) }); }
}

/// Ends the operation: stored outcome, every waiting `outcome` answered once.
pub fn finish(op: &mut Op, o: Outcome, e: &mut Vec<Effect>) {
    op.stage = Stage::Done;
    op.waiting_retry = false;
    op.outcome = Some(o);
    for _ in 0..op.outcome_waiters { reply(e, Addr::Call, op.id, outcome_reply(o)); }
    op.outcome_waiters = 0;
}

/// Answers every waiter of the operation with the `Ended(cause)` of the input once.
pub fn interrupt_waiters(op: &mut Op, cause: EndCause, e: &mut Vec<Effect>) {
    let what = end_err(cause);
    if op.submit_reply { reply(e, Addr::Call, op.id, what); op.submit_reply = false; }
    for _ in 0..op.retry_waiter { reply(e, Addr::Call, op.id, what); }
    op.retry_waiter = 0;
    for w in std::mem::take(&mut op.discard_waiters) { reply(e, w, op.id, what); }
}

pub fn end_err(cause: EndCause) -> &'static str {
    match cause { EndCause::SessionEnded => "Err(SessionEnded)", EndCause::Closed => "Err(Closed)" }
}

pub fn send_mutate(op: &mut Op, e: &mut Vec<Effect>) {
    op.stage = Stage::Sending;
    op.waiting_retry = false;
    op.ever_sent = true;
    op.cycle_sent = true;
    e.push(Effect::Session { op: op.id, what: "Mutate" });
}

pub fn persist_attempt(op: &mut Op, e: &mut Vec<Effect>) {
    op.stage = Stage::PersistingAttempt;
    op.waiting_retry = false;
    e.push(Effect::Store { op: op.id, what: "Commit{Intent{attempts+1}}" });
}

pub fn mark_failed(op: &mut Op, err: ErrKind, e: &mut Vec<Effect>) {
    op.stage = Stage::MarkingFailed;
    op.waiting_retry = false;
    op.error = Some(err);
    e.push(Effect::Store { op: op.id, what: "Commit{MarkFailed}" });
}

pub fn roll_back(op: &mut Op, cause: Cause, e: &mut Vec<Effect>) {
    op.stage = Stage::RollingBack;
    op.waiting_retry = false;
    op.cause = cause;
    let what = if op.pending_owned && cause == Cause::Discard { "Commit{Rollback,RemoveIntent,ReleasePendingOwner}" } else { "Commit{Rollback,RemoveIntent}" };
    e.push(Effect::Store { op: op.id, what });
}

pub fn requeue(op: &mut Op, e: &mut Vec<Effect>) {
    op.stage = Stage::Queued;
    op.waiting_retry = false;
    e.push(Effect::Store { op: op.id, what: "Commit{Requeue}" });
}

pub fn blank_op(id: OpId, spec: &OpSpec) -> Op {
    Op {
        id, model: spec.model.clone(), row_id: spec.row_id.clone(), action: spec.action.clone(), expires_after: spec.expires_after,
        created_at: spec.created_at, write_failures: 0, attempts: spec.attempts, cycle_id: 1, request_id: 0, origin: spec.origin, stage: Stage::Submitting,
        waiting_retry: false, rowless: false, keep: spec.keep, effects_empty: spec.effects_empty, destroy: spec.destroy, response_ignore: spec.response_ignore,
        batch: spec.batch, error: None, cause: Cause::Refusal, deferred: false, restored: false, ever_sent: false, cycle_sent: false,
        pending_owned: false, release_sent: false, submit_reply: false, retry_waiter: 0, swap: None, discard_waiters: Vec::new(), outcome_waiters: 0,
        item: None, land: LandFacts::default(), outcome: None, cancels: None,
    }
}

pub fn fresh_request(s: &mut State) -> u64 { let r = s.next_request; s.next_request += 1; r }

/// Operations of the same row after `op` that show `blocked_by`: every non-Done operation of the row with a larger seq.
pub fn queued_behind(s: &State, op: &Op) -> Vec<OpId> {
    s.ops.values().filter(|o| o.model == op.model && o.row_id == op.row_id && o.id > op.id && o.stage == Stage::Queued).map(|o| o.id).collect()
}

/// The smallest non-Done operation of the row (rule of the table).
pub fn address(s: &State, model: &str, row_id: &str, action: &Option<String>) -> Option<OpId> {
    let row_id = s.aliases.get(&(model.to_string(), row_id.to_string())).map(|r| r.as_str()).unwrap_or(row_id);
    s.ops.values()
        .filter(|o| o.model == model && o.row_id == row_id && o.stage != Stage::Done)
        .filter(|o| action.as_ref().is_none_or(|a| &o.action == a))
        .map(|o| o.id)
        .min()
}

pub fn release_if_owned(op: &mut Op, e: &mut Vec<Effect>) {
    if op.pending_owned && !op.release_sent {
        op.release_sent = true;
        e.push(Effect::Session { op: op.id, what: "Release{request_id}" });
    }
}

/// Earlier operation of the same row in Failed.
pub fn behind_failed(s: &State, op: &Op) -> bool {
    s.ops.values().any(|o| o.model == op.model && o.row_id == op.row_id && o.id < op.id && o.stage == Stage::Failed)
}

/// A `destroy` cancelling an unsent `create` confirms once that create's rollback commits; the later operations of the chain end `Discarded`.
pub fn finish_cancelled(s: &mut State, created: OpId, e: &mut Vec<Effect>) {
    let members: Vec<OpId> = s.ops.values().filter(|o| o.id != created && o.cancels == Some(created)).map(|o| o.id).collect();
    for member in members {
        let mut op = s.ops.remove(&member).unwrap();
        if op.stage == Stage::Submitting {
            if op.submit_reply { reply(e, Addr::Call, op.id, "Receipt"); op.submit_reply = false; }
            finish(&mut op, Outcome::Confirmed, e);
        } else {
            finish(&mut op, Outcome::Discarded, e);
        }
        s.ops.insert(member, op);
    }
}
