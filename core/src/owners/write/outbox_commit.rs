//! Owner `Outbox`: the answers of the Store to the batches of an operation (`Committed`, `Rejected`).


use super::outbox::{Batch, LocalCall, Outbox, OutboxState};
use super::outbox_types::*;

use crate::owners::contract::{Committed, RejectReason, Rejected};


use crate::owners::session::SessionInput;
use crate::runtime::{Effect, Effects, Outbound, ScheduleId};



impl OutboxState {
    pub(super) fn committed(&mut self, committed: Committed, fx: &mut Effects<Outbox>) {
        let Some(batch) = self.batches.remove(&committed.batch) else { return };
        match batch {
            Batch::Member { op_id } => self.member_committed(op_id, fx),
            Batch::Write { reply, call, expire } => {
                Self::reply(fx, reply, OutboxAnswer::Written { revision: committed.revision, shown: committed.shown.into_iter().last().flatten(), files_removed: committed.files_removed });
                if let Some(due) = expire {
                    let LocalCall { model, row, .. } = *call;
                    self.ephemeral.insert(row.clone(), due);
                    let id = ScheduleId(EXPIRE_TIMER + self.next_schedule);
                    self.next_schedule += 1;
                    fx.push(Effect::ScheduleAt { id, at: due, input: OutboxInput::Expire { model, row, due } });
                }
                self.maybe_drained(fx);
            }
            Batch::Operation { op_id, kind } => self.operation_committed(op_id, kind, (committed.revision, committed.shown), fx),
        }
    }

    pub(super) fn operation_committed(&mut self, op_id: OpId, kind: CommitKind, (revision, shown): (i64, Vec<Option<crate::store::Row>>), fx: &mut Effects<Outbox>) {
        let Some(stage) = self.stage_of(op_id) else { return };
        if kind == CommitKind::Cause { return; }
        if let Some(created) = self.ops[&op_id].cancels.filter(|created| *created != op_id && kind == CommitKind::Intent && stage == Stage::RollingBack(Why::Discarded)) {
            if let Some(operation) = self.ops.get_mut(&op_id) { operation.shown = shown.into_iter().last().flatten(); }
            if let Some(reply) = self.submit_replies.remove(&op_id) { Self::reply(fx, reply, OutboxAnswer::Receipt(self.receipt(op_id, revision))); }
            self.finish_cancelled_commit(created, fx);
            return;
        }
        if kind == CommitKind::Rollback && self.ops[&op_id].cancels == Some(op_id) && self.pending_cancelled_intents(op_id) {
            self.cancelled_commits.insert(op_id, revision);
            return;
        }
        if kind == CommitKind::RemoveRow && self.removed.remove(&op_id) {
            self.removing.remove(&op_id);
            if let Some(session) = self.peers.session { fx.push(Effect::Send(Outbound::new(session, SessionInput::Release { req_id: self.ops[&op_id].request_id }))); }
            self.answer_waiters(op_id, &OutboxAnswer::Err(Fault::Ended(crate::ffi::EndCause::Discarded)), fx);
            self.finish(op_id, OutboxOutcome::Discarded, true, fx);
            return;
        }
        if self.removed.contains(&op_id) {
            self.remove_row_operation(op_id, fx);
            return;
        }
        match (kind, stage) {
            (CommitKind::Intent, Stage::Submitting) => {
                let shown = shown.into_iter().last().flatten();
                if let Some(operation) = self.ops.get_mut(&op_id) { operation.shown.clone_from(&shown); }
                let receipt = OutboxAnswer::Receipt(Receipt { optimistic: shown, ..self.receipt(op_id, revision) });
                if let Some(reply) = self.submit_replies.remove(&op_id) {
                    Self::reply(fx, reply, receipt.clone());
                }
                for reply in Self::take_waiters(&mut self.retry_waiters, op_id) {
                    Self::reply(fx, reply, receipt.clone());
                }
                if self.goes_out(op_id) {
                    self.start_sending(op_id, fx);
                } else if let Some(operation) = self.ops.get_mut(&op_id) {
                    operation.stage = Stage::Queued;
                }
            }
            (CommitKind::Attempt, Stage::PersistingAttempt) => {
                if let Some(operation) = self.ops.get_mut(&op_id) {
                    operation.attempts += 1;
                }
                if self.online && !self.row_busy(op_id) {
                    self.start_sending(op_id, fx);
                } else {
                    self.requeue(op_id, fx);
                }
            }
            (CommitKind::Land, Stage::Landing) => {
                if let Some(pending) = self.member_removals.remove(&op_id) {
                    if let Some(transfer) = self.peers.transfer { fx.push(Effect::Send(Outbound::new(transfer, super::transfer::TransferInput::MutationOutcome { transfer_id: pending.member.transfer_id, cycle_id: pending.member.cycle_id, outcome: OutboxOutcome::Discarded, intent_removed: true }))); }
                }
                for (temp, real) in self.ops.get(&op_id).map(|operation| operation.swaps.clone()).unwrap_or_default() {
                    self.swap_row(op_id, &temp, &real);
                }
                if let Some(operation) = self.ops.get(&op_id).filter(|operation| !operation.stale.is_empty()) {
                    fx.push(Effect::Run(OutboxEffect::Stale { families: operation.stale.clone(), revision }));
                }
                let result = self.ops.get(&op_id).and_then(|operation| operation.result.clone());
                let row_id = self.ops.get(&op_id).map(|operation| operation.landed.clone().unwrap_or_else(|| operation.op.row_id.clone()));
                if let Some(operation) = self.ops.get_mut(&op_id) { operation.shown = shown.last().cloned().flatten(); }
                self.finish(op_id, OutboxOutcome::Confirmed { revision, result, row_id }, true, fx);
            }
            (CommitKind::Rollback, Stage::RollingBack(why)) => {
                if let Some((_, error)) = self.confirmed_discards.get(&op_id).cloned() {
                    self.answer_discarded_outcome(op_id, OutboxAnswer::Outcome(OutboxOutcome::Rejected { error }), fx);
                }
                for reply in Self::take_waiters(&mut self.discard_waiters, op_id) {
                    Self::reply(fx, reply, OutboxAnswer::Ok);
                }
                if why == Why::Discarded && (self.ops[&op_id].pending_owned || self.ops[&op_id].pair_in_flight) {
                    if let Some(operation) = self.ops.get_mut(&op_id) { operation.pending_owned = false; operation.pair_in_flight = false; }
                    if let Some(session) = self.peers.session { fx.push(Effect::Send(Outbound::new(session, SessionInput::Release { req_id: self.ops[&op_id].request_id }))); }
                }
                let outcome = if why == Why::Discarded {
                    OutboxOutcome::Discarded
                } else {
                    OutboxOutcome::Rejected { error: self.ops[&op_id].reject_error.clone().unwrap_or_else(|| "refused".into()) }
                };
                if self.ops[&op_id].cancels == Some(op_id) {
                    let following: Vec<OpId> = self.ops.values().filter(|other| other.op_id != op_id && other.cancels == Some(op_id) && other.stage == Stage::RollingBack(Why::Discarded)).map(|other| other.op_id).collect();
                    for id in following {
                        if self.ops[&id].pending_owned || self.ops[&id].pair_in_flight {
                            if let Some(operation) = self.ops.get_mut(&id) { operation.pending_owned = false; operation.pair_in_flight = false; }
                            if let Some(session) = self.peers.session { fx.push(Effect::Send(Outbound::new(session, SessionInput::Release { req_id: self.ops[&id].request_id }))); }
                        }
                        if let Some(reply) = self.submit_replies.remove(&id) { Self::reply(fx, reply, OutboxAnswer::Receipt(self.receipt(id, revision))); }
                        for reply in Self::take_waiters(&mut self.retry_waiters, id) { Self::reply(fx, reply, OutboxAnswer::Receipt(self.receipt(id, revision))); }
                        for reply in Self::take_waiters(&mut self.discard_waiters, id) { Self::reply(fx, reply, OutboxAnswer::Ok); }
                        self.finish(id, OutboxOutcome::Discarded, true, fx);
                    }
                }
                self.finish(op_id, outcome, true, fx);
                self.finish_destroy_of(op_id, revision, fx);
            }
            (CommitKind::ErrorMark, Stage::MarkingFailed) => {
                let error = self.ops[&op_id].error.clone().unwrap_or_else(|| "refused".into());
                self.failed(op_id, error, fx);
            }
            (CommitKind::ClearMark, Stage::MarkingRetry) => {
                let mut transfer_ids = Vec::new();
                if let Some(operation) = self.ops.get_mut(&op_id) {
                    operation.error = None;
                    if !operation.pending_owned { operation.request_id = crate::owners::contract::next_request_id(); }
                    transfer_ids = std::mem::take(&mut operation.retry_uploads).into_iter().map(|row| (row.transfer_id, row.cycle_id)).collect();
                }
                if !transfer_ids.is_empty() { self.retry_transfer_answer(transfer_ids, Ok(revision), fx); }
                let receipt = OutboxAnswer::Receipt(self.receipt(op_id, revision));
                for reply in Self::take_waiters(&mut self.retry_waiters, op_id) {
                    Self::reply(fx, reply, receipt.clone());
                }
                if self.goes_out(op_id) {
                    self.start_sending(op_id, fx);
                } else {
                    self.requeue(op_id, fx);
                }
            }
            _ => {}
        }
        self.drain(fx);
    }

    /// The `destroy` that cancelled an unsent `create` ends with the rollback of that `create`.
    pub(super) fn finish_destroy_of(&mut self, created: OpId, revision: i64, fx: &mut Effects<Outbox>) {
        let destroys: Vec<OpId> = self.ops.values().filter(|operation| operation.cancels == Some(created) && operation.stage == Stage::Submitting).map(|operation| operation.op_id).collect();
        for op_id in destroys {
            if let Some(reply) = self.submit_replies.remove(&op_id) {
                let receipt = self.receipt(op_id, revision);
                Self::reply(fx, reply, OutboxAnswer::Receipt(receipt));
            }
            self.finish(op_id, OutboxOutcome::Confirmed { revision, result: None, row_id: None }, false, fx);
        }
    }

    /// The row `temp` became `real`: the open operations of the row address `real`.
    pub(super) fn swap_row(&mut self, landed: OpId, temp: &str, real: &str) {
        let model = self.ops[&landed].op.model.clone();
        self.bump_selection_row(&model, temp);
        self.bump_selection_row(&model, real);
        self.aliases.insert((model.clone(), temp.to_string()), real.to_string());
        for operation in self.ops.values_mut().filter(|operation| operation.stage != Stage::Done && Self::addresses_row(operation, &model, temp)) {
            Self::resolve_operation(&self.aliases, &mut operation.op);
        }
    }

    pub(super) fn resolve_operation(aliases: &std::collections::BTreeMap<(String, String), String>, op: &mut NewOp) {
        let resolve = |model: &str, row: &mut String| {
            if let Some(real) = aliases.get(&(model.to_string(), row.clone())) { *row = real.clone(); }
        };
        let before = op.row_id.clone();
        resolve(&op.model, &mut op.row_id);
        if before != op.row_id {
            let old = serde_json::Value::String(before).to_string();
            let new = serde_json::Value::String(op.row_id.clone()).to_string();
            op.body = op.body.replace(&old, &new);
        }
        for row in &mut op.effects.rows { resolve(&row.model, &mut row.row_id); }
        for member in &mut op.effects.members { resolve(&member.model, &mut member.row_id); }
        for row in &mut op.effects.tombstones { resolve(&row.model, &mut row.row_id); }
        for row in &mut op.effects.touches { resolve(&row.model, &mut row.row_id); }
        if let Some((model, row)) = &mut op.effects.shown { resolve(model, row); }
        if let OpSource::TransferGroup { members, .. } = &mut op.origin {
            for member in members { resolve(&op.model, &mut member.row_id); }
        }
    }

    pub(super) fn rejected(&mut self, rejected: Rejected, fx: &mut Effects<Outbox>) {
        let Some(batch) = self.batches.remove(&rejected.batch) else { return };
        if let RejectReason::StaleGeneration { models, generation } = &rejected.reason {
            for model in models {
                self.generation.insert(model.clone(), *generation);
            }
        }
        match batch {
            Batch::Member { op_id } => self.member_rejected(op_id, rejected.reason, fx),
            Batch::Write { reply, call, expire } => {
                if matches!(rejected.reason, RejectReason::StaleGeneration { .. }) {
                    let generation = self.generation_of(&call.model);
                    let id = self.next_batch;
                    self.next_batch += 1;
                    self.commit_write(id, &call, generation, fx);
                    self.batches.insert(id, Batch::Write { reply, call, expire });
                } else {
                    Self::reply(fx, reply, OutboxAnswer::Err(Fault::Rejected(rejected.reason)));
                    self.maybe_drained(fx);
                }
            }
            Batch::Operation { op_id, kind } => self.operation_rejected(op_id, kind, rejected.reason, fx),
        }
    }

    pub(super) fn operation_rejected(&mut self, op_id: OpId, kind: CommitKind, reason: RejectReason, fx: &mut Effects<Outbox>) {
        let Some(stage) = self.stage_of(op_id) else { return };
        if kind == CommitKind::Cause { return; }
        if let Some(created) = self.ops[&op_id].cancels.filter(|created| *created != op_id && kind == CommitKind::Intent && stage == Stage::RollingBack(Why::Discarded)) {
            self.submit_refused(op_id, reason, fx);
            self.ops.remove(&op_id);
            self.finish_cancelled_commit(created, fx);
            return;
        }
        if self.removed.contains(&op_id) {
            if kind == CommitKind::RemoveRow { self.removing.remove(&op_id); }
            match reason {
                RejectReason::NoSession | RejectReason::Closed => {
                    self.removed.remove(&op_id);
                    self.answer_waiters(op_id, &OutboxAnswer::Err(Fault::Rejected(reason.clone())), fx);
                    self.finish(op_id, OutboxOutcome::Ended(end_cause(&reason)), false, fx);
                }
                RejectReason::Storage(_) => self.retry_timer(op_id, fx),
                _ => self.remove_row_operation(op_id, fx),
            }
            return;
        }
        let ours = matches!(
            (kind, stage),
            (CommitKind::Intent, Stage::Submitting)
                | (CommitKind::Attempt, Stage::PersistingAttempt)
                | (CommitKind::Land, Stage::Landing)
                | (CommitKind::Rollback, Stage::RollingBack(_))
                | (CommitKind::ErrorMark, Stage::MarkingFailed)
                | (CommitKind::ClearMark, Stage::MarkingRetry)
        );
        if !ours {
            return;
        }
        match reason {
            RejectReason::StaleGeneration { .. } => self.store(op_id, kind, fx),
            RejectReason::Storage(_) if stage == Stage::Submitting => self.submit_refused(op_id, reason, fx),
            RejectReason::Storage(_) if stage == Stage::MarkingRetry => {
                let transfer_ids = self.take_retry_uploads(op_id);
                self.revert_retry(op_id, !transfer_ids.is_empty());
                if !transfer_ids.is_empty() { self.retry_transfer_answer(transfer_ids, Err(Fault::Rejected(reason.clone())), fx); }
                for reply in Self::take_waiters(&mut self.retry_waiters, op_id) {
                    Self::reply(fx, reply, OutboxAnswer::Err(Fault::Rejected(reason.clone())));
                }
            }
            RejectReason::Storage(detail) if kind == CommitKind::Rollback && self.confirmed_discards.contains_key(&op_id) => {
                let error = self.confirmed_discards[&op_id].1.clone();
                if let Some(operation) = self.ops.get_mut(&op_id) { operation.stage = Stage::Failed; }
                let fault = Fault::Rejected(RejectReason::Storage(format!("{detail}: {error}")));
                self.answer_discarded_outcome(op_id, OutboxAnswer::Err(fault.clone()), fx);
                for reply in Self::take_waiters(&mut self.discard_waiters, op_id) { Self::reply(fx, reply, OutboxAnswer::Err(fault.clone())); }
            }
            RejectReason::Storage(_) => self.retry_timer(op_id, fx),
            RejectReason::NoSession | RejectReason::Closed if stage == Stage::Submitting => self.submit_refused(op_id, reason, fx),
            RejectReason::NoSession | RejectReason::Closed => {
                if self.ops[&op_id].cancels == Some(op_id) {
                    let following: Vec<OpId> = self.ops.values().filter(|other| other.op_id != op_id && other.cancels == Some(op_id) && other.stage != Stage::Done).map(|other| other.op_id).collect();
                    for id in following {
                        self.answer_waiters(id, &OutboxAnswer::Err(Fault::Rejected(reason.clone())), fx);
                        self.finish(id, OutboxOutcome::Ended(end_cause(&reason)), false, fx);
                    }
                }
                let transfer_ids = self.take_retry_uploads(op_id);
                if !transfer_ids.is_empty() { self.retry_transfer_answer(transfer_ids, Err(Fault::Rejected(reason.clone())), fx); }
                self.answer_waiters(op_id, &OutboxAnswer::Err(Fault::Rejected(reason.clone())), fx);
                self.finish(op_id, OutboxOutcome::Ended(end_cause(&reason)), false, fx);
            }
            RejectReason::Invalid => match stage {
                Stage::Submitting => self.submit_refused(op_id, reason, fx),
                Stage::MarkingRetry => {
                    let transfer_ids = self.take_retry_uploads(op_id);
                    self.revert_retry(op_id, !transfer_ids.is_empty());
                    if !transfer_ids.is_empty() { self.retry_transfer_answer(transfer_ids, Err(Fault::Internal), fx); }
                    for reply in Self::take_waiters(&mut self.retry_waiters, op_id) {
                        Self::reply(fx, reply, OutboxAnswer::Err(Fault::Internal));
                    }
                }
                Stage::MarkingFailed => {
                    let error = self.ops[&op_id].error.clone().unwrap_or_else(|| "refused".into());
                    self.failed(op_id, error, fx);
                }
                _ => {
                    if self.ops[&op_id].cancels == Some(op_id) {
                        let following: Vec<OpId> = self.ops.values().filter(|other| other.op_id != op_id && other.cancels == Some(op_id) && other.stage != Stage::Done).map(|other| other.op_id).collect();
                        for id in following {
                            if self.ops[&id].stage == Stage::Submitting {
                                self.answer_waiters(id, &OutboxAnswer::Err(Fault::Invalid), fx);
                                if let Some(operation) = self.ops.get_mut(&id) { operation.stage = Stage::Done; }
                            } else if let Some(operation) = self.ops.get_mut(&id) {
                                if let Some(previous) = operation.before_cancel.take() { operation.stage = previous; }
                                operation.cancels = None;
                            }
                        }
                        if let Some(operation) = self.ops.get_mut(&op_id) { operation.cancels = None; operation.before_cancel = None; }
                    }
                    for reply in Self::take_waiters(&mut self.discard_waiters, op_id) {
                        Self::reply(fx, reply, OutboxAnswer::Err(Fault::Internal));
                    }
                    self.fail_with(op_id, "Internal(Invalid)".into(), fx);
                }
            },
        }
    }

    fn pending_cancelled_intents(&self, created: OpId) -> bool {
        self.batches.values().any(|batch| matches!(batch, Batch::Operation { op_id, kind: CommitKind::Intent } if self.ops.get(op_id).is_some_and(|operation| operation.cancels == Some(created) && *op_id != created)))
    }

    fn finish_cancelled_commit(&mut self, created: OpId, fx: &mut Effects<Outbox>) {
        if !self.pending_cancelled_intents(created) {
            if let Some(revision) = self.cancelled_commits.remove(&created) { self.operation_committed(created, CommitKind::Rollback, (revision, Vec::new()), fx); }
        }
    }

    pub(super) fn submit_refused(&mut self, op_id: OpId, reason: RejectReason, fx: &mut Effects<Outbox>) {
        self.answer_waiters(op_id, &OutboxAnswer::Err(Fault::Rejected(reason.clone())), fx);
        if let Some(operation) = self.ops.get_mut(&op_id) {
            operation.stage = Stage::Done;
            operation.outcome = Some(OutboxOutcome::Ended(end_cause(&reason)));
        }
    }

    fn revert_retry(&mut self, op_id: OpId, transfer_retry: bool) {
        let Some(operation) = self.ops.get_mut(&op_id) else { return };
        operation.stage = Stage::Failed;
        operation.cycle_id = operation.cycle_id.saturating_sub(1);
        if !transfer_retry { return; }
        match &mut operation.op.origin {
            OpSource::Api => {}
            OpSource::Transfer { cycle_id, .. } => *cycle_id = cycle_id.saturating_sub(1),
            OpSource::TransferGroup { members, .. } => for member in members { member.cycle_id = member.cycle_id.saturating_sub(1); },
        }
    }

    fn take_retry_uploads(&mut self, op_id: OpId) -> Vec<(u64, u64)> {
        self.ops.get_mut(&op_id).map(|operation| std::mem::take(&mut operation.retry_uploads).into_iter().map(|row| (row.transfer_id, row.cycle_id)).collect()).unwrap_or_default()
    }

}
