//! Owner `Outbox`: the answers of the Store to the batches of an operation (`Committed`, `Rejected`).


use super::outbox::{Batch, Outbox, OutboxState};
use super::outbox_types::*;

use crate::owners::contract::{Committed, RejectReason, Rejected};


use crate::runtime::{Effect, Effects, Outbound, ScheduleId};
use super::transfer::TransferInput;



impl OutboxState {
    pub(super) fn committed(&mut self, committed: Committed, fx: &mut Effects<Outbox>) {
        let Some(batch) = self.batches.remove(&committed.batch) else { return };
        match batch {
            Batch::Write { reply, row, model, expire, .. } => {
                Self::reply(fx, reply, OutboxAnswer::Ok);
                if let Some((due, after)) = expire {
                    self.ephemeral.insert(row.clone(), due);
                    let id = ScheduleId(EXPIRE_TIMER + self.next_schedule);
                    self.next_schedule += 1;
                    fx.push(Effect::Schedule { id, after: std::time::Duration::from_millis(after), input: OutboxInput::Expire { model, row, due } });
                }
                self.maybe_drained(fx);
            }
            Batch::Operation { op_id, kind } => self.operation_committed(op_id, kind, committed.revision, fx),
        }
    }

    pub(super) fn operation_committed(&mut self, op_id: OpId, kind: CommitKind, revision: i64, fx: &mut Effects<Outbox>) {
        let Some(stage) = self.stage_of(op_id) else { return };
        match (kind, stage) {
            (CommitKind::Intent, Stage::Submitting) => {
                let receipt = OutboxAnswer::Receipt(self.receipt(op_id, revision));
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
                if self.goes_out(op_id) {
                    self.start_sending(op_id, fx);
                } else if let Some(operation) = self.ops.get_mut(&op_id) {
                    operation.stage = Stage::Queued;
                }
            }
            (CommitKind::Land, Stage::Landing) => {
                for (temp, real) in self.ops.get(&op_id).map(|operation| operation.swaps.clone()).unwrap_or_default() {
                    self.swap_row(op_id, &temp, &real);
                }
                if let Some(operation) = self.ops.get(&op_id).filter(|operation| !operation.stale.is_empty()) {
                    fx.push(Effect::Run(OutboxEffect::Stale { families: operation.stale.clone(), revision }));
                }
                self.tell_missing_elements(op_id, fx);
                let result = self.ops.get(&op_id).and_then(|operation| operation.result.clone());
                self.finish(op_id, OutboxOutcome::Confirmed { revision, result }, true, fx);
            }
            (CommitKind::Rollback, Stage::RollingBack(why)) => {
                for reply in Self::take_waiters(&mut self.discard_waiters, op_id) {
                    Self::reply(fx, reply, OutboxAnswer::Ok);
                }
                let outcome = if why == Why::Discarded {
                    OutboxOutcome::Discarded
                } else {
                    OutboxOutcome::Rejected { error: self.ops[&op_id].reject_error.clone().unwrap_or_else(|| "refused".into()) }
                };
                self.finish(op_id, outcome, true, fx);
                self.finish_destroy_of(op_id, revision, fx);
            }
            (CommitKind::ErrorMark, Stage::MarkingFailed) => {
                let error = self.ops[&op_id].error.clone().unwrap_or_else(|| "refused".into());
                self.failed(op_id, error, fx);
            }
            (CommitKind::ClearMark, Stage::MarkingRetry) => {
                if let Some(operation) = self.ops.get_mut(&op_id) {
                    operation.error = None;
                }
                let receipt = OutboxAnswer::Receipt(self.receipt(op_id, revision));
                for reply in Self::take_waiters(&mut self.retry_waiters, op_id) {
                    Self::reply(fx, reply, receipt.clone());
                }
                if self.goes_out(op_id) {
                    self.start_sending(op_id, fx);
                } else if let Some(operation) = self.ops.get_mut(&op_id) {
                    operation.stage = Stage::Queued;
                }
            }
            _ => {}
        }
    }

    /// After the batch landed: each element the answer has no row for gets `Failed{Protocol}` before the outcome of the batch reaches the others.
    pub(super) fn tell_missing_elements(&self, op_id: OpId, fx: &mut Effects<Outbox>) {
        let Some(operation) = self.ops.get(&op_id) else { return };
        let (OpSource::Transfer(_), Some(transfer)) = (&operation.op.origin, self.peers.transfer) else { return };
        let error = super::failure::protocol("the answer has no row for the element");
        for temp in &operation.missing {
            let key = format!("{}:{}:{}", operation.op.model, temp, operation.op.action);
            fx.push(Effect::Send(Outbound::new(transfer, TransferInput::MutationOutcome { key, outcome: OutboxOutcome::Failed { error: error.clone() } })));
        }
    }

    /// The `destroy` that cancelled an unsent `create` ends with the rollback of that `create`.
    pub(super) fn finish_destroy_of(&mut self, created: OpId, revision: i64, fx: &mut Effects<Outbox>) {
        let destroys: Vec<OpId> = self.ops.values().filter(|operation| operation.cancels == Some(created) && operation.stage == Stage::Submitting).map(|operation| operation.op_id).collect();
        for op_id in destroys {
            if let Some(reply) = self.submit_replies.remove(&op_id) {
                let receipt = self.receipt(op_id, revision);
                Self::reply(fx, reply, OutboxAnswer::Receipt(receipt));
            }
            self.finish(op_id, OutboxOutcome::Confirmed { revision, result: None }, false, fx);
        }
    }

    /// The row `temp` became `real`: the open operations of the row address `real`.
    pub(super) fn swap_row(&mut self, landed: OpId, temp: &str, real: &str) {
        let model = self.ops[&landed].op.model.clone();
        self.aliases.insert((model.clone(), temp.to_string()), real.to_string());
        for operation in self.ops.values_mut().filter(|operation| operation.stage != Stage::Done && operation.op.model == model && operation.op.row_id == temp) {
            operation.op.row_id = real.to_string();
            operation.op.body = operation.op.body.replace(temp, real);
            for row in operation.op.effects.rows.iter_mut().filter(|row| row.model == model && row.row_id == temp) {
                row.row_id = real.to_string();
            }
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
            Batch::Write { reply, row, values, model, kind, expire } => {
                if matches!(rejected.reason, RejectReason::StaleGeneration { .. }) {
                    let generation = self.generation_of(&model);
                    let id = self.next_batch;
                    self.next_batch += 1;
                    self.commit_write(id, &model, &kind, &row, &values, generation, fx);
                    self.batches.insert(id, Batch::Write { reply, row, values, model, kind, expire });
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
                if let Some(operation) = self.ops.get_mut(&op_id) { operation.stage = Stage::Failed; }
                for reply in Self::take_waiters(&mut self.retry_waiters, op_id) {
                    Self::reply(fx, reply, OutboxAnswer::Err(Fault::Rejected(reason.clone())));
                }
            }
            RejectReason::Storage(_) => self.retry_timer(op_id, fx),
            RejectReason::NoSession | RejectReason::Closed if stage == Stage::Submitting => self.submit_refused(op_id, reason, fx),
            RejectReason::NoSession | RejectReason::Closed => {
                self.answer_waiters(op_id, &OutboxAnswer::Err(Fault::Interrupted), fx);
                self.finish(op_id, OutboxOutcome::Interrupted, false, fx);
            }
            RejectReason::Invalid => match stage {
                Stage::Submitting => self.submit_refused(op_id, reason, fx),
                Stage::MarkingRetry => {
                    if let Some(operation) = self.ops.get_mut(&op_id) { operation.stage = Stage::Failed; }
                    for reply in Self::take_waiters(&mut self.retry_waiters, op_id) {
                        Self::reply(fx, reply, OutboxAnswer::Err(Fault::Internal));
                    }
                }
                Stage::MarkingFailed => {
                    let error = self.ops[&op_id].error.clone().unwrap_or_else(|| "refused".into());
                    self.failed(op_id, error, fx);
                }
                _ => {
                    for reply in Self::take_waiters(&mut self.discard_waiters, op_id) {
                        Self::reply(fx, reply, OutboxAnswer::Err(Fault::Internal));
                    }
                    self.fail_with(op_id, "Internal(Invalid)".into(), fx);
                }
            },
        }
    }

    pub(super) fn submit_refused(&mut self, op_id: OpId, reason: RejectReason, fx: &mut Effects<Outbox>) {
        self.answer_waiters(op_id, &OutboxAnswer::Err(Fault::Rejected(reason)), fx);
        if let Some(operation) = self.ops.get_mut(&op_id) {
            operation.stage = Stage::Done;
            operation.outcome = Some(OutboxOutcome::Interrupted);
        }
    }

}
