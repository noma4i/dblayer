//! Owner `Outbox`: the rows of the table for the network, the retry timers, `retry`, `discard`, `outcome`, `Submit` and `Online`.


use super::outbox::{Outbox, OutboxState};
use super::outbox_types::*;
use crate::owners::contract::RequestId;

use crate::owners::contract::{ReplyId};

use crate::owners::store::{RowValue};
use crate::runtime::{Effect, Effects, Epoch};



impl OutboxState {
    pub(super) fn on_response(&mut self, op_id: OpId, rows: Vec<(String, Vec<RowValue>)>, swap: Option<(String, String)>, errors: Option<bool>, fx: &mut Effects<Outbox>) {
        if self.stage_of(op_id) != Some(Stage::Sending) {
            return;
        }
        if let Some(temporary) = errors {
            return self.transport_failed(op_id, temporary, fx);
        }
        if let Some(operation) = self.ops.get_mut(&op_id) {
            operation.stage = Stage::Landing;
            operation.landing = rows;
            operation.swap = swap;
        }
        self.store(op_id, CommitKind::Land, fx);
    }

    pub(super) fn transport_failed(&mut self, op_id: OpId, temporary: bool, fx: &mut Effects<Outbox>) {
        if self.stage_of(op_id) != Some(Stage::Sending) {
            return;
        }
        if !temporary {
            return self.reject_or_keep(op_id, fx);
        }
        let attempts = self.ops[&op_id].attempts;
        if attempts >= ATTEMPT_LIMIT {
            self.fail_with(op_id, "Exhausted".into(), fx);
        } else if self.online {
            if let Some(operation) = self.ops.get_mut(&op_id) {
                operation.waiting_retry = true;
            }
            self.retry_timer(op_id, fx);
        } else if let Some(operation) = self.ops.get_mut(&op_id) {
            operation.stage = Stage::Queued;
        }
    }

    pub(super) fn retry_due(&mut self, op_id: OpId, cycle_id: u64, fx: &mut Effects<Outbox>) {
        if self.ops.get(&op_id).is_none_or(|operation| operation.cycle_id != cycle_id) {
            return;
        }
        match self.stage_of(op_id) {
            Some(Stage::Sending) if self.online => {
                if self.ops[&op_id].attempts < ATTEMPT_LIMIT {
                    self.begin_attempt(op_id, fx);
                } else {
                    self.fail_with(op_id, "Exhausted".into(), fx);
                }
            }
            Some(Stage::Sending) => {
                self.ops.get_mut(&op_id).map(|operation| operation.stage = Stage::Queued);
            }
            Some(Stage::PersistingAttempt) => self.store(op_id, CommitKind::Attempt, fx),
            Some(Stage::Landing) => self.store(op_id, CommitKind::Land, fx),
            Some(Stage::RollingBack(_)) => self.store(op_id, CommitKind::Rollback, fx),
            Some(Stage::MarkingFailed) => self.store(op_id, CommitKind::ErrorMark, fx),
            _ => {}
        }
    }

    pub(super) fn retry(&mut self, model: &str, row: &str, reply: ReplyId, fx: &mut Effects<Outbox>) {
        let Some(op_id) = self.open_on_row(model, row) else {
            return Self::reply(fx, reply, OutboxAnswer::Err(Fault::NotFound));
        };
        let receipt = self.receipt(op_id, 0);
        match self.ops[&op_id].stage {
            Stage::Submitting => self.retry_waiters.push((op_id, reply)),
            Stage::Queued | Stage::PersistingAttempt | Stage::Sending | Stage::Landing => Self::reply(fx, reply, OutboxAnswer::Receipt(receipt)),
            Stage::RollingBack(_) | Stage::MarkingFailed | Stage::MarkingRetry | Stage::WaitingDeclaration => Self::reply(fx, reply, OutboxAnswer::Err(Fault::InFlight)),
            Stage::Failed => match self.ops[&op_id].error.as_deref() {
                Some("RestoreFailed") => Self::reply(fx, reply, OutboxAnswer::Err(Fault::RestoreFailed)),
                Some("StateLost") => Self::reply(fx, reply, OutboxAnswer::Err(Fault::StateLost)),
                _ => {
                    if let Some(operation) = self.ops.get_mut(&op_id) {
                        operation.stage = Stage::MarkingRetry;
                        operation.cycle_id += 1;
                        operation.attempts = 1;
                        operation.sent = false;
                    }
                    self.retry_waiters.push((op_id, reply));
                    self.store(op_id, CommitKind::ClearMark, fx);
                }
            },
            Stage::Done => Self::reply(fx, reply, OutboxAnswer::Err(Fault::NotFound)),
        }
    }

    pub(super) fn discard(&mut self, model: &str, row: &str, reply: ReplyId, fx: &mut Effects<Outbox>) {
        let Some(op_id) = self.open_on_row(model, row) else {
            return Self::reply(fx, reply, OutboxAnswer::Err(Fault::NotFound));
        };
        match self.ops[&op_id].stage {
            Stage::Submitting | Stage::PersistingAttempt | Stage::Sending | Stage::Landing | Stage::MarkingFailed | Stage::MarkingRetry | Stage::WaitingDeclaration => {
                Self::reply(fx, reply, OutboxAnswer::Err(Fault::InFlight));
            }
            Stage::Queued | Stage::Failed => {
                self.discard_waiters.push((op_id, reply));
                self.ops.get_mut(&op_id).map(|operation| operation.stage = Stage::RollingBack(Why::Discarded));
                self.store(op_id, CommitKind::Rollback, fx);
            }
            Stage::RollingBack(_) => self.discard_waiters.push((op_id, reply)),
            Stage::Done => Self::reply(fx, reply, OutboxAnswer::Err(Fault::NotFound)),
        }
    }

    pub(super) fn outcome(&mut self, receipt: Receipt, reply: ReplyId, fx: &mut Effects<Outbox>) {
        if receipt.epoch != self.epoch {
            return Self::reply(fx, reply, OutboxAnswer::Err(Fault::NoSession));
        }
        let Some(operation) = self.ops.get(&receipt.op_id) else {
            return Self::reply(fx, reply, OutboxAnswer::Err(Fault::NotFound));
        };
        if let Some(error) = operation.failed_errors.get(&receipt.cycle_id).filter(|_| receipt.cycle_id != operation.cycle_id) {
            return Self::reply(fx, reply, OutboxAnswer::Outcome(OutboxOutcome::Failed { error: error.clone() }));
        }
        match operation.stage {
            Stage::Done => Self::reply(fx, reply, OutboxAnswer::Outcome(operation.outcome.clone().unwrap_or(OutboxOutcome::Interrupted))),
            Stage::Failed => Self::reply(fx, reply, OutboxAnswer::Outcome(OutboxOutcome::Failed { error: operation.error.clone().unwrap_or_else(|| "refused".into()) })),
            _ => self.outcome_waiters.push((receipt.op_id, reply)),
        }
    }

    /// `(none) x Submit`: an unsent `create` that a `destroy` of the same row cancels ends with it, the network is not called.
    pub(super) fn submit(&mut self, epoch: Epoch, op: NewOp, reply: ReplyId, fx: &mut Effects<Outbox>) {
        if epoch != self.epoch {
            return Self::reply(fx, reply, OutboxAnswer::Err(Fault::NoSession));
        }
        if !op.durable && !self.online {
            return Self::reply(fx, reply, OutboxAnswer::Err(Fault::Offline));
        }
        let row = self.aliases.get(&(op.model.clone(), op.row_id.clone())).cloned().unwrap_or_else(|| op.row_id.clone());
        let unsent_create = (op.action == "destroy")
            .then(|| self.ops.values().find(|other| other.stage == Stage::Queued && !other.sent && other.op.action == "create" && other.op.model == op.model && other.op.row_id == row).map(|other| other.op_id))
            .flatten();
        let op_id = self.next_op;
        self.add_operation(op_id, op, Stage::Submitting);
        self.submit_replies.insert(op_id, reply);
        if let Some(created) = unsent_create {
            self.ops.get_mut(&op_id).map(|operation| operation.cancels = Some(created));
            self.ops.get_mut(&created).map(|operation| operation.stage = Stage::RollingBack(Why::Discarded));
            self.store(created, CommitKind::Rollback, fx);
        } else {
            self.store(op_id, CommitKind::Intent, fx);
        }
    }

    pub(super) fn online_changed(&mut self, on: bool, fx: &mut Effects<Outbox>) {
        self.online = on;
        let queued: Vec<OpId> = self.ops.values().filter(|operation| operation.stage == Stage::Queued).map(|operation| operation.op_id).collect();
        for op_id in queued {
            if on {
                self.try_start(op_id, fx);
            } else if self.ops.get(&op_id).is_some_and(|operation| !operation.op.durable && !operation.sent) {
                if let Some(operation) = self.ops.get_mut(&op_id) {
                    operation.stage = Stage::RollingBack(Why::Rejected);
                    operation.reject_error = Some("network".into());
                }
                self.store(op_id, CommitKind::Rollback, fx);
            }
        }
    }

}

impl OutboxState {
    /// A `Mutate` of this owner got its answer. An answer of an operation that is not in flight (a late one) is dropped.
    pub(super) fn answered(&mut self, req_id: RequestId, outcome: crate::owners::session::AuthorOutcome, fx: &mut Effects<Outbox>) {
        use crate::owners::contract::ErrorClass;
        use crate::owners::session::AuthorOutcome;
        let Some(op_id) = self.ops.values().find(|operation| operation.request_id == req_id && operation.stage == Stage::Sending).map(|operation| operation.op_id) else { return };
        match outcome {
            AuthorOutcome::Response(body) => {
                let operation = &self.ops[&op_id];
                let temp = Some(operation.op.row_id.clone()).filter(|row| crate::identity::is_temp_id(row));
                fx.push(Effect::Run(OutboxEffect::Decode(super::answer::DecodeJob { op_id, model: operation.op.model.clone(), action: operation.op.action.clone(), temp, body })));
            }
            AuthorOutcome::Error { class, error } => {
                if let Some(operation) = self.ops.get_mut(&op_id) { operation.reject_error = Some(super::failure::of_session(&error, class)); }
                self.transport_failed(op_id, matches!(class, ErrorClass::Network | ErrorClass::Server), fx);
            }
            AuthorOutcome::Dropped => self.transport_failed(op_id, true, fx),
        }
    }
}
