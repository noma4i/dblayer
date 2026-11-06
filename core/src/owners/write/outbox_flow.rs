//! Owner `Outbox`: the rows of the table for the network, the retry timers, `retry`, `discard`, `outcome`, `Submit` and `Online`.


use super::outbox::{Outbox, OutboxState};
use super::outbox_types::*;
use crate::owners::contract::RequestId;

use crate::owners::contract::{ReplyId};

use crate::owners::store::{RowValue};
use crate::owners::session::SessionInput;
use crate::runtime::{Effect, Effects, Epoch, Outbound};



/// What the decoded answer of a mutation carries.
pub(super) struct Answered {
    pub rows: Vec<(String, Vec<RowValue>)>,
    pub swaps: Vec<(String, String)>,
    pub stale: Vec<(String, String)>,
    pub result: Option<String>,
    pub missing: Vec<String>,
}

impl OutboxState {
    /// The rows of a saved answer arrived: the operation goes on as one whose live answer arrived.
    pub(super) fn rows_arrived(&mut self, op_id: OpId) {
        if let Some(operation) = self.ops.get_mut(&op_id).filter(|operation| operation.rows_pending && operation.stage == Stage::Landing) {
            operation.rows_pending = false;
            operation.stage = Stage::Sending;
        }
    }

    pub(super) fn on_response(&mut self, op_id: OpId, answer: Answered, errors: Option<bool>, fx: &mut Effects<Outbox>) {
        let Answered { rows, swaps, stale, result, missing } = answer;
        self.rows_arrived(op_id);
        if self.stage_of(op_id) != Some(Stage::Sending) {
            return;
        }
        if let Some(temporary) = errors {
            return self.transport_failed(op_id, temporary, fx);
        }
        if let Some(operation) = self.ops.get_mut(&op_id) {
            operation.stage = Stage::Landing;
            operation.landed = swaps.iter().find(|(temp, _)| *temp == operation.op.row_id).or(swaps.first()).map(|(_, real)| real.clone()).or_else(|| rows.iter().find(|(model, _)| *model == operation.op.model).and_then(|(_, rows)| rows.last()).map(|row| row.row_id.clone()));
            operation.landing = rows;
            operation.swaps = swaps;
            operation.missing = missing;
            operation.stale = stale;
            operation.result = result;
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
        if !self.ops[&op_id].op.durable {
            if let Some(operation) = self.ops.get_mut(&op_id) {
                operation.stage = Stage::RollingBack(Why::Rejected);
                operation.reject_error = Some(super::failure::network());
            }
            return self.store(op_id, CommitKind::Rollback, fx);
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
                if let Some(operation) = self.ops.get_mut(&op_id) { operation.stage = Stage::Queued; }
            }
            Some(Stage::PersistingAttempt) => self.store(op_id, CommitKind::Attempt, fx),
            Some(Stage::Landing) => self.store(op_id, CommitKind::Land, fx),
            Some(Stage::RollingBack(_)) => self.store(op_id, CommitKind::Rollback, fx),
            Some(Stage::MarkingFailed) => self.store(op_id, CommitKind::ErrorMark, fx),
            _ => {}
        }
    }

    pub(super) fn retry_transfer(&mut self, model: &str, row: &str, reply: ReplyId, uploads: Vec<crate::owners::store::UploadRow>, fx: &mut Effects<Outbox>) {
        let ids: Vec<_> = uploads.iter().map(|upload| (upload.transfer_id, upload.cycle_id)).collect();
        let Some(op_id) = self.open_on_row(model, row) else {
            self.retry_transfer_answer(ids, Err(Fault::NotFound), fx);
            return;
        };
        if self.ops[&op_id].stage != Stage::Failed {
            self.retry_transfer_answer(ids, Err(Fault::InFlight), fx);
            return;
        }
        if matches!(self.ops[&op_id].error.as_deref(), Some("RestoreFailed" | "StateLost")) {
            let error = if self.ops[&op_id].error.as_deref() == Some("StateLost") { Fault::StateLost } else { Fault::RestoreFailed };
            self.retry_transfer_answer(ids, Err(error), fx);
            return;
        }
        let supplied: std::collections::BTreeMap<_, _> = uploads.iter().map(|upload| (upload.transfer_id, upload.cycle_id)).collect();
        let valid = supplied.len() == uploads.len() && match &self.ops[&op_id].op.origin {
            OpSource::Api => false,
            OpSource::Transfer { transfer_id, cycle_id } => supplied.len() == 1 && supplied.get(transfer_id).copied() == cycle_id.checked_add(1),
            OpSource::TransferGroup { members, .. } => supplied.len() == members.len() && members.iter().all(|member| supplied.get(&member.transfer_id).copied() == member.cycle_id.checked_add(1)),
        };
        if !valid { self.retry_transfer_answer(ids, Err(Fault::Invalid), fx); return; }
        let Some(operation) = self.ops.get(&op_id) else {
            self.retry_transfer_answer(ids, Err(Fault::NotFound), fx);
            return;
        };
        let mut origin = operation.op.origin.clone();
        match &mut origin {
            OpSource::Api => { self.retry_transfer_answer(ids, Err(Fault::Invalid), fx); return; }
            OpSource::Transfer { transfer_id, cycle_id } => {
                let Some(next) = supplied.get(&*transfer_id) else { self.retry_transfer_answer(ids, Err(Fault::Invalid), fx); return };
                *cycle_id = *next;
            }
            OpSource::TransferGroup { members, .. } => for member in members {
                let Some(next) = supplied.get(&member.transfer_id) else { self.retry_transfer_answer(ids, Err(Fault::Invalid), fx); return };
                member.cycle_id = *next;
            },
        }
        let Some(operation) = self.ops.get_mut(&op_id) else {
            self.retry_transfer_answer(ids, Err(Fault::NotFound), fx);
            return;
        };
        operation.op.origin = origin;
        operation.retry_uploads = uploads;
        self.retry(model, row, reply, fx);
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
                if self.ops[&op_id].sent && self.ops[&op_id].stage == Stage::Queued {
                    if let Some(session) = self.peers.session { fx.push(Effect::Send(Outbound::new(session, SessionInput::Release { req_id: self.ops[&op_id].request_id }))); }
                }
                self.discard_waiters.push((op_id, reply));
                if let Some(operation) = self.ops.get_mut(&op_id) { operation.stage = Stage::RollingBack(Why::Discarded); }
                self.store(op_id, CommitKind::Rollback, fx);
            }
            Stage::RollingBack(_) => self.discard_waiters.push((op_id, reply)),
            Stage::Done => Self::reply(fx, reply, OutboxAnswer::Err(Fault::NotFound)),
        }
    }

    /// The server removed the row: each of its open operations ends `Discarded`, a request on its way is released.
    pub(super) fn rows_gone(&mut self, ops: Vec<OpId>, fx: &mut Effects<Outbox>) {
        for op_id in ops {
            let Some(operation) = self.ops.get(&op_id) else { continue };
            if matches!(operation.stage, Stage::Done | Stage::Landing | Stage::RollingBack(_) | Stage::Submitting) { continue; }
            if operation.sent {
                if let Some(session) = self.peers.session { fx.push(Effect::Send(Outbound::new(session, SessionInput::Cancel { req_id: operation.request_id }))); }
            }
            self.finish(op_id, OutboxOutcome::Discarded, false, fx);
        }
    }

    /// `(none) x DiscardRestoreFailed`: the operation of the element ends like a `Discard`, the element leaves in the same batch.
    pub(super) fn discard_restore_failed(&mut self, table: &str, key: &str, reply: ReplyId, fx: &mut Effects<Outbox>) {
        let found = self.ops.values().find(|operation| operation.stage == Stage::Failed && operation.restore_element.as_ref().is_some_and(|(found_table, found_key)| found_table == table && found_key == key));
        let Some((model, row)) = found.map(|operation| (operation.op.model.clone(), operation.op.row_id.clone())) else {
            return Self::reply(fx, reply, OutboxAnswer::Err(Fault::NotFound));
        };
        self.discard(&model, &row, reply, fx);
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
        if !self.declarations.contains(&(op.model.clone(), op.action.clone())) {
            return Self::reply(fx, reply, OutboxAnswer::Err(Fault::Invalid));
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
            if let Some(operation) = self.ops.get_mut(&op_id) { operation.cancels = Some(created); }
            if let Some(operation) = self.ops.get_mut(&created) { operation.stage = Stage::RollingBack(Why::Discarded); }
            self.store(created, CommitKind::Rollback, fx);
        } else {
            self.store(op_id, CommitKind::Intent, fx);
        }
    }

    /// `ReserveOp`: the next `op_id` of the sequence, nothing is written. A reserved `op_id` that is not used is not given again.
    pub(super) fn reserve_op(&mut self, epoch: Epoch, target: ReserveTarget, fx: &mut Effects<Outbox>) {
        let result = if epoch == self.epoch {
            self.next_op += 1;
            Ok(self.next_op - 1)
        } else {
            Err(Fault::NoSession)
        };
        self.opened(fx, target, result);
    }

    pub(super) fn opened(&self, fx: &mut Effects<Outbox>, target: ReserveTarget, result: Result<OpId, Fault>) {
        if let Some(transfer) = self.peers.transfer {
            fx.push(Effect::Send(Outbound::new(transfer, super::transfer::TransferInput::OpReserved { target, result })));
        }
    }

    /// `Submit` with the intent written by the batch of Transfer: the operation goes out as after `Submitting x Committed`, with no write and no receipt.
    pub(super) fn submit_reserved(&mut self, epoch: Epoch, op_id: OpId, op: NewOp, fx: &mut Effects<Outbox>) {
        if epoch != self.epoch {
            return;
        }
        self.add_operation(op_id, op, Stage::Submitting);
        self.operation_committed(op_id, CommitKind::Intent, 0, fx);
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
                    operation.reject_error = Some(super::failure::network());
                }
                self.store(op_id, CommitKind::Rollback, fx);
            }
        }
    }

}

impl OutboxState {
    /// The answer of the server is read into rows outside the actor: the codec of the operation is the same for a live and a saved answer.
    pub(super) fn decode_answer(&self, op_id: OpId, body: String, fx: &mut Effects<Outbox>) {
        let operation = &self.ops[&op_id];
        let temp = Some(operation.op.row_id.clone()).filter(|row| crate::identity::is_temp_id(row)).or_else(|| operation.op.effects.rows.iter().find(|row| row.model == operation.op.model && crate::identity::is_temp_id(&row.row_id)).map(|row| row.row_id.clone()));
        let batch = match &operation.op.origin { OpSource::TransferGroup { batch_id, .. } => Some(*batch_id), _ => None };
        fx.push(Effect::Run(OutboxEffect::Decode(super::answer::DecodeJob { op_id, model: operation.op.model.clone(), action: operation.op.action.clone(), temp, batch, body })));
    }

    /// A `Mutate` of this owner got its answer. An answer of an operation that is not in flight (a late one) is dropped.
    pub(super) fn answered(&mut self, req_id: RequestId, outcome: crate::owners::session::AuthorOutcome, fx: &mut Effects<Outbox>) {
        use crate::owners::contract::ErrorClass;
        use crate::owners::session::AuthorOutcome;
        let Some(op_id) = self.ops.values().find(|operation| operation.request_id == req_id && operation.stage == Stage::Sending).map(|operation| operation.op_id) else { return };
        match outcome {
            AuthorOutcome::Response(body) => self.decode_answer(op_id, body, fx),
            AuthorOutcome::Error { class, error } => {
                if let Some(operation) = self.ops.get_mut(&op_id) { operation.reject_error = Some(super::failure::of_session(&error, class)); }
                self.transport_failed(op_id, matches!(class, ErrorClass::Network | ErrorClass::Server), fx);
            }
            AuthorOutcome::Dropped => self.transport_failed(op_id, true, fx),
        }
    }
}

impl OutboxState {
    /// No operation is being written, sent, landed or rolled back, and no local write is on its way.
    fn is_quiet(&self) -> bool {
        self.writes_active() == 0 && self.ops.values().all(|operation| match operation.stage {
            Stage::Submitting | Stage::PersistingAttempt | Stage::Landing | Stage::RollingBack(_) | Stage::MarkingFailed | Stage::MarkingRetry => false,
            Stage::Sending => operation.waiting_retry,
            Stage::WaitingDeclaration | Stage::Queued | Stage::Failed | Stage::Done => true,
        })
    }

    pub(super) fn release_quiet(&mut self, fx: &mut Effects<Outbox>) {
        if !self.quiet_waiters.is_empty() && self.is_quiet() {
            for reply in std::mem::take(&mut self.quiet_waiters) { Self::reply(fx, reply, OutboxAnswer::Ok); }
        }
    }
}
