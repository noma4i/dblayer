//! Owner `Outbox`: the rows of the table for the network, the retry timers, `retry`, `discard`, `outcome`, `Submit` and `Online`.


use crate::owners::contract::RejectReason;
use super::outbox::{Batch, MemberRemoval, Outbox, OutboxState};
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
        if self.removed.contains(&op_id) { return; }
        let Answered { rows, swaps, stale, result, missing } = answer;
        self.rows_arrived(op_id);
        if self.stage_of(op_id) != Some(Stage::Sending) {
            return;
        }
        if let Some(operation) = self.ops.get_mut(&op_id) { operation.pair_in_flight = false; }
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
        self.drain(fx);
    }

    pub(super) fn transport_failed(&mut self, op_id: OpId, temporary: bool, fx: &mut Effects<Outbox>) {
        if self.removed.contains(&op_id) { return; }
        if self.stage_of(op_id) != Some(Stage::Sending) {
            return;
        }
        if let Some(operation) = self.ops.get_mut(&op_id) { operation.pair_in_flight = false; }
        if !temporary {
            self.reject_or_keep(op_id, fx);
            return self.drain(fx);
        }
        let attempts = self.ops[&op_id].attempts;
        if attempts >= ATTEMPT_LIMIT {
            self.fail_with(op_id, "Exhausted".into(), fx);
        } else if self.online {
            if let Some(operation) = self.ops.get_mut(&op_id) {
                operation.waiting_retry = true;
            }
            self.retry_timer(op_id, fx);
        } else {
            self.requeue(op_id, fx);
        }
        self.drain(fx);
    }

    pub(super) fn retry_due(&mut self, op_id: OpId, cycle_id: u64, fx: &mut Effects<Outbox>) {
        if self.ops.get(&op_id).is_none_or(|operation| operation.cycle_id != cycle_id) {
            return;
        }
        if self.ops[&op_id].cancels.is_some_and(|created| created != op_id) && self.stage_of(op_id) == Some(Stage::RollingBack(Why::Discarded)) { return; }
        if self.removed.contains(&op_id) {
            return self.remove_row_operation(op_id, fx);
        }
        if self.stage_of(op_id) == Some(Stage::Sending) && !self.ops[&op_id].waiting_retry {
            return;
        }
        match self.stage_of(op_id) {
            Some(Stage::Sending) if self.online && self.slots_taken() - usize::from(!self.ops[&op_id].waiting_retry) >= DRAIN_LIMIT => {
                self.requeue(op_id, fx);
            }
            Some(Stage::Sending) if self.online => {
                if self.ops[&op_id].attempts < ATTEMPT_LIMIT {
                    self.begin_attempt(op_id, fx);
                } else {
                    self.fail_with(op_id, "Exhausted".into(), fx);
                }
            }
            Some(Stage::Sending) => self.requeue(op_id, fx),
            Some(Stage::PersistingAttempt) => self.store(op_id, CommitKind::Attempt, fx),
            Some(Stage::Landing) => self.store(op_id, CommitKind::Land, fx),
            Some(Stage::RollingBack(_)) => self.store(op_id, CommitKind::Rollback, fx),
            Some(Stage::MarkingFailed) => self.store(op_id, CommitKind::ErrorMark, fx),
            _ => {}
        }
    }

    pub(super) fn retry_transfer(&mut self, model: &str, row: &str, reply: ReplyId, uploads: Vec<crate::owners::store::UploadRow>, fx: &mut Effects<Outbox>) {
        let ids: Vec<_> = uploads.iter().map(|upload| (upload.transfer_id, upload.cycle_id)).collect();
        let Some(op_id) = self.open_on_row(model, row, None) else {
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
        self.retry(model, row, None, reply, fx);
    }

    pub(super) fn retry(&mut self, model: &str, row: &str, action: Option<&str>, reply: ReplyId, fx: &mut Effects<Outbox>) {
        let Some(op_id) = self.open_on_row(model, row, action) else {
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

    pub(super) fn discard(&mut self, model: &str, row: &str, action: Option<&str>, reply: ReplyId, fx: &mut Effects<Outbox>) {
        let Some(op_id) = self.open_on_row(model, row, action) else {
            return Self::reply(fx, reply, OutboxAnswer::Err(Fault::NotFound));
        };
        match self.ops[&op_id].stage {
            Stage::Submitting | Stage::PersistingAttempt | Stage::Sending | Stage::Landing | Stage::MarkingFailed | Stage::MarkingRetry | Stage::WaitingDeclaration => {
                Self::reply(fx, reply, OutboxAnswer::Err(Fault::InFlight));
            }
            Stage::Queued | Stage::Failed => {
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

    pub(super) fn row_removed(&mut self, model: &str, row: &str, revision: u64, fx: &mut Effects<Outbox>) {
        let ids: Vec<_> = self.ops.values().filter(|operation| operation.stage != Stage::Done && Self::addresses_row(operation, model, row)).map(|operation| operation.op_id).collect();
        if !ids.is_empty() { self.bump_selection_row(model, row); }
        for op_id in ids {
            if let OpSource::TransferGroup { members, .. } = &self.ops[&op_id].op.origin {
                if let Some(member) = members.iter().find(|member| member.row_id == row).cloned() {
                    if let Some(pending) = self.member_removals.get_mut(&op_id) {
                        if pending.member.transfer_id != member.transfer_id { pending.deferred.push(OutboxInput::RowRemoved { model: model.into(), row_id: row.into(), revision }); }
                        continue;
                    }
                    if members.len() > 1 {
                        self.member_removals.entry(op_id).or_insert(MemberRemoval { member, in_flight: false, storage_failures: 0, land_after_storage: false, deferred: Vec::new() });
                        self.store_member(op_id, fx);
                    } else {
                        if let Some(operation) = self.ops.get_mut(&op_id) { operation.stage = Stage::RollingBack(Why::Discarded); }
                        self.store(op_id, CommitKind::Rollback, fx);
                    }
                    continue;
                }
            }
            self.removed.insert(op_id);
            self.waiting.remove(&op_id);
            self.remove_row_operation(op_id, fx);
        }
    }

    pub(super) fn store_member(&mut self, op_id: OpId, fx: &mut Effects<Outbox>) {
        let Some(pending) = self.member_removals.get_mut(&op_id).filter(|pending| !pending.in_flight) else { return };
        pending.in_flight = true;
        let member = pending.member.clone();
        let Some(operation) = self.ops.get(&op_id) else { return };
        let OpSource::TransferGroup { batch_id, members } = &operation.op.origin else { return };
        let origin = crate::owners::store::IntentOrigin::TransferGroup { batch_id: *batch_id, transfer_ids: members.iter().filter(|other| other.transfer_id != member.transfer_id).map(|other| other.transfer_id).collect() };
        let basis = std::collections::BTreeMap::from([(operation.op.model.clone(), self.generation_of(&operation.op.model))]);
        let batch = self.next_batch;
        self.next_batch += 1;
        self.batches.insert(batch, Batch::Member { op_id });
        self.commit(batch, basis, vec![crate::owners::store::StoreOp::IntentOrigin { op_id, origin }, crate::owners::store::StoreOp::RemoveUploadByTransferId { transfer_id: member.transfer_id }, crate::owners::store::StoreOp::TransferOutcome { transfer_id: member.transfer_id, cycle_id: member.cycle_id, outcome: OutboxOutcome::Discarded }], 0, fx);
    }

    pub(super) fn member_committed(&mut self, op_id: OpId, fx: &mut Effects<Outbox>) {
        let Some(pending) = self.member_removals.remove(&op_id) else { return };
        if let Some(operation) = self.ops.get_mut(&op_id) {
            if let OpSource::TransferGroup { members, .. } = &mut operation.op.origin { members.retain(|member| member.transfer_id != pending.member.transfer_id); }
        }
        if let Some(transfer) = self.peers.transfer { fx.push(Effect::Send(Outbound::new(transfer, super::transfer::TransferInput::MutationOutcome { transfer_id: pending.member.transfer_id, cycle_id: pending.member.cycle_id, outcome: OutboxOutcome::Discarded, intent_removed: false }))); }
        for input in pending.deferred { self.handle(input, fx); }
    }

    pub(super) fn member_rejected(&mut self, op_id: OpId, reason: crate::owners::contract::RejectReason, fx: &mut Effects<Outbox>) {
        if matches!(reason, crate::owners::contract::RejectReason::StaleGeneration { .. }) {
            if let Some(pending) = self.member_removals.get_mut(&op_id) { pending.in_flight = false; }
            return self.store_member(op_id, fx);
        }
        if matches!(reason, crate::owners::contract::RejectReason::NoSession | crate::owners::contract::RejectReason::Closed) {
            self.member_removals.remove(&op_id);
            return self.finish(op_id, OutboxOutcome::Ended(end_cause(&reason)), false, fx);
        }
        if let Some(pending) = self.member_removals.get_mut(&op_id) {
            if matches!(reason, crate::owners::contract::RejectReason::Storage(_)) { pending.storage_failures = pending.storage_failures.saturating_add(1); }
            if pending.storage_failures >= 2 && !pending.deferred.is_empty() {
                pending.land_after_storage = true;
                if let Some(operation) = self.ops.get_mut(&op_id) { if let OpSource::TransferGroup { members, .. } = &mut operation.op.origin { members.retain(|member| member.transfer_id != pending.member.transfer_id); } }
            }
        }
        if let Some(pending) = self.member_removals.get_mut(&op_id) { pending.in_flight = false; }
        let deferred = self.member_removals.get_mut(&op_id).map(|pending| std::mem::take(&mut pending.deferred)).unwrap_or_default();
        for input in deferred { self.handle(input, fx); }
    }

    pub(super) fn remove_row_operation(&mut self, op_id: OpId, fx: &mut Effects<Outbox>) {
        if self.removing.contains(&op_id) || self.batches.values().any(|batch| matches!(batch, super::outbox::Batch::Operation { op_id: id, .. } if *id == op_id)) { return; }
        self.removing.insert(op_id);
        self.store(op_id, CommitKind::RemoveRow, fx);
    }

    /// What a waiter of `OutcomeDiscarding` is told when the discard of its cycle answers `answer`: the rollback is the failed cycle `Rejected`, a refused rollback is `Storage` with the original error.
    pub(super) fn discard_answer(&mut self, reply: ReplyId, answer: OutboxAnswer) -> OutboxAnswer {
        let Some(error) = self.discard_outcomes.remove(&reply) else { return answer };
        match answer {
            OutboxAnswer::Ok => OutboxAnswer::Outcome(OutboxOutcome::Rejected { error }),
            OutboxAnswer::Err(Fault::Rejected(RejectReason::Storage(detail))) => OutboxAnswer::Err(Fault::Rejected(RejectReason::Storage(format!("{detail}; the failed cycle: {error}")))),
            other => other,
        }
    }

    /// The rollback of a discarded failed cycle was refused by the Store: the waiters of `OutcomeDiscarding` get `Storage`, the intent stays `Failed`. A plain `Discard` waiter keeps the retry of the rollback.
    pub(super) fn outcome_discard_refused(&mut self, op_id: OpId, detail: String, fx: &mut Effects<Outbox>) {
        let ours: Vec<ReplyId> = self.discard_waiters.iter().filter(|(id, reply)| *id == op_id && self.discard_outcomes.contains_key(reply)).map(|(_, reply)| *reply).collect();
        self.discard_waiters.retain(|(_, reply)| !ours.contains(reply));
        for reply in ours {
            let answer = self.discard_answer(reply, OutboxAnswer::Err(Fault::Rejected(RejectReason::Storage(detail.clone()))));
            Self::reply(fx, reply, answer);
        }
        if self.discard_waiters.iter().any(|(id, _)| *id == op_id) {
            self.retry_timer(op_id, fx);
        } else if let Some(operation) = self.ops.get_mut(&op_id) {
            operation.stage = Stage::Failed;
        }
    }

    /// `Outcome` with `discard_on_failure`: a `Failed` cycle that is the current one of the receipt starts the `Discard` of this `op_id` and its reply waits for the commit of the rollback.
    pub(super) fn outcome_discarding(&mut self, receipt: Receipt, reply: ReplyId, fx: &mut Effects<Outbox>) {
        let current = self.epoch == receipt.epoch && self.ops.get(&receipt.op_id).is_some_and(|operation| operation.cycle_id == receipt.cycle_id);
        let stage = self.ops.get(&receipt.op_id).map(|operation| operation.stage);
        match stage {
            Some(Stage::Failed) if current => {
                let error = self.ops[&receipt.op_id].error.clone().unwrap_or_else(|| "refused".into());
                self.discard_outcomes.insert(reply, error);
                self.discard_waiters.push((receipt.op_id, reply));
                if let Some(operation) = self.ops.get_mut(&receipt.op_id) { operation.stage = Stage::RollingBack(Why::Discarded); }
                self.store(receipt.op_id, CommitKind::Rollback, fx);
            }
            Some(Stage::RollingBack(Why::Discarded)) if current && self.ops[&receipt.op_id].error.is_some() && self.discard_outcomes.values().next().is_some() => {
                let error = self.ops[&receipt.op_id].error.clone().unwrap_or_else(|| "refused".into());
                self.discard_outcomes.insert(reply, error);
                self.discard_waiters.push((receipt.op_id, reply));
            }
            _ => self.outcome(receipt, reply, fx),
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
            Stage::Done => match operation.outcome.clone() { Some(outcome) => Self::reply(fx, reply, OutboxAnswer::Outcome(outcome)), None => Self::reply(fx, reply, OutboxAnswer::Err(Fault::Internal)) },
            Stage::Failed => Self::reply(fx, reply, OutboxAnswer::Outcome(OutboxOutcome::Failed { error: operation.error.clone().unwrap_or_else(|| "refused".into()) })),
            _ => self.outcome_waiters.push((receipt.op_id, reply)),
        }
    }

    /// `(none) x Submit`: an unsent `create` that a `destroy` of the same row cancels ends with it, the network is not called.
    pub(super) fn submit(&mut self, epoch: Epoch, mut op: NewOp, reply: ReplyId, fx: &mut Effects<Outbox>) {
        if epoch != self.epoch {
            return Self::reply(fx, reply, OutboxAnswer::Err(Fault::NoSession));
        }
        if !self.declarations.contains(&(op.model.clone(), op.action.clone())) {
            return Self::reply(fx, reply, OutboxAnswer::Err(Fault::Invalid));
        }
        self.now = self.now.max(op.created_at);
        Self::resolve_operation(&self.aliases, &mut op);
        let row = op.row_id.clone();
        let unsent_create = (op.action == "destroy")
            .then(|| self.ops.values().find(|other| other.stage == Stage::Queued && !other.sent && other.op.action == "create" && other.op.model == op.model && other.op.row_id == row).map(|other| other.op_id))
            .flatten();
        let op_id = self.next_op;
        self.add_operation(op_id, op, Stage::Submitting);
        self.submit_replies.insert(op_id, reply);
        if let Some(created) = unsent_create {
            if let Some(operation) = self.ops.get_mut(&op_id) { operation.cancels = Some(created); }
            let sequence = self.ops[&created].sequence;
            let model = self.ops[&created].op.model.clone();
            for operation in self.ops.values_mut().filter(|other| other.op_id != op_id && other.sequence >= sequence && other.stage != Stage::Done && other.op.model == model && other.op.row_id == row) {
                operation.cancels = Some(created);
                operation.before_cancel = Some(operation.stage);
                operation.stage = Stage::RollingBack(Why::Discarded);
            }
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
    pub(super) fn submit_reserved(&mut self, epoch: Epoch, op_id: OpId, mut op: NewOp, fx: &mut Effects<Outbox>) {
        if epoch != self.epoch {
            return;
        }
        Self::resolve_operation(&self.aliases, &mut op);
        self.add_operation(op_id, op, Stage::Submitting);
        self.operation_committed(op_id, CommitKind::Intent, (0, Vec::new()), fx);
    }

    pub(super) fn online_changed(&mut self, on: bool, fx: &mut Effects<Outbox>) {
        self.online = on;
        self.drain(fx);
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
        if self.removed.contains(&op_id) { return; }
        if let Some(operation) = self.ops.get_mut(&op_id) { operation.pair_in_flight = false; }
        match outcome {
            AuthorOutcome::Response { body, kind: crate::transport::AnswerKind::Transient { .. } } => {
                if let Some(operation) = self.ops.get_mut(&op_id) { operation.pending_owned = true; operation.reject_error = Some(body); }
                self.transport_failed(op_id, true, fx);
            }
            AuthorOutcome::Response { body, .. } => {
                if let Some(operation) = self.ops.get_mut(&op_id) {
                    operation.pending_owned = false;
                    operation.stage = Stage::Landing;
                    operation.rows_pending = true;
                }
                self.decode_answer(op_id, body, fx);
                self.drain(fx);
            }
            AuthorOutcome::Error { class, error, pending_retained } => {
                if let Some(operation) = self.ops.get_mut(&op_id) {
                    operation.pending_owned = pending_retained;
                    operation.reject_error = Some(super::failure::of_session(&error, class));
                }
                self.transport_failed(op_id, matches!(class, ErrorClass::Network | ErrorClass::Server), fx);
            }
            AuthorOutcome::Dropped => {
                if let Some(operation) = self.ops.get_mut(&op_id) { operation.pending_owned = true; }
                self.transport_failed(op_id, true, fx);
            }
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
