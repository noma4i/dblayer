//! Owner `Outbox`: the rows of the table for the network, the retry timers, `retry`, `discard`, `outcome`, `Submit` and `Online`.


use super::outbox::{Batch, MemberRemoval, Outbox, OutboxState};
use super::outbox_types::*;
use crate::owners::contract::RequestId;

use crate::owners::contract::{RejectReason, ReplyId};

use crate::owners::store::{RowValue};
use crate::owners::session::SessionInput;
use crate::runtime::{Effect, Effects, Epoch, Outbound, ScheduleId};



/// What the decoded answer of a mutation carries.
pub(super) struct Answered {
    pub rows: Vec<(String, Vec<RowValue>)>,
    pub swaps: Vec<(String, String)>,
    pub stale: Vec<(String, String)>,
    pub result: Option<String>,
    pub missing: Vec<String>,
    pub missing_destroy: bool,
}

impl OutboxState {
    /// The rows of a saved answer arrived: the operation goes on as one whose live answer arrived.
    pub(super) fn rows_arrived(&mut self, op_id: OpId) {
        if let Some(operation) = self.ops.get_mut(&op_id).filter(|operation| operation.rows_pending && operation.stage == Stage::Landing) {
            operation.rows_pending = false;
            operation.stage = Stage::Sending;
        }
    }

    pub(super) fn on_response(&mut self, now: u64, op_id: OpId, answer: Answered, errors: Option<bool>, fx: &mut Effects<Outbox>) {
        if self.removed.contains(&op_id) { return; }
        let Answered { rows, swaps, stale, result, missing, missing_destroy } = answer;
        self.rows_arrived(op_id);
        if self.stage_of(op_id) != Some(Stage::Sending) {
            return;
        }
        if let Some(operation) = self.ops.get_mut(&op_id) { operation.pair_in_flight = false; }
        if let Some(temporary) = errors {
            return self.transport_failed(now, op_id, temporary, None, fx);
        }
        if let Some(operation) = self.ops.get_mut(&op_id) {
            operation.stage = Stage::Landing;
            operation.landed = swaps.iter().find(|(temp, _)| *temp == operation.op.row_id).or(swaps.first()).map(|(_, real)| real.clone()).or_else(|| rows.iter().find(|(model, _)| *model == operation.op.model).and_then(|(_, rows)| rows.last()).map(|row| row.row_id.clone()));
            operation.landing = rows;
            operation.missing_destroy = missing_destroy;
            operation.swaps = swaps;
            operation.missing = missing;
            operation.stale = stale;
            operation.result = result;
        }
        self.store(op_id, CommitKind::Land, fx);
        self.drain(now, fx);
    }

    /// The outcome of an attempt that did not answer: its cause replaces the kept one in memory and, when it differs from the row, in a Store batch before the wait of the Backoff.
    pub(super) fn transport_failed(&mut self, now: u64, op_id: OpId, temporary: bool, cause: Option<crate::value::sync::FailureKind>, fx: &mut Effects<Outbox>) {
        if self.removed.contains(&op_id) { return; }
        if self.stage_of(op_id) != Some(Stage::Sending) {
            return;
        }
        if let Some(operation) = self.ops.get_mut(&op_id) { operation.pair_in_flight = false; operation.cause = cause; }
        if self.ops.get(&op_id).is_some_and(|operation| operation.cause != operation.cause_stored) {
            if let Some(operation) = self.ops.get_mut(&op_id) { operation.cause_stored = operation.cause; }
            self.store(op_id, CommitKind::Cause, fx);
        }
        if !temporary {
            self.reject_or_keep(op_id, fx);
            return self.drain(now, fx);
        }
        let attempts = self.ops[&op_id].attempts;
        if attempts >= ATTEMPT_LIMIT {
            self.fail_exhausted(op_id, fx);
        } else if self.is_online() {
            if let Some(operation) = self.ops.get_mut(&op_id) {
                operation.waiting_retry = true;
            }
            self.retry_timer(op_id, fx);
        } else {
            self.requeue(op_id, fx);
        }
        self.drain(now, fx);
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
            Some(Stage::Sending) if self.is_online() && self.slots_taken() - usize::from(!self.ops[&op_id].waiting_retry) >= DRAIN_LIMIT => {
                self.requeue(op_id, fx);
            }
            Some(Stage::Sending) if self.is_online() => {
                if self.ops[&op_id].attempts < ATTEMPT_LIMIT {
                    self.begin_attempt(op_id, fx);
                } else {
                    self.fail_exhausted(op_id, fx);
                }
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
                _ if self.complete && !self.declarations.contains(&(self.ops[&op_id].op.model.clone(), self.ops[&op_id].op.action.clone())) => {
                    let text = super::failure::undeclared_message(&self.ops[&op_id].op.model, &self.ops[&op_id].op.action);
                    Self::reply(fx, reply, OutboxAnswer::Err(Fault::Rejected(crate::owners::contract::RejectReason::Invalid(text))));
                }
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
        self.discard_op(op_id, reply, fx);
    }

    /// The stages of the internal steps of the library: a `Discard` waits in them for the stage the operation leaves to (D-392).
    pub(super) fn defers_discard(stage: Stage) -> bool { matches!(stage, Stage::Submitting | Stage::PersistingAttempt | Stage::MarkingFailed | Stage::MarkingRetry) }

    /// The end of every step of the owner: an operation that left the stage of an internal step runs its waiting `Discard` commands by the row of its stage.
    pub(super) fn run_waiting_discards(&mut self, fx: &mut Effects<Outbox>) {
        let waiting = std::mem::take(&mut self.discard_waiting);
        for (op_id, reply) in waiting {
            match self.ops.get(&op_id).map(|operation| operation.stage) {
                Some(stage) if Self::defers_discard(stage) => self.discard_waiting.push((op_id, reply)),
                _ => self.discard_op(op_id, reply, fx),
            }
        }
    }

    pub(super) fn discard_op(&mut self, op_id: OpId, reply: ReplyId, fx: &mut Effects<Outbox>) {
        if self.replacements.values().any(|old| *old == Some(op_id)) { return Self::reply(fx, reply, OutboxAnswer::Err(Fault::InFlight)); }
        match self.ops[&op_id].stage {
            Stage::Submitting | Stage::PersistingAttempt | Stage::MarkingFailed | Stage::MarkingRetry => self.discard_waiting.push((op_id, reply)),
            Stage::Sending | Stage::Landing | Stage::WaitingDeclaration => Self::reply(fx, reply, OutboxAnswer::Err(Fault::InFlight)),
            Stage::Queued | Stage::Failed => {
                self.discard_waiters.push((op_id, reply));
                self.start_discard(op_id, fx);
            }
            Stage::RollingBack(_) => self.discard_waiters.push((op_id, reply)),
            Stage::Done => Self::reply(fx, reply, OutboxAnswer::Err(Fault::NotFound)),
        }
    }

    pub(super) fn start_discard(&mut self, op_id: OpId, fx: &mut Effects<Outbox>) {
        if let Some(operation) = self.ops.get_mut(&op_id) { operation.stage = Stage::RollingBack(Why::Discarded); }
        self.store(op_id, CommitKind::Rollback, fx);
    }

    /// The server removed the row: each of its open operations ends `Discarded`, a request on its way is released.
    pub(super) fn rows_gone(&mut self, now: u64, ops: Vec<OpId>, fx: &mut Effects<Outbox>) {
        for op_id in ops {
            let Some(operation) = self.ops.get(&op_id) else { continue };
            if matches!(operation.stage, Stage::Done | Stage::Landing | Stage::RollingBack(_) | Stage::Submitting) { continue; }
            if operation.sent {
                if let Some(session) = self.peers.session { fx.push(Effect::Send(Outbound::new(session, SessionInput::Cancel { req_id: operation.request_id }))); }
            }
            self.finish(now, op_id, OutboxOutcome::Discarded, false, fx);
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
                        self.waiting.remove(&op_id);
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

    pub(super) fn member_committed(&mut self, now: u64, op_id: OpId, fx: &mut Effects<Outbox>) {
        let Some(pending) = self.member_removals.remove(&op_id) else { return };
        if let Some(operation) = self.ops.get_mut(&op_id) {
            if let OpSource::TransferGroup { members, .. } = &mut operation.op.origin { members.retain(|member| member.transfer_id != pending.member.transfer_id); }
        }
        if let Some((item, _)) = self.waiting.get_mut(&op_id) {
            if let OpSource::TransferGroup { members, .. } = &mut item.op.origin { members.retain(|member| member.transfer_id != pending.member.transfer_id); }
            item.removed_rows.retain(|(_, row_id, _)| row_id != &pending.member.row_id);
        }
        if let Some(transfer) = self.peers.transfer { fx.push(Effect::Send(Outbound::new(transfer, super::transfer::TransferInput::MutationOutcome { transfer_id: pending.member.transfer_id, cycle_id: pending.member.cycle_id, outcome: OutboxOutcome::Discarded, intent_removed: false, row_never_created: false }))); }
        for input in pending.deferred { self.handle(now, input, fx); }
        if self.waiting.get(&op_id).is_some_and(|(item, _)| item.removed_rows.is_empty() && self.declarations.contains(&(item.op.model.clone(), item.op.action.clone()))) {
            if let Some((item, pending_lost)) = self.waiting.remove(&op_id) {
                self.ops.remove(&op_id);
                self.classify(now, item, pending_lost, fx);
            }
        }
    }

    pub(super) fn member_rejected(&mut self, now: u64, op_id: OpId, reason: RejectReason, fx: &mut Effects<Outbox>) {
        match &reason {
            RejectReason::StaleGeneration { .. } => {
                if let Some(pending) = self.member_removals.get_mut(&op_id) { pending.in_flight = false; }
                return self.store_member(op_id, fx);
            }
            RejectReason::NoSession | RejectReason::Closed => {
                self.member_removals.remove(&op_id);
                return self.finish(now, op_id, OutboxOutcome::Ended(end_cause(&reason)), false, fx);
            }
            RejectReason::Invalid(_) => return self.member_committed(now, op_id, fx),
            RejectReason::Storage(_) => {}
        }
        if let Some(pending) = self.member_removals.get_mut(&op_id) {
            pending.storage_failures = pending.storage_failures.saturating_add(1);
            if pending.storage_failures >= 2 && !pending.deferred.is_empty() {
                pending.land_after_storage = true;
                if let Some(operation) = self.ops.get_mut(&op_id) { if let OpSource::TransferGroup { members, .. } = &mut operation.op.origin { members.retain(|member| member.transfer_id != pending.member.transfer_id); } }
            }
        }
        if let Some(pending) = self.member_removals.get_mut(&op_id) { pending.in_flight = false; }
        let deferred = self.member_removals.get_mut(&op_id).map(|pending| std::mem::take(&mut pending.deferred)).unwrap_or_default();
        for input in deferred { self.handle(now, input, fx); }
    }

    pub(super) fn remove_row_operation(&mut self, op_id: OpId, fx: &mut Effects<Outbox>) {
        if self.removing.contains(&op_id) || self.batches.values().any(|batch| matches!(batch, super::outbox::Batch::Operation { op_id: id, kind } if *id == op_id && *kind != CommitKind::Cause)) { return; }
        self.removing.insert(op_id);
        self.store(op_id, CommitKind::RemoveRow, fx);
    }

    pub(super) fn outcome(&mut self, receipt: Receipt, reply: ReplyId, discard_on_failure: bool, fx: &mut Effects<Outbox>) {
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
            Stage::Failed if discard_on_failure && receipt.cycle_id == operation.cycle_id => {
                let error = operation.error.clone().unwrap_or_else(|| "refused".into());
                self.outcome_waiters.push((receipt.op_id, receipt.cycle_id, reply, true));
                self.discard_failed_outcome(receipt.op_id, receipt.cycle_id, error, fx);
            }
            Stage::Failed => Self::reply(fx, reply, OutboxAnswer::Outcome(OutboxOutcome::Failed { error: operation.error.clone().unwrap_or_else(|| "refused".into()) })),
            _ => self.outcome_waiters.push((receipt.op_id, receipt.cycle_id, reply, discard_on_failure)),
        }
    }

    /// `(none) x Submit`: an unsent `create` that a `destroy` of the same row cancels ends with it, the network is not called.
    pub(super) fn submit(&mut self, now: u64, epoch: Epoch, mut op: NewOp, reply: ReplyId, fx: &mut Effects<Outbox>) {
        if epoch != self.epoch {
            return Self::reply(fx, reply, OutboxAnswer::Err(Fault::NoSession));
        }
        if !self.declarations.contains(&(op.model.clone(), op.action.clone())) {
            return Self::reply(fx, reply, OutboxAnswer::Err(Fault::Invalid));
        }
        Self::resolve_operation(&self.aliases, &mut op);
        let row = op.row_id.clone();
        let unsent_create = (op.action == "destroy")
            .then(|| self.ops.values().find(|other| other.stage == Stage::Queued && !other.sent && other.op.action == "create" && other.op.model == op.model && other.op.row_id == row).map(|other| other.op_id))
            .flatten();
        let op_id = self.next_op;
        self.add_operation(op_id, op, Stage::Submitting, now);
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

    pub(super) fn reserve_replacement(&mut self, epoch: Epoch, target: ReserveTarget, transfer_id: u64, cycle_id: u64, fx: &mut Effects<Outbox>) {
        let result = if epoch != self.epoch {
            Err(Fault::NoSession)
        } else if let ReserveTarget::Transfer(new_id) = target {
            let old = self.open_for_transfer(transfer_id, cycle_id);
            if old.is_some_and(|id| self.stage_of(id) != Some(Stage::Failed)) {
                Err(Fault::InFlight)
            } else {
                let operation = old.and_then(|id| self.ops.get_mut(&id)).map(|operation| {
                    operation.stage = Stage::RollingBack(Why::Discarded);
                    ReplacementOperation { op_id: operation.op_id, request_id: operation.request_id, release_pending: operation.pending_owned || operation.pair_in_flight }
                });
                self.replacements.insert(new_id, old);
                let op_id = self.next_op;
                self.next_op += 1;
                Ok((op_id, operation))
            }
        } else { Err(Fault::Invalid) };
        if let Some(transfer) = self.peers.transfer {
            fx.push(Effect::Send(Outbound::new(transfer, super::transfer::TransferInput::ReplacementReserved { target, result })));
        }
    }

    pub(super) fn finish_replacement(&mut self, now: u64, target: ReserveTarget, committed: bool, fx: &mut Effects<Outbox>) {
        let ReserveTarget::Transfer(id) = target else { return };
        let Some(Some(old)) = self.replacements.remove(&id) else { return };
        if committed {
            if let Some(operation) = self.ops.get_mut(&old) {
                if operation.pending_owned || operation.pair_in_flight {
                    operation.pending_owned = false;
                    operation.pair_in_flight = false;
                    if let Some(session) = self.peers.session { fx.push(Effect::Send(Outbound::new(session, SessionInput::Release { req_id: operation.request_id }))); }
                }
            }
            self.finish(now, old, OutboxOutcome::Discarded, true, fx);
        } else if let Some(operation) = self.ops.get_mut(&old) {
            operation.stage = Stage::Failed;
        }
    }

    pub(super) fn opened(&self, fx: &mut Effects<Outbox>, target: ReserveTarget, result: Result<OpId, Fault>) {
        if let Some(transfer) = self.peers.transfer {
            fx.push(Effect::Send(Outbound::new(transfer, super::transfer::TransferInput::OpReserved { target, result })));
        }
    }

    /// `Submit` with the intent written by the batch of Transfer: the operation goes out as after `Submitting x Committed`, with no write and no receipt.
    pub(super) fn submit_reserved(&mut self, now: u64, epoch: Epoch, op_id: OpId, mut op: NewOp, created_at: u64, fx: &mut Effects<Outbox>) {
        if epoch != self.epoch {
            return;
        }
        Self::resolve_operation(&self.aliases, &mut op);
        self.add_operation(op_id, op, Stage::Submitting, created_at);
        self.operation_committed(now, op_id, CommitKind::Intent, (0, Vec::new()), fx);
        self.drain(now, fx);
    }

    pub(super) fn online_changed(&mut self, now: u64, on: bool, fx: &mut Effects<Outbox>) {
        self.online = Some(on);
        if !on {
            let waiting: Vec<OpId> = self.ops.values().filter(|operation| operation.stage == Stage::Sending && operation.waiting_retry).map(|operation| operation.op_id).collect();
            for op_id in waiting {
                fx.push(Effect::Cancel(ScheduleId(op_id)));
                self.requeue(op_id, fx);
            }
        }
        self.drain(now, fx);
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
    pub(super) fn answered(&mut self, now: u64, req_id: RequestId, outcome: crate::owners::session::AuthorOutcome, fx: &mut Effects<Outbox>) {
        use crate::owners::contract::ErrorClass;
        use crate::owners::session::AuthorOutcome;
        let Some(op_id) = self.ops.values().find(|operation| operation.request_id == req_id && (operation.stage == Stage::Sending || (operation.stage == Stage::Failed && operation.pending_owned))).map(|operation| operation.op_id) else { return };
        if self.removed.contains(&op_id) { return; }
        if self.stage_of(op_id) == Some(Stage::Failed) { return self.failed_pair_answered(now, op_id, outcome, fx); }
        if let Some(operation) = self.ops.get_mut(&op_id) { operation.pair_in_flight = false; }
        match outcome {
            AuthorOutcome::Response { body, kind: crate::transport::AnswerKind::Transient { .. }, .. } => {
                if let Some(operation) = self.ops.get_mut(&op_id) { operation.pending_owned = true; operation.reject_error = Some(body); }
                self.transport_failed(now, op_id, true, None, fx);
            }
            AuthorOutcome::Response { body, .. } => {
                if let Some(operation) = self.ops.get_mut(&op_id) {
                    operation.pending_owned = false;
                    operation.stage = Stage::Landing;
                    operation.rows_pending = true;
                }
                self.decode_answer(op_id, body, fx);
                self.drain(now, fx);
            }
            AuthorOutcome::Error { class, error, pending_retained } => {
                if let Some(operation) = self.ops.get_mut(&op_id) {
                    operation.pending_owned = pending_retained;
                    operation.reject_error = Some(super::failure::of_session(&error, class));
                }
                let cause = if let crate::owners::session::SessionError::ResponseTooLarge { limit } = &error { Some(crate::value::sync::FailureKind::ResponseTooLarge { limit: *limit }) } else { None };
                self.transport_failed(now, op_id, matches!(class, ErrorClass::Network | ErrorClass::Server), cause, fx);
            }
            AuthorOutcome::Dropped => {
                if let Some(operation) = self.ops.get_mut(&op_id) { operation.pending_owned = true; }
                self.transport_failed(now, op_id, true, None, fx);
            }
        }
    }
}

impl OutboxState {
    /// The pair of a `Failed` operation answered after its `Release`: a success lands the operation, a refusal ends the owner mark, a transient answer changes nothing (Session replays the pair itself).
    fn failed_pair_answered(&mut self, now: u64, op_id: OpId, outcome: crate::owners::session::AuthorOutcome, fx: &mut Effects<Outbox>) {
        use crate::owners::session::AuthorOutcome;
        match outcome {
            AuthorOutcome::Response { kind: crate::transport::AnswerKind::Transient { .. }, .. } | AuthorOutcome::Dropped => {}
            AuthorOutcome::Response { body, .. } => {
                if let Some(operation) = self.ops.get_mut(&op_id) {
                    operation.pending_owned = false;
                    operation.error = None;
                    operation.stage = Stage::Landing;
                    operation.rows_pending = true;
                }
                self.decode_answer(op_id, body, fx);
                self.drain(now, fx);
            }
            AuthorOutcome::Error { pending_retained, .. } => {
                if let Some(operation) = self.ops.get_mut(&op_id) { operation.pending_owned = pending_retained; }
            }
        }
    }

    /// No operation is being written, sent, landed or rolled back, and no local write is on its way.
    #[cfg(test)]
    fn is_quiet(&self) -> bool {
        self.writes_active() == 0 && self.ops.values().all(|operation| match operation.stage {
            Stage::Submitting | Stage::PersistingAttempt | Stage::Landing | Stage::RollingBack(_) | Stage::MarkingFailed | Stage::MarkingRetry => false,
            Stage::Sending => operation.waiting_retry || !self.is_online(),
            Stage::WaitingDeclaration | Stage::Queued | Stage::Failed | Stage::Done => true,
        })
    }

    #[cfg(test)]
    pub(super) fn release_quiet(&mut self, fx: &mut Effects<Outbox>) {
        if !self.quiet_waiters.is_empty() && self.is_quiet() {
            for reply in std::mem::take(&mut self.quiet_waiters) { Self::reply(fx, reply, OutboxAnswer::Ok); }
        }
    }
}
