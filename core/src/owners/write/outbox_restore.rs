//! Owner `Outbox`: the opening (`Restored`, `RegisterOperations`, `RestoreReady`) and the phase `Restoring` with the held calls.


use super::outbox::{Outbox, OutboxState};
use super::outbox_types::*;

use crate::owners::contract::{Online};


use crate::runtime::{Effects};



impl OutboxState {
    /// The class of a restored intent: every intent goes to exactly one class.
    pub(super) fn classify(&mut self, item: StoredOp, pending_lost: bool, fx: &mut Effects<Outbox>) {
        let op_id = item.op_id;
        let (model, action) = (item.op.model.clone(), item.op.action.clone());
        if let Some(error) = item.restore_failed.clone() {
            self.add_operation(op_id, item.op, Stage::Failed);
            let _ = error;
            if let Some(operation) = self.ops.get_mut(&op_id) {
                operation.restore_element = item.restore_element;
            }
            return self.restored_failed(op_id, "RestoreFailed".into());
        }
        if !self.declarations.contains(&(model, action)) {
            self.add_operation(op_id, item.op.clone(), Stage::WaitingDeclaration);
            self.waiting.insert(op_id, (item, pending_lost));
            return;
        }
        let StoredOp { answered, op, error_mark, pending, attempts, cycle_id, .. } = item;
        self.add_operation(op_id, op, Stage::Queued);
        if let Some(operation) = self.ops.get_mut(&op_id) {
            operation.attempts = attempts;
            operation.cycle_id = cycle_id.max(1);
            operation.sent = true;
        }
        if pending_lost {
            return self.restored_failed(op_id, "StateLost".into());
        }
        if let Some((request_id, answer)) = answered {
            crate::owners::contract::seen_request_id(request_id);
            if let Some(operation) = self.ops.get_mut(&op_id) {
                operation.request_id = request_id;
                operation.stage = Stage::Sending;
            }
            return match answer {
                crate::owners::session::StoredAnswer::Ok(body) => {
                    if let Some(operation) = self.ops.get_mut(&op_id) {
                        operation.stage = Stage::Landing;
                        operation.rows_pending = true;
                    }
                    self.decode_answer(op_id, body, fx)
                }
                _ => self.transport_failed(op_id, false, fx),
            };
        }
        if error_mark {
            return self.restored_failed(op_id, "refused".into());
        }
        if attempts >= ATTEMPT_LIMIT {
            return self.fail_with(op_id, "Exhausted".into(), fx);
        }
        let held_pair = pending.is_some();
        if let Some((request_id, sequence)) = pending {
            crate::owners::contract::seen_request_id(request_id);
            if let Some(operation) = self.ops.get_mut(&op_id) {
                operation.request_id = request_id;
                operation.sequence = sequence;
            }
        }
        let _ = held_pair;
        if self.goes_out(op_id) {
            self.begin_attempt(op_id, fx);
        }
    }

    /// No operation owns these answers: one batch takes them out, nothing waits for its result.
    pub(super) fn remove_orphan_answers(&mut self, request_ids: Vec<crate::owners::contract::RequestId>, fx: &mut Effects<Outbox>) {
        if request_ids.is_empty() {
            return;
        }
        let batch = self.next_batch;
        self.next_batch += 1;
        let ops = request_ids.into_iter().map(|request_id| crate::owners::store::StoreOp::RemoveAnswer { request_id }).collect();
        self.commit(batch, std::collections::BTreeMap::new(), ops, 0, fx);
    }

    pub(super) fn restored_failed(&mut self, op_id: OpId, error: String) {
        if let Some(operation) = self.ops.get_mut(&op_id) {
            operation.stage = Stage::Failed;
            operation.error = Some(error.clone());
            let cycle = operation.cycle_id;
            operation.failed_errors.insert(cycle, error);
        }
    }

    pub(super) fn restored(&mut self, stored: Vec<StoredOp>, pending_lost: bool, fx: &mut Effects<Outbox>) {
        let mut stored = stored;
        stored.sort_by_key(|item| item.op_id);
        for item in stored {
            self.classify(item, pending_lost, fx);
        }
    }

    pub(super) fn register(&mut self, declarations: Vec<(String, String)>, fx: &mut Effects<Outbox>) {
        self.declarations.extend(declarations);
        if self.phase == Phase::Restoring {
            return;
        }
        let ready: Vec<OpId> = self.waiting.iter().filter(|(_, (item, _))| self.declarations.contains(&(item.op.model.clone(), item.op.action.clone()))).map(|(op_id, _)| *op_id).collect();
        for op_id in ready {
            if let Some((item, pending_lost)) = self.waiting.remove(&op_id) {
                self.ops.remove(&op_id);
                self.classify(item, pending_lost, fx);
            }
        }
    }

    /// `RestoreReady`: the network effects of the restored operations go out in the order of `seq`.
    pub(super) fn restore_ready(&mut self, fx: &mut Effects<Outbox>) {
        if self.phase != Phase::Classified {
            return;
        }
        self.phase = Phase::Ready;
        let mut deferred = std::mem::take(&mut self.deferred);
        deferred.sort_unstable();
        for op_id in deferred {
            if self.stage_of(op_id) == Some(Stage::PersistingAttempt) {
                self.store(op_id, CommitKind::Attempt, fx);
            }
        }
    }

    /// Every held call gets exactly one answer.
    pub(super) fn answer_held(&mut self, fx: &mut Effects<Outbox>) {
        for input in std::mem::take(&mut self.held) {
            match input {
                OutboxInput::Submit { reply, .. } | OutboxInput::Retry { reply, .. } | OutboxInput::Discard { reply, .. } | OutboxInput::DiscardRestoreFailed { reply, .. } => Self::reply(fx, reply, OutboxAnswer::Err(Fault::Interrupted)),
                OutboxInput::Outcome { reply, .. } => Self::reply(fx, reply, OutboxAnswer::Outcome(OutboxOutcome::Interrupted)),
                OutboxInput::ReserveOp { transfer_id, .. } => self.opened(fx, transfer_id, Err(Fault::Interrupted)),
                _ => {}
            }
        }
    }

    /// `Restoring`: the owner holds the calls until `Restored`.
    pub(super) fn restoring(&mut self, input: OutboxInput, fx: &mut Effects<Outbox>) {
        match input {
            OutboxInput::Submit { epoch, op, reply } => {
                if epoch == self.epoch {
                    self.held.push(OutboxInput::Submit { epoch, op, reply });
                } else {
                    Self::reply(fx, reply, OutboxAnswer::Err(Fault::NoSession));
                }
            }
            OutboxInput::ReservedFloor { op_id } => self.next_op = self.next_op.max(op_id + 1),
            held @ (OutboxInput::ReserveOp { .. } | OutboxInput::SubmitReserved { .. }) => self.held.push(held),
            held @ (OutboxInput::Retry { .. } | OutboxInput::Discard { .. } | OutboxInput::DiscardRestoreFailed { .. } | OutboxInput::Outcome { .. }) => self.held.push(held),
            OutboxInput::Online(Online(on)) => self.online = on,
            OutboxInput::ModelsReset(reset) => {
                for model in reset.models {
                    self.generation.insert(model, reset.generation);
                }
            }
            OutboxInput::EpochStarted(started) => self.base_generation = started.generation,
            OutboxInput::RegisterOperations { declarations } => self.declarations.extend(declarations),
            OutboxInput::EpochEnded(_) => {
                self.ended = true;
                self.answer_held(fx);
            }
            OutboxInput::Closing => {
                self.answer_held(fx);
                self.closing = true;
                self.maybe_drained(fx);
            }
            OutboxInput::Restored { ops, pending_lost } if !self.ended => {
                self.phase = Phase::Classified;
                self.restored(ops, pending_lost, fx);
                self.replay_held(fx);
            }
            OutboxInput::RestoreFailed { error } if !self.ended => {
                self.queue_error = Some(error);
                self.phase = Phase::Classified;
                self.replay_held(fx);
            }
            OutboxInput::OrphanAnswers { .. } | OutboxInput::Resolve { .. } | OutboxInput::Landed { .. } | OutboxInput::WhenQuiet { .. } | OutboxInput::LocalWrite { .. } | OutboxInput::CopyDone { .. } | OutboxInput::CopyFailed { .. } | OutboxInput::Expire { .. } | OutboxInput::ArmExpiry { .. } | OutboxInput::DisarmExpiry { .. } | OutboxInput::ExpireRow { .. } | OutboxInput::Committed(_) | OutboxInput::Rejected(_) => self.handle(input, fx),
            _ => {}
        }
    }

    pub(super) fn replay_held(&mut self, fx: &mut Effects<Outbox>) {
        for input in std::mem::take(&mut self.held) {
            self.handle(input, fx);
        }
    }

}
