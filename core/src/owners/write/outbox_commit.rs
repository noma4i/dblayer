//! Owner `Outbox`: the answers of the Store to the batches of an operation (`Committed`, `Rejected`).


use super::outbox::{Batch, LocalCall, Outbox, OutboxState};
use super::outbox_types::*;

use crate::owners::contract::{Committed, RejectReason, Rejected};


use crate::owners::session::SessionInput;
use crate::runtime::{Effect, Effects, Outbound, ScheduleId};



impl OutboxState {
    pub(super) fn committed(&mut self, now: u64, committed: Committed, fx: &mut Effects<Outbox>) {
        let Some(batch) = self.batches.remove(&committed.batch) else { return };
        match batch {
            Batch::Member { op_id } => self.member_committed(now, op_id, fx),
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
            Batch::Operation { op_id, kind } => {
                if let Some(operation) = self.ops.get_mut(&op_id) { operation.storage_refusals = 0; }
                self.operation_committed(now, op_id, kind, (committed.revision, committed.shown), fx);
                self.drain(now, fx);
            }
        }
    }

    /// The operation of a row the server removed ends `Discarded`: its removal landed, or the Store refused it as invalid data (P-OUT-60).
    fn discard_removed(&mut self, now: u64, op_id: OpId, fx: &mut Effects<Outbox>) {
        self.removed.remove(&op_id);
        self.removing.remove(&op_id);
        if let Some(session) = self.peers.session { fx.push(Effect::Send(Outbound::new(session, SessionInput::Release { req_id: self.ops[&op_id].request_id }))); }
        self.answer_waiters(op_id, &OutboxAnswer::Err(Fault::Ended(crate::ffi::EndCause::Discarded)), fx);
        self.finish(now, op_id, OutboxOutcome::Discarded, true, fx);
    }

    pub(super) fn operation_committed(&mut self, now: u64, op_id: OpId, kind: CommitKind, (revision, shown): (i64, Vec<Option<crate::store::Row>>), fx: &mut Effects<Outbox>) {
        let Some(stage) = self.stage_of(op_id) else { return };
        if kind == CommitKind::Cause { return; }
        if let Some(created) = self.ops[&op_id].cancels.filter(|created| *created != op_id && kind == CommitKind::Intent && stage == Stage::RollingBack(Why::Discarded)) {
            if let Some(operation) = self.ops.get_mut(&op_id) { operation.shown = shown.into_iter().last().flatten(); }
            if let Some(reply) = self.submit_replies.remove(&op_id) { Self::reply(fx, reply, OutboxAnswer::Receipt(self.receipt(op_id, revision))); }
            self.finish_cancelled_commit(now, created, fx);
            return;
        }
        if kind == CommitKind::Rollback && self.ops[&op_id].cancels == Some(op_id) && self.pending_cancelled_intents(op_id) {
            self.cancelled_commits.insert(op_id, revision);
            return;
        }
        if kind == CommitKind::RemoveRow && self.removed.contains(&op_id) {
            return self.discard_removed(now, op_id, fx);
        }
        if self.removed.contains(&op_id) {
            self.remove_row_operation(op_id, fx);
            return;
        }
        match (kind, stage) {
            (CommitKind::Intent, Stage::Submitting) => {
                let shown = shown.into_iter().last().flatten();
                if let Some(operation) = self.ops.get_mut(&op_id) { operation.shown.clone_from(&shown); operation.attempt_unsent = true; }
                let receipt = OutboxAnswer::Receipt(Receipt { optimistic: shown, ..self.receipt(op_id, revision) });
                if let Some(reply) = self.submit_replies.remove(&op_id) {
                    Self::reply(fx, reply, receipt.clone());
                }
                for reply in Self::take_waiters(&mut self.retry_waiters, op_id) {
                    Self::reply(fx, reply, receipt.clone());
                }
                if self.goes_out(op_id) && !self.expired(now, op_id) {
                    self.start_sending(op_id, fx);
                } else if let Some(operation) = self.ops.get_mut(&op_id) {
                    operation.stage = Stage::Queued;
                }
            }
            (CommitKind::Attempt, Stage::PersistingAttempt) => {
                if let Some(operation) = self.ops.get_mut(&op_id) {
                    operation.attempts += 1;
                    operation.attempt_unsent = true;
                }
                if self.is_online() && !self.row_busy(op_id) {
                    self.start_sending(op_id, fx);
                } else {
                    self.requeue(op_id, fx);
                }
            }
            (CommitKind::Land, Stage::Landing) => {
                if let Some(pending) = self.member_removals.remove(&op_id) {
                    if let Some(transfer) = self.peers.transfer { fx.push(Effect::Send(Outbound::new(transfer, super::transfer::TransferInput::MutationOutcome { transfer_id: pending.member.transfer_id, cycle_id: pending.member.cycle_id, outcome: OutboxOutcome::Discarded, intent_removed: true, row_never_created: false }))); }
                }
                let model = self.ops[&op_id].op.model.clone();
                for (temp, real) in self.ops.get(&op_id).map(|operation| operation.swaps.clone()).unwrap_or_default() {
                    self.swap_row(&model, &temp, &real);
                }
                if let Some(operation) = self.ops.get(&op_id) {
                    let families = Self::stale_families_of(operation);
                    if !families.is_empty() { fx.push(Effect::Run(OutboxEffect::Stale { families, revision })); }
                }
                let result = self.ops.get(&op_id).and_then(|operation| operation.result.clone());
                let row_id = self.ops.get(&op_id).map(|operation| operation.landed.clone().unwrap_or_else(|| operation.op.row_id.clone()));
                if let Some(operation) = self.ops.get_mut(&op_id) { operation.shown = shown.last().cloned().flatten(); }
                self.finish(now, op_id, OutboxOutcome::Confirmed { revision, result, row_id }, true, fx);
            }
            (CommitKind::Rollback, Stage::RollingBack(why)) => {
                self.transfer_discards.remove(&op_id);
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
                        self.finish(now, id, OutboxOutcome::Discarded, true, fx);
                    }
                }
                if let Some(operation) = self.ops.get(&op_id).filter(|operation| operation.answer_rows_dropped) {
                    fx.push(Effect::Run(OutboxEffect::Stale { families: Self::stale_families_of(operation), revision }));
                }
                let created = Self::created_temps(&self.ops[&op_id]);
                self.end_temps(created);
                let dependents = self.ops.get_mut(&op_id).map(|operation| std::mem::take(&mut operation.ended_dependents)).unwrap_or_default();
                for id in dependents {
                    if self.ops[&id].pending_owned || self.ops[&id].pair_in_flight {
                        if let Some(operation) = self.ops.get_mut(&id) { operation.pending_owned = false; operation.pair_in_flight = false; }
                        if let Some(session) = self.peers.session { fx.push(Effect::Send(Outbound::new(session, SessionInput::Release { req_id: self.ops[&id].request_id }))); }
                    }
                    if let Some(reply) = self.submit_replies.remove(&id) { Self::reply(fx, reply, OutboxAnswer::Receipt(self.receipt(id, revision))); }
                    if let Some(operation) = self.ops.get_mut(&id) { operation.row_never_created |= operation.reject_error.is_none(); }
                    let error = self.ops[&id].reject_error.clone().unwrap_or_else(super::failure::row_never_created);
                    self.finish(now, id, OutboxOutcome::Rejected { error }, true, fx);
                }
                self.finish(now, op_id, outcome, true, fx);
                self.finish_destroy_of(now, op_id, revision, fx);
            }
            (CommitKind::ErrorMark, Stage::MarkingFailed) => {
                let error = self.ops[&op_id].error.clone().unwrap_or_else(|| "refused".into());
                self.failed(op_id, error, fx);
            }
            (CommitKind::ClearMark, Stage::MarkingRetry) => {
                let mut transfer_ids = Vec::new();
                if let Some(operation) = self.ops.get_mut(&op_id) {
                    operation.error = None;
                    operation.attempt_unsent = true;
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
    }

    /// The `destroy` that cancelled an unsent `create` ends with the rollback of that `create`.
    pub(super) fn finish_destroy_of(&mut self, now: u64, created: OpId, revision: i64, fx: &mut Effects<Outbox>) {
        let destroys: Vec<OpId> = self.ops.values().filter(|operation| operation.cancels == Some(created) && operation.stage == Stage::Submitting).map(|operation| operation.op_id).collect();
        for op_id in destroys {
            if let Some(reply) = self.submit_replies.remove(&op_id) {
                let receipt = self.receipt(op_id, revision);
                Self::reply(fx, reply, OutboxAnswer::Receipt(receipt));
            }
            self.finish(now, op_id, OutboxOutcome::Confirmed { revision, result: None, row_id: None }, false, fx);
        }
    }

    /// The row `temp` became `real`: the open operations of the row address `real`.
    pub(super) fn swap_row(&mut self, model: &str, temp: &str, real: &str) {
        if self.aliases.get(&(model.to_string(), temp.to_string())).is_some_and(|known| known == real) { return; }
        self.bump_selection_row(model, temp);
        self.bump_selection_row(model, real);
        self.aliases.insert((model.to_string(), temp.to_string()), real.to_string());
        for operation in self.ops.values_mut().filter(|operation| operation.stage != Stage::Done && Self::addresses_row(operation, model, temp)) {
            Self::resolve_operation(&self.aliases, &mut operation.op);
        }
    }

    pub(super) fn resolve_operation(aliases: &std::collections::BTreeMap<(String, String), String>, op: &mut NewOp) {
        let resolve = |model: &str, row: &mut String| {
            if let Some(real) = aliases.get(&(model.to_string(), row.clone())) { *row = real.clone(); }
        };
        resolve(&op.model, &mut op.row_id);
        if !aliases.is_empty() && op.body.contains(crate::identity::TEMP_ID_PREFIX) {
            if let Ok(mut body) = serde_json::from_str::<serde_json::Value>(&op.body) {
                let mut changed = false;
                crate::identity::walk_temp_ids(&mut body, &mut |text| {
                    if let Some((_, real)) = aliases.iter().find(|((_, temp), _)| temp == text) { text.clone_from(real); changed = true; }
                });
                if changed { op.body = body.to_string(); }
            }
        }
        for row in &mut op.effects.rows { resolve(&row.model, &mut row.row_id); }
        for member in &mut op.effects.members { resolve(&member.model, &mut member.row_id); }
        for row in &mut op.effects.tombstones { resolve(&row.model, &mut row.row_id); }
        if let Some((model, row)) = &mut op.effects.shown { resolve(model, row); }
        if let OpSource::TransferGroup { members, .. } = &mut op.origin {
            for member in members { resolve(&op.model, &mut member.row_id); }
        }
    }

    pub(super) fn rejected(&mut self, now: u64, rejected: Rejected, fx: &mut Effects<Outbox>) {
        let Some(batch) = self.batches.remove(&rejected.batch) else { return };
        match batch {
            Batch::Member { op_id } => self.member_rejected(now, op_id, rejected.reason, fx),
            Batch::Write { reply, .. } => {
                Self::reply(fx, reply, OutboxAnswer::Err(Fault::of_reject(rejected.reason)));
                self.maybe_drained(fx);
            }
            Batch::Operation { op_id, kind } => {
                self.operation_rejected(now, op_id, kind, rejected.reason, fx);
                self.drain(now, fx);
            }
        }
    }

    pub(super) fn operation_rejected(&mut self, now: u64, op_id: OpId, kind: CommitKind, reason: RejectReason, fx: &mut Effects<Outbox>) {
        let Some(stage) = self.stage_of(op_id) else { return };
        if kind == CommitKind::Cause { return; }
        if !matches!(reason, RejectReason::Storage(_)) { if let Some(operation) = self.ops.get_mut(&op_id) { operation.storage_refusals = 0; } }
        if let Some(created) = self.ops[&op_id].cancels.filter(|created| *created != op_id && kind == CommitKind::Intent && stage == Stage::RollingBack(Why::Discarded)) {
            self.submit_refused(op_id, reason, fx);
            self.ops.remove(&op_id);
            self.finish_cancelled_commit(now, created, fx);
            return;
        }
        if self.removed.contains(&op_id) {
            if kind == CommitKind::RemoveRow { self.removing.remove(&op_id); }
            match reason {
                RejectReason::NoSession | RejectReason::Closed => {
                    self.removed.remove(&op_id);
                    self.answer_waiters(op_id, &OutboxAnswer::Err(Fault::of_reject(reason.clone())), fx);
                    self.finish(now, op_id, OutboxOutcome::Ended(end_cause(&reason)), false, fx);
                }
                RejectReason::Storage(_) => self.storage_retry(op_id, fx),
                RejectReason::Invalid(_) if kind == CommitKind::RemoveRow => self.discard_removed(now, op_id, fx),
                RejectReason::Invalid(_) => self.remove_row_operation(op_id, fx),
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
            RejectReason::Storage(_) if stage == Stage::Submitting => self.submit_refused(op_id, reason, fx),
            RejectReason::Storage(_) if stage == Stage::MarkingRetry => {
                let transfer_ids = self.take_retry_uploads(op_id);
                self.revert_retry(op_id, !transfer_ids.is_empty());
                if !transfer_ids.is_empty() { self.retry_transfer_answer(transfer_ids, Err(Fault::of_reject(reason.clone())), fx); }
                for reply in Self::take_waiters(&mut self.retry_waiters, op_id) {
                    Self::reply(fx, reply, OutboxAnswer::Err(Fault::of_reject(reason.clone())));
                }
            }
            RejectReason::Storage(_) => self.storage_retry(op_id, fx),
            RejectReason::NoSession | RejectReason::Closed if stage == Stage::Submitting => self.submit_refused(op_id, reason, fx),
            RejectReason::NoSession | RejectReason::Closed => {
                if self.ops[&op_id].cancels == Some(op_id) {
                    let following: Vec<OpId> = self.ops.values().filter(|other| other.op_id != op_id && other.cancels == Some(op_id) && other.stage != Stage::Done).map(|other| other.op_id).collect();
                    for id in following {
                        self.answer_waiters(id, &OutboxAnswer::Err(Fault::of_reject(reason.clone())), fx);
                        self.finish(now, id, OutboxOutcome::Ended(end_cause(&reason)), false, fx);
                    }
                }
                self.end_dependents(now, op_id, &reason, fx);
                let transfer_ids = self.take_retry_uploads(op_id);
                if !transfer_ids.is_empty() { self.retry_transfer_answer(transfer_ids, Err(Fault::of_reject(reason.clone())), fx); }
                self.answer_waiters(op_id, &OutboxAnswer::Err(Fault::of_reject(reason.clone())), fx);
                self.finish(now, op_id, OutboxOutcome::Ended(end_cause(&reason)), false, fx);
            }
            RejectReason::Invalid(_) if stage == Stage::Landing && !self.rows_dropped(op_id) => self.without_rows(op_id, CommitKind::Land, fx),
            RejectReason::Invalid(_) if stage == Stage::RollingBack(Why::Rejected) && !self.rows_dropped(op_id) => self.without_rows(op_id, CommitKind::Rollback, fx),
            RejectReason::Invalid(ref detail) => match stage {
                Stage::Submitting => self.submit_refused(op_id, reason, fx),
                Stage::MarkingRetry => {
                    let transfer_ids = self.take_retry_uploads(op_id);
                    self.revert_retry(op_id, !transfer_ids.is_empty());
                    if !transfer_ids.is_empty() { self.retry_transfer_answer(transfer_ids, Err(Fault::of_reject(reason.clone())), fx); }
                    for reply in Self::take_waiters(&mut self.retry_waiters, op_id) {
                        Self::reply(fx, reply, OutboxAnswer::Err(Fault::of_reject(reason.clone())));
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
                                self.answer_waiters(id, &OutboxAnswer::Err(Fault::of_reject(reason.clone())), fx);
                                if let Some(operation) = self.ops.get_mut(&id) { operation.stage = Stage::Done; }
                            } else if let Some(operation) = self.ops.get_mut(&id) {
                                if let Some(previous) = operation.before_cancel.take() { operation.stage = previous; }
                                operation.cancels = None;
                            }
                        }
                        if let Some(operation) = self.ops.get_mut(&op_id) { operation.cancels = None; operation.before_cancel = None; }
                    }
                    self.return_dependents(op_id);
                    for reply in Self::take_waiters(&mut self.discard_waiters, op_id) {
                        Self::reply(fx, reply, OutboxAnswer::Err(Fault::of_reject(reason.clone())));
                    }
                    self.fail_with(op_id, format!("Internal(Invalid): {detail}"), fx);
                    self.drain(now, fx);
                }
            },
        }
    }

    fn stale_families_of(operation: &QueuedOperation) -> Vec<(String, String)> {
        let mut families = operation.stale.clone();
        if !operation.answer_rows_dropped { return families; }
        let effects = &operation.op.effects;
        let models = std::iter::once(&operation.op.model).chain(effects.rows.iter().map(|row| &row.model)).chain(effects.members.iter().map(|row| &row.model)).chain(effects.tombstones.iter().map(|row| &row.model));
        for model in models {
            let family = (model.clone(), String::new());
            if !families.contains(&family) { families.push(family); }
        }
        families
    }

    fn rows_dropped(&self, op_id: OpId) -> bool { self.ops.get(&op_id).is_some_and(|operation| operation.answer_rows_dropped) }

    fn without_rows(&mut self, op_id: OpId, kind: CommitKind, fx: &mut Effects<Outbox>) {
        let Some(operation) = self.ops.get_mut(&op_id) else { return };
        operation.answer_rows_dropped = true;
        if kind == CommitKind::Land {
            operation.landing.clear();
            operation.missing.clear();
            operation.missing_destroy = false;
            operation.result = None;
            operation.landed = None;
        }
        self.store(op_id, kind, fx);
    }

    fn pending_cancelled_intents(&self, created: OpId) -> bool {
        self.batches.values().any(|batch| matches!(batch, Batch::Operation { op_id, kind: CommitKind::Intent } if self.ops.get(op_id).is_some_and(|operation| operation.cancels == Some(created) && *op_id != created)))
    }

    fn finish_cancelled_commit(&mut self, now: u64, created: OpId, fx: &mut Effects<Outbox>) {
        if !self.pending_cancelled_intents(created) {
            if let Some(revision) = self.cancelled_commits.remove(&created) { self.operation_committed(now, created, CommitKind::Rollback, (revision, Vec::new()), fx); }
        }
    }

    pub(super) fn submit_refused(&mut self, op_id: OpId, reason: RejectReason, fx: &mut Effects<Outbox>) {
        self.answer_waiters(op_id, &OutboxAnswer::Err(Fault::of_reject(reason.clone())), fx);
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
