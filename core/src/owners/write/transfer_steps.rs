//! Owner `Transfer`: the steps of a file: the order of starts, `Uploading`, `Completing`, the writes of parts and of `completed`, and `Mutating`.

use super::outbox::{Fault, OutboxOutcome};
use super::outbox_types::Phase;
use super::transfer::{reply, run, Fx, TransferState};
use super::transfer_types::*;
use crate::owners::contract::RejectReason;
use crate::owners::store::StoreOp;
use crate::runtime::{Effect, ScheduleId};
use crate::transport::NetworkPurpose;

/// Attempt ids come from one counter, so the id of a timer of an attempt is never the id of another timer.
fn put_timer(attempt_id: AttemptId) -> ScheduleId { ScheduleId(RETRY_TIMER + attempt_id) }

impl TransferState {
    /// Transfers in the network: only a physical attempt in flight holds a slot.
    pub fn active(&self) -> u32 { self.scan().filter(|transfer| transfer.in_flight).count() as u32 }

    fn operation_of(&self, transfer: &Transfer) -> u64 { transfer.batch_id.and_then(|batch| self.groups.get(&batch)).and_then(|group| group.op_id).or(transfer.op_id).unwrap_or(0) }

    fn progress(transfer: &Transfer) -> Option<u32> { transfer.source.as_ref().filter(|source| source.parts > 0).map(|source| transfer.confirmed_parts * 100 / source.parts) }

    fn can_start(&self, transfer: &Transfer) -> bool {
        let opened = !transfer.restored || self.phase == Phase::Ready;
        let allowed = !self.saving || transfer.urgent;
        opened && allowed && transfer.stage == TransferStage::Ready && (transfer.pending_step.is_none() || transfer.attempts >= ATTEMPT_LIMIT)
    }

    fn step_effect(transfer: &Transfer, operation: u64) -> Option<TransferEffect> {
        let NextStep::Part(part) = transfer.next_step else { return Some(Self::complete_effect(transfer)) };
        Some(TransferEffect::Step { transfer_id: transfer.transfer_id, attempt_id: transfer.attempt_id, cycle_id: transfer.cycle_id, key: transfer.key.clone(), operation, part: Some(part), purpose: NetworkPurpose::Transfer, upload_id: transfer.upload_id.clone(), source: transfer.source.clone()?, steps: transfer.steps.clone()?, place: transfer.place, seeded: transfer.seeded.clone() })
    }

    fn complete_effect(transfer: &Transfer) -> TransferEffect {
        let parts = transfer.etags.iter().enumerate().map(|(index, etag)| (index as u32 + 1, etag.clone())).collect();
        TransferEffect::Complete { transfer_id: transfer.transfer_id, attempt_id: transfer.attempt_id, cycle_id: transfer.cycle_id, key: transfer.key.clone(), upload_id: transfer.upload_id.clone(), parts, place: transfer.place, seeded: transfer.seeded.clone() }
    }

    /// Ready transfers start by `(seq, position)` while a slot is free, the network is up and `saving` allows. A due retry competes in the same order.
    pub(super) fn start_in_order(&mut self, now: crate::runtime::Time, fx: &mut Fx) {
        if self.starts_held { return; }
        while self.online == Some(true) {
            let next = self.scan().filter(|transfer| self.can_start(transfer)).min_by_key(|transfer| transfer.seq).map(|transfer| transfer.transfer_id);
            let Some(id) = next else { return };
            if let Some(abort) = self.transfers.get(&id).filter(|transfer| transfer.attempts >= ATTEMPT_LIMIT).map(|transfer| transfer.source.as_ref().is_some_and(|source| transfer.confirmed_parts < source.parts)) {
                self.begin_failure(now, id, "Exhausted".into(), abort, fx);
                continue;
            }
            if self.active() >= self.slots { return; }
            let attempt_id = self.attempt();
            let Some(transfer) = self.transfers.get_mut(&id) else { return };
            transfer.attempt_id = attempt_id;
            transfer.attempts += 1;
            transfer.stage = TransferStage::PersistingAttempt;
            transfer.in_flight = true;
            transfer.waiting_retry = false;
            self.store(&[id], StoreKind::SyncUpload, fx);
        }
    }

    pub(super) fn attempt_committed(&mut self, now: crate::runtime::Time, id: TransferId, fx: &mut Fx) {
        let Some(operation) = self.transfers.get(&id).map(|transfer| self.operation_of(transfer)) else { return };
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        if self.online != Some(true) || (self.saving && !transfer.urgent) {
            transfer.stage = TransferStage::Ready;
            transfer.in_flight = false;
            self.start_in_order(now, fx);
            return;
        }
        transfer.stage = if transfer.next_step == NextStep::Complete { TransferStage::Completing } else { TransferStage::Uploading };
        let Some(effect) = Self::step_effect(transfer, operation) else { return self.begin_failure(now, id, "Internal(InvalidSource)".into(), false, fx) };
        let Some(progress) = (if transfer.stage == TransferStage::Completing { Some(100) } else { Self::progress(transfer) }) else { return self.begin_failure(now, id, "Internal(InvalidSource)".into(), false, fx) };
        let shown = Shown::Uploading { progress };
        let (uploading, attempt_id) = (transfer.stage == TransferStage::Uploading, transfer.attempt_id);
        self.show(id, shown, fx);
        run(fx, effect);
        if uploading {
            let input = TransferInput::Tick { kind: TransferTick::PutTimeout { transfer_id: id, attempt_id } };
            fx.push(Effect::Schedule { id: put_timer(attempt_id), after: self.put_timeout, input });
        }
    }

    /// `Online(false)` and `PowerSaving(true)` of a transfer that is not urgent: the steps in the network go back to `Ready`.
    pub(super) fn leave_network(&mut self, fx: &mut Fx) {
        let blocked = self.online != Some(true);
        let ids: Vec<TransferId> = self.scan().filter(|transfer| matches!(transfer.stage, TransferStage::Uploading | TransferStage::Completing) && (blocked || (self.saving && !transfer.urgent))).map(|transfer| transfer.transfer_id).collect();
        for id in ids {
            let Some(transfer) = self.transfers.get_mut(&id) else { continue };
            transfer.next_step = if transfer.stage == TransferStage::Completing { NextStep::Complete } else { NextStep::Part(transfer.confirmed_parts + 1) };
            transfer.stage = TransferStage::Ready;
            transfer.waiting_retry = false;
            let attempt_id = transfer.attempt_id;
            if std::mem::replace(&mut transfer.in_flight, false) {
                run(fx, TransferEffect::CancelStep { transfer_id: id });
                fx.push(Effect::Cancel(put_timer(attempt_id)));
            }
            self.show(id, Shown::Queued, fx);
        }
    }

    fn live(&self, id: TransferId, stage: TransferStage, attempt_id: AttemptId) -> bool { self.transfers.get(&id).is_some_and(|transfer| transfer.stage == stage && transfer.in_flight && transfer.attempt_id == attempt_id) }

    pub(super) fn step_ok(&mut self, now: crate::runtime::Time, id: TransferId, attempt_id: AttemptId, part: Option<u32>, upload_id: Option<String>, etag: Option<String>, fx: &mut Fx) {
        if !self.live(id, TransferStage::Uploading, attempt_id) {
            return;
        }
        self.step_answered(id);
        fx.push(Effect::Cancel(put_timer(attempt_id)));
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        if part.is_some_and(|part| part != transfer.confirmed_parts + 1) {
            return;
        }
        transfer.blob_created_at.get_or_insert(now);
        if upload_id.is_some() {
            transfer.upload_id = upload_id;
        }
        transfer.confirmed_parts += 1;
        transfer.attempts = 0;
        transfer.etags.push(etag);
        transfer.last_part = transfer.source.as_ref().is_some_and(|source| transfer.confirmed_parts >= source.parts);
        transfer.in_flight = false;
        transfer.stage = TransferStage::PersistingPart;
        self.store(&[id], StoreKind::ConfirmedParts, fx);
        self.start_in_order(now, fx);
    }

    /// A permanent error or the last attempt: the failure is written, `abort` goes out when a multipart upload exists.
    pub(super) fn mark_failure(&mut self, now: crate::runtime::Time, id: TransferId, error: String, fx: &mut Fx) {
        self.release_step(id, fx);
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        transfer.failed_at = Some(now);
        transfer.stage = TransferStage::PersistingFailure;
        transfer.removal = None;
        transfer.in_flight = false;
        transfer.waiting_retry = false;
        transfer.error = Some(error);
    }

    pub(super) fn begin_failure(&mut self, now: crate::runtime::Time, id: TransferId, error: String, abort: bool, fx: &mut Fx) {
        self.begin_failure_at(now, id, error, abort, None, fx);
    }

    pub(super) fn begin_failure_at(&mut self, now: crate::runtime::Time, id: TransferId, error: String, abort: bool, at: Option<crate::runtime::Time>, fx: &mut Fx) {
        self.mark_failure(now, id, error, fx);
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        if let Some(at) = at { transfer.failed_at = Some(at); }
        transfer.failure_abort = abort;
        self.store(&[id], StoreKind::Failed, fx);
    }

    /// The multipart upload exists on the server whatever became of the attempt that created it.
    pub(super) fn upload_minted(&mut self, id: TransferId, upload_id: String, key: String, fx: &mut Fx) {
        let owned = self.transfers.get(&id).map(|transfer| (transfer.upload_id.is_some(), transfer.stage));
        let known = self.orphans.contains_key(&upload_id) || self.minted.contains_key(&upload_id) || self.pending.values().any(|pending| pending.orphan.as_ref().is_some_and(|(pending_id, _)| *pending_id == upload_id));
        match owned {
            Some((true, _)) => {}
            _ if known => {}
            Some((false, stage)) if stage != TransferStage::Removing && stage != TransferStage::Done => {
                if let Some(transfer) = self.transfers.get_mut(&id) {
                    transfer.upload_id = Some(upload_id);
                }
                self.store(&[id], StoreKind::UploadId, fx);
            }
            _ => {
                let batch = self.commit_ops(vec![StoreOp::UploadOrphan { upload_id: upload_id.clone(), key: key.clone() }], fx);
                if let Some(pending) = batch.and_then(|batch| self.pending.get_mut(&batch)) {
                    pending.kind = StoreKind::LateOrphan;
                    pending.orphan = Some((upload_id, key));
                }
            }
        }
    }

    pub(super) fn step_error(&mut self, now: crate::runtime::Time, id: TransferId, attempt_id: AttemptId, (temporary, error): (bool, String), fx: &mut Fx) {
        if !self.live(id, TransferStage::Uploading, attempt_id) {
            return;
        }
        self.step_answered(id);
        fx.push(Effect::Cancel(put_timer(attempt_id)));
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        if temporary && transfer.attempts < ATTEMPT_LIMIT {
            self.schedule_retry(id, fx);
        } else {
            self.begin_failure(now, id, error, true, fx);
        }
        self.start_in_order(now, fx);
    }

    pub(super) fn completed(&mut self, now: crate::runtime::Time, id: TransferId, attempt_id: AttemptId, signed_id: Option<String>, fx: &mut Fx) {
        if !self.live(id, TransferStage::Completing, attempt_id) {
            return;
        }
        self.step_answered(id);
        if let Some(transfer) = self.transfers.get_mut(&id) {
            transfer.signed_id = signed_id;
            transfer.in_flight = false;
            transfer.stage = TransferStage::PersistingComplete;
        }
        self.store(&[id], StoreKind::Completed, fx);
        self.start_in_order(now, fx);
    }

    pub(super) fn complete_error(&mut self, now: crate::runtime::Time, id: TransferId, attempt_id: AttemptId, (temporary, error): (bool, String), fx: &mut Fx) {
        if !self.live(id, TransferStage::Completing, attempt_id) {
            return;
        }
        self.step_answered(id);
        if temporary && self.transfers.get(&id).is_some_and(|transfer| transfer.attempts < ATTEMPT_LIMIT) {
            self.schedule_retry(id, fx);
        } else {
            self.begin_failure(now, id, error, false, fx);
        }
        self.start_in_order(now, fx);
    }

    pub(super) fn tick(&mut self, now: crate::runtime::Time, kind: TransferTick, fx: &mut Fx) {
        match kind {
            TransferTick::Expire => self.expire(now, fx),
            TransferTick::Retry { transfer_id, attempt_id } => self.tick_retry(now, transfer_id, attempt_id, fx),
            TransferTick::Minted { upload_id, attempt_id } => self.tick_minted(&upload_id, attempt_id, fx),
            TransferTick::StagedRestore { transfer_id } => { if self.transfers.get(&transfer_id).is_some_and(|transfer| transfer.staged_failed.is_some()) { self.write_restored(transfer_id, fx); } }
            TransferTick::PutTimeout { transfer_id, attempt_id } => self.tick_put_timeout(now, transfer_id, attempt_id, fx),
            TransferTick::Shown { transfer_id } => self.tick_shown(transfer_id, fx),
        }
    }

    /// `MintedPending x Tick`: the debt of a minted id is written again. It goes under `Control` when its transfer is gone or the epoch ended, as a row of the live transfer otherwise.
    fn tick_minted(&mut self, upload_id: &str, attempt_id: AttemptId, fx: &mut Fx) {
        let Some(debt) = self.minted.get(upload_id).filter(|debt| debt.attempt_id == attempt_id).cloned() else { return };
        if !self.debt_live(upload_id, debt.transfer) {
            self.minted.remove(upload_id);
            return;
        }
        let holds = debt.transfer.and_then(|id| self.transfers.get(&id)).is_some_and(|transfer| transfer.upload_id.as_deref() == Some(upload_id) && !matches!(transfer.stage, TransferStage::Removing | TransferStage::Done));
        match (debt.transfer, holds && !self.ended) {
            (Some(id), true) => self.store(&[id], StoreKind::UploadId, fx),
            (Some(_), false) if !self.ended => { self.minted.remove(upload_id); }
            _ => {
                if !self.pending.values().any(|pending| pending.kind == StoreKind::ControlOrphan && pending.orphan.as_ref().is_some_and(|(written, _)| written == upload_id)) {
                    self.write_control_orphan(upload_id.to_string(), debt.key, fx);
                }
            }
        }
    }

    fn tick_put_timeout(&mut self, now: crate::runtime::Time, id: TransferId, attempt_id: AttemptId, fx: &mut Fx) {
        if !self.live(id, TransferStage::Uploading, attempt_id) {
            return;
        }
        run(fx, TransferEffect::CancelStep { transfer_id: id });
        self.step_error(now, id, attempt_id, (true, super::failure::of_transport(&crate::transport::TransportError::Network("the upload timed out".into()))), fx);
    }

    fn tick_shown(&mut self, id: TransferId, fx: &mut Fx) {
        let Some(shown) = self.transfers.get(&id).and_then(|transfer| transfer.last_shown.clone().filter(|shown| Self::unwritten(transfer, shown))) else { return };
        self.show(id, shown, fx);
    }

    fn tick_retry(&mut self, now: crate::runtime::Time, id: TransferId, attempt_id: AttemptId, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get(&id) else { return };
        if transfer.attempt_id != attempt_id {
            return;
        }
        match transfer.stage {
            TransferStage::Uploading | TransferStage::Completing if transfer.waiting_retry => {
                let Some(transfer) = self.transfers.get_mut(&id) else { return };
                transfer.stage = TransferStage::Ready;
                transfer.waiting_retry = false;
                self.start_in_order(now, fx);
            }
            TransferStage::PersistingFailure | TransferStage::Removing if self.group_entry(id).is_some_and(|group| self.phase_of(group) == Some(BatchPhase::Mutating)) => {
                let ids = self.group_entry(id).map(|group| self.groups[&group].ids.iter().copied().filter(|member| matches!(self.stage_of(*member), Some(TransferStage::Removing | TransferStage::PersistingFailure))).collect::<Vec<_>>()).unwrap_or_default();
                self.store(&ids, StoreKind::MixedOutcomes, fx);
            }
            TransferStage::PersistingFailure => self.store(&[id], StoreKind::Failed, fx),
            TransferStage::PersistingAttempt => self.store(&[id], StoreKind::SyncUpload, fx),
            TransferStage::PersistingPart => self.store(&[id], StoreKind::ConfirmedParts, fx),
            TransferStage::PersistingComplete => self.store(&[id], StoreKind::Completed, fx),
            TransferStage::Removing => {
                let ids = self.group_entry(id).map(|group| self.groups[&group].ids.iter().copied().filter(|member| self.stage_of(*member) == Some(TransferStage::Removing)).collect::<Vec<_>>()).unwrap_or_else(|| vec![id]);
                self.store(&ids, StoreKind::Removed, fx);
            }
            TransferStage::PersistingBatchIntent => {
                let ids = self.group_entry(id).map(|group| self.remaining(group).into_iter().filter(|member| self.stage_of(*member) == Some(TransferStage::PersistingBatchIntent)).collect::<Vec<_>>()).unwrap_or_else(|| vec![id]);
                self.store(&ids, StoreKind::BatchIntent, fx);
            }
            _ => {}
        }
    }

    pub(super) fn part_committed(&mut self, now: crate::runtime::Time, id: TransferId, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        transfer.stage = TransferStage::Ready;
        transfer.next_step = if transfer.last_part { NextStep::Complete } else { NextStep::Part(transfer.confirmed_parts + 1) };
        let Some(progress) = (if transfer.last_part { Some(100) } else { Self::progress(transfer) }) else { return self.begin_failure(now, id, "Internal(InvalidSource)".into(), false, fx) };
        self.show(id, Shown::Uploading { progress }, fx);
        self.start_in_order(now, fx);
    }

    pub(super) fn complete_committed(&mut self, now: crate::runtime::Time, id: TransferId, fx: &mut Fx) {
        if let Some(group) = self.group_entry(id) {
            self.batch_ready(id, group, fx);
            return self.start_in_order(now, fx);
        }
        self.set(id, TransferStage::Mutating);
        if let Some(transfer) = self.transfers.get_mut(&id) { transfer.intent_live = true; transfer.has_operation = true; }
        self.show(id, Shown::Uploading { progress: 100 }, fx);
        self.submit_mutation(id, fx);
        self.start_in_order(now, fx);
    }

    /// `Rejected(Invalid)`: the write is wrong, no retry helps. `Failed` is written; the write of `Failed` that is invalid stays in memory.
    pub(super) fn invalid_write(&mut self, now: crate::runtime::Time, ids: &[TransferId], marking: bool, detail: &str, fx: &mut Fx) {
        for &id in ids {
            if marking {
                self.failure_committed(id, fx);
            } else {
                self.begin_failure(now, id, format!("Internal(Invalid): {detail}"), false, fx);
            }
        }
    }

    fn waits_for_discard(&self, id: TransferId) -> bool {
        self.transfers.get(&id).is_some_and(|transfer| transfer.stage == TransferStage::Mutating || (transfer.stage == TransferStage::Failed && transfer.discard_sent))
    }

    pub(super) fn mutation_outcome(&mut self, now: crate::runtime::Time, id: TransferId, (outcome, row_never_created): (OutboxOutcome, bool), fx: &mut Fx) {
        if !self.waits_for_discard(id) { return; }
        let waiting = self.transfers.get(&id).is_some_and(|transfer| !transfer.discard_waiters.is_empty() || transfer.cancel_reply.is_some());
        let failed_waiting = waiting && self.stage_of(id) == Some(TransferStage::Failed);
        let next_single = (!matches!(outcome, OutboxOutcome::Ended(_)) && !(failed_waiting && !matches!(outcome, OutboxOutcome::Confirmed { .. } | OutboxOutcome::Discarded))).then(|| self.group_entry(id)).flatten().filter(|group| self.phase_of(*group) == Some(BatchPhase::Singles));
        match outcome {
            OutboxOutcome::Confirmed { revision, result, row_id } => {
                if let Some(transfer) = self.transfers.get_mut(&id) {
                    if let Some((waiter, _)) = transfer.cancel_reply.take() { reply(fx, waiter, TransferAnswer::Cancelled(false)); }
                    for waiter in std::mem::take(&mut transfer.discard_waiters) {
                        reply(fx, waiter, TransferAnswer::Err(Fault::NotFound));
                    }
                }
                self.remove_with_batch(id, Removal::Confirmed { revision, result, row_id }, fx)
            }
            OutboxOutcome::Discarded => self.remove_with_batch(id, Removal::Discarded, fx),
            OutboxOutcome::Rejected { error } if row_never_created => {
                let cycle = self.transfers.get(&id).map_or(0, |transfer| transfer.cycle_id);
                if let Some(transfer) = self.transfers.get_mut(&id) { transfer.intent_live = false; transfer.has_operation = false; }
                self.confirmed_discards.insert(id, (cycle, error));
                self.remove_with_batch(id, Removal::Discarded, fx)
            }
            OutboxOutcome::Rejected { .. } if waiting && !failed_waiting => self.remove_with_batch(id, Removal::Discarded, fx),
            OutboxOutcome::Failed { .. } if waiting && !failed_waiting => {}
            _ if failed_waiting => {}
            OutboxOutcome::Rejected { error } => {
                if let Some(transfer) = self.transfers.get_mut(&id) { transfer.intent_live = false; transfer.has_operation = false; }
                self.begin_failure(now, id, error, false, fx);
            }
            OutboxOutcome::Failed { error } => self.begin_failure(now, id, error, false, fx),
            OutboxOutcome::Ended(cause) => {
                self.interrupt(id, cause, fx);
                self.rounds_idle(id, fx);
            }
        }
        if let Some(group) = next_single {
            self.single_next(group, fx);
        }
    }

    fn remove_with_batch(&mut self, id: TransferId, removal: Removal, fx: &mut Fx) {
        self.remove_with(id, removal, fx);
        self.store(&[id], StoreKind::Removed, fx);
    }

    pub(super) fn discard_outcome(&mut self, key: &str, result: Result<(), Fault>, fx: &mut Fx) {
        let Some(id) = self.open_id(key).filter(|id| self.waits_for_discard(*id)) else { return };
        if let (Err(Fault::Rejected(reason)), Some((_, error))) = (&result, self.confirmed_discards.get(&id).cloned()) {
            match reason {
                RejectReason::Storage(detail) => self.answer_discarded_outcome(id, TransferAnswer::Err(Fault::Rejected(RejectReason::Storage(format!("{detail}: {error}")))), fx),
                RejectReason::NoSession | RejectReason::Closed | RejectReason::StaleGeneration { .. } | RejectReason::Invalid(_) => {}
            }
        }
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        let Err(fault) = result else { return };
        transfer.discard_sent = false;
        let cancelled = transfer.cancel_reply.take().map(|(waiter, _)| waiter);
        for waiter in std::mem::take(&mut transfer.discard_waiters).into_iter().chain(cancelled) {
            reply(fx, waiter, TransferAnswer::Err(fault.clone()));
        }
    }
}
