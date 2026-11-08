//! Owner `Transfer`: the steps of a file: the order of starts, `Uploading`, `Completing`, the writes of parts and of `completed`, and `Mutating`.

use super::outbox::{Fault, OutboxOutcome};
use super::outbox_types::Phase;
use super::transfer::{reply, run, Fx, TransferState};
use super::transfer_types::*;
use crate::owners::contract::RejectReason;
use crate::owners::store::StoreOp;
use crate::runtime::{Effect, ScheduleId, Time};
use crate::transport::NetworkPurpose;

/// Attempt ids come from one counter, so the id of a timer of an attempt is never the id of another timer.
fn put_timer(attempt_id: AttemptId) -> ScheduleId { ScheduleId(RETRY_TIMER + attempt_id) }

impl TransferState {
    /// Transfers in the network: only a physical attempt in flight holds a slot.
    pub fn active(&self) -> u32 { self.scan().filter(|transfer| transfer.in_flight).count() as u32 }

    fn operation_of(&self, transfer: &Transfer) -> u64 { transfer.batch_id.and_then(|batch| self.groups.get(&batch)).and_then(|group| group.op_id).or(transfer.op_id).unwrap_or(0) }

    fn progress(transfer: &Transfer) -> u32 { transfer.confirmed_parts * 100 / transfer.source.parts.max(1) }

    fn can_start(&self, transfer: &Transfer) -> bool {
        let opened = !transfer.restored || self.phase == Phase::Ready;
        let allowed = !self.saving || transfer.urgent;
        opened && allowed && transfer.stage == TransferStage::Ready
    }

    fn step_effect(transfer: &Transfer, operation: u64) -> TransferEffect {
        let NextStep::Part(part) = transfer.next_step else { return Self::complete_effect(transfer) };
        TransferEffect::Step { transfer_id: transfer.transfer_id, attempt_id: transfer.attempt_id, key: transfer.key.clone(), operation, part: Some(part), purpose: NetworkPurpose::Transfer, upload_id: transfer.upload_id.clone(), source: transfer.source.clone(), steps: transfer.steps.clone(), place: transfer.place }
    }

    fn complete_effect(transfer: &Transfer) -> TransferEffect {
        let parts = transfer.etags.iter().enumerate().map(|(index, etag)| (index as u32 + 1, etag.clone())).collect();
        TransferEffect::Complete { transfer_id: transfer.transfer_id, attempt_id: transfer.attempt_id, key: transfer.key.clone(), upload_id: transfer.upload_id.clone(), parts, place: transfer.place }
    }

    /// Ready transfers start by `(seq, position)` while a slot is free, the network is up and `saving` allows. A due retry competes in the same order.
    pub(super) fn start_in_order(&mut self, fx: &mut Fx) {
        while self.online == Some(true) && self.active() < self.slots {
            let next = self.scan().filter(|transfer| self.can_start(transfer)).min_by_key(|transfer| transfer.seq).map(|transfer| transfer.transfer_id);
            let Some(id) = next else { return };
            if let Some(abort) = self.transfers.get(&id).filter(|transfer| transfer.attempts >= ATTEMPT_LIMIT).map(|transfer| transfer.confirmed_parts < transfer.source.parts) {
                self.begin_failure(id, "Exhausted".into(), abort, fx);
                continue;
            }
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

    pub(super) fn attempt_committed(&mut self, id: TransferId, fx: &mut Fx) {
        let Some(operation) = self.transfers.get(&id).map(|transfer| self.operation_of(transfer)) else { return };
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        if self.online != Some(true) || (self.saving && !transfer.urgent) {
            transfer.stage = TransferStage::Ready;
            transfer.in_flight = false;
            self.start_in_order(fx);
            return;
        }
        transfer.stage = if transfer.next_step == NextStep::Complete { TransferStage::Completing } else { TransferStage::Uploading };
        let (effect, shown) = (Self::step_effect(transfer, operation), if transfer.stage == TransferStage::Completing { Shown::Uploading { progress: 100 } } else { Shown::Uploading { progress: Self::progress(transfer) } });
        let (uploading, attempt_id) = (transfer.stage == TransferStage::Uploading, transfer.attempt_id);
        self.show(id, shown, fx);
        run(fx, effect);
        if uploading {
            let input = TransferInput::Tick { now: self.now, kind: TransferTick::PutTimeout { transfer_id: id, attempt_id } };
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

    pub(super) fn step_ok(&mut self, id: TransferId, attempt_id: AttemptId, part: Option<u32>, upload_id: Option<String>, etag: Option<String>, fx: &mut Fx) {
        if !self.live(id, TransferStage::Uploading, attempt_id) {
            return;
        }
        self.step_answered(id);
        fx.push(Effect::Cancel(put_timer(attempt_id)));
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        if part.is_some_and(|part| part != transfer.confirmed_parts + 1) {
            return;
        }
        if upload_id.is_some() {
            transfer.upload_id = upload_id;
        }
        transfer.confirmed_parts += 1;
        transfer.attempts = 0;
        transfer.etags.push(etag);
        transfer.last_part = transfer.confirmed_parts >= transfer.source.parts;
        transfer.in_flight = false;
        transfer.stage = TransferStage::PersistingPart;
        self.store(&[id], StoreKind::ConfirmedParts, fx);
        self.start_in_order(fx);
    }

    /// A permanent error or the last attempt: the failure is written, `abort` goes out when a multipart upload exists.
    pub(super) fn mark_failure(&mut self, id: TransferId, error: String, fx: &mut Fx) {
        self.release_step(id, fx);
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        transfer.stage = TransferStage::PersistingFailure;
        transfer.removal = None;
        transfer.in_flight = false;
        transfer.waiting_retry = false;
        transfer.error = Some(error);
    }

    pub(super) fn begin_failure(&mut self, id: TransferId, error: String, abort: bool, fx: &mut Fx) {
        self.mark_failure(id, error, fx);
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
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

    pub(super) fn step_error(&mut self, id: TransferId, attempt_id: AttemptId, (temporary, error): (bool, String), fx: &mut Fx) {
        if !self.live(id, TransferStage::Uploading, attempt_id) {
            return;
        }
        self.step_answered(id);
        fx.push(Effect::Cancel(put_timer(attempt_id)));
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        if temporary && transfer.attempts < ATTEMPT_LIMIT {
            self.schedule_retry(id, fx);
        } else {
            self.begin_failure(id, error, true, fx);
        }
        self.start_in_order(fx);
    }

    pub(super) fn completed(&mut self, id: TransferId, attempt_id: AttemptId, signed_id: Option<String>, fx: &mut Fx) {
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
        self.start_in_order(fx);
    }

    pub(super) fn complete_error(&mut self, id: TransferId, attempt_id: AttemptId, (temporary, error): (bool, String), fx: &mut Fx) {
        if !self.live(id, TransferStage::Completing, attempt_id) {
            return;
        }
        self.step_answered(id);
        if temporary && self.transfers.get(&id).is_some_and(|transfer| transfer.attempts < ATTEMPT_LIMIT) {
            self.schedule_retry(id, fx);
        } else {
            self.begin_failure(id, error, false, fx);
        }
        self.start_in_order(fx);
    }

    pub(super) fn tick(&mut self, now: Time, kind: TransferTick, fx: &mut Fx) {
        self.now = self.now.max(now);
        match kind {
            TransferTick::Expire => self.expire(fx),
            TransferTick::Retry { transfer_id, attempt_id } => self.tick_retry(transfer_id, attempt_id, fx),
            TransferTick::Minted { upload_id, attempt_id } => self.tick_minted(&upload_id, attempt_id, fx),
            TransferTick::PutTimeout { transfer_id, attempt_id } => self.tick_put_timeout(transfer_id, attempt_id, fx),
        }
    }

    /// `MintedPending x Tick`: the saved record goes to the Store again while its transfer still holds the id.
    fn tick_minted(&mut self, upload_id: &str, attempt_id: AttemptId, fx: &mut Fx) {
        let Some(debt) = self.minted.get(upload_id).filter(|debt| debt.attempt_id == attempt_id).cloned() else { return };
        if self.ended { return; }
        match debt.transfer {
            Some(id) => {
                let holds = self.transfers.get(&id).is_some_and(|transfer| transfer.upload_id.as_deref() == Some(upload_id) && !matches!(transfer.stage, TransferStage::Removing | TransferStage::Done));
                if holds { self.store(&[id], StoreKind::UploadId, fx); } else { self.minted.remove(upload_id); }
            }
            None => {
                let batch = self.commit_ops(vec![StoreOp::UploadOrphan { upload_id: upload_id.to_string(), key: debt.key.clone() }], fx);
                if let Some(pending) = batch.and_then(|batch| self.pending.get_mut(&batch)) {
                    pending.kind = StoreKind::LateOrphan;
                    pending.orphan = Some((upload_id.to_string(), debt.key));
                }
            }
        }
    }

    fn tick_put_timeout(&mut self, id: TransferId, attempt_id: AttemptId, fx: &mut Fx) {
        if !self.live(id, TransferStage::Uploading, attempt_id) {
            return;
        }
        run(fx, TransferEffect::CancelStep { transfer_id: id });
        self.step_error(id, attempt_id, (true, super::failure::of_transport(&crate::transport::TransportError::Network("the upload timed out".into()))), fx);
    }

    fn tick_retry(&mut self, id: TransferId, attempt_id: AttemptId, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get(&id) else { return };
        if transfer.attempt_id != attempt_id {
            return;
        }
        match transfer.stage {
            TransferStage::Uploading | TransferStage::Completing if transfer.waiting_retry => {
                let Some(transfer) = self.transfers.get_mut(&id) else { return };
                transfer.stage = TransferStage::Ready;
                transfer.waiting_retry = false;
                self.start_in_order(fx);
            }
            TransferStage::PersistingFailure | TransferStage::Removing if self.group_entry(id).is_some_and(|group| self.phase_of(group) == Some(BatchPhase::Mutating)) => {
                let ids = self.group_entry(id).map(|group| self.groups[&group].ids.iter().copied().filter(|member| matches!(self.stage_of(*member), Some(TransferStage::Removing | TransferStage::PersistingFailure))).collect::<Vec<_>>()).unwrap_or_default();
                self.store(&ids, StoreKind::MixedOutcomes, fx);
            }
            TransferStage::PersistingFailure => self.store(&[id], StoreKind::Failed, fx),
            TransferStage::PersistingBatchIntent => {
                let ids = self.group_entry(id).map(|group| self.remaining(group).into_iter().filter(|member| self.stage_of(*member) == Some(TransferStage::PersistingBatchIntent)).collect::<Vec<_>>()).unwrap_or_else(|| vec![id]);
                self.store(&ids, StoreKind::BatchIntent, fx);
            }
            _ => {}
        }
    }

    pub(super) fn part_committed(&mut self, id: TransferId, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        transfer.stage = TransferStage::Ready;
        transfer.next_step = if transfer.last_part { NextStep::Complete } else { NextStep::Part(transfer.confirmed_parts + 1) };
        let progress = if transfer.last_part { 100 } else { Self::progress(transfer) };
        self.show(id, Shown::Uploading { progress }, fx);
        self.start_in_order(fx);
    }

    pub(super) fn complete_committed(&mut self, id: TransferId, fx: &mut Fx) {
        if let Some(group) = self.group_entry(id) {
            self.batch_ready(id, group, fx);
            return self.start_in_order(fx);
        }
        self.set(id, TransferStage::Mutating);
        if let Some(transfer) = self.transfers.get_mut(&id) { transfer.intent_live = true; }
        self.show(id, Shown::Uploading { progress: 100 }, fx);
        self.submit_mutation(id, fx);
        self.start_in_order(fx);
    }

    /// `Rejected(Storage)` of a write of a part or of `completed`: durable `Failed` only by a write.
    pub(super) fn persist_failed(&mut self, id: TransferId, reason: RejectReason, fx: &mut Fx) {
        let error = match reason {
            RejectReason::Storage(error) => error,
            _ => "storage".into(),
        };
        self.begin_failure(id, error, false, fx);
    }

    /// `Rejected(Invalid)`: the write is wrong, no retry helps. `Failed` is written; the write of `Failed` that is invalid stays in memory.
    pub(super) fn invalid_write(&mut self, ids: &[TransferId], marking: bool, fx: &mut Fx) {
        for &id in ids {
            if marking {
                self.failure_committed(id, fx);
            } else {
                self.begin_failure(id, "Internal(Invalid)".into(), false, fx);
            }
        }
    }

    fn waits_for_discard(&self, id: TransferId) -> bool {
        self.transfers.get(&id).is_some_and(|transfer| transfer.stage == TransferStage::Mutating || (transfer.stage == TransferStage::Failed && transfer.discard_sent))
    }

    pub(super) fn mutation_outcome(&mut self, id: TransferId, outcome: OutboxOutcome, fx: &mut Fx) {
        if !self.waits_for_discard(id) { return; }
        let waiting = self.transfers.get(&id).is_some_and(|transfer| !transfer.discard_waiters.is_empty() || transfer.cancel_reply.is_some());
        let next_single = (!matches!(outcome, OutboxOutcome::Interrupted) && !(waiting && matches!(outcome, OutboxOutcome::Failed { .. }))).then(|| self.group_entry(id)).flatten().filter(|group| self.phase_of(*group) == Some(BatchPhase::Singles));
        match outcome {
            OutboxOutcome::Confirmed { revision, result, row_id } => {
                if let Some(transfer) = self.transfers.get_mut(&id) {
                    let cancelled = transfer.cancel_reply.take().map(|(waiter, _)| waiter);
                    for waiter in std::mem::take(&mut transfer.discard_waiters).into_iter().chain(cancelled) {
                        reply(fx, waiter, TransferAnswer::Err(Fault::NotFound));
                    }
                }
                self.remove_with_batch(id, Removal::Confirmed { revision, result, row_id }, fx)
            }
            OutboxOutcome::Discarded => self.remove_with_batch(id, Removal::Discarded, fx),
            OutboxOutcome::Rejected { .. } if waiting => self.remove_with_batch(id, Removal::Discarded, fx),
            OutboxOutcome::Failed { .. } if waiting => {}
            OutboxOutcome::Rejected { error } => {
                if let Some(transfer) = self.transfers.get_mut(&id) { transfer.intent_live = false; }
                self.begin_failure(id, error, false, fx);
            }
            OutboxOutcome::Failed { error } => self.begin_failure(id, error, false, fx),
            OutboxOutcome::Interrupted => {
                self.interrupt(id, fx);
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
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        if result.is_ok() {
            return;
        }
        transfer.discard_sent = false;
        let cancelled = transfer.cancel_reply.take().map(|(waiter, _)| waiter);
        for waiter in std::mem::take(&mut transfer.discard_waiters).into_iter().chain(cancelled) {
            reply(fx, waiter, TransferAnswer::Err(Fault::InFlight));
        }
    }
}
