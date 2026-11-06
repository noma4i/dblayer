//! Owner `Transfer`: the steps of a file: the order of starts, `Uploading`, `Completing`, the writes of parts and of `completed`, and `Mutating`.

use super::outbox::{Fault, OutboxOutcome};
use super::outbox_types::Phase;
use super::transfer::{reply, run, Fx, TransferState};
use super::transfer_types::*;
use crate::owners::contract::RejectReason;
use crate::runtime::Time;
use crate::transport::NetworkPurpose;

impl TransferState {
    /// Transfers in the network: only a physical attempt in flight holds a slot.
    pub fn active(&self) -> u32 { self.transfers.values().filter(|transfer| transfer.in_flight).count() as u32 }

    fn progress(transfer: &Transfer) -> u32 { transfer.confirmed_parts * 100 / transfer.source.parts.max(1) }

    fn can_start(&self, transfer: &Transfer) -> bool {
        let opened = !transfer.restored || self.phase == Phase::Ready;
        let allowed = !self.saving || transfer.urgent;
        let due = matches!(transfer.stage, TransferStage::Uploading | TransferStage::Completing) && transfer.waiting_retry && transfer.retry_due;
        opened && allowed && (transfer.stage == TransferStage::Ready || due)
    }

    fn step_effect(transfer: &Transfer) -> TransferEffect {
        let NextStep::Part(part) = transfer.next_step else { return Self::complete_effect(transfer) };
        TransferEffect::Step { transfer_id: transfer.transfer_id, attempt_id: transfer.attempt_id, key: transfer.key.clone(), part: Some(part), purpose: NetworkPurpose::Transfer, upload_id: transfer.upload_id.clone(), source: transfer.source.clone(), steps: transfer.steps.clone(), place: transfer.place }
    }

    fn complete_effect(transfer: &Transfer) -> TransferEffect {
        let parts = transfer.etags.iter().enumerate().map(|(index, etag)| (index as u32 + 1, etag.clone())).collect();
        TransferEffect::Complete { transfer_id: transfer.transfer_id, attempt_id: transfer.attempt_id, key: transfer.key.clone(), upload_id: transfer.upload_id.clone(), parts, place: transfer.place }
    }

    /// Ready transfers start by `(seq, position)` while a slot is free, the network is up and `saving` allows. A due retry competes in the same order.
    pub(super) fn start_in_order(&mut self, fx: &mut Fx) {
        while self.online == Some(true) && self.active() < self.slots {
            let next = self.transfers.values().filter(|transfer| self.can_start(transfer)).min_by_key(|transfer| transfer.seq).map(|transfer| transfer.transfer_id);
            let Some(id) = next else { return };
            let attempt_id = self.attempt();
            let Some(transfer) = self.transfers.get_mut(&id) else { return };
            if transfer.stage == TransferStage::Ready {
                transfer.attempt_id = attempt_id;
                transfer.attempts = 0;
                transfer.stage = if transfer.next_step == NextStep::Complete { TransferStage::Completing } else { TransferStage::Uploading };
            }
            transfer.in_flight = true;
            transfer.waiting_retry = false;
            transfer.retry_due = false;
            let (effect, shown) = (Self::step_effect(transfer), if transfer.stage == TransferStage::Completing { Shown::Uploading { progress: 100 } } else { Shown::Uploading { progress: Self::progress(transfer) } });
            self.show(id, shown, fx);
            run(fx, effect);
        }
    }

    /// `Online(false)` and `PowerSaving(true)` of a transfer that is not urgent: the steps in the network go back to `Ready`.
    pub(super) fn leave_network(&mut self, fx: &mut Fx) {
        let blocked = self.online != Some(true);
        let ids: Vec<TransferId> = self.transfers.values().filter(|transfer| matches!(transfer.stage, TransferStage::Uploading | TransferStage::Completing) && (blocked || (self.saving && !transfer.urgent))).map(|transfer| transfer.transfer_id).collect();
        for id in ids {
            let Some(transfer) = self.transfers.get_mut(&id) else { continue };
            transfer.next_step = if transfer.stage == TransferStage::Completing { NextStep::Complete } else { NextStep::Part(transfer.confirmed_parts + 1) };
            transfer.stage = TransferStage::Ready;
            transfer.waiting_retry = false;
            transfer.retry_due = false;
            if std::mem::replace(&mut transfer.in_flight, false) {
                run(fx, TransferEffect::CancelStep { transfer_id: id });
            }
            self.show(id, Shown::Queued, fx);
        }
    }

    fn live(&self, id: TransferId, stage: TransferStage, attempt_id: AttemptId) -> bool { self.transfers.get(&id).is_some_and(|transfer| transfer.stage == stage && transfer.in_flight && transfer.attempt_id == attempt_id) }

    pub(super) fn step_ok(&mut self, id: TransferId, attempt_id: AttemptId, part: Option<u32>, upload_id: Option<String>, etag: Option<String>, fx: &mut Fx) {
        if !self.live(id, TransferStage::Uploading, attempt_id) {
            return;
        }
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        if part.is_some_and(|part| part != transfer.confirmed_parts + 1) {
            return;
        }
        if upload_id.is_some() {
            transfer.upload_id = upload_id;
        }
        transfer.confirmed_parts += 1;
        transfer.etags.push(etag);
        transfer.last_part = transfer.confirmed_parts >= transfer.source.parts;
        transfer.in_flight = false;
        transfer.stage = TransferStage::PersistingPart;
        self.store(&[id], StoreKind::ConfirmedParts, fx);
        self.start_in_order(fx);
    }

    /// A permanent error or the last attempt: the failure is written, `abort` goes out when a multipart upload exists.
    pub(super) fn begin_failure(&mut self, id: TransferId, error: String, abort: bool, fx: &mut Fx) {
        let attempt_id = self.attempt();
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        transfer.stage = TransferStage::PersistingFailure;
        transfer.removal = None;
        transfer.in_flight = false;
        transfer.waiting_retry = false;
        transfer.error = Some(error);
        transfer.failure_abort = abort;
        if let (true, Some(upload_id)) = (abort, transfer.upload_id.clone()) {
            transfer.aborted = true;
            run(fx, TransferEffect::Abort { transfer_id: id, attempt_id, key: transfer.key.clone(), upload_id });
        }
        self.store(&[id], StoreKind::Failed, fx);
    }

    pub(super) fn step_error(&mut self, id: TransferId, attempt_id: AttemptId, temporary: bool, fx: &mut Fx) {
        if !self.live(id, TransferStage::Uploading, attempt_id) {
            return;
        }
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        if temporary && transfer.attempts + 1 < ATTEMPT_LIMIT {
            transfer.attempts += 1;
            self.schedule_retry(id, fx);
        } else {
            self.begin_failure(id, "failed".into(), true, fx);
        }
        self.start_in_order(fx);
    }

    pub(super) fn completed(&mut self, id: TransferId, attempt_id: AttemptId, signed_id: Option<String>, fx: &mut Fx) {
        if !self.live(id, TransferStage::Completing, attempt_id) {
            return;
        }
        if let Some(transfer) = self.transfers.get_mut(&id) {
            transfer.signed_id = signed_id;
            transfer.in_flight = false;
            transfer.stage = TransferStage::PersistingComplete;
        }
        self.store(&[id], StoreKind::Completed, fx);
        self.start_in_order(fx);
    }

    pub(super) fn complete_error(&mut self, id: TransferId, attempt_id: AttemptId, temporary: bool, fx: &mut Fx) {
        if !self.live(id, TransferStage::Completing, attempt_id) {
            return;
        }
        if temporary {
            if let Some(transfer) = self.transfers.get_mut(&id) {
                transfer.attempts += 1;
            }
            self.schedule_retry(id, fx);
        } else {
            self.begin_failure(id, "failed".into(), false, fx);
        }
        self.start_in_order(fx);
    }

    pub(super) fn tick(&mut self, now: Time, kind: TransferTick, fx: &mut Fx) {
        self.now = self.now.max(now);
        match kind {
            TransferTick::Expire => self.expire(fx),
            TransferTick::Retry { transfer_id, attempt_id } => self.tick_retry(transfer_id, attempt_id, fx),
        }
    }

    fn tick_retry(&mut self, id: TransferId, attempt_id: AttemptId, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        if transfer.attempt_id != attempt_id {
            return;
        }
        match transfer.stage {
            TransferStage::Uploading | TransferStage::Completing if transfer.waiting_retry => {
                transfer.retry_due = true;
                self.start_in_order(fx);
            }
            TransferStage::PersistingFailure => self.store(&[id], StoreKind::Failed, fx),
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
        self.set(id, TransferStage::Mutating);
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

    fn mutating(&self, key: &str) -> Option<TransferId> { self.open_id(key).filter(|id| self.stage_of(*id) == Some(TransferStage::Mutating)) }

    pub(super) fn mutation_outcome(&mut self, key: &str, outcome: OutboxOutcome, fx: &mut Fx) {
        let Some(id) = self.mutating(key) else { return };
        let waiting = self.transfers.get(&id).is_some_and(|transfer| !transfer.discard_waiters.is_empty());
        match outcome {
            OutboxOutcome::Confirmed { revision, .. } => {
                if let Some(transfer) = self.transfers.get_mut(&id) {
                    for waiter in std::mem::take(&mut transfer.discard_waiters) {
                        reply(fx, waiter, TransferAnswer::Err(Fault::NotFound));
                    }
                }
                self.remove_with_batch(id, Removal::Confirmed { revision }, fx)
            }
            OutboxOutcome::Discarded => self.remove_with_batch(id, Removal::Discarded, fx),
            OutboxOutcome::Rejected { .. } if waiting => self.remove_with_batch(id, Removal::Discarded, fx),
            OutboxOutcome::Failed { .. } if waiting => {}
            OutboxOutcome::Rejected { error } | OutboxOutcome::Failed { error } => self.begin_failure(id, error, false, fx),
            OutboxOutcome::Interrupted => {
                self.interrupt(id, fx);
                self.rounds_idle(id, fx);
            }
        }
    }

    fn remove_with_batch(&mut self, id: TransferId, removal: Removal, fx: &mut Fx) {
        self.remove_with(id, removal);
        self.store(&[id], StoreKind::Removed, fx);
    }

    pub(super) fn discard_outcome(&mut self, key: &str, result: Result<(), Fault>, fx: &mut Fx) {
        let Some(id) = self.mutating(key) else { return };
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        if result.is_ok() {
            return;
        }
        transfer.discard_sent = false;
        for waiter in std::mem::take(&mut transfer.discard_waiters) {
            reply(fx, waiter, TransferAnswer::Err(Fault::InFlight));
        }
    }
}
