//! Owner `Transfer`: the opening, the steps, the cancel, `retry`, the expiry and the round.



use super::outbox::{Fault, OutboxInput, OutboxOutcome, Receipt};

use crate::owners::contract::ReplyId;



use crate::runtime::{Effect, Epoch, Outbound, Time};


use super::transfer_types::*;

impl TransferState {
    pub(super) fn restored(&mut self, rows: Vec<StoredTransfer>, fx: &mut Fx) {
        let mut rows = rows;
        rows.sort_by_key(|row| row.seq);
        for row in rows {
            let StoredTransfer { key, seq, source, batch, upload_id, confirmed_parts, completed, failed, file_present, created_at, place, size, sha256, mutation, steps } = row;
            let all = confirmed_parts >= source.parts;
            self.create(&key, source, batch, Some(seq), created_at, TransferStage::Ready, mutation, steps);
            let Some(transfer) = self.transfers.get_mut(&key) else { continue };
            transfer.place = Some(place);
            transfer.size = size;
            transfer.sha256 = sha256;
            transfer.upload_id = upload_id;
            transfer.confirmed_parts = confirmed_parts;
            if !file_present {
                transfer.stage = TransferStage::Failed;
                transfer.error = Some("missing_file".into());
                Self::shows(fx, &key, Shown::Failed { error: "missing_file".into() });
                self.store(&key, StoreKind::Failed, fx);
            } else if let Some(error) = failed {
                transfer.stage = TransferStage::Failed;
                transfer.error = Some(error.clone());
                Self::shows(fx, &key, Shown::Failed { error });
                self.schedule_expire(fx);
            } else if completed {
                transfer.stage = TransferStage::Mutating;
            } else if all {
                transfer.stage = TransferStage::Completing;
                let online = self.online;
                if online {
                    transfer.complete_in_flight = true;
                    run(fx, TransferEffect::Complete { key: key.clone(), upload_id: transfer.upload_id.clone() });
                }
                Self::shows(fx, &key, Shown::Uploading { progress: 100 });
                self.round_needed(self.epoch, fx);
            } else {
                Self::shows(fx, &key, Shown::Queued);
                self.round_needed(self.epoch, fx);
                self.schedule_expire(fx);
            }
        }
        self.start_in_order(fx);
    }

    pub(super) fn fail(&mut self, key: &str, abort: bool, fx: &mut Fx) {
        let error = "failed".to_string();
        if let Some(transfer) = self.transfers.get_mut(key) {
            transfer.stage = TransferStage::Failed;
            transfer.error = Some(error.clone());
        }
        Self::shows(fx, key, Shown::Failed { error });
        if abort {
            let upload_id = self.upload_id_of(key);
            run(fx, TransferEffect::Abort { key: key.into(), upload_id });
        }
        self.store(key, StoreKind::Failed, fx);
    }

    pub(super) fn step_ok(&mut self, key: &str, part: Option<u32>, upload_id: Option<String>, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get_mut(key) else { return };
        if transfer.stage != TransferStage::Uploading || part != Some(transfer.confirmed_parts + 1) {
            return;
        }
        if upload_id.is_some() {
            transfer.upload_id = upload_id;
        }
        transfer.confirmed_parts += 1;
        let all = transfer.confirmed_parts >= transfer.source.parts;
        let progress = if all { 100 } else { Self::progress(transfer) };
        let next = Self::step_effect(transfer);
        let complete = TransferEffect::Complete { key: key.into(), upload_id: transfer.upload_id.clone() };
        if all {
            transfer.stage = TransferStage::Completing;
            transfer.complete_in_flight = true;
        }
        self.store(key, StoreKind::ConfirmedParts, fx);
        Self::shows(fx, key, Shown::Uploading { progress });
        run(fx, if all { complete } else { next });
    }

    pub(super) fn step_error(&mut self, key: &str, part: Option<u32>, temporary: bool, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get_mut(key) else { return };
        if transfer.stage != TransferStage::Uploading || part != Some(transfer.confirmed_parts + 1) {
            return;
        }
        let has_upload = transfer.upload_id.is_some();
        if temporary && !self.online {
            transfer.stage = TransferStage::Ready;
            Self::shows(fx, key, Shown::Queued);
        } else if temporary && transfer.attempts + 1 < ATTEMPT_LIMIT {
            transfer.attempts += 1;
            let attempt = transfer.attempts;
            self.retry_timer(key, attempt, fx);
        } else {
            self.fail(key, has_upload, fx);
        }
        self.start_in_order(fx);
    }

    pub(super) fn tick_retry(&mut self, key: &str, fx: &mut Fx) {
        let online = self.online;
        let Some(transfer) = self.transfers.get_mut(key) else { return };
        match transfer.stage {
            TransferStage::Uploading if online => {
                let step = Self::step_effect(transfer);
                run(fx, step);
            }
            TransferStage::Uploading => {
                transfer.stage = TransferStage::Ready;
                Self::shows(fx, key, Shown::Queued);
                self.start_in_order(fx);
            }
            TransferStage::Completing if online && !transfer.complete_in_flight => {
                transfer.complete_in_flight = true;
                run(fx, TransferEffect::Complete { key: key.into(), upload_id: transfer.upload_id.clone() });
            }
            _ => {}
        }
    }

    pub(super) fn complete_error(&mut self, key: &str, temporary: bool, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get_mut(key) else { return };
        if transfer.stage != TransferStage::Completing {
            return;
        }
        transfer.complete_in_flight = false;
        if !temporary {
            self.fail(key, false, fx);
            self.start_in_order(fx);
        } else if self.online {
            self.retry_timer(key, 1, fx);
        }
    }

    pub(super) fn mutation_outcome(&mut self, key: &str, outcome: OutboxOutcome, fx: &mut Fx) {
        if self.stage_of(key) != Some(TransferStage::Mutating) {
            return;
        }
        match outcome {
            OutboxOutcome::Confirmed { .. } => {
                self.end_transfer(key, fx);
                self.drop_file(key, false, fx);
            }
            OutboxOutcome::Rejected { .. } | OutboxOutcome::Failed { .. } => self.fail(key, false, fx),
            OutboxOutcome::Discarded => {
                self.end_transfer(key, fx);
                self.drop_file(key, false, fx);
            }
            OutboxOutcome::Interrupted => self.end_transfer(key, fx),
        }
    }

    /// Cancel and discard end the transfer in every state that allows it. `cancel` answers with a flag, `discard` with `Ok`.
    pub(super) fn cancel(&mut self, key: &str, rep: ReplyId, cancel: bool, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get(key) else { return reply(fx, rep, TransferAnswer::Err(Fault::NotFound)) };
        let (stage, batch, has_upload) = (transfer.stage, transfer.batch, transfer.upload_id.is_some());
        let ok = if cancel { TransferAnswer::Cancelled(true) } else { TransferAnswer::Ok };
        match stage {
            TransferStage::Done => reply(fx, rep, TransferAnswer::Err(Fault::NotFound)),
            TransferStage::Copying => {
                reply(fx, rep, ok);
                self.end_transfer(key, fx);
                run(fx, TransferEffect::CopyCancel { key: key.into() });
                self.drop_file(key, false, fx);
            }
            TransferStage::Ready => {
                reply(fx, rep, ok);
                self.end_transfer(key, fx);
                self.drop_file(key, true, fx);
                self.start_in_order(fx);
            }
            TransferStage::Uploading | TransferStage::Completing if cancel && batch => reply(fx, rep, TransferAnswer::Err(Fault::InFlight)),
            TransferStage::Uploading | TransferStage::Completing => {
                reply(fx, rep, ok);
                self.end_transfer(key, fx);
                if stage == TransferStage::Uploading {
                    run(fx, TransferEffect::CancelStep { key: key.into() });
                }
                if stage == TransferStage::Completing || has_upload {
                    let upload_id = self.upload_id_of(key);
                    run(fx, TransferEffect::Abort { key: key.into(), upload_id });
                }
                self.drop_file(key, true, fx);
                self.start_in_order(fx);
            }
            TransferStage::Mutating if cancel => reply(fx, rep, TransferAnswer::Cancelled(false).or_in_flight(batch)),
            TransferStage::Mutating => {
                let op = &self.transfers[key].mutation;
                let discard = OutboxInput::Discard { model: op.model.clone(), row_id: op.row_id.clone(), reply: rep };
                fx.push(Effect::Send(Outbound::new(self.peers.outbox, discard)));
            }
            TransferStage::Failed => {
                reply(fx, rep, ok);
                self.end_transfer(key, fx);
                if has_upload {
                    let upload_id = self.upload_id_of(key);
                    run(fx, TransferEffect::Abort { key: key.into(), upload_id });
                }
                self.drop_file(key, true, fx);
            }
        }
    }

    pub(super) fn retry(&mut self, key: &str, rep: ReplyId, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get(key) else { return reply(fx, rep, TransferAnswer::Err(Fault::NotFound)) };
        let stage = transfer.stage;
        if stage == TransferStage::Done {
            return reply(fx, rep, TransferAnswer::Err(Fault::NotFound));
        }
        reply(fx, rep, TransferAnswer::Receipt(Receipt { epoch: self.epoch, op_id: transfer.seq, cycle_id: 1, revision: 0 }));
        if stage != TransferStage::Failed {
            return;
        }
        let all = transfer.confirmed_parts >= transfer.source.parts;
        if all {
            self.set(key, TransferStage::Completing);
            self.transfers.get_mut(key).map(|transfer| transfer.complete_in_flight = true);
            Self::shows(fx, key, Shown::Uploading { progress: 100 });
            self.store(key, StoreKind::Retrying, fx);
            let upload_id = self.upload_id_of(key);
            run(fx, TransferEffect::Complete { key: key.into(), upload_id });
        } else {
            self.set(key, TransferStage::Ready);
            Self::shows(fx, key, Shown::Queued);
            self.store(key, StoreKind::Retrying, fx);
            self.start_in_order(fx);
        }
    }

    pub(super) fn online_changed(&mut self, on: bool, fx: &mut Fx) {
        self.online = on;
        let keys: Vec<Key> = self.transfers.values().map(|transfer| transfer.key.clone()).collect();
        for key in keys {
            let Some(transfer) = self.transfers.get_mut(&key) else { continue };
            match (transfer.stage, on) {
                (TransferStage::Uploading, false) => {
                    transfer.stage = TransferStage::Ready;
                    Self::shows(fx, &key, Shown::Queued);
                    run(fx, TransferEffect::CancelStep { key: key.clone() });
                }
                (TransferStage::Completing, true) if !transfer.complete_in_flight => {
                    transfer.complete_in_flight = true;
                    run(fx, TransferEffect::Complete { key: key.clone(), upload_id: transfer.upload_id.clone() });
                }
                (TransferStage::Failed, true) => self.schedule_expire(fx),
                _ => {}
            }
        }
        self.start_in_order(fx);
    }

    pub(super) fn expire(&mut self, now: Time, fx: &mut Fx) {
        self.now = self.now.max(now);
        let expired: Vec<Key> = self.transfers.values().filter(|transfer| transfer.stage == TransferStage::Failed && self.now.0.saturating_sub(transfer.created_at.0) > EXPIRY_MS).map(|transfer| transfer.key.clone()).collect();
        for key in expired {
            let has_upload = self.transfers[&key].upload_id.is_some();
            self.end_transfer(&key, fx);
            if has_upload {
                let upload_id = self.upload_id_of(&key);
                run(fx, TransferEffect::Abort { key: key.clone(), upload_id });
            }
            self.drop_file(&key, true, fx);
        }
    }

    pub(super) fn interrupt(&mut self, key: &str, fx: &mut Fx) {
        run(fx, TransferEffect::CancelStep { key: key.into() });
        self.set(key, TransferStage::Done);
    }

    pub(super) fn interrupt_all(&mut self, closing: bool, fx: &mut Fx) {
        let open: Vec<(Key, TransferStage)> = self.transfers.values().filter(|transfer| transfer.stage != TransferStage::Done).map(|transfer| (transfer.key.clone(), transfer.stage)).collect();
        for (key, stage) in open {
            if closing && stage == TransferStage::Copying {
                run(fx, TransferEffect::CopyCancel { key: key.clone() });
                self.drop_file(&key, false, fx);
            } else {
                run(fx, TransferEffect::CancelStep { key: key.clone() });
            }
            self.set(&key, TransferStage::Done);
        }
    }

    pub(super) fn round_started(&mut self, round_id: RoundId, epoch: Epoch, fx: &mut Fx) {
        if let RoundState::Requested { epoch: requested } = self.round {
            if epoch == self.epoch && requested == epoch {
                self.round = RoundState::Running { round_id, epoch, next: None };
            } else {
                run(fx, TransferEffect::EndJob { round_id });
            }
        }
    }

    pub(super) fn round_refused(&mut self, fx: &mut Fx) {
        if let RoundState::Requested { .. } = self.round {
            self.round = RoundState::None;
            let first = self.transfers.values().filter(|transfer| matches!(transfer.stage, TransferStage::Ready | TransferStage::Uploading)).min_by_key(|transfer| transfer.seq).map(|transfer| transfer.key.clone());
            if let Some(key) = first {
                run(fx, TransferEffect::Refused { key });
            }
        }
    }

    pub(super) fn round_ended(&mut self, round_id: RoundId, epoch: Epoch, fx: &mut Fx) {
        let RoundState::Running { round_id: running, epoch: running_epoch, next } = self.round else { return };
        if round_id != running || epoch != running_epoch {
            return;
        }
        match next {
            Some(next_epoch) if self.open_in_epoch(next_epoch) => {
                self.round = RoundState::Requested { epoch: next_epoch };
                run(fx, TransferEffect::ScheduleJob { epoch: next_epoch });
            }
            _ => self.round = RoundState::None,
        }
    }
}
