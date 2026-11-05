//! Owner `Transfer`: file upload steps and the background round.
//! One instance per epoch with an identity. The types are in `transfer_types`, the rows of the table for the steps and the rounds in `transfer_flow`.

use std::collections::BTreeMap;

use super::outbox::{Fault, NewOp, Origin, OutboxInput, Receipt};
use crate::files::Location;
use crate::owners::contract::ReplyId;
use crate::owners::contract::{BatchAuthor, Drained, EpochEnded, Online, RejectReason, Rejected};
use crate::owners::store::{StoreBatch, StoreInput, StoreOp};
use crate::pacing::RetryPolicy;
use crate::runtime::{Cause, Effect, Effects, Epoch, Outbound, ScheduleId, StateOwner, Time};
use crate::transport::NetworkPurpose;
pub use super::transfer_types::*;

impl TransferState {
    pub(super) fn shows(fx: &mut Fx, key: &str, shown: Shown) { run(fx, TransferEffect::Shows { key: key.into(), shown }); }

    pub(super) fn store(&mut self, key: &str, kind: StoreKind, fx: &mut Fx) {
        let id = self.next_batch;
        self.next_batch += 1;
        let ops = match (kind, self.transfers.get(key)) {
            (StoreKind::Removed, _) | (_, None) => vec![StoreOp::RemoveUpload { key: key.into() }],
            (kind, Some(transfer)) => vec![StoreOp::Upload { row: upload_row(transfer, kind), kind: upload_kind(kind) }],
        };
        let intents = u64::from(kind == StoreKind::SyncUpload);
        self.batches.insert(id, key.into());
        let batch = StoreBatch { author: BatchAuthor::Transfer, id, epoch: self.epoch, basis: BTreeMap::new(), rows: Vec::new(), intents, credential: None, ops };
        fx.push(Effect::Send(Outbound::new(self.peers.store, StoreInput::Commit(batch))));
    }

    /// `Rejected(NoSession | Closed)` ends the transfer as interrupted, `Rejected(Storage)` leaves it failed with the error shown. Nothing is written again.
    pub(super) fn store_rejected(&mut self, rejected: Rejected, fx: &mut Fx) {
        let Some(key) = self.batches.remove(&rejected.batch) else { return };
        let Some(stage) = self.stage_of(&key) else { return };
        if stage == TransferStage::Done {
            return;
        }
        match rejected.reason {
            RejectReason::Storage(error) => {
                self.set(&key, TransferStage::Failed);
                if let Some(transfer) = self.transfers.get_mut(&key) {
                    transfer.error = Some(error.clone());
                }
                Self::shows(fx, &key, Shown::Failed { error });
            }
            RejectReason::NoSession | RejectReason::Closed | RejectReason::Invalid | RejectReason::StaleGeneration { .. } => self.interrupt(&key, fx),
        }
    }

    pub(super) fn timer(&mut self) -> ScheduleId {
        self.next_timer += 1;
        ScheduleId(self.next_timer)
    }

    pub(super) fn upload_id_of(&self, key: &str) -> Option<String> { self.transfers.get(key).and_then(|transfer| transfer.upload_id.clone()) }

    pub(super) fn active(&self) -> u32 { self.transfers.values().filter(|transfer| matches!(transfer.stage, TransferStage::Uploading | TransferStage::Completing)).count() as u32 }

    pub(super) fn progress(transfer: &Transfer) -> u32 { transfer.confirmed_parts * 100 / transfer.source.parts.max(1) }

    pub(super) fn set(&mut self, key: &str, stage: TransferStage) {
        if let Some(transfer) = self.transfers.get_mut(key) {
            transfer.stage = stage;
        }
    }

    pub(super) fn step_effect(transfer: &Transfer) -> TransferEffect { TransferEffect::Step { key: transfer.key.clone(), part: Some(transfer.confirmed_parts + 1), purpose: NetworkPurpose::Transfer, upload_id: transfer.upload_id.clone(), source: transfer.source.clone() } }

    /// Ready transfers start by `(seq, position)` while a slot is free and the network is up.
    pub(super) fn start_in_order(&mut self, fx: &mut Fx) {
        while self.online && self.active() < self.slots {
            let next = self.transfers.values().filter(|transfer| transfer.stage == TransferStage::Ready).min_by_key(|transfer| transfer.seq).map(|transfer| transfer.key.clone());
            let Some(key) = next else { return };
            let Some(transfer) = self.transfers.get_mut(&key) else { return };
            transfer.stage = TransferStage::Uploading;
            transfer.attempts = 0;
            let progress = Self::progress(transfer);
            let step = Self::step_effect(transfer);
            Self::shows(fx, &key, Shown::Uploading { progress });
            run(fx, step);
        }
    }

    pub(super) fn schedule_expire(&mut self, fx: &mut Fx) {
        let id = self.timer();
        fx.push(Effect::Schedule { id, after: std::time::Duration::ZERO, input: TransferInput::Tick { now: self.now, kind: TransferTick::Expire } });
    }

    pub(super) fn end_transfer(&mut self, key: &str, fx: &mut Fx) {
        self.set(key, TransferStage::Done);
        Self::shows(fx, key, Shown::Gone);
    }

    /// The copy of `key` goes: the file of the directory by `DeleteFile`, the inline bytes (and with `rows` the row of `sync_upload` and the operation) by a Store batch.
    pub(super) fn drop_file(&mut self, key: &str, rows: bool, fx: &mut Fx) {
        let place = self.transfers.get(key).and_then(|transfer| transfer.place);
        if place != Some(Location::Table) {
            run(fx, TransferEffect::DeleteFile { path: file_id(key) });
        }
        if rows || place == Some(Location::Table) {
            self.store(key, StoreKind::Removed, fx);
        }
    }

    pub(super) fn retry_timer(&mut self, key: &str, attempt: u32, fx: &mut Fx) {
        let id = ScheduleId(RETRY_TIMER + self.next_timer);
        self.next_timer += 1;
        let after = RetryPolicy::Upload.backoff().delay(attempt.saturating_sub(1));
        fx.push(Effect::Schedule { id, after, input: TransferInput::Tick { now: self.now, kind: TransferTick::Retry { key: key.into() } } });
    }

    /// The final mutation goes to Outbox as an operation of the origin `Transfer{key}`.
    pub(super) fn submit_mutation(&mut self, key: &str, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get(key) else { return };
        let op = NewOp { origin: Origin::Transfer(key.into()), ..transfer.mutation.clone() };
        fx.push(Effect::Send(Outbound::new(self.peers.outbox, OutboxInput::Submit { epoch: self.epoch, op, reply: NO_REPLY })));
    }

    pub(super) fn round_needed(&mut self, epoch: Epoch, fx: &mut Fx) {
        match self.round {
            RoundState::None if epoch == self.epoch => {
                self.round = RoundState::Requested { epoch };
                run(fx, TransferEffect::ScheduleJob { epoch });
            }
            RoundState::Running { round_id, epoch: running, .. } if epoch != running => self.round = RoundState::Running { round_id, epoch: running, next: Some(epoch) },
            _ => {}
        }
    }

    pub(super) fn open_in_epoch(&self, epoch: Epoch) -> bool { self.transfers.values().any(|transfer| transfer.epoch == epoch && transfer.stage != TransferStage::Done) }

    pub(super) fn create(&mut self, key: &str, source: Source, batch: bool, seq: Option<u64>, created_at: Time, stage: TransferStage, mutation: NewOp, steps: Steps) {
        let seq = seq.unwrap_or_else(|| {
            self.next_seq += 1;
            self.next_seq - 1
        });
        self.next_seq = self.next_seq.max(seq + 1);
        self.transfers.insert(key.into(), Transfer { key: key.into(), seq, source, batch, epoch: self.epoch, stage, attempts: 0, upload_id: None, confirmed_parts: 0, created_at, error: None, complete_in_flight: false, place: None, size: 0, sha256: String::new(), mutation, steps });
    }

    pub(super) fn accept(&mut self, epoch: Epoch, key: Key, source: Source, batch: bool, rep: ReplyId, now: Time, mutation: NewOp, steps: Steps, fx: &mut Fx) {
        if epoch != self.epoch {
            return reply(fx, rep, TransferAnswer::Err(Fault::NoSession));
        }
        self.now = self.now.max(now);
        let copy_from = source.clone();
        self.create(&key, source, batch, None, now, TransferStage::Copying, mutation, steps);
        let seq = self.transfers[&key].seq;
        reply(fx, rep, TransferAnswer::Receipt(Receipt { epoch, op_id: seq, cycle_id: 1, revision: 0 }));
        Self::shows(fx, &key, Shown::Copying);
        run(fx, TransferEffect::Copy { key, source: copy_from });
        self.round_needed(epoch, fx);
    }

}

impl TransferAnswer {
    pub(super) fn or_in_flight(self, batch: bool) -> Self { if batch { Self::Err(Fault::InFlight) } else { self } }
}

pub struct Transfers;

impl StateOwner for Transfers {
    const EPOCHED: bool = true;

    type State = TransferState;
    type Input = TransferInput;
    type Effect = TransferEffect;

    fn step(state: &mut TransferState, input: TransferInput) -> Effects<Self> {
        let mut fx: Fx = Vec::new();
        if let (Some(error), TransferInput::Accept { reply: id, .. } | TransferInput::Cancel { reply: id, .. } | TransferInput::Discard { reply: id, .. } | TransferInput::Retry { reply: id, .. }) = (&state.queue_error, &input) {
            reply(&mut fx, *id, TransferAnswer::Err(Fault::Rejected(RejectReason::Storage(error.clone()))));
            return fx;
        }
        match input {
            TransferInput::Accept { epoch, key, source, batch, reply, now, mutation, steps } => state.accept(epoch, key, source, batch, reply, now, mutation, steps, &mut fx),
            TransferInput::Restored(rows) => state.restored(rows, &mut fx),
            TransferInput::RestoreFailed { error } => state.queue_error = Some(error),
            TransferInput::Committed(committed) => {
                state.batches.remove(&committed.batch);
            }
            TransferInput::Rejected(rejected) => state.store_rejected(rejected, &mut fx),
            TransferInput::CopyDone { key, size, sha256, place } => {
                if state.stage_of(&key) == Some(TransferStage::Copying) {
                    if let Some(transfer) = state.transfers.get_mut(&key) {
                        transfer.size = size;
                        transfer.sha256 = sha256;
                        transfer.place = Some(place);
                    }
                    state.set(&key, TransferStage::Ready);
                    TransferState::shows(&mut fx, &key, Shown::Queued);
                    state.store(&key, StoreKind::SyncUpload, &mut fx);
                    state.start_in_order(&mut fx);
                }
            }
            TransferInput::CopyFailed { key, error } => {
                if state.stage_of(&key) == Some(TransferStage::Copying) {
                    state.set(&key, TransferStage::Failed);
                    TransferState::shows(&mut fx, &key, Shown::Failed { error });
                    state.store(&key, StoreKind::Failed, &mut fx);
                }
            }
            TransferInput::StepOk { key, part, upload_id } => state.step_ok(&key, part, upload_id, &mut fx),
            TransferInput::StepError { key, part, temporary } => state.step_error(&key, part, temporary, &mut fx),
            TransferInput::Completed { key } => {
                if state.stage_of(&key) == Some(TransferStage::Completing) {
                    state.set(&key, TransferStage::Mutating);
                    state.store(&key, StoreKind::Completed, &mut fx);
                    state.submit_mutation(&key, &mut fx);
                    state.start_in_order(&mut fx);
                }
            }
            TransferInput::CompleteError { key, temporary } => state.complete_error(&key, temporary, &mut fx),
            TransferInput::MutationOutcome { key, outcome } => state.mutation_outcome(&key, outcome, &mut fx),
            TransferInput::Cancel { key, reply } => state.cancel(&key, reply, true, &mut fx),
            TransferInput::Discard { key, reply } => state.cancel(&key, reply, false, &mut fx),
            TransferInput::Retry { key, reply } => state.retry(&key, reply, &mut fx),
            TransferInput::SlotFreed => state.start_in_order(&mut fx),
            TransferInput::RoundNeeded { epoch } => state.round_needed(epoch, &mut fx),
            TransferInput::RoundStarted { round_id, epoch } => state.round_started(round_id, epoch, &mut fx),
            TransferInput::RoundRefused { .. } => state.round_refused(&mut fx),
            TransferInput::RoundEnded { round_id, epoch } => state.round_ended(round_id, epoch, &mut fx),
            TransferInput::Online(Online(on)) => state.online_changed(on, &mut fx),
            TransferInput::EpochStarted(started) => state.epoch = started.epoch,
            TransferInput::Tick { now, kind } => match kind {
                TransferTick::Retry { key } => state.tick_retry(&key, &mut fx),
                TransferTick::Expire => state.expire(now, &mut fx),
            },
            TransferInput::EpochEnded(_) => state.interrupt_all(false, &mut fx),
            TransferInput::Closing => {
                state.interrupt_all(true, &mut fx);
                if let RoundState::Running { round_id, .. } = state.round {
                    run(&mut fx, TransferEffect::EndJob { round_id });
                }
                state.round = RoundState::None;
                if !std::mem::replace(&mut state.drained, true) {
                    run(&mut fx, TransferEffect::Drained(Drained { owner: "transfer" }));
                }
            }
            TransferInput::Clock(now) => state.now = state.now.max(now),
        }
        fx
    }

    fn tick(now: Time) -> TransferInput { TransferInput::Clock(now) }

    fn epoch_ended(epoch: Epoch, cause: Cause) -> TransferInput { TransferInput::EpochEnded(EpochEnded { epoch, cause }) }

    fn closing() -> TransferInput { TransferInput::Closing }
}
