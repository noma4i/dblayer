//! Owner `Transfer`: file upload steps and the background round (`specs/machines/transfer.md`).
//! One instance per epoch with an identity. Types of the table: inputs, effects, stages. The rows are in `step`.

use std::collections::BTreeMap;

use super::contract::{BatchId, Drained, EpochEnded, EpochStarted, Online};
use crate::transport::NetworkPurpose;
use super::outbox::{Fault, OutboxOutcome, Receipt, ReplyId};
use crate::runtime::{Cause, Effect, Effects, Epoch, ScheduleId, StateOwner, Time};

pub type Key = String;
pub type RoundId = u32;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Source {
    pub path: String,
    pub parts: u32,
}

/// A row of `sync_upload` kept in the base.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredTransfer {
    pub key: Key,
    pub seq: u64,
    pub source: Source,
    pub batch: bool,
    pub upload_id: Option<String>,
    pub confirmed_parts: u32,
    pub completed: bool,
    pub failed: Option<String>,
    pub file_present: bool,
    pub created_at: Time,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransferTick {
    Retry { key: Key },
    Expire,
}

#[derive(Clone, Debug)]
pub enum TransferInput {
    Accept { epoch: Epoch, key: Key, source: Source, batch: bool, reply: ReplyId, now: Time },
    Restored(Vec<StoredTransfer>),
    CopyDone { key: Key },
    CopyFailed { key: Key, error: String },
    StepOk { key: Key, part: Option<u32> },
    StepError { key: Key, part: Option<u32>, temporary: bool },
    Completed { key: Key },
    CompleteError { key: Key, temporary: bool },
    MutationOutcome { key: Key, outcome: OutboxOutcome },
    Cancel { key: Key, reply: ReplyId },
    Retry { key: Key, reply: ReplyId },
    Discard { key: Key, reply: ReplyId },
    SlotFreed,
    RoundNeeded { epoch: Epoch },
    RoundStarted { round_id: RoundId, epoch: Epoch },
    RoundRefused { epoch: Epoch },
    RoundEnded { round_id: RoundId, epoch: Epoch },
    Online(Online),
    EpochStarted(EpochStarted),
    Tick { now: Time, kind: TransferTick },
    EpochEnded(EpochEnded),
    Closing,
    /// The trait tick: runs `Tick(expire)` of the clock.
    Clock(Time),
}

/// What `uploads[action]` shows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Shown {
    Copying,
    Queued,
    Uploading { progress: u32 },
    Failed { error: String },
    Gone,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransferAnswer {
    Receipt(Receipt),
    Ok,
    Cancelled(bool),
    Err(Fault),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreKind {
    SyncUpload,
    ConfirmedParts,
    Completed,
    Failed,
    Retrying,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransferEffect {
    Copy { key: Key },
    CopyCancel { key: Key },
    Store { key: Key, batch: BatchId, kind: StoreKind },
    /// A step of the file (`part`, `blob`) with `NetworkPurpose::Transfer`.
    Step { key: Key, part: Option<u32>, purpose: NetworkPurpose },
    Complete { key: Key },
    /// To the executor: it adds the Backoff and the clock, then posts `Tick(retry{key})`.
    ScheduleRetry { key: Key, attempt: u32 },
    Abort { key: Key },
    CancelStep { key: Key },
    DeleteFile { key: Key },
    DeleteOperation { key: Key },
    DeleteLocalCopy { key: Key },
    SubmitMutation { key: Key },
    DiscardMutation { key: Key, reply: ReplyId },
    Reply { reply: ReplyId, answer: TransferAnswer },
    Shows { key: Key, shown: Shown },
    /// `uploads[action].error = refused` on the accepted transfer.
    Refused { key: Key },
    /// To the platform.
    ScheduleJob { epoch: Epoch },
    EndJob { round_id: RoundId },
    Drained(Drained),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransferStage {
    Copying,
    Ready,
    Uploading,
    Completing,
    Mutating,
    Failed,
    Done,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RoundState {
    None,
    Requested { epoch: Epoch },
    Running { round_id: RoundId, epoch: Epoch, next: Option<Epoch> },
}

#[derive(Clone, Debug)]
pub struct Transfer {
    pub key: Key,
    pub seq: u64,
    pub source: Source,
    pub batch: bool,
    pub epoch: Epoch,
    pub stage: TransferStage,
    pub attempts: u32,
    pub upload_id: Option<String>,
    pub confirmed_parts: u32,
    pub created_at: Time,
    pub error: Option<String>,
    pub complete_in_flight: bool,
}

#[derive(Debug)]
pub struct TransferState {
    pub epoch: Epoch,
    pub online: bool,
    pub slots: u32,
    pub transfers: BTreeMap<Key, Transfer>,
    pub round: RoundState,
    pub next_seq: u64,
    pub next_batch: BatchId,
    pub now: Time,
    pub drained: bool,
}

impl TransferState {
    pub fn new(epoch: Epoch, slots: u32) -> Self { Self { epoch, online: true, slots, transfers: BTreeMap::new(), round: RoundState::None, next_seq: 1, next_batch: 1, now: Time(0), drained: false } }

    pub fn stage_of(&self, key: &str) -> Option<TransferStage> { self.transfers.get(key).map(|transfer| transfer.stage) }
}

/// A failed transfer older than 7 days is discarded.
const EXPIRY_MS: u64 = 7 * 86_400_000;
/// Attempts of a step before the permanent error (`design.md` 1a).
const ATTEMPT_LIMIT: u32 = 50;

type Fx = Effects<Transfers>;

fn run(fx: &mut Fx, effect: TransferEffect) { fx.push(Effect::Run(effect)); }

fn reply(fx: &mut Fx, reply: ReplyId, answer: TransferAnswer) { run(fx, TransferEffect::Reply { reply, answer }); }

impl TransferState {
    fn shows(fx: &mut Fx, key: &str, shown: Shown) { run(fx, TransferEffect::Shows { key: key.into(), shown }); }

    fn store(&mut self, key: &str, kind: StoreKind, fx: &mut Fx) {
        let batch = self.next_batch;
        self.next_batch += 1;
        run(fx, TransferEffect::Store { key: key.into(), batch, kind });
    }

    fn active(&self) -> u32 { self.transfers.values().filter(|transfer| matches!(transfer.stage, TransferStage::Uploading | TransferStage::Completing)).count() as u32 }

    fn progress(transfer: &Transfer) -> u32 { transfer.confirmed_parts * 100 / transfer.source.parts.max(1) }

    fn set(&mut self, key: &str, stage: TransferStage) {
        if let Some(transfer) = self.transfers.get_mut(key) {
            transfer.stage = stage;
        }
    }

    fn step_effect(transfer: &Transfer) -> TransferEffect { TransferEffect::Step { key: transfer.key.clone(), part: Some(transfer.confirmed_parts + 1), purpose: NetworkPurpose::Transfer } }

    /// Ready transfers start by `(seq, position)` while a slot is free and the network is up.
    fn start_in_order(&mut self, fx: &mut Fx) {
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

    fn schedule_expire(&mut self, fx: &mut Fx) {
        let id = ScheduleId(self.next_batch);
        self.next_batch += 1;
        fx.push(Effect::Schedule { id, at: self.now, input: TransferInput::Tick { now: self.now, kind: TransferTick::Expire } });
    }

    fn end_transfer(&mut self, key: &str, fx: &mut Fx) {
        self.set(key, TransferStage::Done);
        Self::shows(fx, key, Shown::Gone);
    }

    fn delete_all(key: &str, fx: &mut Fx) {
        run(fx, TransferEffect::DeleteFile { key: key.into() });
        run(fx, TransferEffect::DeleteOperation { key: key.into() });
    }

    fn round_needed(&mut self, epoch: Epoch, fx: &mut Fx) {
        match self.round {
            RoundState::None if epoch == self.epoch => {
                self.round = RoundState::Requested { epoch };
                run(fx, TransferEffect::ScheduleJob { epoch });
            }
            RoundState::Running { round_id, epoch: running, .. } if epoch != running => self.round = RoundState::Running { round_id, epoch: running, next: Some(epoch) },
            _ => {}
        }
    }

    fn open_in_epoch(&self, epoch: Epoch) -> bool { self.transfers.values().any(|transfer| transfer.epoch == epoch && transfer.stage != TransferStage::Done) }

    fn create(&mut self, key: &str, source: Source, batch: bool, seq: Option<u64>, created_at: Time, stage: TransferStage) {
        let seq = seq.unwrap_or_else(|| {
            self.next_seq += 1;
            self.next_seq - 1
        });
        self.next_seq = self.next_seq.max(seq + 1);
        self.transfers.insert(key.into(), Transfer { key: key.into(), seq, source, batch, epoch: self.epoch, stage, attempts: 0, upload_id: None, confirmed_parts: 0, created_at, error: None, complete_in_flight: false });
    }

    fn accept(&mut self, epoch: Epoch, key: Key, source: Source, batch: bool, rep: ReplyId, now: Time, fx: &mut Fx) {
        if epoch != self.epoch {
            return reply(fx, rep, TransferAnswer::Err(Fault::NoSession));
        }
        self.now = self.now.max(now);
        self.create(&key, source, batch, None, now, TransferStage::Copying);
        let seq = self.transfers[&key].seq;
        reply(fx, rep, TransferAnswer::Receipt(Receipt { epoch, op_id: seq, revision: 0 }));
        Self::shows(fx, &key, Shown::Copying);
        run(fx, TransferEffect::Copy { key });
        self.round_needed(epoch, fx);
    }

    fn restored(&mut self, rows: Vec<StoredTransfer>, fx: &mut Fx) {
        let mut rows = rows;
        rows.sort_by_key(|row| row.seq);
        for row in rows {
            let StoredTransfer { key, seq, source, batch, upload_id, confirmed_parts, completed, failed, file_present, created_at } = row;
            let all = confirmed_parts >= source.parts;
            self.create(&key, source, batch, Some(seq), created_at, TransferStage::Ready);
            let Some(transfer) = self.transfers.get_mut(&key) else { continue };
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
                    run(fx, TransferEffect::Complete { key: key.clone() });
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

    fn fail(&mut self, key: &str, abort: bool, fx: &mut Fx) {
        let error = "failed".to_string();
        if let Some(transfer) = self.transfers.get_mut(key) {
            transfer.stage = TransferStage::Failed;
            transfer.error = Some(error.clone());
        }
        Self::shows(fx, key, Shown::Failed { error });
        if abort {
            run(fx, TransferEffect::Abort { key: key.into() });
        }
        self.store(key, StoreKind::Failed, fx);
    }

    fn step_ok(&mut self, key: &str, part: Option<u32>, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get_mut(key) else { return };
        if transfer.stage != TransferStage::Uploading || part != Some(transfer.confirmed_parts + 1) {
            return;
        }
        transfer.confirmed_parts += 1;
        let all = transfer.confirmed_parts >= transfer.source.parts;
        let progress = if all { 100 } else { Self::progress(transfer) };
        let next = Self::step_effect(transfer);
        if all {
            transfer.stage = TransferStage::Completing;
            transfer.complete_in_flight = true;
        }
        self.store(key, StoreKind::ConfirmedParts, fx);
        Self::shows(fx, key, Shown::Uploading { progress });
        run(fx, if all { TransferEffect::Complete { key: key.into() } } else { next });
    }

    fn step_error(&mut self, key: &str, part: Option<u32>, temporary: bool, fx: &mut Fx) {
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
            run(fx, TransferEffect::ScheduleRetry { key: key.into(), attempt: transfer.attempts });
        } else {
            self.fail(key, has_upload, fx);
        }
        self.start_in_order(fx);
    }

    fn tick_retry(&mut self, key: &str, fx: &mut Fx) {
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
                run(fx, TransferEffect::Complete { key: key.into() });
            }
            _ => {}
        }
    }

    fn complete_error(&mut self, key: &str, temporary: bool, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get_mut(key) else { return };
        if transfer.stage != TransferStage::Completing {
            return;
        }
        transfer.complete_in_flight = false;
        if !temporary {
            self.fail(key, false, fx);
            self.start_in_order(fx);
        } else if self.online {
            run(fx, TransferEffect::ScheduleRetry { key: key.into(), attempt: 0 });
        }
    }

    fn mutation_outcome(&mut self, key: &str, outcome: OutboxOutcome, fx: &mut Fx) {
        if self.stage_of(key) != Some(TransferStage::Mutating) {
            return;
        }
        match outcome {
            OutboxOutcome::Confirmed { .. } => {
                self.end_transfer(key, fx);
                run(fx, TransferEffect::DeleteLocalCopy { key: key.into() });
            }
            OutboxOutcome::Rejected { .. } | OutboxOutcome::Failed { .. } => self.fail(key, false, fx),
            OutboxOutcome::Discarded => {
                self.end_transfer(key, fx);
                run(fx, TransferEffect::DeleteFile { key: key.into() });
            }
            OutboxOutcome::Interrupted => self.end_transfer(key, fx),
        }
    }

    /// Cancel and discard end the transfer in every state that allows it. `cancel` answers with a flag, `discard` with `Ok`.
    fn cancel(&mut self, key: &str, rep: ReplyId, cancel: bool, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get(key) else { return reply(fx, rep, TransferAnswer::Err(Fault::NotFound)) };
        let (stage, batch, has_upload) = (transfer.stage, transfer.batch, transfer.upload_id.is_some());
        let ok = if cancel { TransferAnswer::Cancelled(true) } else { TransferAnswer::Ok };
        match stage {
            TransferStage::Done => reply(fx, rep, TransferAnswer::Err(Fault::NotFound)),
            TransferStage::Copying => {
                reply(fx, rep, ok);
                self.end_transfer(key, fx);
                run(fx, TransferEffect::CopyCancel { key: key.into() });
                run(fx, TransferEffect::DeleteFile { key: key.into() });
            }
            TransferStage::Ready => {
                reply(fx, rep, ok);
                self.end_transfer(key, fx);
                Self::delete_all(key, fx);
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
                    run(fx, TransferEffect::Abort { key: key.into() });
                }
                Self::delete_all(key, fx);
                self.start_in_order(fx);
            }
            TransferStage::Mutating if cancel => reply(fx, rep, TransferAnswer::Cancelled(false).or_in_flight(batch)),
            TransferStage::Mutating => run(fx, TransferEffect::DiscardMutation { key: key.into(), reply: rep }),
            TransferStage::Failed => {
                reply(fx, rep, ok);
                self.end_transfer(key, fx);
                if has_upload {
                    run(fx, TransferEffect::Abort { key: key.into() });
                }
                Self::delete_all(key, fx);
            }
        }
    }

    fn retry(&mut self, key: &str, rep: ReplyId, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get(key) else { return reply(fx, rep, TransferAnswer::Err(Fault::NotFound)) };
        let stage = transfer.stage;
        if stage == TransferStage::Done {
            return reply(fx, rep, TransferAnswer::Err(Fault::NotFound));
        }
        reply(fx, rep, TransferAnswer::Receipt(Receipt { epoch: self.epoch, op_id: transfer.seq, revision: 0 }));
        if stage != TransferStage::Failed {
            return;
        }
        let all = transfer.confirmed_parts >= transfer.source.parts;
        if all {
            self.set(key, TransferStage::Completing);
            self.transfers.get_mut(key).map(|transfer| transfer.complete_in_flight = true);
            Self::shows(fx, key, Shown::Uploading { progress: 100 });
            self.store(key, StoreKind::Retrying, fx);
            run(fx, TransferEffect::Complete { key: key.into() });
        } else {
            self.set(key, TransferStage::Ready);
            Self::shows(fx, key, Shown::Queued);
            self.store(key, StoreKind::Retrying, fx);
            self.start_in_order(fx);
        }
    }

    fn online_changed(&mut self, on: bool, fx: &mut Fx) {
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
                    run(fx, TransferEffect::Complete { key: key.clone() });
                }
                (TransferStage::Failed, true) => self.schedule_expire(fx),
                _ => {}
            }
        }
        self.start_in_order(fx);
    }

    fn expire(&mut self, now: Time, fx: &mut Fx) {
        self.now = self.now.max(now);
        let expired: Vec<Key> = self.transfers.values().filter(|transfer| transfer.stage == TransferStage::Failed && self.now.0.saturating_sub(transfer.created_at.0) > EXPIRY_MS).map(|transfer| transfer.key.clone()).collect();
        for key in expired {
            let has_upload = self.transfers[&key].upload_id.is_some();
            self.end_transfer(&key, fx);
            if has_upload {
                run(fx, TransferEffect::Abort { key: key.clone() });
            }
            Self::delete_all(&key, fx);
        }
    }

    fn interrupt_all(&mut self, closing: bool, fx: &mut Fx) {
        let open: Vec<(Key, TransferStage)> = self.transfers.values().filter(|transfer| transfer.stage != TransferStage::Done).map(|transfer| (transfer.key.clone(), transfer.stage)).collect();
        for (key, stage) in open {
            if closing && stage == TransferStage::Copying {
                run(fx, TransferEffect::CopyCancel { key: key.clone() });
                run(fx, TransferEffect::DeleteFile { key: key.clone() });
            } else {
                run(fx, TransferEffect::CancelStep { key: key.clone() });
            }
            self.set(&key, TransferStage::Done);
        }
    }

    fn round_started(&mut self, round_id: RoundId, epoch: Epoch, fx: &mut Fx) {
        if let RoundState::Requested { epoch: requested } = self.round {
            if epoch == self.epoch && requested == epoch {
                self.round = RoundState::Running { round_id, epoch, next: None };
            } else {
                run(fx, TransferEffect::EndJob { round_id });
            }
        }
    }

    fn round_refused(&mut self, fx: &mut Fx) {
        if let RoundState::Requested { .. } = self.round {
            self.round = RoundState::None;
            let first = self.transfers.values().filter(|transfer| matches!(transfer.stage, TransferStage::Ready | TransferStage::Uploading)).min_by_key(|transfer| transfer.seq).map(|transfer| transfer.key.clone());
            if let Some(key) = first {
                run(fx, TransferEffect::Refused { key });
            }
        }
    }

    fn round_ended(&mut self, round_id: RoundId, epoch: Epoch, fx: &mut Fx) {
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

impl TransferAnswer {
    fn or_in_flight(self, batch: bool) -> Self { if batch { Self::Err(Fault::InFlight) } else { self } }
}

pub struct Transfers;

impl StateOwner for Transfers {
    const EPOCHED: bool = true;

    type State = TransferState;
    type Input = TransferInput;
    type Effect = TransferEffect;

    fn step(state: &mut TransferState, input: TransferInput) -> Effects<Self> {
        let mut fx: Fx = Vec::new();
        match input {
            TransferInput::Accept { epoch, key, source, batch, reply, now } => state.accept(epoch, key, source, batch, reply, now, &mut fx),
            TransferInput::Restored(rows) => state.restored(rows, &mut fx),
            TransferInput::CopyDone { key } => {
                if state.stage_of(&key) == Some(TransferStage::Copying) {
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
            TransferInput::StepOk { key, part } => state.step_ok(&key, part, &mut fx),
            TransferInput::StepError { key, part, temporary } => state.step_error(&key, part, temporary, &mut fx),
            TransferInput::Completed { key } => {
                if state.stage_of(&key) == Some(TransferStage::Completing) {
                    state.set(&key, TransferStage::Mutating);
                    state.store(&key, StoreKind::Completed, &mut fx);
                    run(&mut fx, TransferEffect::SubmitMutation { key });
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
