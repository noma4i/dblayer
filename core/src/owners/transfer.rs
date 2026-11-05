//! Owner `Transfer`: file upload steps and the background round (`specs/machines/transfer.md`).
//! One instance per epoch with an identity. Types of the table: inputs, effects, stages. The rows are in `step`.

use std::collections::BTreeMap;

use super::contract::{BatchId, Drained, EpochEnded, EpochStarted, Online};
use crate::transport::NetworkPurpose;
use super::outbox::{Fault, OutboxOutcome, Receipt, ReplyId};
use crate::runtime::{Cause, Effects, Epoch, StateOwner, Time};

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
}

impl TransferState {
    pub fn new(epoch: Epoch, slots: u32) -> Self { Self { epoch, online: true, slots, transfers: BTreeMap::new(), round: RoundState::None, next_seq: 1, next_batch: 1 } }

    pub fn stage_of(&self, key: &str) -> Option<TransferStage> { self.transfers.get(key).map(|transfer| transfer.stage) }
}

pub struct Transfers;

impl StateOwner for Transfers {
    const EPOCHED: bool = true;

    type State = TransferState;
    type Input = TransferInput;
    type Effect = TransferEffect;

    // TEMPORARY: the rows of the table are written after the contract of direction B lands.
    fn step(_state: &mut TransferState, _input: TransferInput) -> Effects<Self> { Vec::new() }

    fn tick(now: Time) -> TransferInput { TransferInput::Clock(now) }

    fn epoch_ended(epoch: Epoch, cause: Cause) -> TransferInput { TransferInput::EpochEnded(EpochEnded { epoch, cause }) }

    fn closing() -> TransferInput { TransferInput::Closing }
}
