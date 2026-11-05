//! Owner `Transfer`: file upload steps and the background round.
//! One instance per epoch with an identity. Types of the table: inputs, effects, stages. The rows are in `step`.

use std::collections::BTreeMap;

use super::outbox::{Fault, NewOp, Outbox, OutboxOutcome, Receipt};
use crate::files::Location;
use crate::owners::contract::ReplyId;
use crate::owners::contract::{BatchId, Committed, Drained, EpochEnded, EpochStarted, Online, Rejected};
use crate::owners::store::{StoreWriter, UploadKind, UploadRow};

use crate::runtime::{Address, Effect, Effects, Epoch, Time};
use crate::transport::NetworkPurpose;

/// The reply id of the final mutation Transfer hands to Outbox: Transfer waits for `MutationOutcome`, not for a reply.
pub const NO_REPLY: ReplyId = ReplyId::MAX;
/// Timers of the retry of a step use ids above the ids of the expiry timers.
pub(super) const RETRY_TIMER: u64 = 1 << 32;

/// The owners Transfer sends to.
#[derive(Clone, Copy, Debug)]
pub struct TransferPeers {
    pub store: Address<StoreWriter>,
    pub outbox: Address<Outbox>,
}

/// The id of the copy of `key` in the file directory.
pub fn file_id(key: &str) -> String { key.replace(':', "_") }

/// The description of the steps of a file: the request the codec of the executor builds its steps from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Steps {
    pub path: String,
    pub body: String,
}

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
    pub place: Location,
    pub size: u64,
    pub sha256: String,
    pub mutation: NewOp,
    pub steps: Steps,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransferTick {
    Retry { key: Key },
    Expire,
}

#[derive(Clone, Debug)]
pub enum TransferInput {
    Accept { epoch: Epoch, key: Key, source: Source, batch: bool, reply: ReplyId, now: Time, mutation: NewOp, steps: Steps },
    Restored(Vec<StoredTransfer>),
    RestoreFailed { error: String },
    /// The answer of Store to a batch of Transfer.
    Committed(Committed),
    Rejected(Rejected),
    CopyDone { key: Key, size: u64, sha256: String, place: Location },
    CopyFailed { key: Key, error: String },
    StepOk { key: Key, part: Option<u32>, upload_id: Option<String> },
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
    /// Remove the row of `sync_upload`, the operation and the inline bytes.
    Removed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransferEffect {
    /// Copy `source` into the file directory in chunks of 1 MiB.
    Copy { key: Key, source: Source },
    CopyCancel { key: Key },
    /// A step of the file (`part`, `blob`) with `NetworkPurpose::Transfer`.
    Step { key: Key, part: Option<u32>, purpose: NetworkPurpose, upload_id: Option<String>, source: Source },
    Complete { key: Key, upload_id: Option<String> },
    Abort { key: Key, upload_id: Option<String> },
    CancelStep { key: Key },
    /// The file `path` of the file directory.
    DeleteFile { path: String },
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
    /// `None` while the source is copying.
    pub place: Option<Location>,
    pub size: u64,
    pub sha256: String,
    pub mutation: NewOp,
    pub steps: Steps,
}

#[derive(Debug)]
pub struct TransferState {
    pub(super) peers: TransferPeers,
    pub epoch: Epoch,
    pub online: bool,
    pub slots: u32,
    pub transfers: BTreeMap<Key, Transfer>,
    pub round: RoundState,
    pub next_seq: u64,
    pub next_batch: BatchId,
    /// The transfer each batch in the Store is for.
    pub(super) batches: BTreeMap<BatchId, Key>,
    pub(super) next_timer: u64,
    pub now: Time,
    pub drained: bool,
    pub(super) queue_error: Option<String>,
}

impl TransferState {
    pub fn new(epoch: Epoch, slots: u32, peers: TransferPeers) -> Self { Self { peers, epoch, online: true, slots, transfers: BTreeMap::new(), round: RoundState::None, next_seq: 1, next_batch: 1, batches: BTreeMap::new(), next_timer: 1, now: Time(0), drained: false, queue_error: None } }

    pub fn stage_of(&self, key: &str) -> Option<TransferStage> { self.transfers.get(key).map(|transfer| transfer.stage) }
}

/// The row of `sync_upload` a transfer is in the base after a batch of `kind`.
pub(super) fn upload_row(transfer: &Transfer, kind: StoreKind) -> UploadRow {
    UploadRow {
        key: transfer.key.clone(),
        seq: transfer.seq,
        source: transfer.source.path.clone(),
        parts: transfer.source.parts,
        batch: transfer.batch,
        upload_id: transfer.upload_id.clone(),
        confirmed_parts: transfer.confirmed_parts,
        completed: matches!(kind, StoreKind::Completed) || transfer.stage == TransferStage::Mutating,
        failed: if kind == StoreKind::Failed { transfer.error.clone().or_else(|| Some("failed".into())) } else { transfer.error.clone().filter(|_| kind != StoreKind::Retrying) },
        created_at: transfer.created_at.0,
        inline: transfer.place == Some(Location::Table),
        size: transfer.size,
        sha256: transfer.sha256.clone(),
        path: transfer.mutation.path.clone(),
        body: transfer.mutation.body.clone(),
        on_failure: transfer.mutation.on_failure,
        steps_path: transfer.steps.path.clone(),
        steps_body: transfer.steps.body.clone(),
    }
}

pub(super) fn upload_kind(kind: StoreKind) -> UploadKind {
    match kind {
        StoreKind::SyncUpload | StoreKind::Removed => UploadKind::SyncUpload,
        StoreKind::ConfirmedParts => UploadKind::ConfirmedParts,
        StoreKind::Completed => UploadKind::Completed,
        StoreKind::Failed => UploadKind::Failed,
        StoreKind::Retrying => UploadKind::Retrying,
    }
}

/// A failed transfer older than 7 days is discarded.
pub(super) const EXPIRY_MS: u64 = 7 * 86_400_000;
/// Attempts of a step before the permanent error (`design.md` 1a).
pub(super) const ATTEMPT_LIMIT: u32 = 50;

pub(super) type Fx = Effects<super::transfer::Transfers>;

pub(super) fn run(fx: &mut Fx, effect: TransferEffect) { fx.push(Effect::Run(effect)); }

pub(super) fn reply(fx: &mut Fx, reply: ReplyId, answer: TransferAnswer) { run(fx, TransferEffect::Reply { reply, answer }); }
