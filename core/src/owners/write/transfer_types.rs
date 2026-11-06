//! Owner `Transfer`: the file upload steps. One instance per epoch with an identity.
//! Types of the owner: values, inputs, effects, states. The rows are in `transfer_{open,accept,steps,end}`.

use std::collections::{BTreeMap, BTreeSet};

use super::outbox::{Fault, NewOp, Outbox, OutboxOutcome, Receipt};
use super::outbox_types::{OpId, Phase};
use super::rounds::Rounds;
use crate::files::Location;
use crate::owners::contract::{BatchId, Committed, Drained, EpochEnded, EpochStarted, Online, Rejected, ReplyId};
use crate::owners::store::{StoreWriter, UploadRow};
use crate::runtime::{Address, Effect, Effects, Epoch, Time};
use crate::transport::NetworkPurpose;

/// The reply id of the final mutation Transfer hands to Outbox: Transfer waits for `MutationOutcome`, not for a reply.
pub const NO_REPLY: ReplyId = ReplyId::MAX;
/// Where the signed id of the file goes in the body of the final mutation: Transfer puts the id there when the file is on the server.
pub const SIGNED_ID_SENTINEL: &str = "__dblayer_signed_id__";
pub const EXPIRY_MS: u64 = 7 * 86_400_000;
pub const ATTEMPT_LIMIT: u32 = 50;
pub(super) const RETRY_TIMER: u64 = 1 << 32;

pub type Key = String;
pub type TransferId = u64;
pub use crate::owners::session::AttemptId;
pub type CycleId = u64;
pub use crate::owners::read::deliveries::StreamId;
/// The sink of `uploads[action]`: the key and what it shows.
pub type ShowSink = std::sync::Arc<dyn Fn(&str, Shown) + Send + Sync>;

#[derive(Clone, Copy, Debug)]
pub struct TransferPeers {
    pub store: Address<StoreWriter>,
    pub outbox: Address<Outbox>,
    pub rounds: Option<Address<Rounds>>,
}

/// The id of the copy of `key` in the file directory.
pub fn file_id(key: &str) -> String { key.replace(':', "_") }

/// The request the codec of the executor builds the steps of a file from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Steps {
    pub path: String,
    pub body: String,
}

/// What the cache of blobs holds for the content of a copy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CachedBlob {
    Fresh { blob_id: String },
    Stale,
    Miss,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Source {
    pub path: String,
    pub parts: u32,
}

/// A row of `sync_upload` kept in the base.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredTransfer {
    pub transfer_id: TransferId,
    pub key: Key,
    pub seq: u64,
    pub source: Source,
    pub batch: bool,
    pub upload_id: Option<String>,
    pub confirmed_parts: u32,
    pub etags: Vec<Option<String>>,
    pub completed: bool,
    pub signed_id: Option<String>,
    pub failed: Option<String>,
    pub aborted: bool,
    pub op_id: Option<OpId>,
    pub copying: bool,
    pub file_present: bool,
    pub created_at: Time,
    pub place: Location,
    pub size: u64,
    pub sha256: String,
    pub cycle_id: CycleId,
    pub has_intent: bool,
    pub mutation: NewOp,
    pub steps: Steps,
    /// The row was dropped by the layout move or cannot be read.
    pub restore_failed: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransferTick {
    Retry { transfer_id: TransferId, attempt_id: AttemptId },
    Expire,
}

#[derive(Clone, Debug)]
pub struct BatchItem {
    pub key: Key,
    /// The action asks to go on in power saving.
    pub urgent: bool,
    pub source: Source,
    pub mutation: NewOp,
    pub steps: Steps,
}

#[derive(Clone, Debug)]
pub enum TransferInput {
    Accept { epoch: Epoch, item: Box<BatchItem>, reply: ReplyId, now: Time },
    AcceptBatch { epoch: Epoch, items: Vec<BatchItem>, reply: ReplyId, now: Time },
    Restored { uploads: Vec<StoredTransfer>, outcomes: BTreeMap<TransferId, OutboxOutcome> },
    RestoreFailed { error: String },
    RegisterOperations { declarations: Vec<(String, String)> },
    RestoreReady { epoch: Epoch },
    RoundStatus { epoch: Epoch, transfer_ids: Vec<TransferId>, refused: Option<String> },
    CopyDone { transfer_id: TransferId, attempt_id: AttemptId, size: u64, sha256: String, place: Location, cached: CachedBlob },
    CopyFailed { transfer_id: TransferId, attempt_id: AttemptId, error: String },
    /// The answer of Outbox to `ReserveOp`.
    OpReserved { transfer_id: TransferId, result: Result<OpId, Fault> },
    StepOk { transfer_id: TransferId, attempt_id: AttemptId, part: Option<u32>, upload_id: Option<String>, etag: Option<String> },
    StepError { transfer_id: TransferId, attempt_id: AttemptId, temporary: bool },
    Completed { transfer_id: TransferId, attempt_id: AttemptId, signed_id: Option<String> },
    CompleteError { transfer_id: TransferId, attempt_id: AttemptId, temporary: bool },
    MutationOutcome { key: Key, outcome: OutboxOutcome },
    DiscardOutcome { key: Key, result: Result<(), Fault> },
    Outcome { receipt: Receipt, reply: ReplyId },
    InterestPlus { transfer_id: TransferId, stream_id: StreamId },
    InterestMinus { transfer_id: TransferId, stream_id: StreamId },
    Committed(Committed),
    Rejected(Rejected),
    Cancel { key: Key, reply: ReplyId },
    Retry { key: Key, source: Option<Source>, reply: ReplyId },
    Discard { key: Key, reply: ReplyId },
    SlotFreed,
    Online(Online),
    PowerSaving(bool),
    EpochStarted(EpochStarted),
    Tick { now: Time, kind: TransferTick },
    EpochEnded(EpochEnded),
    Closing,
    /// The trait tick: the clock of the owner.
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
    Receipts(Vec<Receipt>),
    Outcome(OutboxOutcome),
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
    /// Remove the row of `sync_upload`, the parts, the saved outcome and the inline bytes.
    Removed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransferEffect {
    /// Copy `source` into the file directory in chunks of 1 MiB.
    Copy { transfer_id: TransferId, attempt_id: AttemptId, key: Key, source: Source },
    CopyCancel { transfer_id: TransferId },
    /// A step of the file (`part`, `blob`) with `NetworkPurpose::Transfer`.
    Step { transfer_id: TransferId, attempt_id: AttemptId, key: Key, part: Option<u32>, purpose: NetworkPurpose, upload_id: Option<String>, source: Source, steps: Steps, place: Option<Location> },
    Complete { transfer_id: TransferId, attempt_id: AttemptId, key: Key, upload_id: Option<String>, parts: Vec<(u32, Option<String>)>, place: Option<Location> },
    Abort { transfer_id: TransferId, attempt_id: AttemptId, key: Key, upload_id: String },
    CancelStep { transfer_id: TransferId },
    DeleteFile { path: String },
    Reply { reply: ReplyId, answer: TransferAnswer },
    /// `uploads[action]`.
    Shows { key: Key, shown: Shown },
    /// `uploads[action].error = refused` (`None` clears it).
    Refused { key: Key, reason: Option<String> },
    /// Deliveries `TransferStatus` for the stream `stream_id`.
    Status { stream_id: StreamId, transfer_id: TransferId, shown: Shown },
    Started { epoch: Epoch },
    SnapshotRequest { epoch: Epoch },
    RestoreClassified { epoch: Epoch },
    Drained(Drained),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransferStage {
    WaitingDeclaration,
    Reserving,
    Accepting,
    Copying,
    CopyReady,
    PersistingCopy,
    PersistingRetry,
    PersistingFailure,
    Removing,
    Ready,
    Uploading,
    PersistingPart,
    Completing,
    PersistingComplete,
    Mutating,
    Failed,
    Done,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Removal {
    Cancel,
    Confirmed { revision: i64 },
    Discarded,
    CopyFailed { error: String },
    Storage,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NextStep {
    Part(u32),
    Complete,
}

#[derive(Clone, Debug)]
pub struct Transfer {
    pub transfer_id: TransferId,
    pub key: Key,
    pub seq: u64,
    pub source: Source,
    pub batch_id: Option<BatchId>,
    pub epoch: Epoch,
    pub stage: TransferStage,
    pub attempt_id: AttemptId,
    pub attempts: u32,
    pub in_flight: bool,
    pub waiting_retry: bool,
    /// The retry timer fired while no slot was free.
    pub retry_due: bool,
    pub next_step: NextStep,
    pub upload_id: Option<String>,
    pub confirmed_parts: u32,
    pub etags: Vec<Option<String>>,
    pub last_part: bool,
    pub aborted: bool,
    pub signed_id: Option<String>,
    pub created_at: Time,
    pub error: Option<String>,
    pub cycle_id: CycleId,
    pub place: Option<Location>,
    pub size: u64,
    pub sha256: String,
    pub mutation: NewOp,
    pub steps: Steps,
    pub restored: bool,
    pub urgent: bool,
    /// The operation of the optimistic effects written with the acceptance (`ReserveOp`).
    pub op_id: Option<OpId>,
    /// The final mutation left for Outbox: Outbox owns the operation, a removal does not roll it back.
    pub submitted: bool,
    /// The file is already on the server: the cache of blobs gave its `signed_id` (the steps are skipped).
    pub from_cache: bool,
    /// The cache entry of the content is old: the write of the copy removes it.
    pub cache_stale: bool,
    pub round_error: Option<String>,
    pub removal: Option<Removal>,
    pub prior: Option<TransferStage>,
    /// A batch of the Store for the failure mark carries `abort`.
    pub failure_abort: bool,
    pub outcomes: BTreeMap<CycleId, OutboxOutcome>,
    pub outcome_waiters: Vec<(CycleId, ReplyId)>,
    pub accept_reply: Option<ReplyId>,
    pub cancel_reply: Option<(ReplyId, bool)>,
    pub retry_reply: Option<ReplyId>,
    pub discard_waiters: Vec<ReplyId>,
    pub discard_sent: bool,
    pub new_source: Option<Source>,
    pub closing: bool,
}

/// The elements of one `AcceptBatch`: one reply, N receipts after the common `Committed`.
#[derive(Clone, Debug)]
pub struct Group {
    pub ids: Vec<TransferId>,
    pub reply: Option<ReplyId>,
    pub ready: BTreeSet<TransferId>,
}

#[derive(Clone, Debug)]
pub(super) struct Pending {
    pub ids: Vec<TransferId>,
    pub kind: StoreKind,
}

#[derive(Debug)]
pub struct TransferState {
    pub(super) peers: TransferPeers,
    pub epoch: Epoch,
    pub online: Option<bool>,
    pub saving: bool,
    pub slots: u32,
    pub phase: Phase,
    pub transfers: BTreeMap<TransferId, Transfer>,
    pub(super) groups: BTreeMap<u64, Group>,
    pub(super) pending: BTreeMap<BatchId, Pending>,
    pub(super) subscribers: BTreeMap<TransferId, BTreeSet<StreamId>>,
    pub(super) declarations: BTreeSet<(String, String)>,
    pub(super) held: Vec<TransferInput>,
    pub(super) waiting: BTreeMap<TransferId, (StoredTransfer, BTreeMap<TransferId, OutboxOutcome>)>,
    pub next_id: TransferId,
    pub next_seq: u64,
    pub next_group: u64,
    pub next_batch: BatchId,
    pub(super) next_attempt: AttemptId,
    pub(super) next_timer: u64,
    pub now: Time,
    pub drained: bool,
    pub closing: bool,
    pub(super) ended: bool,
    pub(super) queue_error: Option<String>,
}

impl TransferState {
    pub fn new(epoch: Epoch, slots: u32, peers: TransferPeers) -> Self {
        Self {
            peers,
            epoch,
            online: None,
            saving: false,
            slots,
            phase: Phase::Restoring,
            transfers: BTreeMap::new(),
            groups: BTreeMap::new(),
            pending: BTreeMap::new(),
            subscribers: BTreeMap::new(),
            declarations: BTreeSet::new(),
            held: Vec::new(),
            waiting: BTreeMap::new(),
            next_id: 1,
            next_seq: 1,
            next_group: 1,
            next_batch: 1,
            next_attempt: 1,
            next_timer: 1,
            now: Time(0),
            drained: false,
            closing: false,
            ended: false,
            queue_error: None,
        }
    }

    pub fn stage_of(&self, id: TransferId) -> Option<TransferStage> { self.transfers.get(&id).map(|transfer| transfer.stage) }

    /// The open transfer of `key`: a `Done` transfer keeps only its outcomes.
    pub fn open_id(&self, key: &str) -> Option<TransferId> { self.transfers.values().find(|transfer| transfer.key == key && transfer.stage != TransferStage::Done).map(|transfer| transfer.transfer_id) }

    pub(super) fn attempt(&mut self) -> AttemptId {
        self.next_attempt += 1;
        self.next_attempt - 1
    }
}

pub(super) type Fx = Effects<super::transfer::Transfers>;

pub(super) fn run(fx: &mut Fx, effect: TransferEffect) { fx.push(Effect::Run(effect)); }

pub(super) fn reply(fx: &mut Fx, reply: ReplyId, answer: TransferAnswer) { run(fx, TransferEffect::Reply { reply, answer }); }

/// The row of `sync_upload` of a transfer after a batch of `kind`.
pub(super) fn upload_row(transfer: &Transfer, kind: StoreKind) -> UploadRow {
    UploadRow {
        key: transfer.key.clone(),
        seq: transfer.seq,
        source: transfer.source.path.clone(),
        parts: transfer.source.parts,
        batch: transfer.batch_id.is_some(),
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
        transfer_id: transfer.transfer_id,
        cycle_id: transfer.cycle_id,
        signed_id: transfer.signed_id.clone(),
        aborted: transfer.aborted,
        op_id: transfer.op_id,
        etags: transfer.etags.clone(),
    }
}
