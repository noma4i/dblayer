//! Owner `Transfer`: the file upload steps. One instance per epoch with an identity.
//! Types of the owner: values, inputs, effects, states. The rows are in `transfer_{open,accept,steps,end}`.

use std::collections::{BTreeMap, BTreeSet};

use super::outbox::{Fault, NewOp, Outbox, OutboxOutcome, Receipt};
use super::outbox_types::{OpId, Phase};
use super::rounds::Rounds;
use crate::store::files::Location;
use crate::owners::contract::{BatchId, Committed, Drained, EpochEnded, EpochStarted, ModelsReset, Online, Rejected, ReplyId};
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

/// The id of the copy of a transfer in the file directory: it never holds a character of the key.
pub fn file_id(transfer_id: TransferId) -> String { format!("transfers/{transfer_id}") }

/// The request the codec of the executor builds the steps of a file from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Steps {
    pub path: String,
    pub body: String,
}

/// What the copy of the source produced.
pub struct CopiedBlob {
    pub size: u64,
    pub sha256: String,
    pub place: Location,
    pub cached: CachedBlob,
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
    pub batch: Option<u64>,
    pub fields: serde_json::Map<String, serde_json::Value>,
    pub upload_id: Option<String>,
    pub confirmed_parts: u32,
    pub attempts: u32,
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
    pub pending_step: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransferTick {
    Retry { transfer_id: TransferId, attempt_id: AttemptId },
    PutTimeout { transfer_id: TransferId, attempt_id: AttemptId },
    Expire,
}

/// The position and the size of the batch a row of an element carries: `renumber` rewrites them when an element leaves.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Renumber {
    /// The paths in the row of the element.
    pub position: String,
    pub size: String,
    /// The keys in the fields of the element.
    pub position_target: String,
    pub size_target: String,
    pub start: usize,
}

/// What the owner needs to send the elements of a batch as one mutation: the call with at least `minimum` elements goes in one request, fewer go one by one.
#[derive(Clone, Debug)]
pub struct Joint {
    pub minimum: usize,
    pub maximum: usize,
    /// The variable of the mutation that holds the list of the element fields.
    pub items_key: String,
    /// The request of the mutation of the batch: the body holds the variables of the batch without the list.
    pub mutation: NewOp,
    pub renumber: Option<Renumber>,
}

impl Joint {
    /// The text kept in the base: the rule of a batch survives a restart.
    pub fn to_text(&self) -> String {
        let op = &self.mutation;
        let renumber = self.renumber.as_ref().map(|r| serde_json::json!({"position": r.position, "size": r.size, "position_target": r.position_target, "size_target": r.size_target, "start": r.start}));
        serde_json::json!({"minimum": self.minimum, "maximum": self.maximum, "items_key": self.items_key, "renumber": renumber,
            "mutation": {"model": op.model, "row_id": op.row_id, "action": op.action, "keep": op.on_failure == crate::schema::OnFailure::Keep, "path": op.path, "body": op.body}}).to_string()
    }

    pub fn from_text(text: &str) -> Option<Self> {
        let value: serde_json::Value = serde_json::from_str(text).ok()?;
        let (mutation, text_of) = (value.get("mutation")?, |value: &serde_json::Value, key: &str| value.get(key).and_then(serde_json::Value::as_str).map(str::to_string));
        let renumber = value.get("renumber").filter(|found| !found.is_null()).map(|r| Some(Renumber { position: text_of(r, "position")?, size: text_of(r, "size")?, position_target: text_of(r, "position_target")?, size_target: text_of(r, "size_target")?, start: usize::try_from(r.get("start")?.as_u64()?).ok()? })).unwrap_or(None);
        let op = NewOp {
            model: text_of(mutation, "model")?,
            row_id: text_of(mutation, "row_id")?,
            action: text_of(mutation, "action")?,
            on_failure: if mutation.get("keep")?.as_bool()? { crate::schema::OnFailure::Keep } else { crate::schema::OnFailure::Rollback },
            origin: super::outbox::OpSource::Api,
            path: text_of(mutation, "path")?,
            body: text_of(mutation, "body")?,
            effects: Default::default(),
            created_at: 0, expires_after: None,
        };
        Some(Self { minimum: usize::try_from(value.get("minimum")?.as_u64()?).ok()?, maximum: usize::try_from(value.get("maximum")?.as_u64()?).ok()?, items_key: text_of(&value, "items_key")?, mutation: op, renumber })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BatchPhase {
    Collecting,
    Mutating,
    Singles,
}

#[derive(Clone, Debug)]
pub struct BatchItem {
    pub key: Key,
    /// The fields of the element in the list of the mutation of the batch: `SIGNED_ID_SENTINEL` stands for the signed id.
    pub fields: serde_json::Map<String, serde_json::Value>,
    /// The action asks to go on in power saving.
    pub urgent: bool,
    pub source: Source,
    pub mutation: NewOp,
    pub steps: Steps,
}

#[derive(Clone, Debug)]
pub enum TransferInput {
    Accept { epoch: Epoch, item: Box<BatchItem>, reply: ReplyId, now: Time },
    AcceptBatch { epoch: Epoch, items: Vec<BatchItem>, joint: Box<Joint>, reply: ReplyId, now: Time },
    Restored { uploads: Vec<StoredTransfer>, outcomes: TransferOutcomes, orphans: Vec<(String, String)>, batches: BTreeMap<u64, StoredGroup> },
    /// The answer to `abort` of a removed transfer: `answered` is false for a temporary failure.
    AbortDone { upload_id: String, answered: bool },
    RestoreFailed { error: String },
    RegisterOperations { declarations: Vec<(String, String)> },
    RestoreReady { epoch: Epoch },
    RoundStatus { epoch: Epoch, transfer_ids: Vec<TransferId>, refused: Option<String> },
    CopyDone { transfer_id: TransferId, attempt_id: AttemptId, size: u64, sha256: String, place: Location, cached: CachedBlob },
    CopyFailed { transfer_id: TransferId, attempt_id: AttemptId, error: String },
    /// The answer of Outbox to `ReserveOp`.
    OpReserved { target: super::outbox::ReserveTarget, result: Result<OpId, Fault> },
    StepOk { transfer_id: TransferId, attempt_id: AttemptId, part: Option<u32>, upload_id: Option<String>, etag: Option<String> },
    UploadMinted { transfer_id: TransferId, attempt_id: AttemptId, upload_id: String, key: Key },
    StepError { transfer_id: TransferId, attempt_id: AttemptId, temporary: bool, error: String },
    Completed { transfer_id: TransferId, attempt_id: AttemptId, signed_id: Option<String> },
    CompleteError { transfer_id: TransferId, attempt_id: AttemptId, temporary: bool, error: String },
    MutationOutcome { transfer_id: TransferId, outcome: OutboxOutcome },
    MutationOutcomes { batch_id: BatchId, outcomes: BTreeMap<TransferId, OutboxOutcome>, intent_removed: bool },
    RetryOutcome { cycles: Vec<(TransferId, CycleId)>, result: Result<i64, Fault> },
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
    ModelsReset(ModelsReset),
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
    BatchIntent,
    MixedOutcomes,
    /// Remove the row of `sync_upload`, the parts, the saved outcome and the inline bytes.
    Removed,
    /// Write `upload_id` of a multipart upload the server created for an attempt.
    UploadId,
    /// The orphan record of a multipart upload minted after its transfer was removed.
    LateOrphan,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransferEffect {
    /// Copy `source` into the file directory in chunks of 1 MiB.
    Copy { transfer_id: TransferId, attempt_id: AttemptId, key: Key, source: Source },
    CopyCancel { transfer_id: TransferId },
    /// A step of the file (`part`, `blob`) with `NetworkPurpose::Transfer`.
    Step { transfer_id: TransferId, attempt_id: AttemptId, key: Key, operation: u64, part: Option<u32>, purpose: NetworkPurpose, upload_id: Option<String>, source: Source, steps: Steps, place: Option<Location> },
    Complete { transfer_id: TransferId, attempt_id: AttemptId, key: Key, upload_id: Option<String>, parts: Vec<(u32, Option<String>)>, place: Option<Location> },
    Abort { transfer_id: TransferId, attempt_id: AttemptId, key: Key, upload_id: String },
    /// `abort` of an upload whose transfer is gone: the answer comes back as `AbortDone`.
    AbortOrphan { key: Key, upload_id: String },
    CancelStep { transfer_id: TransferId },
    ReleaseStep { request_id: u64 },
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
    PersistingAttempt,
    Uploading,
    PersistingPart,
    Completing,
    PersistingComplete,
    BatchReady,
    PersistingBatchIntent,
    Mutating,
    Failed,
    Done,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Removal {
    Cancel,
    Confirmed { revision: i64, result: Option<String> },
    Discarded,
    CopyFailed { error: String },
    Storage,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NextStep {
    Part(u32),
    Complete,
    /// Every part is on the server and the mutation is what failed.
    Mutation,
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
    pub fields: serde_json::Map<String, serde_json::Value>,
    pub steps: Steps,
    pub restored: bool,
    pub pending_step: Option<u64>,
    pub urgent: bool,
    /// The operation of the optimistic effects written with the acceptance (`ReserveOp`).
    pub op_id: Option<OpId>,
    /// The final mutation left for Outbox: Outbox owns the operation, a removal does not roll it back.
    pub submitted: bool,
    pub intent_live: bool,
    pub waits_outbox: bool,
    /// The file is already on the server: the cache of blobs gave its `signed_id` (the steps are skipped).
    pub from_cache: bool,
    /// The cache entry of the content is old: the write of the copy removes it.
    pub cache_stale: bool,
    pub round_error: Option<String>,
    pub removal: Option<Removal>,
    pub prior: Option<TransferStage>,
    /// A batch of the Store for the failure mark carries `abort`.
    pub failure_abort: bool,
    pub outcomes: CycleOutcomes,
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
    pub op_id: Option<OpId>,
    pub reply: Option<ReplyId>,
    pub ready: BTreeSet<TransferId>,
    pub joint: Joint,
    pub pending_op: Option<NewOp>,
    pub phase: BatchPhase,
    /// The row the mutation of the batch was submitted for: `Retry` names the operation by it.
    pub op_row: Option<(String, String)>,
}

#[derive(Clone, Debug)]
pub struct StoredGroup {
    pub joint: Joint,
    pub op_id: Option<OpId>,
    pub intent_members: Vec<TransferId>,
}

pub type CycleOutcomes = BTreeMap<CycleId, OutboxOutcome>;
pub type TransferOutcomes = BTreeMap<TransferId, CycleOutcomes>;

#[derive(Clone, Debug)]
pub(super) struct Pending {
    pub ids: Vec<TransferId>,
    pub kind: StoreKind,
    pub orphan: Option<(String, Key)>,
}

#[derive(Debug)]
pub struct TransferState {
    pub(super) peers: TransferPeers,
    pub epoch: Epoch,
    pub(super) base_generation: u64,
    pub(super) generations: BTreeMap<String, u64>,
    pub online: Option<bool>,
    pub saving: bool,
    pub slots: u32,
    pub phase: Phase,
    pub transfers: BTreeMap<TransferId, Transfer>,
    pub completed: std::collections::HashMap<TransferId, CycleOutcomes>,
    pub(super) groups: BTreeMap<u64, Group>,
    pub(super) pending: BTreeMap<BatchId, Pending>,
    pub(super) subscribers: BTreeMap<TransferId, BTreeSet<StreamId>>,
    pub(super) declarations: BTreeSet<(String, String)>,
    /// `upload_id` -> key of the removed transfers whose `abort` the server has not answered.
    pub(super) orphans: BTreeMap<String, Key>,
    pub(super) orphan_flight: BTreeSet<String>,
    /// The orphans whose `Online(true)` or `RestoreReady` came while their abort was in flight.
    pub(super) orphan_owed: BTreeSet<String>,
    /// The restored groups that still have an element without its file on the server.
    pub(super) open_groups: BTreeSet<u64>,
    pub(super) held: Vec<TransferInput>,
    pub(super) waiting: BTreeMap<TransferId, (StoredTransfer, TransferOutcomes)>,
    pub next_id: TransferId,
    pub next_seq: u64,
    pub next_group: u64,
    pub next_batch: BatchId,
    pub(super) next_attempt: AttemptId,
    pub(super) next_timer: u64,
    pub put_timeout: std::time::Duration,
    pub now: Time,
    pub drained: bool,
    pub closing: bool,
    pub(super) ended: bool,
    pub(super) queue_error: Option<String>,
    pub examined: std::sync::atomic::AtomicU64,
}

impl TransferState {
    pub fn new(epoch: Epoch, slots: u32, peers: TransferPeers) -> Self {
        Self {
            peers,
            epoch,
            base_generation: 0,
            generations: BTreeMap::new(),
            online: None,
            saving: false,
            slots,
            phase: Phase::Restoring,
            transfers: BTreeMap::new(),
            completed: std::collections::HashMap::new(),
            groups: BTreeMap::new(),
            pending: BTreeMap::new(),
            subscribers: BTreeMap::new(),
            declarations: BTreeSet::new(),
            orphans: BTreeMap::new(),
            orphan_flight: BTreeSet::new(),
            orphan_owed: BTreeSet::new(),
            open_groups: BTreeSet::new(),
            held: Vec::new(),
            waiting: BTreeMap::new(),
            next_id: 1,
            next_seq: 1,
            next_group: 1,
            next_batch: 1,
            examined: std::sync::atomic::AtomicU64::new(0),
            next_attempt: 1,
            next_timer: 1,
            put_timeout: super::upload::UploadConfig::default().put_timeout,
            now: Time(0),
            drained: false,
            closing: false,
            ended: false,
            queue_error: None,
        }
    }

    pub(super) fn scan(&self) -> impl Iterator<Item = &Transfer> + '_ {
        self.transfers.values().inspect(|_| { self.examined.fetch_add(1, std::sync::atomic::Ordering::Relaxed); })
    }

    pub fn stage_of(&self, id: TransferId) -> Option<TransferStage> { self.transfers.get(&id).map(|transfer| transfer.stage) }

    /// The open transfer of `key`: a `Done` transfer keeps only its outcomes.
    pub fn open_id(&self, key: &str) -> Option<TransferId> { self.scan().find(|transfer| transfer.key == key && transfer.stage != TransferStage::Done).map(|transfer| transfer.transfer_id) }

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
        batch: transfer.batch_id,
        fields: transfer.batch_id.filter(|_| !transfer.fields.is_empty()).map(|_| serde_json::Value::Object(transfer.fields.clone()).to_string()),
        upload_id: if kind == StoreKind::Failed && transfer.failure_abort { None } else { transfer.upload_id.clone() },
        confirmed_parts: if kind == StoreKind::Failed && transfer.failure_abort { 0 } else { transfer.confirmed_parts },
        attempts: transfer.attempts,
        completed: matches!(kind, StoreKind::Completed) || transfer.signed_id.is_some(),
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
        cycle_id: transfer.cycle_id + u64::from(kind == StoreKind::Retrying && transfer.stage == TransferStage::PersistingRetry),
        signed_id: transfer.signed_id.clone(),
        aborted: transfer.aborted,
        op_id: transfer.op_id,
        etags: if kind == StoreKind::Failed && transfer.failure_abort { Vec::new() } else { transfer.etags.clone() },
    }
}
