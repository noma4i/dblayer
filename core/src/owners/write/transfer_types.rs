//! Owner `Transfer`: the file upload steps. One instance per epoch with an identity.
//! Types of the owner: values, inputs, effects, states. The rows are in `transfer_{open,accept,steps,end}`.

use std::collections::{BTreeMap, BTreeSet};

use super::outbox::{Fault, NewOp, Outbox, OutboxOutcome, Receipt};
use super::outbox_types::{OpId, Phase};
use super::rounds::Rounds;
use crate::store::files::Location;
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
/// The sink of a `Blob` stream: the stream and what its key shows.
pub type StatusSink = std::sync::Arc<dyn Fn(StreamId, Shown, i64) + Send + Sync>;

#[derive(Clone, Copy, Debug)]
pub struct TransferPeers {
    pub store: Address<StoreWriter>,
    pub outbox: Address<Outbox>,
    pub rounds: Option<Address<Rounds>>,
}

/// The id of the copy of a transfer in the file directory: it never holds a character of the key.
pub fn file_id(transfer_id: TransferId) -> String { format!("transfers/{transfer_id}") }

/// The temporary file of the new copy of a retry with a new source: it replaces `file_id` after the `Committed` of the metadata.
pub fn staged_file_id(transfer_id: TransferId) -> String { format!("{}.staged", file_id(transfer_id)) }

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

#[derive(Clone, Debug)]
pub struct SuppliedSource {
    pub source: Source,
    pub steps: Steps,
    pub values: crate::owners::store::IntentEffects,
    /// The local fields of the values as a JSON object, written by the transaction of the copy.
    pub local: String,
    pub variables: serde_json::Map<String, serde_json::Value>,
    pub wrap: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ActiveSource {
    pub request: SuppliedSource,
    pub body: String,
}

/// A row of `sync_upload` kept in the base.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredTransfer {
    pub transfer_id: TransferId,
    pub key: Key,
    pub seq: u64,
    pub source: Option<Source>,
    pub awaiting_source: bool,
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
    pub has_operation: bool,
    pub mutation: NewOp,
    pub steps: Option<Steps>,
    /// The row was dropped by the layout move or cannot be read.
    pub staged: bool,
    pub failed_at: Option<Time>,
    pub blob_created_at: Option<Time>,
    pub restore_failed: Option<String>,
    pub restore_element: Option<(String, Option<TransferId>)>,
    pub pending_step: Option<u64>,
    /// The answer of the step Session saved before the process died: `(request id, body)`.
    pub saved_answer: Option<(u64, crate::store::session_state::StoredAnswer)>,
}

/// The answer of the saved pair of a step under its request id: the response, or the refusal Session kept.
pub type SeededAnswer = (u64, Result<(String, crate::transport::AnswerKind), crate::owners::session::SessionError>);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransferTick {
    Retry { transfer_id: TransferId, attempt_id: AttemptId },
    PutTimeout { transfer_id: TransferId, attempt_id: AttemptId },
    /// The write of a minted `upload_id` the Store refused is due again.
    Minted { upload_id: String, attempt_id: AttemptId },
    /// The write of the old row after a failed swap, which the Store refused, is due again.
    StagedRestore { transfer_id: TransferId },
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

/// Whether a batch of `count` elements is inside the domain of its declaration: the one predicate of the size of a batch.
pub fn batch_size_admitted((minimum, maximum): (usize, usize), count: usize) -> bool { count >= minimum && count <= maximum }

impl Joint {
    pub fn admits(&self, count: usize) -> bool { batch_size_admitted((self.minimum, self.maximum), count) }

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
            expires_after: None,
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
    pub source: Option<Source>,
    pub mutation: NewOp,
    pub steps: Option<Steps>,
}

/// The refusal of an `abort`: `status` is the HTTP status of the answer, `None` when the call dropped before an answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AbortFault {
    pub temporary: bool,
    pub status: Option<u16>,
}

#[derive(Clone, Debug)]
pub enum TransferInput {
    Accept { epoch: Epoch, item: Box<BatchItem>, reply: ReplyId, now: Time },
    AcceptBatch { epoch: Epoch, items: Vec<BatchItem>, joint: Box<Joint>, reply: ReplyId, now: Time },
    Restored { uploads: Vec<StoredTransfer>, outcomes: TransferOutcomes, orphans: Vec<(String, String)>, batches: BTreeMap<u64, StoredGroup>, held_orphan: Option<String> },
    ReservedFloor { transfer_id: TransferId },
    /// The answer to `abort` of a removed transfer: `Ok` when the server removed the upload or has none, else the refusal.
    AbortDone { upload_id: String, result: Result<(), AbortFault> },
    RestoreFailed { error: String },
    RegisterOperations { declarations: Vec<(String, String)> },
    RestoreReady { epoch: Epoch },
    RoundStatus { epoch: Epoch, transfer_ids: Vec<TransferId>, refused: Option<String> },
    CopyDone { transfer_id: TransferId, attempt_id: AttemptId, size: u64, sha256: String, place: Location, cached: CachedBlob },
    CopyFailed { transfer_id: TransferId, attempt_id: AttemptId, error: String },
    StagedPromoted { transfer_id: TransferId },
    /// The answer of the pair that Session re-sent for a restored step: the transfer starts its attempt, the executor serves the answer to the first request of the step.
    StepAnswer { request_id: u64, outcome: crate::owners::session::AuthorOutcome },
    StagedPromoteFailed { transfer_id: TransferId, detail: String },
    /// The answer of Outbox to `ReserveOp`.
    OpReserved { target: super::outbox::ReserveTarget, result: Result<OpId, Fault> },
    ReplacementReserved { target: super::outbox::ReserveTarget, result: Result<(OpId, Option<super::outbox::ReplacementOperation>), Fault> },
    StepOk { transfer_id: TransferId, attempt_id: AttemptId, part: Option<u32>, upload_id: Option<String>, etag: Option<String> },
    UploadMinted { transfer_id: TransferId, attempt_id: AttemptId, upload_id: String, key: Key },
    StepError { transfer_id: TransferId, attempt_id: AttemptId, temporary: bool, error: String },
    Completed { transfer_id: TransferId, attempt_id: AttemptId, signed_id: Option<String> },
    CompleteError { transfer_id: TransferId, attempt_id: AttemptId, temporary: bool, error: String },
    MutationOutcome { transfer_id: TransferId, cycle_id: CycleId, outcome: OutboxOutcome, intent_removed: bool },
    MutationOutcomes { batch_id: BatchId, members: Vec<(TransferId, CycleId, OutboxOutcome)>, intent_removed: bool },
    RetryOutcome { cycles: Vec<(TransferId, CycleId)>, result: Result<i64, Fault> },
    DiscardOutcome { key: Key, result: Result<(), Fault> },
    Outcome { receipt: Receipt, reply: ReplyId, discard_on_failure: bool },
    /// A `Blob` stream follows the key `model:row:action`, also before a transfer exists.
    StatusInterestPlus { key: Key, stream_id: StreamId },
    StatusInterestMinus { key: Key, stream_id: StreamId },
    Committed(Committed),
    Rejected(Rejected),
    Cancel { key: Key, reply: ReplyId },
    SupplySource { epoch: Epoch, key: Key, source: SuppliedSource, reply: ReplyId },
    SourceFailed { epoch: Epoch, key: Key, message: String, reply: ReplyId },
    Retry { key: Key, source: Option<SuppliedSource>, reply: ReplyId },
    Discard { key: Key, reply: ReplyId },
    /// A `retry` or `discard` without an action: `rows` are the names of the row, `outbox_min` the smallest open operation of the row in Outbox.
    SelectRow { model: String, rows: Vec<String>, outbox_min: Option<(OpId, String)>, kind: super::outbox_types::SelectKind, reply: ReplyId, version: u64 },
    SelectConfirmed { reply: ReplyId },
    SlotFreed,
    Online(Online),
    PowerSaving(bool),
    EpochStarted(EpochStarted),
    Tick { now: Time, kind: TransferTick },
    EpochEnded(EpochEnded),
    Closing,
}

/// What `uploads[action]` shows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Shown {
    Copying,
    WaitingSource,
    Queued,
    Uploading { progress: u32 },
    Failed { error: String },
    Gone,
}

impl Shown {
    /// What the shown value writes to `sync_upload`: two values that give the same columns write once.
    pub fn columns(&self) -> (u8, bool) {
        match self {
            Self::Copying => (0, false),
            Self::WaitingSource => (1, false),
            Self::Queued => (2, false),
            Self::Uploading { progress } => (3, *progress >= 100),
            Self::Failed { .. } => (4, false),
            Self::Gone => (5, false),
        }
    }

    pub fn wait_reason(&self) -> Option<crate::owners::write::upload::WaitReason> {
        match self {
            Self::WaitingSource => Some(crate::owners::write::upload::WaitReason::SourcePending),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransferAnswer {
    Receipt(Receipt),
    Receipts(Vec<Receipt>),
    Outcome(OutboxOutcome, Option<crate::store::Row>),
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
    /// The orphan record of a minted `upload_id` written as data of the owner: the batch carries `Control`, so the end of the session does not refuse it.
    ControlOrphan,
    /// The record of an orphan whose abort the server answered: it goes after `Committed`.
    OrphanRemoved,
    /// The shown value of a row: readers get it after `Committed`.
    Shown,
    /// The marker of a temporary copy goes: a refusal of the Store fails the transfer.
    StagedCleared,
    /// The old row is written back after a failed swap: the temporary file goes and the call is answered after `Committed`.
    StagedRestored,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransferEffect {
    /// Copy `source` into the file directory in chunks of 1 MiB.
    Copy { transfer_id: TransferId, attempt_id: AttemptId, key: Key, source: Source, staged: bool },
    /// The temporary file of the copy takes the place of the copy: the answer comes back as `StagedPromoted`.
    PromoteStaged { transfer_id: TransferId },
    CopyCancel { transfer_id: TransferId },
    /// A step of the file (`part`, `blob`) with `NetworkPurpose::Transfer`.
    Step { transfer_id: TransferId, attempt_id: AttemptId, cycle_id: CycleId, key: Key, operation: u64, part: Option<u32>, purpose: NetworkPurpose, upload_id: Option<String>, source: Source, steps: Steps, place: Option<Location>, seeded: Option<SeededAnswer> },
    Complete { transfer_id: TransferId, attempt_id: AttemptId, cycle_id: CycleId, key: Key, upload_id: Option<String>, parts: Vec<(u32, Option<String>)>, place: Option<Location>, seeded: Option<SeededAnswer> },
    Abort { transfer_id: TransferId, attempt_id: AttemptId, key: Key, upload_id: String },
    /// `abort` of an upload whose transfer is gone: the answer comes back as `AbortDone`.
    AbortOrphan { key: Key, upload_id: String },
    CancelStep { transfer_id: TransferId },
    ReleaseStep { request_id: u64 },
    DeleteFile { path: String },
    Reply { reply: ReplyId, answer: TransferAnswer },
    /// `uploads[action]`.
    /// Deliveries `TransferStatus` for the stream `stream_id`.
    Status { stream_id: StreamId, key: Key, shown: Shown, revision: i64 },
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
    AwaitingSource,
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
    Confirmed { revision: i64, result: Option<String>, row_id: Option<String> },
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
    pub source: Option<Source>,
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
    pub revision: i64,
    pub place: Option<Location>,
    pub size: u64,
    pub sha256: String,
    pub mutation: NewOp,
    pub fields: serde_json::Map<String, serde_json::Value>,
    pub steps: Option<Steps>,
    pub restored: bool,
    pub restore_element: Option<(String, Option<TransferId>)>,
    pub pending_step: Option<u64>,
    pub urgent: bool,
    /// The operation of the optimistic effects written with the acceptance (`ReserveOp`).
    pub op_id: Option<OpId>,
    /// The final mutation left for Outbox: Outbox owns the operation, a removal does not roll it back.
    pub submitted: bool,
    pub intent_live: bool,
    pub has_operation: bool,
    pub waits_outbox: bool,
    /// The file is already on the server: the cache of blobs gave its `signed_id` (the steps are skipped).
    pub from_cache: bool,
    /// The cache entry of the content is old: the write of the copy removes it.
    pub cache_stale: bool,
    pub round_error: Option<String>,
    /// The columns of `sync_upload` that the last shown value wrote: `None` while no batch wrote the row.
    pub stored_shown: Option<(u8, bool)>,
    /// The value the readers of the transfer got last.
    pub last_shown: Option<Shown>,
    pub removal: Option<Removal>,
    pub prior: Option<TransferStage>,
    /// A batch of the Store for the failure mark carries `abort`.
    pub failure_abort: bool,
    pub outcomes: CycleOutcomes,
    pub outcome_waiters: Vec<(CycleId, ReplyId, bool)>,
    pub accept_reply: Option<ReplyId>,
    /// The row of the transfer as the batch of its acceptance showed it.
    pub optimistic: Option<crate::store::Row>,
    pub cancel_reply: Option<(ReplyId, bool)>,
    pub retry_reply: Option<ReplyId>,
    pub supply_reply: Option<ReplyId>,
    pub supply_back: Option<TransferStage>,
    pub supply_values: crate::owners::store::IntentEffects,
    pub supply_local: String,
    pub discard_waiters: Vec<ReplyId>,
    pub discard_sent: bool,
    pub new_source: Option<ActiveSource>,
    /// The state before a retry with a new source: the copy and the metadata stay until the new copy is committed.
    pub previous: Option<Box<Transfer>>,
    pub failed_at: Option<Time>,
    /// `Clock.now()` of the answer of the creation of the blob on the server: the term of its cache entry counts from it.
    pub blob_created_at: Option<Time>,
    /// The answer of the pair Session re-sent for a restored step: `(request id, body)`, served to the first request of the step.
    pub seeded: Option<SeededAnswer>,
    /// The saved answer the batch of the attempt removes from the Store with the attempt it starts.
    pub answer_saved: Option<u64>,
    /// The call a failed swap answers and the fault it gets, after the old row is committed.
    pub staged_failed: Option<(Option<ReplyId>, Fault)>,
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

/// The record of a minted `upload_id` the Store has not confirmed: the debt lives until `Committed`, and no abort leaves before it.
#[derive(Clone, Debug)]
pub(super) struct MintedDebt {
    /// The live transfer whose column holds the id; none for the record of a removed transfer.
    pub transfer: Option<TransferId>,
    pub key: Key,
    pub attempts: u32,
    pub attempt_id: AttemptId,
}

#[derive(Clone, Debug)]
pub(super) struct Pending {
    pub ids: Vec<TransferId>,
    pub kind: StoreKind,
    pub orphan: Option<(String, Key)>,
    /// The columns each transfer shows once the batch commits.
    pub written: BTreeMap<TransferId, (u8, bool)>,
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
    pub(super) confirmed_discards: BTreeMap<TransferId, (CycleId, String)>,
    /// The row a confirmed cycle showed in the batch of its removal.
    pub(super) landed_rows: std::collections::HashMap<(TransferId, u64), crate::store::Row>,
    pub(super) groups: BTreeMap<u64, Group>,
    pub(super) selecting: BTreeMap<ReplyId, (TransferId, super::outbox_types::SelectKind)>,
    pub(super) pending: BTreeMap<BatchId, Pending>,
    pub(super) replacements: BTreeMap<TransferId, (TransferId, Option<super::outbox::ReplacementOperation>)>,
    pub(super) subscribers: BTreeMap<Key, BTreeSet<StreamId>>,
    /// The value row readers and Blob streams got last for a key, with the revision of the batch that wrote it: what a joining reader gets. A key without one shows `Gone`.
    pub(super) published: BTreeMap<Key, (Shown, i64)>,
    pub(super) declarations: BTreeSet<(String, String)>,
    /// `upload_id` -> key of the removed transfers whose `abort` the server has not answered.
    pub(super) orphans: BTreeMap<String, Key>,
    pub(super) orphan_flight: BTreeSet<String>,
    /// `upload_id` -> the write the Store refused.
    pub(super) minted: BTreeMap<String, MintedDebt>,
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
    /// The root of the answer of the abort operation.
    pub abort_root: String,
    /// The held calls replay: a start waits until all of them are classified.
    pub starts_held: bool,
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
            confirmed_discards: BTreeMap::new(),
            landed_rows: std::collections::HashMap::new(),
            groups: BTreeMap::new(),
            selecting: BTreeMap::new(),
            pending: BTreeMap::new(),
            replacements: BTreeMap::new(),
            subscribers: BTreeMap::new(),
            published: BTreeMap::new(),
            declarations: BTreeSet::new(),
            orphans: BTreeMap::new(),
            orphan_flight: BTreeSet::new(),
            minted: BTreeMap::new(),
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
            abort_root: "abortMultipart".into(),
            starts_held: false,
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
    pub fn open_id(&self, key: &str) -> Option<TransferId> { self.scan().filter(|transfer| transfer.key == key && transfer.stage != TransferStage::Done).min_by_key(|transfer| transfer.stage == TransferStage::Failed).map(|transfer| transfer.transfer_id) }

    pub(super) fn attempt(&mut self) -> AttemptId {
        self.next_attempt += 1;
        self.next_attempt - 1
    }
}

pub(super) type Fx = Effects<super::transfer::Transfers>;

pub(super) fn run(fx: &mut Fx, effect: TransferEffect) { fx.push(Effect::Run(effect)); }

pub(super) fn reply(fx: &mut Fx, reply: ReplyId, answer: TransferAnswer) { run(fx, TransferEffect::Reply { reply, answer }); }

/// The cycle a batch of `kind` writes for the transfer: a retry or a supplied source opens the next one.
pub(super) fn cycle_of(transfer: &Transfer, kind: StoreKind) -> CycleId {
    let supplied = transfer.supply_back.is_some() && transfer.new_source.is_some() && transfer.stage == TransferStage::PersistingCopy;
    transfer.cycle_id + u64::from((kind == StoreKind::Retrying && transfer.stage == TransferStage::PersistingRetry) || (supplied && transfer.supply_back == Some(TransferStage::Failed)))
}

/// The row of `sync_upload` of a transfer after a batch of `kind`.
pub(super) fn upload_row(transfer: &Transfer, kind: StoreKind) -> UploadRow {
    let supplied = transfer.supply_back.and(transfer.new_source.as_ref()).filter(|_| transfer.stage == TransferStage::PersistingCopy);
    let source = transfer.source.as_ref();
    let steps = transfer.steps.as_ref();
    let mut fields = transfer.fields.clone();
    if let Some(source) = supplied { fields.extend(source.request.variables.clone()); }
    UploadRow {
        key: transfer.key.clone(),
        seq: transfer.seq,
        source: source.map(|source| source.path.clone()),
        parts: source.map_or(0, |source| source.parts),
        batch: transfer.batch_id,
        fields: transfer.batch_id.filter(|_| !fields.is_empty()).map(|_| serde_json::Value::Object(fields).to_string()),
        upload_id: if kind == StoreKind::Failed && transfer.failure_abort { None } else { transfer.upload_id.clone() },
        confirmed_parts: if kind == StoreKind::Failed && transfer.failure_abort { 0 } else { transfer.confirmed_parts },
        attempts: transfer.attempts,
        completed: matches!(kind, StoreKind::Completed) || transfer.signed_id.is_some(),
        failed: if kind == StoreKind::Failed { transfer.error.clone().or_else(|| Some("failed".into())) } else { transfer.error.clone().filter(|_| kind != StoreKind::Retrying && supplied.is_none()) },
        created_at: transfer.created_at.0,
        inline: transfer.place == Some(Location::Table),
        size: transfer.size,
        sha256: transfer.sha256.clone(),
        path: transfer.mutation.path.clone(),
        body: supplied.map_or_else(|| transfer.mutation.body.clone(), |source| source.body.clone()),
        on_failure: transfer.mutation.on_failure,
        steps_path: steps.map(|steps| steps.path.clone()),
        steps_body: steps.map(|steps| steps.body.clone()),
        transfer_id: transfer.transfer_id,
        cycle_id: cycle_of(transfer, kind),
        signed_id: transfer.signed_id.clone(),
        aborted: transfer.aborted,
        staged: transfer.previous.is_some(),
        failed_at: transfer.failed_at.map(|at| at.0),
        blob_created_at: transfer.blob_created_at.map(|at| at.0),
        op_id: transfer.op_id,
        etags: if kind == StoreKind::Failed && transfer.failure_abort { Vec::new() } else { transfer.etags.clone() },
    }
}

pub(super) fn supplied_body(body: &str, source: &SuppliedSource) -> Option<String> {
    if source.variables.is_empty() { return Some(body.to_string()); }
    let mut body: serde_json::Value = serde_json::from_str(body).ok()?;
    let variables = body.get_mut("variables")?.as_object_mut()?;
    let target = match &source.wrap {
        Some(key) => variables.get_mut(key)?.as_object_mut()?,
        None => variables,
    };
    target.extend(source.variables.clone());
    Some(body.to_string())
}
