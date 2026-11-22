//! Owner `Transfer`: the file upload steps. One instance per epoch with an identity.
//! Types of the owner: values, inputs, effects, states. The rows are in `transfer_{open,accept,steps,end}`.

use std::collections::{BTreeMap, BTreeSet};

use super::outbox::{Fault, NewOp, Outbox, OutboxOutcome, Receipt};
use super::outbox_types::{OpId, Phase};
use super::rounds::Rounds;
use crate::store::files::Location;
use crate::owners::contract::{BatchId, Committed, Drained, EpochEnded, EpochStarted, Online, Rejected, ReplyId};
use crate::owners::store::{StoreWriter, UploadRow};
use crate::runtime::{Address, Effect, EffectId, Effects, Epoch, Time};
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
pub type StatusSink = std::sync::Arc<dyn Fn(StreamId, Shown, Figures, i64) + Send + Sync>;

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

/// The file the platform writes the result of the prepare step to: the measure step reads it, then it goes.
pub fn prepared_file_id(transfer_id: TransferId) -> String { format!("{}.prepared", file_id(transfer_id)) }

/// The task of an effect of a transfer: its panic is the failure of the effect of the kind, for the transfer in the low bits of the id.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskKind {
    Copy = 1,
    Prepare = 3,
    Step,
    Complete,
    /// The read of an outcome: the transfer id bits hold the reply that waits for it.
    OutcomeRead,
}

/// The id of the task of an effect with no answer to wait for (clear, discard, abort of a debt, delete of files): its panic closes the database and nothing else.
pub const DETACHED_TASK: EffectId = 0;

const TASK_ID_BITS: u32 = 56;

pub fn task_id(kind: TaskKind, transfer_id: TransferId) -> EffectId { (kind as u64) << TASK_ID_BITS | (transfer_id & ((1 << TASK_ID_BITS) - 1)) }

pub(super) fn task_of(id: EffectId) -> Option<(TaskKind, TransferId)> {
    let kind = match id >> TASK_ID_BITS {
        1 => TaskKind::Copy,
        3 => TaskKind::Prepare,
        4 => TaskKind::Step,
        5 => TaskKind::Complete,
        6 => TaskKind::OutcomeRead,
        _ => return None,
    };
    Some((kind, id & ((1 << TASK_ID_BITS) - 1)))
}

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
    Prepared { source_sha256: String, source_size: u64, name: Option<String>, mime: Option<String>, blob: Box<CachedBlob> },
    PreparedStale,
}

#[derive(Clone, Debug)]
pub struct PreparedCacheEntry { pub file: String, pub size: u64, pub sha256: String, pub name: Option<String>, pub mime: Option<String>, pub fresh: bool }

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Source {
    pub path: String,
    pub parts: u32,
}

#[derive(Clone, Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct SuppliedSource {
    pub source: Source,
    pub prepare: Option<Vec<u8>>,
    pub max_bytes: Option<u64>,
    pub steps: Steps,
    pub values: crate::owners::store::IntentEffects,
    /// The local fields of the values as a JSON object, written by the transaction of the copy.
    pub local: String,
    pub variables: serde_json::Map<String, serde_json::Value>,
    pub wrap: Option<String>,
}

#[derive(Clone, Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct ActiveSource {
    pub request: SuppliedSource,
    pub body: String,
}

/// A row of `sync_upload` kept in the base.
#[derive(Clone, Debug, PartialEq)]
pub struct StoredTransfer {
    pub transfer_id: TransferId,
    pub key: Key,
    pub seq: u64,
    pub source: Option<Source>,
    pub prepare: Option<Vec<u8>>,
    pub max_bytes: Option<u64>,
    pub awaiting_source: bool,
    pub batch: Option<u64>,
    pub fields: serde_json::Map<String, serde_json::Value>,
    pub upload_id: Option<String>,
    pub confirmed_parts: u32,
    pub floor: u32,
    pub weight: Option<u64>,
    pub weighed_prepare: bool,
    pub batch_floor: u32,
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
    pub source_sha256: Option<String>,
    pub prepare_sha256: Option<String>,
    pub cycle_id: CycleId,
    pub has_intent: bool,
    pub has_operation: bool,
    pub mutation: NewOp,
    pub steps: Option<Steps>,
    /// The row was dropped by the layout move or cannot be read.
    pub staged: bool,
    pub failed_at: Option<Time>,
    pub blob_created_at: Option<Time>,
    pub held: bool,
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
    /// The write of the shown value the Store refused is due again.
    Shown { transfer_id: TransferId },
    /// The write of the share of the batch of a row the Store refused is due again.
    BatchShown { transfer_id: TransferId },
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
    pub on_row: bool,
    /// The final mutation waits for `Commit`: the file goes up to the last part (D-372).
    pub held: bool,
    /// The fields of the element in the list of the mutation of the batch: `SIGNED_ID_SENTINEL` stands for the signed id.
    pub fields: serde_json::Map<String, serde_json::Value>,
    /// The action asks to go on in power saving.
    pub urgent: bool,
    pub source: Option<Source>,
    pub prepare: Option<Vec<u8>>,
    pub max_bytes: Option<u64>,
    pub mutation: NewOp,
    pub steps: Option<Steps>,
    /// The width and height a staged upload measured at its acceptance, given to its commit (D-413).
    pub dimensions: Option<serde_json::Map<String, serde_json::Value>>,
}

/// What `Commit` gives a held transfer: the effects of the new row and the variables the final mutation gets.
#[derive(Clone, Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct CommitRequest {
    pub effects: crate::owners::store::IntentEffects,
    pub variables: serde_json::Map<String, serde_json::Value>,
    pub wrap: Option<String>,
    /// The row of the commit exists already: the owner refuses after it checked the transfer.
    pub refused: bool,
}

/// The `Commit` the Store has not confirmed: the effects and the body of the final mutation that the batch writes.
#[derive(Clone, Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct CommitPlan {
    pub effects: crate::owners::store::IntentEffects,
    pub body: String,
    pub restart: bool,
    pub stage: TransferStage,
}

/// The refusal of an `abort`: `status` is the HTTP status of the answer, `None` when the call dropped before an answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AbortFault {
    pub temporary: bool,
    pub status: Option<u16>,
}

#[derive(Clone, Debug)]
pub enum TransferInput {
    CloudRowsErased { rows: BTreeSet<(String, String)> },
    ForgetPrepared { epoch: Epoch, source_sha256: String, prepare_sha256: String, reply: ReplyId },
    PreparedLookup { source_sha256: String, prepare_sha256: String, reply: ReplyId, found: bool },
    Accept { epoch: Epoch, item: Box<BatchItem>, reply: ReplyId },
    AcceptBatch { epoch: Epoch, items: Vec<BatchItem>, joint: Box<Joint>, reply: ReplyId },
    Restored { uploads: Vec<StoredTransfer>, outcomes: TransferOutcomes, orphans: Vec<(String, String)>, batches: BTreeMap<u64, StoredGroup>, held_orphan: Option<String> },
    ReservedFloor { transfer_id: TransferId },
    /// The rows of the Store that got a server id: the keys of the transfers of those rows follow, the asked temporary id keeps finding them.
    AliasFacts { swaps: Vec<(String, String, String)> },
    AliasesRestored { aliases: BTreeMap<(String, String), String> },
    /// The PUT of the step is sent to the network: the timer of the step starts.
    PutStarted { transfer_id: TransferId, attempt_id: AttemptId },
    /// The answer to `abort` of a removed transfer: `Ok` when the server removed the upload or has none, else the refusal.
    AbortDone { upload_id: String, result: Result<(), AbortFault> },
    RestoreFailed { error: String },
    /// `complete`: the registration checked the operations of the app, so a declaration that is missing now will not come.
    RegisterOperations { declarations: Vec<(String, String)>, complete: bool },
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
    /// Answers when no transfer has work that runs without a new external event.
    #[cfg(test)]
    WhenQuiet { reply: ReplyId },
    UploadMinted { transfer_id: TransferId, attempt_id: AttemptId, upload_id: String, key: Key },
    StepError { transfer_id: TransferId, attempt_id: AttemptId, temporary: bool, error: String },
    Completed { transfer_id: TransferId, attempt_id: AttemptId, signed_id: Option<String> },
    CompleteError { transfer_id: TransferId, attempt_id: AttemptId, temporary: bool, error: String },
    MutationOutcome { transfer_id: TransferId, cycle_id: CycleId, outcome: OutboxOutcome, intent_removed: bool, row_never_created: bool },
    MutationOutcomes { batch_id: BatchId, members: Vec<(TransferId, CycleId, OutboxOutcome)>, intent_removed: bool },
    RetryOutcome { cycles: Vec<(TransferId, CycleId)>, result: Result<i64, Fault> },
    RetryCurrent { cycles: Vec<(TransferId, CycleId)> },
    DiscardOutcome { key: Key, result: Result<(), Fault> },
    Outcome { receipt: Receipt, reply: ReplyId, discard_on_failure: bool },
    /// A `Blob` stream follows the key `model:row:action`, also before a transfer exists.
    StatusInterestPlus { key: Key, stream_id: StreamId },
    StatusInterestMinus { key: Key, stream_id: StreamId },
    Committed(Committed),
    Rejected(Rejected),
    Cancel { key: Key, reply: ReplyId },
    SupplySource { epoch: Epoch, key: Key, source: SuppliedSource, reply: ReplyId },
    Commit { epoch: Epoch, key: Key, request: CommitRequest, reply: ReplyId },
    /// The width and height a staged upload measured: `commit_staged` writes them with the row (D-413).
    StagedDimensions { key: Key, reply: ReplyId },
    SourceFailed { epoch: Epoch, key: Key, message: String, reply: ReplyId },
    Retry { key: Key, source: Option<SuppliedSource>, reply: ReplyId },
    Discard { key: Key, reply: ReplyId },
    /// A `retry` or `discard` without an action: `rows` are the names of the row, `outbox_min` the smallest open operation of the row in Outbox.
    SelectRow { model: String, rows: Vec<String>, outbox_min: Option<(OpId, String)>, kind: super::outbox_types::SelectKind, reply: ReplyId, version: u64 },
    SelectConfirmed { reply: ReplyId },
    SlotFreed,
    /// The prepare slot is free: the waiting transfer with the smallest `(seq, position)` starts.
    PrepareSlotFreed,
    PrepareCleared { transfer_id: TransferId, clear_id: AttemptId },
    /// The platform read the source of the attempt.
    /// The whole percent of the run of the attempt.
    PrepareProgress { transfer_id: TransferId, attempt_id: AttemptId, percent: u32 },
    PutProgress { transfer_id: TransferId, attempt_id: AttemptId, sent: u64 },
    Prepared { transfer_id: TransferId, attempt_id: AttemptId, outcome: crate::ffi::PrepareOutcome },
    /// The task of the effect `id` panicked.
    TaskFailed { id: EffectId },
    Online(Online),
    PowerSaving(bool),
    EpochStarted(EpochStarted),
    Tick { kind: TransferTick },
    EpochEnded(EpochEnded),
    Closing,
}

/// What `uploads[action]` shows.
/// The columns of `sync_upload` a shown value of a row writes: status, share of the file, share of its batch (D-415).
pub type Columns = (u8, u32, u32);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Shown {
    Copying,
    WaitingSource,
    Queued { progress: u32 },
    Uploading { progress: u32 },
    Preparing { progress: u32 },
    Failed { error: String },
    Gone,
}

impl Shown {
    /// What the shown value writes to `sync_upload`: two values that give the same columns write once.
    pub fn columns(&self) -> (u8, u32) {
        match self {
            Self::Copying => (0, 0),
            Self::Preparing { progress } => (0, *progress),
            Self::WaitingSource => (1, 0),
            Self::Queued { progress } => (2, *progress),
            Self::Uploading { progress } => (3, *progress),
            Self::Failed { .. } => (4, 0),
            Self::Gone => (5, 0),
        }
    }

    pub fn status(&self) -> Option<(crate::owners::write::upload::UploadStatus, Option<crate::owners::write::upload::UploadStage>)> {
        use crate::owners::write::upload::{UploadStage, UploadStatus};
        Some(match self {
            Self::Copying | Self::Preparing { .. } => (UploadStatus::Preparing, Some(UploadStage::Prepare)),
            Self::WaitingSource => (UploadStatus::Waiting, Some(UploadStage::Prepare)),
            Self::Queued { .. } => (UploadStatus::Queued, Some(UploadStage::Prepare)),
            Self::Uploading { progress: 100.. } => (UploadStatus::Sending, Some(UploadStage::Mutation)),
            Self::Uploading { .. } => (UploadStatus::Uploading, Some(UploadStage::Part)),
            Self::Failed { .. } => (UploadStatus::Failed, None),
            Self::Gone => return None,
        })
    }

    pub fn wait_reason(&self) -> Option<crate::owners::write::upload::WaitReason> {
        match self {
            Self::WaitingSource => Some(crate::owners::write::upload::WaitReason::SourcePending),
            _ => None,
        }
    }
}

/// The figures of a transfer that its status stream shows next to `Shown`, read from the live state of the transfer.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Figures {
    pub sent_bytes: u64,
    pub total_bytes: u64,
    pub attempts: u32,
    pub batch_share: Option<u32>,
}

impl Figures {
    pub(super) fn of(transfer: &Transfer, shown: &Shown) -> Self {
        let Some((status, _)) = shown.status() else { return Self::default() };
        let parts = transfer.source.as_ref().map_or(0, |source| u64::from(source.parts));
        Self { sent_bytes: crate::owners::write::upload::bytes_sent(status, parts, u64::from(transfer.confirmed_parts), transfer.size), total_bytes: transfer.size, attempts: transfer.attempts, batch_share: transfer.batch_id.map(|_| transfer.batch_floor) }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransferAnswer {
    Receipt(Receipt),
    Receipts(Vec<Receipt>),
    Outcome(OutboxOutcome, Option<crate::store::Row>),
    Ok,
    #[cfg(test)]
    Quiet(u64),
    Cancelled(bool),
    Dimensions(Option<serde_json::Map<String, serde_json::Value>>, bool),
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
    /// The share of the batch of a row whose shown value is the same: no reader of the transfer is woken (D-415).
    BatchShown,
    /// The marker of a temporary copy goes: a refusal of the Store leaves the transfer as it is.
    StagedCleared,
    /// The old row is written back after a failed swap: the temporary file goes and the call is answered after `Committed`.
    StagedRestored,
    /// The row of a held transfer: its values and the final mutation.
    Commit,
}

#[derive(Clone, Debug, PartialEq)]
pub enum TransferEffect {
    LookupPrepared { source_sha256: String, prepare_sha256: String, reply: ReplyId },
    /// Copy `source` into the file directory in chunks of 1 MiB.
    Copy { transfer_id: TransferId, attempt_id: AttemptId, key: Key, source: Source, staged: bool, prepare: Option<Vec<u8>> },
    /// The temporary file of the copy takes the place of the copy: the answer comes back as `StagedPromoted`.
    PromoteStaged { transfer_id: TransferId },
    CopyCancel { transfer_id: TransferId },
    /// The platform writes the result by `plan`: the answers come back as `PrepareProgress` and `Prepared`.
    Prepare { transfer_id: TransferId, attempt_id: AttemptId, descriptor: Vec<u8> },
    PrepareCancel { transfer_id: TransferId, attempt_id: AttemptId },
    /// Read the result of the platform in chunks: the answer comes back as `CopyDone` or `CopyFailed`.
    Measure { transfer_id: TransferId, attempt_id: AttemptId },
    /// A step of the file (`part`, `blob`) with `NetworkPurpose::Transfer`.
    Step { transfer_id: TransferId, attempt_id: AttemptId, cycle_id: CycleId, key: Key, operation: u64, part: Option<u32>, purpose: NetworkPurpose, upload_id: Option<String>, source: Source, steps: Steps, place: Option<Location>, seeded: Option<SeededAnswer>, reason: crate::transport::RequestReason },
    Complete { transfer_id: TransferId, attempt_id: AttemptId, cycle_id: CycleId, key: Key, upload_id: Option<String>, parts: Vec<(u32, Option<String>)>, place: Option<Location>, seeded: Option<SeededAnswer>, reason: crate::transport::RequestReason },
    Abort { transfer_id: TransferId, attempt_id: AttemptId, key: Key, upload_id: String },
    /// `abort` of an upload whose transfer is gone: the answer comes back as `AbortDone`.
    AbortOrphan { key: Key, upload_id: String },
    CancelStep { transfer_id: TransferId },
    ReleaseStep { request_id: u64 },
    DeleteFile { path: String },
    KeepPrepared { transfer_id: TransferId, file: String },
    ClearPrepare { transfer_id: TransferId, clear_id: AttemptId },
    Reply { reply: ReplyId, answer: TransferAnswer },
    /// The receipt of a transfer that is no longer in memory: the record of the Store answers.
    ReadOutcome { reply: ReplyId, id: TransferId, cycle_id: CycleId },
    /// `uploads[action]`.
    /// Deliveries `TransferStatus` for the stream `stream_id`.
    Status { stream_id: StreamId, key: Key, shown: Shown, figures: Figures, revision: i64 },
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
    Preparing,
    Measuring,
    PersistingPrepared,
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
    /// Every part is on the server and `complete` waits for `Commit` (D-372).
    Held,
    PersistingCommit,
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
    Invalid { error: String },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NextStep {
    Part(u32),
    Complete,
    /// Every part is on the server and the mutation is what failed.
    Mutation,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PreparePhase {
    Queued,
    Run,
}

/// The prepare step of a transfer: its phase and what the platform answered.
#[derive(Clone, Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct PrepareRun {
    pub phase: PreparePhase,
    pub percent: u32,
    pub name: Option<String>,
    pub mime: Option<String>,
}

#[derive(Clone, Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub enum Waiting {
    Removal(ReplyId, bool),
    Retry(Option<SuppliedSource>, ReplyId),
    Commit(Epoch, CommitRequest, ReplyId),
    Asked { rep: ReplyId, ids: Vec<(TransferId, CycleId)>, answer: Option<RetryAnswer> },
}

#[derive(Clone, Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub enum RetryAnswer {
    Current,
    Refused(Fault),
}

#[derive(Clone, Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct Transfer {
    pub held: bool,
    pub on_row: bool,
    /// The file was staged: it goes multipart whatever its size, also after `Commit` (D-372).
    pub multipart_forced: bool,
    pub commit_reply: Option<ReplyId>,
    pub commit: Option<CommitPlan>,
    pub prep: Option<PrepareRun>,
    pub transfer_id: TransferId,
    pub key: Key,
    pub seq: u64,
    pub source: Option<Source>,
    pub prepare: Option<Vec<u8>>,
    pub max_bytes: Option<u64>,
    pub dimensions: Option<serde_json::Map<String, serde_json::Value>>,
    pub batch_id: Option<BatchId>,
    pub epoch: Epoch,
    pub stage: TransferStage,
    pub attempt_id: AttemptId,
    pub attempts: u32,
    /// Storage refusals in a row of the writes of the transfer (P-DATA-102).
    pub storage_refusals: u32,
    #[cfg(test)]
    pub(super) last_storage_refusal: Option<String>,
    pub in_flight: bool,
    pub waiting_retry: bool,
    pub next_step: NextStep,
    pub upload_id: Option<String>,
    pub confirmed_parts: u32,
    /// The bytes of the attempt in flight that the platform reported.
    pub inflight: u64,
    /// The last share shown in hundredths: the shown value never goes below it.
    pub floor: u32,
    /// The size of the copy of the source when it was first measured: the weight of the member in its batch, `None` before (D-415).
    pub weight: Option<u64>,
    /// The source had prepare settings when it was weighed: the step counts for half of the member.
    pub weighed_prepare: bool,
    /// The share of the member in its batch in hundredths, never going down.
    pub share_floor: u32,
    /// The share of the batch the row of the member shows in hundredths, never going down.
    pub batch_floor: u32,
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
    pub source_sha256: Option<String>,
    pub prepare_sha256: Option<String>,
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
    pub keep_prepared: Option<(String, u64)>,
    pub prepared_hit: bool,
    pub prepared_stale: bool,
    pub skip_keep: bool,
    pub round_error: Option<String>,
    /// The columns of `sync_upload` that the last shown value wrote: `None` while no batch wrote the row.
    pub stored_shown: Option<Columns>,
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
    pub cancel_replies: Vec<(ReplyId, bool)>,
    pub commands: Vec<Waiting>,
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
    pub clearing: Option<AttemptId>,
    pub closing: bool,
}

impl Transfer {
    /// The file that goes to the network is larger than the limit of its source.
    pub(super) fn over_limit(&self, size: u64) -> bool { self.max_bytes.is_some_and(|limit| size > limit) }
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
    pub record: MintedRecord,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum MintedRecord {
    Upload,
    Orphan,
    Recorded,
}

#[derive(Clone, Debug)]
pub(super) struct Pending {
    pub ids: Vec<TransferId>,
    pub kind: StoreKind,
    pub orphan: Option<(String, Key)>,
    /// The columns each transfer shows once the batch commits.
    pub written: BTreeMap<TransferId, Columns>,
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
    pub(super) confirmed_discards: BTreeMap<TransferId, (CycleId, String)>,
    pub(super) groups: BTreeMap<u64, Group>,
    pub(super) selecting: BTreeMap<ReplyId, (TransferId, super::outbox_types::SelectKind)>,
    pub(super) removal_pending: BTreeSet<TransferId>,
    pub(super) pending: BTreeMap<BatchId, Pending>,
    pub(super) forget_busy: bool,
    pub(super) forget_batches: BTreeMap<BatchId, ReplyId>,
    pub(super) forget_held: Vec<TransferInput>,
    pub(super) replacements: BTreeMap<TransferId, (TransferId, Option<super::outbox::ReplacementOperation>)>,
    /// Per key, the value each Blob stream got last with its revision: a stream gets a value only when it differs.
    pub(super) subscribers: BTreeMap<Key, BTreeMap<StreamId, (Shown, i64)>>,
    /// The value row readers and Blob streams got last for a key, with the revision of the batch that wrote it: what a joining reader gets. A key without one shows `Gone`.
    pub(super) published: BTreeMap<Key, (Shown, i64)>,
    pub(super) declarations: BTreeSet<(String, String)>,
    pub(super) complete: bool,
    /// `upload_id` -> key of the removed transfers whose `abort` the server has not answered.
    pub(super) orphans: BTreeMap<String, Key>,
    pub(super) orphan_flight: BTreeSet<String>,
    /// The transfers `Rounds` holds the round for, and the last share of them sent to `Rounds`.
    pub(super) round_active: BTreeSet<TransferId>,
    pub(super) round_sent: Option<u32>,
    /// `upload_id` -> the write the Store refused.
    pub(super) minted: BTreeMap<String, MintedDebt>,
    pub(super) aliases: super::alias_map::AliasMap,
    /// The orphans whose `Online(true)` or `RestoreReady` came while their abort was in flight.
    pub(super) orphan_owed: BTreeSet<String>,
    /// `attempt_id` of the retry timer -> the refused write batch: its kind and its elements.
    pub(super) retrying: BTreeMap<AttemptId, (StoreKind, Vec<TransferId>)>,
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
    pub sizing: super::upload::Sizing,
    pub drained: bool,
    /// The root of the answer of the abort operation.
    pub abort_root: String,
    pub init_root: String,
    /// The held calls replay: a start waits until all of them are classified.
    pub starts_held: bool,
    #[cfg(test)]
    pub(super) quiet_waiters: Vec<ReplyId>,
    pub(super) handled: u64,
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
            online: None,
            saving: false,
            slots,
            phase: Phase::Restoring,
            transfers: BTreeMap::new(),
            confirmed_discards: BTreeMap::new(),
            groups: BTreeMap::new(),
            selecting: BTreeMap::new(),
            removal_pending: BTreeSet::new(),
            pending: BTreeMap::new(),
            forget_busy: false,
            forget_batches: BTreeMap::new(),
            forget_held: Vec::new(),
            replacements: BTreeMap::new(),
            subscribers: BTreeMap::new(),
            published: BTreeMap::new(),
            declarations: BTreeSet::new(),
            complete: false,
            orphans: BTreeMap::new(),
            orphan_flight: BTreeSet::new(),
            round_active: BTreeSet::new(),
            round_sent: None,
            minted: BTreeMap::new(),
            aliases: super::alias_map::AliasMap::default(),
            orphan_owed: BTreeSet::new(),
            retrying: BTreeMap::new(),
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
            sizing: super::upload::Sizing { multipart_declared: false, single_declared: true, threshold: super::upload::UploadConfig::default().multipart_threshold, part_size: super::upload::UploadConfig::default().part_size },
            drained: false,
            abort_root: "abortMultipart".into(),
            init_root: "initMultipart".into(),
            starts_held: false,
            #[cfg(test)]
            quiet_waiters: Vec::new(),
            handled: 0,
            closing: false,
            ended: false,
            queue_error: None,
        }
    }

    pub(super) fn scan(&self) -> impl Iterator<Item = &Transfer> + '_ {
        self.transfers.values().inspect(|_| { self.examined.fetch_add(1, std::sync::atomic::Ordering::Relaxed); })
    }

    pub fn stage_of(&self, id: TransferId) -> Option<TransferStage> { self.transfers.get(&id).map(|transfer| transfer.stage) }

    /// The rows `temp` became `real`: every key the owner holds follows.
    pub(super) fn swap_keys(&mut self, swaps: Vec<(String, String, String)>) {
        for (model, temp, real) in swaps { self.aliases.insert(&model, &temp, &real); }
        let keys: Vec<(TransferId, Key)> = self.transfers.iter().map(|(id, transfer)| (*id, self.aliases.canonical_key(&transfer.key))).collect();
        for (id, key) in keys { if let Some(transfer) = self.transfers.get_mut(&id) { transfer.key = key; } }
        for (upload_id, key) in self.orphans.clone() { self.orphans.insert(upload_id, self.aliases.canonical_key(&key)); }
        let minted: Vec<(String, Key)> = self.minted.iter().map(|(upload_id, debt)| (upload_id.clone(), self.aliases.canonical_key(&debt.key))).collect();
        for (upload_id, key) in minted { if let Some(debt) = self.minted.get_mut(&upload_id) { debt.key = key; } }
        for (key, streams) in std::mem::take(&mut self.subscribers) { self.subscribers.entry(self.aliases.canonical_key(&key)).or_default().extend(streams); }
        for (key, shown) in std::mem::take(&mut self.published) {
            let key = self.aliases.canonical_key(&key);
            if self.published.get(&key).is_none_or(|held| held.1 <= shown.1) { self.published.insert(key, shown); }
        }
    }

    /// The keys of the call carry the ids the app used: the asked key and the held key meet at the server id.
    pub(super) fn canonical(&self, input: TransferInput) -> TransferInput {
        let key = |key: Key| self.aliases.canonical_key(&key);
        match input {
            TransferInput::Accept { epoch, mut item, reply } => { item.key = key(item.key); TransferInput::Accept { epoch, item, reply } }
            TransferInput::AcceptBatch { epoch, mut items, joint, reply } => { for item in &mut items { item.key = key(std::mem::take(&mut item.key)); } TransferInput::AcceptBatch { epoch, items, joint, reply } }
            TransferInput::StatusInterestPlus { key: asked, stream_id } => TransferInput::StatusInterestPlus { key: key(asked), stream_id },
            TransferInput::StatusInterestMinus { key: asked, stream_id } => TransferInput::StatusInterestMinus { key: key(asked), stream_id },
            TransferInput::Cancel { key: asked, reply } => TransferInput::Cancel { key: key(asked), reply },
            TransferInput::SupplySource { epoch, key: asked, source, reply } => TransferInput::SupplySource { epoch, key: key(asked), source, reply },
            TransferInput::Commit { epoch, key: asked, request, reply } => TransferInput::Commit { epoch, key: key(asked), request, reply },
            TransferInput::UploadMinted { transfer_id, attempt_id, upload_id, key: asked } => TransferInput::UploadMinted { transfer_id, attempt_id, upload_id, key: key(asked) },
            TransferInput::StagedDimensions { key: asked, reply } => TransferInput::StagedDimensions { key: key(asked), reply },
            TransferInput::SourceFailed { epoch, key: asked, message, reply } => TransferInput::SourceFailed { epoch, key: key(asked), message, reply },
            TransferInput::Retry { key: asked, source, reply } => TransferInput::Retry { key: key(asked), source, reply },
            TransferInput::Discard { key: asked, reply } => TransferInput::Discard { key: key(asked), reply },
            other => other,
        }
    }

    /// The open transfer of `key`: a `Done` transfer keeps only its outcomes.
    pub fn open_id(&self, key: &str) -> Option<TransferId> { self.scan().filter(|transfer| transfer.key == key && transfer.stage != TransferStage::Done && !self.replacements.values().any(|(old, _)| *old == transfer.transfer_id)).min_by_key(|transfer| transfer.stage == TransferStage::Failed).map(|transfer| transfer.transfer_id) }

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
        progress: transfer.floor,
        weight: transfer.weight,
        weighed_prepare: transfer.weighed_prepare,
        batch_progress: transfer.batch_floor,
        attempts: transfer.attempts,
        completed: matches!(kind, StoreKind::Completed) || transfer.signed_id.is_some(),
        failed: if kind == StoreKind::Failed { transfer.error.clone().or_else(|| Some("failed".into())) } else { transfer.error.clone().filter(|_| kind != StoreKind::Retrying && supplied.is_none()) },
        created_at: transfer.created_at.0,
        inline: transfer.place == Some(Location::Table),
        size: transfer.size,
        sha256: transfer.sha256.clone(),
        source_sha256: transfer.source_sha256.clone(),
        prepare_sha256: transfer.prepare_sha256.clone(),
        path: transfer.mutation.path.clone(),
        body: supplied.map(|source| source.body.clone()).or_else(|| transfer.commit.as_ref().map(|plan| plan.body.clone())).unwrap_or_else(|| transfer.mutation.body.clone()),
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
        held: transfer.held && transfer.commit.is_none(),
        op_id: transfer.op_id,
        etags: if kind == StoreKind::Failed && transfer.failure_abort { Vec::new() } else { transfer.etags.clone() },
        prepare: transfer.prepare.clone(),
        max_bytes: transfer.max_bytes,
    }
}

pub(super) fn supplied_body(body: &str, source: &SuppliedSource) -> Option<String> { merged_body(body, &source.variables, source.wrap.as_deref()) }

/// The request body with `extra` added to the variables of the mutation (inside `wrap` when the action wraps its input).
pub(super) fn merged_body(body: &str, extra: &serde_json::Map<String, serde_json::Value>, wrap: Option<&str>) -> Option<String> {
    if extra.is_empty() { return Some(body.to_string()); }
    let mut body: serde_json::Value = serde_json::from_str(body).ok()?;
    let variables = body.get_mut("variables")?.as_object_mut()?;
    let target = match wrap {
        Some(key) => variables.get_mut(key)?.as_object_mut()?,
        None => variables,
    };
    target.extend(extra.clone());
    Some(body.to_string())
}
