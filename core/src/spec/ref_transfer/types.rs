use super::*;

pub type TransferId = u64;
pub type AttemptId = u64;
pub type OpId = u64;
pub type GroupId = u64;
pub type StreamId = u64;
pub type ReplyId = u64;
pub type StoreBatch = u64;

pub const ATTEMPT_LIMIT: u32 = 50;
pub const EXPIRY_MS: u64 = 7 * 24 * 3600 * 1000;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct TransferKey { pub model: String, pub row_id: String, pub action: String }

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Source { File { path: String }, Pending }

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AbortFault { pub temporary: bool, pub status: Option<u16> }

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Supplied { pub path: String, pub name: String, pub mime: String, pub values: Vec<String>, pub variables: Vec<String>, pub valid: bool, pub values_declared: bool }

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Check { pub source_valid: bool, pub action_declared: bool, pub has_effects: bool }

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Item { pub key: TransferKey, pub source: Source, pub values: Vec<String>, pub check: Check }

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Cached { Fresh { blob_id: String }, Stale, Miss }

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reason { NoSession, Closed, Storage, Invalid, StaleGeneration(Vec<u64>) }

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Fault { NotFoundAt(usize), NoSession, Invalid, NotFound, InFlight, Ended(EndCause), Closed, Storage, Copy(String), RestoreFailed, Reason(Reason), Failure(Failure) }

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ErrKind { Exhausted, MissingFile, SourceFailed, RestoreFailed, Storage, InternalInvalid, Step(String) }

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Failure { pub kind: ErrKind, pub message: String }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EndCause { SessionEnded, Closed, Cancelled }

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome { Confirmed { revision: u64 }, Discarded, Ended(EndCause), Failed { error: Failure }, Rejected { error: Failure } }

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MutOut { Confirmed { revision: u64 }, Rejected { error: Failure }, Failed { error: Failure }, Discarded, Ended(EndCause) }

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MutTarget { Transfer { transfer_id: TransferId, outcome: MutOut }, Group { batch_id: GroupId, outcomes: BTreeMap<TransferId, MutOut> } }

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Target { Transfer { transfer_id: TransferId }, Group { batch_id: GroupId } }

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Receipt { pub epoch: u64, pub transfer_id: TransferId, pub cycle_id: u64 }

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Answer { Receipt(Receipt), Receipts(Vec<Receipt>), Ok, Bool(bool), Err(Fault), Outcome(Outcome) }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StepKind { CreateBlob, Part(u32), Complete }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Next { Part(u32), Complete, Mutation }

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ReqId { pub transfer_id: TransferId, pub cycle_id: u64, pub step: String, pub part: u32 }

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredPending { pub req_id: ReqId, pub next: Next }

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoredKind { Copying { source_present: bool, source_path: String }, AwaitingSource, Active, Completed, Failed }

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoreGroup { pub batch_id: GroupId, pub op_id: OpId, pub minimum: usize, pub maximum: usize, pub items: Vec<TransferId>, pub readable: bool, pub intent_present: bool, pub single_intent: bool }

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredTransfer {
    pub id: TransferId, pub key: TransferKey, pub op_id: OpId, pub seq: u64, pub pos: u32, pub cycle_id: u64, pub created_at: u64, pub accepted_at: u64,
    pub kind: StoredKind, pub copy_present: bool, pub size: u64, pub sha256: String, pub parts: u32, pub upload_id: Option<String>,
    pub confirmed: Vec<(u32, Option<String>)>, pub attempts: u32, pub complete_attempts: u32, pub signed_id: Option<String>, pub error: Option<Failure>,
    pub pending_step: Option<StoredPending>, pub intent_live: bool, pub has_effects: bool, pub group: Option<RestoreGroup>, pub aborted: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RestoreItem { Failed { id: TransferId, key: TransferKey, error: Failure, retained: bool, seq: u64, pos: u32 }, Row(StoredTransfer) }

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TickKind { Retry { transfer_id: TransferId, attempt_id: AttemptId }, PutTimeout { transfer_id: TransferId, attempt_id: AttemptId }, Expire, MintedWrite { transfer_id: TransferId, attempt_id: AttemptId } }

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Input {
    Accept { epoch: u64, now: u64, item: Item, reply: ReplyId },
    AcceptBatch { epoch: u64, now: u64, minimum: usize, maximum: usize, items: Vec<Item>, reply: ReplyId },
    SupplySource { epoch: u64, key: TransferKey, source: Supplied, reply: ReplyId },
    SourceFailed { epoch: u64, key: TransferKey, message: String, reply: ReplyId },
    Restored { epoch: u64, revision: u64, now: u64, orphans: Vec<(String, TransferKey)>, uploads: Vec<RestoreItem>, transfer_outcomes: BTreeMap<TransferId, BTreeMap<u64, Outcome>>, held_orphan: Option<String> },
    RegisterOperations { declarations: BTreeSet<(String, String)> },
    OpReserved { target: Target, result: Result<OpId, Fault> },
    RestoreReady { epoch: u64 },
    RoundStatus { epoch: u64, transfer_ids: Vec<TransferId>, refused: Option<String> },
    CopyDone { transfer_id: TransferId, attempt_id: AttemptId, size: u64, sha256: String, cached: Cached },
    CopyFailed { transfer_id: TransferId, attempt_id: AttemptId, error: String },
    StepOk { transfer_id: TransferId, attempt_id: AttemptId, part: Option<u32>, etag: Option<String>, upload_id: Option<String> },
    StepError { transfer_id: TransferId, attempt_id: AttemptId, temporary: bool, error: Failure, retained: bool },
    UploadMinted { transfer_id: TransferId, attempt_id: AttemptId, upload_id: String, key: TransferKey },
    Completed { transfer_id: TransferId, attempt_id: AttemptId, signed_id: String },
    CompleteError { transfer_id: TransferId, attempt_id: AttemptId, temporary: bool, error: Failure },
    AbortDone { upload_id: String, result: Result<(), AbortFault> },
    MutationOutcome(MutTarget),
    DiscardOutcome { transfer_id: TransferId, result: Result<(), Fault> },
    RetryOutcome { target: Target, result: Result<u64, Fault> },
    Outcome { transfer_id: TransferId, cycle_id: u64, saved: Option<Outcome>, reply: ReplyId },
    InterestPlus { transfer_id: TransferId, stream_id: StreamId },
    InterestMinus { transfer_id: TransferId, stream_id: StreamId },
    StatusInterestPlus { stream_id: StreamId, key: TransferKey },
    StatusInterestMinus { stream_id: StreamId, key: TransferKey },
    Committed { batch: StoreBatch, epoch: u64, revision: u64 },
    Rejected { batch: StoreBatch, epoch: u64, reason: Reason },
    Cancel { epoch: u64, key: TransferKey, reply: ReplyId },
    Retry { epoch: u64, key: TransferKey, source: Option<Supplied>, reply: ReplyId },
    Discard { epoch: u64, key: TransferKey, reply: ReplyId },
    SelectRow { epoch: u64, model: String, row_id: String, retry: bool, source: Option<Supplied>, outbox_min: Option<OpId>, reply: ReplyId, version: u64 },
    SelectConfirmed { reply: ReplyId },
    SlotFreed,
    Online(bool),
    PowerSaving(bool),
    Tick { now: u64, kind: TickKind },
    EpochEnded { epoch: u64, cause: EndCause },
    Closing,
    EpochStarted { epoch: u64, generation: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OpOrigin { Transfer { transfer_id: TransferId }, TransferGroup { batch_id: GroupId, transfer_ids: Vec<TransferId> } }

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    UploadCopying { op_id: OpId, source_path: Option<String> },
    Effects { op_id: OpId },
    SyncUpload { size: u64, sha256: String, variables: bool },
    Completed { signed_id: String },
    Intent { op_id: OpId, origin: OpOrigin },
    Attempt { attempts: u32 },
    ConfirmedParts { part: Option<u32>, upload_id: Option<String>, all: bool },
    Failed { clear_upload: bool },
    Retrying { reset_budget: bool, new_cycle: bool },
    FailedOutcome { transfer_id: TransferId, cycle_id: u64, error: Failure },
    UploadId { upload_id: String },
    RoundError(Option<String>),
    RemoveUpload { transfer_id: TransferId },
    DiscardedOutcome { transfer_id: TransferId, cycle_id: u64 },
    UploadShown { shown: Shown },
    RemoveIntent,
    UploadOrphan { upload_id: String, key: TransferKey },
    RemoveOrphan { upload_id: String },
    Rollback { op_id: OpId },
    BlobCachePut { sha256: String },
    BlobCacheRemove { sha256: String },
    ItemsMinus { batch_id: GroupId, transfer_id: TransferId },
    Renumber { batch_id: GroupId, start: u32, size: usize },
    DiscardExpired,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OutboxEffect {
    ReserveOp { epoch: u64, target: Target },
    Submit { op_id: OpId, origin: OpOrigin },
    Discard { op_id: OpId, reply_to_transfer: TransferId },
    RetryTransfer { target: Target },
    SelectedOutbox { reply: ReplyId, version: u64 },
    SelectedNone { reply: ReplyId, version: u64 },
    SelectedTransfer { reply: ReplyId, transfer_id: TransferId, version: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Delay { Backoff { attempt: u32 }, Now, PutTimeout }

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Status { Copying, Waiting, Queued, Uploading, Mutating, Done, Failed, Gone }

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Shown { pub status: Status, pub wait_reason: Option<&'static str>, pub sent_bytes: u64, pub total_bytes: u64, pub error: Option<Failure>, pub local_url: bool, pub round_error: Option<String> }

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Effect {
    Row(&'static str),
    Undefined { row: String, reason: &'static str },
    Store { batch: StoreBatch, generation: u64, basis: Option<Vec<u64>>, ops: Vec<Op> },
    Outbox(OutboxEffect),
    Reply(ReplyId, Answer),
    Copy { transfer_id: TransferId, attempt_id: AttemptId, source: Source },
    CopyCancel { transfer_id: TransferId },
    Send { transfer_id: TransferId, attempt_id: AttemptId, step: StepKind, req_id: ReqId, net_purpose_transfer: bool },
    CompleteRequest { transfer_id: TransferId, attempt_id: AttemptId, req_id: ReqId, parts: Vec<(u32, Option<String>)> },
    Abort { upload_id: String, key: TransferKey, net_purpose_transfer: bool },
    TransportCancel { transfer_id: TransferId, attempt_id: AttemptId },
    Schedule { kind: TickKind, delay: Delay },
    CancelTimer { kind: TickKind },
    SelfSlotFreed,
    DeleteFile { transfer_id: TransferId },
    SessionRelease { req_id: ReqId },
    LifecycleStarted { epoch: u64 },
    NetworkSnapshotRequest { epoch: u64 },
    RestoreClassified { epoch: u64 },
    Rounds(RoundsIn),
    Status { stream_id: StreamId, transfer_id: TransferId, shown: Shown },
    Drained,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Back { AwaitingSource, Failed }

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Why { Cancel, Confirmed(u64), Discarded, CopyFailed(String), Storage }

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum St {
    Reserving,
    Accepting { closing: bool },
    AwaitingSource,
    Copying { attempt: AttemptId, supply: Option<Back>, new_cycle: bool },
    CopyReady { attempt: AttemptId },
    WaitingDeclaration { item: Box<RestoreItem> },
    PersistingCopy { supply: Option<Back>, cached: bool, new_cycle: bool },
    PersistingRetry { next: Next, recreate: bool, wait_outbox: bool },
    PersistingFailure { error: Failure, abort: Option<String> },
    Removing { why: Why, prior: Box<St>, orphan: Option<String> },
    Ready { next: Next },
    PersistingAttempt { step: StepKind, attempt: AttemptId },
    Uploading { step: StepKind, attempt: AttemptId, waiting_retry: bool },
    PersistingPart { part: u32, last: bool },
    Completing { attempt: AttemptId, waiting_retry: bool },
    PersistingComplete,
    PersistingBatchIntent { single: bool, retry: Option<AttemptId> },
    BatchReady,
    Mutating,
    Failed,
    Done,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Replies { pub accept: Option<ReplyId>, pub cancel: Option<(ReplyId, bool)>, pub discard_waiters: Vec<ReplyId>, pub supply: Option<(ReplyId, bool)>, pub retry: Option<ReplyId> }

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Transfer {
    pub id: TransferId, pub key: TransferKey, pub op_id: Option<OpId>, pub epoch: u64, pub source: Source, pub st: St,
    pub seq: u64, pub pos: u32, pub cycle_id: u64, pub batch: Option<GroupId>, pub size: u64, pub sha256: Option<String>, pub parts: u32,
    pub upload_id: Option<String>, pub confirmed: Vec<(u32, Option<String>)>, pub attempts: u32, pub complete_attempts: u32, pub signed_id: Option<String>,
    pub created_at: u64, pub accepted_at: u64, pub error: Option<Failure>, pub copy_present: bool, pub has_effects: bool, pub held_step: Option<ReqId>,
    pub round_error: Option<String>, pub replies: Replies, pub outcome_waiters: BTreeMap<u64, Vec<ReplyId>>, pub outcomes: BTreeMap<u64, Outcome>,
    pub intent_live: bool, pub write_retry: Option<AttemptId>, pub intent_after_ready: bool, pub local_url: bool, pub cached: Option<Cached>, pub aborted: bool, pub values: Vec<String>, pub variables: Vec<String>,
    pub stored_shown: Option<Shown>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Phase { Idle, Restoring, Classified, Ready, Closed }

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GroupPhase { Collecting, Mutating, Singles }

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Group {
    pub items: Vec<TransferId>, pub op_id: Option<OpId>, pub minimum: usize, pub maximum: usize, pub phase: GroupPhase,
    pub accept_reply: Option<ReplyId>, pub reserve: BTreeMap<String, Option<OpId>>, pub reserve_need: BTreeSet<String>, pub reserve_failed: bool, pub renumber_start: u32,
    pub submit_pending: bool, pub pack_ids: Vec<TransferId>, pub pack_op: Option<OpId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Held { pub call: Option<Box<Input>>, pub now: u64, pub item: Option<Item>, pub batch: Option<(usize, usize, Vec<Item>)>, pub epoch: u64, pub reply: ReplyId }

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BatchKind { Own { ids: Vec<TransferId>, ops: Vec<Op> }, Detached, Orphan { upload_id: String }, Minted { upload_id: String } }

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Minted { pub transfer_id: TransferId, pub attempt_id: AttemptId, pub is_orphan: bool, pub key: TransferKey, pub in_flight: Option<StoreBatch>, pub debt: bool }

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Post { Reserve(GroupId), GroupCheck { gid: GroupId, from_complete: bool, removal: bool }, CopyBatch(GroupId), CopyFailedBatch { gid: GroupId, failing: TransferId, error: String }, Receipts(GroupId), SinglesNext(GroupId, TransferId) }

#[derive(Clone, Debug)]
pub struct G {
    pub epoch: u64, pub phase: Phase, pub online: Option<bool>, pub saving: bool, pub declarations: BTreeSet<(String, String)>,
    pub generation: u64, pub models: BTreeMap<String, u64>, pub now: u64,
    pub groups: BTreeMap<GroupId, Group>, pub held: Vec<Held>,
    pub orphans: BTreeMap<String, TransferKey>, pub orphan_flight: BTreeSet<String>, pub orphan_owed: BTreeSet<String>, pub minted: BTreeMap<String, Minted>,
    pub next_transfer: u64, pub next_attempt: u64, pub next_batch: u64, pub next_group: u64, pub next_seq: u64,
    pub batches: BTreeMap<StoreBatch, BatchKind>,
    pub shown_batches: BTreeMap<StoreBatch, (TransferId, Shown)>,
    pub file_slots: u32, pub multipart_threshold: u64, pub part_size: u64,
    pub subs: BTreeMap<TransferId, BTreeSet<StreamId>>, pub key_subs: BTreeMap<TransferKey, BTreeSet<StreamId>>,
    pub deferred: Vec<(String, Vec<TransferId>, Vec<Op>)>, pub post: Vec<Post>, pub round_needed: Vec<TransferId>, pub slots_used: u32, pub drained: bool, pub quiet: bool, pub saved_outcomes: BTreeMap<TransferId, BTreeMap<u64, Outcome>>,
    pub selecting: BTreeMap<ReplyId, (TransferId, bool)>,
    pub replacements: BTreeMap<TransferId, TransferId>,
}

#[derive(Clone, Debug)]
pub struct State { pub g: G, pub transfers: BTreeMap<TransferId, Transfer> }

impl State {
    pub fn new(file_slots: u32, multipart_threshold: u64, part_size: u64) -> State {
        State {
            g: G {
                epoch: 0, phase: Phase::Idle, online: None, saving: false, declarations: BTreeSet::new(), generation: 0, models: BTreeMap::new(), now: 0,
                groups: BTreeMap::new(), held: Vec::new(), orphans: BTreeMap::new(), orphan_flight: BTreeSet::new(), orphan_owed: BTreeSet::new(), minted: BTreeMap::new(),
                next_transfer: 1, next_attempt: 1, next_batch: 1, next_group: 1, next_seq: 1, batches: BTreeMap::new(), shown_batches: BTreeMap::new(),
                file_slots, multipart_threshold, part_size, subs: BTreeMap::new(), key_subs: BTreeMap::new(),
                deferred: Vec::new(), post: Vec::new(), round_needed: Vec::new(), slots_used: 0, drained: false, quiet: false, saved_outcomes: BTreeMap::new(),
                selecting: BTreeMap::new(), replacements: BTreeMap::new(),
            },
            transfers: BTreeMap::new(),
        }
    }
}
