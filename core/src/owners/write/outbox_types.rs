//! Owner `Outbox`: operations of the queue, optimism, receipts and local writes.
//! One instance per epoch with an identity. Types of the table: inputs, effects, stages. The rows are in `step`.

use std::collections::{BTreeMap};

use super::transfer::{Transfers};
use crate::owners::contract::{Committed, Drained, EpochEnded, EpochStarted, ModelsReset, Online, RejectReason, Rejected, ReplyId, RequestId};
use crate::owners::session::{SessionCalls};
use crate::owners::store::{IntentEffects, RowValue, StoreWriter};

use crate::schema::OnFailure;
use crate::runtime::{Address, Epoch, Time};

/// `purpose` of a `Mutate`: a call of the queue, and the final mutation of a transfer.
pub(super) const PURPOSE_STANDARD: u8 = 0;
pub(super) const PURPOSE_TRANSFER: u8 = 1;
/// Timers of `Expire` use ids above the ids of the retry timers (`ScheduleId(op_id)`).
pub(super) const EXPIRE_TIMER: u64 = 1 << 32;
/// Attempts of an operation before the permanent error.
pub(super) const ATTEMPT_LIMIT: u32 = 40;
/// Operations with a `Mutate` in flight at the same time: Session sends by `sequence`, so the Outbox is the only queue.
pub const DRAIN_LIMIT: usize = 1;

/// The owners Outbox sends to.
#[derive(Clone, Copy, Debug)]
pub struct OutboxPeers {
    pub store: Address<StoreWriter>,
    /// `None` in a database without a session: only local writes run, a mutation answers `NoSession`.
    pub session: Option<Address<SessionCalls>>,
    /// `None` until the owner Transfer is installed: no operation of the origin `Transfer` exists then.
    pub transfer: Option<Address<Transfers>>,
}

pub type OpId = u64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SelectKind {
    Retry,
    Discard,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReserveTarget {
    Transfer(u64),
    Group(u64),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OpSource {
    Api,
    Transfer { transfer_id: u64, cycle_id: u64 },
    TransferGroup { batch_id: u64, members: Vec<TransferMember> },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransferMember {
    pub transfer_id: u64,
    pub cycle_id: u64,
    pub row_id: String,
}

/// The action of a call: the row it changes and the rule on refusal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewOp {
    pub model: String,
    pub row_id: String,
    pub action: String,
    pub on_failure: OnFailure,
    pub origin: OpSource,
    /// The request of the mutation.
    pub path: String,
    pub body: String,
    /// The optimistic changes of the operation. `RollBack` takes them back by `op_id`.
    pub effects: IntentEffects,
    /// Milliseconds of the clock when the call was made.
    pub created_at: u64,
    /// The time the operation may wait in the queue before it is rolled back with `Rejected{Expired}`.
    pub expires_after: Option<std::time::Duration>,
}

/// An intent kept in the base (`encrypted_pending` pair when a request left before the close).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredOp {
    /// The saved answer of the server to the request of this operation: `(request_id, answer)`.
    pub answered: Option<(RequestId, crate::owners::session::StoredAnswer)>,
    pub op_id: OpId,
    pub op: NewOp,
    pub error_mark: bool,
    pub pending: Option<(RequestId, u64)>,
    /// The durable attempt counter and the cycle of the intent (`0` = a layout without the columns).
    pub attempts: u32,
    pub cycle_id: u64,
    /// The intent was dropped by the layout move or cannot be read: the operation is `Failed{RestoreFailed}`.
    pub restore_failed: Option<String>,
    /// The `(table, key)` of the element of `restore_failed` this operation stands for.
    pub restore_element: Option<(String, String)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Receipt {
    pub epoch: Epoch,
    pub op_id: OpId,
    pub cycle_id: u64,
    pub revision: i64,
    /// The row of the call as its intent batch wrote it.
    pub optimistic: Option<crate::store::Row>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OutboxOutcome {
    Confirmed { revision: i64, result: Option<String>, row_id: Option<String> },
    Rejected { error: String },
    Failed { error: String },
    Discarded,
    Ended(crate::owners::read::deliveries::EndCause),
}

pub(super) fn end_cause(reason: &RejectReason) -> crate::ffi::EndCause {
    match reason {
        RejectReason::Closed => crate::ffi::EndCause::Closed,
        RejectReason::NoSession => crate::ffi::EndCause::SessionEnded,
        RejectReason::Storage(error) => crate::ffi::EndCause::Storage { error: error.clone() },
        RejectReason::Invalid | RejectReason::StaleGeneration { .. } => crate::ffi::EndCause::Invalid { detail: format!("{reason:?}") },
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Fault {
    NoSession,
    SessionEnded,
    NotFound,
    /// The row of the batch element at this index does not exist.
    NotFoundAt(usize),
    InFlight,
    Ended(crate::ffi::EndCause),
    Closed,
    /// The intent was dropped by the layout move: only `discard` ends it.
    RestoreFailed,
    /// The session lost its pending state: `retry` waits for `session_reset`.
    StateLost,
    Internal,
    Invalid,
    Copy(String),
    Rejected(RejectReason),
}

impl Fault {
    pub(super) fn of_end(cause: crate::runtime::Cause) -> Self { if cause == crate::runtime::Cause::Close { Self::Closed } else { Self::SessionEnded } }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OutboxAnswer {
    Receipt(Receipt),
    Outcome(OutboxOutcome),
    Ok,
    Resolved(String),
    /// A local write committed: the revision of its batch and the row the batch shows.
    Written { revision: i64, shown: Option<crate::store::Row>, files_removed: usize },
    /// The first open operation of the row belongs to the owner Transfer under this action.
    Transfer { action: String },
    Err(Fault),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LocalWriteKind {
    SetLocal,
    /// The row is not in the base yet: the value sits on the row when it lands.
    SetLocalPending,
    SetEphemeral { expire_after: Option<u64> },
    /// `source` is a file outside the directory of the files: it is copied first.
    Attach { source: Option<String> },
    Detach,
    /// A source file of the platform the library already copied (`row` is the row, `values` the field).
    AttachCopy { copy: crate::store::files::CopiedFile, name: String, mime: String },
    /// The final rows of a call on a local model or of an action without a mutation.
    Model(IntentEffects),
    /// A call that changes nothing: the row and the revision of one snapshot of the Store, without a new revision.
    Snapshot,
}

#[derive(Clone, Debug)]
pub enum OutboxInput {
    ActivateEpoch(Epoch),
    Submit { epoch: Epoch, op: NewOp, reply: ReplyId },
    /// Transfer asks for the `op_id` of the optimistic effects it writes with the acceptance (`out_reserve_op`).
    ReserveOp { epoch: Epoch, target: ReserveTarget },
    /// The final mutation of a transfer whose intent the batch of Transfer wrote under `op_id` (`out_submit_intent_written`).
    SubmitReserved { epoch: Epoch, op_id: OpId, op: NewOp },
    /// The highest `op_id` a restored transfer holds: `ReserveOp` gives only higher ones.
    ReservedFloor { op_id: OpId },
    AliasesRestored { aliases: BTreeMap<(String, String), String> },
    Restored { ops: Vec<StoredOp>, pending_lost: bool },
    RestoreFailed { error: String },
    /// The saved answers of the open that no stored operation owns: the owner removes them (`out_open_orphan_answer`).
    OrphanAnswers { request_ids: Vec<RequestId> },
    RestoreReady { epoch: Epoch },
    RegisterOperations { declarations: Vec<(String, String)> },
    Committed(Committed),
    Rejected(Rejected),
    /// `errors = Some(temporary)`: the answer carries errors, the codec classified them.
    Response { op_id: OpId, rows: Vec<(String, Vec<RowValue>)>, swaps: Vec<(String, String)>, stale: Vec<(String, String)>, result: Option<String>, missing: Vec<String>, errors: Option<bool> },
    TransportError { op_id: OpId, temporary: bool },
    /// The answer of Session to the `Mutate` with `req_id`: Outbox finds its operation and has the answer read.
    Answered { req_id: RequestId, outcome: crate::owners::session::AuthorOutcome },
    /// The answer could not be landed: `detail` is the text the row and the outcome show.
    Refused { op_id: OpId, temporary: bool, detail: String },
    Dropped { op_id: OpId },
    Retry { model: String, row_id: String, action: Option<String>, reply: ReplyId },
    RetryTransfer { model: String, row_id: String, reply: ReplyId, uploads: Vec<crate::owners::store::UploadRow> },
    Discard { model: String, row_id: String, action: Option<String>, reply: ReplyId },
    /// Transfer found no open transfer of the row that precedes `outbox_min`: the call continues here with the action of that operation.
    SelectedOutbox { model: String, row_id: String, action: String, kind: SelectKind, reply: ReplyId, version: u64 },
    SelectedNone { reply: ReplyId, version: u64 },
    SelectedTransfer { reply: ReplyId, transfer_id: u64, version: u64 },
    Outcome { receipt: Receipt, reply: ReplyId },
    /// Answers `Ok` when no operation is on its way (the platform `settled`).
    WhenQuiet { reply: ReplyId },
    /// The root row the answer of the operation landed.
    /// The row the landing batch of `op_id` showed; `member` names the transfer of a group whose row is asked for.
    Landed { op_id: OpId, reply: ReplyId },
    /// The id a temporary id has after its `create` landed (the same id before).
    Resolve { model: String, row: String, reply: ReplyId },
    Online(Online),
    /// The generation of every model at the start of the epoch.
    EpochStarted(EpochStarted),
    ModelsReset(ModelsReset),
    EpochEnded(EpochEnded),
    Closing,
    /// `Tick(op_id, cycle_id)`: the retry timer of an operation (Backoff, repeat of a batch).
    RetryDue { op_id: OpId, cycle_id: u64 },
    LocalWrite { model: String, epoch: Epoch, write: LocalWriteKind, row: String, values: String, reply: ReplyId, now: Time },
    CopyDone { id: u64 },
    CopyFailed { id: u64 },
    /// `Tick(expire)` of an ephemeral row, scheduled by the owner when the write commits: `due` is the expiry it was scheduled for.
    Expire { model: String, row: String, due: Time },
    ArmExpiry { model: String, row: String, revision: u64, deadline: u64 },
    DisarmExpiry { model: String, row: String },
    /// The server removed the rows of these operations: they end `Discarded` with no `Mutate` and no rollback.
    ArmReset { reset: crate::owners::store::EphemeralReset },
    ResetField { reset: crate::owners::store::EphemeralReset },
    RowsGone { ops: Vec<OpId> },
    /// The server deleted a row (event or NOT_FOUND answer), cascade children included: every operation of it ends `Discarded`.
    RowRemoved { model: String, row_id: String, revision: u64 },
    ExpireRow { model: String, row: String, revision: u64 },
    Tick(Time),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommitKind {
    Intent,
    Attempt,
    Land,
    Rollback,
    ErrorMark,
    ClearMark,
    Requeue,
    LocalWrite,
    RemoveRow,
}

/// What the executor does. Store batches, `Mutate`, `MutationOutcome` and timers are `Effect::Send` and `Effect::Schedule` of the owner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OutboxEffect {
    Reply { reply: ReplyId, answer: OutboxAnswer },
    Drained(Drained),
    Started { epoch: Epoch },
    SnapshotRequest { epoch: Epoch },
    RestoreClassified { epoch: Epoch },
    /// Copy `source` into the directory of the files in chunks, outside the writer of SQLite; the result is `CopyDone{id}` or `CopyFailed{id}`.
    Copy { id: u64, source: String },
    /// Read the answer of an operation outside the actor: the result is `Response` or `Refused`.
    Decode(super::answer::DecodeJob),
    /// The windows of these (model, relation) pairs are stale in memory: the answer of an action that refetches them landed.
    Stale { families: Vec<(String, String)>, revision: i64 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Why {
    Rejected,
    Discarded,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    WaitingDeclaration,
    Submitting,
    Queued,
    PersistingAttempt,
    Sending,
    Landing,
    RollingBack(Why),
    MarkingFailed,
    MarkingRetry,
    Failed,
    Done,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Restoring,
    Classified,
    Ready,
}

#[derive(Clone, Debug)]
pub struct QueuedOperation {
    pub op_id: OpId,
    pub op: NewOp,
    pub stage: Stage,
    /// The durable attempt counter of the cycle.
    pub attempts: u32,
    pub cycle_id: u64,
    pub request_id: RequestId,
    pub sequence: u64,
    pub sends: u32,
    /// A `Mutate` of this operation left in this cycle.
    pub sent: bool,
    /// An unresolved `encrypted_pending` of `request_id` exists: `Restored.pending` and `pending_retained = true` set it, `pending_retained = false` clears it.
    pub pending_owned: bool,
    pub pair_in_flight: bool,
    /// `Sending` after a temporary failure: the backoff timer is set.
    pub waiting_retry: bool,
    /// The rows of the server answer, kept for every attempt to land them.
    pub landing: Vec<(String, Vec<RowValue>)>,
    pub swaps: Vec<(String, String)>,
    /// The temporary rows of the elements of a batch the answer has no row for.
    pub missing: Vec<String>,
    /// The fields of the `returns` block of the answer, as a JSON object.
    pub result: Option<String>,
    /// The (model, relation) pairs the action refetches: their windows turn stale when the answer lands.
    pub stale: Vec<(String, String)>,
    /// The root row of the answer that landed (the real id of a `create`, the row an action returned).
    pub landed: Option<String>,
    /// The row the landing batch shows: the value of the confirmed outcome, fixed by that batch.
    pub shown: Option<crate::store::Row>,
    pub outcome: Option<OutboxOutcome>,
    /// The error of the batch `MarkFailed` that is in flight, and the error shown on the row.
    pub error: Option<String>,
    /// The error of each cycle that ended with `Failed` (the outcome of a receipt is immutable).
    pub failed_errors: BTreeMap<u64, String>,
    pub retry_uploads: Vec<crate::owners::store::UploadRow>,
    pub reject_error: Option<String>,
    /// A `destroy` that waits for the rollback of the unsent `create` it cancels.
    pub cancels: Option<OpId>,
    pub before_cancel: Option<Stage>,
    pub restore_element: Option<(String, String)>,
    /// A saved answer was restored: the operation is `Landing` and waits for the rows the codec reads from the saved body.
    pub rows_pending: bool,
}
