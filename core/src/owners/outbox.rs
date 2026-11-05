//! Owner `Outbox`: operations of the queue, optimism, receipts and local writes (`specs/machines/outbox.md`).
//! One instance per epoch with an identity. Types of the table: inputs, effects, stages. The rows are in `step`.

use std::collections::BTreeMap;

use super::contract::{BatchId, Committed, Drained, EpochEnded, ModelsReset, Online, RejectReason, Rejected, RequestId};
use crate::runtime::{Cause, Effects, Epoch, StateOwner, Time};

pub type OpId = u64;
pub type ReplyId = u64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OnReject {
    Rollback,
    Keep,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Origin {
    Api,
    Transfer(String),
}

/// The action of a call: the row it changes and the rule on refusal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewOp {
    pub model: String,
    pub row_id: String,
    pub action: String,
    pub on_reject: OnReject,
    pub origin: Origin,
}

/// An intent kept in the base (`encrypted_pending` pair when a request left before the close).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredOp {
    pub op_id: OpId,
    pub op: NewOp,
    pub error_mark: bool,
    pub pending: Option<(RequestId, u64)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Receipt {
    pub epoch: Epoch,
    pub op_id: OpId,
    pub revision: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OutboxOutcome {
    Confirmed { revision: i64 },
    Rejected { error: String },
    Failed { error: String },
    Discarded,
    Interrupted,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Fault {
    NoSession,
    NotFound,
    InFlight,
    Interrupted,
    Rejected(RejectReason),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OutboxAnswer {
    Receipt(Receipt),
    Outcome(OutboxOutcome),
    Ok,
    Err(Fault),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LocalWriteKind {
    SetLocal,
    SetEphemeral { expire_after: Option<u64> },
    Attach,
    Detach,
}

#[derive(Clone, Debug)]
pub enum OutboxInput {
    Submit { epoch: Epoch, op: NewOp, reply: ReplyId },
    Restored(Vec<StoredOp>),
    Committed(Committed),
    Rejected(Rejected),
    Response { op_id: OpId },
    TransportError { op_id: OpId, temporary: bool },
    Dropped { op_id: OpId },
    Retry { model: String, row_id: String, reply: ReplyId },
    Discard { model: String, row_id: String, reply: ReplyId },
    Outcome { receipt: Receipt, reply: ReplyId },
    Online(Online),
    ModelsReset(ModelsReset),
    EpochEnded(EpochEnded),
    Closing,
    /// `Tick(op_id)`: the retry timer of an operation (Backoff, landing retry).
    RetryDue { op_id: OpId },
    LocalWrite { epoch: Epoch, write: LocalWriteKind, row: String, reply: ReplyId, now: Time },
    /// `Tick(expire)` of an ephemeral row, scheduled by the owner when the write commits: `due` is the expiry it was scheduled for.
    Expire { row: String, due: Time },
    Tick(Time),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommitKind {
    Intent,
    Land,
    Rollback,
    ErrorMark,
    ClearMark,
    LocalWrite,
    RemoveRow,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OutboxEffect {
    Store { op_id: Option<OpId>, batch: BatchId, kind: CommitKind, generation: u64 },
    Send { op_id: OpId, request_id: RequestId, sequence: u64 },
    CancelRequest { op_id: OpId },
    Reply { reply: ReplyId, answer: OutboxAnswer },
    /// `Transfer MutationOutcome(key, outcome)`.
    TransferOutcome { key: String, outcome: OutboxOutcome },
    Drained(Drained),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Why {
    Rejected,
    Discarded,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    Submitting,
    Queued,
    Sending,
    Landing,
    RollingBack(Why),
    Failed,
    Done,
}

#[derive(Clone, Debug)]
pub struct QueuedOperation {
    pub op_id: OpId,
    pub op: NewOp,
    pub stage: Stage,
    pub attempts: u32,
    pub request_id: RequestId,
    pub sequence: u64,
    pub sends: u32,
    pub outcome: Option<OutboxOutcome>,
}

#[derive(Debug)]
pub struct OutboxState {
    pub epoch: Epoch,
    pub online: bool,
    pub generation: BTreeMap<String, u64>,
    pub ops: BTreeMap<OpId, QueuedOperation>,
    pub next_op: OpId,
    pub next_batch: BatchId,
}

impl OutboxState {
    pub fn new(epoch: Epoch) -> Self { Self { epoch, online: true, generation: BTreeMap::new(), ops: BTreeMap::new(), next_op: 1, next_batch: 1 } }

    pub fn stage_of(&self, op_id: OpId) -> Option<Stage> { self.ops.get(&op_id).map(|operation| operation.stage) }
}

pub struct Outbox;

impl StateOwner for Outbox {
    const EPOCHED: bool = true;

    type State = OutboxState;
    type Input = OutboxInput;
    type Effect = OutboxEffect;

    // TEMPORARY: the rows of the table are written after the contract of direction B lands.
    fn step(_state: &mut OutboxState, _input: OutboxInput) -> Effects<Self> { Vec::new() }

    fn tick(now: Time) -> OutboxInput { OutboxInput::Tick(now) }

    fn epoch_ended(epoch: Epoch, cause: Cause) -> OutboxInput { OutboxInput::EpochEnded(EpochEnded { epoch, cause }) }

    fn closing() -> OutboxInput { OutboxInput::Closing }
}
