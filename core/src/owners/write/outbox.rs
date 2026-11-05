//! Owner `Outbox`: operations of the queue, optimism, receipts and local writes (`specs/machines/outbox.md`).
//! One instance per epoch with an identity. Types of the table: inputs, effects, stages. The rows are in `step`.

use std::collections::BTreeMap;

use crate::owners::contract::{BatchId, Committed, Drained, EpochEnded, ModelsReset, Online, RejectReason, Rejected, ReplyId, RequestId};
use crate::runtime::{Cause, Effect, Effects, Epoch, ScheduleId, StateOwner, Time};

pub type OpId = u64;

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
    LocalWrite { model: String, epoch: Epoch, write: LocalWriteKind, row: String, reply: ReplyId, now: Time },
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
    /// To the executor: it adds the Backoff and the clock, then posts `RetryDue{op_id}`.
    ScheduleRetry { op_id: OpId, attempt: u32 },
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

/// Attempts of an operation before the permanent error (`design.md` 1a).
const ATTEMPT_LIMIT: u32 = 40;

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

/// What a batch sent to the Store is for.
#[derive(Clone, Debug)]
enum Batch {
    Operation { op_id: OpId, kind: CommitKind },
    Write { reply: ReplyId, row: String, model: String, expire: Option<(Time, u64)> },
}

#[derive(Debug)]
pub struct OutboxState {
    pub epoch: Epoch,
    pub online: bool,
    pub generation: BTreeMap<String, u64>,
    pub ops: BTreeMap<OpId, QueuedOperation>,
    pub next_op: OpId,
    pub next_batch: BatchId,
    next_sequence: u64,
    next_schedule: u64,
    batches: BTreeMap<BatchId, Batch>,
    submit_replies: BTreeMap<OpId, ReplyId>,
    retry_waiters: Vec<(OpId, ReplyId)>,
    discard_waiters: Vec<(OpId, ReplyId)>,
    outcome_waiters: Vec<(OpId, ReplyId)>,
    ephemeral: BTreeMap<String, Time>,
    drained: bool,
}

impl OutboxState {
    pub fn new(epoch: Epoch) -> Self {
        Self {
            epoch,
            online: true,
            generation: BTreeMap::new(),
            ops: BTreeMap::new(),
            next_op: 1,
            next_batch: 1,
            next_sequence: 1,
            next_schedule: 1,
            batches: BTreeMap::new(),
            submit_replies: BTreeMap::new(),
            retry_waiters: Vec::new(),
            discard_waiters: Vec::new(),
            outcome_waiters: Vec::new(),
            ephemeral: BTreeMap::new(),
            drained: false,
        }
    }

    pub fn stage_of(&self, op_id: OpId) -> Option<Stage> { self.ops.get(&op_id).map(|operation| operation.stage) }

    fn generation_of(&self, model: &str) -> u64 { self.generation.get(model).copied().unwrap_or(0) }

    /// No earlier operation of the same row is Submitting, Sending, Landing, RollingBack or Failed.
    fn row_free(&self, op_id: OpId) -> bool {
        let Some(operation) = self.ops.get(&op_id) else { return false };
        !self.ops.values().any(|other| {
            other.op_id < op_id
                && other.op.model == operation.op.model
                && other.op.row_id == operation.op.row_id
                && matches!(other.stage, Stage::Submitting | Stage::Sending | Stage::Landing | Stage::RollingBack(_) | Stage::Failed)
        })
    }

    fn goes_out(&self, op_id: OpId) -> bool { self.online && self.row_free(op_id) }

    fn add_operation(&mut self, op_id: OpId, op: NewOp, stage: Stage) {
        let sequence = self.next_sequence;
        self.next_sequence += 1;
        self.ops.insert(op_id, QueuedOperation { op_id, op, stage, attempts: 0, request_id: op_id, sequence, sends: 0, outcome: None });
        self.next_op = self.next_op.max(op_id + 1);
    }

    fn emit_send(&mut self, op_id: OpId, fx: &mut Effects<Outbox>) {
        let Some(operation) = self.ops.get_mut(&op_id) else { return };
        operation.sends += 1;
        fx.push(Effect::Run(OutboxEffect::Send { op_id, request_id: operation.request_id, sequence: operation.sequence }));
    }

    fn store(&mut self, op_id: OpId, kind: CommitKind, fx: &mut Effects<Outbox>) {
        let Some(operation) = self.ops.get(&op_id) else { return };
        let generation = self.generation_of(&operation.op.model);
        let batch = self.next_batch;
        self.next_batch += 1;
        self.batches.insert(batch, Batch::Operation { op_id, kind });
        fx.push(Effect::Run(OutboxEffect::Store { op_id: Some(op_id), batch, kind, generation }));
    }

    fn reply(fx: &mut Effects<Outbox>, reply: ReplyId, answer: OutboxAnswer) { fx.push(Effect::Run(OutboxEffect::Reply { reply, answer })); }

    fn take_waiters(list: &mut Vec<(OpId, ReplyId)>, op_id: OpId) -> Vec<ReplyId> {
        let (mine, rest): (Vec<_>, Vec<_>) = std::mem::take(list).into_iter().partition(|(id, _)| *id == op_id);
        *list = rest;
        mine.into_iter().map(|(_, reply)| reply).collect()
    }

    fn give_outcome(&mut self, op_id: OpId, outcome: &OutboxOutcome, fx: &mut Effects<Outbox>) {
        for reply in Self::take_waiters(&mut self.outcome_waiters, op_id) {
            Self::reply(fx, reply, OutboxAnswer::Outcome(outcome.clone()));
        }
    }

    /// The operation ends: `outcome` goes to the waiters and, for a transfer, to the Transfer owner. The next operation of the row goes out when `next_goes`.
    fn finish(&mut self, op_id: OpId, outcome: OutboxOutcome, next_goes: bool, fx: &mut Effects<Outbox>) {
        let Some(operation) = self.ops.get_mut(&op_id) else { return };
        operation.stage = Stage::Done;
        operation.outcome = Some(outcome.clone());
        let origin = operation.op.origin.clone();
        let (model, row) = (operation.op.model.clone(), operation.op.row_id.clone());
        if let Origin::Transfer(key) = origin {
            fx.push(Effect::Run(OutboxEffect::TransferOutcome { key, outcome: outcome.clone() }));
        }
        self.give_outcome(op_id, &outcome, fx);
        if next_goes {
            self.kick_row(&model, &row, fx);
        }
    }

    /// The first Queued operation of the row goes out when the row is free.
    fn kick_row(&mut self, model: &str, row: &str, fx: &mut Effects<Outbox>) {
        let next = self.ops.values().find(|operation| operation.stage == Stage::Queued && operation.op.model == model && operation.op.row_id == row).map(|operation| operation.op_id);
        if let Some(op_id) = next.filter(|op_id| self.goes_out(*op_id)) {
            self.start_sending(op_id, fx);
        }
    }

    fn start_sending(&mut self, op_id: OpId, fx: &mut Effects<Outbox>) {
        if let Some(operation) = self.ops.get_mut(&op_id) {
            operation.stage = Stage::Sending;
            operation.attempts = 0;
        }
        self.emit_send(op_id, fx);
    }

    /// `Failed` -> start a new rollback with the `Discarded` reason, or fail the operation in `keep` mode.
    fn reject_or_keep(&mut self, op_id: OpId, fx: &mut Effects<Outbox>) {
        let keep = self.ops.get(&op_id).is_some_and(|operation| operation.op.on_reject == OnReject::Keep);
        if keep {
            if let Some(operation) = self.ops.get_mut(&op_id) {
                operation.stage = Stage::Failed;
            }
            self.store(op_id, CommitKind::ErrorMark, fx);
            let failed = OutboxOutcome::Failed { error: "refused".into() };
            self.give_outcome(op_id, &failed, fx);
        } else {
            if let Some(operation) = self.ops.get_mut(&op_id) {
                operation.stage = Stage::RollingBack(Why::Rejected);
            }
            self.store(op_id, CommitKind::Rollback, fx);
        }
    }

    /// The first operation of the row that is not Done: the one `retry` and `discard` address.
    fn open_on_row(&self, model: &str, row: &str) -> Option<OpId> {
        self.ops.values().find(|operation| operation.op.model == model && operation.op.row_id == row && operation.stage != Stage::Done).map(|operation| operation.op_id)
    }

    fn interrupt_all(&mut self, closing: bool, fx: &mut Effects<Outbox>) {
        let open: Vec<OpId> = self.ops.values().filter(|operation| operation.stage != Stage::Done).map(|operation| operation.op_id).collect();
        for op_id in open {
            let Some(stage) = self.stage_of(op_id) else { continue };
            let cancels = if closing { matches!(stage, Stage::Submitting | Stage::Sending | Stage::Landing | Stage::RollingBack(_)) } else { stage == Stage::Sending };
            if cancels {
                fx.push(Effect::Run(OutboxEffect::CancelRequest { op_id }));
            }
            if let Some(reply) = self.submit_replies.remove(&op_id) {
                Self::reply(fx, reply, OutboxAnswer::Err(Fault::Interrupted));
            }
            for reply in Self::take_waiters(&mut self.retry_waiters, op_id).into_iter().chain(Self::take_waiters(&mut self.discard_waiters, op_id)) {
                Self::reply(fx, reply, OutboxAnswer::Err(Fault::Interrupted));
            }
            if let Some(operation) = self.ops.get_mut(&op_id) {
                operation.stage = Stage::Done;
                operation.outcome = Some(OutboxOutcome::Interrupted);
            }
            let origin = self.ops[&op_id].op.origin.clone();
            if let (Origin::Transfer(key), false) = (origin, closing) {
                fx.push(Effect::Run(OutboxEffect::TransferOutcome { key, outcome: OutboxOutcome::Interrupted }));
            }
            self.give_outcome(op_id, &OutboxOutcome::Interrupted, fx);
        }
        let writes: Vec<BatchId> = self.batches.iter().filter(|(_, batch)| matches!(batch, Batch::Write { .. })).map(|(id, _)| *id).collect();
        for id in writes {
            if let Some(Batch::Write { reply, .. }) = self.batches.remove(&id) {
                Self::reply(fx, reply, OutboxAnswer::Err(Fault::Interrupted));
            }
        }
    }

    fn committed(&mut self, committed: Committed, fx: &mut Effects<Outbox>) {
        let Some(batch) = self.batches.remove(&committed.batch) else { return };
        match batch {
            Batch::Write { reply, row, expire, .. } => {
                Self::reply(fx, reply, OutboxAnswer::Ok);
                if let Some((due, after)) = expire {
                    self.ephemeral.insert(row.clone(), due);
                    let id = ScheduleId(self.next_schedule);
                    self.next_schedule += 1;
                    fx.push(Effect::Schedule { id, after: std::time::Duration::from_millis(after), input: OutboxInput::Expire { row, due } });
                }
            }
            Batch::Operation { op_id, kind } => {
                let Some(stage) = self.stage_of(op_id) else { return };
                match (kind, stage) {
                    (CommitKind::Intent, Stage::Submitting) => {
                        let receipt = Receipt { epoch: self.epoch, op_id, revision: committed.revision };
                        if let Some(reply) = self.submit_replies.remove(&op_id) {
                            Self::reply(fx, reply, OutboxAnswer::Receipt(receipt.clone()));
                        }
                        for reply in Self::take_waiters(&mut self.retry_waiters, op_id) {
                            Self::reply(fx, reply, OutboxAnswer::Receipt(receipt.clone()));
                        }
                        if self.goes_out(op_id) {
                            self.start_sending(op_id, fx);
                        } else if let Some(operation) = self.ops.get_mut(&op_id) {
                            operation.stage = Stage::Queued;
                        }
                    }
                    (CommitKind::Land, Stage::Landing) => self.finish(op_id, OutboxOutcome::Confirmed { revision: committed.revision }, true, fx),
                    (CommitKind::Rollback, Stage::RollingBack(why)) => {
                        for reply in Self::take_waiters(&mut self.discard_waiters, op_id) {
                            Self::reply(fx, reply, OutboxAnswer::Ok);
                        }
                        let outcome = if why == Why::Discarded { OutboxOutcome::Discarded } else { OutboxOutcome::Rejected { error: "refused".into() } };
                        self.finish(op_id, outcome, true, fx);
                    }
                    _ => {}
                }
            }
        }
    }

    fn rejected(&mut self, rejected: Rejected, fx: &mut Effects<Outbox>) {
        let Some(batch) = self.batches.remove(&rejected.batch) else { return };
        let stale = matches!(rejected.reason, RejectReason::StaleGeneration { .. });
        let storage = matches!(rejected.reason, RejectReason::Storage(_));
        match batch {
            Batch::Write { reply, row, model, expire } => {
                if stale {
                    let generation = self.generation_of(&model);
                    let id = self.next_batch;
                    self.next_batch += 1;
                    self.batches.insert(id, Batch::Write { reply, row, model, expire });
                    fx.push(Effect::Run(OutboxEffect::Store { op_id: None, batch: id, kind: CommitKind::LocalWrite, generation }));
                } else {
                    Self::reply(fx, reply, OutboxAnswer::Err(Fault::Rejected(rejected.reason)));
                }
            }
            Batch::Operation { op_id, kind } => {
                let Some(stage) = self.stage_of(op_id) else { return };
                match (kind, stage) {
                    (CommitKind::Intent, Stage::Submitting) if stale => self.store(op_id, CommitKind::Intent, fx),
                    (CommitKind::Intent, Stage::Submitting) => {
                        if let Some(reply) = self.submit_replies.remove(&op_id) {
                            Self::reply(fx, reply, OutboxAnswer::Err(Fault::Rejected(rejected.reason.clone())));
                        }
                        for reply in Self::take_waiters(&mut self.retry_waiters, op_id) {
                            Self::reply(fx, reply, OutboxAnswer::Err(Fault::Rejected(rejected.reason.clone())));
                        }
                        if let Some(operation) = self.ops.get_mut(&op_id) {
                            operation.stage = Stage::Done;
                            operation.outcome = Some(OutboxOutcome::Interrupted);
                        }
                    }
                    (CommitKind::Land | CommitKind::Rollback, Stage::Landing | Stage::RollingBack(_)) if stale => self.store(op_id, kind, fx),
                    (CommitKind::Land | CommitKind::Rollback, Stage::Landing | Stage::RollingBack(_)) if storage => {
                        let attempt = self.ops[&op_id].attempts;
                        fx.push(Effect::Run(OutboxEffect::ScheduleRetry { op_id, attempt }));
                    }
                    (CommitKind::Land | CommitKind::Rollback, Stage::Landing | Stage::RollingBack(_)) => {
                        for reply in Self::take_waiters(&mut self.discard_waiters, op_id) {
                            Self::reply(fx, reply, OutboxAnswer::Err(Fault::Interrupted));
                        }
                        self.finish(op_id, OutboxOutcome::Interrupted, false, fx);
                    }
                    _ => {}
                }
            }
        }
    }

    fn transport_failed(&mut self, op_id: OpId, temporary: bool, fx: &mut Effects<Outbox>) {
        if self.stage_of(op_id) != Some(Stage::Sending) {
            return;
        }
        if !temporary {
            self.reject_or_keep(op_id, fx);
            return;
        }
        if !self.online {
            self.ops.get_mut(&op_id).map(|operation| operation.stage = Stage::Queued);
            return;
        }
        let attempts = self.ops[&op_id].attempts;
        if attempts + 1 < ATTEMPT_LIMIT {
            self.ops.get_mut(&op_id).map(|operation| operation.attempts += 1);
            fx.push(Effect::Run(OutboxEffect::ScheduleRetry { op_id, attempt: attempts + 1 }));
        } else {
            self.reject_or_keep(op_id, fx);
        }
    }

    fn retry_due(&mut self, op_id: OpId, fx: &mut Effects<Outbox>) {
        match self.stage_of(op_id) {
            Some(Stage::Sending) if self.online => self.emit_send(op_id, fx),
            Some(Stage::Sending) => { self.ops.get_mut(&op_id).map(|operation| operation.stage = Stage::Queued); }
            Some(Stage::Landing) => self.store(op_id, CommitKind::Land, fx),
            Some(Stage::RollingBack(_)) => self.store(op_id, CommitKind::Rollback, fx),
            _ => {}
        }
    }

    fn retry(&mut self, model: &str, row: &str, reply: ReplyId, fx: &mut Effects<Outbox>) {
        let Some(op_id) = self.open_on_row(model, row) else {
            return Self::reply(fx, reply, OutboxAnswer::Err(Fault::NotFound));
        };
        let receipt = Receipt { epoch: self.epoch, op_id, revision: 0 };
        match self.ops[&op_id].stage {
            Stage::Submitting => self.retry_waiters.push((op_id, reply)),
            Stage::Queued | Stage::Sending | Stage::Landing => Self::reply(fx, reply, OutboxAnswer::Receipt(receipt)),
            Stage::RollingBack(_) => Self::reply(fx, reply, OutboxAnswer::Err(Fault::InFlight)),
            Stage::Failed => {
                self.store(op_id, CommitKind::ClearMark, fx);
                if self.goes_out_ignoring_failed(op_id) {
                    self.start_sending(op_id, fx);
                } else if let Some(operation) = self.ops.get_mut(&op_id) {
                    operation.stage = Stage::Queued;
                }
                Self::reply(fx, reply, OutboxAnswer::Receipt(receipt));
            }
            Stage::Done => Self::reply(fx, reply, OutboxAnswer::Err(Fault::NotFound)),
        }
    }

    /// A Failed operation is itself in a blocking state; only earlier operations of the row count.
    fn goes_out_ignoring_failed(&self, op_id: OpId) -> bool { self.online && self.row_free(op_id) }

    fn discard(&mut self, model: &str, row: &str, reply: ReplyId, fx: &mut Effects<Outbox>) {
        let Some(op_id) = self.open_on_row(model, row) else {
            return Self::reply(fx, reply, OutboxAnswer::Err(Fault::NotFound));
        };
        match self.ops[&op_id].stage {
            Stage::Submitting | Stage::Sending | Stage::Landing => Self::reply(fx, reply, OutboxAnswer::Err(Fault::InFlight)),
            Stage::Queued | Stage::Failed => {
                self.discard_waiters.push((op_id, reply));
                self.ops.get_mut(&op_id).map(|operation| operation.stage = Stage::RollingBack(Why::Discarded));
                self.store(op_id, CommitKind::Rollback, fx);
            }
            Stage::RollingBack(_) => Self::reply(fx, reply, OutboxAnswer::Ok),
            Stage::Done => Self::reply(fx, reply, OutboxAnswer::Err(Fault::NotFound)),
        }
    }

    fn outcome(&mut self, receipt: Receipt, reply: ReplyId, fx: &mut Effects<Outbox>) {
        if receipt.epoch != self.epoch {
            return Self::reply(fx, reply, OutboxAnswer::Err(Fault::NoSession));
        }
        match self.ops.get(&receipt.op_id) {
            None => Self::reply(fx, reply, OutboxAnswer::Err(Fault::NotFound)),
            Some(operation) if operation.stage == Stage::Done => Self::reply(fx, reply, OutboxAnswer::Outcome(operation.outcome.clone().unwrap_or(OutboxOutcome::Interrupted))),
            Some(_) => self.outcome_waiters.push((receipt.op_id, reply)),
        }
    }

    fn restored(&mut self, stored: Vec<StoredOp>, fx: &mut Effects<Outbox>) {
        let mut stored = stored;
        stored.sort_by_key(|item| item.op_id);
        for item in stored {
            let StoredOp { op_id, op, error_mark, pending } = item;
            self.add_operation(op_id, op, Stage::Queued);
            if let (Some((request_id, sequence)), Some(operation)) = (pending, self.ops.get_mut(&op_id)) {
                operation.request_id = request_id;
                operation.sequence = sequence;
            }
            if error_mark {
                self.ops.get_mut(&op_id).map(|operation| operation.stage = Stage::Failed);
            } else if self.goes_out(op_id) {
                self.start_sending(op_id, fx);
            }
        }
    }

    fn local_write(&mut self, model: String, epoch: Epoch, write: LocalWriteKind, row: String, reply: ReplyId, now: Time, fx: &mut Effects<Outbox>) {
        if epoch != self.epoch {
            return Self::reply(fx, reply, OutboxAnswer::Err(Fault::NoSession));
        }
        let expire = match write {
            LocalWriteKind::SetEphemeral { expire_after: Some(after) } => Some((Time(now.0 + after), after)),
            _ => None,
        };
        let generation = self.generation_of(&model);
        let batch = self.next_batch;
        self.next_batch += 1;
        self.batches.insert(batch, Batch::Write { reply, row, model, expire });
        fx.push(Effect::Run(OutboxEffect::Store { op_id: None, batch, kind: CommitKind::LocalWrite, generation }));
    }

    fn expire(&mut self, row: String, due: Time, fx: &mut Effects<Outbox>) {
        if self.ephemeral.get(&row).is_some_and(|written| *written > due) {
            return;
        }
        self.ephemeral.remove(&row);
        let batch = self.next_batch;
        self.next_batch += 1;
        fx.push(Effect::Run(OutboxEffect::Store { op_id: None, batch, kind: CommitKind::RemoveRow, generation: 0 }));
    }
}

pub struct Outbox;

impl StateOwner for Outbox {
    const EPOCHED: bool = true;

    type State = OutboxState;
    type Input = OutboxInput;
    type Effect = OutboxEffect;

    fn step(state: &mut OutboxState, input: OutboxInput) -> Effects<Self> {
        let mut fx: Effects<Self> = Vec::new();
        match input {
            OutboxInput::Submit { epoch, op, reply } => {
                if epoch != state.epoch {
                    OutboxState::reply(&mut fx, reply, OutboxAnswer::Err(Fault::NoSession));
                } else {
                    let op_id = state.next_op;
                    state.add_operation(op_id, op, Stage::Submitting);
                    state.submit_replies.insert(op_id, reply);
                    state.store(op_id, CommitKind::Intent, &mut fx);
                }
            }
            OutboxInput::Restored(stored) => state.restored(stored, &mut fx),
            OutboxInput::Committed(committed) => state.committed(committed, &mut fx),
            OutboxInput::Rejected(rejected) => state.rejected(rejected, &mut fx),
            OutboxInput::Response { op_id } => {
                if state.stage_of(op_id) == Some(Stage::Sending) {
                    state.ops.get_mut(&op_id).map(|operation| operation.stage = Stage::Landing);
                    state.store(op_id, CommitKind::Land, &mut fx);
                }
            }
            OutboxInput::TransportError { op_id, temporary } => state.transport_failed(op_id, temporary, &mut fx),
            OutboxInput::Dropped { op_id } => state.transport_failed(op_id, true, &mut fx),
            OutboxInput::Retry { model, row_id, reply } => state.retry(&model, &row_id, reply, &mut fx),
            OutboxInput::Discard { model, row_id, reply } => state.discard(&model, &row_id, reply, &mut fx),
            OutboxInput::Outcome { receipt, reply } => state.outcome(receipt, reply, &mut fx),
            OutboxInput::Online(Online(on)) => {
                state.online = on;
                if on {
                    let queued: Vec<OpId> = state.ops.values().filter(|operation| operation.stage == Stage::Queued).map(|operation| operation.op_id).collect();
                    for op_id in queued {
                        if state.stage_of(op_id) == Some(Stage::Queued) && state.goes_out(op_id) {
                            state.start_sending(op_id, &mut fx);
                        }
                    }
                }
            }
            OutboxInput::ModelsReset(reset) => {
                for model in reset.models {
                    state.generation.insert(model, reset.generation);
                }
            }
            OutboxInput::EpochEnded(_) => state.interrupt_all(false, &mut fx),
            OutboxInput::Closing => {
                state.interrupt_all(true, &mut fx);
                if !std::mem::replace(&mut state.drained, true) {
                    fx.push(Effect::Run(OutboxEffect::Drained(Drained { owner: "outbox" })));
                }
            }
            OutboxInput::RetryDue { op_id } => state.retry_due(op_id, &mut fx),
            OutboxInput::LocalWrite { model, epoch, write, row, reply, now } => state.local_write(model, epoch, write, row, reply, now, &mut fx),
            OutboxInput::Expire { row, due } => state.expire(row, due, &mut fx),
            OutboxInput::Tick(_) => {}
        }
        fx
    }

    fn tick(now: Time) -> OutboxInput { OutboxInput::Tick(now) }

    fn epoch_ended(epoch: Epoch, cause: Cause) -> OutboxInput { OutboxInput::EpochEnded(EpochEnded { epoch, cause }) }

    fn closing() -> OutboxInput { OutboxInput::Closing }
}
