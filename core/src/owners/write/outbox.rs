//! Owner `Outbox`: operations of the queue, optimism, receipts and local writes.
//! One instance per epoch with an identity. The types are in `outbox_types`, the rows of the table in `outbox_{commit,flow,restore,local}`.

use std::collections::{BTreeMap, BTreeSet};

pub use super::outbox_types::*;
use super::transfer::TransferInput;
use crate::owners::contract::{BatchAuthor, BatchId, EpochEnded, Online, RejectReason, ReplyId};
use crate::owners::session::{SessionInput, OUTBOX_AUTHOR};
use crate::owners::store::{StoreBatch, StoreInput, StoreOp};
use crate::pacing::RetryPolicy;
use crate::schema::OnFailure;
use crate::owners::store::StoreWriter;
use crate::runtime::{Address, Cause, Effect, Effects, Epoch, Outbound, ScheduleId, StateOwner, Time};

/// What a batch sent to the Store is for.
#[derive(Clone, Debug)]
pub(super) enum Batch {
    Operation { op_id: OpId, kind: CommitKind },
    Write { reply: ReplyId, row: String, values: String, model: String, kind: LocalWriteKind, expire: Option<(Time, u64)> },
}

#[derive(Debug)]
pub struct OutboxState {
    pub(super) peers: OutboxPeers,
    pub epoch: Epoch,
    pub online: bool,
    pub generation: BTreeMap<String, u64>,
    /// The generation of the models without an entry in `generation`.
    pub(super) base_generation: u64,
    pub ops: BTreeMap<OpId, QueuedOperation>,
    pub next_op: OpId,
    pub next_batch: BatchId,
    pub(super) next_sequence: u64,
    pub(super) next_schedule: u64,
    pub(super) batches: BTreeMap<BatchId, Batch>,
    pub(super) submit_replies: BTreeMap<OpId, ReplyId>,
    pub(super) retry_waiters: Vec<(OpId, ReplyId)>,
    pub(super) discard_waiters: Vec<(OpId, ReplyId)>,
    pub(super) outcome_waiters: Vec<(OpId, ReplyId)>,
    pub(super) ephemeral: BTreeMap<String, Time>,
    pub(super) expiry: BTreeMap<(String, String), (ScheduleId, u64)>,
    pub(super) resets: BTreeMap<(String, String, String), (ScheduleId, u64)>,
    pub(super) drained: bool,
    pub(super) author: BatchAuthor,
    pub(super) quiet_waiters: Vec<ReplyId>,
    pub(super) closing: bool,
    pub(super) queue_error: Option<String>,
    pub phase: Phase,
    pub(super) ended: bool,
    pub(super) held: Vec<OutboxInput>,
    pub(super) declarations: BTreeSet<(String, String)>,
    /// Restored operations that wait for `RestoreReady` to write their next attempt.
    pub(super) deferred: Vec<OpId>,
    pub(super) waiting: BTreeMap<OpId, (StoredOp, bool)>,
    pub(super) aliases: BTreeMap<(String, String), String>,
    pub(super) copying: BTreeMap<u64, (ReplyId, String, LocalWriteKind, String, String, Time)>,
    pub(super) next_copy: u64,
}

impl OutboxState {
    /// The owner LocalWrites: the same machine in every epoch, it takes only the local writes and talks to Store under its own author.
    pub fn local_writes(epoch: Epoch, store: Address<StoreWriter>) -> Self {
        let mut state = Self::new(epoch, OutboxPeers { store, session: None, transfer: None });
        state.author = BatchAuthor::LocalWrites;
        state.phase = Phase::Ready;
        state
    }

    pub fn new(epoch: Epoch, peers: OutboxPeers) -> Self {
        Self {
            peers,
            epoch,
            online: true,
            generation: BTreeMap::new(),
            base_generation: 0,
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
            expiry: BTreeMap::new(),
            resets: BTreeMap::new(),
            drained: false,
            author: BatchAuthor::Outbox,
            quiet_waiters: Vec::new(),
            closing: false,
            queue_error: None,
            phase: Phase::Restoring,
            ended: false,
            held: Vec::new(),
            declarations: BTreeSet::new(),
            deferred: Vec::new(),
            waiting: BTreeMap::new(),
            aliases: BTreeMap::new(),
            copying: BTreeMap::new(),
            next_copy: 1,
        }
    }

    /// A new epoch: a fresh instance. The platform call gate keeps calls out of an epoch without an identity.
    pub(super) fn begin_epoch(&mut self, epoch: Epoch) {
        let declarations = std::mem::take(&mut self.declarations);
        *self = if self.author == BatchAuthor::LocalWrites { Self::local_writes(epoch, self.peers.store) } else { Self::new(epoch, self.peers) };
        self.declarations = declarations;
    }

    pub fn stage_of(&self, op_id: OpId) -> Option<Stage> { self.ops.get(&op_id).map(|operation| operation.stage) }

    pub(super) fn generation_of(&self, model: &str) -> u64 { self.generation.get(model).copied().unwrap_or(self.base_generation) }

    /// An earlier operation of the same row is not Done.
    pub(super) fn row_busy(&self, op_id: OpId) -> bool {
        let Some(operation) = self.ops.get(&op_id) else { return true };
        self.ops.values().any(|other| other.op_id < op_id && other.op.model == operation.op.model && other.op.row_id == operation.op.row_id && other.stage != Stage::Done)
    }

    pub(super) fn goes_out(&self, op_id: OpId) -> bool { self.online && !self.row_busy(op_id) }

    pub(super) fn add_operation(&mut self, op_id: OpId, op: NewOp, stage: Stage) {
        let sequence = self.next_sequence;
        self.next_sequence += 1;
        self.ops.insert(
            op_id,
            QueuedOperation {
                op_id,
                op,
                stage,
                attempts: 1,
                cycle_id: 1,
                request_id: crate::owners::contract::next_request_id(),
                sequence,
                sends: 0,
                sent: false,
                waiting_retry: false,
                landing: Vec::new(),
                swaps: Vec::new(),
                result: None,
                stale: Vec::new(),
                landed: None,
                outcome: None,
                error: None,
                failed_errors: BTreeMap::new(),
                reject_error: None,
                cancels: None,
                restore_element: None,
                rows_pending: false,
            },
        );
        self.next_op = self.next_op.max(op_id + 1);
    }

    pub(super) fn emit_send(&mut self, op_id: OpId, fx: &mut Effects<Outbox>) {
        let Some(operation) = self.ops.get_mut(&op_id) else { return };
        operation.sends += 1;
        operation.sent = true;
        operation.waiting_retry = false;
        let purpose = if matches!(operation.op.origin, OpSource::Transfer(_)) { PURPOSE_TRANSFER } else { PURPOSE_STANDARD };
        let owner = Some(crate::transport::RequestOwner::Operation { seq: i64::try_from(op_id).unwrap_or(i64::MAX) });
        let mutate = SessionInput::Mutate { author: OUTBOX_AUTHOR, owner, req_id: operation.request_id, epoch: self.epoch, path: operation.op.path.clone(), body: operation.op.body.clone(), purpose };
        if let Some(session) = self.peers.session { fx.push(Effect::Send(Outbound::new(session, mutate))); }
    }

    pub(super) fn store(&mut self, op_id: OpId, kind: CommitKind, fx: &mut Effects<Outbox>) {
        let Some(operation) = self.ops.get(&op_id) else { return };
        let generation = self.generation_of(&operation.op.model);
        let attempt = StoreOp::Attempt { op_id, attempts: if kind == CommitKind::ClearMark { 1 } else { operation.attempts + 1 }, cycle_id: operation.cycle_id };
        let ops = match kind {
            CommitKind::Intent => vec![StoreOp::Intent {
                op_id,
                model: operation.op.model.clone(),
                row_id: operation.op.row_id.clone(),
                action: operation.op.action.clone(),
                payload: operation.op.body.clone(),
                effects: operation.op.effects.clone(),
            }],
            CommitKind::Attempt => vec![attempt],
            CommitKind::Land => operation
                .swaps
                .iter()
                .map(|(temp, real)| StoreOp::Swap { model: operation.op.model.clone(), temp: temp.clone(), real: real.clone() })
                .chain(operation.landing.iter().map(|(model, rows)| StoreOp::Rows { model: model.clone(), rows: rows.clone() }))
                .chain((!operation.stale.is_empty()).then(|| StoreOp::StaleFamilies { families: operation.stale.clone() }))
                .chain([StoreOp::RemoveIntent { op_id }, StoreOp::RemoveAnswer { request_id: operation.request_id }])
                .collect(),
            CommitKind::Rollback if operation.restore_element.is_some() => operation.restore_element.iter().map(|(table, key)| StoreOp::DiscardRestoreFailed { table: table.clone(), key: key.clone() }).chain([StoreOp::RemoveIntent { op_id }]).collect(),
            CommitKind::Rollback => vec![StoreOp::RollBack { op_id }, StoreOp::RemoveIntent { op_id }, StoreOp::RemoveAnswer { request_id: operation.request_id }],
            CommitKind::ErrorMark => vec![StoreOp::MarkError { op_id, error: Some(operation.error.clone().unwrap_or_else(|| "refused".into())) }, StoreOp::RemoveAnswer { request_id: operation.request_id }],
            CommitKind::ClearMark => vec![StoreOp::MarkError { op_id, error: None }, attempt],
            CommitKind::LocalWrite | CommitKind::RemoveRow => Vec::new(),
        };
        let intents = u64::from(kind == CommitKind::Intent);
        let basis = BTreeMap::from([(operation.op.model.clone(), generation)]);
        let batch = self.next_batch;
        self.next_batch += 1;
        self.batches.insert(batch, Batch::Operation { op_id, kind });
        self.commit(batch, basis, ops, intents, fx);
    }

    pub(super) fn commit(&self, id: BatchId, basis: BTreeMap<String, u64>, ops: Vec<StoreOp>, intents: u64, fx: &mut Effects<Outbox>) {
        let mut batch = StoreBatch { author: self.author, id, epoch: self.epoch, basis, rows: Vec::new(), intents, credential: None, ops };
        let entries = batch.models().into_iter().map(|model| {
            let generation = batch.basis.get(&model).copied().unwrap_or_else(|| self.generation_of(&model));
            (model, generation)
        }).chain(batch.basis.clone());
        batch.basis = crate::owners::store::basis_for(entries);
        fx.push(Effect::Send(Outbound::new(self.peers.store, StoreInput::Commit(batch))));
    }

    pub(super) fn retry_timer(&self, op_id: OpId, fx: &mut Effects<Outbox>) {
        let Some(operation) = self.ops.get(&op_id) else { return };
        let after = RetryPolicy::Outbox.backoff().delay(operation.attempts.saturating_sub(1));
        fx.push(Effect::Schedule { id: ScheduleId(op_id), after, input: OutboxInput::RetryDue { op_id, cycle_id: operation.cycle_id } });
    }

    pub(super) fn reply(fx: &mut Effects<Outbox>, reply: ReplyId, answer: OutboxAnswer) { fx.push(Effect::Run(OutboxEffect::Reply { reply, answer })); }

    pub(super) fn take_waiters(list: &mut Vec<(OpId, ReplyId)>, op_id: OpId) -> Vec<ReplyId> {
        let (mine, rest): (Vec<_>, Vec<_>) = std::mem::take(list).into_iter().partition(|(id, _)| *id == op_id);
        *list = rest;
        mine.into_iter().map(|(_, reply)| reply).collect()
    }

    pub(super) fn give_outcome(&mut self, op_id: OpId, outcome: &OutboxOutcome, fx: &mut Effects<Outbox>) {
        for reply in Self::take_waiters(&mut self.outcome_waiters, op_id) {
            Self::reply(fx, reply, OutboxAnswer::Outcome(outcome.clone()));
        }
    }

    pub(super) fn receipt(&self, op_id: OpId, revision: i64) -> Receipt { Receipt { epoch: self.epoch, op_id, cycle_id: self.ops.get(&op_id).map_or(0, |operation| operation.cycle_id), revision } }

    pub(super) fn tell_transfer(&self, op_id: OpId, outcome: &OutboxOutcome, fx: &mut Effects<Outbox>) {
        let Some(operation) = self.ops.get(&op_id) else { return };
        if let (OpSource::Transfer(key), Some(transfer)) = (&operation.op.origin, self.peers.transfer) {
            fx.push(Effect::Send(Outbound::new(transfer, TransferInput::MutationOutcome { key: key.clone(), outcome: outcome.clone() })));
        }
    }

    /// The operation ends: `outcome` goes to the waiters and, for a transfer, to the Transfer owner. The next operation of the row goes out when `next_goes`.
    pub(super) fn finish(&mut self, op_id: OpId, outcome: OutboxOutcome, next_goes: bool, fx: &mut Effects<Outbox>) {
        let Some(operation) = self.ops.get_mut(&op_id) else { return };
        operation.stage = Stage::Done;
        operation.outcome = Some(outcome.clone());
        let (model, row) = (operation.op.model.clone(), operation.op.row_id.clone());
        self.tell_transfer(op_id, &outcome, fx);
        self.give_outcome(op_id, &outcome, fx);
        if next_goes {
            self.kick_row(&model, &row, fx);
        }
    }

    /// The first operation of the row that is not Done goes out when it is Queued and the row is free.
    pub(super) fn kick_row(&mut self, model: &str, row: &str, fx: &mut Effects<Outbox>) {
        let next = self.ops.values().find(|operation| operation.stage != Stage::Done && operation.op.model == model && operation.op.row_id == row).map(|operation| operation.op_id);
        if let Some(op_id) = next {
            self.try_start(op_id, fx);
        }
    }

    /// A Queued operation goes to the network: the first `Mutate` of the cycle at once, a repeat after its attempt is written, the 41st attempt never.
    pub(super) fn try_start(&mut self, op_id: OpId, fx: &mut Effects<Outbox>) {
        let Some(operation) = self.ops.get(&op_id) else { return };
        if operation.stage != Stage::Queued || !self.goes_out(op_id) {
            return;
        }
        if !operation.sent && operation.attempts <= 1 {
            self.start_sending(op_id, fx);
        } else if operation.attempts < ATTEMPT_LIMIT {
            self.begin_attempt(op_id, fx);
        } else {
            self.fail_with(op_id, "Exhausted".into(), fx);
        }
    }

    pub(super) fn start_sending(&mut self, op_id: OpId, fx: &mut Effects<Outbox>) {
        if let Some(operation) = self.ops.get_mut(&op_id) {
            operation.stage = Stage::Sending;
        }
        self.emit_send(op_id, fx);
    }

    /// The batch of the next attempt: after the restore it waits for `RestoreReady`.
    pub(super) fn begin_attempt(&mut self, op_id: OpId, fx: &mut Effects<Outbox>) {
        if let Some(operation) = self.ops.get_mut(&op_id) {
            operation.stage = Stage::PersistingAttempt;
        }
        if self.phase == Phase::Ready {
            self.store(op_id, CommitKind::Attempt, fx);
        } else {
            self.deferred.push(op_id);
        }
    }

    /// The operation stays in the base with the mark on its row until the mark is written.
    pub(super) fn fail_with(&mut self, op_id: OpId, error: String, fx: &mut Effects<Outbox>) {
        if let Some(operation) = self.ops.get_mut(&op_id) {
            operation.stage = Stage::MarkingFailed;
            operation.error = Some(error);
        }
        self.store(op_id, CommitKind::ErrorMark, fx);
    }

    /// The operation is `Failed` with `error` on its row: `outcome` waiters get `Failed`.
    pub(super) fn failed(&mut self, op_id: OpId, error: String, fx: &mut Effects<Outbox>) {
        let Some(operation) = self.ops.get_mut(&op_id) else { return };
        operation.stage = Stage::Failed;
        operation.error = Some(error.clone());
        let cycle = operation.cycle_id;
        operation.failed_errors.insert(cycle, error.clone());
        let failed = OutboxOutcome::Failed { error };
        self.tell_transfer(op_id, &failed, fx);
        self.give_outcome(op_id, &failed, fx);
    }

    /// A permanent refusal of the server: `on_failure` of the action decides.
    pub(super) fn reject_or_keep(&mut self, op_id: OpId, fx: &mut Effects<Outbox>) {
        let keep = self.ops.get(&op_id).is_some_and(|operation| operation.op.on_failure == OnFailure::Keep);
        if keep {
            let detail = self.ops.get(&op_id).and_then(|operation| operation.reject_error.clone()).unwrap_or_else(|| "refused".into());
            self.fail_with(op_id, detail, fx);
        } else {
            if let Some(operation) = self.ops.get_mut(&op_id) {
                operation.stage = Stage::RollingBack(Why::Rejected);
            }
            self.store(op_id, CommitKind::Rollback, fx);
        }
    }

    /// The first operation of the row that is not `Done`: the one `retry` and `discard` address.
    pub(super) fn open_on_row(&self, model: &str, row: &str) -> Option<OpId> {
        let row = self.aliases.get(&(model.to_string(), row.to_string())).map_or(row, String::as_str);
        self.ops.values().find(|operation| operation.op.model == model && operation.op.row_id == row && operation.stage != Stage::Done).map(|operation| operation.op_id)
    }

    pub(super) fn answer_waiters(&mut self, op_id: OpId, answer: &OutboxAnswer, fx: &mut Effects<Outbox>) {
        if let Some(reply) = self.submit_replies.remove(&op_id) {
            Self::reply(fx, reply, answer.clone());
        }
        for reply in Self::take_waiters(&mut self.retry_waiters, op_id).into_iter().chain(Self::take_waiters(&mut self.discard_waiters, op_id)) {
            Self::reply(fx, reply, answer.clone());
        }
    }

    pub(super) fn interrupt_all(&mut self, closing: bool, fx: &mut Effects<Outbox>) {
        let open: Vec<OpId> = self.ops.values().filter(|operation| operation.stage != Stage::Done).map(|operation| operation.op_id).collect();
        for op_id in open {
            let Some(stage) = self.stage_of(op_id) else { continue };
            if stage == Stage::Sending {
                let req_id = self.ops[&op_id].request_id;
                if let Some(session) = self.peers.session { fx.push(Effect::Send(Outbound::new(session, SessionInput::Cancel { req_id }))); }
            }
            self.answer_waiters(op_id, &OutboxAnswer::Err(Fault::Interrupted), fx);
            if let Some(operation) = self.ops.get_mut(&op_id) {
                operation.stage = Stage::Done;
                operation.outcome = Some(OutboxOutcome::Interrupted);
            }
            if !closing {
                self.tell_transfer(op_id, &OutboxOutcome::Interrupted, fx);
            }
            self.give_outcome(op_id, &OutboxOutcome::Interrupted, fx);
        }
        self.waiting.clear();
    }

    pub(super) fn handle(&mut self, input: OutboxInput, fx: &mut Effects<Outbox>) {
        if let (Some(error), OutboxInput::ReserveOp { transfer_id, .. }) = (&self.queue_error, &input) {
            return self.opened(fx, *transfer_id, Err(Fault::Rejected(RejectReason::Storage(error.clone()))));
        }
        if let (Some(error), OutboxInput::Submit { reply, .. } | OutboxInput::Retry { reply, .. } | OutboxInput::Discard { reply, .. } | OutboxInput::DiscardRestoreFailed { reply, .. } | OutboxInput::Outcome { reply, .. }) = (&self.queue_error, &input) {
            return Self::reply(fx, *reply, OutboxAnswer::Err(Fault::Rejected(RejectReason::Storage(error.clone()))));
        }
        match input {
            OutboxInput::Submit { epoch, op, reply } => self.submit(epoch, op, reply, fx),
            OutboxInput::ReserveOp { epoch, transfer_id } => self.reserve_op(epoch, transfer_id, fx),
            OutboxInput::SubmitReserved { epoch, op_id, op } => self.submit_reserved(epoch, op_id, op, fx),
            OutboxInput::Restored { .. } | OutboxInput::RestoreFailed { .. } | OutboxInput::ReservedFloor { .. } => {}
            OutboxInput::OrphanAnswers { request_ids } => self.remove_orphan_answers(request_ids, fx),
            OutboxInput::RestoreReady { epoch } if epoch == self.epoch => self.restore_ready(fx),
            OutboxInput::RestoreReady { .. } => {}
            OutboxInput::RegisterOperations { declarations } => self.register(declarations, fx),
            OutboxInput::Committed(committed) => self.committed(committed, fx),
            OutboxInput::Rejected(rejected) => self.rejected(rejected, fx),
            OutboxInput::Response { op_id, rows, swaps, stale, result, errors } => self.on_response(op_id, rows, swaps, stale, result, errors, fx),
            OutboxInput::TransportError { op_id, temporary } => self.transport_failed(op_id, temporary, fx),
            OutboxInput::Dropped { op_id } => self.transport_failed(op_id, true, fx),
            OutboxInput::Answered { req_id, outcome } => self.answered(req_id, outcome, fx),
            OutboxInput::Refused { op_id, temporary, detail } => {
                self.rows_arrived(op_id);
                if self.stage_of(op_id) == Some(Stage::Sending) {
                    if let Some(operation) = self.ops.get_mut(&op_id) { operation.reject_error = Some(detail); }
                    self.transport_failed(op_id, temporary, fx);
                }
            }
            OutboxInput::Retry { model, row_id, reply } => self.retry(&model, &row_id, reply, fx),
            OutboxInput::Discard { model, row_id, reply } => self.discard(&model, &row_id, reply, fx),
            OutboxInput::DiscardRestoreFailed { table, key, reply } => self.discard_restore_failed(&table, &key, reply, fx),
            OutboxInput::Outcome { receipt, reply } => self.outcome(receipt, reply, fx),
            OutboxInput::Resolve { .. } | OutboxInput::Landed { .. } | OutboxInput::WhenQuiet { .. } => {}
            OutboxInput::Online(Online(on)) => self.online_changed(on, fx),
            OutboxInput::EpochStarted(started) => self.base_generation = started.generation,
            OutboxInput::ModelsReset(reset) => {
                for model in reset.models {
                    self.generation.insert(model, reset.generation);
                }
            }
            OutboxInput::EpochEnded(_) => self.interrupt_all(false, fx),
            OutboxInput::Closing => {
                self.interrupt_all(true, fx);
                self.closing = true;
                self.maybe_drained(fx);
            }
            OutboxInput::RetryDue { op_id, cycle_id } => self.retry_due(op_id, cycle_id, fx),
            OutboxInput::LocalWrite { model, epoch, write, row, values, reply, now } => self.local_write(model, epoch, write, row, values, reply, now, fx),
            OutboxInput::CopyDone { id } => self.copy_done(id, fx),
            OutboxInput::CopyFailed { id } => self.copy_failed(id, fx),
            OutboxInput::Expire { model, row, due } => self.expire(model, row, due, fx),
            OutboxInput::ArmExpiry { model, row, revision, after } => self.arm_expiry(model, row, revision, after, fx),
            OutboxInput::ArmReset { reset } => self.arm_reset(reset, fx),
            OutboxInput::ResetField { reset } => self.reset_field(reset, fx),
            OutboxInput::RowsGone { ops } => self.rows_gone(ops, fx),
            OutboxInput::DisarmExpiry { model, row } => self.disarm_expiry(&model, &row, fx),
            OutboxInput::ExpireRow { model, row, revision } => self.expire_row(model, row, revision, fx),
            OutboxInput::Tick(_) => {}
        }
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
        if let OutboxInput::Landed { op_id, reply } = input {
            let answer = state.ops.get(&op_id).and_then(|operation| operation.landed.clone()).map_or(OutboxAnswer::Err(Fault::NotFound), OutboxAnswer::Resolved);
            OutboxState::reply(&mut fx, reply, answer);
            return fx;
        }
        if let OutboxInput::Resolve { model, row, reply } = input {
            let resolved = state.aliases.get(&(model, row.clone())).cloned().unwrap_or(row);
            OutboxState::reply(&mut fx, reply, OutboxAnswer::Resolved(resolved));
            return fx;
        }
        if let OutboxInput::WhenQuiet { reply } = input {
            state.quiet_waiters.push(reply);
        } else if state.phase == Phase::Restoring {
            state.restoring(input, &mut fx);
        } else {
            state.handle(input, &mut fx);
        }
        state.release_quiet(&mut fx);
        fx
    }

    fn epoch_started(state: &mut OutboxState, epoch: Epoch) { state.begin_epoch(epoch); }

    fn tick(now: Time) -> OutboxInput { OutboxInput::Tick(now) }

    fn epoch_ended(epoch: Epoch, cause: Cause) -> OutboxInput { OutboxInput::EpochEnded(EpochEnded { epoch, cause }) }

    fn closing() -> OutboxInput { OutboxInput::Closing }
}
