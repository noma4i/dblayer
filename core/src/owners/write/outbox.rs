//! Owner `Outbox`: operations of the queue, optimism, receipts and local writes.
//! One instance per epoch with an identity. The types are in `outbox_types`, the rows of the table in `outbox_{commit,flow,restore,local}`.

use std::collections::{BTreeMap, BTreeSet};

pub use super::outbox_types::*;
use super::transfer::TransferInput;
use crate::owners::contract::{BatchAuthor, BatchId, EpochEnded, Online, RejectReason, ReplyId};
use crate::owners::session::{SessionInput, OUTBOX_AUTHOR};
use crate::owners::store::{StoreBatch, StoreInput, StoreOp};
use crate::runtime::RetryPolicy;
use crate::schema::OnFailure;
use crate::owners::store::StoreWriter;
use crate::runtime::{Address, Cause, Effect, Effects, Epoch, Outbound, ScheduleId, StateOwner, Time};

/// What a batch sent to the Store is for.
/// A write of the device that needs no network: the model, what it writes, the row and its values (or the field for a file).
#[derive(Clone, Debug)]
pub(super) struct LocalCall {
    pub(super) model: String,
    pub(super) kind: LocalWriteKind,
    pub(super) row: String,
    pub(super) values: String,
}

#[derive(Clone, Debug)]
pub(super) enum Batch {
    Operation { op_id: OpId, kind: CommitKind },
    Write { reply: ReplyId, call: Box<LocalCall>, expire: Option<Time> },
    Member { op_id: OpId },
}

#[derive(Debug)]
pub(super) struct MemberRemoval {
    pub(super) member: TransferMember,
    pub(super) in_flight: bool,
    pub(super) storage_failures: u8,
    pub(super) land_after_storage: bool,
    pub(super) deferred: Vec<OutboxInput>,
}

#[derive(Debug)]
pub struct OutboxState {
    pub(super) peers: OutboxPeers,
    pub epoch: Epoch,
    pub online: bool,
    /// The clock in milliseconds as the last `Tick` or `Submit` told it.
    pub(super) now: u64,
    pub generation: BTreeMap<String, u64>,
    /// The generation of the models without an entry in `generation`.
    pub(super) base_generation: u64,
    pub ops: BTreeMap<OpId, QueuedOperation>,
    pub next_op: OpId,
    pub next_batch: BatchId,
    pub(super) next_sequence: u64,
    pub(super) next_schedule: u64,
    pub(super) batches: BTreeMap<BatchId, Batch>,
    pub(super) selection_versions: BTreeMap<(String, String), u64>,
    pub(super) selection_clock: u64,
    pub(super) selections: BTreeMap<ReplyId, (String, String, SelectKind)>,
    pub(super) member_removals: BTreeMap<OpId, MemberRemoval>,
    pub(super) cancelled_commits: BTreeMap<OpId, i64>,
    pub(super) submit_replies: BTreeMap<OpId, ReplyId>,
    pub(super) retry_waiters: Vec<(OpId, ReplyId)>,
    pub(super) discard_waiters: Vec<(OpId, ReplyId)>,
    pub(super) outcome_waiters: Vec<(OpId, u64, ReplyId, bool)>,
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
    pub(super) removed: BTreeSet<OpId>,
    pub(super) removing: BTreeSet<OpId>,
    pub(super) aliases: BTreeMap<(String, String), String>,
    pub(super) copying: BTreeMap<u64, (ReplyId, LocalCall, Time)>,
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
            now: 0,
            generation: BTreeMap::new(),
            base_generation: 0,
            ops: BTreeMap::new(),
            next_op: 1,
            next_batch: 1,
            next_sequence: 1,
            next_schedule: 1,
            batches: BTreeMap::new(),
            selection_versions: BTreeMap::new(),
            selection_clock: 0,
            selections: BTreeMap::new(),
            member_removals: BTreeMap::new(),
            cancelled_commits: BTreeMap::new(),
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
            removed: BTreeSet::new(),
            removing: BTreeSet::new(),
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

    pub(super) fn addresses_row(operation: &QueuedOperation, model: &str, row: &str) -> bool {
        operation.op.model == model && match &operation.op.origin {
            OpSource::TransferGroup { members, .. } => members.iter().any(|member| member.row_id == row),
            _ => operation.op.row_id == row,
        }
    }

    fn shares_row(first: &QueuedOperation, second: &QueuedOperation) -> bool {
        match &second.op.origin {
            OpSource::TransferGroup { members, .. } => members.iter().any(|member| Self::addresses_row(first, &second.op.model, &member.row_id)),
            _ => Self::addresses_row(first, &second.op.model, &second.op.row_id),
        }
    }

    /// An earlier operation of the same row is not Done.
    pub(super) fn row_busy(&self, op_id: OpId) -> bool {
        let Some(operation) = self.ops.get(&op_id) else { return true };
        self.ops.values().any(|other| other.sequence < operation.sequence && other.stage != Stage::Done && Self::shares_row(other, operation))
    }

    /// Operations with a `Mutate` in flight (or about to be): `Sending` outside the retry wait, and the write of the next attempt.
    pub fn slots_taken(&self) -> usize {
        self.ops.values().filter(|operation| operation.stage == Stage::PersistingAttempt || (operation.stage == Stage::Sending && !operation.waiting_retry)).count()
    }

    pub(super) fn goes_out(&self, op_id: OpId) -> bool { self.online && !self.row_busy(op_id) && self.slots_taken() < DRAIN_LIMIT }

    fn expired(&self, op_id: OpId) -> bool {
        let Some(operation) = self.ops.get(&op_id) else { return false };
        operation.attempts <= 1 && operation.sends == 0 && !operation.pending_owned && operation.op.expires_after.is_some_and(|after| u128::from(self.now.saturating_sub(operation.op.created_at)) > after.as_millis())
    }

    /// The Queued operations in `seq` order: each one goes out while a slot is free or ends `Rejected{Expired}`.
    pub(super) fn drain(&mut self, fx: &mut Effects<Outbox>) {
        if !self.online {
            return;
        }
        let queued: Vec<OpId> = self.ops.values().filter(|operation| operation.stage == Stage::Queued && !self.removed.contains(&operation.op_id)).map(|operation| operation.op_id).collect();
        for op_id in queued {
            self.try_start(op_id, fx);
        }
    }

    pub(super) fn add_operation(&mut self, op_id: OpId, op: NewOp, stage: Stage) {
        self.bump_selection(&op);
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
                pending_owned: false,
                pair_in_flight: false,
                waiting_retry: false,
                landing: Vec::new(),
                swaps: Vec::new(),
                missing: Vec::new(),
                result: None,
                stale: Vec::new(),
                landed: None,
                shown: None,
                outcome: None,
                error: None,
                failed_errors: BTreeMap::new(),
                retry_uploads: Vec::new(),
                reject_error: None,
                cause: None,
                cause_stored: None,
                cancels: None,
                before_cancel: None,
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
        operation.pair_in_flight = true;
        operation.waiting_retry = false;
        let purpose = if matches!(operation.op.origin, OpSource::Transfer { .. } | OpSource::TransferGroup { .. }) { PURPOSE_TRANSFER } else { PURPOSE_STANDARD };
        let owner = Some(crate::transport::RequestOwner::Operation { seq: i64::try_from(op_id).unwrap_or(i64::MAX) });
        let mutate = SessionInput::Mutate { author: OUTBOX_AUTHOR, owner, req_id: operation.request_id, epoch: self.epoch, path: operation.op.path.clone(), body: operation.op.body.clone(), purpose };
        if let Some(session) = self.peers.session { fx.push(Effect::Send(Outbound::new(session, mutate))); }
    }

    pub(super) fn store(&mut self, op_id: OpId, kind: CommitKind, fx: &mut Effects<Outbox>) {
        let Some(operation) = self.ops.get(&op_id) else { return };
        let generation = self.generation_of(&operation.op.model);
        let attempt = StoreOp::Attempt { op_id, attempts: if kind == CommitKind::ClearMark { 1 } else { operation.attempts + 1 }, cycle_id: operation.cycle_id, cause: if kind == CommitKind::ClearMark { None } else { operation.cause } };
        let mut ops: Vec<StoreOp> = match kind {
            CommitKind::Intent => vec![StoreOp::Intent {
                op_id,
                attempts: operation.attempts,
                cycle_id: operation.cycle_id,
                model: operation.op.model.clone(),
                row_id: operation.op.row_id.clone(),
                action: operation.op.action.clone(),
                payload: operation.op.body.clone(),
                effects: operation.op.effects.clone(),
                origin: match &operation.op.origin {
                    OpSource::Api => crate::owners::store::IntentOrigin::Api,
                    OpSource::Transfer { transfer_id, .. } => crate::owners::store::IntentOrigin::Transfer { transfer_id: *transfer_id },
                    OpSource::TransferGroup { batch_id, members } => crate::owners::store::IntentOrigin::TransferGroup { batch_id: *batch_id, transfer_ids: members.iter().map(|member| member.transfer_id).collect() },
                },
            }],
            CommitKind::Attempt => vec![attempt],
            CommitKind::Land => operation
                .swaps
                .iter()
                .map(|(temp, real)| StoreOp::Swap { model: operation.op.model.clone(), temp: temp.clone(), real: real.clone() })
                .chain(operation.landing.iter().map(|(model, rows)| StoreOp::Rows { model: model.clone(), rows: rows.clone() }))
                .chain((!operation.stale.is_empty()).then(|| StoreOp::StaleFamilies { families: operation.stale.clone() }))
                .chain([StoreOp::RemoveIntent { op_id }, StoreOp::RemoveAnswer { request_id: operation.request_id }])
                .chain(Self::landing_shows_of(operation))
                .collect(),
            CommitKind::Rollback if operation.restore_element.is_some() => operation.restore_element.iter().map(|(table, key)| StoreOp::DiscardRestoreFailed { table: table.clone(), key: key.clone() }).chain([StoreOp::RemoveIntent { op_id }]).collect(),
            CommitKind::Rollback => [StoreOp::RollBack { op_id }, StoreOp::RemoveIntent { op_id }, StoreOp::RemoveAnswer { request_id: operation.request_id }]
                .into_iter()
                .chain(((operation.pending_owned || operation.pair_in_flight) && operation.stage == Stage::RollingBack(Why::Discarded)).then_some(StoreOp::ReleasePendingOwner { request_id: operation.request_id }))
                .collect(),
            CommitKind::ErrorMark => vec![StoreOp::MarkError { op_id, error: Some(operation.error.clone().unwrap_or_else(|| "refused".into())) }, StoreOp::RemoveAnswer { request_id: operation.request_id }],
            CommitKind::ClearMark => std::iter::once(StoreOp::MarkError { op_id, error: None }).chain(std::iter::once(attempt)).chain(operation.retry_uploads.iter().cloned().map(|row| StoreOp::Upload { row, kind: crate::owners::store::UploadKind::Retrying })).collect(),
            CommitKind::Requeue => vec![StoreOp::Requeue { op_id }],
            CommitKind::Cause => vec![StoreOp::Cause { op_id, cause: operation.cause }],
            CommitKind::RemoveRow => vec![StoreOp::RemoveIntent { op_id }, StoreOp::ReleasePendingOwner { request_id: operation.request_id }, StoreOp::RemoveAnswer { request_id: operation.request_id }],
            CommitKind::LocalWrite => Vec::new(),
        };
        let terminal = match kind {
            CommitKind::Land => Some(OutboxOutcome::Confirmed { revision: 0, result: operation.result.clone(), row_id: None }),
            CommitKind::Rollback => Some(if operation.stage == Stage::RollingBack(Why::Discarded) { OutboxOutcome::Discarded } else { OutboxOutcome::Rejected { error: operation.reject_error.clone().unwrap_or_else(|| "refused".into()) } }),
            CommitKind::ErrorMark => Some(OutboxOutcome::Failed { error: operation.error.clone().unwrap_or_else(|| "refused".into()) }),
            _ => None,
        };
        if kind == CommitKind::Rollback && operation.stage == Stage::RollingBack(Why::Discarded) {
            if let OpSource::TransferGroup { members, .. } = &operation.op.origin { if let [member] = members.as_slice() { ops.push(StoreOp::RemoveUploadByTransferId { transfer_id: member.transfer_id }); } }
        }
        if kind == CommitKind::Land {
            if let Some(pending) = self.member_removals.get(&op_id).filter(|pending| pending.land_after_storage) {
                ops.push(StoreOp::RemoveUploadByTransferId { transfer_id: pending.member.transfer_id });
                ops.push(StoreOp::TransferOutcome { transfer_id: pending.member.transfer_id, cycle_id: pending.member.cycle_id, outcome: OutboxOutcome::Discarded });
            }
        }
        if let Some(outcome) = terminal.filter(|_| kind != CommitKind::Rollback || !operation.failed_errors.contains_key(&operation.cycle_id)) {
            ops.extend(Self::transfer_outcomes(operation, &outcome).into_iter().map(|(transfer_id, cycle_id, outcome)| StoreOp::TransferOutcome { transfer_id, cycle_id, outcome }));
        }
        if kind == CommitKind::Rollback && operation.cancels == Some(op_id) {
            let mut earlier = Vec::new();
            let mut following: Vec<_> = self.ops.values().filter(|other| other.op_id != op_id && other.cancels == Some(op_id) && other.stage == Stage::RollingBack(Why::Discarded)).collect();
            following.sort_by_key(|other| std::cmp::Reverse(other.sequence));
            for other in following {
                if let Some((table, key)) = &other.restore_element {
                    earlier.push(StoreOp::DiscardRestoreFailed { table: table.clone(), key: key.clone() });
                } else {
                    earlier.extend([StoreOp::RollBack { op_id: other.op_id }, StoreOp::RemoveAnswer { request_id: other.request_id }]);
                }
                earlier.push(StoreOp::RemoveIntent { op_id: other.op_id });
                if other.pending_owned || other.pair_in_flight { earlier.push(StoreOp::ReleasePendingOwner { request_id: other.request_id }); }
                earlier.extend(Self::transfer_outcomes(other, &OutboxOutcome::Discarded).into_iter().map(|(transfer_id, cycle_id, outcome)| StoreOp::TransferOutcome { transfer_id, cycle_id, outcome }));
            }
            earlier.append(&mut ops);
            ops = earlier;
        }
        let intents = u64::from(kind == CommitKind::Intent);
        let basis = BTreeMap::from([(operation.op.model.clone(), generation)]);
        let batch = self.next_batch;
        self.next_batch += 1;
        self.batches.insert(batch, Batch::Operation { op_id, kind });
        self.commit(batch, basis, ops, intents, fx);
    }

    pub(super) fn commit(&self, id: BatchId, basis: BTreeMap<String, u64>, ops: Vec<StoreOp>, intents: u64, fx: &mut Effects<Outbox>) {
        let mut batch = StoreBatch { author: self.author, id, stamp: crate::runtime::EpochStamp::Epoch(self.epoch), basis, rows: Vec::new(), intents, credential: None, ops };
        let entries = batch.models().into_iter().map(|model| {
            let generation = batch.basis.get(&model).copied().unwrap_or_else(|| self.generation_of(&model));
            (model, generation)
        }).chain(batch.basis.clone());
        batch.basis = crate::owners::store::basis_for(entries);
        fx.push(Effect::Send(Outbound::new(self.peers.store, StoreInput::Commit(batch))));
    }

    pub(super) fn retry_timer(&self, op_id: OpId, fx: &mut Effects<Outbox>) {
        let Some(operation) = self.ops.get(&op_id) else { return };
        fx.push(Effect::Backoff { id: ScheduleId(op_id), policy: RetryPolicy::Outbox, attempt: operation.attempts.max(1), input: OutboxInput::RetryDue { op_id, cycle_id: operation.cycle_id } });
    }

    pub(super) fn reply(fx: &mut Effects<Outbox>, reply: ReplyId, answer: OutboxAnswer) { fx.push(Effect::Run(OutboxEffect::Reply { reply, answer })); }

    pub(super) fn take_waiters(list: &mut Vec<(OpId, ReplyId)>, op_id: OpId) -> Vec<ReplyId> {
        let (mine, rest): (Vec<_>, Vec<_>) = std::mem::take(list).into_iter().partition(|(id, _)| *id == op_id);
        *list = rest;
        mine.into_iter().map(|(_, reply)| reply).collect()
    }

    pub(super) fn give_outcome(&mut self, op_id: OpId, outcome: &OutboxOutcome, fx: &mut Effects<Outbox>) {
        let (mine, rest): (Vec<_>, Vec<_>) = std::mem::take(&mut self.outcome_waiters).into_iter().partition(|(id, _, _, _)| *id == op_id);
        self.outcome_waiters = rest;
        for (_, _, reply, _) in mine {
            Self::reply(fx, reply, OutboxAnswer::Outcome(outcome.clone()));
        }
    }

    /// The rows the landing batch shows: one per member of a group under its real id, otherwise the row that landed.
    pub(super) fn landing_shows_of(operation: &QueuedOperation) -> Vec<StoreOp> {
        let model = operation.op.model.clone();
        match &operation.op.origin {
            OpSource::TransferGroup { members, .. } => members.iter().map(|member| StoreOp::Show { model: model.clone(), row_id: operation.swaps.iter().find(|(temp, _)| *temp == member.row_id).map_or_else(|| member.row_id.clone(), |(_, real)| real.clone()) }).collect(),
            _ => vec![StoreOp::Show { model, row_id: operation.landed.clone().unwrap_or_else(|| operation.op.row_id.clone()) }],
        }
    }

    pub(super) fn receipt(&self, op_id: OpId, revision: i64) -> Receipt { Receipt { epoch: self.epoch, op_id, cycle_id: self.ops.get(&op_id).map_or(0, |operation| operation.cycle_id), revision, optimistic: self.ops.get(&op_id).and_then(|operation| operation.shown.clone()) } }

    pub(super) fn retry_transfer_answer(&self, cycles: Vec<(u64, u64)>, result: Result<i64, Fault>, fx: &mut Effects<Outbox>) {
        if let Some(transfer) = self.peers.transfer {
            fx.push(Effect::Send(Outbound::new(transfer, TransferInput::RetryOutcome { cycles, result })));
        }
    }

    pub(super) fn tell_transfer(&self, op_id: OpId, outcome: &OutboxOutcome, intent_removed: bool, fx: &mut Effects<Outbox>) {
        let Some(operation) = self.ops.get(&op_id) else { return };
        let Some(transfer) = self.peers.transfer else { return };
        let outcomes = Self::transfer_outcomes(operation, outcome);
        match &operation.op.origin {
            OpSource::Api => {}
            OpSource::Transfer { .. } => if let Some((transfer_id, cycle_id, outcome)) = outcomes.into_iter().next() {
                fx.push(Effect::Send(Outbound::new(transfer, TransferInput::MutationOutcome { transfer_id, cycle_id, outcome, intent_removed })));
            },
            OpSource::TransferGroup { batch_id, .. } => {
                fx.push(Effect::Send(Outbound::new(transfer, TransferInput::MutationOutcomes { batch_id: *batch_id, members: outcomes, intent_removed })));
            }
        }
    }

    fn transfer_outcomes(operation: &QueuedOperation, outcome: &OutboxOutcome) -> Vec<(u64, u64, OutboxOutcome)> {
        match &operation.op.origin {
            OpSource::Api => Vec::new(),
            OpSource::Transfer { transfer_id, cycle_id } => vec![(*transfer_id, *cycle_id, outcome.clone())],
            OpSource::TransferGroup { members, .. } => members.iter().map(|member| {
                let outcome = if matches!(outcome, OutboxOutcome::Confirmed { .. }) && operation.missing.contains(&member.row_id) {
                    OutboxOutcome::Failed { error: super::failure::protocol("the answer has no row for the element") }
                } else if let OutboxOutcome::Confirmed { revision, result, .. } = outcome {
                    let row_id = operation.swaps.iter().find(|(temp, _)| *temp == member.row_id).map_or_else(|| member.row_id.clone(), |(_, real)| real.clone());
                    OutboxOutcome::Confirmed { revision: *revision, result: result.clone(), row_id: Some(row_id) }
                } else { outcome.clone() };
                (member.transfer_id, member.cycle_id, outcome)
            }).collect(),
        }
    }

    /// The operation ends: `outcome` goes to the waiters and, for a transfer, to the Transfer owner. The next operation of the row goes out when `next_goes`.
    pub(super) fn finish(&mut self, op_id: OpId, outcome: OutboxOutcome, next_goes: bool, fx: &mut Effects<Outbox>) {
        let Some(operation) = self.ops.get_mut(&op_id) else { return };
        operation.stage = Stage::Done;
        operation.outcome = Some(outcome.clone());
        let op = operation.op.clone();
        self.bump_selection(&op);
        self.tell_transfer(op_id, &outcome, next_goes, fx);
        self.give_outcome(op_id, &outcome, fx);
        if !matches!(outcome, OutboxOutcome::Ended(_)) {
            self.drain(fx);
        }
    }

    /// A Queued operation on a free row ends `Rejected{Expired}` when its time is over, else goes to the network while a slot is free: the first `Mutate` of the cycle at once, a repeat after its attempt is written, the 41st attempt never.
    pub(super) fn try_start(&mut self, op_id: OpId, fx: &mut Effects<Outbox>) {
        let Some(operation) = self.ops.get(&op_id) else { return };
        if operation.stage != Stage::Queued || !self.online || self.row_busy(op_id) {
            return;
        }
        if self.expired(op_id) {
            if let Some(operation) = self.ops.get_mut(&op_id) {
                operation.stage = Stage::RollingBack(Why::Rejected);
                operation.reject_error = Some(super::failure::expired());
            }
            return self.store(op_id, CommitKind::Rollback, fx);
        }
        if self.slots_taken() >= DRAIN_LIMIT {
            return;
        }
        if !operation.sent && operation.attempts <= 1 {
            self.start_sending(op_id, fx);
        } else if operation.attempts < ATTEMPT_LIMIT {
            self.begin_attempt(op_id, fx);
        } else {
            self.fail_exhausted(op_id, fx);
        }
    }

    /// A `Sending` operation waits in the queue again: its column says `pending` once the batch commits.
    pub(super) fn requeue(&mut self, op_id: OpId, fx: &mut Effects<Outbox>) {
        if let Some(operation) = self.ops.get_mut(&op_id) {
            operation.stage = Stage::Queued;
            operation.waiting_retry = false;
        }
        self.store(op_id, CommitKind::Requeue, fx);
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

    /// The Backoff ran out: `Failed` carries the cause of the last attempt when it has a kept one, `Exhausted` otherwise.
    pub(super) fn fail_exhausted(&mut self, op_id: OpId, fx: &mut Effects<Outbox>) {
        let cause = self.ops.get(&op_id).and_then(|operation| operation.cause).map(super::failure::of_cause);
        self.fail_with(op_id, cause.unwrap_or_else(|| "Exhausted".into()), fx);
    }

    /// The operation is `Failed` with `error` on its row: `outcome` waiters get `Failed`.
    pub(super) fn failed(&mut self, op_id: OpId, error: String, fx: &mut Effects<Outbox>) {
        let Some(operation) = self.ops.get_mut(&op_id) else { return };
        operation.stage = Stage::Failed;
        operation.error = Some(error.clone());
        let cycle = operation.cycle_id;
        operation.failed_errors.insert(cycle, error.clone());
        let failed = OutboxOutcome::Failed { error };
        self.tell_transfer(op_id, &failed, false, fx);
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
    /// The open operation of the transfer: by its identity, the cycle of a single transfer and the group of a batch member.
    pub(super) fn open_for_transfer(&self, transfer: u64, cycle: u64) -> Option<OpId> {
        self.ops.values().filter(|operation| operation.stage != Stage::Done && match &operation.op.origin {
            OpSource::Transfer { transfer_id, cycle_id } => *transfer_id == transfer && *cycle_id == cycle,
            OpSource::TransferGroup { members, .. } => members.iter().any(|member| member.transfer_id == transfer),
            _ => false,
        }).map(|operation| operation.op_id).next()
    }

    pub(super) fn open_on_row(&self, model: &str, row: &str, action: Option<&str>) -> Option<OpId> {
        let row = self.aliases.get(&(model.to_string(), row.to_string())).map_or(row, String::as_str);
        self.ops.values().filter(|operation| Self::addresses_row(operation, model, row) && action.is_none_or(|action| operation.op.action == action) && operation.stage != Stage::Done).min_by_key(|operation| operation.sequence).map(|operation| operation.op_id)
    }

    /// A call without an action asks Transfer, which holds the transfers that have no operation yet: the smallest open operation of the row here is the bound.
    fn select_row(&mut self, (model, row_id): (String, String), kind: SelectKind, reply: ReplyId, fx: &mut Effects<Outbox>) {
        let resolved = self.aliases.get(&(model.clone(), row_id.clone())).cloned();
        let outbox_min = self.open_on_row(&model, &row_id, None).map(|op_id| (op_id, self.ops[&op_id].op.action.clone()));
        let Some(transfer) = self.peers.transfer else {
            return match outbox_min {
                Some((_, action)) => self.selected((model, row_id), action, kind, reply, fx),
                None => Self::reply(fx, reply, OutboxAnswer::Err(Fault::NotFound)),
            };
        };
        let version = self.selection_version(&model, &row_id);
        self.selections.insert(reply, (model.clone(), row_id.clone(), kind));
        let rows = std::iter::once(row_id).chain(resolved).collect();
        fx.push(Effect::Send(Outbound::new(transfer, super::transfer::TransferInput::SelectRow { model, rows, outbox_min, kind, reply, version })));
    }

    pub(super) fn bump_selection(&mut self, op: &NewOp) {
        let rows: BTreeSet<String> = match &op.origin { OpSource::TransferGroup { members, .. } => members.iter().map(|member| member.row_id.clone()).collect(), _ => [op.row_id.clone()].into_iter().collect() };
        for row in rows { self.bump_selection_row(&op.model, &row); }
    }

    pub(super) fn bump_selection_row(&mut self, model: &str, row: &str) {
        self.selection_clock += 1;
        self.selection_versions.insert((model.into(), row.into()), self.selection_clock);
    }

    fn selection_version(&self, model: &str, row: &str) -> u64 {
        let row = self.aliases.get(&(model.into(), row.into())).map_or(row, String::as_str);
        self.selection_versions.get(&(model.into(), row.into())).copied().unwrap_or(0)
    }

    fn selection_current(&mut self, reply: ReplyId, version: u64, fx: &mut Effects<Outbox>) -> bool {
        let Some((model, row, kind)) = self.selections.get(&reply).cloned() else { return false };
        if self.selection_version(&model, &row) != version {
            self.select_row((model, row), kind, reply, fx);
            return false;
        }
        self.selections.remove(&reply);
        true
    }

    /// Transfer named the smallest open operation of the row here: an operation of a transfer goes on through its Transfer, any other by its own cell.
    fn selected(&mut self, (model, row_id): (String, String), action: String, kind: SelectKind, reply: ReplyId, fx: &mut Effects<Outbox>) {
        if kind == SelectKind::Retry && self.open_on_row(&model, &row_id, Some(&action)).is_some_and(|op_id| matches!(self.ops[&op_id].op.origin, OpSource::Transfer { .. } | OpSource::TransferGroup { .. })) {
            return Self::reply(fx, reply, OutboxAnswer::Transfer { action });
        }
        match kind {
            SelectKind::Retry => self.retry(&model, &row_id, Some(&action), reply, fx),
            SelectKind::Discard => self.discard(&model, &row_id, Some(&action), reply, fx),
        }
    }

    pub(super) fn answer_waiters(&mut self, op_id: OpId, answer: &OutboxAnswer, fx: &mut Effects<Outbox>) {
        if let Some(reply) = self.submit_replies.remove(&op_id) {
            Self::reply(fx, reply, answer.clone());
        }
        for reply in Self::take_waiters(&mut self.retry_waiters, op_id) {
            Self::reply(fx, reply, answer.clone());
        }
        for reply in Self::take_waiters(&mut self.discard_waiters, op_id) {
            Self::reply(fx, reply, answer.clone());
        }
    }

    pub(super) fn interrupt_all(&mut self, fault: Fault, fx: &mut Effects<Outbox>) {
        let cause = if fault == Fault::Closed { crate::ffi::EndCause::Closed } else { crate::ffi::EndCause::SessionEnded };
        let outcome = OutboxOutcome::Ended(cause);
        let open: Vec<OpId> = self.ops.values().filter(|operation| operation.stage != Stage::Done).map(|operation| operation.op_id).collect();
        for op_id in open {
            let Some(stage) = self.stage_of(op_id) else { continue };
            let transfer_ids: Vec<_> = self.ops.get_mut(&op_id).map(|operation| std::mem::take(&mut operation.retry_uploads).into_iter().map(|row| (row.transfer_id, row.cycle_id)).collect()).unwrap_or_default();
            if !transfer_ids.is_empty() { self.retry_transfer_answer(transfer_ids, Err(fault.clone()), fx); }
            if stage == Stage::Sending {
                let req_id = self.ops[&op_id].request_id;
                if let Some(session) = self.peers.session { fx.push(Effect::Send(Outbound::new(session, SessionInput::Cancel { req_id }))); }
            }
            self.answer_waiters(op_id, &OutboxAnswer::Err(fault.clone()), fx);
            if let Some(operation) = self.ops.get_mut(&op_id) {
                operation.stage = Stage::Done;
                operation.outcome = Some(outcome.clone());
            }
            self.tell_transfer(op_id, &outcome, false, fx);
            self.give_outcome(op_id, &outcome, fx);
        }
        self.waiting.clear();
    }

    pub(super) fn handle(&mut self, input: OutboxInput, fx: &mut Effects<Outbox>) {
        let answered = match &input {
            OutboxInput::Response { op_id, .. } | OutboxInput::TransportError { op_id, .. } | OutboxInput::Dropped { op_id } | OutboxInput::Refused { op_id, .. } => Some(*op_id),
            OutboxInput::Answered { req_id, .. } => self.ops.values().find(|operation| operation.request_id == *req_id).map(|operation| operation.op_id),
            _ => None,
        };
        let targeted = answered.or_else(|| match &input {
            OutboxInput::RetryDue { op_id, .. } => Some(*op_id),
            OutboxInput::Retry { model, row_id, action, .. } | OutboxInput::Discard { model, row_id, action, .. } => self.open_on_row(model, row_id, action.as_deref()),
            OutboxInput::DiscardTransfer { transfer_id, cycle_id, .. } => self.open_for_transfer(*transfer_id, *cycle_id),
            OutboxInput::Outcome { receipt, .. } => Some(receipt.op_id),
            _ => None,
        });
        if let Some(op_id) = targeted { if self.member_removals.get(&op_id).is_some_and(|pending| !pending.in_flight && !pending.land_after_storage) { self.store_member(op_id, fx); } }
        if let Some(op_id) = answered {
            if self.member_removals.get(&op_id).is_some_and(|pending| pending.in_flight) {
                if let Some(pending) = self.member_removals.get_mut(&op_id) { pending.deferred.push(input); }
                return;
            }
        }
        if let (Some(error), OutboxInput::ReserveOp { target, .. }) = (&self.queue_error, &input) {
            return self.opened(fx, *target, Err(Fault::Rejected(RejectReason::Storage(error.clone()))));
        }
        if let (Some(error), OutboxInput::RetryTransfer { uploads, .. }) = (&self.queue_error, &input) {
            self.retry_transfer_answer(uploads.iter().map(|row| (row.transfer_id, row.cycle_id)).collect(), Err(Fault::Rejected(RejectReason::Storage(error.clone()))), fx);
            return;
        }
        if let (Some(error), OutboxInput::Submit { reply, .. } | OutboxInput::Retry { reply, .. } | OutboxInput::Discard { reply, .. } | OutboxInput::Outcome { reply, .. }) = (&self.queue_error, &input) {
            return Self::reply(fx, *reply, OutboxAnswer::Err(Fault::Rejected(RejectReason::Storage(error.clone()))));
        }
        match input {
            OutboxInput::Submit { epoch, op, reply } => self.submit(epoch, op, reply, fx),
            OutboxInput::ReserveOp { epoch, target } => self.reserve_op(epoch, target, fx),
            OutboxInput::SubmitReserved { epoch, op_id, op } => self.submit_reserved(epoch, op_id, op, fx),
            OutboxInput::Restored { .. } | OutboxInput::RestoreFailed { .. } | OutboxInput::ReservedFloor { .. } | OutboxInput::AliasesRestored { .. } => {}
            OutboxInput::OrphanAnswers { request_ids } => self.remove_orphan_answers(request_ids, fx),
            OutboxInput::RestoreReady { epoch } if epoch == self.epoch => self.restore_ready(fx),
            OutboxInput::RestoreReady { .. } => {}
            OutboxInput::RegisterOperations { declarations } => self.register(declarations, fx),
            OutboxInput::Committed(committed) => self.committed(committed, fx),
            OutboxInput::Rejected(rejected) => self.rejected(rejected, fx),
            OutboxInput::Response { op_id, rows, swaps, stale, result, missing, errors } => self.on_response(op_id, super::outbox_flow::Answered { rows, swaps, stale, result, missing }, errors, fx),
            OutboxInput::TransportError { op_id, temporary } => self.transport_failed(op_id, temporary, None, fx),
            OutboxInput::Dropped { op_id } => self.transport_failed(op_id, true, None, fx),
            OutboxInput::Answered { req_id, outcome } => self.answered(req_id, outcome, fx),
            OutboxInput::Refused { op_id, temporary, detail } => {
                self.rows_arrived(op_id);
                if self.stage_of(op_id) == Some(Stage::Sending) {
                    if let Some(operation) = self.ops.get_mut(&op_id) { operation.reject_error = Some(detail); }
                    self.transport_failed(op_id, temporary, None, fx);
                }
            }
            OutboxInput::Retry { model, row_id, action: None, reply } => self.select_row((model, row_id), SelectKind::Retry, reply, fx),
            OutboxInput::Retry { model, row_id, action, reply } => self.retry(&model, &row_id, action.as_deref(), reply, fx),
            OutboxInput::RetryTransfer { model, row_id, reply, uploads } => self.retry_transfer(&model, &row_id, reply, uploads, fx),
            OutboxInput::Discard { model, row_id, action: None, reply } => self.select_row((model, row_id), SelectKind::Discard, reply, fx),
            OutboxInput::Discard { model, row_id, action, reply } => self.discard(&model, &row_id, action.as_deref(), reply, fx),
            OutboxInput::DiscardTransfer { transfer_id, cycle_id, reply } => match self.open_for_transfer(transfer_id, cycle_id) {
                Some(op_id) => self.discard_op(op_id, reply, fx),
                None => Self::reply(fx, reply, OutboxAnswer::Err(Fault::NotFound)),
            },
            OutboxInput::SelectedOutbox { model, row_id, action, kind, reply, version } => { if self.selection_current(reply, version, fx) { self.selected((model, row_id), action, kind, reply, fx); } }
            OutboxInput::SelectedNone { reply, version } => { if self.selection_current(reply, version, fx) { Self::reply(fx, reply, OutboxAnswer::Err(Fault::NotFound)); } }
            OutboxInput::SelectedTransfer { reply, version, .. } => { if self.selection_current(reply, version, fx) { if let Some(transfer) = self.peers.transfer { fx.push(Effect::Send(Outbound::new(transfer, super::transfer::TransferInput::SelectConfirmed { reply }))); } } }
            OutboxInput::Outcome { receipt, reply, discard_on_failure } => self.outcome(receipt, reply, discard_on_failure, fx),
            OutboxInput::Resolve { .. } | OutboxInput::Landed { .. } | OutboxInput::WhenQuiet { .. } => {}
            OutboxInput::Online(Online(on)) => self.online_changed(on, fx),
            OutboxInput::ActivateEpoch(epoch) => self.begin_epoch(epoch),
            OutboxInput::EpochStarted(started) => self.base_generation = started.generation,
            OutboxInput::ModelsReset(reset) => {
                for model in reset.models {
                    self.generation.insert(model, reset.generation);
                }
            }
            OutboxInput::EpochEnded(ended) => self.interrupt_all(Fault::of_end(ended.cause), fx),
            OutboxInput::Closing => {
                self.interrupt_all(Fault::Closed, fx);
                self.closing = true;
                self.maybe_drained(fx);
            }
            OutboxInput::RetryDue { op_id, cycle_id } => self.retry_due(op_id, cycle_id, fx),
            OutboxInput::LocalWrite { model, epoch, write, row, values, reply, now } => self.local_write(LocalCall { model, kind: write, row, values }, epoch, (reply, now), fx),
            OutboxInput::CopyDone { id } => self.copy_done(id, fx),
            OutboxInput::CopyFailed { id } => self.copy_failed(id, fx),
            OutboxInput::Expire { model, row, due } => self.expire(model, row, due, fx),
            OutboxInput::ArmExpiry { model, row, revision, deadline } => self.arm_expiry(model, row, revision, deadline, fx),
            OutboxInput::ArmReset { reset } => self.arm_reset(reset, fx),
            OutboxInput::ResetField { reset } => self.reset_field(reset, fx),
            OutboxInput::RowsGone { ops } => self.rows_gone(ops, fx),
            OutboxInput::RowRemoved { model, row_id, revision } => self.row_removed(&model, &row_id, revision, fx),
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
        if state.closing {
            match &input {
                OutboxInput::Committed(committed) if matches!(state.batches.get(&committed.batch), Some(Batch::Write { .. })) => { state.handle(input, &mut fx); return fx; }
                OutboxInput::Rejected(rejected) if matches!(state.batches.get(&rejected.batch), Some(Batch::Write { .. })) => { state.handle(input, &mut fx); return fx; }
                OutboxInput::CopyDone { .. } | OutboxInput::CopyFailed { .. } => { state.handle(input, &mut fx); return fx; }
                _ => {}
            }
            match &input {
                OutboxInput::Submit { reply, .. } | OutboxInput::Retry { reply, .. } | OutboxInput::Discard { reply, .. } | OutboxInput::DiscardTransfer { reply, .. } | OutboxInput::Outcome { reply, .. } | OutboxInput::WhenQuiet { reply } | OutboxInput::Landed { reply, .. } | OutboxInput::Resolve { reply, .. } | OutboxInput::LocalWrite { reply, .. } => OutboxState::reply(&mut fx, *reply, OutboxAnswer::Err(Fault::Closed)),
                OutboxInput::ReserveOp { target, .. } => state.opened(&mut fx, *target, Err(Fault::Closed)),
                OutboxInput::RetryTransfer { uploads, .. } => state.retry_transfer_answer(uploads.iter().map(|row| (row.transfer_id, row.cycle_id)).collect(), Err(Fault::Closed), &mut fx),
                _ => {}
            }
            return fx;
        }
        if let OutboxInput::ActivateEpoch(epoch) = &input {
            state.begin_epoch(*epoch);
            return fx;
        }
        if let OutboxInput::Tick(now) = &input {
            state.now = state.now.max(now.0);
        }
        if let OutboxInput::Landed { op_id, reply } = input {
            let answer = state.ops.get(&op_id).map_or(OutboxAnswer::Err(Fault::NotFound), |operation| match &operation.outcome {
                Some(OutboxOutcome::Confirmed { revision, .. }) => OutboxAnswer::Written { revision: *revision, shown: operation.shown.clone(), files_removed: 0 },
                _ => OutboxAnswer::Err(Fault::NotFound),
            });
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

    fn epoch_started(epoch: Epoch) -> Option<OutboxInput> { Some(OutboxInput::ActivateEpoch(epoch)) }

    fn tick(now: Time) -> OutboxInput { OutboxInput::Tick(now) }

    fn epoch_ended(epoch: Epoch, cause: Cause) -> OutboxInput { OutboxInput::EpochEnded(EpochEnded { epoch, cause }) }

    fn closing() -> OutboxInput { OutboxInput::Closing }
}
