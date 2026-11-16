//! Owner `Outbox`: operations of the queue, optimism, receipts and local writes.
//! One instance per epoch with an identity. The types are in `outbox_types`, the rows of the table in `outbox_{commit,flow,restore,local}`.

use std::collections::{BTreeMap, BTreeSet};

pub use super::outbox_types::*;
use super::transfer::TransferInput;
use crate::owners::contract::{BatchAuthor, BatchId, EpochEnded, Online, RejectReason, ReplyId};
use crate::owners::session::{SessionInput, OUTBOX_AUTHOR};
use crate::owners::store::{OutcomeOwner, StoreBatch, StoreInput, StoreOp};
use crate::runtime::RetryPolicy;
use crate::schema::OnFailure;
use crate::owners::store::StoreWriter;
use crate::runtime::{Address, Cause, Effect, Effects, Epoch, Outbound, ScheduleId, StateOwner, Time};

#[derive(Clone, Copy, PartialEq, Eq)]
enum TempDependency {
    Clear,
    Open,
}

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
    pub online: Option<bool>,
    /// The clock in milliseconds as the last `Tick` or `Submit` told it.
    pub ops: BTreeMap<OpId, QueuedOperation>,
    pub next_op: OpId,
    pub next_batch: BatchId,
    pub(super) next_sequence: u64,
    pub(super) next_schedule: u64,
    pub(super) batches: BTreeMap<BatchId, Batch>,
    pub(crate) selection_versions: BTreeMap<(String, String), u64>,
    pub(super) selection_clock: u64,
    pub(super) selections: BTreeMap<ReplyId, (String, String, SelectKind)>,
    pub(super) member_removals: BTreeMap<OpId, MemberRemoval>,
    pub(super) cancelled_commits: BTreeMap<OpId, i64>,
    pub(super) submit_replies: BTreeMap<OpId, ReplyId>,
    pub(super) retry_waiters: Vec<(OpId, ReplyId)>,
    pub(super) discard_waiters: Vec<(OpId, ReplyId)>,
    pub(super) discard_waiting: Vec<(OpId, ReplyId)>,
    pub(super) outcome_waiters: Vec<(OpId, u64, ReplyId, bool)>,
    pub(super) confirmed_discards: BTreeMap<OpId, (u64, String)>,
    pub(super) transfer_discards: BTreeMap<OpId, (u64, u64)>,
    pub(super) replacements: BTreeMap<u64, Option<OpId>>,
    pub(super) ephemeral: BTreeMap<String, Time>,
    pub(super) expiry: BTreeMap<(String, String), (ScheduleId, u64)>,
    pub(super) resets: BTreeMap<(String, String, String), (ScheduleId, u64)>,
    pub(super) drained: bool,
    pub(super) author: BatchAuthor,
    #[cfg(test)]
    pub(super) quiet_waiters: Vec<ReplyId>,
    pub(super) closing: bool,
    pub(super) queue_error: Option<String>,
    pub phase: Phase,
    pub(super) ended: bool,
    pub(super) held: Vec<OutboxInput>,
    pub(super) declarations: BTreeSet<(String, String)>,
    pub(super) complete: bool,
    /// Restored operations that wait for `RestoreReady` to write their next attempt.
    pub(super) deferred: Vec<OpId>,
    pub(super) waiting: BTreeMap<OpId, (StoredOp, bool)>,
    pub(super) removed: BTreeSet<OpId>,
    pub(super) orphan_transfers: BTreeSet<OpId>,
    pub(super) removing: BTreeSet<OpId>,
    pub(super) aliases: AliasMap,
    pub(super) ended_creates: std::collections::BTreeSet<String>,
}

impl OutboxState {
    /// The owner LocalWrites: the same machine in every epoch, it takes only the local writes and talks to Store under its own author.
    pub fn local_writes(epoch: Epoch, store: Address<StoreWriter>) -> Self {
        let mut state = Self::new(epoch, OutboxPeers { store, session: None, transfer: None });
        state.author = BatchAuthor::LocalWrites;
        state.online = Some(true);
        state.phase = Phase::Ready;
        state
    }

    pub(super) fn is_online(&self) -> bool { self.online == Some(true) }

    pub fn new(epoch: Epoch, peers: OutboxPeers) -> Self {
        Self {
            peers,
            epoch,
            online: None,
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
            discard_waiting: Vec::new(),
            outcome_waiters: Vec::new(),
            confirmed_discards: BTreeMap::new(),
            transfer_discards: BTreeMap::new(),
            replacements: BTreeMap::new(),
            ephemeral: BTreeMap::new(),
            expiry: BTreeMap::new(),
            resets: BTreeMap::new(),
            drained: false,
            author: BatchAuthor::Outbox,
            #[cfg(test)]
            quiet_waiters: Vec::new(),
            closing: false,
            queue_error: None,
            phase: Phase::Restoring,
            ended: false,
            held: Vec::new(),
            declarations: BTreeSet::new(),
            complete: false,
            deferred: Vec::new(),
            waiting: BTreeMap::new(),
            removed: BTreeSet::new(),
            orphan_transfers: BTreeSet::new(),
            removing: BTreeSet::new(),
            aliases: BTreeMap::new(),
            ended_creates: std::collections::BTreeSet::new(),
        }
    }

    /// A new epoch: a fresh instance. The platform call gate keeps calls out of an epoch without an identity.
    pub(super) fn begin_epoch(&mut self, epoch: Epoch) {
        let (declarations, complete) = (std::mem::take(&mut self.declarations), self.complete);
        *self = if self.author == BatchAuthor::LocalWrites { Self::local_writes(epoch, self.peers.store) } else { Self::new(epoch, self.peers) };
        self.declarations = declarations;
        self.complete = complete;
    }

    pub fn stage_of(&self, op_id: OpId) -> Option<Stage> { self.ops.get(&op_id).map(|operation| operation.stage) }

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
        self.ops.values().any(|other| other.sequence < operation.sequence && other.stage != Stage::Done && Self::shares_row(other, operation) && !Self::awaits_row_of(other, operation)) || self.temp_dependency(operation) != TempDependency::Clear || self.awaits_ended_create(operation)
    }

    fn creates_own_row(operation: &QueuedOperation) -> bool { operation.op.action == "create" || matches!(operation.op.origin, OpSource::TransferGroup { .. }) }

    /// The temp ids of the body of the operation except the row it creates, or the reason the body is no JSON.
    pub(super) fn body_temps(operation: &QueuedOperation) -> Result<Vec<String>, String> {
        if !operation.op.body.contains(crate::identity::TEMP_ID_PREFIX) { return Ok(Vec::new()); }
        let creates_own = Self::creates_own_row(operation);
        let mut body = serde_json::from_str::<serde_json::Value>(&operation.op.body).map_err(|error| format!("the body of {}.{} is not JSON: {error}", operation.op.model, operation.op.action))?;
        let mut temps = Vec::new();
        crate::identity::walk_temp_ids(&mut body, &mut |temp| if !(creates_own && Self::addresses_row(operation, &operation.op.model, temp)) { temps.push(temp.clone()); });
        Ok(temps)
    }

    /// The temp ids the operation waits for: its own row when it acts on one, every temp id of the body except the row it creates.
    fn awaited_temps(operation: &QueuedOperation) -> Result<Vec<String>, String> {
        let mut temps = Vec::new();
        if !Self::creates_own_row(operation) && operation.cancels.is_none() && crate::identity::is_temp_id(&operation.op.row_id) { temps.push(operation.op.row_id.clone()); }
        temps.extend(operation.body_temps.clone()?);
        Ok(temps)
    }

    /// Open while the fate of a temp id the operation waits for is unknown: the cache has neither a real id nor an end for it.
    fn temp_dependency(&self, operation: &QueuedOperation) -> TempDependency {
        let holds = |temp: &String| matches!(operation.op.origin, OpSource::Transfer { .. }) && &operation.op.row_id == temp;
        let Ok(awaited) = Self::awaited_temps(operation) else { return TempDependency::Open };
        let unknown = awaited.iter().any(|temp| !holds(temp) && !self.ended_creates.contains(temp) && !self.aliases.contains_key(temp));
        if unknown { TempDependency::Open } else { TempDependency::Clear }
    }

    /// `waiter` waits for the fate of the temp row that `holder` lands: it cannot go before the holder, whatever their order.
    fn awaits_row_of(waiter: &QueuedOperation, holder: &QueuedOperation) -> bool {
        let held = match &holder.op.origin {
            OpSource::Transfer { .. } => vec![holder.op.row_id.clone()],
            _ => Self::created_temps(holder),
        };
        Self::awaited_temps(waiter).is_ok_and(|awaited| awaited.iter().any(|temp| held.contains(temp)))
    }

    /// The temp ids of the rows the operation creates.
    pub(super) fn created_temps(operation: &QueuedOperation) -> Vec<String> {
        match &operation.op.origin {
            OpSource::TransferGroup { members, .. } => members.iter().map(|member| member.row_id.clone()).filter(|row| crate::identity::is_temp_id(row)).collect(),
            _ if operation.op.action == "create" && crate::identity::is_temp_id(&operation.op.row_id) => vec![operation.op.row_id.clone()],
            _ => Vec::new(),
        }
    }

    /// The temp ids ended without a real id: the operations that wait for them end when they next start.
    pub(super) fn end_temps(&mut self, temps: impl IntoIterator<Item = String>) {
        self.ended_creates.extend(temps);
    }

    /// The operation waits for a temp id whose `create` ended without a real id.
    fn awaits_ended_create(&self, operation: &QueuedOperation) -> bool {
        Self::awaited_temps(operation).is_ok_and(|awaited| awaited.iter().any(|temp| self.ended_creates.contains(temp)))
    }

    /// The Queued operations that wait for the rows the ended `create` made (and for the rows of those operations in turn), the latest first.
    fn dependents_of(&self, create: OpId) -> Vec<OpId> {
        let Some(first) = self.ops.get(&create) else { return Vec::new() };
        let mut ended = Self::created_temps(first);
        if ended.is_empty() { return Vec::new(); }
        let mut found: Vec<&QueuedOperation> = Vec::new();
        loop {
            let next = self.ops.values().find(|other| other.op_id != create && other.stage == Stage::Queued && other.sequence > first.sequence && !found.iter().any(|seen| seen.op_id == other.op_id) && Self::awaited_temps(other).is_ok_and(|awaited| awaited.iter().any(|temp| ended.contains(temp))));
            let Some(next) = next else { break };
            ended.extend(Self::created_temps(next));
            found.push(next);
        }
        found.sort_by_key(|operation| std::cmp::Reverse(operation.sequence));
        found.into_iter().map(|operation| operation.op_id).collect()
    }

    /// The rollback of `create` did not happen and `create` is back in `Failed`: its dependents wait again and the recorded list is cleared.
    pub(super) fn return_dependents(&mut self, create: OpId) {
        let Some(dependents) = self.ops.get_mut(&create).map(|operation| std::mem::take(&mut operation.ended_dependents)) else { return };
        for id in dependents {
            if let Some(other) = self.ops.get_mut(&id).filter(|other| other.stage == Stage::RollingBack(Why::Rejected)) {
                other.stage = Stage::Queued;
                other.reject_error = None;
                other.row_never_created = false;
            }
        }
    }

    /// The epoch ended the rollback of `create`: its dependents end with it.
    pub(super) fn end_dependents(&mut self, now: u64, create: OpId, reason: &RejectReason, fx: &mut Effects<Outbox>) {
        let dependents = self.ops.get_mut(&create).map(|operation| std::mem::take(&mut operation.ended_dependents)).unwrap_or_default();
        for id in dependents {
            self.answer_waiters(id, &OutboxAnswer::Err(Fault::of_reject(reason.clone())), fx);
            self.finish(now, id, OutboxOutcome::Ended(end_cause(reason)), false, fx);
        }
    }

    /// Operations with a `Mutate` in flight (or about to be): `Sending` outside the retry wait, and the write of the next attempt.
    pub fn slots_taken(&self) -> usize {
        self.ops.values().filter(|operation| operation.stage == Stage::PersistingAttempt || (operation.stage == Stage::Sending && !operation.waiting_retry)).count()
    }

    /// Session accepts only the `Mutate` of the held pair while it is held: every other operation waits for its owner.
    fn pair_held_by_other(&self, op_id: OpId) -> bool { self.ops.values().any(|other| other.op_id != op_id && other.pending_owned && !matches!(other.stage, Stage::Done | Stage::Failed)) || self.waiting.iter().any(|(waiting, (item, _))| *waiting != op_id && item.pending.is_some()) }

    /// An operation with a smaller `seq` still writes its intent: the order of `Submit` is the order out.
    fn earlier_submitting(&self, op_id: OpId) -> bool {
        let Some(sequence) = self.ops.get(&op_id).map(|operation| operation.sequence) else { return false };
        self.ops.values().any(|other| other.stage == Stage::Submitting && other.sequence < sequence)
    }

    /// The row, the held pair or an earlier `Submit` keeps the operation back, whatever the slots say.
    pub(super) fn waits_for_turn(&self, op_id: OpId) -> bool { self.row_busy(op_id) || self.pair_held_by_other(op_id) || self.earlier_submitting(op_id) }

    pub(super) fn goes_out(&self, op_id: OpId) -> bool { self.is_online() && !self.waits_for_turn(op_id) && self.slots_taken() < DRAIN_LIMIT }

    pub(super) fn expired(&self, now: u64, op_id: OpId) -> bool {
        let Some(operation) = self.ops.get(&op_id) else { return false };
        operation.attempts <= 1 && operation.sends == 0 && !operation.pending_owned && operation.op.expires_after.is_some_and(|after| u128::from(now.saturating_sub(operation.created_at)) > after.as_millis())
    }

    /// The Queued operations in `seq` order: each one goes out while a slot is free or ends `Rejected{Expired}`.
    pub(super) fn drain(&mut self, now: u64, fx: &mut Effects<Outbox>) {
        if !self.is_online() {
            return;
        }
        let queued: Vec<OpId> = self.ops.values().filter(|operation| operation.stage == Stage::Queued && !self.removed.contains(&operation.op_id)).map(|operation| operation.op_id).collect();
        for op_id in queued {
            self.try_start(now, op_id, fx);
        }
    }

    pub(super) fn add_operation(&mut self, op_id: OpId, op: NewOp, stage: Stage, created_at: u64) {
        self.bump_selection(&op);
        let sequence = self.ops.get(&op_id).map_or_else(|| { let fresh = self.next_sequence; self.next_sequence += 1; fresh }, |known| known.sequence);
        self.ops.insert(
            op_id,
            QueuedOperation {
                op_id,
                op,
                created_at,
                stage,
                attempts: 1,
                storage_refusals: 0,
                answer_rows_dropped: false,
                cycle_id: 1,
                request_id: crate::owners::contract::next_request_id(),
                sequence,
                sends: 0,
                sent: false,
                attempt_unsent: false,
                pending_owned: false,
                pair_in_flight: false,
                waiting_retry: false,
                landing: Vec::new(),
                missing_destroy: false,
                swaps: Vec::new(),
                missing: Vec::new(),
                result: None,
                stale: Vec::new(),
                landed: None,
                shown: None,
                recorded: false,
                outcome: None,
                error: None,
                failed_errors: BTreeMap::new(),
                retry_uploads: Vec::new(),
                reject_error: None,
                row_never_created: false,
                ended_dependents: Vec::new(),
                cause: None,
                cause_stored: None,
                cancels: None,
                before_cancel: None,
                restore_element: None,
                rows_pending: false,
                body_temps: Ok(Vec::new()),
            },
        );
        if let Some(operation) = self.ops.get_mut(&op_id) {
            let temps = Self::body_temps(operation);
            operation.body_temps = temps;
        }
        self.next_op = self.next_op.max(op_id + 1);
    }

    pub(super) fn emit_send(&mut self, op_id: OpId, fx: &mut Effects<Outbox>) {
        let Some(operation) = self.ops.get_mut(&op_id) else { return };
        Self::resolve_operation(&self.aliases, &mut operation.op);
        if operation.body_temps.as_ref().is_ok_and(|temps| !temps.is_empty()) {
            let temps = Self::body_temps(operation);
            operation.body_temps = temps;
        }
        operation.sends += 1;
        operation.sent = true;
        operation.attempt_unsent = false;
        operation.pair_in_flight = true;
        operation.waiting_retry = false;
        let purpose = if matches!(operation.op.origin, OpSource::Transfer { .. } | OpSource::TransferGroup { .. }) { PURPOSE_TRANSFER } else { PURPOSE_STANDARD };
        let owner = Some(crate::transport::RequestOwner::Operation { seq: i64::try_from(op_id).unwrap_or(i64::MAX) });
        let mutate = SessionInput::Mutate { author: OUTBOX_AUTHOR, owner, req_id: operation.request_id, epoch: self.epoch, path: operation.op.path.clone(), body: operation.op.body.clone(), purpose };
        if let Some(session) = self.peers.session { fx.push(Effect::Send(Outbound::new(session, mutate))); }
    }

    pub(super) fn store(&mut self, op_id: OpId, kind: CommitKind, fx: &mut Effects<Outbox>) {
        let Some(operation) = self.ops.get(&op_id) else { return };
        let mut written: Vec<OpId> = Vec::new();
        let attempt = StoreOp::Attempt { op_id, attempts: if kind == CommitKind::ClearMark { 1 } else { operation.attempts + 1 }, cycle_id: operation.cycle_id, cause: if kind == CommitKind::ClearMark { None } else { operation.cause } };
        let mut ops: Vec<StoreOp> = match kind {
            CommitKind::Intent => vec![StoreOp::Intent {
                op_id,
                created_at: operation.created_at,
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
            CommitKind::Land => (operation.answer_rows_dropped && operation.swaps.is_empty() && !Self::created_temps(operation).is_empty()).then_some(StoreOp::RollBack { op_id })
                .into_iter()
                .chain(operation.swaps.iter()
                .filter(|_| !operation.answer_rows_dropped)
                .map(|(temp, real)| StoreOp::Swap { model: operation.op.model.clone(), temp: temp.clone(), real: real.clone() }))
                .chain(operation.landing.iter().map(|(model, rows)| StoreOp::Rows { model: model.clone(), rows: rows.clone() }))
                .chain(operation.missing_destroy.then(|| StoreOp::RemoveServerRows { model: operation.op.model.clone(), ids: vec![operation.op.row_id.clone()] }))
                .chain((!operation.stale.is_empty()).then(|| StoreOp::StaleFamilies { families: operation.stale.clone() }))
                .chain([StoreOp::RemoveIntent { op_id }, StoreOp::RemoveAnswer { request_id: operation.request_id }])
                .collect(),
            CommitKind::Rollback if operation.restore_element.is_some() => operation.restore_element.iter().map(|(table, key)| StoreOp::DiscardRestoreFailed { table: table.clone(), key: key.clone() }).chain([StoreOp::RemoveIntent { op_id }]).collect(),
            CommitKind::Rollback => (!operation.answer_rows_dropped).then_some(StoreOp::RollBack { op_id }).into_iter().chain([StoreOp::RemoveIntent { op_id }, StoreOp::RemoveAnswer { request_id: operation.request_id }])
                .chain(((operation.pending_owned || operation.pair_in_flight) && operation.stage == Stage::RollingBack(Why::Discarded)).then_some(StoreOp::ReleasePendingOwner { request_id: operation.request_id }))
                .collect(),
            CommitKind::ErrorMark => vec![StoreOp::MarkError { op_id, error: Some(operation.error.clone().unwrap_or_else(|| "refused".into())) }, StoreOp::RemoveAnswer { request_id: operation.request_id }],
            CommitKind::ClearMark => std::iter::once(StoreOp::MarkError { op_id, error: None }).chain(std::iter::once(attempt)).chain(operation.retry_uploads.iter().cloned().map(|row| StoreOp::Upload { row, kind: crate::owners::store::UploadKind::Retrying })).collect(),
            CommitKind::Requeue => vec![StoreOp::Requeue { op_id }],
            CommitKind::Cause => vec![StoreOp::Cause { op_id, cause: operation.cause }],
            CommitKind::RemoveRow => vec![StoreOp::RemoveIntent { op_id }, StoreOp::ReleasePendingOwner { request_id: operation.request_id }, StoreOp::RemoveAnswer { request_id: operation.request_id }],
            CommitKind::LocalWrite | CommitKind::Record => Vec::new(),
        };
        let terminal = match kind {
            CommitKind::Land => Some(OutboxOutcome::Confirmed { revision: 0, result: operation.result.clone(), row_id: Some(operation.landed.clone().unwrap_or_else(|| operation.op.row_id.clone())) }),
            CommitKind::Rollback => Some(if operation.stage == Stage::RollingBack(Why::Discarded) { OutboxOutcome::Discarded } else { OutboxOutcome::Rejected { error: operation.reject_error.clone().unwrap_or_else(|| "refused".into()) } }),
            CommitKind::ErrorMark => Some(OutboxOutcome::Failed { error: operation.error.clone().unwrap_or_else(|| "refused".into()) }),
            _ => None,
        };
        if kind == CommitKind::Rollback && operation.stage == Stage::RollingBack(Why::Discarded) {
            if let OpSource::TransferGroup { members, .. } = &operation.op.origin { if let [member] = members.as_slice() { ops.push(StoreOp::RemoveUploadByTransferId { transfer_id: member.transfer_id }); } }
        }
        if kind == CommitKind::Land {
            let leaving = self.member_removals.get(&op_id).filter(|pending| pending.land_after_storage).map(|pending| pending.member.transfer_id);
            if let OpSource::TransferGroup { members, .. } = &operation.op.origin {
                ops.extend(members.iter().filter(|member| Some(member.transfer_id) != leaving && !operation.missing.contains(&member.row_id)).filter_map(|member| member.op_id).map(|op_id| StoreOp::RemoveIntent { op_id }));
            }
            if let Some(pending) = self.member_removals.get(&op_id).filter(|pending| pending.land_after_storage) {
                ops.push(StoreOp::RemoveUploadByTransferId { transfer_id: pending.member.transfer_id });
                ops.push(StoreOp::Outcome { owner: OutcomeOwner::Transfer, id: pending.member.transfer_id, cycle_id: pending.member.cycle_id, model: operation.op.model.clone(), outcome: OutboxOutcome::Discarded });
            }
        }
        if kind == CommitKind::Rollback && self.orphan_transfers.contains(&op_id) {
            if let OpSource::Transfer { transfer_id, cycle_id } = &operation.op.origin {
                ops.push(StoreOp::RetireTransferOutcome { transfer_id: *transfer_id, cycle_id: *cycle_id });
                ops.push(StoreOp::Outcome { owner: OutcomeOwner::Transfer, id: *transfer_id, cycle_id: *cycle_id, model: operation.op.model.clone(), outcome: OutboxOutcome::Discarded });
                written.push(op_id);
            }
        } else if let Some(outcome) = terminal {
            let failed_before = kind == CommitKind::Rollback && operation.failed_errors.contains_key(&operation.cycle_id);
            ops.extend(Self::outcome_records(operation, &outcome, !failed_before));
            written.push(op_id);
        }
        if kind == CommitKind::RemoveRow {
            ops.extend(Self::outcome_records(operation, &OutboxOutcome::Discarded, false));
            written.push(op_id);
        }
        if kind == CommitKind::Rollback {
            for cancelling in self.ops.values().filter(|other| other.cancels == Some(op_id) && other.stage == Stage::Submitting) {
                ops.extend(Self::outcome_records(cancelling, &OutboxOutcome::Confirmed { revision: 0, result: None, row_id: None }, true));
                written.push(cancelling.op_id);
            }
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
                earlier.extend(Self::outcome_records(other, &OutboxOutcome::Discarded, true));
                written.push(other.op_id);
            }
            earlier.append(&mut ops);
            ops = earlier;
        }
        let recorded = if kind == CommitKind::Rollback { self.ops.get(&op_id).map(|create| create.ended_dependents.clone()).unwrap_or_default() } else { Vec::new() };
        let dependents = if kind != CommitKind::Rollback { Vec::new() } else if recorded.is_empty() { self.dependents_of(op_id) } else { recorded };
        if !dependents.is_empty() {
            let outcome = OutboxOutcome::Rejected { error: super::failure::row_never_created() };
            let mut earlier = Vec::new();
            for id in &dependents {
                let other = &self.ops[id];
                if let Some((table, key)) = &other.restore_element {
                    earlier.push(StoreOp::DiscardRestoreFailed { table: table.clone(), key: key.clone() });
                } else {
                    earlier.extend([StoreOp::RollBack { op_id: *id }, StoreOp::RemoveAnswer { request_id: other.request_id }]);
                }
                earlier.push(StoreOp::RemoveIntent { op_id: *id });
                if other.pending_owned || other.pair_in_flight { earlier.push(StoreOp::ReleasePendingOwner { request_id: other.request_id }); }
                earlier.extend(Self::outcome_records(other, &outcome, true));
                written.push(other.op_id);
            }
            earlier.append(&mut ops);
            ops = earlier;
        }
        let intents = u64::from(kind == CommitKind::Intent);
        if let Some(own) = self.ops.get_mut(&op_id) { own.recorded = false; }
        for id in written {
            if let Some(other) = self.ops.get_mut(&id) { other.recorded = true; }
        }
        for id in &dependents {
            if let Some(other) = self.ops.get_mut(id) {
                other.stage = Stage::RollingBack(Why::Rejected);
                other.reject_error = Some(super::failure::row_never_created());
                other.row_never_created = true;
            }
        }
        if let Some(create) = self.ops.get_mut(&op_id) { create.ended_dependents = dependents; }
        let batch = self.next_batch;
        self.next_batch += 1;
        self.batches.insert(batch, Batch::Operation { op_id, kind });
        self.commit(batch, ops, intents, fx);
    }

    pub(super) fn commit(&self, id: BatchId, ops: Vec<StoreOp>, intents: u64, fx: &mut Effects<Outbox>) {
        let batch = StoreBatch { author: self.author, id, stamp: crate::runtime::EpochStamp::Epoch(self.epoch), rows: Vec::new(), intents, credential: None, ops };
        fx.push(Effect::Send(Outbound::new(self.peers.store, StoreInput::Commit(batch))));
    }

    pub(super) fn storage_retry(&mut self, op_id: OpId, fx: &mut Effects<Outbox>) {
        let Some(operation) = self.ops.get_mut(&op_id) else { return };
        operation.storage_refusals = operation.storage_refusals.saturating_add(1);
        fx.push(Effect::Backoff { id: ScheduleId(op_id), policy: RetryPolicy::Outbox, attempt: operation.storage_refusals, floor: std::time::Duration::ZERO, input: OutboxInput::RetryDue { op_id, cycle_id: operation.cycle_id } });
    }

    pub(super) fn retry_timer(&self, op_id: OpId, fx: &mut Effects<Outbox>) {
        let Some(operation) = self.ops.get(&op_id) else { return };
        fx.push(Effect::Backoff { id: ScheduleId(op_id), policy: RetryPolicy::Outbox, attempt: operation.attempts.max(1), floor: std::time::Duration::ZERO, input: OutboxInput::RetryDue { op_id, cycle_id: operation.cycle_id } });
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

    pub(super) fn answer_discarded_outcome(&mut self, op_id: OpId, answer: OutboxAnswer, fx: &mut Effects<Outbox>) {
        let Some((cycle, _)) = self.confirmed_discards.remove(&op_id) else { return };
        let (mine, rest): (Vec<_>, Vec<_>) = std::mem::take(&mut self.outcome_waiters).into_iter().partition(|(id, waits, _, _)| *id == op_id && *waits == cycle);
        self.outcome_waiters = rest;
        for (_, _, reply, _) in mine { Self::reply(fx, reply, answer.clone()); }
    }

    pub(super) fn discard_failed_outcome(&mut self, op_id: OpId, cycle: u64, error: String, fx: &mut Effects<Outbox>) {
        if self.confirmed_discards.contains_key(&op_id) { return; }
        self.confirmed_discards.insert(op_id, (cycle, error));
        self.start_discard(op_id, fx);
    }

    /// The records of the outcome of an operation: one per transfer the operation carries, and its own when a call of the app made it.
    fn outcome_records(operation: &QueuedOperation, outcome: &OutboxOutcome, with_transfers: bool) -> Vec<StoreOp> {
        let model = &operation.op.model;
        let transfers = Self::transfer_outcomes(operation, outcome).into_iter().filter(|_| with_transfers).map(|(id, cycle_id, outcome)| StoreOp::Outcome { owner: OutcomeOwner::Transfer, id, cycle_id, model: model.clone(), outcome });
        let own = (operation.op.origin == OpSource::Api).then(|| StoreOp::Outcome { owner: OutcomeOwner::Operation, id: operation.op_id, cycle_id: operation.cycle_id, model: model.clone(), outcome: outcome.clone() });
        transfers.chain(own).collect()
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
        let row_never_created = matches!(outcome, OutboxOutcome::Rejected { .. }) && operation.row_never_created;
        match &operation.op.origin {
            OpSource::Api => {}
            OpSource::Transfer { .. } => if let Some((transfer_id, cycle_id, outcome)) = outcomes.into_iter().next() {
                fx.push(Effect::Send(Outbound::new(transfer, TransferInput::MutationOutcome { transfer_id, cycle_id, outcome, intent_removed, row_never_created })));
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
    pub(super) fn finish(&mut self, now: u64, op_id: OpId, outcome: OutboxOutcome, next_goes: bool, fx: &mut Effects<Outbox>) {
        self.orphan_transfers.remove(&op_id);
        let Some(operation) = self.ops.get_mut(&op_id) else { return };
        operation.stage = Stage::Done;
        operation.outcome = Some(outcome.clone());
        let op = operation.op.clone();
        self.bump_selection(&op);
        self.tell_transfer(op_id, &outcome, next_goes, fx);
        self.give_outcome(op_id, &outcome, fx);
        if !matches!(outcome, OutboxOutcome::Ended(_)) {
            if self.ops.get(&op_id).is_some_and(|operation| operation.recorded) { self.ops.remove(&op_id); }
            self.drain(now, fx);
        }
    }

    /// A Queued operation on a free row ends `Rejected{Expired}` when its time is over, else goes to the network while a slot is free: the first `Mutate` of the cycle at once, a repeat after its attempt is written, the 41st attempt never.
    pub(super) fn try_start(&mut self, now: u64, op_id: OpId, fx: &mut Effects<Outbox>) {
        let Some(operation) = self.ops.get(&op_id) else { return };
        if operation.stage != Stage::Queued || !self.is_online() {
            return;
        }
        if let Err(detail) = Self::awaited_temps(operation) {
            return self.fail_with(op_id, format!("Internal(Invalid): {detail}"), fx);
        }
        let ended = self.awaits_ended_create(operation);
        if !ended && self.waits_for_turn(op_id) {
            return;
        }
        if ended || self.expired(now, op_id) {
            if let Some(operation) = self.ops.get_mut(&op_id) {
                operation.stage = Stage::RollingBack(Why::Rejected);
                operation.reject_error = Some(if ended { super::failure::row_never_created() } else { super::failure::expired() });
                operation.row_never_created = ended;
            }
            return self.store(op_id, CommitKind::Rollback, fx);
        }
        if self.slots_taken() >= DRAIN_LIMIT {
            return;
        }
        if operation.attempts >= ATTEMPT_LIMIT {
            self.fail_exhausted(op_id, fx);
        } else if operation.attempt_unsent {
            if self.phase == Phase::Ready { self.start_sending(op_id, fx); }
        } else {
            self.begin_attempt(op_id, fx);
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

    /// An operation that enters `Failed` with an unresolved pair lets Session replay it: `Release` once, the owner mark stays until the answer of the pair.
    pub(super) fn hand_pair_to_session(&mut self, op_id: OpId, fx: &mut Effects<Outbox>) {
        let Some(operation) = self.ops.get(&op_id).filter(|operation| operation.pending_owned) else { return };
        let request_id = operation.request_id;
        if let Some(session) = self.peers.session { fx.push(Effect::Send(Outbound::new(session, SessionInput::Release { req_id: request_id }))); }
    }

    /// The operation is `Failed` with `error` on its row: `outcome` waiters get `Failed`.
    pub(super) fn failed(&mut self, op_id: OpId, error: String, fx: &mut Effects<Outbox>) {
        let Some(operation) = self.ops.get_mut(&op_id) else { return };
        operation.stage = Stage::Failed;
        operation.error = Some(error.clone());
        let cycle = operation.cycle_id;
        operation.failed_errors.insert(cycle, error.clone());
        self.hand_pair_to_session(op_id, fx);
        let failed = OutboxOutcome::Failed { error };
        self.tell_transfer(op_id, &failed, false, fx);
        if self.outcome_waiters.iter().any(|(id, waits, _, discard)| *id == op_id && *waits == cycle && *discard) {
            if let OutboxOutcome::Failed { error } = failed { self.discard_failed_outcome(op_id, cycle, error, fx); }
        } else {
            self.give_outcome(op_id, &failed, fx);
        }
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
            OpSource::TransferGroup { members, .. } => members.iter().any(|member| member.transfer_id == transfer && member.cycle_id == cycle),
            _ => false,
        }).map(|operation| operation.op_id).next()
    }

    pub(super) fn open_on_row(&self, model: &str, row: &str, action: Option<&str>) -> Option<OpId> {
        let row = alias_in(&self.aliases, model, row).map_or(row, String::as_str);
        self.ops.values().filter(|operation| Self::addresses_row(operation, model, row) && action.is_none_or(|action| operation.op.action == action) && operation.stage != Stage::Done).min_by_key(|operation| operation.sequence).map(|operation| operation.op_id)
    }

    /// A call without an action asks Transfer, which holds the transfers that have no operation yet: the smallest open operation of the row here is the bound.
    fn select_row(&mut self, (model, row_id): (String, String), kind: SelectKind, reply: ReplyId, fx: &mut Effects<Outbox>) {
        let resolved = alias_in(&self.aliases, &model, &row_id).cloned();
        let outbox_min = self.open_on_row(&model, &row_id, None).map(|op_id| (op_id, self.ops[&op_id].op.action.clone()));
        let Some(transfer) = self.peers.transfer else {
            return match outbox_min {
                Some((_, action)) => self.selected((model, row_id), action, kind, reply, fx),
                None => Self::reply(fx, reply, OutboxAnswer::Err(Fault::NotFound)),
            };
        };
        let key = self.selection_key(&model, &row_id);
        let version = *self.selection_versions.entry(key).or_insert(0);
        self.selections.insert(reply, (model.clone(), row_id.clone(), kind));
        let rows = std::iter::once(row_id).chain(resolved).collect();
        fx.push(Effect::Send(Outbound::new(transfer, super::transfer::TransferInput::SelectRow { model, rows, outbox_min, kind, reply, version })));
    }

    pub(super) fn bump_selection(&mut self, op: &NewOp) {
        let rows: BTreeSet<String> = match &op.origin { OpSource::TransferGroup { members, .. } => members.iter().map(|member| member.row_id.clone()).collect(), _ => [op.row_id.clone()].into_iter().collect() };
        for row in rows { self.bump_selection_row(&op.model, &row); }
    }

    /// Only a row with a selection in flight has a version: the entry goes when its last selection settles.
    pub(super) fn bump_selection_row(&mut self, model: &str, row: &str) {
        for key in [(model.to_string(), row.to_string()), self.selection_key(model, row)] {
            if let Some(version) = self.selection_versions.get_mut(&key) {
                self.selection_clock += 1;
                *version = self.selection_clock;
            }
        }
    }

    fn selection_key(&self, model: &str, row: &str) -> (String, String) {
        (model.to_string(), alias_in(&self.aliases, model, row).cloned().unwrap_or_else(|| row.to_string()))
    }

    fn selection_version(&self, model: &str, row: &str) -> u64 { self.selection_versions.get(&self.selection_key(model, row)).copied().unwrap_or(0) }

    fn forget_selection_versions(&mut self) {
        let live: BTreeSet<(String, String)> = self.selections.values().map(|(model, row, _)| self.selection_key(model, row)).collect();
        self.selection_versions.retain(|key, _| live.contains(key));
    }

    fn selection_current(&mut self, reply: ReplyId, version: u64, fx: &mut Effects<Outbox>) -> bool {
        let Some((model, row, kind)) = self.selections.get(&reply).cloned() else { return false };
        if self.selection_version(&model, &row) != version {
            self.select_row((model, row), kind, reply, fx);
            return false;
        }
        self.selections.remove(&reply);
        self.forget_selection_versions();
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
        for reply in Self::take_waiters(&mut self.discard_waiting, op_id) {
            Self::reply(fx, reply, answer.clone());
        }
    }

    pub(super) fn interrupt_all(&mut self, cause: crate::ffi::EndCause, fx: &mut Effects<Outbox>) {
        let fault = Fault::Ended(cause.clone());
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

    pub(super) fn handle(&mut self, now: u64, input: OutboxInput, fx: &mut Effects<Outbox>) {
        let answered = match &input {
            OutboxInput::Response { op_id, .. } | OutboxInput::MissingDestroy { op_id, .. } | OutboxInput::Unreadable { op_id, .. } | OutboxInput::TransportError { op_id, .. } | OutboxInput::Dropped { op_id } | OutboxInput::Refused { op_id, .. } => Some(*op_id),
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
        if let (Some(error), OutboxInput::ReserveReplacement { target, .. }) = (&self.queue_error, &input) {
            if let Some(transfer) = self.peers.transfer { fx.push(Effect::Send(Outbound::new(transfer, TransferInput::ReplacementReserved { target: *target, result: Err(Fault::Rejected(RejectReason::Storage(error.clone()))) }))); }
            return;
        }
        if let (Some(error), OutboxInput::RetryTransfer { uploads, .. }) = (&self.queue_error, &input) {
            self.retry_transfer_answer(uploads.iter().map(|row| (row.transfer_id, row.cycle_id)).collect(), Err(Fault::Rejected(RejectReason::Storage(error.clone()))), fx);
            return;
        }
        if let (Some(error), OutboxInput::Submit { reply, .. } | OutboxInput::Retry { reply, .. } | OutboxInput::Discard { reply, .. } | OutboxInput::Outcome { reply, .. }) = (&self.queue_error, &input) {
            return Self::reply(fx, *reply, OutboxAnswer::Err(Fault::Rejected(RejectReason::Storage(error.clone()))));
        }
        match input {
            OutboxInput::Submit { epoch, op, reply } => self.submit(now, epoch, op, reply, fx),
            OutboxInput::ReserveOp { epoch, target } => self.reserve_op(epoch, target, fx),
            OutboxInput::ReserveReplacement { epoch, target, transfer_id, cycle_id } => self.reserve_replacement(epoch, target, transfer_id, cycle_id, fx),
            OutboxInput::FinishReplacement { target, committed } => self.finish_replacement(now, target, committed, fx),
            OutboxInput::SubmitReserved { epoch, op_id, op, created_at } => self.submit_reserved(now, epoch, op_id, op, created_at, fx),
            OutboxInput::Restored { .. } | OutboxInput::RestoreFailed { .. } | OutboxInput::ReservedFloor { .. } | OutboxInput::AliasesRestored { .. } => {}
            OutboxInput::OrphanAnswers { request_ids } => self.remove_orphan_answers(request_ids, fx),
            OutboxInput::OrphanTransfers { op_ids } => self.orphan_transfers.extend(op_ids),
            OutboxInput::RestoreReady { epoch } if epoch == self.epoch => self.restore_ready(now, fx),
            OutboxInput::RestoreReady { .. } => {}
            OutboxInput::RegisterOperations { declarations, complete } => self.register(now, declarations, complete, fx),
            OutboxInput::Committed(committed) => self.committed(now, committed, fx),
            OutboxInput::Rejected(rejected) => self.rejected(now, rejected, fx),
            OutboxInput::Response { op_id, rows, swaps, stale, result, missing, errors } => self.on_response(now, op_id, super::outbox_flow::Answered { rows, swaps, stale, result, missing, missing_destroy: false, unreadable: false }, errors, fx),
            OutboxInput::MissingDestroy { op_id, stale } => self.on_response(now, op_id, super::outbox_flow::Answered { rows: Vec::new(), swaps: Vec::new(), stale, result: None, missing: Vec::new(), missing_destroy: true, unreadable: false }, None, fx),
            OutboxInput::Unreadable { op_id, stale } => self.on_response(now, op_id, super::outbox_flow::Answered { rows: Vec::new(), swaps: Vec::new(), stale, result: None, missing: Vec::new(), missing_destroy: false, unreadable: true }, None, fx),
            OutboxInput::OutcomeReadFailed { reply } => Self::reply(fx, reply, OutboxAnswer::Err(Fault::Runtime("the outcome read failed".into()))),
            OutboxInput::TransportError { op_id, temporary } => self.transport_failed(now, op_id, temporary, None, fx),
            OutboxInput::Dropped { op_id } => self.transport_failed(now, op_id, true, None, fx),
            OutboxInput::Answered { req_id, outcome } => self.answered(now, req_id, outcome, fx),
            OutboxInput::Refused { op_id, temporary, detail } => {
                self.rows_arrived(op_id);
                if self.stage_of(op_id) == Some(Stage::Sending) {
                    if let Some(operation) = self.ops.get_mut(&op_id) { operation.reject_error = Some(detail); }
                    self.transport_failed(now, op_id, temporary, None, fx);
                }
            }
            OutboxInput::Retry { model, row_id, action: None, reply } => self.select_row((model, row_id), SelectKind::Retry, reply, fx),
            OutboxInput::Retry { model, row_id, action, reply } => self.retry(&model, &row_id, action.as_deref(), reply, fx),
            OutboxInput::RetryTransfer { model, row_id, reply, uploads } => self.retry_transfer(&model, &row_id, reply, uploads, fx),
            OutboxInput::Discard { model, row_id, action: None, reply } => self.select_row((model, row_id), SelectKind::Discard, reply, fx),
            OutboxInput::Discard { model, row_id, action, reply } => self.discard(&model, &row_id, action.as_deref(), reply, fx),
            OutboxInput::DiscardTransfer { transfer_id, cycle_id, reply } => match self.open_for_transfer(transfer_id, cycle_id) {
                Some(op_id) => {
                    if self.stage_of(op_id).is_some_and(|stage| stage == Stage::Failed || Self::defers_discard(stage)) { self.transfer_discards.insert(op_id, (transfer_id, cycle_id)); }
                    self.discard_op(op_id, reply, fx);
                }
                None => Self::reply(fx, reply, OutboxAnswer::Err(Fault::NotFound)),
            },
            OutboxInput::SelectedOutbox { model, row_id, action, kind, reply, version } => { if self.selection_current(reply, version, fx) { self.selected((model, row_id), action, kind, reply, fx); } }
            OutboxInput::SelectedNone { reply, version } => { if self.selection_current(reply, version, fx) { Self::reply(fx, reply, OutboxAnswer::Err(Fault::NotFound)); } }
            OutboxInput::SelectedTransfer { reply, version, .. } => { if self.selection_current(reply, version, fx) { if let Some(transfer) = self.peers.transfer { fx.push(Effect::Send(Outbound::new(transfer, super::transfer::TransferInput::SelectConfirmed { reply }))); } } }
            OutboxInput::Outcome { receipt, reply, discard_on_failure } => self.outcome(receipt, reply, discard_on_failure, fx),
            OutboxInput::Resolve { .. } => {}
            #[cfg(test)]
            OutboxInput::WhenQuiet { .. } => {}
            OutboxInput::Online(Online(on)) => self.online_changed(now, on, fx),
            OutboxInput::ActivateEpoch(epoch) => self.begin_epoch(epoch),
            OutboxInput::EpochStarted(_) => {}
            OutboxInput::EpochEnded(ended) => self.interrupt_all(crate::ffi::EndCause::of(ended.cause), fx),
            OutboxInput::Closing => {
                self.interrupt_all(crate::ffi::EndCause::Closed, fx);
                self.closing = true;
                self.maybe_drained(fx);
            }
            OutboxInput::RetryDue { op_id, cycle_id } => self.retry_due(op_id, cycle_id, fx),
            OutboxInput::LocalWrite { model, epoch, write, row, values, reply } => self.local_write(now, LocalCall { model, kind: write, row, values }, epoch, reply, fx),
            OutboxInput::Expire { model, row, due } => self.expire(model, row, due, fx),
            OutboxInput::ArmExpiry { model, row, revision, deadline } => self.arm_expiry(model, row, revision, deadline, fx),
            OutboxInput::ArmReset { reset } => self.arm_reset(reset, fx),
            OutboxInput::ResetField { reset } => self.reset_field(reset, fx),
            OutboxInput::RowsGone { ops } => self.rows_gone(now, ops, fx),
            OutboxInput::RowRemoved { model, row_id, revision } => self.row_removed(&model, &row_id, revision, fx),
            OutboxInput::AliasFacts { swaps, ended } => {
                for (model, temp, real) in swaps { self.swap_row(&model, &temp, &real); }
                self.end_temps(ended.into_iter().map(|(_, temp)| temp));
                self.drain(now, fx);
            }
            OutboxInput::DisarmExpiry { model, row } => self.disarm_expiry(&model, &row, fx),
            OutboxInput::ExpireRow { model, row, revision } => self.expire_row(model, row, revision, fx),
        }
    }
}

pub struct Outbox;

impl StateOwner for Outbox {
    const EPOCHED: bool = true;

    type State = OutboxState;
    type Input = OutboxInput;
    type Effect = OutboxEffect;

    #[cfg(test)]
    fn diagnostic(state: &Self::State) -> Option<String> {
        let ops = state.ops.iter().map(|(id, op)| format!("{id}:{:?}:retry={}", op.stage, op.waiting_retry)).collect::<Vec<_>>();
        Some(format!("phase={:?}, online={:?}, ops={ops:?}, batches={:?}, quiet_waiters={:?}", state.phase, state.online, state.batches.keys().collect::<Vec<_>>(), state.quiet_waiters))
    }

    fn step(state: &mut OutboxState, input: OutboxInput, now: crate::runtime::Time) -> Effects<Self> {
        let mut fx: Effects<Self> = Vec::new();
        let now = now.0;
        if state.closing {
            match &input {
                OutboxInput::Committed(committed) if matches!(state.batches.get(&committed.batch), Some(Batch::Write { .. })) => { state.handle(now, input, &mut fx); return fx; }
                OutboxInput::Rejected(rejected) if matches!(state.batches.get(&rejected.batch), Some(Batch::Write { .. })) => { state.handle(now, input, &mut fx); return fx; }
                _ => {}
            }
            match &input {
                OutboxInput::Submit { reply, .. } | OutboxInput::Retry { reply, .. } | OutboxInput::Discard { reply, .. } | OutboxInput::DiscardTransfer { reply, .. } | OutboxInput::Outcome { reply, .. } | OutboxInput::Resolve { reply, .. } | OutboxInput::LocalWrite { reply, .. } => OutboxState::reply(&mut fx, *reply, OutboxAnswer::Err(Fault::Ended(crate::ffi::EndCause::Closed))),
                #[cfg(test)]
                OutboxInput::WhenQuiet { reply } => OutboxState::reply(&mut fx, *reply, OutboxAnswer::Err(Fault::Ended(crate::ffi::EndCause::Closed))),
                OutboxInput::ReserveOp { target, .. } => state.opened(&mut fx, *target, Err(Fault::Ended(crate::ffi::EndCause::Closed))),
                OutboxInput::ReserveReplacement { target, .. } => if let Some(transfer) = state.peers.transfer { fx.push(Effect::Send(Outbound::new(transfer, TransferInput::ReplacementReserved { target: *target, result: Err(Fault::Ended(crate::ffi::EndCause::Closed)) }))); },
                OutboxInput::RetryTransfer { uploads, .. } => state.retry_transfer_answer(uploads.iter().map(|row| (row.transfer_id, row.cycle_id)).collect(), Err(Fault::Ended(crate::ffi::EndCause::Closed)), &mut fx),
                _ => {}
            }
            return fx;
        }
        if let OutboxInput::ActivateEpoch(epoch) = &input {
            state.begin_epoch(*epoch);
            return fx;
        }
        if let OutboxInput::Resolve { model, row, reply } = input {
            let resolved = alias_in(&state.aliases, &model, &row).cloned().unwrap_or(row);
            OutboxState::reply(&mut fx, reply, OutboxAnswer::Resolved(resolved));
            return fx;
        }
        #[cfg(test)]
        if let OutboxInput::WhenQuiet { reply } = input {
            state.quiet_waiters.push(reply);
        } else if state.phase == Phase::Restoring {
            state.restoring(now, input, &mut fx);
        } else {
            state.handle(now, input, &mut fx);
        }
        #[cfg(not(test))]
        if state.phase == Phase::Restoring {
            state.restoring(now, input, &mut fx);
        } else {
            state.handle(now, input, &mut fx);
        }
        state.run_waiting_discards(&mut fx);
        #[cfg(test)]
        state.release_quiet(&mut fx);
        fx
    }

    fn epoch_started(epoch: Epoch) -> Option<OutboxInput> { Some(OutboxInput::ActivateEpoch(epoch)) }

    fn epoch_ended(epoch: Epoch, cause: Cause) -> OutboxInput { OutboxInput::EpochEnded(EpochEnded { epoch, cause }) }

    fn executor_failed(_executor: &str, pending: Vec<crate::runtime::EffectId>) -> Vec<OutboxInput> {
        pending.into_iter().map(|id| if id & OUTCOME_READ_TASK != 0 { OutboxInput::OutcomeReadFailed { reply: id & !OUTCOME_READ_TASK } } else { OutboxInput::Unreadable { op_id: id & !DECODE_TASK, stale: Vec::new() } }).collect()
    }

    fn closing() -> OutboxInput { OutboxInput::Closing }
}
