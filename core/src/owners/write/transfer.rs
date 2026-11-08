//! Owner `Transfer`: file upload steps. One instance per epoch with an identity.
//! The types are in `transfer_types`, the rows of the table in `transfer_{open,accept,steps,end}`; this file holds the helpers of the rows and the dispatch of the inputs.

use std::collections::BTreeMap;

pub use super::transfer_types::*;
use super::outbox::{Fault, NewOp, OpSource, OutboxInput, OutboxOutcome, Receipt};
use super::outbox_types::Phase;
use super::rounds::RoundsInput;
use crate::owners::contract::{BatchAuthor, Drained, EpochEnded, Online, RejectReason, ReplyId};
use crate::owners::store::{StoreBatch, StoreInput, StoreOp, UploadKind};
use crate::runtime::RetryPolicy;
use crate::runtime::{Cause, Effect, Effects, Epoch, Outbound, ScheduleId, StateOwner, Time};

impl TransferState {
    pub(super) fn shown_now(&self, id: TransferId) -> Shown {
        let Some(transfer) = self.transfers.get(&id) else { return Shown::Gone };
        let progress = transfer.confirmed_parts * 100 / transfer.source.parts.max(1);
        match transfer.stage {
            TransferStage::Accepting | TransferStage::Copying | TransferStage::CopyReady => Shown::Copying,
            TransferStage::Failed => Shown::Failed { error: transfer.error.clone().unwrap_or_default() },
            TransferStage::Done => Shown::Gone,
            TransferStage::Completing | TransferStage::PersistingComplete | TransferStage::BatchReady | TransferStage::PersistingBatchIntent | TransferStage::Mutating => Shown::Uploading { progress: 100 },
            TransferStage::Uploading | TransferStage::PersistingPart => Shown::Uploading { progress },
            _ => Shown::Queued,
        }
    }

    /// `uploads[action]` and every `Blob` stream of the transfer get `shown`.
    pub(super) fn show(&self, id: TransferId, shown: Shown, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get(&id) else { return };
        run(fx, TransferEffect::Shows { key: transfer.key.clone(), shown: shown.clone() });
        for stream_id in self.subscribers.get(&id).into_iter().flatten() {
            run(fx, TransferEffect::Status { stream_id: *stream_id, transfer_id: id, shown: shown.clone() });
        }
    }

    pub(super) fn release_step(&mut self, id: TransferId, fx: &mut Fx) {
        if let Some(request_id) = self.transfers.get_mut(&id).and_then(|transfer| transfer.pending_step.take()) {
            run(fx, TransferEffect::ReleaseStep { request_id });
        }
    }

    pub(super) fn step_answered(&mut self, id: TransferId) {
        if let Some(transfer) = self.transfers.get_mut(&id) { transfer.pending_step = None; }
    }

    pub(super) fn set(&mut self, id: TransferId, stage: TransferStage) {
        if let Some(transfer) = self.transfers.get_mut(&id) {
            transfer.stage = stage;
        }
    }

    /// One Store batch for `ids`: a row of `sync_upload` per transfer, or its removal.
    pub(super) fn store(&mut self, ids: &[TransferId], kind: StoreKind, fx: &mut Fx) { self.store_basis(ids, kind, BTreeMap::new(), fx); }

    /// The same batch again with the generations the Store named (`Rejected(StaleGeneration)`).
    fn store_basis(&mut self, ids: &[TransferId], kind: StoreKind, basis: BTreeMap<String, u64>, fx: &mut Fx) {
        if kind == StoreKind::BatchIntent {
            return self.store_batch_intent(ids, basis, fx);
        }
        let leaving: Vec<TransferId> = if kind == StoreKind::Removed { ids.iter().copied().filter(|id| matches!(self.transfers.get(id).and_then(|transfer| transfer.removal.as_ref()), Some(Removal::Cancel))).collect() } else { Vec::new() };
        let numbers: Vec<StoreOp> = leaving.into_iter().flat_map(|id| self.renumber_after(id)).collect();
        let ops: Vec<StoreOp> = ids
            .iter()
            .filter_map(|id| self.transfers.get(id))
            .flat_map(|transfer| {
                let kind = if kind == StoreKind::MixedOutcomes {
                    if transfer.stage == TransferStage::Removing { StoreKind::Removed } else { StoreKind::Failed }
                } else { kind };
                let own = transfer.op_id.filter(|_| !transfer.submitted || transfer.prior == Some(TransferStage::Failed));
                let mut ops = Vec::new();
                if kind == StoreKind::Removed {
                    ops.push(StoreOp::RemoveUpload { key: transfer.key.clone() });
                    if let (Some(Removal::Cancel), Some(upload_id)) = (&transfer.removal, Self::orphan_of(transfer)) {
                        ops.push(StoreOp::UploadOrphan { upload_id, key: transfer.key.clone() });
                    }
                    if let Some(op_id) = own.filter(|_| transfer.restored || !transfer.mutation.effects.is_empty()) {
                        match transfer.removal {
                            Some(Removal::Confirmed { .. }) if transfer.batch_id.is_some() => ops.push(StoreOp::RemoveIntent { op_id }),
                            Some(Removal::Confirmed { .. }) => {}
                            _ => ops.extend([StoreOp::RollBack { op_id }, StoreOp::RemoveIntent { op_id }]),
                        }
                    }
                } else {
                    let cached_copy = kind == StoreKind::SyncUpload && transfer.stage == TransferStage::PersistingCopy && transfer.from_cache;
                    let kind = if cached_copy { StoreKind::Completed } else { kind };
                    ops.push(StoreOp::Upload { row: upload_row(transfer, kind), kind: upload_kind(kind) });
                    if kind == StoreKind::Failed && transfer.failure_abort {
                        if let Some(upload_id) = transfer.upload_id.clone() {
                            ops.push(StoreOp::UploadOrphan { upload_id, key: transfer.key.clone() });
                        }
                    }
                    if transfer.stage == TransferStage::PersistingCopy && transfer.cache_stale {
                        ops.push(StoreOp::BlobCacheRemove { content_sha256: transfer.sha256.clone() });
                    }
                    if let (StoreKind::Completed, TransferStage::PersistingComplete, false, Some(blob_id)) = (kind, transfer.stage, transfer.from_cache, transfer.signed_id.clone()) {
                        ops.push(StoreOp::BlobCachePut { content_sha256: transfer.sha256.clone(), blob_id, created_at: self.now.0 });
                    }
                    if let (StoreKind::SyncUpload, TransferStage::Accepting, Some(group)) = (kind, transfer.stage, transfer.batch_id) {
                        if let Some(entry) = self.groups.get(&group).filter(|entry| entry.ids.first() == Some(&transfer.transfer_id)) {
                            if let Some(op_id) = entry.op_id {
                                ops.push(StoreOp::UploadBatch { batch_id: group, op_id, joint: entry.joint.to_text() });
                            }
                        }
                    }
                    if let (Some(op_id), StoreKind::SyncUpload, TransferStage::Accepting, None) = (own, kind, transfer.stage, transfer.prior) {
                        if !transfer.mutation.effects.is_empty() {
                            ops.push(StoreOp::Effects { op_id, model: transfer.mutation.model.clone(), row_id: transfer.mutation.row_id.clone(), action: transfer.mutation.action.clone(), payload: transfer.mutation.body.clone(), effects: transfer.mutation.effects.clone() });
                        }
                    }
                    if let (Some(op_id), StoreKind::Completed, None, Some(signed)) = (own, kind, transfer.batch_id, transfer.signed_id.as_ref()) {
                        ops.push(StoreOp::Intent { op_id, model: transfer.mutation.model.clone(), row_id: transfer.mutation.row_id.clone(), action: transfer.mutation.action.clone(), payload: transfer.mutation.body.replace(SIGNED_ID_SENTINEL, signed), effects: Default::default(), origin: crate::owners::store::IntentOrigin::Transfer { transfer_id: transfer.transfer_id } });
                    }
                }
                ops
            })
            .chain(numbers)
            .collect();
        let id = self.next_batch;
        self.next_batch += 1;
        self.pending.insert(id, Pending { ids: ids.to_vec(), kind, orphan: None });
        let intents = if kind == StoreKind::SyncUpload { ids.len() as u64 } else { 0 };
        let mut batch = StoreBatch { author: BatchAuthor::Transfer, id, epoch: self.epoch, basis, rows: Vec::new(), intents, credential: None, ops };
        let entries = batch.models().into_iter().map(|model| {
            let generation = batch.basis.get(&model).copied().or_else(|| self.generations.get(&model).copied()).unwrap_or(self.base_generation);
            (model, generation)
        }).chain(batch.basis.clone());
        batch.basis = crate::owners::store::basis_for(entries);
        fx.push(Effect::Send(Outbound::new(self.peers.store, StoreInput::Commit(batch))));
    }

    /// The multipart upload a removed transfer leaves on the server: it gets `abort` until the server answers.
    pub(super) fn orphan_of(transfer: &Transfer) -> Option<String> { transfer.upload_id.clone().filter(|_| !transfer.aborted && transfer.signed_id.is_none()) }

    /// `abort` of every orphan whose abort is not in flight, when the network is up. `network`: the call comes from `Online(true)` or `RestoreReady`, so an abort already in flight is owed again.
    pub(super) fn send_orphans(&mut self, network: bool, fx: &mut Fx) {
        if self.online != Some(true) || self.ended || self.declarations.is_empty() {
            return;
        }
        for (upload_id, key) in &self.orphans {
            if self.orphan_flight.insert(upload_id.clone()) {
                run(fx, TransferEffect::AbortOrphan { key: key.clone(), upload_id: upload_id.clone() });
            } else if network {
                self.orphan_owed.insert(upload_id.clone());
            }
        }
    }

    pub(super) fn commit_ops(&mut self, ops: Vec<StoreOp>, fx: &mut Fx) -> Option<crate::owners::contract::BatchId> {
        if ops.is_empty() {
            return None;
        }
        let id = self.next_batch;
        self.next_batch += 1;
        self.pending.insert(id, Pending { ids: Vec::new(), kind: StoreKind::Removed, orphan: None });
        let mut batch = StoreBatch { author: BatchAuthor::Transfer, id, epoch: self.epoch, basis: BTreeMap::new(), rows: Vec::new(), intents: 0, credential: None, ops };
        batch.basis = crate::owners::store::basis_for(batch.models().into_iter().map(|model| (self.generations.get(&model).copied().unwrap_or(self.base_generation), model)).map(|(generation, model)| (model, generation)));
        fx.push(Effect::Send(Outbound::new(self.peers.store, StoreInput::Commit(batch))));
        Some(id)
    }

    pub(super) fn abort_done(&mut self, upload_id: String, answered: bool, fx: &mut Fx) {
        let owed = self.orphan_owed.remove(&upload_id);
        if !answered {
            let key = self.orphans.get(&upload_id).filter(|_| owed && self.online == Some(true) && !self.ended).cloned();
            match key {
                Some(key) => run(fx, TransferEffect::AbortOrphan { key, upload_id }),
                None => { self.orphan_flight.remove(&upload_id); }
            }
            return;
        }
        self.orphan_flight.remove(&upload_id);
        if self.orphans.remove(&upload_id).is_none() {
            return;
        }
        let id = self.next_batch;
        self.next_batch += 1;
        self.pending.insert(id, Pending { ids: Vec::new(), kind: StoreKind::Removed, orphan: None });
        let batch = StoreBatch { author: BatchAuthor::Transfer, id, epoch: self.epoch, basis: BTreeMap::new(), rows: Vec::new(), intents: 0, credential: None, ops: vec![StoreOp::RemoveOrphan { upload_id }] };
        fx.push(Effect::Send(Outbound::new(self.peers.store, StoreInput::Commit(batch))));
    }

    pub(super) fn rounds_needed(&self, ids: Vec<TransferId>, fx: &mut Fx) {
        if let (Some(rounds), false) = (self.peers.rounds, ids.is_empty()) {
            fx.push(Effect::Send(Outbound::new(rounds, RoundsInput::Needed { epoch: self.epoch, ids })));
        }
    }

    pub(super) fn rounds_idle(&self, id: TransferId, fx: &mut Fx) {
        if let Some(rounds) = self.peers.rounds {
            fx.push(Effect::Send(Outbound::new(rounds, RoundsInput::Idle { epoch: self.epoch, transfer_id: id })));
        }
    }

    pub(super) fn receipt(&self, id: TransferId) -> Receipt { Receipt { epoch: self.epoch, op_id: id, cycle_id: self.transfers.get(&id).map_or(0, |transfer| transfer.cycle_id), revision: 0, optimistic: None } }

    /// The immutable outcome of the current cycle: every waiter of the cycle gets it once.
    pub(super) fn settle(&mut self, id: TransferId, outcome: OutboxOutcome, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        let cycle = transfer.cycle_id;
        transfer.outcomes.entry(cycle).or_insert_with(|| outcome.clone());
        let (mine, rest): (Vec<_>, Vec<_>) = std::mem::take(&mut transfer.outcome_waiters).into_iter().partition(|(waits, _)| *waits == cycle);
        transfer.outcome_waiters = rest;
        let outcome = transfer.outcomes[&cycle].clone();
        for (_, waiter) in mine {
            reply(fx, waiter, TransferAnswer::Outcome(outcome.clone()));
        }
    }

    /// The transfer ends: `Done`, `uploads[action]` gone, the copy file goes by `DeleteFile`.
    pub(super) fn finish(&mut self, id: TransferId, fx: &mut Fx) {
        self.show(id, Shown::Gone, fx);
        self.set(id, TransferStage::Done);
        self.rounds_idle(id, fx);
    }

    pub(super) fn delete_copy(&self, id: TransferId, fx: &mut Fx) {
        if let Some(transfer) = self.transfers.get(&id) {
            run(fx, TransferEffect::DeleteFile { path: file_id(transfer.transfer_id) });
        }
    }

    pub(super) fn timer(&mut self) -> ScheduleId {
        self.next_timer += 1;
        ScheduleId(self.next_timer)
    }

    pub(super) fn schedule_expire(&mut self, fx: &mut Fx) {
        let id = self.timer();
        fx.push(Effect::Schedule { id, after: std::time::Duration::ZERO, input: TransferInput::Tick { now: self.now, kind: TransferTick::Expire } });
    }

    pub(super) fn schedule_retry(&mut self, id: TransferId, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        transfer.waiting_retry = true;
        transfer.in_flight = false;
        let attempt_id = self.next_attempt;
        self.next_attempt += 1;
        let attempt = transfer.attempts;
        transfer.attempt_id = attempt_id;
        let after = RetryPolicy::Upload.backoff().delay(attempt.saturating_sub(1));
        let timer = ScheduleId(RETRY_TIMER + attempt_id);
        fx.push(Effect::Schedule { id: timer, after, input: TransferInput::Tick { now: self.now, kind: TransferTick::Retry { transfer_id: id, attempt_id } } });
    }

    /// Everything that waits for the call answers `answer`; `Interrupted` for the end of the epoch and for the close.
    pub(super) fn interrupt(&mut self, id: TransferId, fx: &mut Fx) {
        self.release_step(id, fx);
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        let fault = |fault: Fault| TransferAnswer::Err(fault);
        let mut replies: Vec<ReplyId> = Vec::new();
        replies.extend(transfer.accept_reply.take());
        replies.extend(transfer.cancel_reply.take().map(|(reply, _)| reply));
        replies.extend(transfer.retry_reply.take());
        replies.append(&mut transfer.discard_waiters);
        let in_flight = std::mem::replace(&mut transfer.in_flight, false) && transfer.stage != TransferStage::PersistingAttempt;
        let grouped = transfer.batch_id.and_then(|group| self.groups.get_mut(&group).and_then(|group| group.reply.take()));
        for reply_id in replies.into_iter().chain(grouped) {
            reply(fx, reply_id, fault(Fault::Interrupted));
        }
        if in_flight {
            run(fx, TransferEffect::CancelStep { transfer_id: id });
        }
        self.settle(id, OutboxOutcome::Interrupted, fx);
        self.set(id, TransferStage::Done);
        self.show(id, Shown::Gone, fx);
    }

    pub(super) fn maybe_drained(&mut self, fx: &mut Fx) {
        let accepting = self.scan().any(|transfer| transfer.stage == TransferStage::Accepting);
        if self.closing && !accepting && !std::mem::replace(&mut self.drained, true) {
            run(fx, TransferEffect::Drained(Drained { owner: "transfer" }));
        }
    }

    fn committed(&mut self, batch: crate::owners::contract::BatchId, fx: &mut Fx) {
        let Some(pending) = self.pending.remove(&batch) else { return };
        if matches!(pending.kind, StoreKind::UploadId | StoreKind::LateOrphan) {
            if let Some((upload_id, key)) = pending.orphan {
                self.orphans.insert(upload_id, key);
                self.send_orphans(false, fx);
            }
            return self.maybe_drained(fx);
        }
        if pending.kind == StoreKind::MixedOutcomes {
            for &id in &pending.ids {
                match self.stage_of(id) {
                    Some(TransferStage::Removing) => self.removing_committed(&[id], fx),
                    Some(TransferStage::PersistingFailure) => self.failure_committed(id, fx),
                    _ => {}
                }
            }
            self.maybe_drained(fx);
            return;
        }
        let stage = pending.ids.first().and_then(|id| self.stage_of(*id));
        match stage {
            Some(TransferStage::Accepting) => self.accepting_committed(&pending.ids, fx),
            Some(TransferStage::PersistingCopy) => self.copy_committed(&pending.ids, fx),
            Some(TransferStage::PersistingAttempt) => self.attempt_committed(pending.ids[0], fx),
            Some(TransferStage::PersistingBatchIntent) => self.batch_intent_committed(&pending.ids, fx),
            Some(TransferStage::PersistingPart) => self.part_committed(pending.ids[0], fx),
            Some(TransferStage::PersistingComplete) => self.complete_committed(pending.ids[0], fx),
            Some(TransferStage::PersistingRetry) if self.transfers.get(&pending.ids[0]).is_some_and(|transfer| transfer.next_step == NextStep::Mutation && !transfer.intent_live) => self.retry_recreate_committed(pending.ids[0], fx),
            Some(TransferStage::PersistingRetry) if self.transfers.get(&pending.ids[0]).is_some_and(|transfer| transfer.next_step == NextStep::Mutation) => self.retry_mutation_committed(&pending.ids, fx),
            Some(TransferStage::PersistingRetry) => self.retry_committed(pending.ids[0], fx),
            Some(TransferStage::PersistingFailure) => {
                for &id in &pending.ids {
                    self.failure_committed(id, fx);
                }
            }
            Some(TransferStage::Removing) => self.removing_committed(&pending.ids, fx),
            _ => {}
        }
        self.maybe_drained(fx);
    }

    fn rejected(&mut self, rejected: crate::owners::contract::Rejected, fx: &mut Fx) {
        let Some(pending) = self.pending.remove(&rejected.batch) else { return };
        if matches!(pending.kind, StoreKind::UploadId | StoreKind::LateOrphan) {
            if let (Some((upload_id, key)), RejectReason::Storage(_)) = (pending.orphan, &rejected.reason) {
                self.orphans.insert(upload_id, key);
                self.send_orphans(false, fx);
            }
            return;
        }
        if pending.kind == StoreKind::MixedOutcomes && matches!(&rejected.reason, RejectReason::Storage(_)) {
            self.schedule_retry(pending.ids[0], fx);
            return;
        }
        let stage = pending.ids.first().and_then(|id| self.stage_of(*id));
        let interrupts = !matches!(rejected.reason, RejectReason::Storage(_));
        let persisting = matches!(stage, Some(TransferStage::PersistingCopy | TransferStage::PersistingAttempt | TransferStage::PersistingBatchIntent | TransferStage::PersistingRetry | TransferStage::PersistingPart | TransferStage::PersistingComplete | TransferStage::PersistingFailure | TransferStage::Removing));
        if let (RejectReason::StaleGeneration { models, generation }, true) = (&rejected.reason, persisting) {
            for model in models { self.generations.insert(model.clone(), *generation); }
            let basis = models.iter().map(|model| (model.clone(), *generation)).collect();
            return self.store_basis(&pending.ids, pending.kind, basis, fx);
        }
        if let (RejectReason::Invalid, true) = (&rejected.reason, persisting) {
            if stage == Some(TransferStage::PersistingBatchIntent) {
                for &id in &pending.ids { self.mark_failure(id, "Internal(Invalid)".into(), fx); }
                self.store(&pending.ids, StoreKind::Failed, fx);
                return;
            }
            self.invalid_write(&pending.ids, stage == Some(TransferStage::PersistingFailure), fx);
            self.start_in_order(fx);
            return;
        }
        match (stage, interrupts) {
            (Some(TransferStage::Accepting), _) => self.accepting_rejected(&pending.ids, rejected.reason, fx),
            (Some(TransferStage::Removing), _) => self.removing_rejected(&pending.ids, rejected.reason, fx),
            (Some(TransferStage::PersistingRetry), false) => self.retry_rejected(pending.ids[0], rejected.reason, fx),
            (Some(TransferStage::PersistingBatchIntent), false) => self.schedule_retry(pending.ids[0], fx),
            (Some(TransferStage::PersistingFailure), false) => self.failure_rejected(pending.ids[0], fx),
            (Some(TransferStage::PersistingCopy), false) => self.copy_rejected(&pending.ids, fx),
            (Some(TransferStage::PersistingAttempt | TransferStage::PersistingPart | TransferStage::PersistingComplete), false) => {
                self.persist_failed(pending.ids[0], rejected.reason, fx);
                self.start_in_order(fx);
            }
            (Some(_), true) => {
                for id in pending.ids {
                    self.interrupt(id, fx);
                }
            }
            _ => {}
        }
        self.maybe_drained(fx);
    }

    /// The prologue of `Online` and `PowerSaving`: the fields first, then the row.
    fn network_changed(&mut self, online: Option<bool>, saving: Option<bool>, fx: &mut Fx) {
        let was_online = self.online;
        if online.is_some() {
            self.online = online;
        }
        if let Some(saving) = saving {
            self.saving = saving;
        }
        self.leave_network(fx);
        if online == Some(true) && was_online != Some(true) {
            self.expire_failed(fx);
            self.send_orphans(true, fx);
        }
        self.start_in_order(fx);
    }

    fn handle(&mut self, input: TransferInput, fx: &mut Fx) {
        if let (Some(error), TransferInput::Accept { reply: id, .. } | TransferInput::AcceptBatch { reply: id, .. } | TransferInput::Cancel { reply: id, .. } | TransferInput::Discard { reply: id, .. } | TransferInput::Retry { reply: id, .. }) = (&self.queue_error, &input) {
            return reply(fx, *id, TransferAnswer::Err(Fault::Rejected(RejectReason::Storage(error.clone()))));
        }
        match input {
            TransferInput::Accept { epoch, item, reply, now } => self.accept(epoch, vec![*item], None, reply, now, fx),
            TransferInput::AcceptBatch { epoch, items, joint, reply, now } => self.accept(epoch, items, Some(*joint), reply, now, fx),
            TransferInput::Restored { .. } | TransferInput::RestoreFailed { .. } => {}
            TransferInput::AbortDone { upload_id, answered } => self.abort_done(upload_id, answered, fx),
            TransferInput::RegisterOperations { declarations } => self.register(declarations, fx),
            TransferInput::RestoreReady { epoch } => self.restore_ready(epoch, fx),
            TransferInput::RoundStatus { epoch, transfer_ids, refused } => self.round_status(epoch, &transfer_ids, refused, fx),
            TransferInput::CopyDone { transfer_id, attempt_id, size, sha256, place, cached } => self.copy_done(transfer_id, attempt_id, CopiedBlob { size, sha256, place, cached }, fx),
            TransferInput::CopyFailed { transfer_id, attempt_id, error } => self.copy_failed(transfer_id, attempt_id, error, fx),
            TransferInput::OpReserved { target, result } => self.op_reserved(target, result, fx),
            TransferInput::StepOk { transfer_id, attempt_id, part, upload_id, etag } => self.step_ok(transfer_id, attempt_id, part, upload_id, etag, fx),
            TransferInput::UploadMinted { transfer_id, upload_id, key, .. } => self.upload_minted(transfer_id, upload_id, key, fx),
            TransferInput::StepError { transfer_id, attempt_id, temporary, error } => self.step_error(transfer_id, attempt_id, (temporary, error), fx),
            TransferInput::Completed { transfer_id, attempt_id, signed_id } => self.completed(transfer_id, attempt_id, signed_id, fx),
            TransferInput::CompleteError { transfer_id, attempt_id, temporary, error } => self.complete_error(transfer_id, attempt_id, (temporary, error), fx),
            TransferInput::MutationOutcome { transfer_id, outcome } => self.mutation_outcome(transfer_id, outcome, fx),
            TransferInput::MutationOutcomes { batch_id, outcomes, intent_removed } => self.finish_batch(batch_id, outcomes, intent_removed, fx),
            TransferInput::RetryOutcome { cycles, result } => self.retry_mutation_outcome(&cycles, result, fx),
            TransferInput::DiscardOutcome { key, result } => self.discard_outcome(&key, result, fx),
            TransferInput::Outcome { receipt, reply } => self.outcome(receipt, reply, fx),
            TransferInput::InterestPlus { transfer_id, stream_id } => self.interest_plus(transfer_id, stream_id, fx),
            TransferInput::InterestMinus { transfer_id, stream_id } => self.interest_minus(transfer_id, stream_id),
            TransferInput::Committed(committed) => self.committed(committed.batch, fx),
            TransferInput::Rejected(rejected) => self.rejected(rejected, fx),
            TransferInput::Cancel { key, reply } => self.cancel(&key, reply, true, fx),
            TransferInput::Discard { key, reply } => self.cancel(&key, reply, false, fx),
            TransferInput::Retry { key, source, reply } => self.retry(&key, source, reply, fx),
            TransferInput::SlotFreed => self.start_in_order(fx),
            TransferInput::Online(Online(on)) => self.network_changed(Some(on), None, fx),
            TransferInput::PowerSaving(saving) => self.network_changed(None, Some(saving), fx),
            TransferInput::EpochStarted(started) => { self.epoch = started.epoch; self.base_generation = started.generation; },
            TransferInput::ModelsReset(reset) if reset.epoch == self.epoch => {
                for model in reset.models { self.generations.insert(model, reset.generation); }
            }
            TransferInput::ModelsReset(_) => {}
            TransferInput::Tick { now, kind } => self.tick(now, kind, fx),
            TransferInput::EpochEnded(_) => self.epoch_ended(fx),
            TransferInput::Closing => self.close(fx),
            TransferInput::Clock(now) => self.now = self.now.max(now),
        }
    }

    /// `Restoring`: the calls on the keys of the base wait for `Restored`, the rest of the table works as in the other phases.
    fn restoring(&mut self, input: TransferInput, fx: &mut Fx) {
        match input {
            TransferInput::Restored { uploads, outcomes, orphans, batches } if !self.ended => {
                self.orphans.extend(orphans);
                self.restore_groups(batches);
                self.restored(uploads, outcomes, fx)
            }
            TransferInput::RestoreFailed { error } if !self.ended => {
                self.queue_error = Some(error);
                self.phase = Phase::Classified;
                run(fx, TransferEffect::RestoreClassified { epoch: self.epoch });
                self.replay_held(fx);
            }
            held @ (TransferInput::Accept { .. } | TransferInput::AcceptBatch { .. }) if Self::in_epoch(&held, self.epoch) => self.held.push(held),
            held @ (TransferInput::Cancel { .. } | TransferInput::Retry { .. } | TransferInput::Discard { .. } | TransferInput::Outcome { .. }) => self.held.push(held),
            TransferInput::EpochStarted(started) => {
                self.epoch = started.epoch;
                self.base_generation = started.generation;
                run(fx, TransferEffect::Started { epoch: started.epoch });
                run(fx, TransferEffect::SnapshotRequest { epoch: started.epoch });
            }
            TransferInput::Closing => {
                self.answer_held(fx);
                self.close(fx);
            }
            TransferInput::EpochEnded(_) => {
                self.ended = true;
                self.answer_held(fx);
                self.epoch_ended(fx);
            }
            other => self.handle(other, fx),
        }
    }

    /// A new epoch starts a new instance of the table: the declarations of the operations and the counters of ids stay, the rest of the state of the older epoch goes.
    fn begin_epoch(&mut self, epoch: Epoch) {
        let fresh = Self::new(epoch, self.slots, self.peers);
        let kept = Self { declarations: std::mem::take(&mut self.declarations), next_id: self.next_id, next_seq: self.next_seq, next_group: self.next_group, next_batch: self.next_batch, next_attempt: self.next_attempt, next_timer: self.next_timer, put_timeout: self.put_timeout, now: self.now, ..fresh };
        *self = kept;
    }

    fn in_epoch(input: &TransferInput, epoch: Epoch) -> bool { matches!(input, TransferInput::Accept { epoch: found, .. } | TransferInput::AcceptBatch { epoch: found, .. } if *found == epoch) }

    pub(super) fn replay_held(&mut self, fx: &mut Fx) {
        for input in std::mem::take(&mut self.held) {
            self.handle(input, fx);
        }
    }

    /// Every held call gets exactly one answer.
    pub(super) fn answer_held(&mut self, fx: &mut Fx) {
        for input in std::mem::take(&mut self.held) {
            match input {
                TransferInput::Accept { reply: id, .. } | TransferInput::AcceptBatch { reply: id, .. } | TransferInput::Cancel { reply: id, .. } | TransferInput::Retry { reply: id, .. } | TransferInput::Discard { reply: id, .. } => reply(fx, id, TransferAnswer::Err(Fault::Interrupted)),
                TransferInput::Outcome { reply: id, .. } => reply(fx, id, TransferAnswer::Outcome(OutboxOutcome::Interrupted)),
                _ => {}
            }
        }
    }

    /// The final mutation goes to Outbox as an operation of the origin `Transfer{key}`.
    pub(super) fn submit_mutation(&mut self, id: TransferId, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        transfer.submitted = true;
        let signed = transfer.signed_id.clone().unwrap_or_default();
        let mut op = NewOp { origin: OpSource::Transfer { transfer_id: transfer.transfer_id, cycle_id: transfer.cycle_id }, body: transfer.mutation.body.replace(SIGNED_ID_SENTINEL, &signed), ..transfer.mutation.clone() };
        let input = match transfer.op_id {
            Some(op_id) => {
                op.effects = Default::default();
                OutboxInput::SubmitReserved { epoch: self.epoch, op_id, op }
            }
            None => OutboxInput::Submit { epoch: self.epoch, op, reply: NO_REPLY },
        };
        fx.push(Effect::Send(Outbound::new(self.peers.outbox, input)));
    }
}

pub struct Transfers;

impl StateOwner for Transfers {
    const EPOCHED: bool = true;

    type State = TransferState;
    type Input = TransferInput;
    type Effect = TransferEffect;

    fn step(state: &mut TransferState, input: TransferInput) -> Effects<Self> {
        let mut fx: Fx = Vec::new();
        if let TransferInput::EpochStarted(started) = &input {
            state.begin_epoch(started.epoch);
        }
        if state.phase == Phase::Restoring {
            state.restoring(input, &mut fx);
        } else {
            state.handle(input, &mut fx);
        }
        fx
    }

    fn tick(now: Time) -> TransferInput { TransferInput::Clock(now) }

    fn epoch_ended(epoch: Epoch, cause: Cause) -> TransferInput { TransferInput::EpochEnded(EpochEnded { epoch, cause }) }

    fn closing() -> TransferInput { TransferInput::Closing }
}

pub(super) fn upload_kind(kind: StoreKind) -> UploadKind {
    match kind {
        StoreKind::SyncUpload | StoreKind::UploadId | StoreKind::LateOrphan | StoreKind::Removed | StoreKind::BatchIntent | StoreKind::MixedOutcomes => UploadKind::SyncUpload,
        StoreKind::ConfirmedParts => UploadKind::ConfirmedParts,
        StoreKind::Completed => UploadKind::Completed,
        StoreKind::Failed => UploadKind::Failed,
        StoreKind::Retrying => UploadKind::Retrying,
    }
}
