//! Owner `Transfer`: the acceptance of a file (`Accept`, `AcceptBatch`), the copy of the source and the common write of the acceptance.

use std::collections::{BTreeMap, BTreeSet};

use super::outbox::{Fault, OutboxInput, ReserveTarget};
use super::transfer::{reply, run, Fx, TransferState};
use super::transfer_types::*;
use crate::owners::contract::{RejectReason, ReplyId};
use crate::owners::store::StoreOp;
use crate::runtime::{Effect, Epoch, Outbound, RetryPolicy, Time};

impl TransferState {
    pub(super) fn supply_source(&mut self, epoch: Epoch, key: &str, source: SuppliedSource, rep: ReplyId, fx: &mut Fx) {
        if epoch != self.epoch { return reply(fx, rep, TransferAnswer::Err(Fault::NoSession)); }
        let Some(id) = self.open_id(key) else { return reply(fx, rep, TransferAnswer::Err(Fault::NotFound)) };
        match self.stage_of(id) {
            Some(TransferStage::AwaitingSource) => {},
            Some(TransferStage::Copying | TransferStage::PersistingCopy) if self.transfers[&id].supply_back.is_some() => return reply(fx, rep, TransferAnswer::Err(Fault::InFlight)),
            _ => return reply(fx, rep, TransferAnswer::Err(Fault::Invalid)),
        }
        if source_invalid(&source) { return reply(fx, rep, TransferAnswer::Err(Fault::Invalid)); }
        self.start_source(id, source, TransferStage::AwaitingSource, rep, fx);
    }

    pub(super) fn start_source(&mut self, id: TransferId, source: SuppliedSource, back: TransferStage, rep: ReplyId, fx: &mut Fx) {
        let Some(body) = self.transfers.get(&id).and_then(|transfer| supplied_body(&transfer.mutation.body, &source)) else { return reply(fx, rep, TransferAnswer::Err(Fault::Invalid)) };
        let attempt_id = self.attempt();
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        let (file, staged) = (source.source.clone(), transfer.previous.is_some());
        transfer.source = Some(file.clone());
        transfer.prepare.clone_from(&source.prepare);
        transfer.steps = Some(source.steps.clone());
        transfer.supply_values = source.values.clone();
        transfer.supply_local = source.local.clone();
        transfer.new_source = Some(ActiveSource { request: source, body });
        transfer.supply_back = Some(back);
        transfer.supply_reply = Some(rep);
        transfer.stage = TransferStage::Copying;
        transfer.attempt_id = attempt_id;
        let effect = TransferEffect::Copy { transfer_id: id, attempt_id, key: transfer.key.clone(), source: file, staged };
        run(fx, effect);
        self.rounds_needed(vec![id], fx);
    }

    pub(super) fn source_failed(&mut self, now: Time, epoch: Epoch, key: &str, message: String, rep: ReplyId, fx: &mut Fx) {
        if epoch != self.epoch { return reply(fx, rep, TransferAnswer::Err(Fault::NoSession)); }
        let Some(id) = self.open_id(key) else { return reply(fx, rep, TransferAnswer::Err(Fault::NotFound)) };
        if self.stage_of(id) != Some(TransferStage::AwaitingSource) {
            let busy = self.transfers.get(&id).is_some_and(|transfer| matches!(transfer.stage, TransferStage::Copying | TransferStage::PersistingCopy) && transfer.supply_back.is_some());
            return reply(fx, rep, TransferAnswer::Err(if busy { Fault::InFlight } else { Fault::Invalid }));
        }
        if let Some(transfer) = self.transfers.get_mut(&id) { transfer.supply_reply = Some(rep); }
        self.begin_failure(now, id, format!("source_failed:{message}"), false, fx);
    }

    pub(super) fn create(&mut self, item: BatchItem, batch_id: Option<u64>, created_at: Time, stage: TransferStage) -> TransferId {
        let (transfer_id, seq) = (self.next_id, self.next_seq);
        self.next_id += 1;
        self.next_seq += 1;
        let BatchItem { key, held, fields, urgent, source, prepare, mutation, steps } = item;
        let transfer = Transfer {
            held,
            commit_reply: None,
            commit: None,
            prep: None,
            transfer_id,
            key,
            seq,
            source,
            prepare,
            batch_id,
            epoch: self.epoch,
            stage,
            attempt_id: 0,
            attempts: 0,
            storage_refusals: 0,
            in_flight: false,
            waiting_retry: false,
            next_step: NextStep::Part(1),
            upload_id: None,
            confirmed_parts: 0,
            inflight: 0,
            floor: 0,
            etags: Vec::new(),
            last_part: false,
            aborted: false,
            signed_id: None,
            created_at,
            error: None,
            cycle_id: 1,
            revision: 0,
            place: None,
            size: 0,
            sha256: String::new(),
            mutation,
            fields,
            steps,
            restored: false,
            restore_element: None,
            pending_step: None,
            answer_saved: None,
            staged_failed: None,
            urgent,
            op_id: None,
            submitted: false,
            intent_live: false,
            has_operation: false,
            waits_outbox: false,
            from_cache: false,
            cache_stale: false,
            round_error: None,
            stored_shown: None,
            last_shown: None,
            removal: None,
            prior: None,
            failure_abort: false,
            outcomes: Default::default(),
            outcome_waiters: Vec::new(),
            accept_reply: None,
            optimistic: None,
            cancel_reply: None,
            retry_reply: None,
            supply_reply: None,
            supply_back: None,
            supply_values: Default::default(),
            supply_local: String::new(),
            discard_waiters: Vec::new(),
            discard_sent: false,
            new_source: None,
            previous: None,
            failed_at: None,
            blob_created_at: None,
            seeded: None,
            closing: false,
        };
        self.transfers.insert(transfer_id, transfer);
        transfer_id
    }

    fn valid(item: &BatchItem) -> bool {
        match (&item.source, &item.steps) {
            (None, None) => true,
            (Some(source), Some(_)) => !source.path.is_empty() && source.parts > 0,
            _ => false,
        }
    }

    /// `Accept` and `AcceptBatch`: every element is checked before the transaction, the rows of all N elements go in one batch.
    pub(super) fn accept(&mut self, now: Time, epoch: Epoch, items: Vec<BatchItem>, joint: Option<Joint>, rep: ReplyId, fx: &mut Fx) {
        if epoch != self.epoch || self.closing || self.ended {
            return reply(fx, rep, TransferAnswer::Err(if self.closing { Fault::Ended(crate::ffi::EndCause::Closed) } else { Fault::NoSession }));
        }
        let held_wrong = items.iter().any(|item| item.held && (item.source.is_none() || joint.is_some()));
        if items.is_empty() || held_wrong || !items.iter().all(Self::valid) || joint.as_ref().is_some_and(|joint| !joint.admits(items.len())) {
            return reply(fx, rep, TransferAnswer::Err(Fault::Invalid));
        }
        let group = joint.as_ref().map(|_| {
            self.next_group += 1;
            self.next_group - 1
        });
        let mut keys = BTreeSet::new();
        if items.iter().any(|item| !keys.insert(&item.key)) { return reply(fx, rep, TransferAnswer::Err(Fault::InFlight)); }
        let old: Vec<_> = items.iter().map(|item| self.open_id(&item.key)).collect();
        if old.iter().flatten().any(|id| self.transfers.get(id).is_none_or(|transfer| transfer.stage != TransferStage::Failed || transfer.batch_id.is_some())) {
            return reply(fx, rep, TransferAnswer::Err(Fault::InFlight));
        }
        let ids: Vec<TransferId> = items.into_iter().map(|item| {
            self.create(item, group, now, TransferStage::Reserving)
        }).collect();
        for (&id, old) in ids.iter().zip(old) {
            if let Some(old) = old {
                self.replacements.insert(id, (old, None));
                self.remove_with(old, Removal::Discarded, fx);
                if let Some(transfer) = self.transfers.get_mut(&old) { transfer.prior = Some(TransferStage::Failed); }
            }
        }
        match (group, joint) {
            (Some(group), Some(joint)) => {
                self.groups.insert(group, Group { ids: ids.clone(), op_id: None, reply: Some(rep), ready: BTreeSet::new(), joint, pending_op: None, phase: BatchPhase::Collecting, op_row: None });
            }
            _ => {
                if let Some(transfer) = self.transfers.get_mut(&ids[0]) {
                    transfer.accept_reply = Some(rep);
                }
            }
        }
        for &id in &ids {
            let target = ReserveTarget::Transfer(id);
            let input = match self.replacements.get(&id).and_then(|(old, _)| self.transfers.get(old)) {
                Some(transfer) => OutboxInput::ReserveReplacement { epoch: self.epoch, target, transfer_id: transfer.transfer_id, cycle_id: transfer.cycle_id },
                None => OutboxInput::ReserveOp { epoch: self.epoch, target },
            };
            fx.push(Effect::Send(Outbound::new(self.peers.outbox, input)));
        }
        if let Some(group) = group {
            fx.push(Effect::Send(Outbound::new(self.peers.outbox, OutboxInput::ReserveOp { epoch: self.epoch, target: ReserveTarget::Group(group) })));
        }
    }

    /// `Reserving x OpReserved`: the batch of the acceptance writes the optimistic effects under the reserved `op_id`s. An answer of a transfer that is not `Reserving` is foreign.
    pub(super) fn op_reserved(&mut self, target: ReserveTarget, result: Result<u64, Fault>, fx: &mut Fx) {
        let group = match target {
            ReserveTarget::Transfer(id) if self.stage_of(id) == Some(TransferStage::Reserving) && self.transfers.get(&id).is_some_and(|transfer| transfer.op_id.is_none()) => self.group_of(id),
            ReserveTarget::Group(id) if self.groups.get(&id).is_some_and(|group| group.op_id.is_none() && group.ids.iter().any(|member| self.stage_of(*member) == Some(TransferStage::Reserving))) => Some(id),
            _ => return,
        };
        let ids = group.and_then(|group| self.groups.get(&group).map(|group| group.ids.clone())).unwrap_or_else(|| match target { ReserveTarget::Transfer(id) => vec![id], ReserveTarget::Group(_) => Vec::new() });
        match result {
            Ok(op_id) => {
                match target {
                    ReserveTarget::Transfer(id) => if let Some(transfer) = self.transfers.get_mut(&id) { transfer.op_id = Some(op_id); },
                    ReserveTarget::Group(id) => if let Some(group) = self.groups.get_mut(&id) { group.op_id = Some(op_id); },
                }
                let group_ready = group.is_none_or(|group| self.groups.get(&group).is_some_and(|group| group.op_id.is_some()));
                if group_ready && ids.iter().all(|member| self.transfers.get(member).is_some_and(|transfer| transfer.op_id.is_some())) {
                    for member in &ids {
                        self.set(*member, TransferStage::Accepting);
                    }
                    self.store(&ids, StoreKind::SyncUpload, fx);
                }
            }
            Err(fault) => {
                let group_reply = group.and_then(|group| self.groups.get_mut(&group).and_then(|group| group.reply.take()));
                let own = ids.first().and_then(|id| self.transfers.get_mut(id)).and_then(|transfer| transfer.accept_reply.take());
                for member in ids {
                    self.restore_replaced(member, fx);
                    self.set(member, TransferStage::Done);
                }
                if let Some(rep) = own.or(group_reply) {
                    reply(fx, rep, TransferAnswer::Err(fault));
                }
            }
        }
    }

    /// `Accepting x Committed`: the copy starts, the round is asked for.
    pub(super) fn accepting_committed(&mut self, ids: &[TransferId], fx: &mut Fx) {
        for &id in ids {
            if let Some(transfer) = self.transfers.get_mut(&id) { transfer.has_operation = !transfer.mutation.effects.is_empty(); }
            if self.transfers.get(&id).is_some_and(|transfer| transfer.closing) {
                self.interrupt(id, crate::ffi::EndCause::Closed, fx);
                continue;
            }
            if self.transfers.get(&id).is_some_and(|transfer| transfer.source.is_none()) {
                self.set(id, TransferStage::AwaitingSource);
                self.show(id, Shown::WaitingSource, fx);
                if let Some(group) = self.group_of(id) {
                    if let Some(entry) = self.groups.get_mut(&group) { entry.ready.insert(id); }
                }
                continue;
            }
            let attempt_id = self.attempt();
            let Some(transfer) = self.transfers.get_mut(&id) else { continue };
            transfer.stage = TransferStage::Copying;
            transfer.attempt_id = attempt_id;
            let Some(source) = transfer.source.clone() else { continue };
            let effect = TransferEffect::Copy { transfer_id: id, attempt_id, key: transfer.key.clone(), source, staged: false };
            self.show(id, Shown::Copying, fx);
            run(fx, effect);
        }
        let live: Vec<TransferId> = ids.iter().copied().filter(|id| self.stage_of(*id) == Some(TransferStage::Copying)).collect();
        if live.len() != ids.len() { self.schedule_expire(fx); }
        if live.is_empty() {
            let receipts: Vec<_> = ids.iter().map(|id| self.receipt(*id)).collect();
            if let Some(group) = ids.first().and_then(|id| self.group_of(*id)) {
                if let Some(rep) = self.groups.get_mut(&group).and_then(|group| group.reply.take()) { reply(fx, rep, TransferAnswer::Receipts(receipts)); }
            } else if let Some(id) = ids.first() {
                if let Some(rep) = self.transfers.get_mut(id).and_then(|transfer| transfer.accept_reply.take()) { reply(fx, rep, TransferAnswer::Receipt(receipts[0].clone())); }
            }
        } else {
            self.rounds_needed(live, fx);
        }
    }

    /// `Accepting x Rejected`: no receipt, nothing is accepted.
    pub(super) fn accepting_rejected(&mut self, ids: &[TransferId], reason: RejectReason, fx: &mut Fx) {
        let fault = if self.closing { Fault::Ended(crate::ffi::EndCause::Closed) } else { Fault::of_reject(reason) };
        for &id in ids {
            let Some(transfer) = self.transfers.get_mut(&id) else { continue };
            let group_reply = transfer.batch_id.and_then(|group| self.groups.get_mut(&group).and_then(|group| group.reply.take()));
            if let Some(rep) = transfer.accept_reply.take().or(group_reply) {
                reply(fx, rep, TransferAnswer::Err(fault.clone()));
            }
            transfer.stage = TransferStage::Done;
        }
    }

    fn group_of(&self, id: TransferId) -> Option<u64> { self.transfers.get(&id).and_then(|transfer| transfer.batch_id) }

    pub(super) fn copy_done(&mut self, now: Time, id: TransferId, attempt_id: AttemptId, copy: CopiedBlob, fx: &mut Fx) {
        if self.stage_of(id) == Some(TransferStage::Measuring) { return self.measured(now, id, attempt_id, copy, fx); }
        let CopiedBlob { size, sha256, place, cached } = copy;
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        if transfer.stage != TransferStage::Copying || transfer.attempt_id != attempt_id {
            return;
        }
        let cached = if transfer.prepare.is_some() || transfer.held { CachedBlob::Miss } else { cached };
        transfer.size = size;
        transfer.sha256 = sha256;
        transfer.place = Some(place);
        transfer.cache_stale = cached == CachedBlob::Stale;
        if let CachedBlob::Fresh { blob_id } = cached {
            transfer.from_cache = true;
            transfer.signed_id = Some(blob_id);
        }
        if transfer.supply_back.is_some() {
            transfer.stage = TransferStage::PersistingCopy;
            return self.store(&[id], StoreKind::SyncUpload, fx);
        }
        let Some(group) = self.group_of(id) else {
            self.set(id, TransferStage::PersistingCopy);
            return self.store(&[id], StoreKind::SyncUpload, fx);
        };
        let Some(entry) = self.groups.get_mut(&group) else { return };
        entry.ready.insert(id);
        if entry.ready.len() < entry.ids.len() {
            return self.set(id, TransferStage::CopyReady);
        }
        let group_ids = entry.ids.clone();
        let ids: Vec<_> = group_ids.into_iter().filter(|member| self.stage_of(*member) == Some(TransferStage::CopyReady) || *member == id).collect();
        for &member in &ids {
            self.set(member, TransferStage::PersistingCopy);
        }
        self.store(&ids, StoreKind::SyncUpload, fx);
    }

    /// `CopyFailed` of the current attempt: a call that waits gets the error and the metadata goes, a restored transfer is `Failed`.
    pub(super) fn copy_failed(&mut self, now: Time, id: TransferId, attempt_id: AttemptId, error: String, fx: &mut Fx) {
        if self.stage_of(id) == Some(TransferStage::Measuring) { return self.measure_failed(now, id, attempt_id, error, fx); }
        let Some(transfer) = self.transfers.get(&id) else { return };
        if transfer.stage != TransferStage::Copying || transfer.attempt_id != attempt_id {
            return;
        }
        if transfer.supply_back.is_some() {
            return self.source_rejected(id, Fault::Copy(error), fx);
        }
        let ids = match self.group_of(id) {
            Some(group) => self.groups.get(&group).map(|group| group.ids.clone()).unwrap_or_default(),
            None => vec![id],
        };
        let waiting = ids.len() > 1 || self.transfers[&id].accept_reply.is_some();
        if !waiting {
            self.set(id, TransferStage::PersistingFailure);
            if let Some(transfer) = self.transfers.get_mut(&id) {
                transfer.error = Some(error);
                transfer.failure_abort = false;
            }
            return self.store(&[id], StoreKind::Failed, fx);
        }
        for &member in &ids {
            if member != id && self.stage_of(member) == Some(TransferStage::Copying) {
                run(fx, TransferEffect::CopyCancel { transfer_id: member });
            }
            self.remove_with(member, Removal::CopyFailed { error: error.clone() }, fx);
        }
        self.store(&ids, StoreKind::Removed, fx);
    }

    /// The transfer waits in `Removing` for the batch of its removal.
    pub(super) fn remove_with(&mut self, id: TransferId, removal: Removal, fx: &mut Fx) {
        self.release_step(id, fx);
        if let Some(transfer) = self.transfers.get_mut(&id) {
            transfer.stage = TransferStage::Removing;
            transfer.removal = Some(removal);
        }
    }

    /// The temporary file took the place of the copy: the marker of the row goes, a call that waits for the swap gets its answer.
    pub(super) fn staged_promoted(&mut self, now: Time, id: TransferId, fx: &mut Fx) {
        let Some((key, stage)) = self.transfers.get(&id).map(|transfer| (transfer.key.clone(), transfer.stage)) else { return };
        if let Some(previous) = self.transfers.get_mut(&id).and_then(|transfer| transfer.previous.take()) {
            if let Some(upload_id) = previous.upload_id {
                self.orphans.insert(upload_id, previous.key);
                self.send_orphans(false, fx);
            }
        }
        let batch = self.send_batch(vec![StoreOp::UploadStaged { key }], fx);
        self.pending.insert(batch, Pending { ids: vec![id], kind: StoreKind::StagedCleared, orphan: None, written: BTreeMap::new() });
        match stage {
            TransferStage::PersistingCopy => self.copy_committed(now, &[id], fx),
            TransferStage::PersistingPrepared => self.prepared_ready(now, id, fx),
            _ => {}
        }
    }

    /// The rename of the temporary file failed: the old copy and its metadata stay, only the temporary file goes, the call that waits for the swap learns it.
    pub(super) fn staged_promote_failed(&mut self, now: Time, id: TransferId, detail: String, fx: &mut Fx) {
        if self.stage_of(id) == Some(TransferStage::PersistingPrepared) { return self.fail_prepare(now, id, crate::value::sync::PrepareReason::Failed, detail, fx); }
        let fault = Fault::Rejected(RejectReason::Storage(detail));
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        let (Some(_), Some(previous)) = (transfer.supply_back.take(), transfer.previous.take()) else { return };
        let rep = transfer.supply_reply.take();
        *transfer = *previous;
        transfer.staged_failed = Some((rep, fault));
        self.write_restored(id, fx);
    }

    /// The batch of the old row after a failed swap: the call that waited for the swap and the temporary file wait for its `Committed`.
    pub(super) fn write_restored(&mut self, id: TransferId, fx: &mut Fx) {
        self.store(&[id], StoreKind::Failed, fx);
        if let Some(pending) = self.pending.get_mut(&(self.next_batch - 1)) { pending.kind = StoreKind::StagedRestored; }
    }

    /// `StagedRestored x Rejected`: nothing is answered before the old row is durable, a refusal of the Store goes again after the backoff.
    pub(super) fn staged_restore_rejected(&mut self, id: TransferId, reason: RejectReason, fx: &mut Fx) {
        if !self.transfers.get(&id).is_some_and(|transfer| transfer.staged_failed.is_some()) { return; }
        match reason {
            RejectReason::Storage(_) => self.backoff_restore(id, fx),
            RejectReason::Invalid(_) => self.staged_restored(id, fx),
            RejectReason::StaleGeneration { generations, .. } => {
                self.generations.extend(generations);
                self.write_restored(id, fx);
            }
            RejectReason::Closed | RejectReason::NoSession => self.interrupt(id, super::outbox_types::end_cause(&reason), fx),
        }
    }

    fn backoff_restore(&mut self, id: TransferId, fx: &mut Fx) {
        let Some(attempt) = self.storage_refused(id) else { return };
        let timer = self.timer();
        fx.push(Effect::Backoff { id: timer, policy: RetryPolicy::Upload, attempt, input: TransferInput::Tick { kind: TransferTick::StagedRestore { transfer_id: id } } });
    }

    /// The old row is committed after a failed swap: the temporary file goes, the call that waited for the swap learns the fault.
    pub(super) fn staged_restored(&mut self, id: TransferId, fx: &mut Fx) {
        let Some((rep, fault)) = self.transfers.get_mut(&id).and_then(|transfer| transfer.staged_failed.take()) else { return };
        run(fx, TransferEffect::DeleteFile { path: staged_file_id(id) });
        self.rounds_idle(id, fx);
        if let Some(rep) = rep { reply(fx, rep, TransferAnswer::Err(fault)); }
    }

    /// The file of the transfer is ready: a file the server holds already goes to the mutation, any other waits for its first part.
    pub(super) fn enter_upload(&mut self, id: TransferId, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        let (cached, batch) = (transfer.from_cache, transfer.batch_id);
        if cached && batch.is_none() { transfer.has_operation = true; }
        transfer.stage = if cached && batch.is_some() { TransferStage::BatchReady } else if cached { TransferStage::Mutating } else { TransferStage::Ready };
        transfer.intent_live = cached && batch.is_none();
        if cached && batch.is_some() {
            self.show(id, Shown::Uploading { progress: 100 }, fx);
            if let Some(group) = batch { self.batch_release(group, fx); }
        } else if cached {
            self.show(id, Shown::Uploading { progress: 100 }, fx);
            self.submit_mutation(id, fx);
        } else {
            self.show_queued(id, fx);
        }
    }

    /// `PersistingCopy x Committed`: the receipt, the transfer is `Ready`.
    pub(super) fn copy_committed(&mut self, now: Time, ids: &[TransferId], fx: &mut Fx) {
        let swapping: Vec<TransferId> = ids.iter().copied().filter(|id| self.transfers.get(id).is_some_and(|transfer| transfer.previous.is_some())).collect();
        for &id in &swapping {
            run(fx, TransferEffect::PromoteStaged { transfer_id: id });
        }
        let ids: Vec<TransferId> = ids.iter().copied().filter(|id| !swapping.contains(id)).collect();
        if ids.is_empty() { return; }
        let ids = &ids[..];
        let mut receipts = Vec::new();
        let mut accepted = Vec::new();
        for &id in ids {
            let Some(transfer) = self.transfers.get_mut(&id) else { continue };
            let supplied = transfer.supply_back.take();
            if supplied.is_some() { transfer.has_operation = true; }
            if supplied == Some(TransferStage::Failed) { transfer.cycle_id += 1; }
            if supplied.is_none() { accepted.push(id); }
            let supply_reply = transfer.supply_reply.take();
            if let Some(source) = transfer.new_source.take() {
                transfer.fields.extend(source.request.variables);
                transfer.mutation.body = source.body;
            }
            transfer.supply_values = Default::default();
            transfer.supply_local = String::new();
            transfer.next_step = NextStep::Part(1);
            transfer.confirmed_parts = 0;
            transfer.floor = 0;
            transfer.upload_id = None;
            transfer.blob_created_at = None;
            transfer.error = None;
            transfer.failed_at = None;
            transfer.etags.clear();
            let preparing = transfer.prepare.is_some();
            receipts.push(self.receipt(id));
            if let (Some(back), Some(rep)) = (supplied, supply_reply) {
                reply(fx, rep, if back == TransferStage::Failed { TransferAnswer::Receipt(self.receipt(id)) } else { TransferAnswer::Ok });
            }
            if preparing { self.queue_prepare(id, fx); } else { self.enter_upload(id, fx); }
        }
        for (position, &id) in ids.iter().enumerate() {
            let Some(transfer) = self.transfers.get_mut(&id) else { continue };
            if let Some(rep) = transfer.accept_reply.take() {
                reply(fx, rep, TransferAnswer::Receipt(receipts[position].clone()));
            }
        }
        if let Some(group) = ids.first().and_then(|id| self.group_of(*id)) {
            let group_ids = self.groups.get(&group).map(|entry| entry.ids.clone()).unwrap_or_default();
            if let Some(rep) = self.groups.get_mut(&group).and_then(|group| group.reply.take()) {
                reply(fx, rep, TransferAnswer::Receipts(group_ids.iter().map(|id| self.receipt(*id)).collect()));
            }
        }
        self.rounds_needed(accepted, fx);
        self.start_in_order(now, fx);
    }

    /// `PersistingCopy x Rejected(Storage)`: the copy is not durable, the whole batch is removed.
    pub(super) fn copy_rejected(&mut self, ids: &[TransferId], reason: RejectReason, fx: &mut Fx) {
        let mut removing = Vec::new();
        for &id in ids {
            if self.transfers.get(&id).is_some_and(|transfer| transfer.supply_back.is_some()) {
                self.source_rejected(id, Fault::of_reject(reason.clone()), fx);
                continue;
            }
            self.remove_with(id, Removal::Storage, fx);
            let accept = self.transfers.get_mut(&id).and_then(|transfer| transfer.accept_reply.take());
            let group = self.group_entry(id).and_then(|group| self.groups.get_mut(&group).and_then(|group| group.reply.take()));
            if let Some(rep) = accept.or(group) { reply(fx, rep, TransferAnswer::Err(Fault::of_reject(reason.clone()))); }
            removing.push(id);
        }
        if !removing.is_empty() { self.store(&removing, StoreKind::Removed, fx); }
    }

    pub(super) fn source_rejected(&mut self, id: TransferId, fault: Fault, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        let Some(back) = transfer.supply_back.take() else { return };
        if let Some(previous) = transfer.previous.take() {
            let rep = transfer.supply_reply.take();
            *transfer = *previous;
            run(fx, TransferEffect::DeleteFile { path: staged_file_id(id) });
            self.rounds_idle(id, fx);
            if let Some(rep) = rep { reply(fx, rep, TransferAnswer::Err(fault)); }
            return;
        }
        transfer.stage = back;
        transfer.source = None;
        transfer.steps = None;
        transfer.new_source = None;
        transfer.supply_values = Default::default();
        transfer.supply_local = String::new();
        let rep = transfer.supply_reply.take();
        self.delete_copy(id, fx);
        self.rounds_idle(id, fx);
        if let Some(rep) = rep { reply(fx, rep, TransferAnswer::Err(fault)); }
    }
}

/// A source without a path or without parts cannot be uploaded.
pub(super) fn source_invalid(source: &SuppliedSource) -> bool { source.source.path.is_empty() || source.source.parts == 0 }
