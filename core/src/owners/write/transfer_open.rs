//! Owner `Transfer`: the opening: `Restored`, `RegisterOperations`, `RestoreReady` and the class of every restored row.

use std::collections::{BTreeMap, BTreeSet};

use super::outbox::OutboxOutcome;
use super::outbox_types::Phase;
use super::transfer::{run, Fx, TransferState};
use super::transfer_types::*;

impl TransferState {
    fn insert_restored(&mut self, row: &StoredTransfer, stage: TransferStage) -> TransferId {
        if let Some(transfer) = self.transfers.get_mut(&row.transfer_id) {
            transfer.stage = stage;
            return row.transfer_id;
        }
        let item = BatchItem { key: row.key.clone(), held: row.held, fields: row.fields.clone(), urgent: false, source: row.source.clone(), prepare: row.prepare.clone(), mutation: row.mutation.clone(), steps: row.steps.clone() };
        let id = row.transfer_id;
        self.next_id = self.next_id.max(id + 1);
        let created = self.create(item, row.batch.filter(|group| self.groups.contains_key(group)), row.created_at, stage);
        if let Some(mut transfer) = self.transfers.remove(&created) {
            transfer.transfer_id = id;
            transfer.seq = row.seq;
            transfer.restored = true;
            transfer.restore_element = row.restore_element.clone();
            transfer.failed_at = row.failed_at;
            transfer.blob_created_at = row.blob_created_at;
            transfer.stored_shown = Some((u8::MAX, 0));
            transfer.pending_step = row.pending_step.filter(|pending| row.saved_answer.as_ref().is_none_or(|(request_id, _)| request_id != pending));
            if let Some((request_id, answer)) = &row.saved_answer {
                transfer.seeded = Some((*request_id, crate::owners::session::session_answer(answer.clone()).map(|body| (body, crate::transport::AnswerKind::Answered { sequence: None }))));
                transfer.answer_saved = Some(*request_id);
            }
            transfer.upload_id = row.upload_id.clone();
            transfer.confirmed_parts = row.confirmed_parts;
            transfer.floor = if row.prepare.is_some() { 0 } else { row.floor };
            transfer.attempts = row.attempts;
            transfer.etags = row.etags.clone();
            transfer.signed_id = row.signed_id.clone();
            transfer.aborted = row.aborted;
            transfer.op_id = row.op_id;
            transfer.submitted = row.completed && (row.has_intent || !row.has_operation) && !row.batch.and_then(|group| self.groups.get(&group)).is_some_and(|group| group.phase == BatchPhase::Mutating);
            transfer.intent_live = row.has_intent;
            transfer.has_operation = row.has_operation;
            transfer.place = Some(row.place);
            transfer.size = row.size;
            transfer.sha256 = row.sha256.clone();
            transfer.cycle_id = row.cycle_id.max(1);
            transfer.next_step = if row.source.as_ref().is_some_and(|source| row.confirmed_parts >= source.parts) { NextStep::Complete } else { NextStep::Part(row.confirmed_parts + 1) };
            let group = transfer.batch_id;
            self.transfers.insert(id, transfer);
            if let Some(group) = group {
                self.rejoin(id, group);
            }
        }
        self.next_seq = self.next_seq.max(row.seq + 1);
        id
    }

    /// The element is in the group once, in the order of `seq`.
    fn rejoin(&mut self, id: TransferId, group: u64) {
        if let Some(entry) = self.groups.get_mut(&group) {
            entry.ids.retain(|found| *found != id);
            let seq = self.transfers.get(&id).map(|transfer| transfer.seq).unwrap_or(u64::MAX);
            let at = entry.ids.partition_point(|found| self.transfers.get(found).is_some_and(|transfer| transfer.seq <= seq));
            entry.ids.insert(at, id);
        }
    }

    pub(super) fn restore_groups(&mut self, batches: BTreeMap<u64, StoredGroup>) {
        for (group, stored) in batches {
            self.next_group = self.next_group.max(group + 1);
            let phase = if stored.intent_members.is_empty() { BatchPhase::Collecting } else { BatchPhase::Mutating };
            self.groups.insert(group, Group { ids: Vec::new(), op_id: stored.op_id, reply: None, ready: BTreeSet::new(), joint: stored.joint, pending_op: None, phase, op_row: None });
        }
    }

    fn failed_restored(&mut self, id: TransferId, error: String, fx: &mut Fx) {
        if let Some(transfer) = self.transfers.get_mut(&id) {
            transfer.stage = TransferStage::Failed;
            transfer.error = Some(error.clone());
        }
        self.settle(id, OutboxOutcome::Failed { error: error.clone() }, fx);
        self.show(id, Shown::Failed { error }, fx);
        self.schedule_expire(fx);
    }

    /// Every restored row has exactly one class.
    pub(super) fn classify(&mut self, now: crate::runtime::Time, row: StoredTransfer, outcomes: &TransferOutcomes, fx: &mut Fx) {
        if row.restore_failed.is_some() {
            let id = self.insert_restored(&row, TransferStage::Failed);
            return self.failed_restored(id, "RestoreFailed".into(), fx);
        }
        if row.held {
            let id = self.insert_restored(&row, TransferStage::Ready);
            if row.copying { return self.restore_copy(id, fx); }
            self.delete_prepare_files(id, fx);
            return self.discard_held(id, fx);
        }
        if !self.declarations.contains(&(row.mutation.model.clone(), row.mutation.action.clone())) {
            if self.complete {
                let id = self.insert_restored(&row, TransferStage::Ready);
                return self.fail_undeclared(now, id, fx);
            }
            let id = self.insert_restored(&row, TransferStage::WaitingDeclaration);
            self.waiting.insert(id, (row, outcomes.clone()));
            return;
        }
        self.classify_declared(now, row, outcomes, fx);
    }

    /// The registration is complete and does not declare the upload operation of a saved transfer: it fails with the local refusal and its held step goes to Session.
    fn fail_undeclared(&mut self, now: crate::runtime::Time, id: TransferId, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get(&id) else { return };
        let error = super::failure::undeclared_upload(&transfer.mutation.model, &transfer.mutation.action);
        self.release_step(id, fx);
        self.begin_failure(now, id, error, false, fx);
    }

    fn classify_declared(&mut self, now: crate::runtime::Time, row: StoredTransfer, outcomes: &TransferOutcomes, fx: &mut Fx) {
        let id = self.insert_restored(&row, TransferStage::Ready);
        if row.staged { run(fx, TransferEffect::PromoteStaged { transfer_id: id }); }
        if let Some(saved) = outcomes.get(&id) {
            if let Some(transfer) = self.transfers.get_mut(&id) { transfer.outcomes = saved.clone(); }
        }
        if row.awaiting_source {
            self.schedule_expire(fx);
            return self.begin_failure(now, id, "missing_file".into(), false, fx);
        }
        if row.copying {
            return self.restore_copy(id, fx);
        }
        if row.completed {
            if let Some(saved) = outcomes.get(&id).and_then(|cycles| cycles.get(&row.cycle_id.max(1))) {
                return self.restore_completed(now, id, &row, Some(saved), fx);
            }
            if let Some(error) = row.failed.clone() {
                return self.failed_restored(id, error, fx);
            }
            if !row.has_intent && outcomes.get(&id).is_some_and(|cycles| cycles.keys().any(|cycle| *cycle < row.cycle_id)) {
                if let Some(group) = row.batch.and_then(|group| self.groups.get_mut(&group)) {
                    group.phase = BatchPhase::Singles;
                    self.set(id, TransferStage::BatchReady);
                    return self.show(id, Shown::Uploading { progress: 100 }, fx);
                }
                self.set(id, TransferStage::PersistingBatchIntent);
                return self.show(id, Shown::Uploading { progress: 100 }, fx);
            }
            return self.restore_completed(now, id, &row, None, fx);
        }
        if let Some(error) = row.failed.clone() {
            return self.failed_restored(id, error, fx);
        }
        if !row.file_present {
            if let Some(transfer) = self.transfers.get_mut(&id) {
                transfer.stage = TransferStage::PersistingFailure;
            }
            return self.begin_failure(now, id, "missing_file".into(), false, fx);
        }
        if row.prepare.is_some() { return self.restore_prepare(id, fx); }
        if self.restore_put(id, &row, fx) { return; }
        if row.source.as_ref().is_some_and(|source| row.confirmed_parts >= source.parts) { self.show(id, Shown::Uploading { progress: row.floor.max(99) }, fx); } else { self.show_queued(id, fx); }
        self.rounds_needed(vec![id], fx);
        self.schedule_expire(fx);
    }

    /// A multipart upload outside the process with a recorded attempt: the system may still run its PUT, so the transfer waits in `Uploading` for `PutEnded` or `PutMissing` after `ResumePuts`.
    fn restore_put(&mut self, id: TransferId, row: &StoredTransfer, fx: &mut Fx) -> bool {
        let waits = self.sizing.detached && row.attempts > 0 && row.upload_id.is_some() && row.pending_step.is_none() && row.source.as_ref().is_some_and(|source| row.confirmed_parts < source.parts)
            && self.transfers.get(&id).and_then(|transfer| transfer.steps.as_ref()).and_then(|steps| serde_json::from_str::<super::upload_codec::StepsBody>(&steps.body).ok()).is_some_and(|body| body.multipart);
        if !waits { return false; }
        let attempt_id = self.attempt();
        let Some(transfer) = self.transfers.get_mut(&id) else { return false };
        transfer.stage = TransferStage::Uploading;
        transfer.in_flight = true;
        transfer.put_detached = true;
        transfer.attempt_id = attempt_id;
        let Some(progress) = Self::share(transfer, self.sizing.part_size, false) else { return false };
        self.show(id, Shown::Uploading { progress }, fx);
        self.rounds_needed(vec![id], fx);
        self.schedule_expire(fx);
        true
    }

    fn restore_copy(&mut self, id: TransferId, fx: &mut Fx) {
        self.remove_with(id, Removal::CopyFailed { error: "unaccepted".into() }, fx);
        self.store(&[id], StoreKind::Removed, fx);
    }

    fn restore_completed(&mut self, now: crate::runtime::Time, id: TransferId, row: &StoredTransfer, saved: Option<&OutboxOutcome>, fx: &mut Fx) {
        match saved {
            Some(OutboxOutcome::Confirmed { revision, result, row_id }) => {
                self.remove_with(id, Removal::Confirmed { revision: *revision, result: result.clone(), row_id: row_id.clone() }, fx);
                self.store(&[id], StoreKind::Removed, fx);
            }
            Some(OutboxOutcome::Discarded) => {
                self.remove_with(id, Removal::Discarded, fx);
                self.store(&[id], StoreKind::Removed, fx);
            }
            Some(OutboxOutcome::Rejected { error } | OutboxOutcome::Failed { error }) => {
                if let (Some(saved_error), Some(_)) = (&row.failed, row.failed_at) {
                    self.failed_restored(id, saved_error.clone(), fx);
                } else {
                    self.set(id, TransferStage::PersistingFailure);
                    self.begin_failure_at(now, id, error.clone(), false, row.failed_at, fx);
                }
            }
            Some(OutboxOutcome::Ended(cause)) => {
                self.completed.entry(id).or_default().insert(row.cycle_id.max(1), OutboxOutcome::Ended(cause.clone()));
                self.set(id, TransferStage::Done);
            }
            None => {
                if row.has_intent {
                    if let Some(group) = row.batch.and_then(|group| self.groups.get_mut(&group)) {
                        if group.phase == BatchPhase::Collecting { group.phase = BatchPhase::Singles; }
                        if group.op_row.is_none() { group.op_row = Some((row.mutation.model.clone(), row.mutation.row_id.clone())); }
                    }
                    self.set(id, TransferStage::Mutating);
                    self.show(id, Shown::Uploading { progress: 100 }, fx);
                    self.rounds_needed(vec![id], fx);
                    return;
                }
                let group = row.batch.filter(|group| self.open_groups.contains(group) && self.groups.get(group).is_some_and(|entry| entry.op_id.is_some() && entry.phase == BatchPhase::Collecting));
                if let Some(group) = group {
                    if let Some(transfer) = self.transfers.get_mut(&id) {
                        transfer.batch_id = Some(group);
                        transfer.stage = TransferStage::BatchReady;
                    }
                    self.rejoin(id, group);
                    return self.show(id, Shown::Uploading { progress: 100 }, fx);
                }
                self.failed_restored(id, "RestoreFailed".into(), fx);
            }
        }
    }

    pub(super) fn restored(&mut self, now: crate::runtime::Time, mut uploads: Vec<StoredTransfer>, outcomes: TransferOutcomes, fx: &mut Fx) {
        uploads.sort_by_key(|row| row.seq);
        self.open_groups = uploads.iter().filter_map(|row| row.batch).collect();
        let held: BTreeSet<TransferId> = uploads.iter().map(|row| row.transfer_id).collect();
        for (id, cycles) in outcomes.iter().filter(|(id, _)| !held.contains(id)) { self.completed.insert(*id, cycles.clone()); }
        for mut row in uploads {
            if row.batch.is_some_and(|group| self.groups.get(&group).is_none_or(|entry| entry.op_id.is_none())) {
                row.restore_failed = Some("invalid batch provenance".into());
            }
            self.classify(now, row, &outcomes, fx);
        }
        self.phase = Phase::Classified;
        run(fx, TransferEffect::RestoreClassified { epoch: self.epoch });
        self.replay_held_accepts(now, fx);
    }

    /// A declaration arrives: a transfer that waits for it is classified again.
    pub(super) fn register(&mut self, now: crate::runtime::Time, declarations: Vec<(String, String)>, complete: bool, fx: &mut Fx) {
        self.declarations.extend(declarations);
        self.complete = complete;
        if self.phase == Phase::Restoring {
            return;
        }
        let ready: Vec<TransferId> = self.waiting.iter().filter(|(_, (row, _))| self.declarations.contains(&(row.mutation.model.clone(), row.mutation.action.clone()))).map(|(id, _)| *id).collect();
        let classified = !ready.is_empty();
        let undeclared: Vec<TransferId> = self.waiting.iter().filter(|(_, (row, _))| self.complete && !self.declarations.contains(&(row.mutation.model.clone(), row.mutation.action.clone()))).map(|(id, _)| *id).collect();
        for id in undeclared {
            self.waiting.remove(&id);
            self.set(id, TransferStage::Ready);
            self.fail_undeclared(now, id, fx);
        }
        for id in ready {
            if let Some((row, outcomes)) = self.waiting.remove(&id) {
                self.classify_declared(now, row, &outcomes, fx);
            }
        }
        if self.phase == Phase::Ready {
            self.send_orphans(false, fx);
            if classified { self.release_classified(now, fx); }
        }
    }

    /// `RestoreReady`: the network start of the restored transfers goes in the order of `seq`.
    pub(super) fn restore_ready(&mut self, now: crate::runtime::Time, epoch: crate::runtime::Epoch, fx: &mut Fx) {
        if epoch != self.epoch || self.phase != Phase::Classified {
            return;
        }
        self.phase = Phase::Ready;
        self.send_orphans(true, fx);
        self.replay_held(now, fx);
        self.release_classified(now, fx);
        if self.sizing.detached {
            let identities = self.scan().filter(|transfer| transfer.put_detached && transfer.in_flight && transfer.stage == TransferStage::Uploading).map(|transfer| self.put_identity(transfer)).collect();
            run(fx, TransferEffect::ResumePuts { identities });
        }
    }

    fn release_classified(&mut self, now: crate::runtime::Time, fx: &mut Fx) {
        let groups: Vec<_> = self.groups.keys().copied().collect();
        for group in groups {
            if self.phase_of(group) == Some(BatchPhase::Singles) { self.single_next(group, fx); }
            else { self.batch_release(group, fx); }
        }
        let singles: Vec<_> = self.scan().filter(|transfer| transfer.batch_id.is_none() && transfer.stage == TransferStage::PersistingBatchIntent && !self.pending.values().any(|pending| pending.ids.contains(&transfer.transfer_id))).map(|transfer| transfer.transfer_id).collect();
        for id in singles { self.store(&[id], StoreKind::BatchIntent, fx); }
        if self.scan().any(|transfer| transfer.stage == TransferStage::Preparing) { self.free_prepare_slot(fx); }
        self.start_in_order(now, fx);
    }
}
