//! Owner `Transfer`: the ends of a transfer: `cancel`, `discard`, `retry`, `Failed`, `Removing`, the expiry, the outcomes, the streams of status, the end of the epoch and the close.

use std::collections::BTreeMap;
use super::outbox::{Fault, OutboxInput, OutboxOutcome, Receipt};
use super::transfer::{reply, run, Fx, TransferState};
use super::transfer_types::*;
use crate::owners::contract::{RejectReason, ReplyId};
use crate::runtime::{Effect, Outbound};

impl TransferState {
    fn refuse(fx: &mut Fx, rep: ReplyId, fault: Fault) { reply(fx, rep, TransferAnswer::Err(fault)); }

    fn begin_cancel(&mut self, id: TransferId, cancel: Option<(ReplyId, bool)>, prior: Option<TransferStage>, fx: &mut Fx) {
        self.remove_with(id, Removal::Cancel, fx);
        if let Some(transfer) = self.transfers.get_mut(&id) {
            transfer.cancel_reply = cancel;
            transfer.prior = prior;
        }
        self.store(&[id], StoreKind::Removed, fx);
    }

    /// `cancel_upload` (`cancel`) and `discard`: the rows of the table by the state of the transfer.
    pub(super) fn cancel(&mut self, key: &str, rep: ReplyId, cancel: bool, fx: &mut Fx) {
        let Some(id) = self.open_id(key) else { return Self::refuse(fx, rep, Fault::NotFound) };
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        let (stage, batched) = (transfer.stage, transfer.batch_id.is_some());
        if self.group_entry(id).is_some_and(|group| self.phase_of(group) == Some(BatchPhase::Mutating)) {
            return Self::refuse(fx, rep, Fault::InFlight);
        }
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        match stage {
            TransferStage::AwaitingSource => self.begin_cancel(id, Some((rep, cancel)), Some(TransferStage::AwaitingSource), fx),
            TransferStage::Copying if transfer.supply_back.is_some() => Self::refuse(fx, rep, Fault::InFlight),
            TransferStage::Copying if !batched => {
                run(fx, TransferEffect::CopyCancel { transfer_id: id });
                self.begin_cancel(id, Some((rep, cancel)), None, fx);
            }
            TransferStage::Ready => {
                self.begin_cancel(id, Some((rep, cancel)), Some(TransferStage::Ready), fx);
                self.start_in_order(fx);
            }
            TransferStage::Uploading | TransferStage::Completing => {
                transfer.next_step = if stage == TransferStage::Completing { NextStep::Complete } else { NextStep::Part(transfer.confirmed_parts + 1) };
                if std::mem::replace(&mut transfer.in_flight, false) {
                    run(fx, TransferEffect::CancelStep { transfer_id: id });
                }
                self.begin_cancel(id, Some((rep, cancel)), Some(TransferStage::Ready), fx);
                self.start_in_order(fx);
            }
            TransferStage::Failed => self.discard_failed(id, Some((rep, cancel)), fx),
            TransferStage::BatchReady => self.begin_cancel(id, Some((rep, cancel)), Some(TransferStage::BatchReady), fx),
            TransferStage::Mutating if cancel => reply(fx, rep, if batched { TransferAnswer::Err(Fault::InFlight) } else { TransferAnswer::Cancelled(false) }),
            TransferStage::Mutating => self.discard_through_outbox(id, Some((rep, false)), fx),
            _ => Self::refuse(fx, rep, Fault::InFlight),
        }
    }

    /// `discard` and the expiry of a `Failed` transfer: the Outbox owns the operation of a saved or live intent, otherwise the transfer leaves alone.
    fn discard_failed(&mut self, id: TransferId, waiter: Option<(ReplyId, bool)>, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get(&id) else { return };
        let through_outbox = transfer.intent_live && transfer.signed_id.is_some() && !matches!(transfer.error.as_deref(), Some("missing_file" | "RestoreFailed"));
        if through_outbox {
            self.discard_through_outbox(id, waiter, fx);
        } else {
            self.begin_cancel(id, waiter, Some(TransferStage::Failed), fx);
        }
    }

    /// The Outbox owns the live intent of the final mutation: it rolls the row back and answers with `MutationOutcome(Discarded)`.
    fn discard_through_outbox(&mut self, id: TransferId, waiter: Option<(ReplyId, bool)>, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        match waiter {
            Some((rep, false)) => transfer.discard_waiters.push(rep),
            Some((rep, true)) if transfer.cancel_reply.is_some() => return Self::refuse(fx, rep, Fault::InFlight),
            Some((rep, true)) => transfer.cancel_reply = Some((rep, true)),
            None => {}
        }
        if !std::mem::replace(&mut transfer.discard_sent, true) {
            let (model, row_id, action) = (transfer.mutation.model.clone(), transfer.mutation.row_id.clone(), transfer.mutation.action.clone());
            fx.push(Effect::Send(Outbound::new(self.peers.outbox, OutboxInput::Discard { model, row_id, action: Some(action), reply: NO_REPLY })));
        }
    }

    /// The smallest `op_id` among the open transfers of the row decides: a transfer that does not follow the operation of Outbox answers the call itself.
    pub(super) fn select_row(&mut self, (model, rows): (String, Vec<String>), outbox_min: Option<(super::outbox::OpId, String)>, kind: super::outbox_types::SelectKind, rep: ReplyId, fx: &mut Fx) {
        let named = |key: &str| rows.iter().any(|row| key.strip_prefix(&format!("{model}:{row}:")).is_some());
        let first = self.scan().filter(|transfer| transfer.stage != TransferStage::Done && named(&transfer.key)).filter_map(|transfer| {
            let op_id = transfer.op_id.or_else(|| self.group_entry(transfer.transfer_id).and_then(|group| self.groups.get(&group)?.op_id))?;
            Some((op_id, transfer.key.clone()))
        }).min();
        match (first, outbox_min) {
            (Some((op_id, key)), min) if min.as_ref().is_none_or(|(smallest, _)| op_id <= *smallest) => match kind {
                super::outbox_types::SelectKind::Discard => self.cancel(&key, rep, false, fx),
                super::outbox_types::SelectKind::Retry => self.retry(&key, None, rep, fx),
            },
            (_, Some((_, action))) => {
                let row_id = rows.into_iter().next().unwrap_or_default();
                fx.push(Effect::Send(Outbound::new(self.peers.outbox, OutboxInput::SelectedOutbox { model, row_id, action, kind, reply: rep })));
            }
            (_, None) => fx.push(Effect::Send(Outbound::new(self.peers.outbox, OutboxInput::SelectedNone { reply: rep }))),
        }
    }

    pub(super) fn retry(&mut self, key: &str, source: Option<SuppliedSource>, rep: ReplyId, fx: &mut Fx) {
        let Some(id) = self.open_id(key) else { return Self::refuse(fx, rep, Fault::NotFound) };
        let mutation_failed = self.transfers.get(&id).is_some_and(|transfer| transfer.stage == TransferStage::Failed && transfer.signed_id.is_some() && transfer.error.as_deref() != Some("missing_file") && transfer.error.as_deref() != Some("RestoreFailed"));
        if mutation_failed && self.transfers.get(&id).is_some_and(|transfer| !transfer.intent_live) {
            if let Some(transfer) = self.transfers.get_mut(&id) {
                transfer.next_step = NextStep::Mutation;
                transfer.stage = TransferStage::PersistingRetry;
                transfer.retry_reply = Some(rep);
            }
            self.store(&[id], StoreKind::Retrying, fx);
            return;
        }
        if mutation_failed && self.batch_retry_mutation(id, rep, fx) {
            return;
        }
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        if mutation_failed {
            transfer.next_step = NextStep::Mutation;
            transfer.stage = TransferStage::PersistingRetry;
            transfer.retry_reply = Some(rep);
            self.request_mutation_retry(&[id], fx);
            return;
        }
        match transfer.stage {
            TransferStage::Ready | TransferStage::Uploading | TransferStage::Completing | TransferStage::BatchReady | TransferStage::Mutating => {
                let receipt = self.receipt(id);
                reply(fx, rep, TransferAnswer::Receipt(receipt));
            }
            TransferStage::Failed if transfer.error.as_deref().is_some_and(|error| error == "missing_file" || error.starts_with("source_failed:")) => {
                let Some(source) = source else { return Self::refuse(fx, rep, Fault::Invalid) };
                self.start_source(id, source, TransferStage::Failed, rep, fx);
            }
            TransferStage::Failed if transfer.error.as_deref() == Some("RestoreFailed") => Self::refuse(fx, rep, Fault::RestoreFailed),
            TransferStage::Failed if source.is_some() && transfer.source.is_some() && transfer.place.is_some() => {
                let Some(source) = source else { return };
                transfer.previous = Some(Box::new(transfer.clone()));
                self.start_source(id, source, TransferStage::Failed, rep, fx);
            }
            TransferStage::Failed => {
                let all = transfer.source.as_ref().is_some_and(|source| transfer.confirmed_parts >= source.parts);
                if transfer.aborted {
                    transfer.confirmed_parts = 0;
                    transfer.upload_id = None;
                    transfer.blob_created_at = None;
                    transfer.etags.clear();
                    transfer.aborted = false;
                    transfer.next_step = NextStep::Part(1);
                } else {
                    transfer.next_step = if all { NextStep::Complete } else { NextStep::Part(transfer.confirmed_parts + 1) };
                }
                transfer.stage = TransferStage::PersistingRetry;
                transfer.attempts = 0;
                transfer.retry_reply = Some(rep);
                self.store(&[id], StoreKind::Retrying, fx);
            }
            _ => Self::refuse(fx, rep, Fault::InFlight),
        }
    }

    pub(super) fn retry_committed(&mut self, id: TransferId, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        transfer.cycle_id += 1;
        transfer.stage = TransferStage::Ready;
        transfer.error = None;
        transfer.failed_at = None;
        let rep = transfer.retry_reply.take();
        let receipt = self.receipt(id);
        if let Some(rep) = rep {
            reply(fx, rep, TransferAnswer::Receipt(receipt));
        }
        self.show(id, Shown::Queued, fx);
        self.rounds_needed(vec![id], fx);
        self.start_in_order(fx);
    }

    pub(super) fn retry_recreate_committed(&mut self, id: TransferId, fx: &mut Fx) {
        let group = self.group_entry(id);
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        transfer.cycle_id += 1;
        transfer.stage = if group.is_some() { TransferStage::BatchReady } else { TransferStage::PersistingBatchIntent };
        transfer.error = None;
        transfer.failed_at = None;
        let rep = transfer.retry_reply.take();
        if let Some(entry) = group.and_then(|group| self.groups.get_mut(&group)) { entry.phase = BatchPhase::Singles; }
        if let Some(rep) = rep { reply(fx, rep, TransferAnswer::Receipt(self.receipt(id))); }
        if let Some(group) = group { self.single_next(group, fx); }
        else { self.store(&[id], StoreKind::BatchIntent, fx); }
    }

    pub(super) fn retry_rejected(&mut self, id: TransferId, reason: RejectReason, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        transfer.stage = TransferStage::Failed;
        if let Some(rep) = transfer.retry_reply.take() {
            Self::refuse(fx, rep, Fault::Rejected(reason));
        }
    }

    pub(super) fn failure_committed(&mut self, id: TransferId, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        if transfer.failure_abort {
            if let Some(upload_id) = transfer.upload_id.take() {
                self.orphans.insert(upload_id, transfer.key.clone());
            }
            transfer.confirmed_parts = 0;
            transfer.etags.clear();
            transfer.aborted = true;
            transfer.failure_abort = false;
        }
        transfer.stage = TransferStage::Failed;
        let error = transfer.error.clone().unwrap_or_default();
        let had_source = transfer.source.is_some();
        if let Some(rep) = transfer.supply_reply.take() { reply(fx, rep, TransferAnswer::Ok); }
        self.settle(id, OutboxOutcome::Failed { error: error.clone() }, fx);
        self.show(id, Shown::Failed { error }, fx);
        if had_source { self.rounds_idle(id, fx); }
        self.batch_after_removal(id, fx);
        self.send_orphans(false, fx);
    }

    pub(super) fn failure_rejected(&mut self, id: TransferId, fx: &mut Fx) { self.schedule_retry(id, fx); }

    fn removal_replies(&mut self, id: TransferId, removal: &Removal, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        let cancel = transfer.cancel_reply.take();
        let accept = transfer.accept_reply.take();
        let waiters = std::mem::take(&mut transfer.discard_waiters);
        let group = transfer.batch_id.and_then(|group| self.groups.get_mut(&group).and_then(|group| group.reply.take()));
        match removal {
            Removal::Cancel => {
                if let Some((rep, cancel)) = cancel {
                    reply(fx, rep, if cancel { TransferAnswer::Cancelled(true) } else { TransferAnswer::Ok });
                }
                if let Some(rep) = accept {
                    Self::refuse(fx, rep, Fault::Interrupted);
                }
            }
            Removal::Confirmed { .. } => {}
            Removal::Discarded => {
                for rep in waiters {
                    reply(fx, rep, TransferAnswer::Ok);
                }
                if let Some((rep, _)) = cancel {
                    reply(fx, rep, TransferAnswer::Cancelled(true));
                }
            }
            Removal::CopyFailed { error } => {
                if let Some(rep) = accept.or(group) {
                    Self::refuse(fx, rep, Fault::Copy(error.clone()));
                }
            }
            Removal::Storage => {
                if let Some(rep) = accept.or(group) {
                    Self::refuse(fx, rep, Fault::Rejected(RejectReason::Storage("storage".into())));
                }
            }
        }
    }

    /// The rows the batch of a removal showed, one per transfer that asked for a `Show`.
    pub(super) fn shown_rows(&self, ids: &[TransferId], shown: &[Option<crate::store::Row>]) -> BTreeMap<TransferId, crate::store::Row> {
        let asking = ids.iter().copied().filter(|id| self.transfers.get(id).is_some_and(Self::shows_landed_row));
        asking.zip(shown).filter_map(|(id, row)| row.clone().map(|row| (id, row))).collect()
    }

    pub(super) fn shows_landed_row(transfer: &super::transfer_types::Transfer) -> bool {
        transfer.stage == TransferStage::Removing && matches!(transfer.removal, Some(Removal::Confirmed { row_id: Some(_), .. }))
    }

    pub(super) fn removing_committed(&mut self, ids: &[TransferId], mut rows: BTreeMap<TransferId, crate::store::Row>, fx: &mut Fx) {
        for &id in ids {
            let Some(removal) = self.transfers.get_mut(&id).and_then(|transfer| transfer.removal.take()) else { continue };
            self.removal_replies(id, &removal, fx);
            if let (Removal::Cancel, Some(transfer)) = (&removal, self.transfers.get(&id)) {
                if let Some(upload_id) = Self::orphan_of(transfer) {
                    self.orphans.insert(upload_id, transfer.key.clone());
                }
            }
            match removal.clone() {
                Removal::Cancel | Removal::Discarded => self.settle(id, OutboxOutcome::Discarded, fx),
                Removal::Confirmed { revision, result, row_id } => {
                    if let (Some(row), Some(cycle)) = (rows.remove(&id), self.transfers.get(&id).map(|transfer| transfer.cycle_id)) { self.landed_rows.insert((id, cycle), row); }
                    self.settle(id, OutboxOutcome::Confirmed { revision, result, row_id }, fx)
                }
                Removal::CopyFailed { .. } | Removal::Storage => run(fx, TransferEffect::DeleteFile { path: file_id(id) }),
            }
            if matches!(removal, Removal::Cancel | Removal::Discarded | Removal::Confirmed { .. }) && self.transfers.get(&id).is_some_and(|transfer| transfer.source.is_some()) {
                self.delete_copy(id, fx);
            }
            self.finish(id, fx);
            self.batch_after_removal(id, fx);
            if let Some(transfer) = self.transfers.remove(&id) {
                if !transfer.outcomes.is_empty() { self.completed.insert(id, transfer.outcomes); }
                if let Some(group_id) = transfer.batch_id {
                    if let Some(group) = self.groups.get_mut(&group_id) { group.ids.retain(|member| *member != id); }
                    if self.groups.get(&group_id).is_some_and(|group| group.ids.is_empty()) { self.groups.remove(&group_id); }
                }
            }
        }
        self.send_orphans(false, fx);
        self.start_in_order(fx);
    }

    pub(super) fn removing_rejected(&mut self, ids: &[TransferId], reason: RejectReason, fx: &mut Fx) {
        if !matches!(reason, RejectReason::Storage(_)) {
            for &id in ids {
                self.interrupt(id, super::outbox_types::end_cause(&reason), fx);
            }
            return;
        }
        for &id in ids {
            let Some(transfer) = self.transfers.get_mut(&id) else { continue };
            let removal = transfer.removal.take();
            match (transfer.prior.take(), removal) {
                (Some(prior), _) => {
                    transfer.stage = prior;
                    if let Some((rep, _)) = transfer.cancel_reply.take() {
                        Self::refuse(fx, rep, Fault::Rejected(reason.clone()));
                    }
                    if prior == TransferStage::Ready {
                        self.show(id, Shown::Queued, fx);
                    }
                    if prior == TransferStage::BatchReady {
                        self.show(id, Shown::Uploading { progress: 100 }, fx);
                    }
                }
                (None, Some(Removal::Confirmed { .. } | Removal::Discarded)) => self.begin_failure(id, "storage".into(), false, fx),
                (None, _) => {
                    for rep in [transfer.cancel_reply.take().map(|(rep, _)| rep), transfer.accept_reply.take()].into_iter().flatten() {
                        Self::refuse(fx, rep, Fault::Rejected(reason.clone()));
                    }
                    self.finish(id, fx);
                }
            }
        }
        self.start_in_order(fx);
    }

    /// `Online(true)` asks for the check of the `Failed` transfers at once.
    pub(super) fn expire_failed(&mut self, fx: &mut Fx) {
        if self.scan().any(|transfer| transfer.stage == TransferStage::Failed) {
            self.schedule_expire(fx);
        }
    }

    /// `Tick(expire)`: a `Failed` transfer older than 7 days is discarded.
    pub(super) fn expire(&mut self, fx: &mut Fx) {
        let old: Vec<TransferId> = self.scan().filter(|transfer| transfer.stage == TransferStage::Failed && self.now.0.saturating_sub(transfer.failed_at.unwrap_or(transfer.created_at).0) > EXPIRY_MS).map(|transfer| transfer.transfer_id).collect();
        for id in old {
            self.discard_failed(id, None, fx);
        }
    }

    pub(super) fn outcome(&mut self, receipt: Receipt, rep: ReplyId, fx: &mut Fx) {
        if let Some(outcome) = self.completed.get(&receipt.op_id).and_then(|cycles| cycles.get(&receipt.cycle_id)) {
            return reply(fx, rep, TransferAnswer::Outcome(outcome.clone(), self.landed_rows.get(&(receipt.op_id, receipt.cycle_id)).cloned()));
        }
        let Some(transfer) = self.transfers.get_mut(&receipt.op_id) else { return Self::refuse(fx, rep, Fault::NotFound) };
        if let Some(found) = transfer.outcomes.get(&receipt.cycle_id) {
            return reply(fx, rep, TransferAnswer::Outcome(found.clone(), self.landed_rows.get(&(receipt.op_id, receipt.cycle_id)).cloned()));
        }
        if receipt.cycle_id == transfer.cycle_id && transfer.stage != TransferStage::Done {
            transfer.outcome_waiters.push((receipt.cycle_id, rep));
        } else {
            Self::refuse(fx, rep, Fault::NotFound);
        }
    }

    pub(super) fn interest_plus(&mut self, key: Key, stream_id: StreamId, fx: &mut Fx) {
        let shown = self.shown_by_key(&key);
        self.subscribers.entry(key.clone()).or_default().insert(stream_id);
        run(fx, TransferEffect::Status { stream_id, key, shown });
    }

    pub(super) fn interest_minus(&mut self, key: &str, stream_id: StreamId) {
        if let Some(streams) = self.subscribers.get_mut(key) {
            streams.remove(&stream_id);
            if streams.is_empty() {
                self.subscribers.remove(key);
            }
        }
    }

    pub(super) fn round_status(&mut self, epoch: crate::runtime::Epoch, ids: &[TransferId], refused: Option<String>, fx: &mut Fx) {
        if epoch != self.epoch {
            return;
        }
        let (mut ops, mut shows) = (Vec::new(), Vec::new());
        for &id in ids {
            let Some(transfer) = self.transfers.get_mut(&id) else { continue };
            if transfer.stage == TransferStage::Done || transfer.round_error == refused {
                continue;
            }
            transfer.round_error = refused.clone();
            ops.push(crate::owners::store::StoreOp::UploadRoundError { key: transfer.key.clone(), error: refused.clone() });
            if refused.is_none() || matches!(transfer.stage, TransferStage::Ready | TransferStage::Uploading) {
                shows.push((id, refused.is_some()));
            }
        }
        for (id, refusing) in shows {
            let shown = if refusing { Shown::Failed { error: "refused".into() } } else { self.shown_now(id) };
            self.show(id, shown, fx);
        }
        self.commit_ops(ops, fx);
    }

    fn open_ids(&self) -> Vec<TransferId> { self.scan().filter(|transfer| transfer.stage != TransferStage::Done).map(|transfer| transfer.transfer_id).collect() }

    pub(super) fn epoch_ended(&mut self, fx: &mut Fx) {
        self.ended = true;
        self.pending.clear();
        self.minted.clear();
        for id in self.open_ids() {
            self.interrupt(id, crate::ffi::EndCause::SessionEnded, fx);
        }
        self.answer_held(Fault::SessionEnded, fx);
    }

    pub(super) fn close(&mut self, fx: &mut Fx) {
        self.closing = true;
        for id in self.open_ids() {
            match self.stage_of(id) {
                Some(TransferStage::Accepting) => {
                    if let Some(transfer) = self.transfers.get_mut(&id) {
                        transfer.closing = true;
                    }
                }
                Some(TransferStage::Copying | TransferStage::CopyReady) => {
                    run(fx, TransferEffect::CopyCancel { transfer_id: id });
                    run(fx, TransferEffect::DeleteFile { path: file_id(id) });
                    self.interrupt(id, crate::ffi::EndCause::Closed, fx);
                }
                _ => self.interrupt(id, crate::ffi::EndCause::Closed, fx),
            }
        }
        self.answer_held(Fault::Closed, fx);
        self.maybe_drained(fx);
    }
}
