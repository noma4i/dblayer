//! Owner `Transfer`: the ends of a transfer: `cancel`, `discard`, `retry`, `Failed`, `Removing`, the expiry, the outcomes, the streams of status, the end of the epoch and the close.

use super::outbox::{Fault, OutboxInput, OutboxOutcome, Receipt};
use super::transfer::{reply, run, Fx, TransferState};
use super::transfer_types::*;
use crate::owners::contract::{RejectReason, ReplyId};
use crate::runtime::{Effect, Outbound};

impl TransferState {
    fn refuse(fx: &mut Fx, rep: ReplyId, fault: Fault) { reply(fx, rep, TransferAnswer::Err(fault)); }

    fn send_abort(&mut self, id: TransferId, fx: &mut Fx) {
        let attempt_id = self.attempt();
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        let Some(upload_id) = transfer.upload_id.clone() else { return };
        if !std::mem::replace(&mut transfer.aborted, true) {
            run(fx, TransferEffect::Abort { transfer_id: id, attempt_id, key: transfer.key.clone(), upload_id });
        }
    }

    fn begin_cancel(&mut self, id: TransferId, cancel: Option<(ReplyId, bool)>, prior: Option<TransferStage>, fx: &mut Fx) {
        self.remove_with(id, Removal::Cancel);
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
        match stage {
            TransferStage::Copying if !batched => {
                run(fx, TransferEffect::CopyCancel { transfer_id: id });
                self.begin_cancel(id, Some((rep, cancel)), None, fx);
            }
            TransferStage::Ready => {
                self.begin_cancel(id, Some((rep, cancel)), Some(TransferStage::Ready), fx);
                self.start_in_order(fx);
            }
            TransferStage::Uploading | TransferStage::Completing if !(cancel && batched) => {
                transfer.next_step = if stage == TransferStage::Completing { NextStep::Complete } else { NextStep::Part(transfer.confirmed_parts + 1) };
                if std::mem::replace(&mut transfer.in_flight, false) {
                    run(fx, TransferEffect::CancelStep { transfer_id: id });
                }
                self.send_abort(id, fx);
                self.begin_cancel(id, Some((rep, cancel)), Some(TransferStage::Ready), fx);
                self.start_in_order(fx);
            }
            TransferStage::Failed => {
                self.send_abort(id, fx);
                self.begin_cancel(id, Some((rep, cancel)), Some(TransferStage::Failed), fx);
            }
            TransferStage::Mutating if cancel => reply(fx, rep, if batched { TransferAnswer::Err(Fault::InFlight) } else { TransferAnswer::Cancelled(false) }),
            TransferStage::Mutating => {
                transfer.discard_waiters.push(rep);
                if !std::mem::replace(&mut transfer.discard_sent, true) {
                    let (model, row_id) = (transfer.mutation.model.clone(), transfer.mutation.row_id.clone());
                    fx.push(Effect::Send(Outbound::new(self.peers.outbox, OutboxInput::Discard { model, row_id, reply: NO_REPLY })));
                }
            }
            _ => Self::refuse(fx, rep, Fault::InFlight),
        }
    }

    pub(super) fn retry(&mut self, key: &str, source: Option<Source>, rep: ReplyId, fx: &mut Fx) {
        let Some(id) = self.open_id(key) else { return Self::refuse(fx, rep, Fault::NotFound) };
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        match transfer.stage {
            TransferStage::Ready | TransferStage::Uploading | TransferStage::Completing | TransferStage::Mutating => {
                let receipt = self.receipt(id);
                reply(fx, rep, TransferAnswer::Receipt(receipt));
            }
            TransferStage::Failed if transfer.error.as_deref() == Some("missing_file") => {
                let Some(source) = source else { return Self::refuse(fx, rep, Fault::Invalid) };
                transfer.source = source;
                transfer.stage = TransferStage::Accepting;
                transfer.prior = Some(TransferStage::Failed);
                transfer.accept_reply = Some(rep);
                transfer.place = None;
                transfer.confirmed_parts = 0;
                transfer.upload_id = None;
                transfer.etags.clear();
                self.store(&[id], StoreKind::Retrying, fx);
            }
            TransferStage::Failed if transfer.error.as_deref() == Some("RestoreFailed") => Self::refuse(fx, rep, Fault::RestoreFailed),
            TransferStage::Failed => {
                let all = transfer.confirmed_parts >= transfer.source.parts;
                if transfer.aborted {
                    transfer.confirmed_parts = 0;
                    transfer.upload_id = None;
                    transfer.etags.clear();
                    transfer.aborted = false;
                    transfer.next_step = NextStep::Part(1);
                } else {
                    transfer.next_step = if all { NextStep::Complete } else { NextStep::Part(transfer.confirmed_parts + 1) };
                }
                transfer.stage = TransferStage::PersistingRetry;
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
        transfer.attempts = 0;
        let rep = transfer.retry_reply.take();
        let receipt = self.receipt(id);
        if let Some(rep) = rep {
            reply(fx, rep, TransferAnswer::Receipt(receipt));
        }
        self.show(id, Shown::Queued, fx);
        self.rounds_needed(vec![id], fx);
        self.start_in_order(fx);
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
        transfer.stage = TransferStage::Failed;
        let error = transfer.error.clone().unwrap_or_default();
        self.settle(id, OutboxOutcome::Failed { error: error.clone() }, fx);
        self.show(id, Shown::Failed { error }, fx);
        self.rounds_idle(id, fx);
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

    pub(super) fn removing_committed(&mut self, ids: &[TransferId], fx: &mut Fx) {
        for &id in ids {
            let Some(removal) = self.transfers.get_mut(&id).and_then(|transfer| transfer.removal.take()) else { continue };
            self.removal_replies(id, &removal, fx);
            match removal {
                Removal::Cancel | Removal::Discarded => self.settle(id, OutboxOutcome::Discarded, fx),
                Removal::Confirmed { revision } => self.settle(id, OutboxOutcome::Confirmed { revision, result: None }, fx),
                Removal::CopyFailed { .. } | Removal::Storage => run(fx, TransferEffect::DeleteFile { path: file_id(&self.transfers[&id].key) }),
            }
            if matches!(removal, Removal::Cancel | Removal::Discarded | Removal::Confirmed { .. }) {
                self.delete_copy(id, fx);
            }
            self.finish(id, fx);
        }
        self.start_in_order(fx);
    }

    pub(super) fn removing_rejected(&mut self, ids: &[TransferId], reason: RejectReason, fx: &mut Fx) {
        if !matches!(reason, RejectReason::Storage(_)) {
            for &id in ids {
                self.interrupt(id, fx);
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
        if self.transfers.values().any(|transfer| transfer.stage == TransferStage::Failed) {
            self.schedule_expire(fx);
        }
    }

    /// `Tick(expire)`: a `Failed` transfer older than 7 days is discarded.
    pub(super) fn expire(&mut self, fx: &mut Fx) {
        let old: Vec<TransferId> = self.transfers.values().filter(|transfer| transfer.stage == TransferStage::Failed && self.now.0.saturating_sub(transfer.created_at.0) > EXPIRY_MS).map(|transfer| transfer.transfer_id).collect();
        for id in old {
            self.send_abort(id, fx);
            self.begin_cancel(id, None, Some(TransferStage::Failed), fx);
        }
    }

    pub(super) fn outcome(&mut self, receipt: Receipt, rep: ReplyId, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get_mut(&receipt.op_id) else { return Self::refuse(fx, rep, Fault::NotFound) };
        if let Some(found) = transfer.outcomes.get(&receipt.cycle_id) {
            return reply(fx, rep, TransferAnswer::Outcome(found.clone()));
        }
        if receipt.cycle_id == transfer.cycle_id && transfer.stage != TransferStage::Done {
            transfer.outcome_waiters.push((receipt.cycle_id, rep));
        } else {
            Self::refuse(fx, rep, Fault::NotFound);
        }
    }

    pub(super) fn interest_plus(&mut self, id: TransferId, stream_id: StreamId, fx: &mut Fx) {
        self.subscribers.entry(id).or_default().insert(stream_id);
        let shown = self.shown_now(id);
        run(fx, TransferEffect::Status { stream_id, transfer_id: id, shown });
    }

    pub(super) fn interest_minus(&mut self, id: TransferId, stream_id: StreamId) {
        if let Some(streams) = self.subscribers.get_mut(&id) {
            streams.remove(&stream_id);
            if streams.is_empty() {
                self.subscribers.remove(&id);
            }
        }
    }

    pub(super) fn round_status(&mut self, epoch: crate::runtime::Epoch, ids: &[TransferId], refused: Option<String>, fx: &mut Fx) {
        if epoch != self.epoch {
            return;
        }
        for &id in ids {
            let Some(transfer) = self.transfers.get_mut(&id) else { continue };
            if transfer.stage == TransferStage::Done || transfer.round_error == refused {
                continue;
            }
            transfer.round_error = refused.clone();
            let shows = refused.is_none() || matches!(transfer.stage, TransferStage::Ready | TransferStage::Uploading);
            if shows {
                run(fx, TransferEffect::Refused { key: transfer.key.clone(), reason: refused.clone() });
            }
        }
    }

    fn open_ids(&self) -> Vec<TransferId> { self.transfers.values().filter(|transfer| transfer.stage != TransferStage::Done).map(|transfer| transfer.transfer_id).collect() }

    pub(super) fn epoch_ended(&mut self, fx: &mut Fx) {
        self.ended = true;
        self.pending.clear();
        for id in self.open_ids() {
            self.interrupt(id, fx);
        }
        self.answer_held(fx);
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
                    run(fx, TransferEffect::DeleteFile { path: file_id(&self.transfers[&id].key) });
                    self.interrupt(id, fx);
                }
                _ => self.interrupt(id, fx),
            }
        }
        self.answer_held(fx);
        self.maybe_drained(fx);
    }
}
