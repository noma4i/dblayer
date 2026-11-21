//! Owner `Transfer`: the ends of a transfer: `cancel`, `discard`, `retry`, `Failed`, `Removing`, the expiry, the outcomes, the streams of status, the end of the epoch and the close.

use super::outbox::{Fault, OutboxInput, OutboxOutcome, Receipt};
use super::outbox_types::Phase;
use super::transfer::{reply, run, Fx, TransferState};
use super::transfer_types::*;
use crate::owners::contract::{RejectReason, ReplyId};
use crate::runtime::{Effect, Outbound};

impl TransferState {
    fn refuse(fx: &mut Fx, rep: ReplyId, fault: Fault) { reply(fx, rep, TransferAnswer::Err(fault)); }

    fn begin_cancel(&mut self, id: TransferId, cancel: Option<(ReplyId, bool)>, prior: Option<TransferStage>, fx: &mut Fx) {
        self.remove_with(id, Removal::Cancel, fx);
        if let Some(transfer) = self.transfers.get_mut(&id) {
            transfer.cancel_replies.extend(cancel);
            transfer.prior = prior;
        }
        self.store(&[id], StoreKind::Removed, fx);
    }

    /// `cancel_upload` (`cancel`) and `discard`: the rows of the table by the state of the transfer.
    pub(super) fn cancel(&mut self, now: crate::runtime::Time, key: &str, rep: ReplyId, cancel: bool, fx: &mut Fx) {
        let Some(id) = self.open_id(key) else { return Self::refuse(fx, rep, Fault::NotFound) };
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        let (stage, batched, committing) = (transfer.stage, transfer.batch_id.is_some(), transfer.commit_reply.is_some());
        let supplying = transfer.supply_back.is_some();
        if stage == TransferStage::Mutating && !Self::defers_removal(stage, batched, supplying, committing) && self.group_entry(id).is_some_and(|group| self.phase_of(group) == Some(BatchPhase::Mutating)) {
            return Self::refuse(fx, rep, Fault::InFlight);
        }
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        if Self::defers_removal(stage, batched, supplying, committing) {
            transfer.commands.push(Waiting::Removal(rep, cancel));
            self.removal_pending.insert(id);
            return;
        }
        if stage == TransferStage::Removing {
            transfer.cancel_replies.push((rep, cancel));
            return;
        }
        if cancel && stage != TransferStage::Mutating && transfer.prepare.is_none() && transfer.source_sha256.is_some() && transfer.prepare_sha256.is_some() {
            transfer.keep_prepared = Some((uuid::Uuid::new_v4().to_string(), now.0));
        }
        match stage {
            TransferStage::AwaitingSource => self.begin_cancel(id, Some((rep, cancel)), Some(TransferStage::AwaitingSource), fx),
            TransferStage::Copying => {
                run(fx, TransferEffect::CopyCancel { transfer_id: id });
                self.begin_cancel(id, Some((rep, cancel)), None, fx);
            }
            TransferStage::Preparing | TransferStage::Measuring => {
                self.stop_prepare(id, fx);
                self.begin_cancel(id, Some((rep, cancel)), None, fx);
                self.free_prepare_slot(fx);
            }
            TransferStage::Ready => {
                self.begin_cancel(id, Some((rep, cancel)), Some(TransferStage::Ready), fx);
                self.start_in_order(now, fx);
            }
            TransferStage::Uploading | TransferStage::Completing => {
                transfer.next_step = if stage == TransferStage::Completing { NextStep::Complete } else { NextStep::Part(transfer.confirmed_parts + 1) };
                self.release_step(id, fx);
                let Some(transfer) = self.transfers.get_mut(&id) else { return };
                if std::mem::replace(&mut transfer.in_flight, false) {
                    run(fx, TransferEffect::CancelStep { transfer_id: id });
                }
                self.begin_cancel(id, Some((rep, cancel)), Some(TransferStage::Ready), fx);
                self.start_in_order(now, fx);
            }
            TransferStage::Failed => self.discard_failed(id, Some((rep, cancel)), fx),
            TransferStage::BatchReady => self.begin_cancel(id, Some((rep, cancel)), Some(TransferStage::BatchReady), fx),
            TransferStage::Held => self.begin_cancel(id, Some((rep, cancel)), Some(TransferStage::Held), fx),
            TransferStage::Mutating if cancel => reply(fx, rep, if batched { TransferAnswer::Err(Fault::InFlight) } else { TransferAnswer::Cancelled(false) }),
            TransferStage::Mutating => self.discard_through_outbox(id, Some((rep, false)), fx),
            _ => Self::refuse(fx, rep, Fault::InFlight),
        }
    }

    /// The stages of the internal steps of the library: a removal command waits in them for the stage it leaves to (D-392).
    pub(super) fn defers_removal(stage: TransferStage, batched: bool, supply: bool, committing: bool) -> bool {
        if committing { return true; }
        match stage {
            TransferStage::Copying => batched || supply,
            TransferStage::Reserving | TransferStage::Accepting | TransferStage::CopyReady | TransferStage::PersistingPrepared | TransferStage::PersistingCopy | TransferStage::PersistingRetry | TransferStage::PersistingFailure | TransferStage::PersistingAttempt | TransferStage::PersistingPart | TransferStage::PersistingComplete | TransferStage::PersistingBatchIntent => true,
            _ => false,
        }
    }

    /// The end of every step of the owner: a transfer that left the stage of an internal step runs its waiting removal commands, the first by the row of its stage, the rest join the removal.
    pub(super) fn run_waiting_removals(&mut self, now: crate::runtime::Time, fx: &mut Fx) {
        for id in std::mem::take(&mut self.removal_pending) {
            let Some(transfer) = self.transfers.get_mut(&id) else { continue };
            let expired_commit = transfer.stage == TransferStage::Uploading && now.0 >= transfer.created_at.0.saturating_add(EXPIRY_MS) && transfer.commands.iter().any(|command| matches!(command, Waiting::Commit(..)));
            if Self::defers_removal(transfer.stage, transfer.batch_id.is_some(), transfer.supply_back.is_some(), transfer.commit_reply.is_some()) || expired_commit {
                self.removal_pending.insert(id);
                continue;
            }
            let (key, waiting) = (transfer.key.clone(), std::mem::take(&mut transfer.commands));
            let mut rest = std::collections::VecDeque::from(waiting);
            while let Some(command) = rest.pop_front() {
                let Some(transfer) = self.transfers.get(&id) else {
                    let (Waiting::Removal(rep, _) | Waiting::Retry(_, rep) | Waiting::Commit(_, _, rep) | Waiting::Asked { rep, .. }) = command;
                    Self::refuse(fx, rep, Fault::NotFound);
                    continue;
                };
                let command = match command {
                    Waiting::Asked { rep, ids, answer: Some(RetryAnswer::Current) } if matches!(transfer.stage, TransferStage::Failed | TransferStage::PersistingFailure | TransferStage::PersistingRetry) && ids.contains(&(id, transfer.cycle_id)) => Waiting::Retry(None, rep),
                    other => other,
                };
                if Self::defers_removal(transfer.stage, transfer.batch_id.is_some(), transfer.supply_back.is_some(), transfer.commit_reply.is_some()) || matches!(command, Waiting::Asked { answer: None, .. }) || (transfer.stage == TransferStage::Removing && matches!(command, Waiting::Retry(..))) {
                    rest.push_front(command);
                    break;
                }
                let stage = transfer.stage;
                match command {
                    Waiting::Removal(rep, cancel) => self.cancel(now, &key, rep, cancel, fx),
                    Waiting::Retry(Some(_), rep) if stage != TransferStage::Failed => Self::refuse(fx, rep, Fault::InFlight),
                    Waiting::Retry(source, rep) => self.retry(&key, source, rep, fx),
                    Waiting::Commit(epoch, request, rep) => self.commit(now, epoch, &key, request, rep, fx),
                    Waiting::Asked { rep, ids, answer } => self.apply_retry_answer(&key, id, rep, ids, answer, fx),
                }
                if let Some(transfer) = self.transfers.get_mut(&id) {
                    for pushed in std::mem::take(&mut transfer.commands).into_iter().rev() { rest.push_front(pushed); }
                }
            }
            if let Some(transfer) = self.transfers.get_mut(&id) {
                let later = std::mem::take(&mut transfer.commands);
                transfer.commands = rest.into_iter().chain(later).collect();
                if !transfer.commands.is_empty() { self.removal_pending.insert(id); }
            }
        }
    }

    pub(super) fn record_retry_answer(&mut self, cycles: &[(TransferId, CycleId)], answer: RetryAnswer) -> bool {
        let mut found = false;
        for &(id, cycle) in cycles {
            let Some(transfer) = self.transfers.get_mut(&id) else { continue };
            for command in &mut transfer.commands {
                if let Waiting::Asked { answer: slot @ None, ids, .. } = command {
                    if ids.contains(&(id, cycle)) { *slot = Some(answer.clone()); found = true; self.removal_pending.insert(id); }
                }
            }
        }
        found
    }

    fn apply_retry_answer(&mut self, key: &str, id: TransferId, rep: ReplyId, ids: Vec<(TransferId, CycleId)>, answer: Option<RetryAnswer>, fx: &mut Fx) {
        for (member, _) in &ids {
            if let Some(transfer) = self.transfers.get_mut(member) { transfer.waits_outbox = false; }
        }
        let current = self.transfers.get(&id).map(|transfer| (transfer.stage, transfer.cycle_id));
        match (answer, current) {
            (Some(RetryAnswer::Current), Some((TransferStage::Mutating, own))) if ids.contains(&(id, own)) => reply(fx, rep, TransferAnswer::Receipt(self.receipt(id))),
            (Some(RetryAnswer::Refused(Fault::NotFound)), Some((TransferStage::Failed, _))) if self.transfers.get(&id).is_some_and(|transfer| !transfer.intent_live) => self.retry(key, None, rep, fx),
            (Some(RetryAnswer::Refused(fault)), _) => Self::refuse(fx, rep, fault),
            _ => Self::refuse(fx, rep, Fault::InFlight),
        }
    }

    /// `discard` and the expiry of a `Failed` transfer: the Outbox owns the operation of a saved or live intent, otherwise the transfer leaves alone.
    pub(super) fn discard_failed(&mut self, id: TransferId, waiter: Option<(ReplyId, bool)>, fx: &mut Fx) {
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
            Some((rep, true)) => transfer.cancel_replies.push((rep, true)),
            None => {}
        }
        if !std::mem::replace(&mut transfer.discard_sent, true) {
            let (transfer_id, cycle_id) = (transfer.transfer_id, transfer.cycle_id);
            fx.push(Effect::Send(Outbound::new(self.peers.outbox, OutboxInput::DiscardTransfer { transfer_id, cycle_id, reply: NO_REPLY })));
        }
    }

    /// The smallest `op_id` among the open transfers of the row decides: a transfer that does not follow the operation of Outbox answers the call itself.
    pub(super) fn select_row(&mut self, (model, rows): (String, Vec<String>), outbox_min: Option<(super::outbox::OpId, String)>, kind: super::outbox_types::SelectKind, rep: ReplyId, version: u64, fx: &mut Fx) {
        self.selecting.remove(&rep);
        let named = |key: &str| rows.iter().any(|row| key.strip_prefix(&format!("{model}:{row}:")).is_some());
        let first = self.scan().filter(|transfer| transfer.stage != TransferStage::Done && named(&transfer.key)).filter_map(|transfer| {
            let op_id = transfer.op_id.or_else(|| self.group_entry(transfer.transfer_id).and_then(|group| self.groups.get(&group)?.op_id))?;
            Some((op_id, transfer.transfer_id))
        }).min();
        match (first, outbox_min) {
            (Some((op_id, id)), min) if min.as_ref().is_none_or(|(smallest, _)| op_id <= *smallest) => {
                self.selecting.insert(rep, (id, kind));
                fx.push(Effect::Send(Outbound::new(self.peers.outbox, OutboxInput::SelectedTransfer { reply: rep, transfer_id: id, version })));
            }
            (_, Some((_, action))) => {
                let row_id = rows.into_iter().next().unwrap_or_default();
                fx.push(Effect::Send(Outbound::new(self.peers.outbox, OutboxInput::SelectedOutbox { model, row_id, action, kind, reply: rep, version })));
            }
            (_, None) => fx.push(Effect::Send(Outbound::new(self.peers.outbox, OutboxInput::SelectedNone { reply: rep, version }))),
        }
    }

    pub(super) fn retry(&mut self, key: &str, source: Option<SuppliedSource>, rep: ReplyId, fx: &mut Fx) {
        let Some(id) = self.open_id(key) else { return Self::refuse(fx, rep, Fault::NotFound) };
        if source.as_ref().is_some_and(super::transfer_accept::source_invalid) { return Self::refuse(fx, rep, Fault::Invalid); }
        if let Some(transfer) = self.transfers.get_mut(&id).filter(|transfer| transfer.stage == TransferStage::PersistingFailure) {
            transfer.commands.push(Waiting::Retry(source, rep));
            self.removal_pending.insert(id);
            return;
        }
        let undeclared = self.transfers.get(&id).filter(|transfer| self.complete && transfer.stage == TransferStage::Failed && !self.declarations.contains(&(transfer.mutation.model.clone(), transfer.mutation.action.clone()))).map(|transfer| super::failure::undeclared_upload_message(&transfer.mutation.model, &transfer.mutation.action));
        if let Some(text) = undeclared { return Self::refuse(fx, rep, Fault::Rejected(crate::owners::contract::RejectReason::Invalid(text))); }
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
        if source.is_none() && self.transfers.get(&id).is_some_and(|transfer| transfer.stage == TransferStage::Mutating) {
            let ids: Vec<TransferId> = match self.group_entry(id).filter(|group| self.phase_of(*group) == Some(BatchPhase::Mutating)) {
                Some(group) => self.remaining(group).into_iter().filter(|member| self.stage_of(*member) == Some(TransferStage::Mutating)).collect(),
                None => vec![id],
            };
            let asking = ids.iter().any(|member| self.transfers.get(member).is_some_and(|transfer| transfer.commands.iter().any(|command| matches!(command, Waiting::Asked { answer: None, .. }))));
            let asked: Vec<(TransferId, CycleId)> = if asking { self.transfers.get(&id).map(|transfer| vec![(id, transfer.cycle_id)]).unwrap_or_default() } else { ids.iter().filter_map(|member| self.transfers.get(member)).map(|transfer| (transfer.transfer_id, transfer.cycle_id)).collect() };
            if let Some(transfer) = self.transfers.get_mut(&id) { transfer.commands.push(Waiting::Asked { rep, ids: asked, answer: None }); }
            self.removal_pending.insert(id);
            if !asking { self.request_mutation_retry(&ids, fx); }
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
            TransferStage::Failed if transfer.prepare.is_some() && source.is_some() => Self::refuse(fx, rep, Fault::Invalid),
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

    pub(super) fn answer_joined(&mut self, id: TransferId, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        let mut joined = Vec::new();
        while matches!(transfer.commands.first(), Some(Waiting::Retry(None, _))) {
            if let Waiting::Retry(_, rep) = transfer.commands.remove(0) { joined.push(rep); }
        }
        for rep in joined { reply(fx, rep, TransferAnswer::Receipt(self.receipt(id))); }
    }

    pub(super) fn refuse_joined(&mut self, id: TransferId, fault: &Fault, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        let mut joined = Vec::new();
        while matches!(transfer.commands.first(), Some(Waiting::Retry(..))) {
            if let Waiting::Retry(_, rep) = transfer.commands.remove(0) { joined.push(rep); }
        }
        for rep in joined { Self::refuse(fx, rep, fault.clone()); }
    }

    pub(super) fn retry_committed(&mut self, now: crate::runtime::Time, id: TransferId, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        transfer.cycle_id += 1;
        transfer.stage = TransferStage::Ready;
        transfer.error = None;
        transfer.failed_at = None;
        let (rep, preparing) = (transfer.retry_reply.take(), transfer.prepare.is_some());
        let held = transfer.held && transfer.next_step == NextStep::Complete;
        if held { transfer.stage = TransferStage::Held; }
        let receipt = self.receipt(id);
        if let Some(rep) = rep {
            reply(fx, rep, TransferAnswer::Receipt(receipt));
        }
        self.answer_joined(id, fx);
        if held {
            self.show(id, Shown::Uploading { progress: 99 }, fx);
            return self.rounds_idle(id, fx);
        }
        if !preparing && self.transfers.get(&id).is_some_and(|transfer| transfer.over_limit(transfer.size)) { return self.fail_too_large(now, id, fx); }
        if preparing { self.queue_prepare(id, fx); } else { self.show_queued(id, fx); }
        self.rounds_needed(vec![id], fx);
        self.start_in_order(now, fx);
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
        self.answer_joined(id, fx);
        if let Some(group) = group { self.single_next(group, fx); }
        else { self.store(&[id], StoreKind::BatchIntent, fx); }
    }

    pub(super) fn retry_rejected(&mut self, id: TransferId, reason: RejectReason, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        transfer.stage = TransferStage::Failed;
        let fault = Fault::of_reject(reason);
        if let Some(rep) = transfer.retry_reply.take() {
            Self::refuse(fx, rep, fault.clone());
        }
        self.refuse_joined(id, &fault, fx);
    }

    pub(super) fn failure_committed(&mut self, now: crate::runtime::Time, id: TransferId, fx: &mut Fx) {
        self.leave_retry(id, fx);
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
        self.show(id, Shown::Failed { error: error.clone() }, fx);
        self.settle(id, OutboxOutcome::Failed { error }, fx);
        if had_source { self.rounds_idle(id, fx); }
        self.batch_after_removal(now, id, fx);
        self.send_orphans(false, fx);
    }

    pub(super) fn failure_rejected(&mut self, kind: StoreKind, ids: &[TransferId], fx: &mut Fx) { self.storage_retry_batch(kind, ids, fx); }

    fn answer_removed(cancels: Vec<(ReplyId, bool)>, fx: &mut Fx) {
        for (rep, cancel) in cancels { reply(fx, rep, if cancel { TransferAnswer::Cancelled(true) } else { TransferAnswer::Ok }); }
    }

    fn removal_replies(&mut self, id: TransferId, removal: &Removal, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get_mut(&id) else { return };
        let cancels = std::mem::take(&mut transfer.cancel_replies);
        let accept = transfer.accept_reply.take();
        let waiters = std::mem::take(&mut transfer.discard_waiters);
        let mut cancels = cancels;
        let mut kept = Vec::new();
        for command in std::mem::take(&mut transfer.commands) {
            match command {
                Waiting::Removal(rep, cancel) => cancels.push((rep, cancel)),
                Waiting::Retry(_, rep) | Waiting::Asked { rep, .. } => Self::refuse(fx, rep, Fault::NotFound),
                other => kept.push(other),
            }
        }
        transfer.commands = kept;
        let group = transfer.batch_id.and_then(|group| self.groups.get_mut(&group).and_then(|group| group.reply.take()));
        match removal {
            Removal::Cancel => {
                let cause = if cancels.iter().any(|(_, cancelled)| *cancelled) { crate::ffi::EndCause::Cancelled } else { crate::ffi::EndCause::Discarded };
                for (rep, cancel) in cancels {
                    reply(fx, rep, if cancel { TransferAnswer::Cancelled(true) } else { TransferAnswer::Ok });
                }
                if let Some(rep) = accept {
                    Self::refuse(fx, rep, Fault::Ended(cause));
                }
            }
            Removal::Confirmed { .. } => {
                for (rep, cancel) in cancels {
                    reply(fx, rep, if cancel { TransferAnswer::Cancelled(false) } else { TransferAnswer::Err(Fault::NotFound) });
                }
            }
            Removal::Discarded => {
                for rep in waiters {
                    reply(fx, rep, TransferAnswer::Ok);
                }
                for (rep, _) in cancels {
                    reply(fx, rep, TransferAnswer::Cancelled(true));
                }
            }
            Removal::CopyFailed { error } => {
                if let Some(rep) = accept.or(group) {
                    Self::refuse(fx, rep, Fault::Copy(error.clone()));
                }
                Self::answer_removed(cancels, fx);
            }
            Removal::Storage => {
                if let Some(rep) = accept.or(group) {
                    Self::refuse(fx, rep, Fault::Rejected(RejectReason::Storage("storage".into())));
                }
                Self::answer_removed(cancels, fx);
            }
            Removal::Invalid { error } => {
                if let Some(rep) = accept.or(group) {
                    Self::refuse(fx, rep, Fault::Rejected(RejectReason::Invalid(error.clone())));
                }
                Self::answer_removed(cancels, fx);
            }
        }
    }

    pub(super) fn removing_committed(&mut self, now: crate::runtime::Time, ids: &[TransferId], fx: &mut Fx) {
        for &id in ids {
            let Some(removal) = self.transfers.get_mut(&id).and_then(|transfer| transfer.removal.take()) else { continue };
            self.removal_replies(id, &removal, fx);
            if let Some(transfer) = self.transfers.get(&id) {
                if let Some(upload_id) = Self::removal_orphan(transfer, &removal) {
                    self.orphans.insert(upload_id, transfer.key.clone());
                }
            }
            match removal.clone() {
                Removal::Cancel | Removal::Discarded => self.settle(id, OutboxOutcome::Discarded, fx),
                Removal::Confirmed { revision, result, row_id } => self.settle(id, OutboxOutcome::Confirmed { revision, result, row_id }, fx),
                Removal::CopyFailed { .. } | Removal::Storage | Removal::Invalid { .. } => run(fx, TransferEffect::DeleteFile { path: file_id(id) }),
            }
            if let Some((file, _)) = self.transfers.get(&id).and_then(|transfer| transfer.keep_prepared.as_ref()) {
                run(fx, TransferEffect::KeepPrepared { transfer_id: id, file: file.clone() });
            } else if matches!(removal, Removal::Cancel | Removal::Discarded | Removal::Confirmed { .. }) && self.transfers.get(&id).is_some_and(|transfer| transfer.source.is_some() || transfer.restore_element.as_ref().is_some_and(|(_, holder)| holder.is_some())) {
                self.delete_copy(id, fx);
            }
            self.finish(id, fx);
            self.batch_after_removal(now, id, fx);
            self.drop_transfer(id, fx);
        }
        self.send_orphans(false, fx);
        self.start_in_order(now, fx);
    }

    /// A transfer in its terminal stage leaves the table in the step that ended it: its outcomes are the records of the Store.
    pub(super) fn drop_transfer(&mut self, id: TransferId, fx: &mut Fx) {
        let Some(transfer) = self.transfers.remove(&id) else { return };
        if transfer.prepare.is_some() { self.free_prepare_slot(fx); }
        if let Some(group_id) = transfer.batch_id {
            if let Some(group) = self.groups.get_mut(&group_id) { group.ids.retain(|member| *member != id); }
            if self.phase != Phase::Restoring && self.groups.get(&group_id).is_some_and(|group| group.ids.is_empty()) { self.groups.remove(&group_id); }
        }
    }

    pub(super) fn removing_rejected(&mut self, now: crate::runtime::Time, kind: StoreKind, ids: &[TransferId], reason: RejectReason, fx: &mut Fx) {
        if let RejectReason::Storage(_) = reason {
            return self.storage_retry_batch(kind, ids, fx);
        }
        if !matches!(reason, RejectReason::Invalid(_)) {
            for &id in ids {
                self.interrupt(id, super::outbox_types::end_cause(&reason), fx);
            }
            return;
        }
        let mut answered = Vec::new();
        for &id in ids {
            let Some(transfer) = self.transfers.get_mut(&id) else { continue };
            match (transfer.prior, transfer.removal.clone()) {
                (Some(prior), Some(Removal::Cancel)) => {
                    transfer.stage = prior;
                    transfer.prior = None;
                    transfer.removal = None;
                    for (rep, _) in std::mem::take(&mut transfer.cancel_replies) {
                        Self::refuse(fx, rep, Fault::of_reject(reason.clone()));
                    }
                    if prior == TransferStage::Ready {
                        self.show_queued(id, fx);
                    }
                    if prior == TransferStage::Held {
                        self.show(id, Shown::Uploading { progress: 99 }, fx);
                    }
                    if prior == TransferStage::BatchReady {
                        self.show(id, Shown::Uploading { progress: 100 }, fx);
                    }
                }
                (_, Some(removal)) => {
                    transfer.removal = None;
                    self.removal_replies(id, &removal, fx);
                    match removal {
                        Removal::Cancel | Removal::Discarded => self.settle(id, OutboxOutcome::Discarded, fx),
                        Removal::Confirmed { revision, result, row_id } => self.settle(id, OutboxOutcome::Confirmed { revision, result, row_id }, fx),
                        Removal::CopyFailed { .. } | Removal::Storage | Removal::Invalid { .. } => {}
                    }
                    answered.push(id);
                }
                (_, None) => {}
            }
        }
        for id in answered {
            self.finish(id, fx);
            self.batch_after_removal(now, id, fx);
        }
        self.start_in_order(now, fx);
    }

    /// `Online(true)` asks for the check of the `Failed` transfers at once.
    pub(super) fn expire_failed(&mut self, fx: &mut Fx) {
        if self.scan().any(|transfer| transfer.stage == TransferStage::Failed) {
            self.schedule_expire(fx);
        }
    }

    /// `Tick(expire)`: a `Failed` transfer older than 7 days is discarded.
    pub(super) fn expire(&mut self, now: crate::runtime::Time, fx: &mut Fx) {
        self.send_batch(vec![crate::owners::store::StoreOp::PreparedCacheExpire { now: now.0 }], fx);
        let old: Vec<TransferId> = self.scan().filter(|transfer| transfer.stage == TransferStage::Failed && transfer.failed_at.is_some_and(|failed_at| now.0.saturating_sub(failed_at.0) > EXPIRY_MS)).map(|transfer| transfer.transfer_id).collect();
        for id in old {
            self.discard_failed(id, None, fx);
        }
    }

    pub(super) fn outcome(&mut self, receipt: Receipt, rep: ReplyId, discard_on_failure: bool, fx: &mut Fx) {
        let Some(transfer) = self.transfers.get_mut(&receipt.op_id) else { return run(fx, TransferEffect::ReadOutcome { reply: rep, id: receipt.op_id, cycle_id: receipt.cycle_id }) };
        if self.confirmed_discards.get(&receipt.op_id).is_some_and(|(cycle, _)| *cycle == receipt.cycle_id) {
            transfer.outcome_waiters.push((receipt.cycle_id, rep, discard_on_failure));
            return;
        }
        if let Some(found) = transfer.outcomes.get(&receipt.cycle_id) {
            if discard_on_failure && receipt.cycle_id == transfer.cycle_id && transfer.stage == TransferStage::Failed {
                if let OutboxOutcome::Failed { error } = found {
                    let error = error.clone();
                    transfer.outcome_waiters.push((receipt.cycle_id, rep, true));
                    self.confirmed_discards.insert(receipt.op_id, (receipt.cycle_id, error));
                    self.discard_failed(receipt.op_id, None, fx);
                    return;
                }
            }
            return reply(fx, rep, TransferAnswer::Outcome(found.clone(), None));
        }
        if receipt.cycle_id == transfer.cycle_id && transfer.stage != TransferStage::Done {
            transfer.outcome_waiters.push((receipt.cycle_id, rep, discard_on_failure));
        } else {
            Self::refuse(fx, rep, Fault::NotFound);
        }
    }

    pub(super) fn interest_plus(&mut self, key: Key, stream_id: StreamId, fx: &mut Fx) {
        let (shown, revision) = self.published_by_key(&key);
        let figures = self.figures_by_key(&key, &shown);
        self.subscribers.entry(key.clone()).or_default().insert(stream_id, (shown.clone(), revision));
        run(fx, TransferEffect::Status { stream_id, key, shown, figures, revision });
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

    pub(super) fn epoch_ended(&mut self, cause: crate::runtime::Cause, fx: &mut Fx) {
        self.ended = true;
        let end = crate::ffi::EndCause::of(cause);
        for (reply_id, _) in std::mem::take(&mut self.selecting) { reply(fx, reply_id, TransferAnswer::Err(Fault::Ended(end.clone()))); }
        let unanswered: Vec<(String, String)> = self.pending.values().filter(|pending| matches!(pending.kind, StoreKind::UploadId | StoreKind::LateOrphan)).filter_map(|pending| self.debt_of(pending).map(|(upload_id, key, _)| (upload_id, key))).collect();
        let active: Vec<(String, String)> = if cause == crate::runtime::Cause::Close { Vec::new() } else {
            self.scan().filter(|transfer| transfer.stage != TransferStage::Done).filter_map(|transfer| Self::orphan_of(transfer).map(|upload_id| (upload_id, transfer.key.clone()))).filter(|(upload_id, _)| !self.orphans.contains_key(upload_id)).collect()
        };
        self.pending.retain(|_, pending| pending.kind == StoreKind::ControlOrphan);
        self.retrying.clear();
        for (upload_id, key) in unanswered.into_iter().chain(active) { self.minted.entry(upload_id).or_insert(MintedDebt { transfer: None, key, attempts: 1, attempt_id: 0, record: MintedRecord::Orphan }); }
        let done: Vec<String> = self.minted.iter().filter(|(upload_id, debt)| !self.debt_live(upload_id, debt.transfer)).map(|(upload_id, _)| upload_id.clone()).collect();
        for upload_id in done { self.minted.remove(&upload_id); }
        let owed: Vec<(String, String)> = self.minted.iter().filter(|(upload_id, debt)| debt.record != MintedRecord::Recorded && !self.record_in_flight(upload_id)).map(|(upload_id, debt)| (upload_id.clone(), debt.key.clone())).collect();
        for (upload_id, key) in owed { self.write_control_orphan(upload_id, key, fx); }
        for id in self.open_ids() {
            self.interrupt(id, end.clone(), fx);
        }
        self.answer_held(if end == crate::ffi::EndCause::Closed { crate::ffi::EndCause::Closed } else { crate::ffi::EndCause::SessionEnded }, fx);
    }

    pub(super) fn close(&mut self, fx: &mut Fx) {
        self.closing = true;
        for (reply_id, _) in std::mem::take(&mut self.selecting) { reply(fx, reply_id, TransferAnswer::Err(Fault::Ended(crate::ffi::EndCause::Closed))); }
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
        self.answer_held(crate::ffi::EndCause::Closed, fx);
        self.maybe_drained(fx);
    }
}
