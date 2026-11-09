use super::*;

pub fn tx_store(g: &mut G, t: &mut Transfer, i: &Input, e: &mut Vec<Effect>) {
    let st = t.st.clone();
    match i {
        Input::Committed { .. } => store_committed(g, t, st, e),
        Input::Rejected { reason, .. } => store_rejected(g, t, st, reason, e),
        _ => {}
    }
}

pub fn end_interrupted(g: &mut G, t: &mut Transfer, e: &mut Vec<Effect>, name: &'static str, cause: EndCause) {
    row(e, name);
    free_held(t, e);
    t.st = St::Done;
    interrupt_all(g, t, e, cause);
    let c = t.cycle_id;
    t.outcomes.entry(c).or_insert(Outcome::Ended(cause));
}

pub fn store_rejected(g: &mut G, t: &mut Transfer, st: St, reason: &Reason, e: &mut Vec<Effect>) {
    let ended = matches!(reason, Reason::NoSession | Reason::Closed);
    let cause = if matches!(reason, Reason::Closed) { EndCause::Closed } else { EndCause::SessionEnded };
    match st {
        St::Accepting { closing } => {
            let interrupted = closing || ended || matches!(reason, Reason::StaleGeneration(_));
            row(e, if closing { "xfer_accepting_closing" } else if interrupted { "xfer_accepting_rejected_interrupted" } else { "xfer_accepting_rejected" });
            e.push(Effect::DeleteFile { transfer_id: t.id });
            if interrupted { reply_accept(g, t, e, Answer::Err(Fault::Ended(cause))); } else { reply_accept(g, t, e, Answer::Err(Fault::Reason(reason.clone()))); }
            t.st = St::Done;
        }
        St::PersistingAttempt { .. } => {
            if ended { end_interrupted(g, t, e, "xfer_attempt_ended", cause); }
            else if matches!(reason, Reason::Storage | Reason::Invalid) {
                row(e, "xfer_attempt_rejected");
                let error = if *reason == Reason::Storage { fail(ErrKind::Storage, "storage") } else { fail(ErrKind::InternalInvalid, "invalid") };
                t.st = St::PersistingFailure { error, abort: None };
                commit(g, t, e, vec![Op::Failed { clear_upload: false }]);
            }
        }
        St::PersistingCopy { supply: Some(back), .. } => {
            if ended { end_interrupted(g, t, e, "xfer_supply_end", cause); }
            else if *reason == Reason::Storage {
                row(e, "xfer_supply_rejected");
                e.push(Effect::DeleteFile { transfer_id: t.id });
                if let Some((r, _)) = t.replies.supply.take() { reply(e, r, Answer::Err(Fault::Storage)); }
                t.st = match back { Back::AwaitingSource => St::AwaitingSource, Back::Failed => St::Failed };
            } else {
                row(e, "xfer_persisting_invalid");
                t.st = St::PersistingFailure { error: fail(ErrKind::InternalInvalid, "invalid"), abort: None };
                commit(g, t, e, vec![Op::Failed { clear_upload: false }]);
            }
        }
        St::PersistingCopy { .. } => {
            if ended { end_interrupted(g, t, e, "xfer_removing_no_session", cause); }
            else if *reason == Reason::Storage {
                row(e, "xfer_persisting_copy_rejected");
                let prior = t.st.clone();
                t.st = St::Removing { why: Why::Storage, prior: Box::new(prior), orphan: None };
                let mut ops = vec![Op::RemoveUpload { transfer_id: t.id }];
                ops.extend(rollback_op(t));
                cm(g, t, e, "rs", ops);
            } else {
                row(e, "xfer_persisting_invalid");
                t.st = St::PersistingFailure { error: fail(ErrKind::InternalInvalid, "invalid"), abort: None };
                commit(g, t, e, vec![Op::Failed { clear_upload: false }]);
            }
        }
        St::PersistingRetry { .. } => {
            if ended { end_interrupted(g, t, e, "xfer_removing_no_session", cause); }
            else if *reason == Reason::Storage {
                row(e, "xfer_persisting_retry_rejected");
                t.st = St::Failed;
                if let Some(r) = t.replies.retry.take() { reply(e, r, Answer::Err(Fault::Storage)); }
            } else {
                row(e, "xfer_persisting_invalid");
                t.st = St::PersistingFailure { error: fail(ErrKind::InternalInvalid, "invalid"), abort: None };
                commit(g, t, e, vec![Op::Failed { clear_upload: false }]);
            }
        }
        St::PersistingPart { .. } | St::PersistingComplete => {
            if ended { end_interrupted(g, t, e, "xfer_removing_no_session", cause); }
            else if *reason == Reason::Storage {
                row(e, "xfer_persisting_storage_failure");
                t.st = St::PersistingFailure { error: fail(ErrKind::Storage, "storage"), abort: None };
                commit(g, t, e, vec![Op::Failed { clear_upload: false }]);
            } else {
                row(e, "xfer_persisting_invalid");
                t.st = St::PersistingFailure { error: fail(ErrKind::InternalInvalid, "invalid"), abort: None };
                commit(g, t, e, vec![Op::Failed { clear_upload: false }]);
            }
        }
        St::PersistingFailure { error, abort } => {
            if ended { end_interrupted(g, t, e, "xfer_removing_no_session", cause); }
            else if *reason == Reason::Storage {
                row(e, "xfer_persisting_failure_storage");
                let na = g.next_attempt;
                g.next_attempt += 1;
                t.write_retry = Some(na);
                e.push(Effect::Schedule { kind: TickKind::Retry { transfer_id: t.id, attempt_id: na }, delay: Delay::Backoff { attempt: 1 } });
                t.st = St::PersistingFailure { error, abort };
            } else {
                row(e, "xfer_persisting_failure_invalid");
                t.st = St::Failed;
                t.error = Some(error.clone());
                fix_outcome(t, e, Outcome::Failed { error });
                e.push(Effect::Rounds(RoundsIn::Idle { epoch: t.epoch, transfer_id: t.id }));
            }
        }
        St::Removing { prior, .. } => {
            if ended { end_interrupted(g, t, e, "xfer_removing_no_session", cause); reply_none(); }
            else if *reason == Reason::Storage {
                row(e, "xfer_removing_rejected");
                if let Some((r, _)) = t.replies.cancel.take() { reply(e, r, Answer::Err(Fault::Storage)); }
                for r in std::mem::take(&mut t.replies.discard_waiters) { reply_vec(e, r, Fault::Storage); }
                if matches!(*prior, St::Ready { .. }) { e.push(Effect::SelfSlotFreed); }
                t.st = *prior;
            } else {
                row(e, "xfer_persisting_invalid");
                t.st = St::PersistingFailure { error: fail(ErrKind::InternalInvalid, "invalid"), abort: None };
                commit(g, t, e, vec![Op::Failed { clear_upload: false }]);
            }
        }
        St::PersistingBatchIntent { single, .. } => {
            if ended { end_interrupted(g, t, e, "xfer_batch_intent_ended", cause); }
            else if *reason == Reason::Storage {
                row(e, "xfer_batch_intent_rejected");
                let na = g.next_attempt;
                g.next_attempt += 1;
                t.st = St::PersistingBatchIntent { single, retry: Some(na) };
                e.push(Effect::Schedule { kind: TickKind::Retry { transfer_id: t.id, attempt_id: na }, delay: Delay::Backoff { attempt: 1 } });
            } else {
                row(e, "xfer_batch_intent_invalid");
                t.st = St::PersistingFailure { error: fail(ErrKind::InternalInvalid, "invalid"), abort: None };
                commit(g, t, e, vec![Op::Failed { clear_upload: false }]);
            }
        }
        other => row(e, stray_row(&other)),
    }
}

pub fn reply_none() {}
pub fn reply_vec(e: &mut Vec<Effect>, r: ReplyId, f: Fault) { e.push(Effect::Reply(r, Answer::Err(f))); }

pub fn store_committed(g: &mut G, t: &mut Transfer, st: St, e: &mut Vec<Effect>) {
    match st {
        St::Accepting { closing } => {
            if closing {
                row(e, "xfer_accepting_closing");
                e.push(Effect::DeleteFile { transfer_id: t.id });
                reply_accept(g, t, e, Answer::Err(Fault::Ended(EndCause::Closed)));
                t.st = St::Done;
                return;
            }
            match t.source.clone() {
                Source::Pending => {
                    row(e, "xfer_pending_accepted");
                    t.st = St::AwaitingSource;
                    e.push(Effect::Schedule { kind: TickKind::Expire, delay: Delay::Now });
                    match t.batch {
                        None => { if let Some(r) = t.replies.accept.take() { reply(e, r, Answer::Receipt(receipt(t))); } }
                        Some(gid) => g.post.push(Post::Receipts(gid)),
                    }
                }
                Source::File { .. } => {
                    row(e, "xfer_accepting_committed");
                    let a = g.next_attempt;
                    g.next_attempt += 1;
                    t.st = St::Copying { attempt: a, supply: None, new_cycle: false };
                    e.push(Effect::Copy { transfer_id: t.id, attempt_id: a, source: t.source.clone() });
                    g.round_needed.push(t.id);
                }
            }
        }
        St::PersistingCopy { supply: None, cached, .. } => {
            t.local_url = true;
            t.copy_present = true;
            match (t.batch, cached) {
                (None, true) => {
                    row(e, "xfer_persisting_copy_cached_committed");
                    t.st = St::Mutating;
                    t.intent_live = true;
                    if let Some(r) = t.replies.accept.take() { reply(e, r, Answer::Receipt(receipt(t))); }
                    e.push(Effect::Outbox(OutboxEffect::Submit { op_id: t.op_id.unwrap_or(0), origin: OpOrigin::Transfer { transfer_id: t.id } }));
                }
                (None, false) => {
                    row(e, "xfer_persisting_copy_committed");
                    t.cycle_id = 1;
                    t.st = St::Ready { next: Next::Part(1) };
                    if let Some(r) = t.replies.accept.take() { reply(e, r, Answer::Receipt(receipt(t))); }
                    e.push(Effect::SelfSlotFreed);
                }
                (Some(gid), c) => {
                    row(e, "xfer_persisting_copy_committed");
                    t.cycle_id = 1;
                    t.st = if c { St::BatchReady } else { St::Ready { next: Next::Part(1) } };
                    e.push(Effect::SelfSlotFreed);
                    g.post.push(Post::Receipts(gid));
                    if c { g.post.push(Post::GroupCheck { gid, from_complete: false, removal: false }); }
                }
            }
        }
        St::PersistingCopy { supply: Some(_), cached, new_cycle } => {
            t.local_url = true;
            if new_cycle { free_held(t, e); t.cycle_id += 1; }
            let ans = if new_cycle { Answer::Receipt(receipt(t)) } else { Answer::Ok };
            if !cached {
                row(e, "xfer_supply_committed");
                t.st = St::Ready { next: Next::Part(1) };
                if let Some((r, _)) = t.replies.supply.take() { reply(e, r, ans); }
                e.push(Effect::SelfSlotFreed);
            } else if t.batch.is_none() {
                row(e, "xfer_supply_cached_committed");
                t.st = St::Mutating;
                t.intent_live = true;
                if let Some((r, _)) = t.replies.supply.take() { reply(e, r, ans); }
                e.push(Effect::Outbox(OutboxEffect::Submit { op_id: t.op_id.unwrap_or(0), origin: OpOrigin::Transfer { transfer_id: t.id } }));
            } else {
                row(e, "xfer_supply_cached_batch");
                t.st = St::BatchReady;
                if let Some((r, _)) = t.replies.supply.take() { reply(e, r, ans); }
                e.push(Effect::SelfSlotFreed);
                if let Some(gid) = t.batch { g.post.push(Post::GroupCheck { gid, from_complete: false, removal: false }); }
            }
        }
        St::PersistingRetry { next, recreate, wait_outbox } => {
            if wait_outbox { row(e, stray_row(&St::PersistingRetry { next, recreate, wait_outbox })); return; }
            row(e, "xfer_persisting_retry_committed");
            free_held(t, e);
            t.cycle_id += 1;
            t.attempts = 0;
            t.complete_attempts = 0;
            t.error = None;
            if next == Next::Mutation && recreate {
                let na = g.next_attempt;
                g.next_attempt += 1;
                let _ = na;
                t.st = St::PersistingBatchIntent { single: true, retry: None };
                if let Some(r) = t.replies.retry.take() { reply(e, r, Answer::Receipt(receipt(t))); }
                commit(g, t, e, vec![Op::Intent { op_id: t.op_id.unwrap_or(0), origin: OpOrigin::Transfer { transfer_id: t.id } }]);
            } else {
                t.st = St::Ready { next };
                if let Some(r) = t.replies.retry.take() { reply(e, r, Answer::Receipt(receipt(t))); }
                e.push(Effect::SelfSlotFreed);
                g.round_needed.push(t.id);
            }
        }
        St::PersistingFailure { error, abort } => {
            row(e, "xfer_persisting_failure_committed");
            t.st = St::Failed;
            t.error = Some(error.clone());
            fix_outcome(t, e, Outcome::Failed { error });
            if let Some((r, _)) = t.replies.supply.take() { reply(e, r, Answer::Ok); }
            if let Some(u) = abort {
                g.orphans.insert(u, t.key.clone());
                t.upload_id = None;
                t.confirmed.clear();
                t.aborted = true;
                dispatch_aborts(g, e);
            }
            e.push(Effect::Rounds(RoundsIn::Idle { epoch: t.epoch, transfer_id: t.id }));
        }
        St::Removing { why, orphan, .. } => removing_committed(g, t, why, orphan, e),
        St::PersistingAttempt { step, attempt } => {
            if g.online == Some(true) && !g.saving {
                row(e, "xfer_attempt_committed");
                let req = req_id_of(t, step);
                match step {
                    StepKind::Complete => {
                        t.st = St::Completing { attempt, waiting_retry: false };
                        e.push(Effect::CompleteRequest { transfer_id: t.id, attempt_id: attempt, req_id: req, parts: t.confirmed.clone() });
                    }
                    _ => {
                        t.st = St::Uploading { step, attempt, waiting_retry: false };
                        e.push(Effect::Send { transfer_id: t.id, attempt_id: attempt, step, req_id: req, net_purpose_transfer: true });
                        e.push(Effect::Schedule { kind: TickKind::PutTimeout { transfer_id: t.id, attempt_id: attempt }, delay: Delay::PutTimeout });
                    }
                }
            } else {
                row(e, "xfer_attempt_committed_wait");
                t.st = St::Ready { next: step_kind_next(step) };
            }
        }
        St::PersistingPart { last, .. } => {
            if last { row(e, "xfer_persisting_part_last"); t.st = St::Ready { next: Next::Complete }; }
            else { row(e, "xfer_persisting_part_committed"); t.st = St::Ready { next: first_unconfirmed(t) }; }
            e.push(Effect::SelfSlotFreed);
        }
        St::PersistingComplete => {
            match t.batch {
                None => {
                    row(e, "xfer_persisting_complete_committed");
                    t.st = St::Mutating;
                    t.intent_live = true;
                    e.push(Effect::Outbox(OutboxEffect::Submit { op_id: t.op_id.unwrap_or(0), origin: OpOrigin::Transfer { transfer_id: t.id } }));
                    e.push(Effect::SelfSlotFreed);
                }
                Some(gid) => {
                    t.st = St::BatchReady;
                    e.push(Effect::SelfSlotFreed);
                    g.post.push(Post::GroupCheck { gid, from_complete: true, removal: false });
                }
            }
        }
        St::PersistingBatchIntent { single, .. } => {
            row(e, "xfer_batch_intent_committed");
            t.st = St::Mutating;
            t.intent_live = true;
            if single {
                if let Some(gid) = t.batch { if let Some(gr) = g.groups.get_mut(&gid) { gr.phase = GroupPhase::Singles; } }
                e.push(Effect::Outbox(OutboxEffect::Submit { op_id: t.op_id.unwrap_or(0), origin: OpOrigin::Transfer { transfer_id: t.id } }));
            } else if let Some(gid) = t.batch {
                let mut submit = None;
                if let Some(gr) = g.groups.get_mut(&gid) {
                    gr.phase = GroupPhase::Mutating;
                    if gr.submit_pending { gr.submit_pending = false; submit = Some((gr.pack_op.unwrap_or(0), gr.pack_ids.clone())); }
                }
                if let Some((op, ids)) = submit { e.push(Effect::Outbox(OutboxEffect::Submit { op_id: op, origin: OpOrigin::TransferGroup { batch_id: gid, transfer_ids: ids } })); }
            }
            e.push(Effect::SelfSlotFreed);
        }
        other => row(e, stray_row(&other)),
    }
}

pub fn removing_committed(g: &mut G, t: &mut Transfer, why: Why, orphan: Option<String>, e: &mut Vec<Effect>) {
    let had_effects = t.has_effects;
    let is_conf = matches!(why, Why::Confirmed(_));
    let cancel = t.replies.cancel.take();
    let waiters = std::mem::take(&mut t.replies.discard_waiters);
    match why {
        Why::Cancel => {
            row(e, "xfer_removing_committed_cancel");
            if t.copy_present { e.push(Effect::DeleteFile { transfer_id: t.id }); }
            e.push(Effect::SelfSlotFreed);
            e.push(Effect::Rounds(RoundsIn::Idle { epoch: t.epoch, transfer_id: t.id }));
            if let Some((r, c)) = cancel { reply(e, r, ans_ok(c)); }
            for r in waiters { reply(e, r, Answer::Ok); }
            reply_accept(g, t, e, Answer::Err(Fault::Ended(EndCause::Cancelled)));
            fix_outcome(t, e, Outcome::Discarded);
        }
        Why::Confirmed(rev) => {
            row(e, "xfer_removing_committed_confirmed");
            if t.copy_present { e.push(Effect::DeleteFile { transfer_id: t.id }); }
            e.push(Effect::Rounds(RoundsIn::Idle { epoch: t.epoch, transfer_id: t.id }));
            fix_outcome(t, e, Outcome::Confirmed { revision: rev });
        }
        Why::Discarded => {
            row(e, "xfer_removing_committed_discarded");
            if t.copy_present { e.push(Effect::DeleteFile { transfer_id: t.id }); }
            for r in waiters { reply(e, r, Answer::Ok); }
            if let Some((r, c)) = cancel { reply(e, r, ans_ok(c)); }
            fix_outcome(t, e, Outcome::Discarded);
        }
        Why::CopyFailed(err) => {
            row(e, "xfer_removing_committed_copy_failed");
            e.push(Effect::DeleteFile { transfer_id: t.id });
            reply_accept(g, t, e, Answer::Err(Fault::Copy(err)));
        }
        Why::Storage => {
            row(e, "xfer_removing_committed_copy_failed");
            e.push(Effect::DeleteFile { transfer_id: t.id });
            reply_accept(g, t, e, Answer::Err(Fault::Storage));
        }
    }
    if had_effects && !is_conf { row(e, "xfer_remove_rolls_back_optimistic"); }
    t.local_url = false;
    t.st = St::Done;
    if let Some(u) = orphan { g.orphans.insert(u, t.key.clone()); dispatch_aborts(g, e); }
    remove_from_group(g, t);
}
