use super::*;

pub fn begin_remove(g: &mut G, t: &mut Transfer, e: &mut Vec<Effect>, why: Why, cancel: Option<(ReplyId, bool)>, intent: bool, with_orphan: bool, slot_freed: bool, tag: Option<&str>) {
    if let St::Uploading { attempt, waiting_retry: false, .. } | St::Completing { attempt, waiting_retry: false } = t.st {
        if matches!(t.st, St::Uploading { .. }) { row(e, "xfer_put_cancelled"); }
        e.push(Effect::TransportCancel { transfer_id: t.id, attempt_id: attempt });
        cancel_put(e, t.id, attempt);
    }
    let orphan = if with_orphan { t.upload_id.clone() } else { None };
    if let Some(c) = cancel { t.replies.cancel = Some(c); }
    free_held(t, e);
    let mut ops = vec![Op::RemoveUpload { transfer_id: t.id }];
    if intent { ops.push(Op::RemoveIntent); }
    if let Some(u) = &orphan { ops.push(Op::UploadOrphan { upload_id: u.clone(), key: t.key.clone() }); }
    if matches!(why, Why::Cancel | Why::CopyFailed(_) | Why::Storage) { ops.extend(rollback_op(t)); }
    if let Some(gid) = t.batch {
        if matches!(why, Why::Cancel) {
            if let Some(gr) = g.groups.get(&gid) {
                if gr.phase == GroupPhase::Collecting {
                    ops.push(Op::ItemsMinus { batch_id: gid, transfer_id: t.id });
                    let remaining = gr.items.iter().filter(|x| **x != t.id).count();
                    if remaining >= gr.minimum && remaining > 0 { ops.push(Op::Renumber { batch_id: gid, start: gr.renumber_start, size: remaining }); }
                }
            }
        }
    }
    let prior = t.st.clone();
    t.st = St::Removing { why, prior: Box::new(prior), orphan };
    match tag { Some(p) => cm(g, t, e, p, ops), None => { commit(g, t, e, ops); } }
    if slot_freed { e.push(Effect::SelfSlotFreed); }
}

pub fn ans_ok(is_cancel: bool) -> Answer { if is_cancel { Answer::Bool(true) } else { Answer::Ok } }

pub fn tx_cancel(g: &mut G, t: &mut Transfer, r: ReplyId, is_cancel: bool, e: &mut Vec<Effect>) {
    let st = t.st.clone();
    let busy = |e: &mut Vec<Effect>, name: &'static str| { row(e, name); reply(e, r, Answer::Err(Fault::InFlight)); };
    if let Some(gid) = t.batch {
        if g.groups.get(&gid).map(|x| x.phase == GroupPhase::Mutating).unwrap_or(false) { busy(e, "xfer_batch_member_in_flight"); return; }
    }
    let member = t.batch.map(|gid| g.groups.get(&gid).map(|x| x.phase.clone())).flatten();
    match st {
        St::Reserving => busy(e, "xfer_reserving_refuses"),
        St::Accepting { .. } => busy(e, "xfer_accepting_other"),
        St::WaitingDeclaration { .. } => busy(e, "xfer_waiting_declaration_refuses"),
        St::CopyReady { .. } => busy(e, "xfer_copy_ready_refuses"),
        St::Copying { supply: Some(_), .. } | St::PersistingCopy { supply: Some(_), .. } => busy(e, "xfer_supply_busy"),
        St::PersistingCopy { .. } => busy(e, "xfer_persisting_copy_refuses"),
        St::PersistingAttempt { .. } => busy(e, "xfer_attempt_busy"),
        St::PersistingPart { .. } => busy(e, "xfer_persisting_part_refuses"),
        St::PersistingComplete => busy(e, "xfer_persisting_complete_refuses"),
        St::PersistingRetry { .. } => busy(e, "xfer_persisting_retry_refuses"),
        St::PersistingFailure { .. } => busy(e, "xfer_persisting_failure_refuses"),
        St::PersistingBatchIntent { .. } => busy(e, "xfer_batch_intent_busy"),
        St::Removing { .. } => busy(e, "xfer_removing_refuses"),
        St::Copying { .. } => {
            if t.batch.is_some() { busy(e, "xfer_copy_cancel_batch"); return; }
            row(e, "xfer_copy_cancel");
            e.push(Effect::CopyCancel { transfer_id: t.id });
            begin_remove(g, t, e, Why::Cancel, Some((r, is_cancel)), false, false, false, None);
        }
        St::AwaitingSource => {
            row(e, if t.batch.is_some() { "xfer_pending_batch_cancel" } else { "xfer_pending_cancel" });
            begin_remove(g, t, e, Why::Cancel, Some((r, is_cancel)), false, false, false, None);
        }
        St::Mutating => {
            if is_cancel {
                if t.batch.is_none() { row(e, "xfer_mutating_cancel"); reply(e, r, Answer::Bool(false)); } else { busy(e, "xfer_mutating_cancel"); }
            } else {
                row(e, "xfer_mutating_discard");
                let first = t.replies.discard_waiters.is_empty();
                t.replies.discard_waiters.push(r);
                if first { e.push(Effect::Outbox(OutboxEffect::Discard { op_id: t.op_id.unwrap_or(0), reply_to_transfer: t.id })); }
            }
        }
        St::Failed => {
            if member == Some(GroupPhase::Collecting) {
                row(e, if is_cancel { "xfer_batch_member_cancel" } else { "xfer_batch_member_discard" });
                begin_remove(g, t, e, Why::Cancel, Some((r, is_cancel)), false, true, true, None);
            } else { failed_remove(g, t, Some((r, is_cancel)), false, e); }
        }
        St::BatchReady => {
            match member {
                Some(GroupPhase::Singles) => { row(e, "xfer_batch_singles_member_cancel"); begin_remove(g, t, e, Why::Cancel, Some((r, is_cancel)), false, false, false, None); }
                Some(GroupPhase::Collecting) => {
                    row(e, if is_cancel { "xfer_batch_member_cancel" } else { "xfer_batch_member_discard" });
                    begin_remove(g, t, e, Why::Cancel, Some((r, is_cancel)), false, true, true, None);
                }
                _ => {
                    row(e, "xfer_batch_singles_cancel");
                    begin_remove(g, t, e, Why::Cancel, Some((r, is_cancel)), false, false, true, None);
                }
            }
        }
        St::Ready { .. } | St::Uploading { .. } | St::Completing { .. } => {
            if member == Some(GroupPhase::Collecting) {
                row(e, if is_cancel { "xfer_batch_member_cancel" } else { "xfer_batch_member_discard" });
                begin_remove(g, t, e, Why::Cancel, Some((r, is_cancel)), false, true, true, None);
                return;
            }
            let name = match (&st, t.upload_id.is_some()) {
                (St::Ready { .. }, true) => if g.online == Some(true) { "xfer_discard_online_abort" } else { "xfer_discard_deferred_abort" },
                (St::Ready { .. }, false) => "xfer_ready_cancel",
                (St::Uploading { .. }, _) => if is_cancel { "xfer_uploading_cancel" } else { "xfer_uploading_discard" },
                (_, _) => if is_cancel { "xfer_completing_cancel" } else { "xfer_completing_discard" },
            };
            row(e, name);
            begin_remove(g, t, e, Why::Cancel, Some((r, is_cancel)), false, true, true, None);
        }
        St::Done => {}
    }
}

pub fn failed_remove(g: &mut G, t: &mut Transfer, cancel: Option<(ReplyId, bool)>, expired: bool, e: &mut Vec<Effect>) {
    let live = t.intent_live && t.signed_id.is_some();
    if live {
        let is_cancel = cancel.map(|c| c.1).unwrap_or(false);
        if !expired { row(e, if is_cancel { "xfer_failed_cancel_live_intent" } else { "xfer_failed_discard_live_intent" }); }
        let first = t.replies.discard_waiters.is_empty() && t.replies.cancel.is_none();
        if let Some((r, c)) = cancel { if c { t.replies.cancel = Some((r, true)); } else { t.replies.discard_waiters.push(r); } }
        if first { e.push(Effect::Outbox(OutboxEffect::Discard { op_id: t.op_id.unwrap_or(0), reply_to_transfer: t.id })); }
        return;
    }
    if t.upload_id.is_some() {
        if !expired { row(e, "xfer_discard"); }
        begin_remove(g, t, e, Why::Cancel, cancel, t.intent_live, true, false, None);
    } else {
        if !expired { row(e, "xfer_discard_no_upload"); }
        begin_remove(g, t, e, Why::Cancel, cancel, t.intent_live, false, false, None);
    }
}

pub fn tx_retry(g: &mut G, t: &mut Transfer, source: &Option<Supplied>, r: ReplyId, e: &mut Vec<Effect>) {
    let st = t.st.clone();
    let busy = |e: &mut Vec<Effect>, name: &'static str| { row(e, name); reply(e, r, Answer::Err(Fault::InFlight)); };
    let ok = |e: &mut Vec<Effect>, name: &'static str, t: &Transfer| { row(e, name); reply(e, r, Answer::Receipt(receipt(t))); };
    match st {
        St::Reserving => busy(e, "xfer_reserving_refuses"),
        St::Accepting { .. } => busy(e, "xfer_accepting_other"),
        St::WaitingDeclaration { .. } => busy(e, "xfer_waiting_declaration_refuses"),
        St::AwaitingSource => busy(e, "xfer_pending_retry"),
        St::Copying { supply: Some(_), .. } | St::PersistingCopy { supply: Some(_), .. } => busy(e, "xfer_supply_busy"),
        St::Copying { .. } => busy(e, "xfer_copy_retry"),
        St::CopyReady { .. } => busy(e, "xfer_copy_ready_refuses"),
        St::PersistingCopy { .. } => busy(e, "xfer_persisting_copy_refuses"),
        St::PersistingAttempt { .. } => busy(e, "xfer_attempt_busy"),
        St::PersistingPart { .. } => busy(e, "xfer_persisting_part_refuses"),
        St::PersistingComplete => busy(e, "xfer_persisting_complete_refuses"),
        St::PersistingRetry { .. } => busy(e, "xfer_persisting_retry_refuses"),
        St::PersistingFailure { .. } => busy(e, "xfer_persisting_failure_refuses"),
        St::PersistingBatchIntent { .. } => busy(e, "xfer_batch_intent_busy"),
        St::Removing { .. } => busy(e, "xfer_removing_refuses"),
        St::Ready { .. } => ok(e, "xfer_ready_retry", t),
        St::Uploading { .. } => ok(e, "xfer_uploading_retry", t),
        St::Completing { .. } => ok(e, "xfer_completing_retry", t),
        St::Mutating => ok(e, "xfer_mutating_retry", t),
        St::BatchReady => ok(e, "xfer_batch_ready_retry", t),
        St::Done => {}
        St::Failed => failed_retry(g, t, source, r, e),
    }
}

pub fn failed_retry(g: &mut G, t: &mut Transfer, source: &Option<Supplied>, r: ReplyId, e: &mut Vec<Effect>) {
    let kind = t.error.as_ref().map(|x| x.kind.clone());
    if kind == Some(ErrKind::RestoreFailed) { row(e, "xfer_failed_retry_restore_failed"); reply(e, r, Answer::Err(Fault::RestoreFailed)); return; }
    let no_copy = matches!(kind, Some(ErrKind::MissingFile) | Some(ErrKind::SourceFailed)) && !t.copy_present;
    if let Some(src) = source {
        if !src.valid || !src.values_declared {
            row(e, "xfer_retry_invalid_source");
            reply(e, r, Answer::Err(Fault::Invalid));
            return;
        }
        row(e, if no_copy { "xfer_retry_new_source" } else { "xfer_retry_source_replaces_copy" });
        if t.batch.is_some() { row(e, "xfer_batch_retry_file"); }
        begin_supply(g, t, e, src, r, Back::Failed);
        return;
    }
    if no_copy { row(e, "xfer_retry_needs_source"); reply(e, r, Answer::Err(Fault::Invalid)); return; }
    t.replies.retry = Some(r);
    let completed = t.signed_id.is_some();
    if t.batch.is_some() { row(e, "xfer_batch_retry_file"); }
    if completed && !t.intent_live {
        row(e, "xfer_batch_retry_missing");
        row(e, "xfer_retry_removed_intent");
        t.st = St::PersistingRetry { next: Next::Mutation, recreate: true, wait_outbox: false };
        commit(g, t, e, vec![Op::Retrying { reset_budget: true, new_cycle: false }]);
        return;
    }
    if completed && t.intent_live {
        row(e, "xfer_batch_retry_mutation");
        t.st = St::PersistingRetry { next: Next::Mutation, recreate: false, wait_outbox: true };
        let target = match t.batch { Some(gid) => Target::Group { batch_id: gid }, None => Target::Transfer { transfer_id: t.id } };
        e.push(Effect::Outbox(OutboxEffect::RetryTransfer { target }));
        return;
    }
    let all = t.parts > 0 && t.confirmed.len() as u32 >= t.parts;
    let next = if all { row(e, "xfer_retry_complete"); Next::Complete }
        else if t.aborted && t.upload_id.is_none() && t.confirmed.is_empty() { row(e, "xfer_retry_after_abort"); Next::Part(1) }
        else { row(e, "xfer_retry"); first_unconfirmed(t) };
    t.st = St::PersistingRetry { next, recreate: false, wait_outbox: false };
    commit(g, t, e, vec![Op::Retrying { reset_budget: true, new_cycle: false }]);
}

pub fn begin_supply(g: &mut G, t: &mut Transfer, e: &mut Vec<Effect>, src: &Supplied, r: ReplyId, back: Back) {
    let attempt = g.next_attempt;
    g.next_attempt += 1;
    let new_cycle = back == Back::Failed;
    t.values = src.values.clone();
    t.variables = src.variables.clone();
    t.source = Source::File { path: src.path.clone() };
    t.replies.supply = Some((r, new_cycle));
    t.st = St::Copying { attempt, supply: Some(back), new_cycle };
    e.push(Effect::Copy { transfer_id: t.id, attempt_id: attempt, source: t.source.clone() });
    g.round_needed.push(t.id);
}

pub fn tx_supply(g: &mut G, t: &mut Transfer, src: &Supplied, r: ReplyId, e: &mut Vec<Effect>) {
    match t.st.clone() {
        St::AwaitingSource => {
            if !src.valid || !src.values_declared { row(e, "xfer_supply_invalid"); reply(e, r, Answer::Err(Fault::Invalid)); return; }
            row(e, "xfer_supply_starts_copy");
            begin_supply(g, t, e, src, r, Back::AwaitingSource);
        }
        St::Copying { supply: Some(_), .. } | St::PersistingCopy { supply: Some(_), .. } => { row(e, "xfer_supply_busy"); reply(e, r, Answer::Err(Fault::InFlight)); }
        _ => { row(e, "xfer_supply_not_awaiting"); reply(e, r, Answer::Err(Fault::Invalid)); }
    }
}

pub fn tx_source_failed(g: &mut G, t: &mut Transfer, message: &str, r: ReplyId, e: &mut Vec<Effect>) {
    match t.st.clone() {
        St::AwaitingSource => {
            row(e, "xfer_source_failed");
            t.replies.supply = Some((r, false));
            t.st = St::PersistingFailure { error: fail(ErrKind::SourceFailed, message), abort: None };
            commit(g, t, e, vec![Op::Failed { clear_upload: false }]);
        }
        St::Copying { supply: Some(_), .. } | St::PersistingCopy { supply: Some(_), .. } => { row(e, "xfer_supply_busy"); reply(e, r, Answer::Err(Fault::InFlight)); }
        _ => { row(e, "xfer_supply_not_awaiting"); reply(e, r, Answer::Err(Fault::Invalid)); }
    }
}

pub fn tx_mutation_outcome(g: &mut G, t: &mut Transfer, out: &MutOut, e: &mut Vec<Effect>) {
    match t.st.clone() {
        St::Mutating => mutation_outcome(g, t, out, e),
        St::BatchReady => {
            let singles = t.batch.and_then(|gid| g.groups.get(&gid)).map(|x| x.phase == GroupPhase::Singles).unwrap_or(false);
            let _ = singles;
            row(e, "xfer_batch_ready_other");
        }
        St::Failed => {
            let waiting = !t.replies.discard_waiters.is_empty() || t.replies.cancel.is_some();
            if waiting && matches!(out, MutOut::Discarded) {
                row(e, "xfer_failed_discarded");
                t.intent_live = false;
                begin_remove_after_discard(g, t, e);
            } else if waiting && matches!(out, MutOut::Confirmed { .. }) {
                row(e, "xfer_failed_confirmed_before_discard");
                if let MutOut::Confirmed { revision } = out {
                    for r in std::mem::take(&mut t.replies.discard_waiters) { reply(e, r, Answer::Err(Fault::NotFound)); }
                    if let Some((r, _)) = t.replies.cancel.take() { reply(e, r, Answer::Bool(false)); }
                    t.intent_live = false;
                    let prior = t.st.clone();
                    t.st = St::Removing { why: Why::Confirmed(*revision), prior: Box::new(prior), orphan: None };
                    commit(g, t, e, vec![Op::RemoveUpload { transfer_id: t.id }]);
                }
            } else if waiting {
                row(e, "xfer_failed_outcome_failed_waiting");
            } else { row(e, "xfer_failed_other"); }
        }
        other => row(e, stray_row(&other)),
    }
}

pub fn begin_remove_after_discard(g: &mut G, t: &mut Transfer, e: &mut Vec<Effect>) {
    let prior = t.st.clone();
    t.st = St::Removing { why: Why::Discarded, prior: Box::new(prior), orphan: None };
    commit(g, t, e, vec![Op::RemoveUpload { transfer_id: t.id }]);
}

pub fn mutation_outcome(g: &mut G, t: &mut Transfer, out: &MutOut, e: &mut Vec<Effect>) {
    let q = g.quiet;
    let group_singles = t.batch.and_then(|gid| g.groups.get(&gid)).map(|x| x.phase == GroupPhase::Singles).unwrap_or(false);
    let gid = t.batch.unwrap_or(0);
    let waiters = !t.replies.discard_waiters.is_empty();
    let prior = St::Mutating;
    let post_singles = |g: &mut G, t: &Transfer| { if group_singles { g.post.push(Post::SinglesNext(gid, t.id)); } };
    match out {
        MutOut::Confirmed { revision } => {
            if waiters {
                if !q { row(e, "xfer_mutation_confirmed_discard_waiting"); }
                for r in std::mem::take(&mut t.replies.discard_waiters) { reply(e, r, Answer::Err(Fault::NotFound)); }
            } else if !q { row(e, "xfer_mutation_confirmed"); }
            t.intent_live = false;
            t.st = St::Removing { why: Why::Confirmed(*revision), prior: Box::new(prior), orphan: None };
            cm_mo(g, t, e, vec![Op::RemoveUpload { transfer_id: t.id }]);
            post_singles(g, &*t);
        }
        MutOut::Rejected { error } => {
            t.intent_live = false;
            if !waiters {
                if !q { row(e, "xfer_mutation_failed"); }
                t.st = St::PersistingFailure { error: error.clone(), abort: None };
                cm_mo(g, t, e, vec![Op::Failed { clear_upload: false }]);
            } else {
                if !q { row(e, "xfer_mutation_rejected_discard_waiting"); }
                t.st = St::Removing { why: Why::Discarded, prior: Box::new(prior), orphan: None };
                cm_mo(g, t, e, vec![Op::RemoveUpload { transfer_id: t.id }]);
            }
            post_singles(g, &*t);
        }
        MutOut::Failed { error } => {
            if !waiters {
                if !q { row(e, "xfer_mutation_failed"); }
                t.st = St::PersistingFailure { error: error.clone(), abort: None };
                cm_mo(g, t, e, vec![Op::Failed { clear_upload: false }]);
                post_singles(g, &*t);
            } else if !q { row(e, "xfer_mutation_failed_discard_waiting"); }
        }
        MutOut::Discarded => {
            if !q { row(e, "xfer_mutation_discarded"); }
            t.intent_live = false;
            t.st = St::Removing { why: Why::Discarded, prior: Box::new(prior), orphan: None };
            cm_mo(g, t, e, vec![Op::RemoveUpload { transfer_id: t.id }]);
            post_singles(g, &*t);
        }
        MutOut::Interrupted => {
            if waiters {
                if !q { row(e, "xfer_mutation_interrupted_discard_waiting"); }
                for r in std::mem::take(&mut t.replies.discard_waiters) { reply(e, r, Answer::Err(Fault::Interrupted)); }
            } else if !q { row(e, "xfer_mutation_interrupted"); }
            fix_outcome(t, e, Outcome::Interrupted);
            t.st = St::Done;
        }
    }
}

pub fn cm_mo(g: &mut G, t: &Transfer, e: &mut Vec<Effect>, ops: Vec<Op>) {
    if g.quiet { cm(g, t, e, "mo", ops); } else { commit(g, t, e, ops); }
}

pub fn tx_discard_outcome(_g: &mut G, t: &mut Transfer, result: &Result<(), Fault>, e: &mut Vec<Effect>) {
    match t.st.clone() {
        St::Failed if t.replies.discard_waiters.is_empty() && t.replies.cancel.is_none() => row(e, "r2_discard_outcome_stray"),
        St::Mutating | St::Failed => match result {
            Err(f) => {
                row(e, if t.st == St::Failed { "xfer_failed_discard_outcome" } else { "xfer_mutating_discard_refused" });
                for r in std::mem::take(&mut t.replies.discard_waiters) { reply(e, r, Answer::Err(f.clone())); }
                if let Some((r, _)) = t.replies.cancel.take() { reply(e, r, Answer::Err(f.clone())); }
            }
            Ok(()) => row(e, if t.st == St::Failed { "xfer_failed_discard_outcome" } else { "xfer_mutating_discard_accepted" }),
        },
        St::AwaitingSource => row(e, "xfer_pending_stray"),
        other => row(e, stray_row(&other)),
    }
}

pub fn tx_retry_outcome(g: &mut G, t: &mut Transfer, target: &Target, result: &Result<u64, Fault>, e: &mut Vec<Effect>) {
    let mine = match target { Target::Transfer { transfer_id } => *transfer_id == t.id, Target::Group { batch_id } => Some(*batch_id) == t.batch };
    match t.st.clone() {
        St::PersistingRetry { wait_outbox: true, .. } if mine => match result {
            Ok(_) => {
                row(e, "xfer_retry_mutation_outcome");
                free_held(t, e);
                t.cycle_id += 1;
                t.intent_live = true;
                t.st = St::Mutating;
                if let Some(r) = t.replies.retry.take() { reply(e, r, Answer::Receipt(receipt(t))); }
            }
            Err(f) => {
                row(e, "xfer_retry_mutation_rejected");
                t.st = St::Failed;
                if let Some(r) = t.replies.retry.take() { reply(e, r, Answer::Err(f.clone())); }
            }
        },
        _ => row(e, "xfer_retry_mutation_stale"),
    }
    let _ = g;
}

pub fn tx_outcome(t: &mut Transfer, cycle: u64, saved: &Option<Outcome>, r: ReplyId, e: &mut Vec<Effect>) {
    if cycle != t.cycle_id {
        match t.outcomes.get(&cycle).or(saved.as_ref()) {
            Some(o) => { row(e, "xfer_outcome_old_cycle"); reply(e, r, Answer::Outcome(o.clone())); }
            None => { row(e, "xfer_outcome_unknown"); reply(e, r, Answer::Err(Fault::NotFound)); }
        }
        return;
    }
    if t.st == St::Failed {
        if let Some(o) = t.outcomes.get(&cycle) { row(e, "xfer_failed_outcome"); reply(e, r, Answer::Outcome(o.clone())); return; }
    }
    row(e, if t.st == St::AwaitingSource { "xfer_pending_outcome" } else { "xfer_outcome_wait" });
    t.outcome_waiters.entry(cycle).or_default().push(r);
}

pub fn tx_round_status(g: &mut G, t: &mut Transfer, refused: &Option<String>, e: &mut Vec<Effect>) {
    row(e, "xfer_round_status");
    t.round_error = refused.clone();
    let b = g.next_batch;
    g.next_batch += 1;
    g.batches.insert(b, BatchKind::Detached);
    e.push(Effect::Store { batch: b, generation: g.generation, basis: None, ops: vec![Op::RoundError(refused.clone())] });
}

pub fn tx_end(g: &mut G, t: &mut Transfer, e: &mut Vec<Effect>, closing: bool) {
    let st = t.st.clone();
    if st == St::Done { return; }
    if closing {
        if let St::Accepting { .. } = st { t.st = St::Accepting { closing: true }; row(e, "xfer_accepting_closing"); return; }
    }
    let name: &'static str = match (&st, closing) {
        (St::Reserving, _) => "xfer_reserving_end",
        (St::PersistingAttempt { .. }, _) => "xfer_attempt_close",
        (St::PersistingBatchIntent { .. }, _) => "xfer_batch_intent_close",
        (St::AwaitingSource, true) => "xfer_pending_closing",
        (St::AwaitingSource, false) => "xfer_pending_epoch_end",
        (St::Copying { supply: Some(_), .. }, _) | (St::PersistingCopy { supply: Some(_), .. }, _) => "xfer_supply_end",
        (St::Copying { .. }, true) | (St::CopyReady { .. }, true) => "xfer_closing_copying",
        (_, true) => "xfer_closing",
        _ => "xfer_epoch_ended",
    };
    row(e, name);
    match &st {
        St::Uploading { attempt, waiting_retry: false, .. } | St::Completing { attempt, waiting_retry: false } => {
            if matches!(st, St::Uploading { .. }) { row(e, "xfer_put_cancelled"); }
            e.push(Effect::TransportCancel { transfer_id: t.id, attempt_id: *attempt });
            cancel_put(e, t.id, *attempt);
        }
        St::Copying { .. } | St::CopyReady { .. } | St::PersistingCopy { supply: Some(_), .. } => {
            e.push(Effect::CopyCancel { transfer_id: t.id });
            if closing || matches!(st, St::Copying { supply: Some(_), .. } | St::PersistingCopy { supply: Some(_), .. }) { e.push(Effect::DeleteFile { transfer_id: t.id }); }
        }
        _ => {}
    }
    free_held(t, e);
    t.st = St::Done;
    let cause = if closing { EndCause::Closed } else { EndCause::SessionEnded };
    interrupt_all(g, t, e, cause);
    let c = t.cycle_id;
    t.outcomes.entry(c).or_insert(Outcome::Interrupted);
    t.local_url = false;
    e.push(Effect::Rounds(RoundsIn::Idle { epoch: t.epoch, transfer_id: t.id }));
}

pub fn remove_from_group(g: &mut G, t: &Transfer) {
    if let Some(gid) = t.batch {
        if let Some(gr) = g.groups.get_mut(&gid) { gr.items.retain(|x| *x != t.id); }
        g.post.push(Post::GroupCheck { gid, from_complete: false, removal: true });
    }
}
