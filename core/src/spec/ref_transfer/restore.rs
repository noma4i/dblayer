use super::*;

pub fn classify_row(g: &mut G, t: &mut Transfer, r: &StoredTransfer, item: &RestoreItem, outs: &BTreeMap<TransferId, BTreeMap<u64, Outcome>>, e: &mut Vec<Effect>) {
    if let Some(gr) = &r.group {
        g.groups.entry(gr.batch_id).or_insert(Group {
            items: gr.items.clone(), op_id: Some(gr.op_id), minimum: gr.minimum, maximum: gr.maximum,
            phase: if gr.intent_present { if gr.single_intent { GroupPhase::Singles } else { GroupPhase::Mutating } } else { GroupPhase::Collecting },
            accept_reply: None, reserve: BTreeMap::new(), reserve_need: BTreeSet::new(), reserve_failed: false, renumber_start: 0,
            submit_pending: false, pack_ids: gr.items.clone(), pack_op: Some(gr.op_id),
        });
    }
    if !declared(g, &r.key) {
        row(e, "xfer_open_waiting_declaration");
        t.st = St::WaitingDeclaration { item: Box::new(item.clone()) };
        return;
    }
    match &r.kind {
        StoredKind::AwaitingSource => {
            row(e, "xfer_open_pending_source");
            t.source = Source::Pending;
            let error = fail(ErrKind::MissingFile, "missing_file");
            t.st = St::PersistingFailure { error, abort: None };
            commit(g, t, e, vec![Op::Failed { clear_upload: false }]);
            e.push(Effect::Schedule { kind: TickKind::Expire, delay: Delay::Now });
        }
        StoredKind::Failed => {
            row(e, "xfer_open_failed");
            t.st = St::Failed;
            let er = r.error.clone().unwrap_or(fail(ErrKind::Step("failed".into()), "failed"));
            t.error = Some(er.clone());
            fix_outcome(t, e, Outcome::Failed { error: er });
            e.push(Effect::Schedule { kind: TickKind::Expire, delay: Delay::Now });
        }
        StoredKind::Copying { source_present, source_path } => {
            if *source_present {
                row(e, "xfer_open_copying");
                t.source = Source::File { path: source_path.clone() };
                let a = g.next_attempt;
                g.next_attempt += 1;
                t.st = St::Copying { attempt: a, supply: None, new_cycle: false };
                e.push(Effect::Copy { transfer_id: t.id, attempt_id: a, source: t.source.clone() });
                g.round_needed.push(t.id);
            } else {
                row(e, "xfer_open_missing_file");
                missing_file(g, t, e);
            }
        }
        StoredKind::Active | StoredKind::Completed => {
            if !r.copy_present { row(e, "xfer_open_missing_file"); missing_file(g, t, e); return; }
            if r.kind == StoredKind::Active { classify_active(g, t, r, e); } else { classify_completed(g, t, r, outs, e); }
        }
    }
}

pub fn missing_file(g: &mut G, t: &mut Transfer, e: &mut Vec<Effect>) {
    let error = fail(ErrKind::MissingFile, "missing_file");
    t.st = St::PersistingFailure { error, abort: None };
    commit(g, t, e, vec![Op::Failed { clear_upload: false }]);
}

pub fn classify_active(g: &mut G, t: &mut Transfer, r: &StoredTransfer, e: &mut Vec<Effect>) {
    if let Some(p) = &r.pending_step {
        row(e, "xfer_open_pending_step");
        t.st = St::Ready { next: p.next };
        t.held_step = Some(p.req_id.clone());
        g.round_needed.push(t.id);
        return;
    }
    let next = first_unconfirmed(t);
    if next == Next::Complete {
        row(e, "xfer_open_completing");
        t.st = St::Ready { next: Next::Complete };
    } else {
        row(e, "xfer_open_ready");
        t.st = St::Ready { next };
    }
    e.push(Effect::SelfSlotFreed);
    g.round_needed.push(t.id);
    e.push(Effect::Schedule { kind: TickKind::Expire, delay: Delay::Now });
}

pub fn classify_completed(g: &mut G, t: &mut Transfer, r: &StoredTransfer, outs: &BTreeMap<TransferId, BTreeMap<u64, Outcome>>, e: &mut Vec<Effect>) {
    if let Some(gr) = &r.group {
        if gr.intent_present { row(e, "xfer_open_batch_intent"); t.st = St::Mutating; return; }
        if gr.readable { row(e, "xfer_open_batch_ready"); t.st = St::BatchReady; return; }
        row(e, "xfer_open_completed_no_intent");
        restore_failed_state(t, e);
        return;
    }
    let saved = outs.get(&t.id).and_then(|m| m.get(&t.cycle_id)).cloned();
    match saved {
        Some(Outcome::Confirmed { revision }) => {
            row(e, "xfer_open_saved_outcome");
            t.st = St::Removing { why: Why::Confirmed(revision), prior: Box::new(St::Mutating), orphan: None };
            commit(g, t, e, vec![Op::RemoveUpload { transfer_id: t.id }]);
        }
        Some(Outcome::Discarded) => {
            row(e, "xfer_open_saved_outcome_discarded");
            t.st = St::Removing { why: Why::Discarded, prior: Box::new(St::Mutating), orphan: None };
            commit(g, t, e, vec![Op::RemoveUpload { transfer_id: t.id }]);
        }
        Some(Outcome::Rejected { error }) => {
            row(e, "xfer_open_saved_outcome_rejected");
            t.st = St::PersistingFailure { error, abort: None };
            commit(g, t, e, vec![Op::Failed { clear_upload: false }]);
        }
        Some(Outcome::Failed { error }) => {
            row(e, "xfer_open_saved_outcome_failed");
            t.st = St::PersistingFailure { error, abort: None };
            commit(g, t, e, vec![Op::Failed { clear_upload: false }]);
        }
        Some(Outcome::Ended(_)) => {
            row(e, "xfer_open_saved_interrupted");
            t.st = St::Done;
        }
        None => {
            if r.intent_live { row(e, "xfer_open_mutating"); t.st = St::Mutating; }
            else if r.cycle_id > 1 && outs.get(&t.id).map(|m| m.keys().any(|c| *c < r.cycle_id)).unwrap_or(false) {
                row(e, "xfer_open_retry_after_crash");
                t.st = St::PersistingBatchIntent { single: true, retry: None };
                t.intent_after_ready = true;
            } else {
                row(e, "xfer_open_completed_no_intent");
                restore_failed_state(t, e);
            }
        }
    }
}

pub fn restore_failed_state(t: &mut Transfer, e: &mut Vec<Effect>) {
    t.st = St::Failed;
    let er = fail(ErrKind::RestoreFailed, "restore_failed");
    t.error = Some(er.clone());
    fix_outcome(t, e, Outcome::Failed { error: er });
}

pub fn store_reply(s: &mut State, batch: StoreBatch, epoch: u64, res: Result<u64, Reason>, e: &mut Vec<Effect>) {
    if epoch != s.g.epoch { row(e, "r4_store_reply_other"); return; }
    if shown_committed(s, batch, &res, e) { return; }
    let kind = match s.g.batches.remove(&batch) { Some(k) => k, None => { row(e, "r2_store_reply_unknown"); return; } };
    match kind {
        BatchKind::Own { ids, ops } => {
            if let Err(Reason::StaleGeneration(gens)) = &res {
                let first = ids.iter().find_map(|id| s.transfers.get(id));
                let (accepting, rname) = match first {
                    Some(t) => (matches!(t.st, St::Accepting { .. }), match t.st { St::PersistingAttempt { .. } => "xfer_attempt_stale", St::PersistingBatchIntent { .. } => "xfer_batch_intent_stale", _ => "xfer_persisting_stale_generation" }),
                    None => (false, "xfer_persisting_stale_generation"),
                };
                if !accepting && first.is_some() {
                    row(e, rname);
                    let b = s.g.next_batch;
                    s.g.next_batch += 1;
                    s.g.batches.insert(b, BatchKind::Own { ids, ops: ops.clone() });
                    e.push(Effect::Store { batch: b, generation: s.g.generation, basis: Some(gens.clone()), ops });
                    return;
                }
            }
            let input = match &res { Ok(rev) => Input::Committed { batch, epoch, revision: *rev }, Err(r) => Input::Rejected { batch, epoch, reason: r.clone() } };
            for id in ids { run_on(s, id, &input, e); }
        }
        BatchKind::Detached => row(e, "r3_detached_store_reply_ignored"),
        BatchKind::Orphan { upload_id } => {
            if res.is_ok() { s.g.orphans.remove(&upload_id); row(e, "xfer_orphan_abort_done"); } else { row(e, "xfer_orphan_remove_rejected"); }
        }
        BatchKind::Minted { upload_id } => minted_reply(s, &upload_id, res, e),
    }
    maybe_drained(s, e);
}

pub fn minted_reply(s: &mut State, upload_id: &str, res: Result<u64, Reason>, e: &mut Vec<Effect>) {
    let (is_orphan, key) = match s.g.minted.get_mut(upload_id) { Some(m) => { m.in_flight = None; (m.is_orphan, m.key.clone()) } None => { row(e, "r2_minted_reply_unknown"); return; } };
    match res {
        Ok(_) => {
            row(e, "xfer_minted_write_committed");
            if let Some(m) = s.g.minted.get_mut(upload_id) { m.debt = false; }
            if is_orphan { s.g.orphans.insert(upload_id.to_string(), key); dispatch_inner(&mut s.g, e, false); }
        }
        Err(Reason::Storage) => {
            row(e, "xfer_minted_write_storage");
            let tid = s.g.minted.get(upload_id).map(|m| m.transfer_id).unwrap_or(0);
            let a = s.g.next_attempt;
            s.g.next_attempt += 1;
            if let Some(m) = s.g.minted.get_mut(upload_id) { m.attempt_id = a; }
            e.push(Effect::Schedule { kind: TickKind::MintedWrite { transfer_id: tid, attempt_id: a }, delay: Delay::Backoff { attempt: 1 } });
        }
        Err(Reason::NoSession) | Err(Reason::Closed) => {
            row(e, "xfer_minted_write_interrupted");
            if let Some(m) = s.g.minted.get_mut(upload_id) { m.debt = false; }
        }
        Err(Reason::Invalid) => row(e, "xfer_minted_write_invalid"),
        Err(_) => {
            row(e, "xfer_minted_write_other");
            let tid = s.g.minted.get(upload_id).map(|m| m.transfer_id).unwrap_or(0);
            let a = s.g.next_attempt;
            s.g.next_attempt += 1;
            if let Some(m) = s.g.minted.get_mut(upload_id) { m.attempt_id = a; }
            e.push(Effect::Schedule { kind: TickKind::MintedWrite { transfer_id: tid, attempt_id: a }, delay: Delay::Backoff { attempt: 1 } });
        }
    }
}

pub fn minted_tick(s: &mut State, tid: TransferId, aid: AttemptId, e: &mut Vec<Effect>) {
    let found = s.g.minted.iter().find(|(_, m)| m.transfer_id == tid && m.attempt_id == aid).map(|(k, _)| k.clone());
    let upload_id = match found { Some(u) => u, None => { row(e, "r2_minted_tick_unknown"); return; } };
    let upload_id = upload_id.as_str();
    let (is_orphan, key, flight) = match s.g.minted.get(upload_id) { Some(m) => (m.is_orphan, m.key.clone(), m.in_flight), None => { row(e, "r2_minted_tick_unknown"); return; } };
    if flight.is_some() || s.g.phase == Phase::Idle || s.g.phase == Phase::Closed { row(e, "xfer_minted_write_retry_skipped"); return; }
    row(e, "xfer_minted_write_retry");
    let b = s.g.next_batch;
    s.g.next_batch += 1;
    s.g.batches.insert(b, BatchKind::Minted { upload_id: upload_id.to_string() });
    if let Some(m) = s.g.minted.get_mut(upload_id) { m.in_flight = Some(b); }
    let ops = if is_orphan { vec![Op::UploadOrphan { upload_id: upload_id.to_string(), key }] } else { vec![Op::UploadId { upload_id: upload_id.to_string() }] };
    e.push(Effect::Store { batch: b, generation: s.g.generation, basis: None, ops });
}

pub fn dispatch_aborts(g: &mut G, e: &mut Vec<Effect>) { dispatch_inner(g, e, false); }
pub fn dispatch_aborts_owe(g: &mut G, e: &mut Vec<Effect>) { dispatch_inner(g, e, true); }

pub fn dispatch_inner(g: &mut G, e: &mut Vec<Effect>, owe: bool) {
    if g.orphans.is_empty() || g.online != Some(true) { return; }
    if g.declarations.is_empty() { if owe { row(e, "xfer_orphan_abort_waits_declaration"); } return; }
    let list: Vec<(String, TransferKey)> = g.orphans.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    for (uid, key) in list {
        if g.orphan_flight.contains(&uid) {
            if owe { g.orphan_owed.insert(uid); row(e, "xfer_orphan_abort_owed"); }
        } else {
            g.orphan_flight.insert(uid.clone());
            row(e, "xfer_orphan_abort_sent");
            e.push(Effect::Abort { upload_id: uid, key, net_purpose_transfer: true });
        }
    }
}

pub fn abort_done(g: &mut G, upload_id: &str, result: &Result<(), AbortFault>, e: &mut Vec<Effect>) {
    let gone = matches!(result, Ok(()) | Err(AbortFault { status: Some(404 | 410), .. }));
    if gone {
        row(e, "xfer_orphan_abort_done");
        g.orphan_flight.remove(upload_id);
        g.orphan_owed.remove(upload_id);
        let b = g.next_batch;
        g.next_batch += 1;
        g.batches.insert(b, BatchKind::Orphan { upload_id: upload_id.to_string() });
        e.push(Effect::Store { batch: b, generation: g.generation, basis: None, ops: vec![Op::RemoveOrphan { upload_id: upload_id.to_string() }] });
        return;
    }
    if g.orphan_owed.contains(upload_id) {
        g.orphan_owed.remove(upload_id);
        if g.online == Some(true) {
            row(e, "xfer_orphan_abort_owed_resent");
            let key = g.orphans.get(upload_id).cloned();
            if let Some(key) = key { e.push(Effect::Abort { upload_id: upload_id.to_string(), key, net_purpose_transfer: true }); }
        } else {
            row(e, "xfer_orphan_abort_owed_offline");
            g.orphan_flight.remove(upload_id);
        }
    } else {
        let temporary = matches!(result, Err(AbortFault { temporary: true, .. }));
        row(e, if temporary { "xfer_orphan_abort_retry_later" } else { "xfer_orphan_abort_refused_keeps_debt" });
        g.orphan_flight.remove(upload_id);
    }
}
