use super::*;

pub fn owner(s: &mut State, i: &Input, e: &mut Vec<Effect>) {
    s.g.slots_used = s.transfers.values().filter(|t| holds_slot(&t.st)).count() as u32;
    match i {
        Input::EpochStarted { epoch, generation } => {
            row(e, "xfer_epoch_started_generation");
            s.g.generation = *generation;
            if s.g.phase == Phase::Idle {
                s.g.epoch = *epoch;
                s.g.phase = Phase::Restoring;
                e.push(Effect::LifecycleStarted { epoch: *epoch });
                e.push(Effect::NetworkSnapshotRequest { epoch: *epoch });
            }
        }
        Input::ModelsReset { models, generation } => {
            row(e, "xfer_models_reset_generation");
            s.g.generation = *generation;
            for m in models { s.g.models.insert(m.clone(), *generation); }
            broadcast(s, i, e);
        }
        Input::Accept { epoch, now, item, reply: r } => accept(s, *epoch, *now, item.clone(), *r, e),
        Input::AcceptBatch { epoch, now, minimum, maximum, items, reply: r } => accept_batch(s, *epoch, *now, *minimum, *maximum, items.clone(), *r, e),
        Input::Restored { epoch, now, orphans, uploads, transfer_outcomes, held_orphan, .. } => { s.g.now = *now; restored(s, *epoch, orphans, uploads, transfer_outcomes, held_orphan, e) }
        Input::RestoreReady { epoch } => {
            if *epoch == s.g.epoch && s.g.phase == Phase::Classified {
                row(e, "xfer_restore_ready");
                s.g.phase = Phase::Ready;
                e.push(Effect::SelfSlotFreed);
                dispatch_aborts_owe(&mut s.g, e);
                broadcast(s, i, e);
                for h in std::mem::take(&mut s.g.held) { if let Some(c) = h.call { owner_addressed(s, &c, e); } }
            } else {
                row(e, "xfer_restore_ready_other");
                if *epoch == s.g.epoch && s.g.phase == Phase::Ready { dispatch_aborts(&mut s.g, e); }
            }
        }
        Input::RegisterOperations { declarations } => {
            s.g.declarations = declarations.clone();
            if s.g.phase == Phase::Restoring { row(e, "xfer_restoring_register"); }
            broadcast(s, i, e);
            dispatch_aborts(&mut s.g, e);
        }
        Input::Committed { batch, epoch, revision } => store_reply(s, *batch, *epoch, Ok(*revision), e),
        Input::Rejected { batch, epoch, reason } => store_reply(s, *batch, *epoch, Err(reason.clone()), e),
        Input::Online(b) => {
            s.g.online = Some(*b);
            broadcast(s, i, e);
            if *b { dispatch_aborts_owe(&mut s.g, e); }
        }
        Input::PowerSaving(b) => { s.g.saving = *b; broadcast(s, i, e); }
        Input::SlotFreed => broadcast(s, i, e),
        Input::Tick { now, kind } => {
            s.g.now = *now;
            match kind {
                TickKind::Expire => broadcast(s, i, e),
                TickKind::Retry { transfer_id, .. } | TickKind::PutTimeout { transfer_id, .. } => run_on(s, *transfer_id, i, e),
                TickKind::MintedWrite { transfer_id, attempt_id } => minted_tick(s, *transfer_id, *attempt_id, e),
            }
        }
        Input::EpochEnded { epoch, cause } => {
            if *epoch != s.g.epoch { row(e, "r4_epoch_ended_other"); return; }
            if s.g.phase == Phase::Restoring {
                row(e, "xfer_restoring_closing");
                for h in std::mem::take(&mut s.g.held) { reply(e, h.reply, Answer::Err(Fault::Ended(*cause))); }
                s.g.phase = Phase::Idle;
                return;
            }
            broadcast(s, i, e);
            for h in std::mem::take(&mut s.g.held) { reply(e, h.reply, Answer::Err(Fault::Ended(*cause))); }
            if !s.g.selecting.is_empty() { row(e, "xfer_select_epoch_end"); }
            for (r, _) in std::mem::take(&mut s.g.selecting) { reply(e, r, Answer::Err(Fault::Ended(*cause))); }
        }
        Input::Closing => closing(s, e),
        Input::RoundStatus { epoch, transfer_ids, refused } => {
            if *epoch != s.g.epoch { row(e, "r4_round_status_other"); return; }
            for id in transfer_ids { run_on(s, *id, &Input::RoundStatus { epoch: *epoch, transfer_ids: vec![*id], refused: refused.clone() }, e); }
        }
        _ => owner_addressed(s, i, e),
    }
}

pub fn broadcast(s: &mut State, i: &Input, e: &mut Vec<Effect>) {
    for id in order(s) { run_on(s, id, i, e); }
}

pub fn maybe_drained(s: &mut State, e: &mut Vec<Effect>) {
    if s.g.phase == Phase::Closed && !s.g.drained && !s.transfers.values().any(|t| matches!(t.st, St::Accepting { closing: true })) {
        s.g.drained = true;
        e.push(Effect::Drained);
    }
}

pub fn closing(s: &mut State, e: &mut Vec<Effect>) {
    if s.g.phase == Phase::Closed { row(e, "xfer_done_closing"); return; }
    if s.g.phase == Phase::Restoring {
        row(e, "xfer_restoring_closing");
    } else if s.transfers.is_empty() {
        row(e, "xfer_done_closing");
    } else {
        broadcast(s, &Input::Closing, e);
    }
    for h in std::mem::take(&mut s.g.held) { reply(e, h.reply, Answer::Err(Fault::Ended(EndCause::Closed))); }
    if !s.g.selecting.is_empty() { row(e, "xfer_select_epoch_end"); }
    for (r, _) in std::mem::take(&mut s.g.selecting) { reply(e, r, Answer::Err(Fault::Ended(EndCause::Closed))); }
    s.g.phase = Phase::Closed;
    maybe_drained(s, e);
}

pub fn odd_key(k: &TransferKey) -> bool {
    [&k.model, &k.row_id, &k.action].iter().any(|x| x.contains('/') || x.contains("..") || x.contains(':'))
}

pub fn invalid_item(it: &Item) -> bool {
    let c = &it.check;
    !c.action_declared || !c.has_file_field || (matches!(it.source, Source::File { .. }) && !c.source_valid)
}

pub fn accept(s: &mut State, epoch: u64, now: u64, item: Item, r: ReplyId, e: &mut Vec<Effect>) {
    s.g.now = now;
    if s.g.phase == Phase::Closed { row(e, "xfer_accept_after_closing"); reply(e, r, Answer::Err(Fault::Closed)); return; }
    if epoch != s.g.epoch || s.g.phase == Phase::Idle { row(e, "xfer_accept_stale"); reply(e, r, Answer::Err(Fault::NoSession)); return; }
    if s.g.phase == Phase::Restoring {
        row(e, "xfer_accept_while_restoring");
        s.g.held.push(Held { call: None, now, item: Some(item), batch: None, epoch, reply: r });
        return;
    }
    let fails = (find_open(s, &item.key).is_some() as u8) + (invalid_item(&item) as u8) + (!item.check.row_exists as u8);
    if fails > 1 { row(e, "xfer_accept_check_order"); }
    if find_open(s, &item.key).is_some() { row(e, "xfer_accept_key_open"); reply(e, r, Answer::Err(Fault::InFlight)); return; }
    if invalid_item(&item) { row(e, "xfer_accept_invalid"); reply(e, r, Answer::Err(Fault::Invalid)); return; }
    if !item.check.row_exists { row(e, "xfer_accept_row_missing"); reply(e, r, Answer::Err(Fault::NotFound)); return; }
    if odd_key(&item.key) { row(e, "xfer_key_path_chars"); }
    let has_effects = item.check.has_effects;
    let mut t = new_transfer(&mut s.g, item.key.clone(), item.source.clone(), epoch, now, None, 0, has_effects, item.values.clone());
    t.replies.accept = Some(r);
    e.push(Effect::Outbox(OutboxEffect::ReserveOp { epoch, target: Target::Transfer { transfer_id: t.id } }));
    row(e, if has_effects { "xfer_accept_reserve_op" } else { "xfer_accept" });
    s.transfers.insert(t.id, t);
}

pub fn accept_batch(s: &mut State, epoch: u64, now: u64, minimum: usize, maximum: usize, items: Vec<Item>, r: ReplyId, e: &mut Vec<Effect>) {
    s.g.now = now;
    if s.g.phase == Phase::Closed { row(e, "xfer_accept_after_closing"); reply(e, r, Answer::Err(Fault::Closed)); return; }
    if epoch != s.g.epoch || s.g.phase == Phase::Idle { row(e, "xfer_accept_batch_invalid"); reply(e, r, Answer::Err(Fault::NoSession)); return; }
    if s.g.phase == Phase::Restoring {
        row(e, "xfer_accept_while_restoring");
        s.g.held.push(Held { call: None, now, item: None, batch: Some((minimum, maximum, items)), epoch, reply: r });
        return;
    }
    if items.len() < minimum || items.len() > maximum { row(e, "xfer_accept_batch_length"); reply(e, r, Answer::Err(Fault::Invalid)); return; }
    if items.iter().any(|it| find_open(s, &it.key).is_some()) { row(e, "xfer_accept_key_open"); reply(e, r, Answer::Err(Fault::InFlight)); return; }
    if items.iter().any(invalid_item) { row(e, "xfer_accept_batch_invalid"); reply(e, r, Answer::Err(Fault::Invalid)); return; }
    if let Some(ix) = items.iter().position(|it| !it.check.row_exists) { row(e, "xfer_accept_batch_row_missing"); reply(e, r, Answer::Err(Fault::NotFoundAt(ix))); return; }
    if items.iter().any(|it| odd_key(&it.key)) { row(e, "xfer_key_path_chars"); }
    row(e, "xfer_accept_batch");
    let gid = s.g.next_group;
    s.g.next_group += 1;
    let mut ids = Vec::new();
    let mut need = BTreeSet::new();
    need.insert("g".to_string());
    for (n, it) in items.iter().enumerate() {
        let t = new_transfer(&mut s.g, it.key.clone(), it.source.clone(), epoch, now, Some(gid), n as u32, it.check.has_effects, it.values.clone());
        need.insert(format!("t{}", t.id));
        ids.push(t.id);
        e.push(Effect::Outbox(OutboxEffect::ReserveOp { epoch, target: Target::Transfer { transfer_id: t.id } }));
        s.transfers.insert(t.id, t);
    }
    e.push(Effect::Outbox(OutboxEffect::ReserveOp { epoch, target: Target::Group { batch_id: gid } }));
    s.g.groups.insert(gid, Group {
        items: ids, op_id: None, minimum, maximum, phase: GroupPhase::Collecting, accept_reply: Some(r), reserve: BTreeMap::new(), reserve_need: need,
        reserve_failed: false, renumber_start: 0, submit_pending: false, pack_ids: vec![], pack_op: None,
    });
}

pub fn settle(s: &mut State, id: TransferId) {
    if s.transfers.get(&id).map(|t| t.st == St::Done).unwrap_or(false) {
        if let Some(t) = s.transfers.remove(&id) { after(s, t); }
    }
}

pub fn restored(s: &mut State, epoch: u64, orphans: &Vec<(String, TransferKey)>, uploads: &Vec<RestoreItem>, outs: &BTreeMap<TransferId, BTreeMap<u64, Outcome>>, held_orphan: &Option<String>, e: &mut Vec<Effect>) {
    if s.g.phase != Phase::Restoring || epoch != s.g.epoch { row(e, "r2_restored_stray"); return; }
    row(e, "xfer_restored");
    s.g.phase = Phase::Classified;
    s.g.saved_outcomes = outs.clone();
    for (u, k) in orphans { s.g.orphans.insert(u.clone(), k.clone()); }
    if let Some(u) = held_orphan { if s.g.orphans.contains_key(u) { row(e, "xfer_restored_held_orphan_flight"); s.g.orphan_flight.insert(u.clone()); } else { row(e, "xfer_restored_held_orphan_without_record"); } }
    let mut idx: Vec<usize> = (0..uploads.len()).collect();
    idx.sort_by_key(|&k| match &uploads[k] {
        RestoreItem::Failed { seq, pos, .. } => (0u8, *seq, *pos),
        RestoreItem::Row(r) => (if declared(&s.g, &r.key) { 1u8 } else { 0u8 }, r.seq, r.pos),
    });
    for k in idx { classify(s, &uploads[k], outs, e); }
    let gids: Vec<GroupId> = s.g.groups.keys().cloned().collect();
    for gid in gids { s.g.post.push(Post::GroupCheck { gid, from_complete: false, removal: false }); }
    run_post(s, e);
    e.push(Effect::RestoreClassified { epoch });
    let held = std::mem::take(&mut s.g.held);
    let mut calls = Vec::new();
    for h in held {
        if let Some(it) = h.item { accept(s, h.epoch, h.now, it, h.reply, e); }
        else if let Some((mn, mx, items)) = h.batch { accept_batch(s, h.epoch, h.now, mn, mx, items, h.reply, e); }
        else if h.call.is_some() { calls.push(h); }
    }
    s.g.held = calls;
}

pub fn stored_to_transfer(g: &mut G, r: &StoredTransfer) -> Transfer {
    let epoch = g.epoch;
    let mut t = new_transfer(g, r.key.clone(), Source::File { path: String::new() }, epoch, r.created_at, r.group.as_ref().map(|x| x.batch_id), r.pos, r.has_effects, vec![]);
    g.next_transfer = g.next_transfer.max(r.id + 1);
    t.id = r.id;
    t.op_id = Some(r.op_id);
    t.seq = r.seq;
    t.cycle_id = r.cycle_id;
    t.accepted_at = r.accepted_at;
    t.size = r.size;
    t.sha256 = if r.sha256.is_empty() { None } else { Some(r.sha256.clone()) };
    t.parts = r.parts;
    t.upload_id = r.upload_id.clone();
    t.confirmed = r.confirmed.clone();
    t.attempts = r.attempts;
    t.complete_attempts = r.complete_attempts;
    t.signed_id = r.signed_id.clone();
    t.error = r.error.clone();
    t.copy_present = r.copy_present;
    t.intent_live = r.intent_live;
    t.aborted = r.aborted;
    t.local_url = r.copy_present;
    t
}

pub fn classify(s: &mut State, item: &RestoreItem, outs: &BTreeMap<TransferId, BTreeMap<u64, Outcome>>, e: &mut Vec<Effect>) {
    match item {
        RestoreItem::Failed { id, key, error, seq, pos, .. } => {
            row(e, "xfer_open_restore_failed");
            let epoch = s.g.epoch;
            let mut t = new_transfer(&mut s.g, key.clone(), Source::Pending, epoch, 0, None, *pos, false, vec![]);
            s.g.next_transfer = s.g.next_transfer.max(*id + 1);
            t.id = *id;
            t.seq = *seq;
            t.st = St::Failed;
            let er = Failure { kind: ErrKind::RestoreFailed, message: error.message.clone() };
            t.error = Some(er.clone());
            fix_outcome(&mut t, e, Outcome::Failed { error: er });
            t.stored_shown = Some(shown(&s.g, &t));
            s.transfers.insert(t.id, t);
        }
        RestoreItem::Row(r) => {
            let mut t = stored_to_transfer(&mut s.g, r);
            classify_row(&mut s.g, &mut t, r, item, outs, e);
            t.stored_shown = Some(shown(&s.g, &t));
            if t.st == St::Done { after(s, t); } else { s.transfers.insert(t.id, t); }
        }
    }
}
