use super::*;

pub fn unknown_shown() -> Shown { Shown { status: Status::Gone, wait_reason: None, sent_bytes: 0, total_bytes: 0, error: None, local_url: false, round_error: None } }

pub fn owner_addressed(s: &mut State, i: &Input, e: &mut Vec<Effect>) {
    if s.g.phase == Phase::Restoring || s.g.phase == Phase::Classified {
        let held = match i {
            Input::SupplySource { epoch, reply, .. } | Input::SourceFailed { epoch, reply, .. } | Input::Cancel { epoch, reply, .. } | Input::Discard { epoch, reply, .. } | Input::Retry { epoch, reply, .. } if *epoch == s.g.epoch => Some((*epoch, *reply)),
            Input::Outcome { reply, .. } => Some((s.g.epoch, *reply)),
            _ => None,
        };
        if let Some((epoch, reply)) = held {
            row(e, "xfer_calls_held_while_restoring");
            let now = s.g.now;
            s.g.held.push(Held { call: Some(Box::new(i.clone())), now, item: None, batch: None, epoch, reply });
            return;
        }
    }
    match i {
        Input::SupplySource { epoch, key, reply: r, .. } | Input::SourceFailed { epoch, key, reply: r, .. } => {
            if *epoch != s.g.epoch { row(e, "xfer_supply_stale_epoch"); reply(e, *r, Answer::Err(Fault::NoSession)); return; }
            match find_open(s, key) { Some(id) => run_on(s, id, i, e), None => { row(e, "xfer_supply_no_transfer"); reply(e, *r, Answer::Err(Fault::NotFound)); } }
        }
        Input::Cancel { epoch, key, reply: r } | Input::Discard { epoch, key, reply: r } | Input::Retry { epoch, key, reply: r, .. } => {
            if *epoch != s.g.epoch { row(e, "r4b_no_session"); reply(e, *r, Answer::Err(Fault::NoSession)); return; }
            match find_open(s, key) { Some(id) => run_on(s, id, i, e), None => { row(e, "xfer_done"); row(e, "boundary_transfer_selector"); reply(e, *r, Answer::Err(Fault::NotFound)); } }
        }
        Input::SelectRow { epoch, model, row_id, retry, source: _, outbox_min, reply: r, version } => {
            if *epoch != s.g.epoch { row(e, "r4b_no_session"); reply(e, *r, Answer::Err(Fault::NoSession)); return; }
            if s.g.selecting.remove(r).is_some() { row(e, "xfer_select_replaced"); }
            let mut best: Option<(OpId, TransferId)> = None;
            for t in s.transfers.values() {
                if t.key.model == *model && t.key.row_id == *row_id {
                    if let Some(op) = t.op_id {
                        if outbox_min.map(|m| op <= m).unwrap_or(true) && best.as_ref().map(|b| op < b.0).unwrap_or(true) { best = Some((op, t.id)); }
                    }
                }
            }
            match best {
                Some((_, id)) => {
                    row(e, "xfer_select_transfer");
                    s.g.selecting.insert(*r, (id, *retry));
                    e.push(Effect::Outbox(OutboxEffect::SelectedTransfer { reply: *r, transfer_id: id, version: *version }));
                }
                None => {
                    row(e, "boundary_selector_without_action");
                    if outbox_min.is_some() { e.push(Effect::Outbox(OutboxEffect::SelectedOutbox { reply: *r, version: *version })); } else { e.push(Effect::Outbox(OutboxEffect::SelectedNone { reply: *r, version: *version })); }
                }
            }
        }
        Input::SelectConfirmed { reply: r } => {
            match s.g.selecting.remove(r) {
                Some((transfer_id, retry)) => {
                    row(e, "xfer_select_confirmed");
                    if let Some(key) = s.transfers.get(&transfer_id).map(|t| t.key.clone()) {
                        let inner = if retry { Input::Retry { epoch: s.g.epoch, key, source: None, reply: *r } } else { Input::Discard { epoch: s.g.epoch, key, reply: *r } };
                        run_on(s, transfer_id, &inner, e);
                    }
                }
                None => row(e, "xfer_select_confirmed_stray"),
            }
        }
        Input::Outcome { transfer_id, saved, reply: r, .. } => {
            if s.transfers.contains_key(transfer_id) { run_on(s, *transfer_id, i, e); }
            else {
                match saved {
                    Some(o) => { row(e, "xfer_done_outcome"); reply(e, *r, Answer::Outcome(o.clone())); }
                    None => { row(e, "xfer_outcome_unknown"); reply(e, *r, Answer::Err(Fault::NotFound)); }
                }
            }
        }
        Input::InterestPlus { transfer_id, stream_id } => {
            if s.transfers.contains_key(transfer_id) { run_on(s, *transfer_id, i, e); }
            else {
                row(e, "xfer_interest_unknown");
                s.g.subs.entry(*transfer_id).or_default().insert(*stream_id);
                e.push(Effect::Status { stream_id: *stream_id, transfer_id: *transfer_id, shown: unknown_shown() });
            }
        }
        Input::InterestMinus { transfer_id, stream_id } => {
            let had = s.g.subs.get(transfer_id).map(|x| x.contains(stream_id)).unwrap_or(false);
            if had {
                row(e, "xfer_interest_minus");
                if let Some(x) = s.g.subs.get_mut(transfer_id) { x.remove(stream_id); }
            } else { row(e, "xfer_interest_minus_unknown"); }
        }
        Input::StatusInterestPlus { stream_id, key } => {
            row(e, "xfer_status_by_key");
            s.g.key_subs.entry(key.clone()).or_default().insert(*stream_id);
            let cur = find_open(s, key).and_then(|id| s.transfers.get(&id)).and_then(|t| t.stored_shown.clone().map(|sh| (t.id, sh)));
            match cur {
                Some((id, sh)) => e.push(Effect::Status { stream_id: *stream_id, transfer_id: id, shown: sh }),
                None => e.push(Effect::Status { stream_id: *stream_id, transfer_id: 0, shown: unknown_shown() }),
            }
        }
        Input::StatusInterestMinus { stream_id, key } => {
            row(e, "xfer_status_by_key");
            if let Some(x) = s.g.key_subs.get_mut(key) { x.remove(stream_id); }
        }
        Input::AbortDone { upload_id, result } => abort_done(&mut s.g, upload_id, result, e),
        Input::UploadMinted { transfer_id, upload_id, key, .. } => upload_minted(s, *transfer_id, upload_id, key, e),
        Input::OpReserved { target: Target::Group { batch_id }, result } => {
            let gid = *batch_id;
            match s.g.groups.get_mut(&gid) {
                None => row(e, "r2_op_reserved_stray"),
                Some(gr) => {
                    match result {
                        Ok(op) => { gr.op_id = Some(*op); gr.reserve.insert("g".to_string(), Some(*op)); }
                        Err(f) => { gr.reserve_failed = true; if let Some(r) = gr.accept_reply.take() { reply(e, r, Answer::Err(f.clone())); } }
                    }
                    s.g.post.push(Post::Reserve(gid));
                    run_post(s, e);
                }
            }
        }
        Input::MutationOutcome(MutTarget::Group { batch_id, outcomes }) => {
            let gid = *batch_id;
            let ids: Vec<TransferId> = s.g.groups.get(&gid).map(|g| g.pack_ids.clone()).unwrap_or_default();
            if ids.is_empty() { row(e, "r2_group_outcome_stray"); return; }
            let all_conf = outcomes.values().all(|o| matches!(o, MutOut::Confirmed { .. }));
            let all_bad = outcomes.values().all(|o| matches!(o, MutOut::Rejected { .. } | MutOut::Failed { .. }));
            row(e, if all_conf { "xfer_batch_confirmed" } else if all_bad { "xfer_batch_failed" } else { "xfer_batch_mixed_outcomes" });
            s.g.quiet = true;
            for id in ids {
                if let Some(o) = outcomes.get(&id) { run_on(s, id, &Input::MutationOutcome(MutTarget::Transfer { transfer_id: id, outcome: o.clone() }), e); }
            }
            s.g.quiet = false;
        }
        Input::MutationOutcome(MutTarget::Transfer { transfer_id, .. })
        | Input::DiscardOutcome { transfer_id, .. }
        | Input::CopyDone { transfer_id, .. }
        | Input::CopyFailed { transfer_id, .. }
        | Input::StepOk { transfer_id, .. }
        | Input::StepError { transfer_id, .. }
        | Input::Completed { transfer_id, .. }
        | Input::CompleteError { transfer_id, .. } => run_on(s, *transfer_id, i, e),
        Input::RetryOutcome { target, .. } => {
            let id = match target { Target::Transfer { transfer_id } => Some(*transfer_id), Target::Group { batch_id } => s.g.groups.get(batch_id).and_then(|g| g.items.first().cloned()) };
            match id { Some(id) if s.transfers.contains_key(&id) => run_on(s, id, i, e), _ => row(e, "xfer_retry_mutation_stale") }
        }
        Input::OpReserved { target: Target::Transfer { transfer_id }, .. } => run_on(s, *transfer_id, i, e),
        _ => row(e, "r2_unrouted_input"),
    }
}

pub fn upload_minted(s: &mut State, id: TransferId, uid: &str, key: &TransferKey, e: &mut Vec<Effect>) {
    let dup = s.g.orphans.contains_key(uid) || s.g.minted.contains_key(uid) || s.transfers.get(&id).map(|t| t.upload_id.as_deref() == Some(uid)).unwrap_or(false);
    if dup { row(e, "xfer_upload_minted_dup"); return; }
    let live = s.transfers.get(&id).map(|t| !matches!(t.st, St::Removing { .. } | St::Done)).unwrap_or(false);
    let b = s.g.next_batch;
    s.g.next_batch += 1;
    s.g.batches.insert(b, BatchKind::Minted { upload_id: uid.to_string() });
    if live {
        row(e, "xfer_upload_minted");
        if let Some(t) = s.transfers.get_mut(&id) { t.upload_id = Some(uid.to_string()); }
        s.g.minted.insert(uid.to_string(), Minted { transfer_id: id, attempt_id: 0, is_orphan: false, key: key.clone(), in_flight: Some(b), debt: true });
        e.push(Effect::Store { batch: b, generation: s.g.generation, basis: None, ops: vec![Op::UploadId { upload_id: uid.to_string() }] });
    } else {
        row(e, "xfer_upload_minted_late");
        s.g.minted.insert(uid.to_string(), Minted { transfer_id: id, attempt_id: 0, is_orphan: true, key: key.clone(), in_flight: Some(b), debt: true });
        e.push(Effect::Store { batch: b, generation: s.g.generation, basis: None, ops: vec![Op::UploadOrphan { upload_id: uid.to_string(), key: key.clone() }] });
    }
}

pub fn run_post(s: &mut State, e: &mut Vec<Effect>) {
    while !s.g.post.is_empty() {
        let p = s.g.post.remove(0);
        match p {
            Post::Reserve(gid) => post_reserve(s, gid, e),
            Post::GroupCheck { gid, from_complete, removal } => group_check(s, gid, from_complete, removal, e),
            Post::CopyBatch(gid) => post_copy_batch(s, gid, e),
            Post::CopyFailedBatch { gid, failing, error } => post_copy_failed_batch(s, gid, failing, &error, e),
            Post::Receipts(gid) => post_receipts(s, gid, e),
            Post::SinglesNext(gid, done) => post_singles_next(s, gid, done, e),
        }
    }
}

pub fn post_reserve(s: &mut State, gid: GroupId, e: &mut Vec<Effect>) {
    let gr = match s.g.groups.get(&gid) { Some(g) => g.clone(), None => return };
    if gr.reserve_failed {
        row(e, "xfer_reserving_refused");
        for id in gr.items.iter() {
            if let Some(t) = s.transfers.get_mut(id) { if t.st == St::Reserving { t.st = St::Done; } }
            settle(s, *id);
        }
        s.g.groups.remove(&gid);
        return;
    }
    let ready = gr.reserve_need.iter().all(|k| matches!(gr.reserve.get(k), Some(Some(_))));
    if !ready { row(e, "xfer_reserving_batch_wait"); return; }
    row(e, "xfer_accept_optimistic_row");
    for id in gr.items.iter() {
        let g = &mut s.g;
        if let Some(t) = s.transfers.get_mut(id) {
            if t.st != St::Reserving { continue; }
            t.st = St::Accepting { closing: false };
            let op = t.op_id.unwrap_or(0);
            let mut ops = vec![Op::UploadCopying { op_id: op, source_path: match &t.source { Source::File { path } => Some(path.clone()), Source::Pending => None } }];
            if t.has_effects { ops.push(Op::Effects { op_id: op }); }
            commit_group(g, t.id, format!("acc{}", gid), ops);
        }
    }
}

pub fn group_check(s: &mut State, gid: GroupId, from_complete: bool, removal: bool, e: &mut Vec<Effect>) {
    let gr = match s.g.groups.get(&gid) { Some(g) => g.clone(), None => return };
    let members: Vec<TransferId> = gr.items.iter().cloned().filter(|id| s.transfers.contains_key(id)).collect();
    if members.is_empty() { return; }
    let all_ready = members.iter().all(|id| s.transfers[id].st == St::BatchReady);
    if !all_ready { if from_complete { row(e, "xfer_batch_item_ready"); } return; }
    if gr.phase != GroupPhase::Collecting { return; }
    let origin_op = gr.op_id.unwrap_or(0);
    if members.len() >= gr.minimum {
        row(e, if removal { "xfer_batch_removal_releases_mutation" } else { "xfer_batch_mutation" });
        if let Some(g) = s.g.groups.get_mut(&gid) { g.pack_ids = members.clone(); g.pack_op = Some(origin_op); g.submit_pending = true; }
        for id in members.iter() {
            if let Some(t) = s.transfers.get_mut(id) { t.st = St::PersistingBatchIntent { single: false, retry: None }; }
            commit_group(&mut s.g, *id, format!("bi{}", gid), vec![Op::Intent { op_id: origin_op, origin: OpOrigin::TransferGroup { batch_id: gid, transfer_ids: members.clone() } }]);
        }
    } else {
        row(e, if removal { "xfer_batch_removal_releases_mutation" } else { "xfer_batch_singles" });
        let first = members[0];
        let op = s.transfers[&first].op_id.unwrap_or(0);
        if let Some(t) = s.transfers.get_mut(&first) { t.st = St::PersistingBatchIntent { single: true, retry: None }; }
        commit_group(&mut s.g, first, format!("bi{}", gid), vec![Op::Intent { op_id: op, origin: OpOrigin::Transfer { transfer_id: first } }]);
    }
}

pub fn post_copy_batch(s: &mut State, gid: GroupId, e: &mut Vec<Effect>) {
    let gr = match s.g.groups.get(&gid) { Some(g) => g.clone(), None => return };
    let copying = gr.items.iter().any(|id| matches!(s.transfers.get(id).map(|t| &t.st), Some(St::Copying { supply: None, .. })));
    if copying { row(e, "xfer_copy_done_batch_wait"); return; }
    row(e, "xfer_copy_done_batch_last");
    for id in gr.items.iter() {
        let g = &mut s.g;
        if let Some(t) = s.transfers.get_mut(id) {
            if !matches!(t.st, St::CopyReady { .. }) { continue; }
            let cached = matches!(t.cached, Some(Cached::Fresh { .. }));
            t.st = St::PersistingCopy { supply: None, cached, new_cycle: false };
            let mut ops = vec![Op::SyncUpload { size: t.size, sha256: t.sha256.clone().unwrap_or_default(), variables: false }];
            match t.cached.clone() {
                Some(Cached::Fresh { blob_id }) => { t.signed_id = Some(blob_id.clone()); ops.push(Op::Completed { signed_id: blob_id }); }
                Some(Cached::Stale) => ops.push(Op::BlobCacheRemove { sha256: t.sha256.clone().unwrap_or_default() }),
                _ => {}
            }
            commit_group(g, t.id, format!("pc{}", gid), ops);
        }
    }
}

pub fn post_copy_failed_batch(s: &mut State, gid: GroupId, failing: TransferId, error: &str, e: &mut Vec<Effect>) {
    let gr = match s.g.groups.get(&gid) { Some(g) => g.clone(), None => return };
    for id in gr.items.iter() {
        if *id == failing { continue; }
        let g = &mut s.g;
        if let Some(t) = s.transfers.get_mut(id) {
            if !matches!(t.st, St::Copying { supply: None, .. } | St::CopyReady { .. }) { continue; }
            e.push(Effect::CopyCancel { transfer_id: t.id });
            let prior = t.st.clone();
            t.st = St::Removing { why: Why::CopyFailed(error.to_string()), prior: Box::new(prior), orphan: None };
            let mut ops = vec![Op::RemoveUpload { transfer_id: t.id }];
            ops.extend(rollback_op(t));
            commit_group(g, t.id, format!("cf{}", gid), ops);
        }
    }
}

pub fn post_receipts(s: &mut State, gid: GroupId, e: &mut Vec<Effect>) {
    let gr = match s.g.groups.get(&gid) { Some(g) => g.clone(), None => return };
    if gr.accept_reply.is_none() { return; }
    let pending = gr.items.iter().any(|id| match s.transfers.get(id).map(|t| &t.st) {
        Some(St::Reserving) | Some(St::Accepting { .. }) | Some(St::CopyReady { .. }) | Some(St::PersistingCopy { supply: None, .. }) | Some(St::Copying { supply: None, .. }) => true,
        _ => false,
    });
    if pending { return; }
    let rs: Vec<Receipt> = gr.items.iter().filter_map(|id| s.transfers.get(id)).map(receipt).collect();
    if let Some(g) = s.g.groups.get_mut(&gid) { if let Some(r) = g.accept_reply.take() { reply(e, r, Answer::Receipts(rs)); } }
}

pub fn post_singles_next(s: &mut State, gid: GroupId, done: TransferId, e: &mut Vec<Effect>) {
    let gr = match s.g.groups.get(&gid) { Some(g) => g.clone(), None => return };
    let pos = gr.items.iter().position(|x| *x == done).map(|p| p + 1).unwrap_or(0);
    let next = gr.items.iter().skip(pos).cloned().find(|id| s.transfers.get(id).map(|t| t.st == St::BatchReady).unwrap_or(false));
    if let Some(n) = next {
        row(e, "xfer_batch_singles_next");
        let op = s.transfers[&n].op_id.unwrap_or(0);
        if let Some(t) = s.transfers.get_mut(&n) { t.st = St::PersistingBatchIntent { single: true, retry: None }; }
        commit_group(&mut s.g, n, format!("bs{}", n), vec![Op::Intent { op_id: op, origin: OpOrigin::Transfer { transfer_id: n } }]);
    }
}

pub fn cm(g: &mut G, t: &Transfer, e: &mut Vec<Effect>, prefix: &str, ops: Vec<Op>) {
    match t.batch {
        Some(gid) => commit_group(g, t.id, format!("{}{}", prefix, gid), ops),
        None => { commit(g, t, e, ops); }
    }
}

pub fn stray_row(st: &St) -> &'static str {
    match st {
        St::Reserving => "r2_reserving_stray",
        St::Accepting { .. } => "r2_accepting_stray",
        St::AwaitingSource => "xfer_pending_stray",
        St::Copying { .. } => "xfer_copy_other",
        St::CopyReady { .. } => "xfer_copy_ready_other",
        St::WaitingDeclaration { .. } => "xfer_waiting_declaration_other",
        St::PersistingCopy { .. } | St::PersistingRetry { .. } | St::PersistingFailure { .. } | St::Removing { .. } | St::PersistingPart { .. } | St::PersistingComplete => "xfer_persisting_other",
        St::PersistingAttempt { .. } => "xfer_attempt_other",
        St::PersistingBatchIntent { .. } => "xfer_batch_intent_other",
        St::BatchReady => "xfer_batch_ready_other",
        St::Mutating => "xfer_mutating_other",
        St::Failed => "xfer_failed_other",
        St::Ready { .. } => "xfer_ready_other",
        St::Uploading { .. } => "xfer_uploading_other",
        St::Completing { .. } => "xfer_completing_other",
        St::Done => "xfer_done",
    }
}
