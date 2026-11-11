use super::*;

pub fn tx(g: &mut G, t: &mut Transfer, i: &Input, e: &mut Vec<Effect>) {
    match i {
        Input::Online(_) | Input::PowerSaving(_) | Input::SlotFreed => tx_signal(g, t, i, e),
        Input::RegisterOperations { .. } => tx_register(g, t, e),
        Input::RestoreReady { .. } => tx_awaiting_only(g, t, i, e),
        Input::Tick { kind, .. } => tx_tick(g, t, g.now, kind, e),
        Input::Cancel { reply, .. } => tx_cancel(g, t, *reply, true, e),
        Input::Discard { reply, .. } => tx_cancel(g, t, *reply, false, e),
        Input::Retry { source, reply, .. } => tx_retry(g, t, source, *reply, e),
        Input::SupplySource { source, reply, .. } => tx_supply(g, t, source, *reply, e),
        Input::SourceFailed { message, reply, .. } => tx_source_failed(g, t, message, *reply, e),
        Input::OpReserved { result, .. } => tx_op_reserved(g, t, result, e),
        Input::CopyDone { attempt_id, size, sha256, cached, .. } => tx_copy_done(g, t, *attempt_id, *size, sha256, cached, e),
        Input::CopyFailed { attempt_id, error, .. } => tx_copy_failed(g, t, *attempt_id, error, e),
        Input::StepOk { attempt_id, part, etag, upload_id, .. } => tx_step_ok(g, t, *attempt_id, *part, etag, upload_id, e),
        Input::StepError { attempt_id, temporary, error, retained, .. } => tx_step_error(g, t, *attempt_id, *temporary, error, *retained, e),
        Input::Completed { attempt_id, signed_id, .. } => tx_completed(g, t, *attempt_id, signed_id, e),
        Input::CompleteError { attempt_id, temporary, error, .. } => tx_complete_error(g, t, *attempt_id, *temporary, error, e),
        Input::MutationOutcome(MutTarget::Transfer { outcome, .. }) => tx_mutation_outcome(g, t, outcome, e),
        Input::DiscardOutcome { result, .. } => tx_discard_outcome(g, t, result, e),
        Input::RetryOutcome { target, result } => tx_retry_outcome(g, t, target, result, e),
        Input::Outcome { cycle_id, saved, reply, .. } => tx_outcome(t, *cycle_id, saved, *reply, e),
        Input::InterestPlus { stream_id, .. } => {
            row(e, if t.st == St::AwaitingSource { "xfer_pending_interest" } else { "xfer_interest" });
            g.subs.entry(t.id).or_default().insert(*stream_id);
            e.push(Effect::Status { stream_id: *stream_id, transfer_id: t.id, shown: t.stored_shown.clone().unwrap_or_else(unknown_shown) });
        }
        Input::RoundStatus { refused, .. } => tx_round_status(g, t, refused, e),
        Input::Committed { .. } | Input::Rejected { .. } => tx_store(g, t, i, e),
        Input::EpochEnded { cause, .. } => tx_end(g, t, e, *cause == EndCause::Closed),
        Input::Closing => tx_end(g, t, e, true),
        _ => row(e, "r2_unrouted_tx"),
    }
}

pub fn expire_awaiting(g: &mut G, t: &mut Transfer, e: &mut Vec<Effect>, name: &'static str) {
    row(e, name);
    begin_remove(g, t, e, Why::Cancel, None, false, false, false, None);
}

pub fn tx_awaiting_only(g: &mut G, t: &mut Transfer, i: &Input, e: &mut Vec<Effect>) {
    if matches!(i, Input::RestoreReady { .. }) && t.intent_after_ready {
        t.intent_after_ready = false;
        commit(g, t, e, vec![Op::Intent { op_id: t.op_id.unwrap_or(0), origin: OpOrigin::Transfer { transfer_id: t.id } }]);
        return;
    }
    if t.st != St::AwaitingSource { return; }
    if matches!(i, Input::RestoreReady { .. }) { row(e, "xfer_awaiting_source_expires"); e.push(Effect::Schedule { kind: TickKind::Expire, delay: Delay::Now }); }
    else { row(e, "xfer_pending_signals"); }
}

pub fn tx_register(g: &mut G, t: &mut Transfer, e: &mut Vec<Effect>) {
    match t.st.clone() {
        St::WaitingDeclaration { item } => {
            if declared(g, &t.key) {
                row(e, "xfer_register_rechecks_waiting");
                if let RestoreItem::Row(r) = *item {
                    let outs = g.saved_outcomes.clone();
                    let it = RestoreItem::Row(r.clone());
                    classify_row(g, t, &r, &it, &outs, e);
                }
            } else { row(e, "xfer_register_still_waiting"); }
        }
        St::AwaitingSource => row(e, "xfer_pending_signals"),
        _ => row(e, "xfer_register_other"),
    }
}

pub fn tx_signal(g: &mut G, t: &mut Transfer, i: &Input, e: &mut Vec<Effect>) {
    let on = matches!(i, Input::Online(true));
    let off = matches!(i, Input::Online(false));
    let ps_t = matches!(i, Input::PowerSaving(true));
    match t.st.clone() {
        St::AwaitingSource => {
            if on { row(e, "xfer_awaiting_source_expires"); e.push(Effect::Schedule { kind: TickKind::Expire, delay: Delay::Now }); }
            else { row(e, "xfer_pending_signals"); }
        }
        St::Reserving => row(e, "xfer_reserving_signals"),
        St::Accepting { .. } => row(e, "xfer_accepting_signals"),
        St::Ready { next } => ready_signal(g, t, e, next, on, off, ps_t),
        St::Uploading { step, attempt, waiting_retry } => {
            if off || ps_t {
                row(e, if off { "xfer_step_offline" } else { "xfer_step_power_saving" });
                if !waiting_retry {
                    row(e, "xfer_put_cancelled");
                    e.push(Effect::TransportCancel { transfer_id: t.id, attempt_id: attempt });
                    cancel_put(e, t.id, attempt);
                }
                t.st = St::Ready { next: step_kind_next(step) };
                e.push(Effect::SelfSlotFreed);
            } else { row(e, "xfer_step_online"); }
        }
        St::Completing { attempt, waiting_retry } => {
            if off || ps_t {
                row(e, "xfer_complete_offline");
                if !waiting_retry { e.push(Effect::TransportCancel { transfer_id: t.id, attempt_id: attempt }); }
                t.st = St::Ready { next: Next::Complete };
                e.push(Effect::SelfSlotFreed);
            } else { row(e, "xfer_completing_other"); }
        }
        St::Failed => {
            if on { row(e, "xfer_failed_online"); e.push(Effect::Schedule { kind: TickKind::Expire, delay: Delay::Now }); }
            else { row(e, "xfer_failed_other"); }
        }
        St::Done => {}
        other => row(e, stray_row(&other)),
    }
}

pub fn ready_signal(g: &mut G, t: &mut Transfer, e: &mut Vec<Effect>, next: Next, on: bool, off: bool, ps_t: bool) {
    if off { row(e, "xfer_ready_offline"); return; }
    if ps_t { row(e, "xfer_ready_wait"); return; }
    let _ = on;
    if g.phase != Phase::Ready || g.online != Some(true) || g.saving { row(e, "xfer_ready_wait"); return; }
    let step = match next {
        Next::Part(1) if t.size < g.multipart_threshold => StepKind::CreateBlob,
        Next::Part(n) => StepKind::Part(n),
        Next::Complete => StepKind::Complete,
        Next::Mutation => unreachable!("Ready holds Part or Complete, never Mutation"),
    };
    if counter(t, step) >= ATTEMPT_LIMIT {
        row(e, "xfer_ready_attempts_exhausted");
        let abort = t.upload_id.clone();
        let mut ops = vec![Op::Failed { clear_upload: abort.is_some() }];
        if let Some(u) = &abort { ops.push(Op::UploadOrphan { upload_id: u.clone(), key: t.key.clone() }); }
        free_held(t, e);
        t.st = St::PersistingFailure { error: fail(ErrKind::Exhausted, "attempts exhausted"), abort };
        commit(g, t, e, ops);
        return;
    }
    if g.slots_used >= g.file_slots { row(e, "xfer_ready_wait"); return; }
    g.slots_used += 1;
    let attempt = g.next_attempt;
    g.next_attempt += 1;
    if step == StepKind::Complete { t.complete_attempts += 1; } else { t.attempts += 1; }
    row(e, match step { StepKind::CreateBlob => "xfer_ready_start_small", StepKind::Part(_) => "xfer_ready_start", StepKind::Complete => "xfer_ready_start_complete" });
    t.st = St::PersistingAttempt { step, attempt };
    commit(g, t, e, vec![Op::Attempt { attempts: counter(t, step) }]);
}

pub fn tx_tick(g: &mut G, t: &mut Transfer, now: u64, kind: &TickKind, e: &mut Vec<Effect>) {
    match kind {
        TickKind::Expire => tick_expire(g, t, now, e),
        TickKind::Retry { attempt_id, .. } => tick_retry(g, t, *attempt_id, e),
        TickKind::PutTimeout { attempt_id, .. } => tick_put(g, t, *attempt_id, e),
        TickKind::MintedWrite { .. } => {}
    }
}

pub fn tick_expire(g: &mut G, t: &mut Transfer, now: u64, e: &mut Vec<Effect>) {
    let age = now.saturating_sub(t.created_at);
    match t.st.clone() {
        St::Failed => {
            let Some(failed_age) = t.failed_at.map(|at| now.saturating_sub(at)) else { row(e, "xfer_not_expired"); return; };
            if failed_age > EXPIRY_MS { row(e, "xfer_expired"); failed_remove(g, t, None, true, e); }
            else if failed_age == EXPIRY_MS { row(e, "xfer_expiry_exact_week_keeps"); }
            else { row(e, "xfer_not_expired"); }
        }
        St::AwaitingSource => {
            if age > EXPIRY_MS { expire_awaiting(g, t, e, "xfer_pending_expired"); } else { row(e, "xfer_pending_not_expired"); }
        }
        _ => {}
    }
}

pub fn tick_retry(g: &mut G, t: &mut Transfer, aid: AttemptId, e: &mut Vec<Effect>) {
    match t.st.clone() {
        St::Uploading { step, attempt, waiting_retry: true } if attempt == aid => {
            if g.online == Some(true) {
                row(e, "xfer_step_tick");
                t.st = St::Ready { next: step_kind_next(step) };
                e.push(Effect::SelfSlotFreed);
            } else { row(e, "xfer_step_tick_other"); }
        }
        St::Uploading { .. } => row(e, "xfer_step_tick_other"),
        St::Completing { attempt, waiting_retry: true } if attempt == aid && g.online == Some(true) => {
            row(e, "xfer_complete_tick");
            t.st = St::Ready { next: Next::Complete };
            e.push(Effect::SelfSlotFreed);
        }
        St::Completing { .. } => row(e, "xfer_completing_stray"),
        St::PersistingFailure { error: _, abort } if t.write_retry == Some(aid) => {
            row(e, "xfer_persisting_failure_tick");
            t.write_retry = None;
            let mut ops = vec![Op::Failed { clear_upload: abort.is_some() }];
            if let Some(u) = &abort { ops.push(Op::UploadOrphan { upload_id: u.clone(), key: t.key.clone() }); }
            commit(g, t, e, ops);
        }
        St::PersistingBatchIntent { single, retry: Some(a) } if a == aid => {
            row(e, "xfer_batch_intent_retry_storage");
            let op = t.op_id.unwrap_or(0);
            let origin = if single { OpOrigin::Transfer { transfer_id: t.id } } else {
                let ids = t.batch.and_then(|gid| g.groups.get(&gid)).map(|gr| gr.pack_ids.clone()).unwrap_or_default();
                OpOrigin::TransferGroup { batch_id: t.batch.unwrap_or(0), transfer_ids: ids }
            };
            let op = if single { op } else { t.batch.and_then(|gid| g.groups.get(&gid)).and_then(|gr| gr.pack_op).unwrap_or(op) };
            commit(g, t, e, vec![Op::Intent { op_id: op, origin }]);
        }
        other => row(e, stray_row(&other)),
    }
}

pub fn tick_put(g: &mut G, t: &mut Transfer, aid: AttemptId, e: &mut Vec<Effect>) {
    match t.st.clone() {
        St::Uploading { step, attempt, waiting_retry: false } if attempt == aid => {
            row(e, "xfer_put_timeout");
            e.push(Effect::TransportCancel { transfer_id: t.id, attempt_id: attempt });
            uploading_step_error(g, t, step, attempt, true, fail(ErrKind::Step("put_timeout".into()), "put_timeout"), false, e);
        }
        St::Uploading { .. } => row(e, "xfer_put_timeout_other"),
        St::AwaitingSource => row(e, "xfer_pending_stray"),
        St::Done => {}
        _ => row(e, "xfer_put_timeout_other"),
    }
}

pub fn uploading_step_error(g: &mut G, t: &mut Transfer, step: StepKind, attempt: AttemptId, temporary: bool, error: Failure, retained: bool, e: &mut Vec<Effect>) {
    if temporary && g.online != Some(true) { row(e, "xfer_step_temp_offline"); return; }
    cancel_put(e, t.id, attempt);
    if retained { t.held_step = Some(req_id_of(t, step)); }
    if temporary && g.online == Some(true) && counter(t, step) < ATTEMPT_LIMIT {
        row(e, "xfer_step_temp");
        let na = g.next_attempt;
        g.next_attempt += 1;
        t.st = St::Uploading { step, attempt: na, waiting_retry: true };
        e.push(Effect::Schedule { kind: TickKind::Retry { transfer_id: t.id, attempt_id: na }, delay: Delay::Backoff { attempt: counter(t, step) } });
        e.push(Effect::SelfSlotFreed);
    } else {
        row(e, "xfer_step_fail");
        free_held(t, e);
        let abort = t.upload_id.clone();
        let mut ops = vec![Op::Failed { clear_upload: true }];
        if let Some(u) = &abort { ops.push(Op::UploadOrphan { upload_id: u.clone(), key: t.key.clone() }); }
        t.st = St::PersistingFailure { error, abort };
        commit(g, t, e, ops);
        e.push(Effect::SelfSlotFreed);
    }
}

pub fn tx_step_error(g: &mut G, t: &mut Transfer, aid: AttemptId, temporary: bool, error: &Failure, retained: bool, e: &mut Vec<Effect>) {
    match t.st.clone() {
        St::Uploading { step, attempt, waiting_retry: false } if attempt == aid => uploading_step_error(g, t, step, attempt, temporary, error.clone(), retained, e),
        St::Uploading { .. } => row(e, "xfer_uploading_stray"),
        St::Ready { .. } => {
            if temporary && g.online != Some(true) { row(e, "xfer_step_temp_offline"); } else { row(e, "xfer_ready_other"); }
        }
        other => row(e, stray_row(&other)),
    }
}

pub fn tx_step_ok(g: &mut G, t: &mut Transfer, aid: AttemptId, part: Option<u32>, etag: &Option<String>, upload_id: &Option<String>, e: &mut Vec<Effect>) {
    match t.st.clone() {
        St::Uploading { step, attempt, waiting_retry: false } if attempt == aid => {
            cancel_put(e, t.id, attempt);
            if t.upload_id.is_none() { if let Some(u) = upload_id { t.upload_id = Some(u.clone()); } }
            let n = match step { StepKind::CreateBlob => 1, StepKind::Part(n) => part.unwrap_or(n), StepKind::Complete => unreachable!("Uploading never holds the Complete step") };
            if !t.confirmed.iter().any(|c| c.0 == n) { t.confirmed.push((n, etag.clone())); }
            let last = step == StepKind::CreateBlob || t.confirmed.len() as u32 >= t.parts;
            row(e, if last { "xfer_all_parts" } else { "xfer_step_ok" });
            t.st = St::PersistingPart { part: n, last };
            commit(g, t, e, vec![Op::ConfirmedParts { part: Some(n), upload_id: t.upload_id.clone(), all: last }]);
        }
        St::Uploading { .. } => row(e, "xfer_uploading_stray"),
        other => row(e, stray_row(&other)),
    }
}

pub fn tx_completed(g: &mut G, t: &mut Transfer, aid: AttemptId, signed_id: &str, e: &mut Vec<Effect>) {
    match t.st.clone() {
        St::Completing { attempt, waiting_retry: false } if attempt == aid => {
            t.signed_id = Some(signed_id.to_string());
            let sha = t.sha256.clone().unwrap_or_default();
            let mut ops = vec![Op::Completed { signed_id: signed_id.to_string() }, Op::BlobCachePut { sha256: sha }];
            if t.batch.is_none() {
                row(e, "xfer_completed");
                ops.push(Op::Intent { op_id: t.op_id.unwrap_or(0), origin: OpOrigin::Transfer { transfer_id: t.id } });
            } else { row(e, "xfer_completed_batch"); }
            t.st = St::PersistingComplete;
            commit(g, t, e, ops);
        }
        St::Completing { .. } => row(e, "xfer_completing_stray"),
        other => row(e, stray_row(&other)),
    }
}

pub fn tx_complete_error(g: &mut G, t: &mut Transfer, aid: AttemptId, temporary: bool, error: &Failure, e: &mut Vec<Effect>) {
    match t.st.clone() {
        St::Completing { attempt, waiting_retry: false } if attempt == aid => {
            if temporary && g.online == Some(true) && t.complete_attempts < ATTEMPT_LIMIT {
                row(e, "xfer_complete_temp");
                let na = g.next_attempt;
                g.next_attempt += 1;
                t.st = St::Completing { attempt: na, waiting_retry: true };
                e.push(Effect::Schedule { kind: TickKind::Retry { transfer_id: t.id, attempt_id: na }, delay: Delay::Backoff { attempt: t.complete_attempts } });
            } else if temporary && g.online != Some(true) {
                row(e, "xfer_step_temp_offline");
            } else {
                row(e, "xfer_complete_fail");
                free_held(t, e);
                t.st = St::PersistingFailure { error: error.clone(), abort: None };
                commit(g, t, e, vec![Op::Failed { clear_upload: false }]);
                e.push(Effect::SelfSlotFreed);
            }
        }
        St::Completing { .. } => row(e, "xfer_completing_stray"),
        St::Ready { .. } => { if temporary && g.online != Some(true) { row(e, "xfer_complete_temp_offline"); } else { row(e, "xfer_ready_other"); } }
        other => row(e, stray_row(&other)),
    }
}

pub fn tx_op_reserved(g: &mut G, t: &mut Transfer, result: &Result<OpId, Fault>, e: &mut Vec<Effect>) {
    if t.st != St::Reserving { row(e, "r2_op_reserved_stray"); return; }
    match (t.batch, result) {
        (None, Ok(op)) => {
            row(e, "xfer_accept_optimistic_row");
            t.op_id = Some(*op);
            t.st = St::Accepting { closing: false };
            let mut ops = vec![Op::UploadCopying { op_id: *op, source_path: match &t.source { Source::File { path } => Some(path.clone()), Source::Pending => None } }];
            if t.has_effects { ops.push(Op::Effects { op_id: *op }); }
            commit(g, t, e, ops);
        }
        (None, Err(f)) => {
            row(e, "xfer_reserving_refused");
            reply_accept(g, t, e, Answer::Err(f.clone()));
            t.st = St::Done;
        }
        (Some(gid), Ok(op)) => {
            t.op_id = Some(*op);
            if let Some(gr) = g.groups.get_mut(&gid) { gr.reserve.insert(format!("t{}", t.id), Some(*op)); }
            g.post.push(Post::Reserve(gid));
        }
        (Some(gid), Err(f)) => {
            if let Some(gr) = g.groups.get_mut(&gid) { gr.reserve_failed = true; if let Some(r) = gr.accept_reply.take() { reply(e, r, Answer::Err(f.clone())); } }
            g.post.push(Post::Reserve(gid));
        }
    }
}

pub fn tx_copy_done(g: &mut G, t: &mut Transfer, aid: AttemptId, size: u64, sha: &str, cached: &Cached, e: &mut Vec<Effect>) {
    match t.st.clone() {
        St::Copying { attempt, supply, new_cycle } if attempt == aid => {
            t.size = size;
            t.sha256 = Some(sha.to_string());
            t.parts = parts_for(g, size);
            t.cached = Some(cached.clone());
            t.copy_present = true;
            let is_fresh = matches!(cached, Cached::Fresh { .. });
            let mut ops = vec![Op::SyncUpload { size, sha256: sha.to_string(), variables: supply.is_some() }];
            if let Some(back) = &supply {
                if t.has_effects || !t.values.is_empty() { ops.push(Op::Effects { op_id: t.op_id.unwrap_or(0) }); }
                if new_cycle {
                    if let Some(u) = t.upload_id.take() { ops.push(Op::UploadOrphan { upload_id: u, key: t.key.clone() }); }
                    t.confirmed.clear();
                    ops.push(Op::Retrying { reset_budget: true, new_cycle: true });
                }
                match cached {
                    Cached::Stale => ops.push(Op::BlobCacheRemove { sha256: sha.to_string() }),
                    Cached::Fresh { blob_id } => {
                        t.signed_id = Some(blob_id.clone());
                        ops.push(Op::Completed { signed_id: blob_id.clone() });
                        if t.batch.is_none() { ops.push(Op::Intent { op_id: t.op_id.unwrap_or(0), origin: OpOrigin::Transfer { transfer_id: t.id } }); }
                    }
                    Cached::Miss => {}
                }
                row(e, if is_fresh { "xfer_supply_cached" } else { "xfer_supply_copy_done" });
                t.st = St::PersistingCopy { supply: Some(back.clone()), cached: is_fresh, new_cycle };
                commit(g, t, e, ops);
                return;
            }
            if let Some(gid) = t.batch {
                t.st = St::CopyReady { attempt };
                g.post.push(Post::CopyBatch(gid));
                return;
            }
            match cached {
                Cached::Miss => row(e, "xfer_copy_done"),
                Cached::Stale => { row(e, "xfer_copy_stale_cache_removed"); ops.push(Op::BlobCacheRemove { sha256: sha.to_string() }); }
                Cached::Fresh { blob_id } => {
                    row(e, "xfer_copy_cached_skips_steps");
                    t.signed_id = Some(blob_id.clone());
                    ops.push(Op::Completed { signed_id: blob_id.clone() });
                    ops.push(Op::Intent { op_id: t.op_id.unwrap_or(0), origin: OpOrigin::Transfer { transfer_id: t.id } });
                }
            }
            t.st = St::PersistingCopy { supply: None, cached: is_fresh, new_cycle: false };
            commit(g, t, e, ops);
        }
        St::Copying { .. } => row(e, "xfer_copy_stray"),
        other => row(e, stray_row(&other)),
    }
}

pub fn tx_copy_failed(g: &mut G, t: &mut Transfer, aid: AttemptId, error: &str, e: &mut Vec<Effect>) {
    match t.st.clone() {
        St::Copying { attempt, supply, .. } if attempt == aid => {
            if let Some(back) = supply {
                row(e, "xfer_supply_copy_failed");
                e.push(Effect::DeleteFile { transfer_id: t.id });
                if let Some((r, _)) = t.replies.supply.take() { reply(e, r, Answer::Err(Fault::Copy(error.to_string()))); }
                t.st = match back { Back::AwaitingSource => St::AwaitingSource, Back::Failed => St::Failed };
                return;
            }
            if let Some(gid) = t.batch {
                row(e, "xfer_copy_failed_batch");
                let prior = t.st.clone();
                t.st = St::Removing { why: Why::CopyFailed(error.to_string()), prior: Box::new(prior), orphan: None };
                let mut ops = vec![Op::RemoveUpload { transfer_id: t.id }];
                ops.extend(rollback_op(t));
                commit_group(g, t.id, format!("cf{}", gid), ops);
                g.post.push(Post::CopyFailedBatch { gid, failing: t.id, error: error.to_string() });
                return;
            }
            let has_reply = t.replies.accept.is_some();
            if has_reply {
                row(e, "xfer_copy_failed");
                let prior = t.st.clone();
                t.st = St::Removing { why: Why::CopyFailed(error.to_string()), prior: Box::new(prior), orphan: None };
                let mut ops = vec![Op::RemoveUpload { transfer_id: t.id }];
                ops.extend(rollback_op(t));
                commit(g, t, e, ops);
            } else {
                row(e, "xfer_copy_failed_restored");
                t.st = St::PersistingFailure { error: fail(ErrKind::Step("copy_failed".into()), error), abort: None };
                commit(g, t, e, vec![Op::Failed { clear_upload: false }]);
            }
        }
        St::Copying { .. } => row(e, "xfer_copy_stray"),
        other => row(e, stray_row(&other)),
    }
}
