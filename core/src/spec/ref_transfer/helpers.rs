use super::*;

pub fn row(e: &mut Vec<Effect>, r: &'static str) { e.push(Effect::Row(r)); }
pub fn undef(e: &mut Vec<Effect>, r: &str, reason: &'static str) { e.push(Effect::Undefined { row: r.to_string(), reason }); }
pub fn reply(e: &mut Vec<Effect>, r: ReplyId, a: Answer) { e.push(Effect::Reply(r, a)); }
pub fn fail(kind: ErrKind, message: &str) -> Failure { Failure { kind, message: message.to_string() } }
pub fn receipt(t: &Transfer) -> Receipt { Receipt { epoch: t.epoch, transfer_id: t.id, cycle_id: t.cycle_id } }
pub fn key_text(k: &TransferKey) -> String { format!("{}:{}:{}", k.model, k.row_id, k.action) }

pub fn holds_slot(st: &St) -> bool {
    matches!(st, St::PersistingAttempt { .. } | St::Uploading { waiting_retry: false, .. } | St::Completing { waiting_retry: false, .. })
}

pub fn step_kind_next(step: StepKind) -> Next { match step { StepKind::CreateBlob => Next::Part(1), StepKind::Part(n) => Next::Part(n), StepKind::Complete => Next::Complete } }

pub fn req_id_of(t: &Transfer, step: StepKind) -> ReqId {
    let (name, part) = match step { StepKind::CreateBlob => ("blob", 1), StepKind::Part(n) => ("part", n), StepKind::Complete => ("complete", 0) };
    ReqId { transfer_id: t.id, cycle_id: t.cycle_id, step: name.to_string(), part }
}

pub fn order(s: &State) -> Vec<TransferId> {
    let mut v: Vec<(u64, u32, TransferId)> = s.transfers.values().map(|t| (t.seq, t.pos, t.id)).collect();
    v.sort();
    v.into_iter().map(|x| x.2).collect()
}

pub fn shown(g: &G, t: &Transfer) -> Shown {
    let (status, wait_reason) = match &t.st {
        St::Reserving | St::Accepting { .. } | St::Copying { .. } | St::CopyReady { .. } | St::PersistingCopy { .. } => (Status::Copying, None),
        St::AwaitingSource => (Status::Waiting, Some("sourcePending")),
        St::WaitingDeclaration { .. } => (Status::Waiting, None),
        St::Ready { .. } | St::PersistingAttempt { .. } | St::PersistingRetry { .. } => (Status::Queued, None),
        St::Uploading { .. } | St::Completing { .. } | St::PersistingPart { .. } | St::PersistingComplete | St::PersistingBatchIntent { .. } | St::BatchReady => (Status::Uploading, None),
        St::Mutating => (Status::Mutating, None),
        St::PersistingFailure { .. } | St::Failed => (Status::Failed, None),
        St::Removing { prior, .. } => match **prior { St::Failed => (Status::Failed, None), _ => (Status::Uploading, None) },
        St::Done => (Status::Done, None),
    };
    let sent = (t.confirmed.len() as u64).saturating_mul(g.part_size).min(t.size);
    Shown { status, wait_reason, sent_bytes: sent, total_bytes: t.size, error: if matches!(t.st, St::Failed) { t.error.clone() } else { None }, local_url: t.local_url, round_error: t.round_error.clone() }
}

pub fn status_push(g: &G, t: &Transfer, before: &Shown, e: &mut Vec<Effect>) {
    let after = shown(g, t);
    if *before == after { return; }
    let mut ids: Vec<StreamId> = g.subs.get(&t.id).map(|x| x.iter().cloned().collect()).unwrap_or_default();
    if let Some(k) = g.key_subs.get(&t.key) { ids.extend(k.iter().cloned()); }
    for stream_id in ids { e.push(Effect::Status { stream_id, transfer_id: t.id, shown: after.clone() }); }
}

pub fn commit(g: &mut G, t: &Transfer, e: &mut Vec<Effect>, ops: Vec<Op>) -> StoreBatch {
    let b = g.next_batch;
    g.next_batch += 1;
    g.batches.insert(b, BatchKind::Own { ids: vec![t.id], ops: ops.clone() });
    e.push(Effect::Store { batch: b, generation: g.generation, basis: None, ops });
    b
}

pub fn commit_group(g: &mut G, id: TransferId, tag: String, ops: Vec<Op>) {
    for d in g.deferred.iter_mut() {
        if d.0 == tag { d.1.push(id); d.2.extend(ops); return; }
    }
    g.deferred.push((tag, vec![id], ops));
}

pub fn settle_post(s: &mut State, e: &mut Vec<Effect>) {
    let d = std::mem::take(&mut s.g.deferred);
    for (_, ids, ops) in d {
        let b = s.g.next_batch;
        s.g.next_batch += 1;
        s.g.batches.insert(b, BatchKind::Own { ids, ops: ops.clone() });
        e.push(Effect::Store { batch: b, generation: s.g.generation, basis: None, ops });
    }
    if !s.g.round_needed.is_empty() {
        let ids = std::mem::take(&mut s.g.round_needed);
        e.push(Effect::Rounds(RoundsIn::Needed { epoch: s.g.epoch, ids }));
    }
}

pub fn rollback_op(t: &Transfer) -> Vec<Op> { match (t.has_effects, t.op_id) { (true, Some(op)) => vec![Op::Rollback { op_id: op }], _ => vec![] } }

pub fn free_held(t: &mut Transfer, e: &mut Vec<Effect>) {
    if let Some(r) = t.held_step.take() { row(e, "xfer_release_pending_step"); e.push(Effect::SessionRelease { req_id: r }); }
}

pub fn cancel_put(e: &mut Vec<Effect>, transfer_id: TransferId, attempt_id: AttemptId) {
    row(e, "xfer_put_timeout_cancelled");
    e.push(Effect::CancelTimer { kind: TickKind::PutTimeout { transfer_id, attempt_id } });
}

pub fn fix_outcome(t: &mut Transfer, e: &mut Vec<Effect>, out: Outcome) {
    t.outcomes.insert(t.cycle_id, out.clone());
    if let Some(ws) = t.outcome_waiters.remove(&t.cycle_id) { for r in ws { reply(e, r, Answer::Outcome(out.clone())); } }
}

pub fn reply_accept(g: &mut G, t: &mut Transfer, e: &mut Vec<Effect>, a: Answer) {
    if let Some(gid) = t.batch {
        if let Some(gr) = g.groups.get_mut(&gid) { if let Some(r) = gr.accept_reply.take() { reply(e, r, a); } }
    } else if let Some(r) = t.replies.accept.take() { reply(e, r, a); }
}

pub fn interrupt_all(g: &mut G, t: &mut Transfer, e: &mut Vec<Effect>) {
    reply_accept(g, t, e, Answer::Err(Fault::Interrupted));
    if let Some((r, _)) = t.replies.cancel.take() { reply(e, r, Answer::Err(Fault::Interrupted)); }
    if let Some((r, _)) = t.replies.supply.take() { reply(e, r, Answer::Err(Fault::Interrupted)); }
    if let Some(r) = t.replies.retry.take() { reply(e, r, Answer::Err(Fault::Interrupted)); }
    for r in std::mem::take(&mut t.replies.discard_waiters) { reply(e, r, Answer::Err(Fault::Interrupted)); }
    let all: Vec<(u64, Vec<ReplyId>)> = std::mem::take(&mut t.outcome_waiters).into_iter().collect();
    for (_, ws) in all { for r in ws { reply(e, r, Answer::Outcome(Outcome::Interrupted)); } }
}

pub fn counter(t: &Transfer, step: StepKind) -> u32 { if step == StepKind::Complete { t.complete_attempts } else { t.attempts } }

pub fn parts_for(g: &G, size: u64) -> u32 {
    if size < g.multipart_threshold { 1 } else { ((size + g.part_size - 1) / g.part_size).max(1) as u32 }
}

pub fn first_unconfirmed(t: &Transfer) -> Next {
    for n in 1..=t.parts { if !t.confirmed.iter().any(|c| c.0 == n) { return Next::Part(n); } }
    Next::Complete
}

pub fn declared(g: &G, k: &TransferKey) -> bool { g.declarations.contains(&(k.model.clone(), k.action.clone())) }

pub fn new_transfer(g: &mut G, key: TransferKey, source: Source, epoch: u64, now: u64, batch: Option<GroupId>, pos: u32, has_effects: bool, values: Vec<String>) -> Transfer {
    let id = g.next_transfer;
    g.next_transfer += 1;
    let seq = g.next_seq;
    g.next_seq += 1;
    Transfer {
        id, key, op_id: None, epoch, source, st: St::Reserving, seq, pos, cycle_id: 1, batch, size: 0, sha256: None, parts: 1, upload_id: None, confirmed: vec![],
        attempts: 0, complete_attempts: 0, signed_id: None, created_at: now, accepted_at: now, error: None, copy_present: false, has_effects, held_step: None, round_error: None,
        replies: Replies::default(), outcome_waiters: BTreeMap::new(), outcomes: BTreeMap::new(), intent_live: false, write_retry: None, intent_after_ready: false, local_url: false, cached: None, aborted: false, values, variables: vec![],
    }
}

pub fn step(s: &State, i: &Input) -> (State, Vec<Effect>) {
    let mut s = s.clone();
    let mut e = Vec::new();
    owner(&mut s, i, &mut e);
    settle_post(&mut s, &mut e);
    (s, e)
}

pub fn after(s: &mut State, t: Transfer) {
    if t.st == St::Done {
        s.g.subs.remove(&t.id);
    } else {
        s.transfers.insert(t.id, t);
    }
}

pub fn run_on(s: &mut State, id: TransferId, i: &Input, e: &mut Vec<Effect>) {
    let mut t = match s.transfers.remove(&id) { Some(t) => t, None => { row(e, "stray_unknown_transfer"); return; } };
    let before = shown(&s.g, &t);
    tx(&mut s.g, &mut t, i, e);
    status_push(&s.g, &t, &before, e);
    after(s, t);
    run_post(s, e);
}

pub fn find_open(s: &State, key: &TransferKey) -> Option<TransferId> { s.transfers.values().find(|t| t.key == *key).map(|t| t.id) }
