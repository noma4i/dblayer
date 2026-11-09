//! The LocalWrites owner: local writes of any epoch.

use super::types::*;

fn drained_if_idle(s: &mut State, e: &mut Vec<Effect>) {
    if s.local_closing && s.local.is_empty() && !s.local_drained {
        s.local_drained = true;
        e.push(Effect::Drained("LocalWrites"));
    }
}

fn done(s: &mut State, id: u64, what: &'static str, e: &mut Vec<Effect>) {
    s.local.remove(&id);
    s.local_expiry.remove(&id);
    reply(e, Addr::Call, id, what);
    drained_if_idle(s, e);
}

pub fn write(s: &mut State, epoch: Epoch, id: u64, kind: &LKind, f: &LFlags, e: &mut Vec<Effect>) {
    if !s.epoch_is(epoch) {
        row(e, "out_local_write_stale");
        reply(e, Addr::Call, id, "Err(NoSession)");
        return;
    }
    if s.local_closing {
        row(e, "out_local_write_closing");
        reply(e, Addr::Call, id, "Err(Closed)");
        return;
    }
    let writing = |s: &mut State, e: &mut Vec<Effect>, what: &'static str| {
        s.local.insert(id, LStage::Writing);
        if f.expiry { s.local_expiry.insert(id); }
        e.push(Effect::Store { op: id, what });
    };
    match kind {
        LKind::Attach if f.source_outside_files_dir => {
            row(e, "out_local_attach_copy");
            s.local.insert(id, LStage::Copying);
            e.push(Effect::Store { op: id, what: "Copy(source)" });
        }
        LKind::SetEphemeral => { row(e, "out_local_set_ephemeral"); writing(s, e, "Commit{EphemeralField}"); }
        LKind::Attach | LKind::Detach if !f.row_exists => { row(e, "out_local_attach_missing_row"); reply(e, Addr::Call, id, "Err(NotFound)"); }
        LKind::SetLocal if !f.path_is_local => { row(e, "out_local_set_local_not_local"); reply(e, Addr::Call, id, "Err(Invalid{path})"); }
        LKind::SetLocal if f.same_value => { row(e, "out_local_set_local_same"); reply(e, Addr::Call, id, "Ok"); }
        LKind::SetLocal if !f.row_exists => { row(e, "out_local_set_local_pending"); writing(s, e, "Commit{LocalPending}"); }
        LKind::Noop if !f.memory_model => { row(e, "out_local_noop_snapshot"); reply(e, Addr::Call, id, "Written{shown,revision,changed=false}"); }
        LKind::Noop => { row(e, "out_local_write_memory_same"); writing(s, e, "Commit{Touch expiry}"); }
        _ if f.memory_model && f.memory_same_values => { row(e, "out_local_write_memory_same"); writing(s, e, "Commit{Touch expiry}"); }
        _ => { row(e, "out_local_write"); writing(s, e, "Commit{rows local,files,basis=generation}"); }
    }
}

pub fn batch(s: &mut State, id: u64, i: &Input, e: &mut Vec<Effect>) {
    match (s.local.get(&id).copied(), i) {
        (Some(LStage::Writing), Input::Committed { .. }) => {
            row(e, "out_local_write_committed");
            if s.local_expiry.contains(&id) { e.push(Effect::Schedule { op: id, what: "Tick(expire{write_revision})" }); }
            done(s, id, "Ok", e);
        }
        (Some(LStage::Writing), Input::Rejected { reason: Reason::StaleGeneration(_), .. }) => {
            row(e, "out_local_write_stale_generation");
            e.push(Effect::Store { op: id, what: "Commit{basis=generations}(local_pending)" });
        }
        (Some(LStage::Writing), Input::Rejected { reason, .. }) => {
            row(e, "out_local_write_rejected");
            let what = match reason { Reason::NoSession => "Err(NoSession)", Reason::Closed => "Err(Closed)", Reason::Storage => "Err(Storage)", _ => "Err(Invalid)" };
            done(s, id, what, e);
        }
        _ => row(e, "rule_R2_foreign"),
    }
}

pub fn copy_result(s: &mut State, id: u64, ok: bool, e: &mut Vec<Effect>) {
    if s.local.get(&id) != Some(&LStage::Copying) { row(e, "rule_R2_foreign"); return; }
    if ok {
        row(e, "out_local_attach_copied");
        s.local.insert(id, LStage::Writing);
        e.push(Effect::Store { op: id, what: "Commit{AttachFile}" });
    } else {
        row(e, "out_local_attach_copy_failed");
        done(s, id, "Err(Storage{file})", e);
    }
}

/// Inputs without an addressee that reach the active writes: Online, ModelsReset, Tick, EpochEnded, Closing.
pub fn broadcast(s: &mut State, i: &Input, e: &mut Vec<Effect>) {
    let ids: Vec<(u64, LStage)> = s.local.iter().map(|(k, v)| (*k, *v)).collect();
    match i {
        Input::Closing => {
            s.local_closing = true;
            for (_, st) in &ids { row(e, if *st == LStage::Writing { "out_local_write_closing_waits" } else { "out_local_attach_copy_waits" }); }
            if ids.is_empty() && !s.local_drained {
                row(e, "out_local_closing");
                s.local_drained = true;
                e.push(Effect::Drained("LocalWrites"));
            }
        }
        Input::EpochEnded(_) => {
            for (_, st) in &ids { row(e, if *st == LStage::Writing { "out_local_write_epoch_ended" } else { "out_local_attach_copy_waits" }); }
        }
        Input::Online(_) | Input::ModelsReset { .. } | Input::Tick { .. } => {
            for (_, st) in &ids { row(e, if *st == LStage::Writing { "out_local_write_other" } else { "out_local_attach_copy_other" }); }
        }
        _ => {}
    }
}

pub fn ephemeral(i: &Input, e: &mut Vec<Effect>) {
    match i {
        Input::ExpireRow { .. } => { row(e, "out_ephemeral_expire"); e.push(Effect::Store { op: 0, what: "Commit{ExpireRow{write_revision}}" }); }
        Input::ResetEphemeral { .. } => { row(e, "out_ephemeral_reset_by_position"); e.push(Effect::Store { op: 0, what: "Commit{ResetEphemeral{position}}" }); }
        _ => {}
    }
}
