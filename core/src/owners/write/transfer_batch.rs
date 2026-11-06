//! Owner `Transfer`: the elements of a batch. They wait in `BatchReady` until every remaining element has its file on the server, then the mutation of the batch goes out as one request, or as one request per element when fewer than `minimum` remain.

use serde_json::{Map, Value};

use super::outbox::{NewOp, Origin, OutboxInput, OutboxOutcome};
use super::transfer::{reply, Fx, TransferState};
use super::transfer_types::*;
use crate::owners::store::{StoreOp, UploadKind};
use crate::runtime::{Effect, Outbound};

/// The key Outbox returns with the outcome of the mutation of a whole batch.
pub(super) fn batch_key(group: u64) -> String { format!("batch:{group}") }

fn set_path(root: &mut Value, path: &str, value: Value) {
    let mut at = root;
    let keys: Vec<&str> = path.split('.').collect();
    for (index, key) in keys.iter().enumerate() {
        if !at.is_object() {
            *at = Value::Object(Map::new());
        }
        let Some(object) = at.as_object_mut() else { return };
        if index + 1 == keys.len() {
            object.insert((*key).to_string(), value);
            return;
        }
        at = object.entry((*key).to_string()).or_insert_with(|| Value::Object(Map::new()));
    }
}

fn with_signed_id(fields: &Map<String, Value>, signed: &str) -> Value {
    Value::Object(fields.iter().map(|(name, value)| (name.clone(), if value.as_str() == Some(SIGNED_ID_SENTINEL) { Value::String(signed.into()) } else { value.clone() })).collect())
}

/// Puts `number` under `key` in the variables of a stored body, also inside the object that wraps them.
fn set_number(body: &str, key: &str, number: usize) -> Option<String> {
    let mut parsed: Value = serde_json::from_str(body).ok()?;
    let variables = parsed.get_mut("variables")?.as_object_mut()?;
    if variables.contains_key(key) {
        variables.insert(key.into(), Value::from(number));
    } else {
        let wrapped = variables.values_mut().filter_map(Value::as_object_mut).find(|object| object.contains_key(key))?;
        wrapped.insert(key.into(), Value::from(number));
    }
    Some(parsed.to_string())
}

impl TransferState {
    pub(super) fn group_entry(&self, id: TransferId) -> Option<u64> {
        let group = self.transfers.get(&id)?.batch_id?;
        self.groups.contains_key(&group).then_some(group)
    }

    /// The elements that still take part in the batch, in the original order.
    pub(super) fn remaining(&self, group: u64) -> Vec<TransferId> {
        let Some(entry) = self.groups.get(&group) else { return Vec::new() };
        entry.ids.iter().copied().filter(|id| !matches!(self.stage_of(*id), None | Some(TransferStage::Done | TransferStage::Removing))).collect()
    }

    pub(super) fn phase_of(&self, group: u64) -> Option<BatchPhase> { self.groups.get(&group).map(|entry| entry.phase) }

    /// `PersistingComplete x Committed` of an element: it waits for the others.
    pub(super) fn batch_ready(&mut self, id: TransferId, group: u64, fx: &mut Fx) {
        self.set(id, TransferStage::BatchReady);
        self.show(id, Shown::Uploading { progress: 100 }, fx);
        self.rounds_idle(id, fx);
        self.batch_release(group, fx);
    }

    /// The mutation goes out when every remaining element is in `BatchReady`.
    pub(super) fn batch_release(&mut self, group: u64, fx: &mut Fx) {
        let Some(entry) = self.groups.get(&group) else { return };
        if entry.phase != BatchPhase::Collecting {
            return;
        }
        let minimum = entry.joint.minimum;
        let remaining = self.remaining(group);
        if remaining.is_empty() || !remaining.iter().all(|id| self.stage_of(*id) == Some(TransferStage::BatchReady)) {
            return;
        }
        if remaining.len() >= minimum {
            self.submit_joint(group, &remaining, fx);
        } else {
            if let Some(entry) = self.groups.get_mut(&group) {
                entry.phase = BatchPhase::Singles;
            }
            self.single_next(group, fx);
        }
    }

    fn submit_joint(&mut self, group: u64, remaining: &[TransferId], fx: &mut Fx) {
        let Some(entry) = self.groups.get(&group) else { return };
        let joint = entry.joint.clone();
        let mut body: Value = serde_json::from_str(&joint.mutation.body).unwrap_or(Value::Object(Map::new()));
        let items: Vec<Value> = remaining.iter().filter_map(|id| self.transfers.get(id)).map(|transfer| with_signed_id(&transfer.fields, transfer.signed_id.as_deref().unwrap_or_default())).collect();
        set_path(&mut body, &format!("variables.{}", joint.items_key), Value::Array(items));
        let first = self.transfers.get(&remaining[0]).map(|transfer| (transfer.mutation.model.clone(), transfer.mutation.row_id.clone())).unwrap_or_default();
        for &id in remaining {
            self.set(id, TransferStage::Mutating);
            self.show(id, Shown::Uploading { progress: 100 }, fx);
        }
        if let Some(entry) = self.groups.get_mut(&group) {
            entry.phase = BatchPhase::Mutating;
            entry.op_row = Some(first.clone());
        }
        let op = NewOp { model: first.0, row_id: first.1, origin: Origin::Transfer(batch_key(group)), body: body.to_string(), ..joint.mutation };
        fx.push(Effect::Send(Outbound::new(self.peers.outbox, OutboxInput::Submit { epoch: self.epoch, op, reply: NO_REPLY })));
    }

    /// `Singles`: the next element by order goes with its own mutation.
    pub(super) fn single_next(&mut self, group: u64, fx: &mut Fx) {
        let remaining = self.remaining(group);
        if remaining.iter().any(|id| self.stage_of(*id) == Some(TransferStage::Mutating)) {
            return;
        }
        let Some(&next) = remaining.iter().find(|id| self.stage_of(**id) == Some(TransferStage::BatchReady)) else { return };
        self.set(next, TransferStage::Mutating);
        self.show(next, Shown::Uploading { progress: 100 }, fx);
        self.submit_mutation(next, fx);
    }

    /// The outcome of the mutation of the whole batch.
    pub(super) fn batch_outcome(&mut self, group: u64, outcome: OutboxOutcome, fx: &mut Fx) {
        if self.phase_of(group) != Some(BatchPhase::Mutating) {
            return;
        }
        let ids: Vec<TransferId> = self.remaining(group).into_iter().filter(|id| self.stage_of(*id) == Some(TransferStage::Mutating)).collect();
        match outcome {
            OutboxOutcome::Confirmed { revision, .. } => {
                for &id in &ids {
                    self.remove_with(id, Removal::Confirmed { revision });
                }
                self.store(&ids, StoreKind::Removed, fx);
            }
            OutboxOutcome::Discarded => {
                for &id in &ids {
                    self.remove_with(id, Removal::Discarded);
                }
                self.store(&ids, StoreKind::Removed, fx);
            }
            OutboxOutcome::Rejected { error } | OutboxOutcome::Failed { error } => {
                for &id in &ids {
                    self.mark_failure(id, error.clone());
                }
                self.store(&ids, StoreKind::Failed, fx);
            }
            OutboxOutcome::Interrupted => {
                for &id in &ids {
                    self.interrupt(id, fx);
                    self.rounds_idle(id, fx);
                }
            }
        }
    }

    /// After an element left: the others may be ready now, or the next single goes.
    pub(super) fn batch_after_removal(&mut self, id: TransferId, fx: &mut Fx) {
        let Some(group) = self.group_entry(id) else { return };
        match self.phase_of(group) {
            Some(BatchPhase::Collecting) => self.batch_release(group, fx),
            Some(BatchPhase::Singles) => self.single_next(group, fx),
            _ => {}
        }
    }

    /// An element of a batch in `Collecting` leaves: the others get new positions and sizes in the same write.
    pub(super) fn renumber_after(&mut self, id: TransferId) -> Vec<StoreOp> {
        let Some(group) = self.group_entry(id) else { return Vec::new() };
        let Some(entry) = self.groups.get(&group) else { return Vec::new() };
        let (joint, phase) = (entry.joint.clone(), entry.phase);
        let left: Vec<TransferId> = self.remaining(group).into_iter().filter(|other| *other != id).collect();
        let Some(renumber) = joint.renumber.filter(|_| phase == BatchPhase::Collecting && left.len() >= joint.minimum) else { return Vec::new() };
        let mut ops = Vec::new();
        for (index, other) in left.iter().enumerate() {
            let (position, size) = (renumber.start + index, left.len());
            let Some(transfer) = self.transfers.get_mut(other) else { continue };
            transfer.fields.insert(renumber.position_target.clone(), Value::from(position));
            transfer.fields.insert(renumber.size_target.clone(), Value::from(size));
            for (key, number) in [(&renumber.position_target, position), (&renumber.size_target, size)] {
                if let Some(body) = set_number(&transfer.mutation.body, key, number) {
                    transfer.mutation.body = body;
                }
            }
            ops.push(StoreOp::Numbers { model: transfer.mutation.model.clone(), row_id: transfer.mutation.row_id.clone(), writes: vec![(renumber.position.clone(), position as i64), (renumber.size.clone(), size as i64)] });
            ops.push(StoreOp::Upload { row: upload_row(transfer, StoreKind::SyncUpload), kind: UploadKind::SyncUpload });
        }
        ops
    }

    /// `Failed x Retry` of an element whose mutation failed: the file is on the server, the mutation goes again.
    pub(super) fn batch_retry_mutation(&mut self, id: TransferId, rep: crate::owners::contract::ReplyId, fx: &mut Fx) -> bool {
        let Some(group) = self.group_entry(id) else { return false };
        let Some(phase) = self.phase_of(group).filter(|phase| *phase != BatchPhase::Collecting) else { return false };
        let ids: Vec<TransferId> = if phase == BatchPhase::Mutating { self.remaining(group).into_iter().filter(|member| self.stage_of(*member) == Some(TransferStage::Failed)).collect() } else { vec![id] };
        for &member in &ids {
            if let Some(transfer) = self.transfers.get_mut(&member) {
                transfer.next_step = NextStep::Mutation;
                transfer.stage = TransferStage::PersistingRetry;
            }
        }
        if let Some(transfer) = self.transfers.get_mut(&id) {
            transfer.retry_reply = Some(rep);
        }
        self.store(&ids, StoreKind::Retrying, fx);
        true
    }

    /// `PersistingRetry x Committed` with the mutation as the next step: the same stored mutation goes again.
    pub(super) fn retry_mutation_committed(&mut self, ids: &[TransferId], fx: &mut Fx) {
        let mut sent = false;
        for &id in ids {
            let Some(transfer) = self.transfers.get_mut(&id) else { continue };
            transfer.cycle_id += 1;
            transfer.stage = TransferStage::Mutating;
            transfer.error = None;
            let rep = transfer.retry_reply.take();
            let (model, row_id) = self.groups.get(&transfer.batch_id.unwrap_or(0)).filter(|entry| entry.phase == BatchPhase::Mutating).and_then(|entry| entry.op_row.clone()).unwrap_or((transfer.mutation.model.clone(), transfer.mutation.row_id.clone()));
            let receipt = self.receipt(id);
            if let Some(rep) = rep {
                reply(fx, rep, TransferAnswer::Receipt(receipt));
            }
            self.show(id, Shown::Uploading { progress: 100 }, fx);
            if !std::mem::replace(&mut sent, true) {
                fx.push(Effect::Send(Outbound::new(self.peers.outbox, OutboxInput::Retry { model, row_id, reply: NO_REPLY })));
            }
        }
    }
}
