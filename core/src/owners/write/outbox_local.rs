//! Owner `LocalWrites` inside the Outbox actor: local writes, file copies, ephemeral expiry and `Drained`.
use std::collections::{BTreeMap};

use super::outbox::{Batch, Outbox, OutboxState};
use super::outbox_types::*;

use crate::owners::contract::{BatchId, Drained, RejectReason, ReplyId};

use crate::owners::store::{RowValue, StoreOp};
use crate::runtime::{Effect, Effects, Epoch, ScheduleId, Time};



impl OutboxState {
    pub(super) fn writes_active(&self) -> usize { self.copying.len() + self.batches.values().filter(|batch| matches!(batch, Batch::Write { .. })).count() }

    pub(super) fn maybe_drained(&mut self, fx: &mut Effects<Outbox>) {
        if self.closing && self.writes_active() == 0 && !self.drained {
            self.drained = true;
            fx.push(Effect::Run(OutboxEffect::Drained(Drained { owner: if self.author == crate::owners::contract::BatchAuthor::LocalWrites { "local_writes" } else { "outbox" } })));
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn local_write(&mut self, model: String, epoch: Epoch, write: LocalWriteKind, row: String, values: String, reply: ReplyId, now: Time, fx: &mut Effects<Outbox>) {
        if self.closing {
            return Self::reply(fx, reply, OutboxAnswer::Err(Fault::Closed));
        }
        if epoch != self.epoch {
            return Self::reply(fx, reply, OutboxAnswer::Err(Fault::NoSession));
        }
        if let LocalWriteKind::Attach { source: Some(source) } = &write {
            let id = self.next_copy;
            self.next_copy += 1;
            fx.push(Effect::Run(OutboxEffect::Copy { id, source: source.clone() }));
            self.copying.insert(id, (reply, model, write, row, values, now));
            return;
        }
        self.write_now(model, write, row, values, reply, now, fx);
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn write_now(&mut self, model: String, write: LocalWriteKind, row: String, values: String, reply: ReplyId, now: Time, fx: &mut Effects<Outbox>) {
        let expire = match write {
            LocalWriteKind::SetEphemeral { expire_after: Some(after) } => Some((Time(now.0 + after), after)),
            _ => None,
        };
        let generation = self.generation_of(&model);
        let batch = self.next_batch;
        self.next_batch += 1;
        self.commit_write(batch, &model, &write, &row, &values, generation, fx);
        self.batches.insert(batch, Batch::Write { reply, row, values, model, kind: write, expire });
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn commit_write(&self, batch: BatchId, model: &str, kind: &LocalWriteKind, row: &str, values: &str, generation: u64, fx: &mut Effects<Outbox>) {
        let rows = vec![RowValue { row_id: row.into(), values: values.into() }];
        let op = match kind {
            LocalWriteKind::Model(effects) => StoreOp::Planned { effects: effects.clone() },
            LocalWriteKind::AttachBytes { bytes, name, mime } => StoreOp::AttachFile { model: model.into(), row_id: row.into(), field: values.into(), bytes: bytes.clone(), name: name.clone(), mime: mime.clone() },
            LocalWriteKind::Detach => StoreOp::DetachFile { model: model.into(), row_id: row.into(), field: values.into() },
            LocalWriteKind::SetEphemeral { .. } => StoreOp::EphemeralRows { model: model.into(), rows },
            LocalWriteKind::SetLocalPending => StoreOp::LocalPending { model: model.into(), rows },
            _ => StoreOp::LocalRows { model: model.into(), rows },
        };
        self.commit(batch, BTreeMap::from([(model.into(), generation)]), vec![op], 0, fx);
    }

    pub(super) fn copy_done(&mut self, id: u64, fx: &mut Effects<Outbox>) {
        if let Some((reply, model, write, row, values, now)) = self.copying.remove(&id) {
            let write = match write {
                LocalWriteKind::Attach { .. } => LocalWriteKind::Attach { source: None },
                other => other,
            };
            self.write_now(model, write, row, values, reply, now, fx);
        }
    }

    pub(super) fn copy_failed(&mut self, id: u64, fx: &mut Effects<Outbox>) {
        if let Some((reply, ..)) = self.copying.remove(&id) {
            Self::reply(fx, reply, OutboxAnswer::Err(Fault::Rejected(RejectReason::Storage("file".into()))));
        }
        self.maybe_drained(fx);
    }

    pub(super) fn expire(&mut self, model: String, row: String, due: Time, fx: &mut Effects<Outbox>) {
        if self.ephemeral.get(&row).is_some_and(|written| *written > due) {
            return;
        }
        self.ephemeral.remove(&row);
        let batch = self.next_batch;
        self.next_batch += 1;
        self.commit(batch, BTreeMap::new(), vec![StoreOp::RemoveRows { model, ids: vec![row] }], 0, fx);
    }

    pub(super) fn arm_expiry(&mut self, model: String, row: String, revision: u64, after: u64, fx: &mut Effects<Outbox>) {
        self.disarm_expiry(&model, &row, fx);
        let id = ScheduleId(EXPIRE_TIMER + self.next_schedule);
        self.next_schedule += 1;
        self.expiry.insert((model.clone(), row.clone()), (id, revision));
        fx.push(Effect::Schedule { id, after: std::time::Duration::from_millis(after), input: OutboxInput::ExpireRow { model, row, revision } });
    }

    pub(super) fn arm_reset(&mut self, reset: crate::owners::store::EphemeralReset, fx: &mut Effects<Outbox>) {
        let key = (reset.model.clone(), reset.row_id.clone(), reset.field.clone());
        if let Some((id, _)) = self.resets.remove(&key) { fx.push(Effect::Cancel(id)); }
        let id = ScheduleId(EXPIRE_TIMER + self.next_schedule);
        self.next_schedule += 1;
        self.resets.insert(key, (id, reset.position));
        let after = std::time::Duration::from_millis(reset.after_ms);
        fx.push(Effect::Schedule { id, after, input: OutboxInput::ResetField { reset } });
    }

    pub(super) fn reset_field(&mut self, reset: crate::owners::store::EphemeralReset, fx: &mut Effects<Outbox>) {
        let key = (reset.model.clone(), reset.row_id.clone(), reset.field.clone());
        if !self.resets.get(&key).is_some_and(|(_, held)| *held == reset.position) { return; }
        self.resets.remove(&key);
        let batch = self.next_batch;
        self.next_batch += 1;
        self.commit(batch, BTreeMap::new(), vec![StoreOp::ResetEphemeral { model: reset.model, row_id: reset.row_id, field: reset.field, position: reset.position }], 0, fx);
    }

    pub(super) fn disarm_expiry(&mut self, model: &str, row: &str, fx: &mut Effects<Outbox>) {
        if let Some((id, _)) = self.expiry.remove(&(model.to_string(), row.to_string())) { fx.push(Effect::Cancel(id)); }
    }

    pub(super) fn expire_row(&mut self, model: String, row: String, revision: u64, fx: &mut Effects<Outbox>) {
        if !self.expiry.get(&(model.clone(), row.clone())).is_some_and(|(_, held)| *held == revision) { return; }
        self.expiry.remove(&(model.clone(), row.clone()));
        let batch = self.next_batch;
        self.next_batch += 1;
        self.commit(batch, BTreeMap::new(), vec![StoreOp::ExpireRow { model, row_id: row, write_revision: revision }], 0, fx);
    }

}
