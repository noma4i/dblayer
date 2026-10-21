use std::collections::{BTreeMap, BTreeSet, HashSet};

use serde_json::{Map, Value};

use crate::commit::{CommitEnvelope, CommitResult, DestroyReason, EntityOp, OperationStatus, OperationTransition, ScopeOp, WriteOrigin};
use crate::files::references_of_row;
use crate::identity::{Generation, ModelKey, RowId};
use crate::store::{Reader, StoreError};

use super::error::DbError;
use super::local_write::{plan_fault, read_outbox, OutboxOperation};
use super::open::Db;
use super::prior::{Prior, PriorRow};

#[derive(Default)]
struct Restore {
    entity_ops: Vec<EntityOp>,
    scope_ops: Vec<ScopeOp>,
    transitions: Vec<OperationTransition>,
    discarded_files: Vec<String>,
}

struct Open<'a> {
    entry: &'a OutboxOperation,
    hidden: bool,
    prior: Prior,
}

struct Chain<'a> {
    open: BTreeMap<i64, Open<'a>>,
    changed: BTreeSet<i64>,
}

impl<'a> Chain<'a> {
    fn new(outbox: &'a [OutboxOperation], own: i64) -> Self {
        let open = outbox
            .iter()
            .filter(|entry| entry.operation.id.0 != own)
            .filter_map(|entry| {
                let hidden = entry.operation.status != OperationStatus::Failed;
                Some((entry.operation.id.0, Open { entry, hidden, prior: Prior::of(entry)? }))
            })
            .collect();
        Self { open, changed: BTreeSet::new() }
    }

    fn later_writer(&self, own: i64, model: &str, id: &str, field: &str) -> Option<i64> {
        self.open.range(own + 1..).find(|(_, open)| open.prior.rows.iter().any(|row| writes(row, open.hidden, model, id, field))).map(|(seq, _)| *seq)
    }

    fn earlier_writer(&self, own: i64, model: &str, id: &str, field: &str) -> Option<i64> {
        self.open
            .range(..own)
            .rev()
            .find(|(_, open)| open.prior.rows.iter().any(|row| matches!(row, PriorRow::Edited { .. }) && writes(row, open.hidden, model, id, field)))
            .map(|(seq, _)| *seq)
    }

    fn hand_down(&mut self, seq: i64, model: &str, id: &str, field: &str, value: Value) {
        let Some(open) = self.open.get_mut(&seq) else { return };
        for row in &mut open.prior.rows {
            if let PriorRow::Edited { model: owner, id: row_id, before, .. } | PriorRow::Destroyed { model: owner, id: row_id, before, .. } = row {
                if owner == model && row_id == id {
                    before.insert(field.to_string(), value.clone());
                    self.changed.insert(seq);
                }
            }
        }
    }

    fn reown(&mut self, seq: i64, model: &str, id: &str, field: &str, revision: i64) {
        let Some(open) = self.open.get_mut(&seq) else { return };
        for row in &mut open.prior.rows {
            if let PriorRow::Edited { model: owner, id: row_id, owned, .. } = row {
                if owner == model && row_id == id {
                    owned.insert(field.to_string(), revision);
                    self.changed.insert(seq);
                }
            }
        }
    }

    fn drop_op(&mut self, seq: i64) {
        self.open.remove(&seq);
        self.changed.remove(&seq);
    }

    fn rewrites(&self) -> Vec<OperationTransition> {
        self.changed
            .iter()
            .filter_map(|seq| self.open.get(seq))
            .map(|open| open.entry.rewritten(open.entry.operation.row_ids.clone(), open.entry.input.clone(), Some(open.prior.to_value())))
            .collect()
    }
}

fn writes(row: &PriorRow, hidden: bool, model: &str, id: &str, field: &str) -> bool {
    match row {
        PriorRow::Edited { model: owner, id: row_id, owned, .. } => owner == model && row_id == id && owned.contains_key(field),
        PriorRow::Destroyed { model: owner, id: row_id, .. } => hidden && owner == model && row_id == id,
        PriorRow::Created { .. } => false,
    }
}

fn restore(reader: &Reader<'_>, outbox: &[OutboxOperation], entry: &OutboxOperation) -> Result<Restore, StoreError> {
    let mut plan = Restore::default();
    let Some(prior) = Prior::of(entry) else { return Ok(plan) };
    let own = entry.operation.id.0;
    let mut chain = Chain::new(outbox, own);
    let revision = reader.revision_seq()? + 1;
    let mut taken = HashSet::new();
    for row in &prior.rows {
        let Ok(row_id) = RowId::new(row.id()) else { continue };
        match row {
            PriorRow::Created { model, .. } => {
                plan.entity_ops.push(EntityOp::Destroy { model: ModelKey::new(model), id: row_id.clone(), reason: DestroyReason::Expired });
                plan.discarded_files.extend(references_of_row(reader.connection(), model, row_id.as_str())?.into_iter().map(|file| file.id));
                for follower in outbox.iter().filter(|other| other.operation.id.0 != own && other.operation.row_ids.contains(&row_id)) {
                    plan.transitions.push(OperationTransition::Remove { seq: follower.operation.id.0, expected: None });
                    chain.drop_op(follower.operation.id.0);
                }
            }
            PriorRow::Destroyed { .. } if entry.operation.status == OperationStatus::Failed => {}
            PriorRow::Destroyed { .. } => {
                let (entity_ops, scope_ops) = row.reappearing(reader, &mut taken)?;
                plan.entity_ops.extend(entity_ops);
                plan.scope_ops.extend(scope_ops);
            }
            PriorRow::Edited { model, id, before, owned } => {
                restore_edit(reader, &mut chain, own, (model, id), (before, owned), revision, &mut plan)?;
            }
        }
    }
    plan.transitions.extend(chain.rewrites());
    Ok(plan)
}

fn restore_edit(
    reader: &Reader<'_>,
    chain: &mut Chain<'_>,
    own: i64,
    (model, id): (&str, &str),
    (before, owned): (&Map<String, Value>, &BTreeMap<String, i64>),
    revision: i64,
    plan: &mut Restore,
) -> Result<(), StoreError> {
    let stored = reader.stored_row(model, id)?;
    let current = stored.as_ref().map(|stored| stored.row.clone());
    let mut restored = current.clone().unwrap_or_default();
    for (field, owned_at) in owned {
        let value = before.get(field).cloned().unwrap_or(Value::Null);
        if let Some(later) = chain.later_writer(own, model, id, field) {
            chain.hand_down(later, model, id, field, value);
            continue;
        }
        let Some(stored) = &stored else { continue };
        if current.is_none() || stored.field_rev.get(field) != Some(owned_at) {
            continue;
        }
        let moves = restored.get(field) != Some(&value);
        restored.insert(field.clone(), value);
        if let Some(earlier) = chain.earlier_writer(own, model, id, field) {
            chain.reown(earlier, model, id, field, if moves { revision } else { *owned_at });
        }
    }
    if let (Some(current), Ok(row_id)) = (current, RowId::new(id)) {
        if current != restored {
            plan.entity_ops.push(EntityOp::Upsert { model: ModelKey::new(model), id: row_id, row: restored });
        }
    }
    Ok(())
}

impl Db {
    pub(crate) fn remove_upload_item(&self, model: &str, id: &RowId, seq: i64, remove_row: bool) -> Result<bool, DbError> {
        let generation = self.generation();
        let spec = self.spec(model)?;
        let mut removed = false;
        self.commit_following(generation, |reader| {
            let outbox = read_outbox(reader)?;
            let Some(entry) = outbox.iter().find(|entry| entry.operation.id.0 == seq) else { return Ok(None) };
            if entry.operation.transport_started { return Err(DbError::InFlight { model: model.into(), id: id.to_string() }); }
            let position = reader.connection().query_row("SELECT position FROM op_item WHERE seq = ?1 AND row_id = ?2", rusqlite::params![seq, id.as_str()], |row| row.get(0))?;
            let mut envelope = CommitEnvelope::new(WriteOrigin::Optimistic, generation);
            let mut prior = Prior::of(entry).unwrap_or_default();
            if remove_row {
                let mut selected = entry.clone();
                selected.rollback = Some(Prior { rows: prior.rows.iter().filter(|row| row.id() == id.as_str()).cloned().collect() }.to_value());
                let plan = restore(reader, &outbox, &selected)?;
                envelope.entity_ops = plan.entity_ops;
                envelope.scope_ops = plan.scope_ops;
                envelope.operation_transitions = plan.transitions;
            }
            prior.rows.retain(|row| row.id() != id.as_str());
            let rows: Vec<_> = entry.operation.row_ids.iter().filter(|row| *row != id).cloned().collect();
            if let Some(batch) = spec.actions.iter().find(|action| entry.operation.action_key == crate::operation::Operation::action_key(model, &action.name)).and_then(|action| action.batch.as_ref()).filter(|batch| rows.len() >= batch.min) {
                if let Some(fields) = &batch.renumber {
                    let position = spec.integer_path(&fields.position).ok_or_else(|| DbError::InvalidUpload(fields.position.clone()))?;
                    let size = spec.integer_path(&fields.size).ok_or_else(|| DbError::InvalidUpload(fields.size.clone()))?;
                    for (index, row_id) in rows.iter().enumerate() {
                        if let Some(stored) = reader.stored_row(model, row_id.as_str())? {
                            let mut row = stored.row;
                            write_number(&mut row, &position, index + fields.position_from);
                            write_number(&mut row, &size, rows.len());
                            envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new(model), id: row_id.clone(), row });
                        }
                    }
                }
            }
            envelope.operation_transitions.push(entry.rewritten(rows, entry.input.clone(), Some(prior.to_value())));
            envelope.operation_transitions.push(OperationTransition::RemoveUploadItem { seq, position });
            removed = true;
            Ok(Some(envelope))
        }).map_err(plan_fault)?;
        Ok(removed)
    }

    pub fn roll_back(&self, seq: i64) -> Result<CommitResult, DbError> {
        self.roll_back_at(seq, self.generation())
    }

    pub(crate) fn roll_back_at(&self, seq: i64, generation: Generation) -> Result<CommitResult, DbError> {
        let mut discarded = Vec::new();
        let result = self.commit_following(generation, |reader| {
                let outbox = read_outbox(reader)?;
                let entry = outbox.iter().find(|entry| entry.operation.id.0 == seq).ok_or(DbError::UnknownOperation(seq))?;
                let plan = restore(reader, &outbox, entry)?;
                let mut envelope = CommitEnvelope::new(WriteOrigin::Optimistic, generation);
                envelope.entity_ops = plan.entity_ops;
                envelope.scope_ops = plan.scope_ops;
                envelope.operation_transitions = plan.transitions;
                envelope.operation_transitions.push(OperationTransition::Close { seq, status: OperationStatus::RolledBack, once: false });
                discarded = plan.discarded_files;
                Ok(Some(envelope))
            })
            .map_err(plan_fault)?;
        self.shared.files.discard(&discarded);
        Ok(result)
    }
}

fn write_number(row: &mut Map<String, Value>, keys: &[&str], number: usize) {
    let key = keys[0];
    if keys.len() == 1 {
        row.insert(key.to_string(), Value::from(number));
        return;
    }
    let value = row.entry(key.to_string()).or_insert_with(|| Value::Object(Map::new()));
    if !value.is_object() { *value = Value::Object(Map::new()); }
    if let Value::Object(object) = value { write_number(object, &keys[1..], number); }
}
