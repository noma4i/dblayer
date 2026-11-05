use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map, Value};

use crate::commit::{CommitEnvelope, CommitResult, DestroyReason, EntityOp, OperationTransition, ScopeOp, WriteOrigin};
use crate::operation::OperationStatus;
use crate::files::references_of_row;
use crate::identity::{Generation, ModelKey, RowId};
use crate::store::{Reader, StoreError};

use super::commit::Gate;
use super::error::DbError;
use super::local_write::{plan_fault, OutboxOperation};
use super::open::Db;
use super::prior::{Prior, PriorRow};

pub(crate) type Still<'a> = dyn Fn(&Reader<'_>, &OutboxOperation) -> Result<bool, StoreError> + 'a;

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
    fn new(outbox: &'a [OutboxOperation], own: i64) -> Result<Self, StoreError> {
        let open = outbox
            .iter()
            .filter(|entry| entry.operation.id.0 != own)
            .filter_map(|entry| {
                let hidden = entry.operation.status != OperationStatus::Failed;
                Prior::of(entry).transpose().map(|prior| prior.map(|prior| (entry.operation.id.0, Open { entry, hidden, prior })))
            })
            .collect::<Result<_, _>>()?;
        Ok(Self { open, changed: BTreeSet::new() })
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

    /// The next open operation that changes the same member of the same list. It inherits the state before this operation.
    fn later_member_writer(&self, own: i64, model: &str, id: &str, scope_key: &str) -> Option<i64> {
        self.open.range(own + 1..).find(|(_, open)| open.prior.rows.iter().any(|row| matches!(row, PriorRow::Join { model: m, id: i, scope_key: s, .. } | PriorRow::Leave { model: m, id: i, scope_key: s, .. } if m == model && i == id && s == scope_key))).map(|(seq, _)| *seq)
    }

    fn hand_down_member(&mut self, seq: i64, (model, id, scope_key): (&str, &str, &str), before: &Option<String>) {
        let Some(open) = self.open.get_mut(&seq) else { return };
        for row in &mut open.prior.rows {
            if let PriorRow::Join { model: m, id: i, scope_key: s, before: held, .. } | PriorRow::Leave { model: m, id: i, scope_key: s, before: held, .. } = row {
                if m == model && i == id && s == scope_key {
                    *held = before.clone();
                    self.changed.insert(seq);
                }
            }
        }
    }

    fn reown_member(&mut self, own: i64, (model, id, scope_key): (&str, &str, &str), revision: i64) {
        let earlier = self.open.range(..own).rev().find(|(_, open)| open.prior.rows.iter().any(|row| matches!(row, PriorRow::Join { model: m, id: i, scope_key: s, .. } | PriorRow::Leave { model: m, id: i, scope_key: s, .. } if m == model && i == id && s == scope_key))).map(|(seq, _)| *seq);
        let Some(seq) = earlier else { return };
        let Some(open) = self.open.get_mut(&seq) else { return };
        for row in &mut open.prior.rows {
            if let PriorRow::Join { model: m, id: i, scope_key: s, owned, .. } | PriorRow::Leave { model: m, id: i, scope_key: s, owned, .. } = row {
                if m == model && i == id && s == scope_key { *owned = revision; self.changed.insert(seq); }
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
        PriorRow::Created { .. } | PriorRow::Join { .. } | PriorRow::Leave { .. } => false,
    }
}

fn restore(reader: &Reader<'_>, outbox: &[OutboxOperation], entry: &OutboxOperation) -> Result<Restore, StoreError> {
    let mut plan = Restore::default();
    let Some(prior) = Prior::of(entry)? else { return Ok(plan) };
    let own = entry.operation.id.0;
    let mut chain = Chain::new(outbox, own)?;
    let revision = reader.revision_seq()? + 1;
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
                let (entity_ops, scope_ops) = row.reappearing();
                plan.entity_ops.extend(entity_ops);
                plan.scope_ops.extend(scope_ops);
            }
            PriorRow::Join { model, id, scope_key, before, .. } | PriorRow::Leave { model, id, scope_key, before, .. } => {
                restore_member(reader, &mut chain, own, (model, id, scope_key), before, &mut plan)?;
            }
            PriorRow::Edited { model, id, before, owned } => {
                restore_edit(reader, &mut chain, own, (model, id), (before, owned), revision, &mut plan)?;
            }
        }
    }
    plan.transitions.extend(chain.rewrites());
    Ok(plan)
}

/// A refused change of a list member puts the member back the way it was before the operation, unless a later operation changed it again: that one inherits the state and decides.
fn restore_member(reader: &Reader<'_>, chain: &mut Chain<'_>, own: i64, (model, id, scope_key): (&str, &str, &str), before: &Option<String>, plan: &mut Restore) -> Result<(), StoreError> {
    if let Some(later) = chain.later_member_writer(own, model, id, scope_key) {
        chain.hand_down_member(later, (model, id, scope_key), before);
        return Ok(());
    }
    let owns = reader.connection().query_row(
        "SELECT EXISTS (SELECT 1 FROM op_effect e JOIN scope_revision r ON r.model = e.model AND r.scope_key = e.scope_key AND r.row_id = e.row_id AND r.operation_seq = e.seq WHERE e.seq = ?1 AND e.model = ?2 AND e.scope_key = ?3 AND e.row_id = ?4 AND e.kind IN ('join', 'leave'))",
        rusqlite::params![own, model, scope_key, id], |row| row.get::<_, bool>(0),
    )?;
    if !owns { return Ok(()); }
    let (Ok(row), key) = (RowId::new(id), crate::identity::ListKey::from_raw(scope_key)) else { return Ok(()) };
    let scope = crate::commit::ScopeRef::new(ModelKey::new(model), key);
    let present = reader.scope_has_member(model, scope_key, id)?;
    chain.reown_member(own, (model, id, scope_key), reader.revision_seq()? + 1);
    match (before, present) {
        (Some(order_key), true) => plan.scope_ops.push(ScopeOp::Move { scope, row, order_key: order_key.clone() }),
        (Some(order_key), false) => plan.scope_ops.push(ScopeOp::Append { scope, row, order_key: order_key.clone() }),
        (None, true) => plan.scope_ops.push(ScopeOp::Detach { scope, row }),
        (None, false) => {}
    }
    Ok(())
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
    pub(crate) fn remove_upload_item(&self, admitted: &crate::ffi::legacy::Admitted, model: &str, id: &RowId, seq: i64, remove_row: bool) -> Result<bool, DbError> {
        self.remove_upload_item_if(admitted, model, id, seq, remove_row, None)
    }

    pub(crate) fn remove_upload_item_if(&self, admitted: &crate::ffi::legacy::Admitted, model: &str, id: &RowId, seq: i64, remove_row: bool, still: Option<&Still<'_>>) -> Result<bool, DbError> {
        let generation = self.generation();
        let spec = self.spec(model)?;
        let mut removed = false;
        self.commit_following(&admitted.authority(), generation, |reader| {
            removed = false;
            let Some(entry) = OutboxOperation::read(reader, seq)? else { return Ok(None) };
            if let Some(still) = still { if !still(reader, &entry)? { return Ok(None); } }
            if entry.operation.transport_started && entry.operation.status == OperationStatus::Pending { return Err(DbError::InFlight { model: model.into(), id: id.to_string() }); }
            let position = reader.connection().query_row("SELECT position FROM op_item WHERE seq = ?1 AND row_id = ?2", rusqlite::params![seq, id.as_str()], |row| row.get(0))?;
            let mut envelope = CommitEnvelope::new(WriteOrigin::Optimistic, generation);
            let mut prior = Prior::of(&entry)?.unwrap_or_default();
            if remove_row {
                let mut selected = entry.clone();
                selected.rollback = Some(Prior { rows: prior.rows.iter().filter(|row| row.id() == id.as_str()).cloned().collect() }.to_value());
                let plan = restore(reader, &Prior::related_operations(reader, &selected)?, &selected)?;
                envelope.entity_ops = plan.entity_ops;
                envelope.scope_ops = plan.scope_ops;
                envelope.operation_transitions = plan.transitions;
            }
            prior.rows.retain(|row| row.id() != id.as_str());
            let rows: Vec<_> = entry.operation.row_ids.iter().filter(|row| *row != id).cloned().collect();
            let mut inputs = Vec::new();
            if let Some(batch) = crate::operation::Operation::upload_action(&entry.operation.action_key).and_then(|(_, name)| spec.upload_action(&name)).and_then(|action| action.batch.as_ref()).filter(|batch| rows.len() >= batch.min) {
                if let Some(fields) = &batch.renumber {
                    let position = spec.integer_path(&fields.position).ok_or_else(|| DbError::InvalidUpload(fields.position.clone()))?;
                    let size = spec.integer_path(&fields.size).ok_or_else(|| DbError::InvalidUpload(fields.size.clone()))?;
                    for (index, row_id) in rows.iter().enumerate() {
                        if let Some(stored) = reader.stored_row(model, row_id.as_str())? {
                            let mut row = stored.row;
                            write_number(&mut row, &position, index + fields.position_from);
                            write_number(&mut row, &size, rows.len());
                            let item_position: i64 = reader.connection().query_row("SELECT position FROM op_item WHERE seq = ?1 AND row_id = ?2", rusqlite::params![seq, row_id.as_str()], |row| row.get(0))?;
                            let value = crate::operation::read_item_value(reader.connection(), seq, item_position, "input")?.ok_or_else(|| DbError::InvalidUpload("upload input is missing".into()))?;
                            let mut input: crate::upload::UploadInput = serde_json::from_value(value).map_err(|error| DbError::InvalidUpload(error.to_string()))?;
                            for (target, source) in &batch.input {
                                input.variables.insert(target.clone(), row.get(source).cloned().unwrap_or(Value::Null));
                            }
                            inputs.push((item_position, input));
                            envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new(model), id: row_id.clone(), row });
                        }
                    }
                }
            }
            envelope.operation_transitions.push(entry.rewritten(rows, entry.input.clone(), Some(prior.to_value())));
            envelope.operation_transitions.push(OperationTransition::RemoveUploadItem { seq, position, inputs });
            removed = true;
            Ok(Some(envelope))
        }).map_err(plan_fault)?;
        Ok(removed)
    }

    pub fn roll_back(&self, admitted: &crate::ffi::legacy::Admitted, seq: i64) -> Result<CommitResult, DbError> {
        self.roll_back_at(admitted, seq, self.generation())
    }

    pub(crate) fn roll_back_at(&self, admitted: &crate::ffi::legacy::Admitted, seq: i64, generation: Generation) -> Result<CommitResult, DbError> {
        self.roll_back_if(admitted, seq, generation, None)
    }

    pub(crate) fn roll_back_if(&self, admitted: &crate::ffi::legacy::Admitted, seq: i64, generation: Generation, still: Option<&Still<'_>>) -> Result<CommitResult, DbError> {
        self.roll_back_gated(&admitted.authority(), seq, generation, still)
    }

    pub(crate) fn roll_back_gated(&self, gate: &Gate, seq: i64, generation: Generation, still: Option<&Still<'_>>) -> Result<CommitResult, DbError> {
        let mut discarded = Vec::new();
        let result = self.commit_following(gate, generation, |reader| {
                discarded = Vec::new();
                let found = OutboxOperation::read(reader, seq)?;
                let entry = match (found, still) {
                    (Some(entry), None) => entry,
                    (Some(entry), Some(still)) => {
                        if !still(reader, &entry)? { return Ok(None); }
                        entry
                    }
                    (None, None) => return Err(DbError::UnknownOperation(seq)),
                    (None, Some(_)) => return Ok(None),
                };
                let plan = restore(reader, &Prior::related_operations(reader, &entry)?, &entry)?;
                let mut envelope = CommitEnvelope::new(WriteOrigin::Optimistic, generation);
                envelope.entity_ops = plan.entity_ops;
                envelope.scope_ops = plan.scope_ops;
                envelope.operation_transitions = plan.transitions;
                envelope.operation_transitions.push(OperationTransition::Close { seq, status: OperationStatus::RolledBack });
                discarded = plan.discarded_files;
                Ok(Some(envelope))
            })
            .map_err(plan_fault)?;
        self.shared.files.discard(&discarded).map_err(super::files::io_fault)?;
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
