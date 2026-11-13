use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map, Value};

use crate::store::{BatchPlan, CommitResult, DestroyReason, EntityOp, OperationTransition, ScopeOp};
use crate::owners::write::operation_status::OperationStatus;
use crate::store::files::references_of_row;
use crate::identity::{ModelKey, RowId, RowRef};
use crate::store::{Reader, StoreError};

use crate::store::plan::{OutboxOperation, Prior, PriorRow};

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

fn restore(reader: &Reader<'_>, outbox: &[OutboxOperation], entry: &OutboxOperation, revision: i64, rows: &super::apply::Prefetched) -> Result<Restore, StoreError> {
    let mut plan = Restore::default();
    let Some(prior) = Prior::of(entry)? else { return Ok(plan) };
    let own = entry.operation.id.0;
    let mut chain = Chain::new(outbox, own)?;
    for row in prior.rows.iter().rev() {
        let Ok(row_id) = RowId::new(row.id()) else { continue };
        match row {
            PriorRow::Created { model, .. } => {
                plan.entity_ops.push(EntityOp::Destroy { model: ModelKey::new(model), id: row_id.clone(), reason: DestroyReason::Expired });
                plan.discarded_files.extend(references_of_row(reader.connection(), model, row_id.as_str())?.into_iter().map(|file| file.id));
            }
            PriorRow::Destroyed { .. } if entry.operation.status == OperationStatus::Failed => {}
            PriorRow::Destroyed { .. } => {
                let (entity_ops, scope_ops) = row.reappearing();
                plan.entity_ops.extend(entity_ops);
                plan.scope_ops.extend(scope_ops);
            }
            PriorRow::Join { model, id, scope_key, before, .. } | PriorRow::Leave { model, id, scope_key, before, .. } => {
                restore_member(reader, &mut chain, own, (model, id, scope_key), before, revision, &mut plan)?;
            }
            PriorRow::Edited { model, id, before, owned } => {
                restore_edit(rows, &mut chain, own, (model, id), (before, owned), revision, &mut plan)?;
            }
        }
    }
    let created: BTreeSet<RowRef> = plan.entity_ops.iter().filter(|op| matches!(op, EntityOp::Destroy { reason: DestroyReason::Expired, .. })).map(EntityOp::row_ref).collect();
    plan.entity_ops.retain(|op| matches!(op, EntityOp::Destroy { .. }) || !created.contains(&op.row_ref()));
    plan.transitions.extend(chain.rewrites());
    Ok(plan)
}

/// A refused change of a list member puts the member back the way it was before the operation, unless a later operation changed it again: that one inherits the state and decides.
fn restore_member(reader: &Reader<'_>, chain: &mut Chain<'_>, own: i64, (model, id, scope_key): (&str, &str, &str), before: &Option<String>, revision: i64, plan: &mut Restore) -> Result<(), StoreError> {
    if let Some(later) = chain.later_member_writer(own, model, id, scope_key) {
        chain.hand_down_member(later, (model, id, scope_key), before);
        return Ok(());
    }
    let owns = reader.connection().query_row(
        include_str!("sql/rollback/restore_member.sql"),
        rusqlite::params![own, model, scope_key, id], |row| row.get::<_, bool>(0),
    )?;
    if !owns { return Ok(()); }
    let (Ok(row), key) = (RowId::new(id), crate::identity::ListKey::from_raw(scope_key)) else { return Ok(()) };
    let scope = crate::store::ScopeRef::new(ModelKey::new(model), key);
    let present = reader.scope_has_member(model, scope_key, id)?;
    chain.reown_member(own, (model, id, scope_key), revision);
    match (before, present) {
        (Some(order_key), true) => plan.scope_ops.push(ScopeOp::Move { scope, row, order_key: order_key.clone() }),
        (Some(order_key), false) => plan.scope_ops.push(ScopeOp::Append { scope, row, order_key: order_key.clone() }),
        (None, true) => plan.scope_ops.push(ScopeOp::Detach { scope, row }),
        (None, false) => {}
    }
    Ok(())
}

fn restore_edit(
    rows: &super::apply::Prefetched,
    chain: &mut Chain<'_>,
    own: i64,
    (model, id): (&str, &str),
    (before, owned): (&Map<String, Value>, &BTreeMap<String, i64>),
    revision: i64,
    plan: &mut Restore,
) -> Result<(), StoreError> {
    let stored = rows.get(&(model.to_string(), id.to_string())).cloned().flatten();
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

pub(super) fn apply(writer: &mut super::row_write::RowWrite<'_>, op_id: u64) -> Result<CommitResult, StoreError> {
    let seq = i64::try_from(op_id).map_err(|_| StoreError::Constraint("operation id is too large".into()))?;
    let connection = writer.connection;
    let models = writer.models;
    let reader = Reader::new(connection, models);
    let entry = OutboxOperation::read(&reader, seq)?.ok_or_else(|| StoreError::Constraint(format!("operation {seq} does not exist")))?;
    let mut wanted = BTreeMap::<String, Vec<String>>::new();
    for row in Prior::of(&entry)?.into_iter().flat_map(|prior| prior.rows) {
        if let PriorRow::Edited { model, id, .. } = row { wanted.entry(model).or_default().push(id); }
    }
    for (model, ids) in wanted { writer.prefetch(&model, &ids, &writer.closing.clone())?; }
    let plan = restore(&reader, &Prior::related_operations(&reader, &entry)?, &entry, writer.revision, &writer.current)?;
    let mut envelope = BatchPlan::new();
    envelope.entity_ops = plan.entity_ops;
    envelope.scope_ops = plan.scope_ops;
    envelope.operation_transitions = plan.transitions;
    envelope.operation_transitions.push(OperationTransition::Close { seq, status: OperationStatus::RolledBack });
    writer.apply(&envelope)
}
