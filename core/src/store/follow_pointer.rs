use std::collections::{BTreeMap, HashMap, HashSet};

use rusqlite::Connection;
use serde_json::{json, Map, Value};

use crate::store::{BatchPlan, DestroyReason, EntityOp};
use crate::identity::{ModelKey, RowId, RowRef};
use crate::query::{compile_keys, fetch_keys, Order, Query, Where};
use crate::schema::{AssociationKind, AssociationSpec};
use crate::value::{read_id, same_fields, SortDirection};

use super::{ModelInfo, Reader, StoreError};

type Row = Map<String, Value>;

fn fault(error: impl std::fmt::Display) -> StoreError { StoreError::Constraint(error.to_string()) }

#[derive(Default)]
struct Parents {
    rows: BTreeMap<RowRef, Row>,
}

struct Ops<'a> {
    entities: &'a mut Vec<EntityOp>,
    written: HashMap<RowRef, usize>,
    doomed: HashSet<RowRef>,
}

impl Ops<'_> {
    fn current(&self, reader: &Reader<'_>, parents: &Parents, parent: &RowRef) -> Result<Option<Row>, StoreError> {
        if let Some(after) = parents.rows.get(parent) {
            return Ok(Some(after.clone()));
        }
        if let Some(EntityOp::Upsert { row, .. }) = self.written.get(parent).map(|index| &self.entities[*index]) {
            return Ok(Some(row.clone()));
        }
        reader.point(parent.model.as_str(), parent.id.as_str())
    }

    fn set(&mut self, parents: &mut Parents, parent: RowRef, merged: Row) {
        if let Some(index) = self.written.get(&parent) {
            if let EntityOp::Upsert { row, .. } = &mut self.entities[*index] {
                *row = merged;
            }
            return;
        }
        parents.rows.insert(parent, merged);
    }
}

fn pointer_associations(models: &BTreeMap<String, ModelInfo>, model: &ModelKey) -> Result<Vec<AssociationSpec>, StoreError> {
    let spec = &models.get(model.as_str()).ok_or_else(|| StoreError::UnknownModel(model.to_string()))?.spec;
    Ok(spec.associations.iter().filter(|association| association.kind == AssociationKind::BelongsTo && !association.sets.is_empty()).cloned().collect())
}

/// A parent row names its newest child in `sets`: a landing child advances the pointer, a replaced child renames it, a destroyed child hands it to the previous sibling.
pub(super) fn follow_parent_pointers(connection: &Connection, models: &BTreeMap<String, ModelInfo>, envelope: &mut BatchPlan) -> Result<(), StoreError> {
    let reader = Reader::new(connection, models);
    let mut parents = Parents::default();
    let written = envelope.entity_ops.iter().enumerate().filter(|(_, op)| matches!(op, EntityOp::Upsert { .. })).map(|(index, op)| (op.row_ref(), index)).collect();
    let doomed = envelope.entity_ops.iter().filter(|op| matches!(op, EntityOp::Destroy { .. })).map(EntityOp::row_ref).collect();
    let changes = envelope.entity_ops.clone();
    let mut ops = Ops { entities: &mut envelope.entity_ops, written, doomed };
    for op in changes {
        match op {
            EntityOp::Upsert { model, row, .. } => advance(models, &reader, &model, &row, &mut ops, &mut parents)?,
            EntityOp::Destroy { model, id, reason: DestroyReason::Replaced { by }, .. } => rename(models, &reader, &model, &id, &by, &mut ops, &mut parents)?,
            EntityOp::Destroy { model, id, .. } => retreat(models, &reader, &model, &id, &mut ops, &mut parents)?,
            EntityOp::Touch { .. } => {}
        }
    }
    envelope.entity_ops.retain(|op| !matches!(op, EntityOp::Touch { .. }) || !parents.rows.contains_key(&op.row_ref()));
    for (parent, after) in parents.rows {
        envelope.entity_ops.push(EntityOp::Upsert { model: parent.model, id: parent.id, row: after });
    }
    Ok(())
}

fn advance(models: &BTreeMap<String, ModelInfo>, reader: &Reader<'_>, model: &ModelKey, row: &Row, ops: &mut Ops<'_>, parents: &mut Parents) -> Result<(), StoreError> {
    for association in pointer_associations(models, model)? {
        let Some(parent_id) = row.get(&association.foreign_key).and_then(read_id) else { continue };
        let parent = RowRef::new(association.target.clone(), parent_id);
        if ops.doomed.contains(&parent) {
            continue;
        }
        let Some(current) = ops.current(reader, parents, &parent)? else { continue };
        if !association.child_is_newer(&current, row) {
            continue;
        }
        let mut merged = current.clone();
        for (parent_field, child_field) in &association.sets {
            if let Some(value) = row.get(child_field) {
                merged.insert(parent_field.clone(), value.clone());
            }
        }
        if !same_fields(&merged, &current) {
            ops.set(parents, parent, merged);
        }
    }
    Ok(())
}

fn rename(models: &BTreeMap<String, ModelInfo>, reader: &Reader<'_>, model: &ModelKey, id: &RowId, by: &RowId, ops: &mut Ops<'_>, parents: &mut Parents) -> Result<(), StoreError> {
    let associations = pointer_associations(models, model)?;
    if associations.is_empty() {
        return Ok(());
    }
    let Some(child) = reader.point(model.as_str(), id.as_str())? else { return Ok(()) };
    for association in associations {
        let Some(parent_id) = child.get(&association.foreign_key).and_then(read_id) else { continue };
        let Some(pointer) = association.pointer_field() else { continue };
        let parent = RowRef::new(association.target.clone(), parent_id);
        if ops.doomed.contains(&parent) {
            continue;
        }
        let Some(current) = ops.current(reader, parents, &parent)? else { continue };
        if current.get(pointer).and_then(read_id).as_ref() != Some(id) {
            continue;
        }
        let mut merged = current.clone();
        merged.insert(pointer.to_string(), json!(by.as_str()));
        ops.set(parents, parent, merged);
    }
    Ok(())
}

fn retreat(models: &BTreeMap<String, ModelInfo>, reader: &Reader<'_>, model: &ModelKey, id: &RowId, ops: &mut Ops<'_>, parents: &mut Parents) -> Result<(), StoreError> {
    let associations = pointer_associations(models, model)?;
    if associations.is_empty() {
        return Ok(());
    }
    let Some(child) = reader.point(model.as_str(), id.as_str())? else { return Ok(()) };
    let child_spec = &models.get(model.as_str()).ok_or_else(|| StoreError::UnknownModel(model.to_string()))?.spec;
    for association in associations {
        let Some(parent_id) = child.get(&association.foreign_key).and_then(read_id).filter(|_| !association.newer.is_empty()) else { continue };
        let Some(pointer) = association.pointer_field() else { continue };
        let parent = RowRef::new(association.target.clone(), parent_id.clone());
        if ops.doomed.contains(&parent) {
            continue;
        }
        let Some(current) = ops.current(reader, parents, &parent)? else { continue };
        if current.get(pointer).and_then(read_id).as_ref() != Some(id) {
            continue;
        }
        let siblings = u32::try_from(ops.doomed.iter().filter(|row| row.model == *model).count()).unwrap_or(u32::MAX);
        let query = Query::new(model.as_str())
            .filtered(Where::equal(&association.foreign_key, json!(parent_id.as_str())))
            .ordered(association.newer.iter().map(|newer| Order::new(&newer.child, SortDirection::Desc)).collect())
            .limited(siblings + 1);
        let statement = compile_keys(&query, child_spec).map_err(fault)?;
        let mut previous = None;
        for (sibling_id, _) in fetch_keys(reader.connection(), &statement).map_err(fault)? {
            let sibling = RowRef::new(model.clone(), RowId::new(&sibling_id).map_err(fault)?);
            if ops.doomed.contains(&sibling) {
                continue;
            }
            previous = reader.point(model.as_str(), &sibling_id)?;
            break;
        }
        let mut merged = current.clone();
        for (parent_field, child_field) in &association.sets {
            merged.insert(parent_field.clone(), previous.as_ref().and_then(|row| row.get(child_field).cloned()).unwrap_or(Value::Null));
        }
        if !same_fields(&merged, &current) {
            ops.set(parents, parent, merged);
        }
    }
    Ok(())
}
