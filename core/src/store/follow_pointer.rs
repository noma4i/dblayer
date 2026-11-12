use std::collections::{BTreeMap, HashMap, HashSet};

use rusqlite::Connection;
use serde_json::{json, Map, Value};

use crate::store::{BatchPlan, DestroyReason, EntityOp};
use crate::identity::{ModelKey, RowId, RowRef};
use crate::schema::{AssociationKind, AssociationSpec};
use crate::value::{read_id, same_fields};

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
    current: Option<&'a mut super::apply::Prefetched>,
    children_read: usize,
}

impl Ops<'_> {
    fn current(&mut self, reader: &Reader<'_>, parents: &Parents, parent: &RowRef) -> Result<Option<Row>, StoreError> {
        if let Some(after) = parents.rows.get(parent) {
            return Ok(Some(after.clone()));
        }
        if let Some(EntityOp::Upsert { row, .. }) = self.written.get(parent).map(|index| &self.entities[*index]) {
            return Ok(Some(row.clone()));
        }
        self.load(reader, parent)
    }

    fn load(&mut self, reader: &Reader<'_>, row: &RowRef) -> Result<Option<Row>, StoreError> {
        let Some(cache) = self.current.as_deref_mut() else { return reader.point(row.model.as_str(), row.id.as_str()) };
        let key = (row.model.to_string(), row.id.to_string());
        if !cache.contains_key(&key) { cache.insert(key.clone(), reader.stored_row(row.model.as_str(), row.id.as_str())?); }
        Ok(cache.get(&key).and_then(|row| row.as_ref()).map(|row| row.row.clone()))
    }

    fn load_child(&mut self, reader: &Reader<'_>, row: &RowRef) -> Result<Option<Row>, StoreError> {
        self.children_read += 1;
        self.load(reader, row)
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
pub(super) fn follow_parent_pointers(connection: &Connection, models: &BTreeMap<String, ModelInfo>, envelope: &mut BatchPlan, current: Option<&mut super::apply::Prefetched>) -> Result<usize, StoreError> {
    let reader = Reader::new(connection, models);
    let mut parents = Parents::default();
    let written = envelope.entity_ops.iter().enumerate().filter(|(_, op)| matches!(op, EntityOp::Upsert { .. })).map(|(index, op)| (op.row_ref(), index)).collect();
    let doomed = envelope.entity_ops.iter().filter(|op| matches!(op, EntityOp::Destroy { .. })).map(EntityOp::row_ref).collect();
    let (mut changes, rest): (Vec<EntityOp>, Vec<EntityOp>) = envelope.entity_ops.iter().cloned().partition(|op| matches!(op, EntityOp::Destroy { reason: DestroyReason::Replaced { .. }, .. }));
    changes.extend(rest);
    let replaced = std::mem::take(&mut envelope.replaced);
    let mut ops = Ops { entities: &mut envelope.entity_ops, written, doomed, current, children_read: 0 };
    for op in changes {
        match op {
            EntityOp::Upsert { model, row, .. } => advance(models, &reader, (&model, &row), &replaced, &mut ops, &mut parents)?,
            EntityOp::Destroy { model, id, reason: DestroyReason::Replaced { by }, .. } => rename(models, &reader, &model, &id, &by, &mut ops, &mut parents)?,
            EntityOp::Destroy { model, id, .. } => retreat(models, &reader, &model, &id, &mut ops, &mut parents)?,
            EntityOp::Touch { .. } => {}
        }
    }
    let children_read = ops.children_read;
    envelope.replaced = replaced;
    envelope.entity_ops.retain(|op| !matches!(op, EntityOp::Touch { .. }) || !parents.rows.contains_key(&op.row_ref()));
    for (parent, after) in parents.rows {
        envelope.entity_ops.push(EntityOp::Upsert { model: parent.model, id: parent.id, row: after });
    }
    Ok(children_read)
}

fn advance(models: &BTreeMap<String, ModelInfo>, reader: &Reader<'_>, (model, row): (&ModelKey, &Row), replaced: &BTreeMap<RowRef, Row>, ops: &mut Ops<'_>, parents: &mut Parents) -> Result<(), StoreError> {
    let associations = pointer_associations(models, model)?;
    if associations.is_empty() {
        return Ok(());
    }
    let mut canonical = row.clone();
    super::apply::canonicalize(models.get(model.as_str()).ok_or_else(|| StoreError::UnknownModel(model.to_string()))?, &mut canonical);
    let row = &canonical;
    let child = row.get("id").and_then(read_id).map(|id| RowRef::new(model.clone(), id));
    let stored = match &child { Some(child) => ops.load_child(reader, child)?.or_else(|| replaced.get(child).cloned()), None => None };
    for association in associations {
        let Some(parent_id) = row.get(&association.foreign_key).and_then(read_id) else { continue };
        let parent = RowRef::new(association.target.clone(), parent_id);
        if ops.doomed.contains(&parent) {
            continue;
        }
        let Some(current) = ops.current(reader, parents, &parent)? else { continue };
        let written = association.written_by(&current, row, stored.as_ref());
        if written.is_empty() {
            continue;
        }
        let mut merged = current.clone();
        for (parent_field, child_field) in written {
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
    let Some(child) = ops.load(reader, &RowRef::new(model.clone(), id.clone()))? else { return Ok(()) };
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
    let Some(child) = ops.load_child(reader, &RowRef::new(model.clone(), id.clone()))? else { return Ok(()) };
    let table = &models.get(model.as_str()).ok_or_else(|| StoreError::UnknownModel(model.to_string()))?.table;
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
        let mut siblings = Vec::new();
        for sibling in first_ranked(reader, (model, table), &association, &parent_id, &ops.doomed)? {
            siblings.extend(ops.load_child(reader, &sibling)?);
        }
        let previous = association.newest(&siblings);
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

fn first_ranked(reader: &Reader<'_>, (model, table): (&ModelKey, &str), association: &AssociationSpec, parent_id: &RowId, doomed: &HashSet<RowRef>) -> Result<Vec<RowRef>, StoreError> {
    let order = crate::store::ddl::newer_order(association).into_iter().map(|(term, descending)| if descending { format!("{term} DESC") } else { term }).collect::<Vec<_>>().join(", ");
    let fields: String = association.newer.iter().map(|newer| format!(", {}", crate::store::ddl::sql_ident(&crate::store::ddl::column_name(&newer.child)))).collect();
    let foreign_key = crate::store::ddl::sql_ident(&crate::store::ddl::column_name(&association.foreign_key));
    let mut statement = reader.connection().prepare_cached(&format!("SELECT id{fields} FROM {table} WHERE {foreign_key} = ?1 ORDER BY {order}"))?;
    let mut rows = statement.query([parent_id.as_str()])?;
    let mut first: Option<(bool, Vec<rusqlite::types::Value>)> = None;
    let mut found = Vec::new();
    while let Some(row) = rows.next()? {
        let id: String = row.get(0)?;
        let rank = (crate::identity::is_temp_id(&id), (1..=association.newer.len()).map(|column| row.get(column)).collect::<Result<Vec<_>, _>>()?);
        if first.as_ref().is_some_and(|first| *first != rank) {
            break;
        }
        let sibling = RowRef::new(model.clone(), RowId::new(&id).map_err(fault)?);
        if doomed.contains(&sibling) {
            continue;
        }
        first.get_or_insert(rank);
        found.push(sibling);
    }
    Ok(found)
}
