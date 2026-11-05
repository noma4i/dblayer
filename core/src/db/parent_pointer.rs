use std::collections::{BTreeMap, HashMap, HashSet};

use serde_json::{json, Map, Value};

use crate::commit::{CommitEnvelope, DestroyReason, EntityOp};
use crate::identity::{ModelKey, RowId, RowRef};
use crate::query::{Order, Query, Where};
use crate::schema::{AssociationKind, AssociationSpec};
use crate::store::Reader;
use crate::value::{read_id, same_fields, SortDirection};

use super::error::DbError;
use super::open::Db;

type Row = Map<String, Value>;

pub(crate) struct PointerEdit {
    pub parent: RowRef,
    pub before: Row,
    pub after: Row,
}

#[derive(Default)]
pub(crate) struct Followed {
    pub edits: Vec<PointerEdit>,
}

#[derive(Default)]
struct Parents {
    rows: BTreeMap<RowRef, (Row, Row)>,
}

struct Ops<'a> {
    entities: &'a mut Vec<EntityOp>,
    written: HashMap<RowRef, usize>,
    doomed: HashSet<RowRef>,
}

impl Ops<'_> {
    fn current(&self, reader: &Reader<'_>, parents: &Parents, parent: &RowRef) -> Result<Option<Row>, DbError> {
        if let Some((_, after)) = parents.rows.get(parent) {
            return Ok(Some(after.clone()));
        }
        if let Some(EntityOp::Upsert { row, .. }) = self.written.get(parent).map(|index| &self.entities[*index]) {
            return Ok(Some(row.clone()));
        }
        Ok(reader.point(parent.model.as_str(), parent.id.as_str())?)
    }

    fn set(&mut self, parents: &mut Parents, parent: RowRef, current: Row, merged: Row) {
        if let Some(index) = self.written.get(&parent) {
            if let EntityOp::Upsert { row, .. } = &mut self.entities[*index] {
                *row = merged;
            }
            return;
        }
        let before = parents.rows.remove(&parent).map_or(current, |(before, _)| before);
        parents.rows.insert(parent, (before, merged));
    }
}

impl Db {
    pub(crate) fn follow_parent_pointers(&self, reader: &Reader<'_>, envelope: &mut CommitEnvelope) -> Result<Followed, DbError> {
        let mut parents = Parents::default();
        let written = envelope.entity_ops.iter().enumerate().filter(|(_, op)| matches!(op, EntityOp::Upsert { .. })).map(|(index, op)| (op.row_ref(), index)).collect();
        let doomed = envelope.entity_ops.iter().filter(|op| matches!(op, EntityOp::Destroy { .. })).map(EntityOp::row_ref).collect();
        let changes = envelope.entity_ops.clone();
        let mut ops = Ops { entities: &mut envelope.entity_ops, written, doomed };
        for op in changes {
            match op {
                EntityOp::Upsert { model, row, .. } => self.advance_parents(reader, &model, &row, &mut ops, &mut parents)?,
                EntityOp::Destroy { reason: DestroyReason::Replaced { .. }, .. } => {}
                EntityOp::Destroy { model, id, .. } => self.retreat_parents(reader, &model, &id, &mut ops, &mut parents)?,
                EntityOp::Touch { .. } => {}
            }
        }
        let mut followed = Followed::default();
        envelope.entity_ops.retain(|op| !matches!(op, EntityOp::Touch { .. }) || !parents.rows.contains_key(&op.row_ref()));
        for (parent, (before, after)) in parents.rows {
            envelope.entity_ops.push(EntityOp::Upsert { model: parent.model.clone(), id: parent.id.clone(), row: after.clone() });
            followed.edits.push(PointerEdit { parent, before, after });
        }
        Ok(followed)
    }

    fn pointer_associations(&self, model: &ModelKey) -> Result<Vec<AssociationSpec>, DbError> {
        let spec = self.shared.spec(model.as_str())?;
        Ok(spec.associations.iter().filter(|association| association.kind == AssociationKind::BelongsTo && !association.sets.is_empty()).cloned().collect())
    }

    fn advance_parents(&self, reader: &Reader<'_>, model: &ModelKey, row: &Row, ops: &mut Ops<'_>, parents: &mut Parents) -> Result<(), DbError> {
        for association in self.pointer_associations(model)? {
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
                ops.set(parents, parent, current, merged);
            }
        }
        Ok(())
    }

    fn retreat_parents(&self, reader: &Reader<'_>, model: &ModelKey, id: &RowId, ops: &mut Ops<'_>, parents: &mut Parents) -> Result<(), DbError> {
        let associations = self.pointer_associations(model)?;
        if associations.is_empty() {
            return Ok(());
        }
        let Some(child) = reader.point(model.as_str(), id.as_str())? else { return Ok(()) };
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
            let siblings = ops.doomed.iter().filter(|row| row.model == *model).count() as u32;
            let query = Query::new(model.as_str())
                .filtered(Where::equal(&association.foreign_key, json!(parent_id.as_str())))
                .ordered(association.newer.iter().map(|newer| Order::new(&newer.child, SortDirection::Desc)).collect())
                .limited(siblings + 1);
            let mut previous = None;
            for (sibling_id, _) in self.shared.query_keys(reader, &query)? {
                let sibling = RowRef::new(model.clone(), RowId::new(&sibling_id)?);
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
                ops.set(parents, parent, current, merged);
            }
        }
        Ok(())
    }
}
