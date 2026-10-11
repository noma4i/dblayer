use std::collections::{BTreeMap, HashSet};

use serde_json::{json, Map, Value};

use crate::commit::{CommitEnvelope, EntityOp};
use crate::identity::{ModelKey, RowId, RowRef};
use crate::query::{Order, Query, Where};
use crate::schema::{AssociationKind, AssociationSpec};
use crate::store::Reader;
use crate::value::{read_id, SortDirection};

use super::error::DbError;
use super::open::Db;

type Parents = BTreeMap<(String, String), Map<String, Value>>;

fn is_newer(association: &AssociationSpec, parent: &Map<String, Value>, child: &Map<String, Value>) -> bool {
    let Some(newer) = &association.newer else { return true };
    let Some(value) = child.get(&newer.child).and_then(Value::as_str) else { return false };
    parent.get(&newer.parent).and_then(Value::as_str).is_none_or(|current| current < value)
}

impl Db {
    pub(crate) fn follow_parent_pointers(&self, reader: &Reader<'_>, envelope: &mut CommitEnvelope) -> Result<(), DbError> {
        let mut parents = Parents::new();
        let written: HashSet<RowRef> = envelope.entity_ops.iter().filter(|op| matches!(op, EntityOp::Upsert { .. })).map(EntityOp::row_ref).collect();
        let doomed: HashSet<RowRef> = envelope.entity_ops.iter().filter(|op| matches!(op, EntityOp::Destroy { .. })).map(EntityOp::row_ref).collect();
        for op in envelope.entity_ops.clone() {
            match op {
                EntityOp::Upsert { model, row, .. } => self.advance_parents(reader, &model, &row, &written, &mut parents)?,
                EntityOp::Destroy { model, id, .. } => self.retreat_parents(reader, &model, &id, &doomed, &written, &mut parents)?,
                EntityOp::Touch { .. } => {}
            }
        }
        for ((model, id), row) in parents {
            envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new(&model), id: RowId::new(&id)?, row });
        }
        Ok(())
    }

    fn pointer_associations(&self, model: &ModelKey) -> Result<Vec<AssociationSpec>, DbError> {
        let spec = self.shared.spec(model.as_str())?;
        Ok(spec.associations.iter().filter(|association| association.kind == AssociationKind::BelongsTo && !association.sets.is_empty()).cloned().collect())
    }

    fn parent_row(reader: &Reader<'_>, parents: &Parents, association: &AssociationSpec, id: &RowId) -> Result<Option<Map<String, Value>>, DbError> {
        match parents.get(&(association.target.to_string(), id.to_string())) {
            Some(row) => Ok(Some(row.clone())),
            None => Ok(reader.point(association.target.as_str(), id.as_str())?),
        }
    }

    fn advance_parents(&self, reader: &Reader<'_>, model: &ModelKey, row: &Map<String, Value>, written: &HashSet<RowRef>, parents: &mut Parents) -> Result<(), DbError> {
        for association in self.pointer_associations(model)? {
            let Some(parent_id) = row.get(&association.foreign_key).and_then(read_id) else { continue };
            if written.contains(&RowRef::new(association.target.clone(), parent_id.clone())) {
                continue;
            }
            let Some(current) = Self::parent_row(reader, parents, &association, &parent_id)? else { continue };
            if !is_newer(&association, &current, row) {
                continue;
            }
            let mut merged = current.clone();
            for (parent_field, child_field) in &association.sets {
                merged.insert(parent_field.clone(), row.get(child_field).cloned().unwrap_or(Value::Null));
            }
            if merged != current {
                parents.insert((association.target.to_string(), parent_id.to_string()), merged);
            }
        }
        Ok(())
    }

    fn retreat_parents(
        &self,
        reader: &Reader<'_>,
        model: &ModelKey,
        id: &RowId,
        doomed: &HashSet<RowRef>,
        written: &HashSet<RowRef>,
        parents: &mut Parents,
    ) -> Result<(), DbError> {
        let associations = self.pointer_associations(model)?;
        if associations.is_empty() {
            return Ok(());
        }
        let Some(child) = reader.point(model.as_str(), id.as_str())? else { return Ok(()) };
        for association in associations {
            let (Some(newer), Some(parent_id)) = (&association.newer, child.get(&association.foreign_key).and_then(read_id)) else { continue };
            let Some(pointer) = association.sets.iter().find(|(_, child_field)| child_field.as_str() == "id").map(|(parent_field, _)| parent_field) else { continue };
            if written.contains(&RowRef::new(association.target.clone(), parent_id.clone())) {
                continue;
            }
            let Some(current) = Self::parent_row(reader, parents, &association, &parent_id)? else { continue };
            if current.get(pointer).and_then(read_id).as_ref() != Some(id) {
                continue;
            }
            let siblings = doomed.iter().filter(|row| row.model == *model).count() as u32;
            let query = Query::new(model.as_str())
                .filtered(Where::equal(&association.foreign_key, json!(parent_id.as_str())))
                .ordered(vec![Order::new(&newer.child, SortDirection::Desc)])
                .limited(siblings + 1);
            let mut previous = None;
            for (sibling_id, _) in self.shared.query_keys(reader, &query)? {
                let sibling = RowRef::new(model.clone(), RowId::new(&sibling_id)?);
                if doomed.contains(&sibling) {
                    continue;
                }
                previous = reader.point(model.as_str(), &sibling_id)?;
                break;
            }
            let mut merged = current.clone();
            for (parent_field, child_field) in &association.sets {
                merged.insert(parent_field.clone(), previous.as_ref().and_then(|row| row.get(child_field).cloned()).unwrap_or(Value::Null));
            }
            if merged != current {
                parents.insert((association.target.to_string(), parent_id.to_string()), merged);
            }
        }
        Ok(())
    }
}
