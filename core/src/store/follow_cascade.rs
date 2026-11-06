use std::collections::{BTreeMap, HashSet};

use rusqlite::Connection;
use serde_json::json;

use crate::store::{BatchPlan, DestroyReason, EntityOp};
use crate::identity::{RowId, RowRef};
use crate::query::{compile_keys, fetch_keys, Query, Where};
use crate::schema::Dependent;

use super::{ModelInfo, Reader, StoreError};

fn fault(error: impl std::fmt::Display) -> StoreError { StoreError::Constraint(error.to_string()) }

pub(super) fn descendants(models: &BTreeMap<String, ModelInfo>, reader: &Reader<'_>, parent: &RowRef, seen: &mut HashSet<RowRef>, result: &mut Vec<RowRef>) -> Result<(), StoreError> {
    let spec = &models.get(parent.model.as_str()).ok_or_else(|| StoreError::UnknownModel(parent.model.to_string()))?.spec;
    for association in spec.associations.iter().filter(|association| association.owns_children() && association.dependent == Some(Dependent::Destroy)) {
        let model = association.target.as_str();
        let child_spec = &models.get(model).ok_or_else(|| StoreError::UnknownModel(model.to_string()))?.spec;
        let query = Query::new(model).filtered(Where::equal(&association.foreign_key, json!(parent.id.as_str())));
        let statement = compile_keys(&query, child_spec).map_err(fault)?;
        for (id, _) in fetch_keys(reader.connection(), &statement).map_err(fault)? {
            let child = RowRef::new(association.target.clone(), RowId::new(&id).map_err(fault)?);
            if seen.insert(child.clone()) {
                descendants(models, reader, &child, seen, result)?;
                result.push(child);
            }
        }
    }
    Ok(())
}

/// A destroyed row takes the rows of its `dependent: :destroy` children with it, in the same batch and for the same reason.
pub(super) fn follow_destroys(connection: &Connection, models: &BTreeMap<String, ModelInfo>, envelope: &mut BatchPlan) -> Result<(), StoreError> {
    let reader = Reader::new(connection, models);
    let mut seen: HashSet<RowRef> = envelope.entity_ops.iter().filter(|&op| matches!(op, EntityOp::Destroy { .. })).map(|op| op.row_ref()).collect();
    let parents: Vec<(RowRef, DestroyReason)> = envelope.entity_ops.iter().filter_map(|op| match op {
        EntityOp::Destroy { model, id, reason } if !matches!(reason, DestroyReason::Replaced { .. }) => Some((RowRef::new(model.clone(), id.clone()), reason.clone())),
        _ => None,
    }).collect();
    for (parent, reason) in parents {
        let mut children = Vec::new();
        descendants(models, &reader, &parent, &mut HashSet::from([parent.clone()]), &mut children)?;
        for child in children {
            if seen.insert(child.clone()) {
                envelope.entity_ops.push(EntityOp::Destroy { model: child.model, id: child.id, reason: reason.clone() });
            }
        }
    }
    Ok(())
}
