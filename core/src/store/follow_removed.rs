use std::collections::BTreeMap;

use rusqlite::Connection;
use serde_json::json;

use crate::store::{BatchPlan, DestroyReason, EntityOp};
use crate::identity::{ModelKey, RowId};
use crate::store::query::{compile_keys, fetch_keys, Query, Where};

use super::{ModelInfo, StoreError};
use super::window_write::fault;


/// A row that lands removes the rows of the models that name its model in `removed_by` and match it, in the same batch.
pub(super) fn follow_removed_by(connection: &Connection, models: &BTreeMap<String, ModelInfo>, envelope: &mut BatchPlan) -> Result<(), StoreError> {
    let landed: Vec<(ModelKey, serde_json::Map<String, serde_json::Value>)> = envelope.entity_ops.iter().filter_map(|op| match op {
        EntityOp::Upsert { model, row, .. } => Some((model.clone(), row.clone())),
        _ => None,
    }).collect();
    for (source, row) in landed {
        for info in models.values().filter(|info| info.spec.removed_by.iter().any(|removal| removal.model == source.as_str())) {
            let spec = &info.spec;
            for removal in spec.removed_by.iter().filter(|removal| removal.model == source.as_str()) {
                let clauses: Option<Vec<Where>> = removal.matches.iter().map(|(own, theirs)| row.get(theirs).filter(|value| !value.is_null()).map(|value| Where::equal(own, json!(value)))).collect();
                let Some(clauses) = clauses.filter(|clauses| !clauses.is_empty()) else { continue };
                let query = Query::new(spec.key.as_str()).filtered(Where::AllOf { clauses });
                let statement = compile_keys(&query, spec).map_err(fault)?;
                for (id, _) in fetch_keys(connection, &statement).map_err(fault)? {
                    let id = RowId::new(&id).map_err(fault)?;
                    if !envelope.entity_ops.iter().any(|op| matches!(op, EntityOp::Destroy { model, id: doomed, .. } if model == &spec.key && doomed == &id)) {
                        envelope.entity_ops.push(EntityOp::Destroy { model: spec.key.clone(), id, reason: DestroyReason::Local });
                    }
                }
            }
        }
    }
    Ok(())
}
