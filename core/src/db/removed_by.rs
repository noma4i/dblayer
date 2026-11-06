use serde_json::json;

use crate::commit::{CommitEnvelope, DestroyReason, EntityOp};
use crate::identity::{ModelKey, RowId};
use crate::query::{Query, Where};
use crate::store::Reader;

use super::error::DbError;
use super::open::Db;

impl Db {
    /// A row that lands removes the rows of the models that name its model in `removed_by` and match it, in the same batch (M40).
    pub(crate) fn follow_removed_by(&self, reader: &Reader<'_>, envelope: &mut CommitEnvelope) -> Result<(), DbError> {
        let landed: Vec<(ModelKey, serde_json::Map<String, serde_json::Value>)> = envelope.entity_ops.iter().filter_map(|op| match op {
            EntityOp::Upsert { model, row, .. } => Some((model.clone(), row.clone())),
            _ => None,
        }).collect();
        for (source, row) in landed {
            for spec in self.shared.specs.values().filter(|spec| spec.removed_by.iter().any(|removal| removal.model == source.as_str())) {
                for removal in spec.removed_by.iter().filter(|removal| removal.model == source.as_str()) {
                    let clauses: Option<Vec<Where>> = removal.matches.iter().map(|(own, theirs)| row.get(theirs).filter(|value| !value.is_null()).map(|value| Where::equal(own, json!(value)))).collect();
                    let Some(clauses) = clauses.filter(|clauses| !clauses.is_empty()) else { continue };
                    let query = Query::new(spec.key.as_str()).filtered(Where::AllOf { clauses });
                    for (id, _) in self.shared.query_keys(reader, &query)? {
                        let id = RowId::new(&id).map_err(|error| DbError::Internal(error.to_string()))?;
                        if !envelope.entity_ops.iter().any(|op| matches!(op, EntityOp::Destroy { model, id: doomed, .. } if model == &spec.key && doomed == &id)) {
                            envelope.entity_ops.push(EntityOp::Destroy { model: spec.key.clone(), id, reason: DestroyReason::Local });
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
