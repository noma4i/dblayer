use serde_json::Value;

use crate::commit::{CommitEnvelope, EntityOp, WriteOrigin};
use crate::identity::{ModelKey, RowId};

use super::error::DbError;
use super::local_write::plan_fault;
use super::open::Db;

impl Db {
    pub fn set_local(&self, model: &str, id: &RowId, path: &str, value: Value) -> Result<(), DbError> {
        let spec = self.shared.spec(model)?;
        let slot = spec.local_slot(path).ok_or_else(|| DbError::NotLocalField { model: model.to_string(), path: path.to_string() })?;
        let value = match slot.key {
            None if !value.is_null() => slot.field.decode(&value).ok_or_else(|| DbError::LocalValue { model: model.to_string(), path: path.to_string() })?,
            _ => value,
        };
        let generation = self.generation();
        self.commit_following(generation, |reader| {
                let Some(mut row) = reader.point(model, id.as_str())? else {
                    crate::store::save_pending(reader.connection(), model, id.as_str(), path, &value)?;
                    return Ok::<_, DbError>(None);
                };
                slot.write(&mut row, value);
                let mut envelope = CommitEnvelope::new(WriteOrigin::Optimistic, generation);
                envelope.writes_local = true;
                envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new(model), id: id.clone(), row });
                Ok(Some(envelope))
            })
            .map_err(plan_fault)?;
        Ok(())
    }
}
