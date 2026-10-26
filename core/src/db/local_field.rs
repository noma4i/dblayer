use serde_json::Value;

use crate::commit::{CommitEnvelope, EntityOp, PendingLocal, WriteOrigin};
use crate::identity::{ModelKey, RowId};
use crate::schema::ValueFault;

use super::error::DbError;
use super::local_write::plan_fault;
use super::open::Db;

impl Db {
    pub fn set_local(&self, model: &str, id: &RowId, path: &str, value: Value) -> Result<(), DbError> {
        let spec = self.shared.spec(model)?;
        let slot = spec.local_slot(path).ok_or_else(|| DbError::NotLocalField { model: model.to_string(), path: path.to_string() })?;
        let value = match slot.key {
            None if !value.is_null() => {
                match slot.field.fault(&value, path, true, &mut Vec::new()) {
                    Some(ValueFault::Unknown(field)) => return Err(DbError::UnknownField { model: model.to_string(), field }),
                    Some(ValueFault::Invalid(_)) => return Err(DbError::LocalValue { model: model.to_string(), path: path.to_string() }),
                    None => slot.field.decode(&value).ok_or_else(|| DbError::LocalValue { model: model.to_string(), path: path.to_string() })?,
                }
            }
            _ => value,
        };
        let generation = self.generation();
        self.commit_following(generation, |reader| {
                let mut envelope = CommitEnvelope::new(WriteOrigin::Optimistic, generation);
                let Some(mut row) = reader.point(model, id.as_str())? else {
                    envelope.pending_locals.push(PendingLocal { model: model.to_string(), row_id: id.to_string(), path: path.to_string(), value: value.clone() });
                    return Ok::<_, DbError>(Some(envelope));
                };
                slot.write(&mut row, value.clone());
                envelope.writes_local = true;
                envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new(model), id: id.clone(), row });
                Ok(Some(envelope))
            })
            .map_err(plan_fault)?;
        Ok(())
    }
}
