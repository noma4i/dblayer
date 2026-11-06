use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use crate::store::{RowChange, RowChangeKind};
use crate::identity::{ModelKey, RowId, RowRef};

use super::overlay::{OverlaySnapshot, RowKey};
use super::{ModelInfo, StoreError};

pub(super) fn apply(snapshot: &mut OverlaySnapshot, models: &BTreeMap<String, ModelInfo>, writes: super::feed_write::OverlayWrites) -> Result<(Vec<RowChange>, Vec<crate::owners::store::EphemeralReset>), StoreError> {
    let mut arms: Vec<crate::owners::store::EphemeralReset> = Vec::new();
    let mut before = BTreeMap::new();
    for (model, row_id, values) in writes {
        let info = models.get(&model).ok_or_else(|| StoreError::UnknownModel(model.clone()))?;
        let row = RowKey { model: model.clone(), row_id: row_id.clone() };
        for (name, value) in values {
            let field = info.spec.ephemeral_field(&name).ok_or_else(|| StoreError::Constraint(format!("{model}.{name} is not ephemeral")))?;
            let value = if value.is_null() && field.field.nullable { Value::Null } else { field.field.decode(&value).ok_or_else(|| StoreError::Constraint(format!("invalid value for {model}.{name}")))? };
            let key = (model.clone(), row_id.clone(), name.clone());
            before.entry(key).or_insert_with(|| snapshot.values.get(&row).and_then(|fields| fields.get(&name)).map_or_else(|| field.initial(), |slot| slot.value.clone()));
            let position = snapshot.set(row.clone(), name.clone(), value);
            arms.retain(|arm| !(arm.model == model && arm.row_id == row_id && arm.field == name));
            if let Some(window) = field.reset_window() { arms.push(crate::owners::store::EphemeralReset { model: model.clone(), row_id: row_id.clone(), field: name, position, after_ms: u64::try_from(window.as_millis()).unwrap_or(u64::MAX) }); }
        }
    }
    let mut changed: BTreeMap<RowRef, BTreeSet<String>> = BTreeMap::new();
    for ((model, id, field), before) in before {
        let key = RowKey { model: model.clone(), row_id: id.clone() };
        let Some(slot) = snapshot.values.get(&key).and_then(|fields| fields.get(&field)) else {
            return Err(StoreError::Constraint(format!("the written field {field} of {model} is missing")));
        };
        if before != slot.value {
            let id = RowId::new(&id).map_err(|error| StoreError::Constraint(error.to_string()))?;
            changed.entry(RowRef::new(ModelKey::new(&model), id)).or_default().insert(field);
        }
    }
    Ok((changed.into_iter().map(|(row, fields)| RowChange { row, kind: RowChangeKind::Updated, fields }).collect(), arms))
}

/// Fields whose value written at `position` is still in the overlay go back to the default.
pub(super) fn reset(snapshot: &mut OverlaySnapshot, models: &BTreeMap<String, ModelInfo>, resets: &[(String, String, String, u64)]) -> Result<Vec<RowChange>, StoreError> {
    let mut changes = Vec::new();
    for (model, row_id, field, position) in resets {
        let info = models.get(model).ok_or_else(|| StoreError::UnknownModel(model.clone()))?;
        let spec = info.spec.ephemeral_field(field).ok_or_else(|| StoreError::Constraint(format!("{model}.{field} is not ephemeral")))?;
        let key = RowKey { model: model.clone(), row_id: row_id.clone() };
        let held = snapshot.values.get(&key).and_then(|fields| fields.get(field)).map(|slot| slot.value.clone());
        if snapshot.expire(&key, field, *position) && held != Some(spec.initial()) {
            let id = RowId::new(row_id).map_err(|error| StoreError::Constraint(error.to_string()))?;
            changes.push(RowChange { row: RowRef::new(ModelKey::new(model), id), kind: RowChangeKind::Updated, fields: BTreeSet::from([field.clone()]) });
        }
    }
    Ok(changes)
}
