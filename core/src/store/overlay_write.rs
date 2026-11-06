use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use crate::commit::{RowChange, RowChangeKind};
use crate::identity::{ModelKey, RowId, RowRef};

use super::overlay::{OverlaySnapshot, RowKey};
use super::{ModelInfo, StoreError};

pub(super) fn apply(snapshot: &mut OverlaySnapshot, models: &BTreeMap<String, ModelInfo>, writes: super::feed_write::OverlayWrites) -> Result<Vec<RowChange>, StoreError> {
    let mut before = BTreeMap::new();
    for (model, row_id, values) in writes {
        let info = models.get(&model).ok_or_else(|| StoreError::UnknownModel(model.clone()))?;
        let row = RowKey { model: model.clone(), row_id: row_id.clone() };
        for (name, value) in values {
            let field = info.spec.ephemeral_field(&name).ok_or_else(|| StoreError::Constraint(format!("{model}.{name} is not ephemeral")))?;
            let value = if value.is_null() && field.field.nullable { Value::Null } else { field.field.decode(&value).ok_or_else(|| StoreError::Constraint(format!("invalid value for {model}.{name}")))? };
            let key = (model.clone(), row_id.clone(), name.clone());
            before.entry(key).or_insert_with(|| snapshot.values.get(&row).and_then(|fields| fields.get(&name)).map_or_else(|| field.initial(), |slot| slot.value.clone()));
            snapshot.set(row.clone(), name, value);
        }
    }
    let mut changed: BTreeMap<RowRef, BTreeSet<String>> = BTreeMap::new();
    for ((model, id, field), before) in before {
        let key = RowKey { model: model.clone(), row_id: id.clone() };
        let after = &snapshot.values.get(&key).and_then(|fields| fields.get(&field)).expect("the field was written").value;
        if &before != after {
            let id = RowId::new(&id).map_err(|error| StoreError::Constraint(error.to_string()))?;
            changed.entry(RowRef::new(ModelKey::new(&model), id)).or_default().insert(field);
        }
    }
    Ok(changed.into_iter().map(|(row, fields)| RowChange { row, kind: RowChangeKind::Updated, fields }).collect())
}
