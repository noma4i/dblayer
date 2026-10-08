use std::collections::HashSet;

use serde_json::Value;

use crate::commit::{EntityOp, OperationRecord, OperationTransition};
use crate::db::OutboxOperation;
use crate::identity::{ModelKey, RowId};
use crate::store::{Reader, StoreError};
use crate::value::read_id;

use super::super::land::admit;
use super::super::normalize::Normalized;

pub(crate) fn response_ops(reader: &Reader<'_>, base: i64, model: &str, normalized: &Normalized, drops: &mut usize) -> Result<Vec<EntityOp>, StoreError> {
    let roots = admit(reader, base, model, &normalized.roots, drops)?;
    let root_ids: HashSet<RowId> = roots.iter().filter_map(|row| row.get("id").and_then(read_id)).collect();
    let mut ops = Vec::new();
    for (related_model, row) in &normalized.related {
        for row in admit(reader, base, related_model.as_str(), std::slice::from_ref(row), drops)? {
            let Some(id) = row.get("id").and_then(read_id) else { continue };
            if related_model.as_str() == model && root_ids.contains(&id) {
                continue;
            }
            ops.push(EntityOp::Upsert { model: related_model.clone(), id, row });
        }
    }
    for row in roots {
        if let Some(id) = row.get("id").and_then(read_id) {
            ops.push(EntityOp::Upsert { model: ModelKey::new(model), id, row });
        }
    }
    Ok(ops)
}

pub(crate) fn retarget(entry: &OutboxOperation, temp: &RowId, real: &RowId) -> OperationTransition {
    let mut input = entry.input.clone();
    if let Some(object) = input.as_object_mut().filter(|object| object.get("id").and_then(Value::as_str) == Some(temp.as_str())) {
        object.insert("id".to_string(), Value::String(real.to_string()));
    }
    let operation = &entry.operation;
    OperationTransition::Begin(OperationRecord {
        seq: Some(operation.id.0),
        action_key: operation.action_key.clone(),
        status: operation.status,
        row_ids: operation.row_ids.iter().map(|id| if id == temp { real.clone() } else { id.clone() }).collect(),
        temp_id: operation.temp_id.clone(),
        input,
        transport_started: operation.transport_started,
        base_revision: Some(operation.base_revision),
        created_at: operation.created_at,
        dedupe_key: operation.dedupe_key.clone(),
        rollback: entry.rollback.clone(),
        upload: None,
    })
}
