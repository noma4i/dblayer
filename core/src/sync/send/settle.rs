use std::collections::HashSet;

use crate::commit::EntityOp;
use crate::db::Db;
use crate::identity::{ModelKey, RowId};
use crate::store::Reader;
use crate::value::read_id;

use super::super::error::SyncError;
use super::super::land::admit;
use super::super::normalize::Normalized;

pub(crate) fn response_ops(db: &Db, reader: &Reader<'_>, base: i64, closing: i64, model: &str, normalized: &Normalized, drops: &mut usize) -> Result<Vec<EntityOp>, SyncError> {
    let roots = admit(db, reader, base, Some(closing), model, &normalized.roots, drops)?.rows;
    let root_ids: HashSet<RowId> = roots.iter().filter_map(|row| row.get("id").and_then(read_id)).collect();
    let mut ops = Vec::new();
    for (related_model, row) in &normalized.related {
        for row in admit(db, reader, base, Some(closing), related_model.as_str(), std::slice::from_ref(row), drops)?.rows {
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
