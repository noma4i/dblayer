use std::collections::BTreeMap;

use crate::commit::{BatchPlan, EntityOp, FileOp};
use crate::files::{placement, sha256_hex, FileDescriptor, FileDirectories, FileRecord, Location};
use crate::identity::{ModelKey, RowId};
use crate::owners::store::StoreOp;
use crate::schema::FieldKind;

use super::{ModelInfo, Reader, StoreError};

pub(super) struct PreparedFile {
    pub index: usize,
    pub record: FileRecord,
}

pub(super) fn prepare_files(files: &FileDirectories, ops: &[StoreOp], now: i64) -> Result<Vec<PreparedFile>, StoreError> {
    let mut prepared: Vec<PreparedFile> = Vec::new();
    for (index, op) in ops.iter().enumerate() {
        let StoreOp::AttachFile { model, row_id, field, bytes, name, mime } = op else { continue };
        let descriptor = FileDescriptor { id: uuid::Uuid::new_v4().to_string(), size: bytes.len() as u64, name: Some(name.clone()), mime: Some(mime.clone()), sha256: Some(sha256_hex(bytes)) };
        let location = placement(bytes.len(), files.has_directory());
        if location == Location::Directory {
            if let Err(error) = files.prepare(&descriptor.id, bytes) {
                let cleanup = prepared.iter().filter(|file| file.record.location == Location::Directory).filter_map(|file| files.discard_partial(&file.record.id).err()).collect::<Vec<_>>();
                if !cleanup.is_empty() { return Err(StoreError::Io(std::io::Error::other(format!("{error}; file cleanup: {cleanup:?}")))); }
                return Err(error.into());
            }
        }
        prepared.push(PreparedFile { index, record: FileRecord { id: descriptor.id.clone(), model: model.clone(), row_id: row_id.clone(), field: field.clone(), descriptor, location, data: if location == Location::Table { bytes.clone() } else { Vec::new() }, created_at: now } });
    }
    Ok(prepared)
}

pub(super) fn attach(reader: &Reader<'_>, models: &BTreeMap<String, ModelInfo>, envelope: &mut BatchPlan, record: &FileRecord) -> Result<(), StoreError> {
    let info = models.get(&record.model).ok_or_else(|| StoreError::UnknownModel(record.model.clone()))?;
    if !info.spec.field(&record.field).is_some_and(|field| field.kind == FieldKind::File) {
        return Err(StoreError::Constraint(format!("{} is not a file field", record.field)));
    }
    let id = RowId::new(&record.row_id).map_err(|error| StoreError::Constraint(error.to_string()))?;
    let model = ModelKey::new(&record.model);
    let row = envelope.entity_ops.iter_mut().find_map(|op| match op {
        EntityOp::Upsert { model: current, id: row_id, row } if current == &model && row_id == &id => Some(row),
        _ => None,
    });
    if let Some(row) = row {
        row.insert(record.field.clone(), record.descriptor.to_value());
    } else {
        let mut row = reader.point(&record.model, &record.row_id)?.ok_or_else(|| StoreError::Constraint(format!("row {} does not exist", record.row_id)))?;
        row.insert(record.field.clone(), record.descriptor.to_value());
        envelope.entity_ops.push(EntityOp::Upsert { model, id, row });
    }
    envelope.file_ops.push(FileOp::Put(record.clone()));
    Ok(())
}

pub(super) fn detach(reader: &Reader<'_>, models: &BTreeMap<String, ModelInfo>, envelope: &mut BatchPlan, model: &str, row_id: &str, field: &str) -> Result<(), StoreError> {
    let info = models.get(model).ok_or_else(|| StoreError::UnknownModel(model.to_string()))?;
    let spec = info.spec.field(field).filter(|spec| spec.kind == FieldKind::File).ok_or_else(|| StoreError::Constraint(format!("{field} is not a file field")))?;
    let id = RowId::new(row_id).map_err(|error| StoreError::Constraint(error.to_string()))?;
    let key = ModelKey::new(model);
    let existing = envelope.entity_ops.iter_mut().find_map(|op| match op {
        EntityOp::Upsert { model: current, id: current_id, row } if current == &key && current_id == &id => Some(row),
        _ => None,
    });
    let mut fetched = if existing.is_none() { reader.point(model, row_id)? } else { None };
    let row = existing.or(fetched.as_mut()).ok_or_else(|| StoreError::Constraint(format!("row {row_id} does not exist")))?;
    if spec.nullable { row.insert(field.to_string(), serde_json::Value::Null); } else { row.remove(field); }
    if let Some(row) = fetched { envelope.entity_ops.push(EntityOp::Upsert { model: key.clone(), id: id.clone(), row }); }
    envelope.file_ops.push(FileOp::Remove { model: key, row_id: id, field: field.to_string() });
    Ok(())
}
