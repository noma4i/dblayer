use std::path::PathBuf;

use serde_json::Value;

use crate::commit::{CommitEnvelope, EntityOp, FileOp, WriteOrigin};
use crate::files::{find, placement, sha256_hex, FileDescriptor, FileRecord, Location};
use crate::identity::{ModelKey, RowId};
use crate::schema::FieldKind;
use crate::store::StoreError;

use super::error::DbError;
use super::local_write::plan_fault;
use super::open::{locked, Db};

pub(super) fn io_fault(error: std::io::Error) -> DbError {
    DbError::Store(StoreError::from(error))
}

impl Db {
    fn file_field(&self, model: &str, field: &str) -> Result<bool, DbError> {
        let found = self.shared.spec(model)?.field(field).filter(|spec| spec.kind == FieldKind::File);
        found.map(|spec| spec.nullable).ok_or_else(|| DbError::NotFileField { model: model.to_string(), field: field.to_string() })
    }

    fn stored_file(&self, model: &str, id: &RowId, field: &str) -> Result<Option<FileRecord>, DbError> {
        self.file_field(model, field)?;
        self.shared.read(|reader| Ok(find(reader.connection(), model, id.as_str(), field)?))
    }

    pub fn attach(&self, model: &str, id: &RowId, field: &str, bytes: &[u8], name: &str, mime: &str) -> Result<FileDescriptor, DbError> {
        self.file_field(model, field)?;
        let _gate = locked(&self.shared.files_gate);
        let descriptor = FileDescriptor {
            id: uuid::Uuid::new_v4().to_string(),
            size: bytes.len() as u64,
            name: Some(name.to_string()),
            mime: Some(mime.to_string()),
            sha256: Some(sha256_hex(bytes)),
        };
        let location = placement(bytes.len(), self.shared.files.has_directory());
        if location == Location::Directory {
            self.shared.files.write(&descriptor.id, bytes).map_err(io_fault)?;
        }
        let record = FileRecord {
            id: descriptor.id.clone(),
            model: model.to_string(),
            row_id: id.to_string(),
            field: field.to_string(),
            descriptor: descriptor.clone(),
            location,
            data: if location == Location::Table { bytes.to_vec() } else { Vec::new() },
            created_at: self.shared.store.lock().now_ms(),
        };
        let generation = self.generation();
        let mut replaced: Option<String> = None;
        let committed = self.shared.store.commit_with(generation, |reader| {
            let mut row = reader
                .point(model, id.as_str())?
                .ok_or_else(|| DbError::NotFound { model: model.to_string(), id: id.to_string() })?;
            replaced = find(reader.connection(), model, id.as_str(), field)?.map(|old| old.id);
            row.insert(field.to_string(), descriptor.to_value());
            let mut envelope = CommitEnvelope::new(WriteOrigin::Optimistic, generation);
            envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new(model), id: id.clone(), row });
            envelope.file_ops.push(FileOp::Put(record));
            Ok(Some(envelope))
        });
        match committed {
            Ok(result) => {
                self.shared.files.discard(&replaced.into_iter().collect::<Vec<_>>());
                self.publish(&result);
                Ok(descriptor)
            }
            Err(error) => {
                self.shared.files.discard(&[descriptor.id]);
                Err(plan_fault(error))
            }
        }
    }

    pub fn file(&self, model: &str, id: &RowId, field: &str) -> Result<Option<FileDescriptor>, DbError> {
        self.file_field(model, field)?;
        Ok(self.find(model, id)?.and_then(|row| row.get(field).and_then(FileDescriptor::from_value)))
    }

    pub fn read(&self, model: &str, id: &RowId, field: &str) -> Result<Option<Vec<u8>>, DbError> {
        let Some(record) = self.stored_file(model, id, field)? else { return Ok(None) };
        match record.location {
            Location::Table => Ok(Some(record.data)),
            Location::Directory => self.shared.files.read(&record.id).map(Some).map_err(io_fault),
        }
    }

    pub fn url(&self, model: &str, id: &RowId, field: &str) -> Result<Option<PathBuf>, DbError> {
        let _gate = locked(&self.shared.files_gate);
        let Some(record) = self.stored_file(model, id, field)? else { return Ok(None) };
        match record.location {
            Location::Table => {
                let name = record.descriptor.name.as_deref().unwrap_or_default();
                self.shared.files.materialize(&record.id, name, &record.data).map(Some).map_err(io_fault)
            }
            Location::Directory => Ok(self.shared.files.path(&record.id)),
        }
    }

    pub fn detach(&self, model: &str, id: &RowId, field: &str) -> Result<bool, DbError> {
        let nullable = self.file_field(model, field)?;
        let _gate = locked(&self.shared.files_gate);
        let generation = self.generation();
        let mut removed: Option<String> = None;
        let result = self
            .shared
            .store
            .commit_with(generation, |reader| {
                let mut row = reader
                    .point(model, id.as_str())?
                    .ok_or_else(|| DbError::NotFound { model: model.to_string(), id: id.to_string() })?;
                let old = find(reader.connection(), model, id.as_str(), field)?;
                let described = row.get(field).is_some_and(|value| !value.is_null());
                if old.is_none() && !described {
                    return Ok(None);
                }
                if nullable {
                    row.insert(field.to_string(), Value::Null);
                } else {
                    row.remove(field);
                }
                let mut envelope = CommitEnvelope::new(WriteOrigin::Optimistic, generation);
                envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new(model), id: id.clone(), row });
                if let Some(old) = old {
                    envelope.file_ops.push(FileOp::Remove { model: ModelKey::new(model), row_id: id.clone(), field: field.to_string() });
                    removed = Some(old.id);
                }
                Ok(Some(envelope))
            })
            .map_err(plan_fault)?;
        self.shared.files.discard(&removed.into_iter().collect::<Vec<_>>());
        self.publish(&result);
        Ok(!result.is_noop())
    }
}
