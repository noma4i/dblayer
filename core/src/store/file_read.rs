//! The files of file fields as the platform reads them.

use std::path::PathBuf;

use crate::files::{find, FileDescriptor, FileRecord, Location};
use crate::identity::RowId;
use crate::schema::FieldKind;

use super::db_error::DbError;
use super::{Store, StoreError};

pub(crate) fn io_fault(error: std::io::Error) -> DbError {
    DbError::Store(StoreError::from(error))
}

impl Store {
    pub(crate) fn file_field(&self, model: &str, field: &str) -> Result<bool, DbError> {
        let found = self.spec(model)?.field(field).filter(|spec| spec.kind == FieldKind::File);
        found.map(|spec| spec.nullable).ok_or_else(|| DbError::NotFileField { model: model.to_string(), field: field.to_string() })
    }

    fn stored_file(&self, model: &str, id: &RowId, field: &str) -> Result<Option<FileRecord>, DbError> {
        self.file_field(model, field)?;
        self.read_models(&[model], |reader| {
            if reader.point(model, id.as_str())?.is_none() {
                return Ok(None);
            }
            Ok(find(reader.connection(), model, id.as_str(), field)?)
        })
    }

    pub fn file(&self, model: &str, id: &RowId, field: &str) -> Result<Option<FileDescriptor>, DbError> {
        self.file_field(model, field)?;
        Ok(self.find(model, id)?.and_then(|row| row.get(field).and_then(FileDescriptor::from_value)))
    }

    pub fn read_file(&self, model: &str, id: &RowId, field: &str) -> Result<Option<Vec<u8>>, DbError> {
        let Some(record) = self.stored_file(model, id, field)? else { return Ok(None) };
        match record.location {
            Location::Table => Ok(Some(record.data)),
            Location::Directory => self.files().read(&record.id).map(Some).map_err(io_fault),
        }
    }

    pub fn url(&self, model: &str, id: &RowId, field: &str) -> Result<Option<PathBuf>, DbError> {
        let Some(record) = self.stored_file(model, id, field)? else { return Ok(None) };
        match record.location {
            Location::Table => {
                let name = record.descriptor.name.as_deref().unwrap_or_default();
                self.files().materialize(&record.id, name, &record.data).map(Some).map_err(io_fault)
            }
            Location::Directory => Ok(self.files().path(&record.id)),
        }
    }

    /// Drops the blobs whose lifetime ended before the first use of the database.
    pub(crate) fn expire_blobs_before_use(&self) -> Result<(), DbError> {
        let failed_ttl = i64::try_from(crate::upload::UploadConfig::default().failed_ttl.as_millis()).unwrap_or(i64::MAX);
        let now = self.lock().now_ms();
        let failed_due = now.saturating_sub(failed_ttl);
        let ready_due = now.saturating_sub(crate::upload::BLOB_LIFETIME_MS);
        let due = self.read_models(&[], |reader| Ok(super::blob::due(reader.connection(), ready_due, failed_due)?))?;
        if due.is_empty() { return Ok(()); }
        let mut envelope = crate::commit::CommitEnvelope::new(crate::commit::WriteOrigin::Response, self.generation());
        envelope.operation_transitions.extend(due.iter().map(|(id, _)| crate::commit::OperationTransition::RetireBlob { id: id.clone(), expired: true }));
        self.commit(envelope)?;
        let files: Vec<String> = self.read_models(&[], |reader| {
            Ok(due.iter().filter_map(|(_, file)| file.as_ref()).filter_map(|id| match reader.connection().query_row("SELECT EXISTS (SELECT 1 FROM file WHERE id = ?1)", [id], |row| row.get::<_, bool>(0)) { Ok(false) => Some(Ok(id.clone())), Ok(true) => None, Err(error) => Some(Err(StoreError::from(error))) }).collect::<Result<Vec<_>, _>>()?)
        })?;
        self.files().discard(&files).map_err(io_fault)?;
        Ok(())
    }

    /// The row from the pool of the readers, or `None` when it does not exist.
    pub fn peek_row(&self, model: &str, id: &RowId) -> Option<super::Row> {
        self.find(model, id).ok().flatten()
    }
}
