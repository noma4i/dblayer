//! The files of file fields as the platform reads them.

use std::path::PathBuf;

use crate::store::files::{find, FileDescriptor, FileRecord, Location};
use crate::identity::RowId;
use crate::schema::FieldKind;

use super::db_error::CoreError;
use super::{Store, StoreError};

pub(crate) fn io_fault(error: std::io::Error) -> CoreError {
    CoreError::Store(StoreError::from(error))
}

impl Store {
    pub(crate) fn file_field(&self, model: &str, field: &str) -> Result<bool, CoreError> {
        let found = self.spec(model)?.field(field).filter(|spec| spec.kind == FieldKind::File);
        found.map(|spec| spec.nullable).ok_or_else(|| CoreError::NotFileField { model: model.to_string(), field: field.to_string() })
    }

    fn stored_file(&self, model: &str, id: &RowId, field: &str) -> Result<Option<FileRecord>, CoreError> {
        self.file_field(model, field)?;
        self.read_models(&[model], |reader| {
            if reader.point(model, id.as_str())?.is_none() {
                return Ok(None);
            }
            Ok(find(reader.connection(), model, id.as_str(), field)?)
        })
    }

    pub fn file(&self, model: &str, id: &RowId, field: &str) -> Result<Option<FileDescriptor>, CoreError> {
        self.file_field(model, field)?;
        Ok(self.find(model, id)?.and_then(|row| row.get(field).and_then(FileDescriptor::from_value)))
    }

    pub fn read_file(&self, model: &str, id: &RowId, field: &str) -> Result<Option<Vec<u8>>, CoreError> {
        let Some(record) = self.stored_file(model, id, field)? else { return Ok(None) };
        match record.location {
            Location::Table => Ok(Some(record.data)),
            Location::Directory => self.files().read(&record.id).map(Some).map_err(io_fault),
        }
    }

    pub fn url(&self, model: &str, id: &RowId, field: &str) -> Result<Option<PathBuf>, CoreError> {
        let Some(record) = self.stored_file(model, id, field)? else { return Ok(None) };
        match record.location {
            Location::Table => {
                let name = record.descriptor.name.as_deref().unwrap_or_default();
                self.files().materialize(&record.id, name, &record.data).map(Some).map_err(io_fault)
            }
            Location::Directory => Ok(self.files().path(&record.id)),
        }
    }

    /// The row from the pool of the readers, or `None` when it does not exist.
    pub fn peek_row(&self, model: &str, id: &RowId) -> Option<super::Row> {
        self.find(model, id).ok().flatten()
    }
}
