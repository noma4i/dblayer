use std::path::Path;

use crate::commit::UploadAdmission;
use crate::files::{CopiedSource, FileDescriptor, FileDirectories, FileRecord, Location};
use crate::identity::{Generation, RowId};
use crate::upload::{UploadInput, UploadState};

use super::{locked, Db, DbError};

pub(crate) struct UploadFiles<'a> {
    files: &'a FileDirectories,
    owned: Vec<String>,
    now: i64,
}

impl UploadFiles<'_> {
    pub(crate) fn prepare(&mut self, model: &str, row: RowId, client_key: String, mut input: UploadInput, state: UploadState) -> Result<UploadAdmission, DbError> {
        let id = uuid::Uuid::new_v4().to_string();
        let name = input.name.clone().unwrap_or_else(|| Path::new(&input.path).file_name().unwrap_or_default().to_string_lossy().into_owned());
        let mime = input.mime.clone().unwrap_or_else(|| crate::upload::mime_of(&name).into());
        let source = match self.files.copy_source(&id, Path::new(&input.path)).map_err(super::files::io_fault)? {
            CopiedSource::Unreadable(error) => Err(error),
            CopiedSource::Ready { size, sha256, data, location } => {
                if location == Location::Directory { self.owned.push(id.clone()); }
                Ok(FileRecord {
                    descriptor: FileDescriptor { id: id.clone(), size, name: Some(name.clone()), mime: Some(mime.clone()), sha256: Some(sha256) },
                    id, model: model.to_string(), row_id: row.to_string(), field: String::new(), location, data, created_at: self.now,
                })
            }
        };
        input.path.clear();
        input.name = Some(name);
        input.mime = Some(mime);
        Ok(UploadAdmission { row, client_key, input, state, source })
    }
}

impl Drop for UploadFiles<'_> {
    fn drop(&mut self) {
        if !self.owned.is_empty() { let _ = self.files.discard(&self.owned); }
    }
}

impl Db {
    pub(crate) fn preparing_uploads<T, E: From<DbError> + std::fmt::Display>(&self, model: &str, generation: Generation, run: impl FnOnce(&mut UploadFiles<'_>, Generation) -> Result<T, E>) -> Result<T, E> {
        let spec = self.shared.spec(model)?;
        if spec.is_local() || spec.is_memory() {
            return Err(DbError::InvalidUpload("upload requires a server model".into()).into());
        }
        let _gate = locked(&self.shared.files_gate);
        self.shared.store.lock().ensure_generation(generation).map_err(DbError::from)?;
        let mut files = UploadFiles { files: &self.shared.files, owned: Vec::new(), now: self.shared.store.lock().now_ms() };
        let result = run(&mut files, generation);
        let owned = std::mem::take(&mut files.owned);
        if let Err(error) = &result {
            if let Err(cleanup) = files.files.discard(&owned) {
                return Err(super::files::io_fault(std::io::Error::other(format!("{error}; file cleanup: {cleanup}"))).into());
            }
        }
        result
    }
}
