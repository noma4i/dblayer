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
    pub(crate) fn prepare(&mut self, model: &str, row: RowId, client_key: String, mut input: UploadInput, mut state: UploadState) -> Result<UploadAdmission, DbError> {
        let id = uuid::Uuid::new_v4().to_string();
        let name = input.name.clone().unwrap_or_else(|| Path::new(&input.path).file_name().unwrap_or_default().to_string_lossy().into_owned());
        let mime = input.mime.clone().unwrap_or_else(|| crate::upload::mime_of(&name).into());
        let source = match self.files.copy_source(&id, Path::new(&input.path)).map_err(super::files::io_fault)? {
            CopiedSource::Unreadable(error) => Err(error),
            CopiedSource::Ready { size, sha256, data, location } => {
                state.total_bytes = size;
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
    pub(crate) fn expire_blobs_before_use(&self) -> Result<(), DbError> {
        let failed_due = self.shared.store.lock().now_ms().saturating_sub(self.shared.blob_failed_ttl_ms.load(std::sync::atomic::Ordering::Relaxed));
        self.expire_blob_cache(failed_due)
    }

    pub(crate) fn expire_blob_cache(&self, failed_due: i64) -> Result<(), DbError> {
        let ready_due = self.shared.store.lock().now_ms().saturating_sub(crate::upload::BLOB_LIFETIME_MS);
        let due = self.shared.read(&[], |reader| Ok(crate::store::blob::due(reader.connection(), ready_due, failed_due)?))?;
        if due.is_empty() { return Ok(()); }
        let mut envelope = crate::commit::CommitEnvelope::new(crate::commit::WriteOrigin::Response, self.generation());
        envelope.operation_transitions.extend(due.iter().map(|(id, _)| crate::commit::OperationTransition::RetireBlob { id: id.clone(), expired: true }));
        self.land(envelope)?;
        let files: Vec<String> = self.shared.read(&[], |reader| {
            Ok(due.iter().filter_map(|(_, file)| file.as_ref()).filter_map(|id| match reader.connection().query_row("SELECT EXISTS (SELECT 1 FROM file WHERE id = ?1)", [id], |row| row.get::<_, bool>(0)) { Ok(false) => Some(Ok(id.clone())), Ok(true) => None, Err(error) => Some(Err(crate::store::StoreError::from(error))) }).collect::<Result<Vec<_>, _>>()?)
        })?;
        self.shared.files.discard(&files).map_err(super::files::io_fault)?;
        Ok(())
    }

    // Preparation closures only copy files and reserve deferred commits; they must not publish or call foreign code.
    pub(crate) fn prepare_blob<'a, T, E: From<DbError> + std::fmt::Display>(&'a self, generation: Generation, run: impl FnOnce(&mut UploadFiles<'_>, &mut Option<crate::store::Committed<'a>>) -> Result<T, E>) -> Result<T, E> {
        let _gate = locked(&self.shared.files_gate);
        self.shared.store.lock().ensure_generation(&self.authority()?, generation).map_err(DbError::from)?;
        let mut files = UploadFiles { files: &self.shared.files, owned: Vec::new(), now: self.shared.store.lock().now_ms() };
        let mut committed = None;
        let result = run(&mut files, &mut committed);
        if result.is_ok() { files.owned.clear(); }
        drop(files);
        drop(_gate);
        if let Some(committed) = committed { committed.publish(); }
        result
    }

    pub fn blob_snapshot(&self, id: &str) -> Result<crate::upload::BlobSnapshot, DbError> {
        let store = self.shared.store.lock();
        store.ensure_healthy()?;
        crate::store::blob::snapshot(&store.connection, id)?.ok_or_else(|| DbError::InvalidUpload("unknown Blob in this database".into()))
    }

    pub(crate) fn preparing_uploads<'a, T, E: From<DbError> + std::fmt::Display>(&'a self, model: &str, generation: Generation, run: impl FnOnce(&mut UploadFiles<'_>, Generation, &mut Option<crate::store::Committed<'a>>) -> Result<T, E>) -> Result<T, E> {
        let spec = self.shared.spec(model)?;
        if spec.is_local() || spec.is_memory() {
            return Err(DbError::InvalidUpload("upload requires a server model".into()).into());
        }
        let _gate = locked(&self.shared.files_gate);
        self.shared.store.lock().ensure_generation(&self.authority()?, generation).map_err(DbError::from)?;
        let mut files = UploadFiles { files: &self.shared.files, owned: Vec::new(), now: self.shared.store.lock().now_ms() };
        let mut committed = None;
        let result = run(&mut files, generation, &mut committed);
        let owned = std::mem::take(&mut files.owned);
        let result = if let Err(error) = &result {
            if let Err(cleanup) = files.files.discard(&owned) {
                Err(super::files::io_fault(std::io::Error::other(format!("{error}; file cleanup: {cleanup}"))).into())
            } else {
                result
            }
        } else { result };
        drop(files);
        drop(_gate);
        if let Some(committed) = committed { committed.publish(); }
        result
    }
}
