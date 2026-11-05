use super::{Db, DbError};

impl Db {
    pub(crate) fn expire_blobs_before_use(&self, admitted: &crate::ffi::admission::Admitted) -> Result<(), DbError> {
        let failed_due = self.shared.store.lock().now_ms().saturating_sub(self.shared.blob_failed_ttl_ms.load(std::sync::atomic::Ordering::Relaxed));
        self.expire_blob_cache(admitted, failed_due)
    }

    pub(crate) fn expire_blob_cache(&self, admitted: &crate::ffi::admission::Admitted, failed_due: i64) -> Result<(), DbError> {
        let ready_due = self.shared.store.lock().now_ms().saturating_sub(crate::upload::BLOB_LIFETIME_MS);
        let due = self.shared.read(&[], |reader| Ok(crate::store::blob::due(reader.connection(), ready_due, failed_due)?))?;
        if due.is_empty() { return Ok(()); }
        let mut envelope = crate::commit::CommitEnvelope::new(crate::commit::WriteOrigin::Response, self.generation());
        envelope.operation_transitions.extend(due.iter().map(|(id, _)| crate::commit::OperationTransition::RetireBlob { id: id.clone(), expired: true }));
        self.land(admitted, envelope)?;
        let files: Vec<String> = self.shared.read(&[], |reader| {
            Ok(due.iter().filter_map(|(_, file)| file.as_ref()).filter_map(|id| match reader.connection().query_row("SELECT EXISTS (SELECT 1 FROM file WHERE id = ?1)", [id], |row| row.get::<_, bool>(0)) { Ok(false) => Some(Ok(id.clone())), Ok(true) => None, Err(error) => Some(Err(crate::store::StoreError::from(error))) }).collect::<Result<Vec<_>, _>>()?)
        })?;
        self.shared.files.discard(&files).map_err(super::files::io_fault)?;
        Ok(())
    }

    // Preparation closures only copy files and reserve deferred commits; they must not publish or call foreign code.

    pub fn blob_snapshot(&self, id: &str) -> Result<crate::upload::BlobSnapshot, DbError> {
        let store = self.shared.store.lock();
        store.ensure_healthy()?;
        crate::store::blob::snapshot(&store.connection, id)?.ok_or_else(|| DbError::InvalidUpload("unknown Blob in this database".into()))
    }

}
