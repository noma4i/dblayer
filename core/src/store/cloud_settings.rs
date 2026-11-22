use rusqlite::Connection;

use super::{ddl, Store, StoreError};

const CLOUD_ENABLED: &str = "cloud_enabled";

fn read(connection: &Connection) -> Result<Option<bool>, StoreError> {
    match ddl::manifest_read(connection, CLOUD_ENABLED)?.as_deref() {
        None => Ok(None),
        Some("0") => Ok(Some(false)),
        Some("1") => Ok(Some(true)),
        Some(_) => Err(StoreError::CorruptManifest(CLOUD_ENABLED.into())),
    }
}

impl Store {
    pub(crate) fn stored_cloud_enabled(&self) -> Result<Option<bool>, StoreError> {
        read(&self.lock().connection)
    }

    pub(crate) fn write_cloud_enabled(&self, enabled: bool) -> Result<(), StoreError> {
        let inner = self.lock();
        if read(&inner.connection)? == Some(enabled) { return Ok(()); }
        ddl::manifest_write(&inner.connection, CLOUD_ENABLED, if enabled { "1" } else { "0" })?;
        Ok(())
    }
}
