use std::fs;
use std::path::Path;

use rusqlite::Connection;

use crate::ddl::{migrate_core, prepare_connection};

use super::{Store, StoreError};

impl Store {
    pub fn open(path: &Path) -> Result<Store, StoreError> {
        if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
            fs::create_dir_all(parent)?;
        }
        let connection = Connection::open(path)?;
        prepare_connection(&connection)?;
        migrate_core(&connection)?;
        Ok(Store::from_connection(connection))
    }

    pub fn open_in_memory() -> Result<Store, StoreError> {
        let connection = Connection::open_in_memory()?;
        prepare_connection(&connection)?;
        migrate_core(&connection)?;
        Ok(Store::from_connection(connection))
    }

    #[cfg(test)]
    pub fn journal_mode(&self) -> Result<String, StoreError> {
        let inner = self.lock();
        Ok(inner.connection.query_row("PRAGMA journal_mode", [], |row| row.get(0))?)
    }

    #[cfg(test)]
    pub fn table_exists(&self, name: &str) -> Result<bool, StoreError> {
        let inner = self.lock();
        let found: Option<i64> = rusqlite::OptionalExtension::optional(inner.connection.query_row(
            "SELECT 1 FROM (SELECT type, name FROM sqlite_master UNION ALL SELECT type, name FROM sqlite_temp_master) WHERE type = 'table' AND name = ?1",
            [name],
            |row| row.get(0),
        ))?;
        Ok(found.is_some())
    }
}
