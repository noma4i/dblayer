use std::fs;
use std::path::{Path, PathBuf};

use rusqlite::{Connection, ErrorCode};

use crate::ddl::{create_core_tables, migrate_core, prepare_connection, recreate_reason, sql_ident, DdlError, RecreateReason};
use crate::files::FileDirectories;
use crate::transport::encrypted::state;

use super::{Store, StoreError};

fn is_unreadable(error: &StoreError) -> bool {
    let (StoreError::Sqlite(error) | StoreError::Ddl(DdlError::Sqlite(error))) = error else { return false };
    matches!(error, rusqlite::Error::SqliteFailure(failure, _) if matches!(failure.code, ErrorCode::NotADatabase | ErrorCode::DatabaseCorrupt))
}

fn probe(path: &Path) -> Result<Result<Store, RecreateReason>, StoreError> {
    let connection = Connection::open(path)?;
    let outcome = prepare_connection(&connection).map_err(StoreError::from).and_then(|()| Ok(recreate_reason(&connection)?));
    match outcome {
        Err(error) if is_unreadable(&error) => return Ok(Err(RecreateReason::UnreadableFile)),
        Err(error) => return Err(error),
        Ok(Some(reason)) => {
            recreate(&connection)?;
            FileDirectories::beside(path).remove_all()?;
            let mut store = Store::from_connection(connection);
            store.readers = Some(super::readers::Readers::open(path)?);
            store.recreated = Some(reason);
            return Ok(Ok(store));
        }
        Ok(None) => {}
    }
    migrate_core(&connection)?;
    let mut store = Store::from_connection(connection);
    store.readers = Some(super::readers::Readers::open(path)?);
    Ok(Ok(store))
}

fn recreate(connection: &Connection) -> Result<(), StoreError> {
    let transaction = connection.unchecked_transaction()?;
    let tables: Vec<String> = transaction
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'")?
        .query_map([], |row| row.get(0))?
        .collect::<Result<_, _>>()?;
    let session = if tables.iter().any(|table| table == "encrypted_session") { state::session(&transaction)? } else { None };
    let pending = if tables.iter().any(|table| table == "encrypted_pending") { state::pending(&transaction)? } else { None };
    for table in &tables {
        transaction.execute_batch(&format!("DROP TABLE IF EXISTS {}", sql_ident(table)))?;
    }
    create_core_tables(&transaction)?;
    if let Some(session) = session {
        state::restore_session(&transaction, &session)?;
    }
    if let Some(pending) = pending {
        state::store_pending(&transaction, &pending)?;
        if let Some(owner) = pending.owner {
            transaction.execute("INSERT INTO sqlite_sequence (name, seq) VALUES ('op', ?1)", [owner.seq])?;
        }
    }
    transaction.commit()?;
    Ok(())
}

fn remove_database(path: &Path) -> Result<(), StoreError> {
    for suffix in ["", "-wal", "-shm"] {
        let mut name = path.as_os_str().to_os_string();
        name.push(suffix);
        match fs::remove_file(PathBuf::from(name)) {
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => return Err(error.into()),
            _ => {}
        }
    }
    Ok(FileDirectories::beside(path).remove_all()?)
}

impl Store {
    pub fn open(path: &Path) -> Result<Store, StoreError> {
        if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
            fs::create_dir_all(parent)?;
        }
        let reason = match probe(path)? {
            Ok(store) => return Ok(store),
            Err(reason) => reason,
        };
        remove_database(path)?;
        let Ok(mut store) = probe(path)? else { return Err(std::io::Error::other(format!("a recreated database at {} is not usable", path.display())).into()) };
        store.recreated = Some(reason);
        Ok(store)
    }

    pub fn open_in_memory() -> Result<Store, StoreError> {
        let connection = Connection::open_in_memory()?;
        prepare_connection(&connection)?;
        migrate_core(&connection)?;
        Ok(Store::from_connection(connection))
    }

    pub fn recreated(&self) -> Option<RecreateReason> {
        self.recreated
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
