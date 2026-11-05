use std::fs;
use std::path::{Path, PathBuf};

use rusqlite::{Connection, ErrorCode};

use crate::ddl::{create_core_tables, prepare_connection, sql_ident, DdlError, CARRIED_TABLES};
use crate::files::FileDirectories;

use super::{Store, StoreError};

pub(crate) fn is_unreadable(error: &StoreError) -> bool {
    let (StoreError::Sqlite(error) | StoreError::Ddl(DdlError::Sqlite(error))) = error else { return false };
    matches!(error, rusqlite::Error::SqliteFailure(failure, _) if matches!(failure.code, ErrorCode::NotADatabase | ErrorCode::DatabaseCorrupt))
}

fn probe(path: &Path) -> Result<Store, StoreError> {
    let connection = Connection::open(path)?;
    prepare_connection(&connection)?;
    let mut store = Store::from_connection(connection);
    store.readers = Some(super::readers::Readers::open(path)?);
    Ok(store)
}

pub(super) fn recreate(connection: &Connection) -> Result<bool, StoreError> {
    let tables: Vec<String> = connection
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'")?
        .query_map([], |row| row.get(0))?
        .collect::<Result<_, _>>()?;
    let carried: Vec<&str> = CARRIED_TABLES.iter().copied().filter(|table| tables.iter().any(|found| found == table)).collect();
    for table in &carried {
        let filter = if *table == "file" && columns_of(connection, table)?.iter().any(|(name, _)| name == "field") { " WHERE field LIKE 'op_item.%'" } else { "" };
        connection.execute_batch(&format!("CREATE TEMP TABLE {} AS SELECT * FROM {}{filter}", sql_ident(&carry_name(table)), sql_ident(table)))?;
    }
    for table in &tables {
        connection.execute_batch(&format!("DROP TABLE IF EXISTS {}", sql_ident(table)))?;
    }
    create_core_tables(connection)?;
    for table in &carried {
        carry_rows(connection, table)?;
        connection.execute_batch(&format!("DROP TABLE {}", sql_ident(&carry_name(table))))?;
    }
    if carried.contains(&"op_value") {
        connection.execute_batch("DELETE FROM op_value WHERE part = 'prior';")?;
    }
    Ok(carried.contains(&"file"))
}

fn carry_name(table: &str) -> String { format!("carry_{table}") }

fn columns_of(connection: &Connection, table: &str) -> Result<Vec<(String, bool)>, StoreError> {
    Ok(connection
        .prepare("SELECT name, \"notnull\" = 1 AND dflt_value IS NULL AND pk = 0 FROM pragma_table_info(?1) ORDER BY cid")?
        .query_map([table], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<_, _>>()?)
}

/// Moves the rows by the columns both layouts have. A new column takes its DEFAULT. A table whose new layout has a required column without DEFAULT that the old layout lacks moves no rows.
fn carry_rows(connection: &Connection, table: &str) -> Result<(), StoreError> {
    let source = carry_name(table);
    let old: Vec<String> = columns_of(connection, &source)?.into_iter().map(|(name, _)| name).collect();
    let new = columns_of(connection, table)?;
    if new.iter().any(|(name, required)| *required && !old.contains(name)) { return Ok(()); }
    let shared: Vec<String> = new.into_iter().map(|(name, _)| name).filter(|name| old.contains(name)).map(|name| sql_ident(&name)).collect();
    if shared.is_empty() { return Ok(()); }
    let list = shared.join(", ");
    connection.execute_batch(&format!("INSERT OR IGNORE INTO {} ({list}) SELECT {list} FROM {}", sql_ident(table), sql_ident(&source)))?;
    Ok(())
}

pub(crate) fn remove_database(path: &Path) -> Result<(), StoreError> {
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
        probe(path)
    }

    pub fn open_in_memory() -> Result<Store, StoreError> {
        let connection = Connection::open_in_memory()?;
        prepare_connection(&connection)?;
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
