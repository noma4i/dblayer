use std::fs;
use std::path::{Path, PathBuf};

use rusqlite::{Connection, ErrorCode};

use crate::store::ddl::{create_core_tables, prepare_connection, sql_ident, DdlError, CARRIED_TABLES};
use crate::store::files::FileDirectories;

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DroppedRow {
    pub table: String,
    pub key: String,
    pub reason: &'static str,
    pub columns: String,
}

pub(super) struct Recreated {
    pub files_kept: bool,
    pub dropped: Vec<DroppedRow>,
}

pub(super) fn recreate(connection: &Connection) -> Result<Recreated, StoreError> {
    let tables: Vec<String> = connection
        .prepare(include_str!("sql/open/recreate_select_sqlite_master.sql"))?
        .query_map([], |row| row.get(0))?
        .collect::<Result<_, _>>()?;
    let carried: Vec<&str> = CARRIED_TABLES.iter().copied().filter(|table| tables.iter().any(|found| found == table)).collect();
    let manifest = carried_manifest(connection, &tables)?;
    for table in &carried {
        let filter = if *table == "file" && columns_of(connection, table)?.iter().any(|(name, _)| name == "field") { " WHERE field LIKE 'op_item.%'" } else { "" };
        connection.execute_batch(&format!(include_str!("sql/open/recreate_create_table.sql"), sql_ident(&carry_name(table)), sql_ident(table), filter = filter))?;
    }
    for table in &tables {
        connection.execute_batch(&format!(include_str!("sql/open/recreate_drop_table.sql"), sql_ident(table)))?;
    }
    create_core_tables(connection)?;
    for (key, value) in &manifest {
        crate::store::ddl::manifest_write(connection, key, value)?;
    }
    let mut dropped = Vec::new();
    for table in &carried {
        dropped.extend(carry_rows(connection, table)?);
        connection.execute_batch(&format!(include_str!("sql/open/recreate_drop_table_2.sql"), sql_ident(&carry_name(table))))?;
    }
    if carried.contains(&"op_value") {
        connection.execute_batch(include_str!("sql/open/recreate_delete_op_value.sql"))?;
    }
    for row in &dropped {
        connection.execute(
            include_str!("sql/open/recreate_insert_restore_failed.sql"),
            rusqlite::params![row.table, row.key, row.reason, row.columns],
        )?;
    }
    Ok(Recreated { files_kept: carried.contains(&"file"), dropped })
}

fn carried_manifest(connection: &Connection, tables: &[String]) -> Result<Vec<(String, String)>, StoreError> {
    if !tables.iter().any(|table| table == "manifest") {
        return Ok(Vec::new());
    }
    let mut found = Vec::new();
    for key in crate::store::ddl::CARRIED_MANIFEST_KEYS {
        if let Some(value) = crate::store::ddl::manifest_read(connection, key)? {
            found.push((key.to_string(), value));
        }
    }
    Ok(found)
}

fn carry_name(table: &str) -> String { format!("carry_{table}") }

fn columns_of(connection: &Connection, table: &str) -> Result<Vec<(String, bool)>, StoreError> {
    Ok(connection
        .prepare(include_str!("sql/open/columns_of.sql"))?
        .query_map([table], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<_, _>>()?)
}

fn value_json(value: &rusqlite::types::Value) -> serde_json::Value {
    use rusqlite::types::Value;
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Integer(number) => serde_json::Value::from(*number),
        Value::Real(number) => serde_json::Number::from_f64(*number).map_or(serde_json::Value::Null, serde_json::Value::Number),
        Value::Text(text) => serde_json::Value::String(text.clone()),
        Value::Blob(bytes) => serde_json::json!({ "blob": bytes.iter().map(|byte| format!("{byte:02x}")).collect::<String>() }),
    }
}

fn refusal(error: &rusqlite::Error) -> Option<&'static str> {
    const CONSTRAINT_DATATYPE: i32 = 3091;
    match error {
        rusqlite::Error::SqliteFailure(failure, _) if failure.extended_code == CONSTRAINT_DATATYPE => Some("type"),
        rusqlite::Error::SqliteFailure(failure, _) if failure.code == rusqlite::ErrorCode::ConstraintViolation => Some("constraint"),
        _ => None,
    }
}

fn carry_rows(connection: &Connection, table: &str) -> Result<Vec<DroppedRow>, StoreError> {
    use sha2::{Digest, Sha256};
    let source = carry_name(table);
    let old: Vec<String> = columns_of(connection, &source)?.into_iter().map(|(name, _)| name).collect();
    let new = columns_of(connection, table)?;
    let required_missing = new.iter().any(|(name, required)| *required && !old.contains(name));
    let shared: Vec<String> = new.into_iter().map(|(name, _)| name).filter(|name| old.contains(name)).collect();
    let select = old.iter().map(|name| sql_ident(name)).collect::<Vec<_>>().join(", ");
    let insert = format!(include_str!("sql/open/carry_rows_insert.sql"), sql_ident(table), shared.iter().map(|name| sql_ident(name)).collect::<Vec<_>>().join(", "), (1..=shared.len()).map(|at| format!("?{at}")).collect::<Vec<_>>().join(", "));
    let positions: Vec<usize> = shared.iter().filter_map(|name| old.iter().position(|found| found == name)).collect();
    let rows: Vec<Vec<rusqlite::types::Value>> = connection
        .prepare(&format!(include_str!("sql/open/carry_rows_select.sql"), sql_ident(&source), select = select))?
        .query_map([], |row| (0..old.len()).map(|at| row.get(at)).collect())?
        .collect::<Result<_, _>>()?;
    let mut dropped = Vec::new();
    for row in rows {
        let refuse = |reason: &'static str| {
            let columns = serde_json::Value::Object(old.iter().cloned().zip(row.iter().map(value_json)).collect()).to_string();
            let key: String = Sha256::digest(format!("{table}\0{columns}").as_bytes()).iter().take(16).map(|byte| format!("{byte:02x}")).collect();
            DroppedRow { table: table.to_string(), key, reason, columns }
        };
        if required_missing || shared.is_empty() {
            dropped.push(refuse("missing_column"));
            continue;
        }
        connection.execute_batch(include_str!("sql/open/carry_rows_savepoint_carry_row.sql"))?;
        let params = rusqlite::params_from_iter(positions.iter().map(|at| row[*at].clone()));
        match connection.execute(&insert, params) {
            Ok(_) => connection.execute_batch(include_str!("sql/open/carry_rows_release_carry_row.sql"))?,
            Err(error) => {
                let Some(reason) = refusal(&error) else { return Err(error.into()) };
                connection.execute_batch(include_str!("sql/open/carry_rows_rollback_to.sql"))?;
                dropped.push(refuse(reason));
            }
        }
    }
    Ok(dropped)
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
        Ok(inner.connection.query_row(include_str!("sql/open/journal_mode.sql"), [], |row| row.get(0))?)
    }

    #[cfg(test)]
    pub fn table_exists(&self, name: &str) -> Result<bool, StoreError> {
        let inner = self.lock();
        let found: Option<i64> = rusqlite::OptionalExtension::optional(inner.connection.query_row(
            include_str!("sql/open/table_exists.sql"),
            [name],
            |row| row.get(0),
        ))?;
        Ok(found.is_some())
    }
}
