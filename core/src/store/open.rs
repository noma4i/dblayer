use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use rusqlite::{Connection, ErrorCode, OptionalExtension};

use crate::ddl::{create_core_tables, manifest_read, outbox_format, prepare_connection, sql_ident, DdlError, FORMAT_VERSION_KEY, OUTBOX_FORMAT_KEY, OUTBOX_TABLES};
use crate::files::FileDirectories;
use crate::transport::encrypted::state;

use super::{ModelInfo, Store, StoreError};

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

pub(super) fn legacy_format(connection: &Connection) -> Result<bool, StoreError> {
    const LEGACY_FORMAT: &str = "36962b20688fd50d6443ff71baf6b0a8d657a4bf48c1d979c56f315176c64699";
    let has_manifest: bool = connection.query_row("SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'manifest')", [], |row| row.get(0))?;
    let schema: i64 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    Ok(has_manifest && (schema == 0 || schema == crate::ddl::SCHEMA_VERSION) && manifest_read(connection, FORMAT_VERSION_KEY)?.as_deref() == Some(LEGACY_FORMAT))
}

pub(super) fn migrate_transfer_identity(connection: &Connection, models: &BTreeMap<String, ModelInfo>) -> Result<(), StoreError> {
    let (pending, lost) = legacy_pending(connection, true)?;
    let lost = match lost { Some((seq, position)) => lost_item(connection, models, seq, position)?, None => None };
    connection.execute_batch("ALTER TABLE op_item ADD COLUMN transfer_operation_id INTEGER NOT NULL DEFAULT 0;
        UPDATE op_item SET transfer_operation_id = seq;
        ALTER TABLE upload_orphan RENAME TO transfer_abort;
        ALTER TABLE transfer_abort ADD COLUMN operation_id INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE transfer_abort ADD COLUMN item_id TEXT NOT NULL DEFAULT '';
        UPDATE transfer_abort SET operation_id = -(SELECT MIN(other.id) FROM transfer_abort AS other WHERE other.storage_key = transfer_abort.storage_key AND other.upload_id = transfer_abort.upload_id AND other.abort_operation = transfer_abort.abort_operation), item_id = 'legacy-abort-' || (SELECT MIN(other.id) FROM transfer_abort AS other WHERE other.storage_key = transfer_abort.storage_key AND other.upload_id = transfer_abort.upload_id AND other.abort_operation = transfer_abort.abort_operation);
        DELETE FROM transfer_abort WHERE id NOT IN (SELECT MIN(id) FROM transfer_abort GROUP BY storage_key, upload_id, abort_operation);
        DROP TABLE encrypted_pending;
        CREATE TABLE encrypted_pending (id INTEGER PRIMARY KEY CHECK (id = 1), credential_digest TEXT NOT NULL, request_id TEXT NOT NULL, response_key TEXT NOT NULL, sequence INTEGER NOT NULL, path TEXT NOT NULL, body TEXT NOT NULL, owner_kind TEXT, owner_operation_id INTEGER, owner_item_id TEXT, owner_step TEXT, purpose INTEGER NOT NULL CHECK (purpose IN (0, 1))) STRICT;")?;
    if let Some(pending) = pending {
        if let Some(crate::transport::RequestOwner::Unmatched { operation_id }) = &pending.owner {
            connection.execute("UPDATE op SET status = 'failed', transport_started = 0, error_kind = 'missing_file', error_message = 'Select the file again' WHERE seq = ?1", [operation_id])?;
            if let Some(item) = &lost {
                let state = crate::upload::UploadState { phase: crate::upload::UploadStatus::Failed, error: Some(crate::syncstate::OperationFailure::new(crate::syncstate::FailureKind::MissingFile, "Select the file again")), ..crate::upload::UploadState::queued() };
                crate::operation::write_upload(connection, *operation_id, item.position, &state)?;
                connection.execute("UPDATE op_item SET row_id = ?3, client_key = ?4, file_id = NULL, source_error = 'missing_file' WHERE seq = ?1 AND position = ?2", rusqlite::params![operation_id, item.position, item.row, item.key])?;
            }
        }
        state::store_pending(connection, &pending)?;
    }
    crate::ddl::rebuild_transfer_tables(connection)?;
    create_core_tables(connection)?;
    Ok(())
}

pub(super) fn recreate(connection: &Connection) -> Result<bool, StoreError> {
    let tables: Vec<String> = connection
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'")?
        .query_map([], |row| row.get(0))?
        .collect::<Result<_, _>>()?;
    let session = if tables.iter().any(|table| table == "encrypted_session") { state::session(connection)? } else { None };
    let pending = if tables.iter().any(|table| table == "encrypted_pending") { legacy_pending(connection, false)?.0 } else { None };
    let outbox = tables.iter().any(|table| table == "manifest") && manifest_read(connection, OUTBOX_FORMAT_KEY)?.as_deref() == Some(outbox_format().as_str());
    if outbox {
        connection.execute_batch("CREATE TEMP TABLE retained_transfer_files AS SELECT * FROM file WHERE field LIKE 'op_item.%';")?;
    }
    for table in tables.iter().filter(|table| !(outbox && table.as_str() != "file" && OUTBOX_TABLES.contains(&table.as_str()))) {
        connection.execute_batch(&format!("DROP TABLE IF EXISTS {}", sql_ident(table)))?;
    }
    create_core_tables(connection)?;
    if outbox {
        connection.execute_batch("INSERT INTO file SELECT * FROM retained_transfer_files; DROP TABLE retained_transfer_files; DELETE FROM op_value WHERE part = 'prior';")?;
    }
    if let Some(session) = session {
        state::restore_session(connection, &session)?;
    }
    if let Some(pending) = pending {
        state::store_pending(connection, &pending)?;
        if let Some(owner) = pending.owner {
            let seq = match owner { crate::transport::RequestOwner::Operation { seq } => seq, crate::transport::RequestOwner::Transfer { operation_id, .. } | crate::transport::RequestOwner::Unmatched { operation_id } => operation_id };
            connection.execute("UPDATE sqlite_sequence SET seq = MAX(seq, ?1) WHERE name = 'op'", [seq])?;
            connection.execute("INSERT INTO sqlite_sequence (name, seq) SELECT 'op', ?1 WHERE NOT EXISTS (SELECT 1 FROM sqlite_sequence WHERE name = 'op')", [seq])?;
        }
    }
    Ok(outbox)
}

struct LostItem { position: i64, row: String, key: String }

fn lost_item(connection: &Connection, models: &BTreeMap<String, ModelInfo>, seq: i64, position: i64) -> Result<Option<LostItem>, StoreError> {
    let Some(input) = crate::operation::read_item_value(connection, seq, position, "input")? else { return Ok(None) };
    let action_key: String = connection.query_row("SELECT action_key FROM op WHERE seq = ?1", [seq], |row| row.get(0))?;
    let Some((model, name)) = crate::operation::Operation::upload_action(&action_key) else { return Ok(None) };
    let Some(action) = models.get(&model).and_then(|info| info.spec.upload_action(&name)) else { return Ok(None) };
    let mut statement = connection.prepare("SELECT row_id FROM op_row WHERE seq = ?1 AND row_id NOT IN (SELECT row_id FROM op_item WHERE seq = ?1 AND row_id IS NOT NULL) ORDER BY pos")?;
    let candidates = statement.query_map([seq], |row| row.get::<_, String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
    let Some(batch) = &action.batch else {
        let [row] = candidates.as_slice() else { return Ok(None) };
        return Ok(Some(LostItem { position, row: row.clone(), key: uuid::Uuid::new_v4().to_string() }));
    };
    let Some(field) = action.correlate.first() else { return Ok(None) };
    let key = batch.input.iter().find(|(_, source)| *source == field).and_then(|(target, _)| input.pointer(&format!("/variables/{target}"))).and_then(serde_json::Value::as_str).map(str::to_string);
    let Some(key) = key else { return Ok(None) };
    let table = models.get(&model).map(|info| info.table.clone()).unwrap_or_else(|| crate::ddl::table_name(&model));
    let lookup = format!("SELECT id FROM {} WHERE id = ?1 AND {} = ?2", sql_ident(&table), sql_ident(&crate::ddl::column_name(field)));
    let mut matches = Vec::new();
    for row in candidates {
        if connection.query_row(&lookup, rusqlite::params![row, key], |found| found.get::<_, String>(0)).optional()?.is_some() { matches.push(row); }
    }
    let [row] = matches.as_slice() else { return Ok(None) };
    Ok(Some(LostItem { position, row: row.clone(), key }))
}

fn legacy_pending(connection: &Connection, preserve_items: bool) -> Result<(Option<state::Pending>, Option<(i64, i64)>), StoreError> {
    let old: bool = connection.query_row("SELECT EXISTS (SELECT 1 FROM pragma_table_info('encrypted_pending') WHERE name = 'upload')", [], |row| row.get(0))?;
    if !old { return Ok((state::pending(connection)?, None)); }
    let pending = connection.query_row("SELECT credential_digest, request_id, response_key, sequence, path, body, owner_seq, item_position, stage, upload FROM encrypted_pending WHERE id = 1", [], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?, row.get::<_, i64>(3)?, row.get::<_, String>(4)?, row.get::<_, String>(5)?, row.get::<_, Option<i64>>(6)?, row.get::<_, Option<i64>>(7)?, row.get::<_, Option<String>>(8)?, row.get::<_, i64>(9)?))
    }).optional()?;
    let Some((credential_digest, request_id, response_key, sequence, path, body, seq, position, stage, upload)) = pending else { return Ok((None, None)) };
    let mut lost = None;
    let mut purpose = match upload { 0 => crate::transport::NetworkPurpose::Standard, 1 => crate::transport::NetworkPurpose::Transfer, _ => return Err(StoreError::Constraint("invalid legacy network purpose".into())) };
    let owner = match (seq, position, stage) {
        (Some(seq), Some(-1), Some(_)) => Some(crate::transport::RequestOwner::Operation { seq }),
        (Some(seq), Some(position), Some(stage)) => {
            let item_id: Option<String> = if preserve_items { connection.query_row("SELECT client_key FROM op_item WHERE seq = ?1 AND position = ?2", rusqlite::params![seq, position], |row| row.get(0)).optional()? } else { None };
            match (item_id, crate::operation::enum_from(&stage)) {
                (Some(item_id), Ok(step)) => Some(crate::transport::RequestOwner::Transfer { operation_id: seq, item_id, step }),
                (None, _) if preserve_items => {
                    lost = Some((seq, position));
                    Some(crate::transport::RequestOwner::Unmatched { operation_id: seq })
                }
                _ => Some(crate::transport::RequestOwner::Unmatched { operation_id: seq }),
            }
        }
        (Some(seq), None, Some(_)) => Some(crate::transport::RequestOwner::Unmatched { operation_id: seq }),
        (None, None, None) if preserve_items => {
            let request: serde_json::Value = serde_json::from_str(&body).map_err(|error| StoreError::Constraint(error.to_string()))?;
            let abort = request.get("operationName").and_then(|value| value.as_str());
            let key = request.pointer("/variables/key").and_then(|value| value.as_str());
            let upload_id = request.pointer("/variables/uploadId").and_then(|value| value.as_str());
            if let (Some(abort), Some(key), Some(upload_id)) = (abort, key, upload_id) {
                let id: Option<i64> = connection.query_row("SELECT MIN(id) FROM upload_orphan WHERE abort_operation = ?1 AND storage_key = ?2 AND upload_id = ?3", rusqlite::params![abort, key, upload_id], |row| row.get(0))?;
                match id {
                    Some(id) => {
                        purpose = crate::transport::NetworkPurpose::Transfer;
                        Some(crate::transport::RequestOwner::Transfer { operation_id: -id, item_id: format!("legacy-abort-{id}"), step: crate::upload::UploadStage::Abort })
                    }
                    None => None,
                }
            } else { None }
        }
        (None, None, None) => None,
        _ => return Err(StoreError::Constraint("incomplete legacy pending owner".into())),
    };
    Ok((Some(state::Pending { credential_digest, request_id, response_key, sequence, path, body, owner, purpose }), lost))
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
