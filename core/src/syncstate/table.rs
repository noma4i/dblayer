use std::collections::{BTreeSet, HashMap};

use rusqlite::types::Value as SqlValue;
use rusqlite::{params, params_from_iter, Connection, Row};

use crate::operation::OperationStatus;
use crate::identity::RowId;
use crate::operation::{attach_extensions, read_extensions, write_extensions, failure_at, failure_cells, info_at, info_cells, read_operation, read_operation_uploads, read_fields, row_operation_sequences, Operation, INFO_COLUMNS, OPEN_STATUSES};

use super::derive::{fold_entries, SyncEntry};
use super::info::{SyncInfo, SyncState};

pub type SyncKey = (String, String);

const CHUNK: usize = 500;

const SELECT_STORED_WITH_ID: &str = "SELECT row_id, state, error_kind, error_message, attempts, since FROM row_sync";

const SELECT_STORED: &str = "SELECT state, error_kind, error_message, attempts, since FROM row_sync";

pub fn operation_keys(action_key: &str, row_ids: &[RowId], temp_id: Option<&RowId>) -> Vec<SyncKey> {
    let Some((model, _)) = Operation::split_action_key(action_key) else { return Vec::new() };
    let mut ids: Vec<&RowId> = row_ids.iter().chain(temp_id).collect();
    ids.sort();
    ids.dedup();
    ids.into_iter().map(|id| (model.clone(), id.to_string())).collect()
}

fn open_entries(connection: &Connection, keys: Option<&BTreeSet<SyncKey>>, batch: &dyn Fn(&str) -> bool) -> rusqlite::Result<HashMap<SyncKey, Vec<SyncEntry>>> {
    let sequences = match keys {
        Some(keys) => row_operation_sequences(connection, keys)?,
        None => {
            let mut statement = connection.prepare_cached(&format!("SELECT seq FROM op WHERE status IN {OPEN_STATUSES} ORDER BY seq"))?;
            let rows = statement.query_map([], |row| row.get::<_, i64>(0))?.collect::<rusqlite::Result<BTreeSet<_>>>()?;
            rows
        }
    };
    let mut grouped: HashMap<SyncKey, Vec<SyncEntry>> = HashMap::new();
    for seq in sequences {
        let Some(operation) = read_operation(connection, seq)? else { continue };
        let uploads = read_operation_uploads(connection, seq)?;
        let mut statement = connection.prepare_cached("SELECT row_id, position FROM op_item WHERE seq = ?1 AND row_id IS NOT NULL")?;
        let positions = statement.query_map([seq], |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)))?.collect::<rusqlite::Result<HashMap<_, _>>>()?;
        let Some((_, action)) = Operation::split_action_key(&operation.action_key) else { continue };
        let fields = if action == "update" { read_fields(connection, seq)? } else { Vec::new() };
        let entry = SyncEntry {
            action,
            status: operation.status,
            transport_started: operation.transport_started,
            attempts: operation.attempts,
            error: operation.error.clone(),
            since: operation.since,
            fields,
            upload: None,
            batch: batch(&operation.action_key),
        };
        for key in operation_keys(&operation.action_key, &operation.row_ids, operation.temp_id.as_ref()).into_iter().filter(|key| keys.is_none_or(|keys| keys.contains(key))) {
            let mut item = entry.clone();
            let position = positions.get(&key.1).copied().unwrap_or(0);
            item.upload = uploads.get(&(seq, position)).cloned();
            if item.status == OperationStatus::Pending && item.upload.as_ref().is_some_and(|state| state.phase == crate::upload::UploadStatus::Failed) {
                item.status = OperationStatus::Failed;
                item.error = item.upload.as_ref().and_then(|state| state.error.clone());
            }
            grouped.entry(key).or_default().push(item);
        }
    }
    Ok(grouped)
}

fn names_of(connection: &Connection, table: &str, key: &SyncKey) -> rusqlite::Result<Vec<String>> {
    let mut statement = connection.prepare_cached(&format!("SELECT name FROM {table} WHERE model = ?1 AND row_id = ?2 ORDER BY pos"))?;
    let names = statement.query_map(params![key.0, key.1], |row| row.get(0))?;
    names.collect()
}

fn stored_info(connection: &Connection, key: &SyncKey, row: &Row<'_>, first: usize) -> rusqlite::Result<SyncInfo> {
    let mut info = SyncInfo {
        state: SyncState::from_sql(&row.get::<_, String>(first)?)?,
        actions: names_of(connection, "sync_action", key)?,
        dirty_fields: names_of(connection, "sync_field", key)?,
        error: failure_at(row, first + 1)?,
        attempts: row.get(first + 3)?,
        since: row.get(first + 4)?,
        uploads: std::collections::BTreeMap::new(),
    };
    let owner = [SqlValue::Text(key.0.clone()), SqlValue::Text(key.1.clone())];
    let extensions = read_extensions(connection, "sync_row_error", &["model", "row_id"], "model = ?1 AND row_id = ?2", &owner)?.into_iter().next().map(|(_, extensions)| extensions);
    attach_extensions(&mut info.error, extensions);
    Ok(info)
}

fn read_uploads(connection: &Connection, model: &str, ids: &[String]) -> rusqlite::Result<HashMap<String, std::collections::BTreeMap<String, crate::upload::UploadInfo>>> {
    let mut found: HashMap<String, std::collections::BTreeMap<String, crate::upload::UploadInfo>> = HashMap::new();
    for chunk in ids.chunks(CHUNK) {
        let marks = vec!["?"; chunk.len()].join(", ");
        let mut statement = connection.prepare_cached(&format!("SELECT row_id, action, {INFO_COLUMNS}, size, transfer_id FROM sync_upload WHERE model = ? AND row_id IN ({marks})"))?;
        let arguments = std::iter::once(model).chain(chunk.iter().map(String::as_str));
        let mut rows = statement.query(params_from_iter(arguments))?;
        while let Some(row) = rows.next()? {
            if let Some(mut info) = info_at(row, 2)? {
                let (id, action): (String, String) = (row.get(0)?, row.get(1)?);
                if let (Some(size), Some(transfer_id)) = (row.get::<_, Option<i64>>(10)?, row.get::<_, Option<i64>>(11)?) {
                    if size > 0 {
                        info.copy_file = Some(crate::owners::write::transfer::file_id(u64::try_from(transfer_id).unwrap_or_default()));
                    }
                }
                found.entry(id).or_default().insert(action, info);
            }
        }
        let owner = std::iter::once(SqlValue::Text(model.to_string())).chain(chunk.iter().cloned().map(SqlValue::Text)).collect::<Vec<_>>();
        for (key, extensions) in read_extensions(connection, "sync_upload_error", &["model", "row_id", "action"], &format!("model = ? AND row_id IN ({marks})"), &owner)? {
            if let [_, SqlValue::Text(id), SqlValue::Text(action)] = key.as_slice() {
                if let Some(info) = found.get_mut(id).and_then(|uploads| uploads.get_mut(action)) {
                    attach_extensions(&mut info.error, Some(extensions));
                }
            }
        }
    }
    Ok(found)
}

pub fn read_sync(connection: &Connection, model: &str, ids: &[String]) -> rusqlite::Result<HashMap<String, SyncInfo>> {
    let mut found = HashMap::new();
    for chunk in ids.chunks(CHUNK) {
        let marks = vec!["?"; chunk.len()].join(", ");
        let sql = format!("{SELECT_STORED_WITH_ID} WHERE model = ? AND row_id IN ({marks})");
        let mut statement = connection.prepare_cached(&sql)?;
        let arguments = std::iter::once(model).chain(chunk.iter().map(String::as_str));
        let mut rows = statement.query(params_from_iter(arguments))?;
        while let Some(row) = rows.next()? {
            let id: String = row.get(0)?;
            let info = stored_info(connection, &(model.to_string(), id.clone()), row, 1)?;
            found.insert(id, info);
        }
    }
    for (id, uploads) in read_uploads(connection, model, ids)? {
        found.entry(id).or_default().uploads = uploads;
    }
    Ok(found)
}

fn stored(connection: &Connection, key: &SyncKey) -> rusqlite::Result<Option<SyncInfo>> {
    let mut statement = connection.prepare_cached(&format!("{SELECT_STORED} WHERE model = ?1 AND row_id = ?2"))?;
    let mut rows = statement.query(params![key.0, key.1])?;
    let info = rows.next()?.map(|row| stored_info(connection, key, row, 0)).transpose()?;
    let uploads = read_uploads(connection, &key.0, std::slice::from_ref(&key.1))?.remove(&key.1);
    Ok(match (info, uploads) {
        (None, None) => None,
        (info, uploads) => Some(SyncInfo { uploads: uploads.unwrap_or_default(), ..info.unwrap_or_default() }),
    })
}

pub fn clear_sync(connection: &Connection, model: Option<&str>) -> rusqlite::Result<()> {
    for table in ["sync_action", "sync_field", "sync_upload", "sync_upload_error", "row_sync", "sync_row_error"] {
        match model {
            Some(model) => connection.execute(&format!("DELETE FROM {table} WHERE model = ?1"), [model])?,
            None => connection.execute(&format!("DELETE FROM {table}"), [])?,
        };
    }
    Ok(())
}

fn write(connection: &Connection, key: &SyncKey, info: &SyncInfo) -> rusqlite::Result<()> {
    remove(connection, key)?;
    let [kind, message] = failure_cells(info.error.as_ref());
    let cells: Vec<SqlValue> = vec![key.0.clone().into(), key.1.clone().into(), info.state.as_sql().to_string().into(), kind, message, i64::from(info.attempts).into(), info.since.into()];
    let marks = vec!["?"; cells.len()].join(", ");
    connection
        .prepare_cached(&format!("INSERT INTO row_sync (model, row_id, state, error_kind, error_message, attempts, since) VALUES ({marks})"))?
        .execute(params_from_iter(cells))?;
    write_extensions(connection, "sync_row_error", &["model", "row_id"], &[SqlValue::Text(key.0.clone()), SqlValue::Text(key.1.clone())], info.error.as_ref())?;
    for (action, upload) in &info.uploads {
        write_extensions(connection, "sync_upload_error", &["model", "row_id", "action"], &[SqlValue::Text(key.0.clone()), SqlValue::Text(key.1.clone()), SqlValue::Text(action.clone())], upload.error.as_ref())?;
        let mut cells: Vec<SqlValue> = vec![key.0.clone().into(), key.1.clone().into(), action.clone().into()];
        cells.extend(info_cells(Some(upload))?);
        let marks = vec!["?"; cells.len()].join(", ");
        connection.prepare_cached(&format!("INSERT OR IGNORE INTO sync_upload (model, row_id, action, {INFO_COLUMNS}) VALUES ({marks})"))?.execute(params_from_iter(cells))?;
    }
    for (table, names) in [("sync_action", &info.actions), ("sync_field", &info.dirty_fields)] {
        let mut insert = connection.prepare_cached(&format!("INSERT INTO {table} (model, row_id, pos, name) VALUES (?1, ?2, ?3, ?4)"))?;
        for (pos, name) in names.iter().enumerate() {
            insert.execute(params![key.0, key.1, pos as i64, name])?;
        }
    }
    Ok(())
}

fn remove(connection: &Connection, key: &SyncKey) -> rusqlite::Result<()> {
    for table in ["sync_action", "sync_field", "sync_upload_error", "row_sync", "sync_row_error"] {
        connection.prepare_cached(&format!("DELETE FROM {table} WHERE model = ?1 AND row_id = ?2"))?.execute(params![key.0, key.1])?;
    }
    connection.prepare_cached("DELETE FROM sync_upload WHERE model = ?1 AND row_id = ?2 AND seq IS NULL")?.execute(params![key.0, key.1])?;
    Ok(())
}

pub fn recompute_sync(connection: &Connection, keys: &BTreeSet<SyncKey>) -> rusqlite::Result<BTreeSet<SyncKey>> {
    recompute_sync_with(connection, keys, &|_| false)
}

pub fn recompute_sync_with(connection: &Connection, keys: &BTreeSet<SyncKey>, batch: &dyn Fn(&str) -> bool) -> rusqlite::Result<BTreeSet<SyncKey>> {
    let mut changed = BTreeSet::new();
    if keys.is_empty() {
        return Ok(changed);
    }
    let entries = open_entries(connection, Some(keys), batch)?;
    for key in keys {
        let next = entries.get(key).and_then(|entries| fold_entries(entries));
        let current = stored(connection, key)?;
        if next == current {
            continue;
        }
        match &next {
            Some(info) => write(connection, key, info)?,
            None => remove(connection, key)?,
        }
        changed.insert(key.clone());
    }
    Ok(changed)
}

pub fn rebuild_sync(connection: &Connection) -> rusqlite::Result<usize> {
    rebuild_sync_with(connection, &|_| false)
}

pub fn rebuild_sync_with(connection: &Connection, batch: &dyn Fn(&str) -> bool) -> rusqlite::Result<usize> {
    let mut keys: BTreeSet<SyncKey> = open_entries(connection, None, batch)?.into_keys().collect();
    let mut statement = connection.prepare("SELECT model, row_id FROM row_sync")?;
    for key in statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))? {
        keys.insert(key?);
    }
    Ok(recompute_sync_with(connection, &keys, batch)?.len())
}
