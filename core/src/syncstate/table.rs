use std::collections::{BTreeSet, HashMap};

use rusqlite::types::Value as SqlValue;
use rusqlite::{params, params_from_iter, Connection, Row};

use crate::commit::OperationStatus;
use crate::identity::RowId;
use crate::operation::{failure_at, failure_cells, info_at, info_cells, read_all_fields, read_upload_items, read_all_rows, Operation, INFO_COLUMNS};

use super::derive::{fold_entries, SyncEntry};
use super::info::{SyncInfo, SyncState};

pub type SyncKey = (String, String);

const CHUNK: usize = 500;

const SELECT_STORED_WITH_ID: &str = "SELECT row_id, state, error_kind, error_message, error_extensions, attempts, since, up_status, up_stage, up_sent, up_total, up_wait, up_attempts, up_error_kind, up_error_message, up_error_extensions FROM row_sync";

const SELECT_STORED: &str = "SELECT state, error_kind, error_message, error_extensions, attempts, since, up_status, up_stage, up_sent, up_total, up_wait, up_attempts, up_error_kind, up_error_message, up_error_extensions FROM row_sync";

pub fn operation_keys(action_key: &str, row_ids: &[RowId], temp_id: Option<&RowId>) -> Vec<SyncKey> {
    let Some((model, _)) = Operation::split_action_key(action_key) else { return Vec::new() };
    let mut ids: Vec<&RowId> = row_ids.iter().chain(temp_id).collect();
    ids.sort();
    ids.dedup();
    ids.into_iter().map(|id| (model.clone(), id.to_string())).collect()
}

fn open_entries(connection: &Connection) -> rusqlite::Result<HashMap<SyncKey, Vec<SyncEntry>>> {
    let mut rows_of = read_all_rows(connection)?;
    let uploads = read_upload_items(connection)?;
    let mut statement = connection.prepare_cached("SELECT seq, row_id, position FROM op_item WHERE row_id IS NOT NULL")?;
    let positions = statement.query_map([], |row| Ok(((row.get::<_, i64>(0)?, row.get::<_, String>(1)?), row.get::<_, i64>(2)?)))?.collect::<rusqlite::Result<HashMap<_, _>>>()?;
    let mut fields_of = read_all_fields(connection)?;
    let sql = "SELECT seq, action_key, status, temp_id, transport_started, attempts, error_kind, error_message, error_extensions, since FROM op WHERE status IN ('pending', 'failed', 'delivery_unknown') ORDER BY seq";
    let mut statement = connection.prepare_cached(&sql)?;
    let mut rows = statement.query([])?;
    let mut grouped: HashMap<SyncKey, Vec<SyncEntry>> = HashMap::new();
    while let Some(row) = rows.next()? {
        let seq: i64 = row.get(0)?;
        let action_key: String = row.get(1)?;
        let Some(status) = OperationStatus::from_sql(&row.get::<_, String>(2)?) else { continue };
        let row_ids = rows_of.remove(&seq).unwrap_or_default();
        let temp_id: Option<RowId> = row.get::<_, Option<String>>(3)?.and_then(|text| RowId::new(&text).ok());
        let Some((_, action)) = Operation::split_action_key(&action_key) else { continue };
        let fields = if action == "update" { fields_of.remove(&seq).unwrap_or_default() } else { Vec::new() };
        let entry = SyncEntry {
            action,
            status,
            transport_started: row.get::<_, i64>(4)? != 0,
            attempts: row.get::<_, u32>(5)?,
            error: failure_at(row, 6)?,
            since: row.get(9)?,
            fields,
            upload: None,
        };
        for key in operation_keys(&action_key, &row_ids, temp_id.as_ref()) {
            let mut item = entry.clone();
            let position = positions.get(&(seq, key.1.clone())).copied().unwrap_or(0);
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
    Ok(SyncInfo {
        state: SyncState::from_sql(&row.get::<_, String>(first)?).unwrap_or_default(),
        actions: names_of(connection, "sync_action", key)?,
        dirty_fields: names_of(connection, "sync_field", key)?,
        error: failure_at(row, first + 1)?,
        attempts: row.get(first + 4)?,
        since: row.get(first + 5)?,
        upload: info_at(row, first + 6)?,
    })
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
    Ok(found)
}

fn stored(connection: &Connection, key: &SyncKey) -> rusqlite::Result<Option<SyncInfo>> {
    let mut statement = connection.prepare_cached(&format!("{SELECT_STORED} WHERE model = ?1 AND row_id = ?2"))?;
    let mut rows = statement.query(params![key.0, key.1])?;
    rows.next()?.map(|row| stored_info(connection, key, row, 0)).transpose()
}

pub fn clear_sync(connection: &Connection, model: Option<&str>) -> rusqlite::Result<()> {
    for table in ["sync_action", "sync_field", "row_sync"] {
        match model {
            Some(model) => connection.execute(&format!("DELETE FROM {table} WHERE model = ?1"), [model])?,
            None => connection.execute(&format!("DELETE FROM {table}"), [])?,
        };
    }
    Ok(())
}

fn write(connection: &Connection, key: &SyncKey, info: &SyncInfo) -> rusqlite::Result<()> {
    remove(connection, key)?;
    let [kind, message, extensions] = failure_cells(info.error.as_ref())?;
    let mut cells: Vec<SqlValue> = vec![key.0.clone().into(), key.1.clone().into(), info.state.as_sql().to_string().into(), kind, message, extensions, i64::from(info.attempts).into(), info.since.into()];
    cells.extend(info_cells(info.upload.as_ref())?);
    let marks = vec!["?"; cells.len()].join(", ");
    connection
        .prepare_cached(&format!("INSERT INTO row_sync (model, row_id, state, error_kind, error_message, error_extensions, attempts, since, {INFO_COLUMNS}) VALUES ({marks})"))?
        .execute(params_from_iter(cells))?;
    for (table, names) in [("sync_action", &info.actions), ("sync_field", &info.dirty_fields)] {
        let mut insert = connection.prepare_cached(&format!("INSERT INTO {table} (model, row_id, pos, name) VALUES (?1, ?2, ?3, ?4)"))?;
        for (pos, name) in names.iter().enumerate() {
            insert.execute(params![key.0, key.1, pos as i64, name])?;
        }
    }
    Ok(())
}

fn remove(connection: &Connection, key: &SyncKey) -> rusqlite::Result<()> {
    for table in ["sync_action", "sync_field", "row_sync"] {
        connection.prepare_cached(&format!("DELETE FROM {table} WHERE model = ?1 AND row_id = ?2"))?.execute(params![key.0, key.1])?;
    }
    Ok(())
}

pub fn recompute_sync(connection: &Connection, keys: &BTreeSet<SyncKey>) -> rusqlite::Result<BTreeSet<SyncKey>> {
    let mut changed = BTreeSet::new();
    if keys.is_empty() {
        return Ok(changed);
    }
    let entries = open_entries(connection)?;
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
    let mut keys: BTreeSet<SyncKey> = open_entries(connection)?.into_keys().collect();
    let mut statement = connection.prepare("SELECT model, row_id FROM row_sync")?;
    for key in statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))? {
        keys.insert(key?);
    }
    Ok(recompute_sync(connection, &keys)?.len())
}
