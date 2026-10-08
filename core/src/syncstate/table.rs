use std::collections::{BTreeSet, HashMap};

use rusqlite::{params, params_from_iter, Connection, Row};
use serde_json::Value;

use crate::commit::OperationStatus;
use crate::identity::RowId;
use crate::operation::Operation;

use super::derive::{fold_entries, SyncEntry};
use super::info::{OperationFailure, SyncInfo, SyncState};

pub type SyncKey = (String, String);

const CHUNK: usize = 500;

const OPEN_ENTRIES: &str = "SELECT action_key, status, row_ids, temp_id, transport_started, attempts, error, since, \
     CASE WHEN action_key LIKE '%:update' THEN input END, upload \
     FROM op WHERE status IN ('pending', 'failed', 'delivery_unknown') ORDER BY seq";

const SELECT_STORED: &str = "SELECT state, actions, dirty_fields, error, attempts, since, upload FROM row_sync";

pub fn operation_keys(action_key: &str, row_ids: &[RowId], temp_id: Option<&RowId>) -> Vec<SyncKey> {
    let Some((model, _)) = Operation::split_action_key(action_key) else { return Vec::new() };
    let mut ids: Vec<&RowId> = row_ids.iter().chain(temp_id).collect();
    ids.sort();
    ids.dedup();
    ids.into_iter().map(|id| (model.clone(), id.to_string())).collect()
}

fn json_list(bytes: &[u8]) -> Vec<String> {
    serde_json::from_slice(bytes).unwrap_or_default()
}

fn open_entries(connection: &Connection) -> rusqlite::Result<HashMap<SyncKey, Vec<SyncEntry>>> {
    let mut statement = connection.prepare_cached(OPEN_ENTRIES)?;
    let mut rows = statement.query([])?;
    let mut grouped: HashMap<SyncKey, Vec<SyncEntry>> = HashMap::new();
    while let Some(row) = rows.next()? {
        let action_key: String = row.get(0)?;
        let Some(status) = OperationStatus::from_sql(&row.get::<_, String>(1)?) else { continue };
        let row_ids: Vec<RowId> = serde_json::from_slice(&row.get::<_, Vec<u8>>(2)?).unwrap_or_default();
        let temp_id: Option<RowId> = row.get::<_, Option<String>>(3)?.and_then(|text| RowId::new(&text).ok());
        let error: Option<OperationFailure> = row.get::<_, Option<Vec<u8>>>(6)?.and_then(|bytes| serde_json::from_slice(&bytes).ok());
        let fields: Vec<String> = match row.get::<_, Option<Vec<u8>>>(8)?.and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok()) {
            Some(Value::Object(input)) => input.keys().filter(|name| *name != "id").cloned().collect(),
            _ => Vec::new(),
        };
        let Some((_, action)) = Operation::split_action_key(&action_key) else { continue };
        let entry = SyncEntry {
            action,
            status,
            transport_started: row.get::<_, i64>(4)? != 0,
            attempts: row.get::<_, u32>(5)?,
            error,
            since: row.get(7)?,
            fields,
            upload: row.get::<_, Option<Vec<u8>>>(9)?.and_then(|bytes| serde_json::from_slice(&bytes).ok()),
        };
        for key in operation_keys(&action_key, &row_ids, temp_id.as_ref()) {
            grouped.entry(key).or_default().push(entry.clone());
        }
    }
    Ok(grouped)
}

fn stored_info(row: &Row<'_>, first: usize) -> rusqlite::Result<SyncInfo> {
    Ok(SyncInfo {
        state: SyncState::from_sql(&row.get::<_, String>(first)?).unwrap_or_default(),
        actions: json_list(&row.get::<_, Vec<u8>>(first + 1)?),
        dirty_fields: json_list(&row.get::<_, Vec<u8>>(first + 2)?),
        error: row.get::<_, Option<Vec<u8>>>(first + 3)?.and_then(|bytes| serde_json::from_slice(&bytes).ok()),
        attempts: row.get(first + 4)?,
        since: row.get(first + 5)?,
        upload: row.get::<_, Option<Vec<u8>>>(first + 6)?.and_then(|bytes| serde_json::from_slice(&bytes).ok()),
    })
}

pub fn read_sync(connection: &Connection, model: &str, ids: &[String]) -> rusqlite::Result<HashMap<String, SyncInfo>> {
    let mut found = HashMap::new();
    for chunk in ids.chunks(CHUNK) {
        let marks = vec!["?"; chunk.len()].join(", ");
        let sql = format!("SELECT row_id, state, actions, dirty_fields, error, attempts, since, upload FROM row_sync WHERE model = ? AND row_id IN ({marks})");
        let mut statement = connection.prepare_cached(&sql)?;
        let arguments = std::iter::once(model).chain(chunk.iter().map(String::as_str));
        let mut rows = statement.query(params_from_iter(arguments))?;
        while let Some(row) = rows.next()? {
            let id: String = row.get(0)?;
            let info = stored_info(row, 1)?;
            found.insert(id, info);
        }
    }
    Ok(found)
}

fn stored(connection: &Connection, key: &SyncKey) -> rusqlite::Result<Option<SyncInfo>> {
    let mut statement = connection.prepare_cached(&format!("{SELECT_STORED} WHERE model = ?1 AND row_id = ?2"))?;
    let mut rows = statement.query(params![key.0, key.1])?;
    rows.next()?.map(|row| stored_info(row, 0)).transpose()
}

fn write(connection: &Connection, key: &SyncKey, info: &SyncInfo) -> rusqlite::Result<()> {
    let error = info.error.as_ref().map(|error| serde_json::to_vec(error).expect("failure serializes"));
    connection.prepare_cached(
        "INSERT OR REPLACE INTO row_sync (model, row_id, state, actions, dirty_fields, error, attempts, since, upload) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
    )?
    .execute(params![
        key.0,
        key.1,
        info.state.as_sql(),
        serde_json::to_vec(&info.actions).expect("actions serialize"),
        serde_json::to_vec(&info.dirty_fields).expect("fields serialize"),
        error,
        info.attempts,
        info.since,
        info.upload.as_ref().map(|upload| serde_json::to_vec(upload).expect("upload serializes")),
    ])?;
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
            None => {
                connection.prepare_cached("DELETE FROM row_sync WHERE model = ?1 AND row_id = ?2")?.execute(params![key.0, key.1])?;
            }
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
