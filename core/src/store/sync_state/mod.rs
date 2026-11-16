use std::collections::{BTreeSet, HashMap};

use rusqlite::types::Value as SqlValue;
use rusqlite::{params, params_from_iter, Connection, Row};

use crate::identity::RowId;
use crate::store::operation::{attach_extensions, read_extensions, write_extensions, failure_at, failure_cells, info_at, read_operations, read_fields, row_operation_sequences, OPEN_STATUSES};
use crate::owners::write::operation_status::Operation;

use crate::value::sync::{fold_entries, SyncEntry, SyncInfo, SyncState};

pub type SyncKey = (String, String);

const SELECT_STORED_WITH_ID: &str = include_str!("../sql/sync_state/statement_select_row_sync.sql");

const SELECT_STORED: &str = include_str!("../sql/sync_state/statement_select_row_sync_2.sql");

pub fn operation_keys(action_key: &str, row_ids: &[RowId], temp_id: Option<&RowId>) -> Vec<SyncKey> {
    let Some((model, _)) = Operation::split_action_key(action_key) else { return Vec::new() };
    let mut ids: Vec<&RowId> = row_ids.iter().chain(temp_id).collect();
    ids.sort();
    ids.dedup();
    ids.into_iter().map(|id| (model.clone(), id.to_string())).collect()
}

fn open_entries(connection: &Connection, keys: Option<&BTreeSet<SyncKey>>) -> rusqlite::Result<HashMap<SyncKey, Vec<SyncEntry>>> {
    let sequences = match keys {
        Some(keys) => row_operation_sequences(connection, keys)?,
        None => {
            let mut statement = connection.prepare_cached(&format!(include_str!("../sql/sync_state/open_entries_select_op.sql"), OPEN_STATUSES = OPEN_STATUSES))?;
            let rows = statement.query_map([], |row| row.get::<_, i64>(0))?.collect::<rusqlite::Result<BTreeSet<_>>>()?;
            rows
        }
    };
    let operations = read_operations(connection, &sequences)?;
    let updates: Vec<i64> = operations.iter().filter_map(|(seq, operation)| operation.as_ref().filter(|operation| Operation::split_action_key(&operation.action_key).is_some_and(|(_, action)| action == "update")).map(|_| *seq)).collect();
    let mut update_fields = read_fields(connection, &updates)?;
    let mut grouped: HashMap<SyncKey, Vec<SyncEntry>> = HashMap::new();
    for (seq, operation) in operations {
        let Some(operation) = operation else { continue };
        let Some((_, action)) = Operation::split_action_key(&operation.action_key) else { continue };
        let fields = update_fields.remove(&seq).unwrap_or_default();
        let entry = SyncEntry {
            action,
            status: operation.status,
            attempts: operation.attempts,
            error: operation.error.clone(),
            since: operation.since,
            fields,
        };
        for key in operation_keys(&operation.action_key, &operation.row_ids, operation.temp_id.as_ref()).into_iter().filter(|key| keys.is_none_or(|keys| keys.contains(key))) {
            grouped.entry(key).or_default().push(entry.clone());
        }
    }
    Ok(grouped)
}

fn names_of(connection: &Connection, table: &str, key: &SyncKey) -> rusqlite::Result<Vec<String>> {
    let mut statement = connection.prepare_cached(&format!(include_str!("../sql/sync_state/names_of.sql"), table = table))?;
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

fn unsigned(value: i64, column: usize) -> rusqlite::Result<u64> { u64::try_from(value).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(column, value)) }

type Uploads = std::collections::BTreeMap<String, crate::owners::write::upload::UploadInfo>;

fn read_uploads(connection: &Connection, model: &str, ids: &[String]) -> rusqlite::Result<HashMap<String, (Uploads, BTreeSet<String>)>> {
    let mut found: HashMap<String, (Uploads, BTreeSet<String>)> = HashMap::new();
    if !ids.is_empty() {
        let marks = "SELECT value FROM json_each(?)";
        let mut statement = connection.prepare_cached(&format!(include_str!("../sql/sync_state/read_uploads.sql"), marks = marks))?;
        let ids = serde_json::to_string(ids).map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
        let mut rows = statement.query(params![model, ids])?;
        while let Some(row) = rows.next()? {
            let outcome: Option<String> = row.get(17)?;
            if matches!(outcome.as_deref(), Some("confirmed" | "discarded")) { continue; }
            if let Some(mut info) = info_at(row, 2)? {
                let (id, action): (String, String) = (row.get(0)?, row.get(1)?);
                if matches!(outcome.as_deref(), Some("failed" | "rejected")) {
                    info.status = crate::owners::write::upload::UploadStatus::Failed;
                    info.error = Some(crate::owners::write::failure::parse(&row.get::<_, Option<String>>(18)?.unwrap_or_default()));
                }
                let size = row.get::<_, Option<i64>>(10)?.map_or(Ok(0), |size| unsigned(size, 10))?;
                info.copy_file = crate::owners::write::upload::TransferFile::of(size, crate::owners::write::transfer::file_id(unsigned(row.get(11)?, 11)?), row.get::<_, Option<String>>(15)?, info.status == crate::owners::write::upload::UploadStatus::Failed);
                if let (None, Some(reason)) = (&info.error, row.get::<_, Option<String>>(13)?) {
                    info.error = Some(crate::value::sync::OperationFailure::new(crate::value::sync::FailureKind::Refused, reason));
                }
                info.sent_bytes = crate::owners::write::upload::bytes_sent(info.status, info.total_bytes, info.sent_bytes, size);
                info.total_bytes = size;
                info.share = u32::try_from(row.get::<_, i64>(14)?).unwrap_or(0);
                info.batch_share = row.get::<_, Option<i64>>(16)?.map(|share| u32::try_from(share).unwrap_or(0));
                let entry = found.entry(id).or_default();
                if row.get::<_, Option<i64>>(12)?.is_some() {
                    entry.1.insert(action.clone());
                }
                entry.0.insert(action, info);
            }
        }
    }
    Ok(found)
}

pub fn read_sync(connection: &Connection, model: &str, ids: &[String]) -> rusqlite::Result<HashMap<String, SyncInfo>> {
    let mut found = HashMap::new();
    if !ids.is_empty() {
        let encoded = serde_json::to_string(ids).map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
        let sql = format!("{SELECT_STORED_WITH_ID} WHERE model = ? AND row_id IN (SELECT value FROM json_each(?))");
        let mut statement = connection.prepare_cached(&sql)?;
        let mut rows = statement.query(params![model, encoded])?;
        while let Some(row) = rows.next()? {
            let id: String = row.get(0)?;
            let info = SyncInfo { state: SyncState::from_sql(&row.get::<_, String>(1)?)?, actions: Vec::new(), dirty_fields: Vec::new(), error: failure_at(row, 2)?, attempts: row.get(4)?, since: row.get(5)?, uploads: Default::default() };
            found.insert(id, info);
        }
        for table in ["sync_action", "sync_field"] {
            let sql = format!("SELECT row_id, name FROM {table} WHERE model = ? AND row_id IN (SELECT value FROM json_each(?)) ORDER BY row_id, pos");
            let mut statement = connection.prepare_cached(&sql)?;
            let names = statement.query_map(params![model, encoded], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))?;
            for pair in names {
                let (id, name) = pair?;
                if let Some(info) = found.get_mut(&id) { if table == "sync_action" { info.actions.push(name); } else { info.dirty_fields.push(name); } }
            }
        }
        let args = [SqlValue::Text(model.to_string()), SqlValue::Text(encoded)];
        for (owner, extensions) in read_extensions(connection, "sync_row_error", &["model", "row_id"], "model = ?1 AND row_id IN (SELECT value FROM json_each(?2))", &args)? {
            if let Some(SqlValue::Text(id)) = owner.get(1) { if let Some(info) = found.get_mut(id) { attach_extensions(&mut info.error, Some(extensions)); } }
        }
    }
    for (id, (uploads, batched)) in read_uploads(connection, model, ids)? {
        let info = found.entry(id).or_default();
        info.uploads = uploads;
        info.fold_transfers(&batched);
    }
    Ok(found)
}

fn stored(connection: &Connection, key: &SyncKey) -> rusqlite::Result<Option<SyncInfo>> {
    let mut statement = connection.prepare_cached(&format!("{SELECT_STORED} WHERE model = ?1 AND row_id = ?2"))?;
    let mut rows = statement.query(params![key.0, key.1])?;
    let info = rows.next()?.map(|row| stored_info(connection, key, row, 0)).transpose()?;
    Ok(info)
}

pub fn clear_sync(connection: &Connection, model: Option<&str>) -> rusqlite::Result<()> {
    for table in ["sync_action", "sync_field", "sync_upload", "row_sync", "sync_row_error"] {
        match model {
            Some(model) => connection.execute(&format!(include_str!("../sql/sync_state/clear_sync_delete_table.sql"), table = table), [model])?,
            None => connection.execute(&format!(include_str!("../sql/sync_state/clear_sync_delete_table_2.sql"), table = table), [])?,
        };
    }
    crate::store::prepare_rows::clear_prepare_without_upload(connection)
}

fn write(connection: &Connection, key: &SyncKey, info: &SyncInfo) -> rusqlite::Result<()> {
    remove(connection, key)?;
    let [kind, message] = failure_cells(info.error.as_ref());
    let cells: Vec<SqlValue> = vec![key.0.clone().into(), key.1.clone().into(), info.state.as_sql().to_string().into(), kind, message, i64::from(info.attempts).into(), info.since.into()];
    let marks = vec!["?"; cells.len()].join(", ");
    connection
        .prepare_cached(&format!(include_str!("../sql/sync_state/write_insert_row_sync.sql"), marks = marks))?
        .execute(params_from_iter(cells))?;
    write_extensions(connection, "sync_row_error", &["model", "row_id"], &[SqlValue::Text(key.0.clone()), SqlValue::Text(key.1.clone())], info.error.as_ref())?;
    for (table, names) in [("sync_action", &info.actions), ("sync_field", &info.dirty_fields)] {
        let mut insert = connection.prepare_cached(&format!(include_str!("../sql/sync_state/write_insert_table.sql"), table = table))?;
        for (pos, name) in names.iter().enumerate() {
            insert.execute(params![key.0, key.1, pos as i64, name])?;
        }
    }
    Ok(())
}

fn remove(connection: &Connection, key: &SyncKey) -> rusqlite::Result<()> {
    for table in ["sync_action", "sync_field", "row_sync", "sync_row_error"] {
        connection.prepare_cached(&format!(include_str!("../sql/sync_state/remove_delete_table.sql"), table = table))?.execute(params![key.0, key.1])?;
    }
    Ok(())
}

pub fn recompute_sync(connection: &Connection, keys: &BTreeSet<SyncKey>) -> rusqlite::Result<BTreeSet<SyncKey>> {
    let mut changed = BTreeSet::new();
    if keys.is_empty() {
        return Ok(changed);
    }
    let entries = open_entries(connection, Some(keys))?;
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
    let mut keys: BTreeSet<SyncKey> = open_entries(connection, None)?.into_keys().collect();
    let mut statement = connection.prepare(include_str!("../sql/sync_state/rebuild_sync_with.sql"))?;
    for key in statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))? {
        keys.insert(key?);
    }
    Ok(recompute_sync(connection, &keys)?.len())
}

#[cfg(test)]
mod tests;
