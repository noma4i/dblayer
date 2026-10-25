use rusqlite::{params, Connection};
use serde_json::Value;

use crate::commit::{OperationRecord, OperationStatus};
use crate::identity::RowId;
use crate::operation::{delete_operation, read_item_value, read_value, read_operation_uploads, write_item_value, write_upload};
use crate::upload::UploadInput;

use super::StoreError;

pub(super) fn remove_item(connection: &Connection, seq: i64, position: i64) -> Result<(), StoreError> {
    crate::operation::record_orphan_uploads(connection, seq, Some(position))?;
    connection.execute("DELETE FROM file WHERE id = (SELECT file_id FROM op_item WHERE seq = ?1 AND position = ?2) AND field LIKE 'op_item.%'", params![seq, position])?;
    erase_item(connection, seq, position)
}

fn erase_item(connection: &Connection, seq: i64, position: i64) -> Result<(), StoreError> {
    for table in ["op_value", "op_error", "op_part"] {
        connection.execute(&format!("DELETE FROM {table} WHERE seq = ?1 AND item_position = ?2"), params![seq, position])?;
    }
    for table in ["op_item_header", "op_item_retry", "op_item"] {
        connection.execute(&format!("DELETE FROM {table} WHERE seq = ?1 AND position = ?2"), params![seq, position])?;
    }
    Ok(())
}

pub(super) fn separate_items(connection: &Connection, seq: i64, action: &str, mutation: &str, now: i64) -> Result<Vec<i64>, StoreError> {
    let common = read_value(connection, seq, "input")?.unwrap_or(Value::Null);
    let prior = read_value(connection, seq, "prior")?.unwrap_or(Value::Null);
    let states = read_operation_uploads(connection, seq)?;
    let mut statement = connection.prepare("SELECT position, row_id, client_key, file_id, source_error FROM op_item WHERE seq = ?1 ORDER BY position")?;
    let items = statement.query_map([seq], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?, row.get::<_, Option<String>>(3)?, row.get::<_, Option<String>>(4)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
    let mut sequences = Vec::new();
    for (position, row, key, file, error) in items {
        let value = read_item_value(connection, seq, position, "input")?.unwrap_or(Value::Null);
        let mut input: UploadInput = serde_json::from_value(value).map_err(|error| StoreError::from(std::io::Error::other(error)))?;
        input.steps.mutation = mutation.to_string();
        let mut variables = common.as_object().cloned().unwrap_or_default();
        variables.extend(input.variables.clone());
        let rollback = prior["rows"].as_array().map(|rows| serde_json::json!({"rows": rows.iter().filter(|item| item["id"].as_str() == Some(&row)).collect::<Vec<_>>() }));
        let id = RowId::new(&row).map_err(|error| StoreError::from(std::io::Error::other(error)))?;
        let record = OperationRecord {
            seq: None, action_key: action.to_string(), status: OperationStatus::Pending, row_ids: vec![id.clone()], temp_id: Some(id),
            input: Value::Object(variables), transport_started: false, base_revision: None, created_at: now, dedupe_key: None, rollback, upload: None,
        };
        let next = super::apply::begin_operation(connection, &record)?;
        sequences.push(next);
        let state = states.get(&(seq, position)).ok_or_else(|| StoreError::from(std::io::Error::other("upload state is missing")))?;
        erase_item(connection, seq, position)?;
        write_upload(connection, next, 0, state)?;
        connection.execute("UPDATE op_item SET row_id = ?2, client_key = ?3, file_id = ?4, source_error = ?5 WHERE seq = ?1 AND position = 0", params![next, row, key, file, error])?;
        let value = serde_json::to_value(input).map_err(|error| StoreError::from(std::io::Error::other(error)))?;
        write_item_value(connection, next, 0, "input", Some(&value))?;
    }
    delete_operation(connection, seq)?;
    Ok(sequences)
}

impl super::Store {
    pub(crate) fn upload_orphans(&self) -> Result<Vec<(i64, String, String, String)>, StoreError> {
        let inner = self.lock();
        let mut statement = inner.connection.prepare("SELECT id, storage_key, upload_id, abort_operation FROM upload_orphan ORDER BY id")?;
        let rows = statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    pub(crate) fn settle_upload_orphan(&self, id: i64) -> Result<(), StoreError> {
        self.lock().connection.execute("DELETE FROM upload_orphan WHERE id = ?1", [id])?;
        Ok(())
    }
}
