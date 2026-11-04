use rusqlite::{params, Connection};
use serde_json::Value;

use crate::commit::{OperationRecord};
use crate::operation::OperationStatus;
use crate::identity::RowId;
use crate::operation::{delete_operation, read_item_value, read_value, read_operation_uploads, write_item_value, write_upload};
use crate::upload::UploadInput;

use super::StoreError;

pub(super) fn remove_item(connection: &Connection, seq: i64, position: i64) -> Result<(), StoreError> {
    crate::operation::record_transfer_aborts(connection, seq, Some(position))?;
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
    let prior = crate::db::op_effect::read_prior(connection, seq)?.unwrap_or(Value::Null);
    let states = read_operation_uploads(connection, seq)?;
    let mut statement = connection.prepare("SELECT position, row_id, client_key, file_id, source_error, transfer_operation_id FROM op_item WHERE seq = ?1 ORDER BY position")?;
    let items = statement.query_map([seq], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?, row.get::<_, Option<String>>(3)?, row.get::<_, Option<String>>(4)?, row.get::<_, i64>(5)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
    let mut sequences = Vec::new();
    for (position, row, key, file, error, operation_id) in items {
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
        connection.execute("UPDATE scope_revision SET operation_seq = ?3 WHERE operation_seq = ?1 AND row_id = ?2", params![seq, row, next])?;
        sequences.push(next);
        let state = states.get(&(seq, position)).ok_or_else(|| StoreError::from(std::io::Error::other("upload state is missing")))?;
        erase_item(connection, seq, position)?;
        write_upload(connection, next, 0, state)?;
        connection.execute("UPDATE op_item SET row_id = ?2, client_key = ?3, file_id = ?4, source_error = ?5, transfer_operation_id = ?6 WHERE seq = ?1 AND position = 0", params![next, row, key, file, error, operation_id])?;
        let value = serde_json::to_value(input).map_err(|error| StoreError::from(std::io::Error::other(error)))?;
        write_item_value(connection, next, 0, "input", Some(&value))?;
    }
    delete_operation(connection, seq)?;
    Ok(sequences)
}

impl super::Store {
    pub(crate) fn transfer_abort_name(&self, operation_id: i64, item_id: &str) -> Result<Option<String>, StoreError> {
        use rusqlite::OptionalExtension;
        let inner = self.lock();
        Ok(inner.connection.query_row("SELECT abort_operation FROM transfer_abort WHERE operation_id = ?1 AND item_id = ?2", params![operation_id, item_id], |row| row.get(0)).optional()?)
    }

    pub(crate) fn pending_transfer_aborts(&self) -> Result<Vec<crate::upload::Transfer>, StoreError> {
        let inner = self.lock();
        let mut statement = inner.connection.prepare("SELECT id, operation_id, item_id, storage_key, upload_id, abort_operation FROM transfer_abort ORDER BY id")?;
        let rows = statement.query_map([], |row| Ok(crate::upload::Transfer::Abort {
            id: row.get(0)?,
            operation_id: row.get(1)?, item_id: row.get(2)?,
            key: row.get(3)?, upload_id: row.get(4)?, operation: row.get(5)?,
        }))?.collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    pub(crate) fn settle_transfer_abort(&self, authority: &crate::lifecycle::Authority, id: i64) -> Result<(), StoreError> {
        self.write_store(authority, |inner| {
            inner.connection.execute("DELETE FROM transfer_abort WHERE id = ?1", [id])?;
            Ok(())
        })
    }

    pub(crate) fn settle_transfer_abort_identity(&self, authority: &crate::lifecycle::Authority, operation_id: i64, item_id: &str) -> Result<bool, StoreError> {
        self.write_store(authority, |inner| {
            Ok(inner.connection.execute("DELETE FROM transfer_abort WHERE operation_id = ?1 AND item_id = ?2", params![operation_id, item_id])? > 0)
        })
    }
}
