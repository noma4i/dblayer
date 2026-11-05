use rusqlite::{params, Connection};

use crate::operation::{read_value, write_value};

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

pub(super) fn confirm_item(connection: &Connection, seq: i64, position: i64, item: &str, rows: [&str; 2]) -> Result<(), StoreError> {
    let mut confirmed: Vec<(String, String)> = read_value(connection, seq, "confirmed")?.map(serde_json::from_value).transpose()?.unwrap_or_default();
    confirmed.push((item.to_string(), rows[1].to_string()));
    write_value(connection, seq, "confirmed", Some(&serde_json::to_value(&confirmed)?))?;
    connection.execute("DELETE FROM file WHERE id = (SELECT file_id FROM op_item WHERE seq = ?1 AND position = ?2) AND field LIKE 'op_item.%'", params![seq, position])?;
    for row in rows {
        connection.execute("DELETE FROM op_row WHERE seq = ?1 AND row_id = ?2", params![seq, row])?;
        connection.execute("DELETE FROM op_effect_value WHERE seq = ?1 AND position IN (SELECT position FROM op_effect WHERE seq = ?1 AND row_id = ?2)", params![seq, row])?;
        connection.execute("DELETE FROM op_effect WHERE seq = ?1 AND row_id = ?2", params![seq, row])?;
        connection.execute("UPDATE scope_revision SET operation_seq = NULL WHERE operation_seq = ?1 AND row_id = ?2", params![seq, row])?;
    }
    erase_item(connection, seq, position)?;
    connection.execute("UPDATE op SET transport_started = 0, attempts = 0 WHERE seq = ?1", [seq])?;
    Ok(())
}

impl super::Store {
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

    pub(crate) fn settle_transfer_abort(&self, authority: &crate::lifecycle::authority::Authority, id: i64) -> Result<(), StoreError> {
        self.write_store(authority, |inner| {
            inner.connection.execute("DELETE FROM transfer_abort WHERE id = ?1", [id])?;
            Ok(())
        })
    }
}
