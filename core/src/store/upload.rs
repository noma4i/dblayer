use rusqlite::{params, Connection};

use crate::operation::{read_value, write_value};

use super::StoreError;

pub(super) fn remove_item(connection: &Connection, seq: i64, position: i64) -> Result<(), StoreError> {
    crate::operation::record_transfer_aborts(connection, seq, Some(position))?;
    connection.execute(include_str!("sql/upload/remove_item.sql"), params![seq, position])?;
    erase_item(connection, seq, position)
}

fn erase_item(connection: &Connection, seq: i64, position: i64) -> Result<(), StoreError> {
    for table in ["op_value", "op_error", "op_part"] {
        connection.execute(&format!(include_str!("sql/upload/erase_item_delete_table.sql"), table = table), params![seq, position])?;
    }
    for table in ["op_item_header", "op_item_retry", "op_item"] {
        connection.execute(&format!(include_str!("sql/upload/erase_item_delete_table_2.sql"), table = table), params![seq, position])?;
    }
    Ok(())
}

pub(super) fn confirm_item(connection: &Connection, seq: i64, position: i64, item: &str, rows: [&str; 2]) -> Result<(), StoreError> {
    let mut confirmed: Vec<(String, String)> = read_value(connection, seq, "confirmed")?.map(serde_json::from_value).transpose()?.unwrap_or_default();
    confirmed.push((item.to_string(), rows[1].to_string()));
    write_value(connection, seq, "confirmed", Some(&serde_json::to_value(&confirmed)?))?;
    connection.execute(include_str!("sql/upload/remove_item.sql"), params![seq, position])?;
    for row in rows {
        connection.execute(include_str!("sql/upload/confirm_item_delete_op_row.sql"), params![seq, row])?;
        connection.execute(include_str!("sql/upload/confirm_item_delete_op_effect_value.sql"), params![seq, row])?;
        connection.execute(include_str!("sql/upload/confirm_item_delete_op_effect.sql"), params![seq, row])?;
        connection.execute(include_str!("sql/upload/confirm_item_update_scope_revision.sql"), params![seq, row])?;
    }
    erase_item(connection, seq, position)?;
    connection.execute(include_str!("sql/upload/confirm_item_update_op.sql"), [seq])?;
    Ok(())
}
