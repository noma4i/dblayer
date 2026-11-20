//! The rows of `sync_upload_prepare`: one opaque descriptor per transfer.

use std::collections::BTreeMap;

use rusqlite::Connection;

use super::StoreError;

fn id_of(transfer_id: u64) -> Result<i64, StoreError> {
    i64::try_from(transfer_id).map_err(|_| StoreError::Constraint("transfer id is too large".into()))
}

pub(super) fn put_prepare(connection: &Connection, transfer_id: u64, descriptor: &[u8]) -> Result<(), StoreError> {
    connection.execute(include_str!("sql/prepare_rows/put_prepare.sql"), rusqlite::params![id_of(transfer_id)?, descriptor])?;
    Ok(())
}

pub(super) fn remove_prepare_of(connection: &Connection, key: [&str; 3]) -> Result<(), StoreError> {
    connection.execute(include_str!("sql/prepare_rows/remove_prepare_of_key.sql"), key)?;
    Ok(())
}

pub(crate) fn clear_prepare_without_upload(connection: &Connection) -> rusqlite::Result<()> {
    connection.execute(include_str!("sql/prepare_rows/clear_prepare_without_upload.sql"), [])?;
    Ok(())
}

pub(crate) fn read_prepares(connection: &Connection) -> rusqlite::Result<BTreeMap<i64, Vec<u8>>> {
    let mut statement = connection.prepare(include_str!("sql/prepare_rows/read_prepare.sql"))?;
    let rows = statement.query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?)))?;
    rows.collect()
}
