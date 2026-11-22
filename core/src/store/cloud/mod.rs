use super::StoreError;
use rusqlite::{Connection, OptionalExtension, Transaction, params};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingRecord {
    pub model: String,
    pub row_id: String,
    pub version: i64,
    pub deleted: bool,
}

pub(crate) fn enable(tx: &Transaction<'_>) -> Result<(), StoreError> {
    tx.execute_batch(include_str!("../sql/cloud_enable.sql"))?;
    Ok(())
}

pub(crate) fn stage(tx: &Transaction<'_>, domain: &str, record: &PendingRecord) -> Result<(), StoreError> {
    tx.execute(include_str!("../sql/cloud_stage.sql"), params![domain, record.model, record.row_id, record.version, record.deleted])?;
    Ok(())
}

pub(crate) fn pending(connection: &Connection, domain: &str, limit: u32) -> Result<Vec<PendingRecord>, StoreError> {
    let mut statement = connection.prepare(include_str!("../sql/cloud_pending.sql"))?;
    let rows = statement.query_map(params![domain, limit], |row| {
        Ok(PendingRecord { model: row.get(0)?, row_id: row.get(1)?, version: row.get(2)?, deleted: row.get(3)? })
    })?;
    Ok(rows.collect::<Result<Vec<_>, _>>()?)
}

pub(crate) fn acknowledge(tx: &Transaction<'_>, domain: &str, record: &PendingRecord) -> Result<(), StoreError> {
    tx.execute(include_str!("../sql/cloud_acknowledge.sql"), params![domain, record.model, record.row_id, record.version])?;
    Ok(())
}

pub(crate) fn write_progress(tx: &Transaction<'_>, domain: &str, bytes: &[u8]) -> Result<(), StoreError> {
    tx.execute(include_str!("../sql/cloud_write_progress.sql"), params![domain, bytes])?;
    Ok(())
}

pub(crate) fn progress(connection: &Connection, domain: &str) -> Result<Option<Vec<u8>>, StoreError> {
    Ok(connection.query_row(include_str!("../sql/cloud_progress.sql"), [domain], |row| row.get(0)).optional()?)
}

pub(crate) fn reset(tx: &Transaction<'_>, domain: &str) -> Result<(), StoreError> {
    tx.execute(include_str!("../sql/cloud_reset_records.sql"), [domain])?;
    tx.execute(include_str!("../sql/cloud_reset_progress.sql"), [domain])?;
    Ok(())
}
