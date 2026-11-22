use super::StoreError;
use crate::cloud::{HybridClock, Stamp};
use crate::identity::ListKey;
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use std::collections::{BTreeMap, BTreeSet};

use super::{CommitResult, ModelInfo, Reader, RowChangeKind};

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

pub(crate) fn capture_local(tx: &Transaction<'_>, domain: Option<&str>, models: &BTreeMap<String, ModelInfo>, result: &CommitResult) -> Result<(), StoreError> {
    let (Some(domain), Some(version)) = (domain, result.revision) else { return Ok(()) };
    let reader = Reader::new(tx, models);
    let mut captured = BTreeSet::new();
    for change in &result.rows {
        let Some(info) = models.get(change.row.model.as_str()).filter(|info| info.spec.is_local()) else { continue };
        if change.row.id.is_temp() || captured.contains(&change.row) { continue; }
        let deleted = change.kind == RowChangeKind::Destroyed;
        let eligible = if deleted {
            reader.tombstone_seq(change.row.model.as_str(), change.row.id.as_str())? == Some(version)
        } else {
            matches!(change.kind, RowChangeKind::Inserted | RowChangeKind::Updated) && info.spec.fields.iter().any(|field| change.fields.contains(&field.name) && !field.derived && !matches!(field.kind, crate::schema::FieldKind::File))
        };
        if eligible {
            stage(tx, domain, &PendingRecord { model: change.row.model.to_string(), row_id: change.row.id.to_string(), version, deleted })?;
            captured.insert(change.row.clone());
        }
    }
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
    tx.execute(include_str!("../sql/cloud_reset_clock.sql"), [domain])?;
    tx.execute(include_str!("../sql/cloud_reset_fields.sql"), [domain])?;
    Ok(())
}

fn read_stamp(row: &rusqlite::Row<'_>) -> rusqlite::Result<Stamp> {
    let millis: Vec<u8> = row.get(0)?;
    let replica: Vec<u8> = row.get(2)?;
    let invalid = |index| rusqlite::Error::InvalidColumnType(index, "stamp".into(), rusqlite::types::Type::Blob);
    Ok(Stamp {
        millis: u64::from_be_bytes(millis.try_into().map_err(|_| invalid(0))?),
        counter: row.get(1)?,
        replica: replica.try_into().map_err(|_| invalid(2))?,
    })
}

fn clock_stamp(connection: &Connection, domain: &str) -> Result<Option<Stamp>, StoreError> {
    Ok(connection.query_row(include_str!("../sql/cloud_read_clock.sql"), [domain], read_stamp).optional()?)
}

fn write_clock(tx: &Transaction<'_>, domain: &str, stamp: Stamp) -> Result<(), StoreError> {
    tx.execute(include_str!("../sql/cloud_write_clock.sql"), params![domain, stamp.millis.to_be_bytes().as_slice(), stamp.counter, stamp.replica.as_slice()])?;
    Ok(())
}

fn load_clock(connection: &Connection, domain: &str) -> Result<HybridClock, StoreError> {
    let last = clock_stamp(connection, domain)?;
    let replica = last.map_or_else(|| *uuid::Uuid::new_v4().as_bytes(), |stamp| stamp.replica);
    Ok(HybridClock::new(replica, last))
}

pub(crate) fn tick_stamp(tx: &Transaction<'_>, domain: &str, wall_millis: u64) -> Result<Stamp, StoreError> {
    let stamp = load_clock(tx, domain)?.tick(wall_millis).ok_or_else(|| StoreError::Invalid("cloud clock overflow".into()))?;
    write_clock(tx, domain, stamp)?;
    Ok(stamp)
}

pub(crate) fn observe_stamp(tx: &Transaction<'_>, domain: &str, stamp: Stamp) -> Result<(), StoreError> {
    let observed = load_clock(tx, domain)?.observe(stamp);
    write_clock(tx, domain, observed)
}

pub(crate) fn write_field_stamp(tx: &Transaction<'_>, domain: &str, model: &str, row_id: &str, path: &[String], stamp: Stamp) -> Result<(), StoreError> {
    let path = ListKey::compose(path);
    tx.execute(include_str!("../sql/cloud_write_field.sql"), params![domain, model, row_id, path.as_str(), stamp.millis.to_be_bytes().as_slice(), stamp.counter, stamp.replica.as_slice()])?;
    Ok(())
}

pub(crate) fn field_stamp(connection: &Connection, domain: &str, model: &str, row_id: &str, path: &[String]) -> Result<Option<Stamp>, StoreError> {
    let path = ListKey::compose(path);
    Ok(connection.query_row(include_str!("../sql/cloud_read_field.sql"), params![domain, model, row_id, path.as_str()], read_stamp).optional()?)
}
