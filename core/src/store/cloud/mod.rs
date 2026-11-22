use super::StoreError;
use crate::cloud::{CloudStamp, HybridClock};
use crate::identity::ListKey;
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use std::collections::{BTreeMap, BTreeSet};

use super::{CommitResult, ModelInfo, Reader, RowChangeKind};
pub(super) mod project;
mod status;

#[derive(Debug)]
pub(crate) struct PendingCloudRecord {
    pub key: String,
    pub model: String,
    pub row_id: String,
    pub version: i64,
    pub system_fields: Option<Vec<u8>>,
    pub payload: Result<Vec<u8>, crate::cloud::RecordError>,
}

impl super::Store {
    pub(crate) fn bootstrap_cloud_chunk(&self, scope: &crate::owners::cloud::CloudScope) -> Result<bool, StoreError> {
        let inner = self.lock();
        inner.require_cloud_scope(scope)?;
        let tx = inner.connection.unchecked_transaction()?;
        let domain = &scope.domain;
        let checkpoint: Option<(String, String, bool)> = tx.query_row(
            "SELECT model, row_id, done FROM cloud_bootstrap WHERE domain = ?1", [domain],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        ).optional()?;
        if checkpoint.as_ref().is_some_and(|(_, _, done)| *done) { return Ok(true); }
        let mut cursor = checkpoint.map(|(model, id, _)| (model, id)).unwrap_or_default();
        let reader = Reader::new(&tx, &inner.models);
        let mut count = 0;
        for (model, info) in inner.models.iter().filter(|(_, info)| !info.spec.is_memory()) {
            if model < &cursor.0 { continue; }
            let after = if model == &cursor.0 { cursor.1.as_str() } else { "" };
            let sql = format!("SELECT id FROM {} WHERE id > ?1 ORDER BY id LIMIT ?2", info.table);
            let ids = tx.prepare(&sql)?.query_map(params![after, 128 - count], |row| row.get::<_, String>(0))?.collect::<Result<Vec<_>, _>>()?;
            for id in ids {
                cursor = (model.clone(), id.clone());
                count += 1;
                if crate::identity::RowId::new(&id).map_err(|error| StoreError::Invalid(error.to_string()))?.is_temp() { continue; }
                if !info.spec.is_local() && has_open_operation(&tx, model, &id)? { continue; }
                let Some(row) = reader.stored_row(model, &id)? else { continue };
                let tracked: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM cloud_record WHERE domain = ?1 AND model = ?2 AND row_id = ?3)", params![domain, model, id], |row| row.get(0))?;
                let mut missing = Vec::new();
                for (path, _) in project::project(&info.spec, &row.row) {
                    if field_stamp(&tx, domain, model, &id, &path)?.is_none() { missing.push(path); }
                }
                if !missing.is_empty() {
                    let stamp = match clock_stamp(&tx, domain)? {
                        Some(stamp) => CloudStamp { millis: 0, counter: 0, replica: stamp.replica },
                        None => {
                            let stamp = CloudStamp { millis: 0, counter: 0, replica: *uuid::Uuid::new_v4().as_bytes() };
                            write_clock(&tx, domain, stamp)?;
                            stamp
                        }
                    };
                    for path in &missing { write_field_stamp(&tx, domain, model, &id, path, stamp)?; }
                }
                if !missing.is_empty() || !tracked {
                    stage(&tx, domain, &PendingRecord { model: model.clone(), row_id: id, version: row.revision, deleted: false })?;
                }
            }
            if count == 128 { break; }
        }
        let done = count < 128;
        tx.execute("INSERT INTO cloud_bootstrap (domain, model, row_id, done) VALUES (?1, ?2, ?3, ?4) ON CONFLICT(domain) DO UPDATE SET model = excluded.model, row_id = excluded.row_id, done = excluded.done", params![domain, cursor.0, cursor.1, done])?;
        tx.commit()?;
        Ok(done)
    }

    pub(crate) fn pending_cloud_records(&self, scope: &crate::owners::cloud::CloudScope, limit: u32) -> Result<Vec<PendingCloudRecord>, StoreError> {
        self.lock().require_cloud_scope(scope)?;
        let domain = scope.domain.as_str();
        self.read(&[], |reader| {
            let mut records = Vec::new();
            for (pending, system_fields) in pending_with_fields(reader.connection(), domain, limit)? {
                let key = crate::cloud::record_key(&pending.model, &pending.row_id);
                let mut record = crate::cloud::CloudRecord { model: pending.model, row_id: pending.row_id, server_revision: None, deleted: pending.deleted, fields: Vec::new() };
                if !record.deleted {
                    let info = reader.models().get(&record.model).ok_or_else(|| StoreError::UnknownModel(record.model.clone()))?;
                    let row = reader.stored_row(&record.model, &record.row_id)?.ok_or_else(|| StoreError::Constraint("pending cloud row is missing".into()))?;
                    record.server_revision = info.spec.server_revision.as_ref().and_then(|field| row.row.get(field)).and_then(serde_json::Value::as_i64);
                    for (path, value) in project::project(&info.spec, &row.row) {
                        if let Some(stamp) = field_stamp(reader.connection(), domain, &record.model, &record.row_id, &path)? {
                            record.fields.push(crate::cloud::CloudField { path, data: crate::cloud::StampedValue { value, stamp } });
                        }
                    }
                }
                records.push(PendingCloudRecord { key, model: record.model.clone(), row_id: record.row_id.clone(), version: pending.version, system_fields, payload: crate::cloud::encode(&record) });
            }
            Ok(records)
        })
    }

    pub(crate) fn cloud_resume(&self, scope: &crate::owners::cloud::CloudScope) -> Result<Option<Vec<u8>>, StoreError> {
        let inner = self.lock();
        inner.require_cloud_scope(scope)?;
        progress(&inner.connection, &scope.domain)
    }

    pub(crate) fn confirm_cloud_send(&self, scope: &crate::owners::cloud::CloudScope, record: &PendingCloudRecord, system_fields: Option<&[u8]>) -> Result<(), StoreError> {
        let inner = self.lock();
        inner.require_cloud_scope(scope)?;
        let tx = inner.connection.unchecked_transaction()?;
        let pending = PendingRecord { model: record.model.clone(), row_id: record.row_id.clone(), version: record.version, deleted: false };
        acknowledge(&tx, &scope.domain, &pending, system_fields)?;
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn block_cloud_send(&self, scope: &crate::owners::cloud::CloudScope, record: &PendingCloudRecord) -> Result<(), StoreError> {
        let inner = self.lock();
        inner.require_cloud_scope(scope)?;
        inner.connection.execute(include_str!("../sql/cloud_block_send.sql"), params![scope.domain, record.model, record.row_id, record.version])?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingRecord {
    pub model: String,
    pub row_id: String,
    pub version: i64,
    pub deleted: bool,
}

pub(crate) fn enable(tx: &Transaction<'_>) -> Result<(), StoreError> {
    tx.execute_batch(include_str!("../sql/cloud_enable.sql"))?;
    tx.execute_batch(&super::nodes::node_table_statement("", "cloud_waiting_value", &[("seq", "INTEGER"), ("field", "INTEGER")]))?;
    Ok(())
}

pub(crate) fn capture_local(tx: &Transaction<'_>, domain: Option<&str>, models: &BTreeMap<String, ModelInfo>, originals: &BTreeMap<crate::identity::RowRef, Option<serde_json::Map<String, serde_json::Value>>>, result: &CommitResult, now: i64) -> Result<(), StoreError> {
    let (Some(domain), Some(version)) = (domain, result.revision) else { return Ok(()) };
    let reader = Reader::new(tx, models);
    let mut captured = BTreeSet::new();
    let server_removed: BTreeSet<_> = result.server_removed.iter().collect();
    for change in &result.rows {
        let Some(info) = models.get(change.row.model.as_str()).filter(|info| !info.spec.is_memory()) else { continue };
        if change.row.id.is_temp() || captured.contains(&change.row) { continue; }
        if !info.spec.is_local() && (server_removed.contains(&change.row) || has_open_operation(tx, change.row.model.as_str(), change.row.id.as_str())?) { continue; }
        let confirmed = change.kind == RowChangeKind::Synced && !info.spec.is_local();
        let deleted = change.kind == RowChangeKind::Destroyed || (confirmed && reader.tombstone_seq(change.row.model.as_str(), change.row.id.as_str())? == Some(version));
        if deleted && !info.spec.is_local() && !confirmed { continue; }
        let eligible = if deleted {
            reader.tombstone_seq(change.row.model.as_str(), change.row.id.as_str())? == Some(version)
        } else {
            if !matches!(change.kind, RowChangeKind::Inserted | RowChangeKind::Updated) && !confirmed { continue; }
            let before = originals.get(&change.row).and_then(Option::as_ref).map(|row| project::project(&info.spec, row)).unwrap_or_default();
            let Some(after) = reader.stored_row(change.row.model.as_str(), change.row.id.as_str())? else { continue };
            let changed: Vec<_> = project::project(&info.spec, &after.row).into_iter().filter(|(path, value)| {
                if confirmed { after.field_rev.get(&path[0]) == Some(&version) } else { before.get(path) != Some(value) }
            }).collect();
            if changed.is_empty() && change.kind != RowChangeKind::Inserted { continue; }
            if !changed.is_empty() {
                let stamp = tick_stamp(tx, domain, u64::try_from(now).unwrap_or(0))?;
                for (path, _) in changed { write_field_stamp(tx, domain, change.row.model.as_str(), change.row.id.as_str(), &path, stamp)?; }
            }
            true
        };
        if eligible {
            stage(tx, domain, &PendingRecord { model: change.row.model.to_string(), row_id: change.row.id.to_string(), version, deleted })?;
            captured.insert(change.row.clone());
        }
    }
    for row in &result.server_removed {
        if !models.get(row.model.as_str()).is_some_and(|info| !info.spec.is_memory() && !info.spec.is_local()) { continue; }
        if row.id.is_temp() || captured.contains(row) || has_open_operation(tx, row.model.as_str(), row.id.as_str())? { continue; }
        stage(tx, domain, &PendingRecord { model: row.model.to_string(), row_id: row.id.to_string(), version, deleted: true })?;
        captured.insert(row.clone());
    }
    Ok(())
}

fn operation_row_sql(model: &str, row_id: &str) -> String {
    format!(include_str!("../sql/operation/row_operation_sequences.sql"), OPEN_STATUSES = super::operation::OPEN_STATUSES)
        .replace("?1", model).replace("?2", row_id)
}

fn has_open_operation(connection: &Connection, model: &str, row_id: &str) -> Result<bool, StoreError> {
    let sql = format!("SELECT EXISTS({})", operation_row_sql("?1", "?2"));
    Ok(connection.query_row(&sql, params![model, row_id], |row| row.get(0))?)
}

pub(crate) fn stage(tx: &Transaction<'_>, domain: &str, record: &PendingRecord) -> Result<(), StoreError> {
    tx.execute(include_str!("../sql/cloud_stage.sql"), params![domain, record.model, record.row_id, record.version, record.deleted])?;
    Ok(())
}

#[cfg(test)]
pub(crate) fn pending(connection: &Connection, domain: &str, limit: u32) -> Result<Vec<PendingRecord>, StoreError> {
    Ok(pending_with_fields(connection, domain, limit)?.into_iter().map(|(record, _)| record).collect())
}

fn pending_with_fields(connection: &Connection, domain: &str, limit: u32) -> Result<Vec<(PendingRecord, Option<Vec<u8>>)>, StoreError> {
    let sql = format!(include_str!("../sql/cloud_pending.sql"), OPEN_OPERATIONS = operation_row_sql("cloud_record.model", "cloud_record.row_id"));
    let mut statement = connection.prepare_cached(&sql)?;
    let rows = statement.query_map(params![domain, limit], |row| {
        Ok((PendingRecord { model: row.get(0)?, row_id: row.get(1)?, version: row.get(2)?, deleted: row.get(3)? }, row.get(4)?))
    })?;
    Ok(rows.collect::<Result<Vec<_>, _>>()?)
}

pub(crate) fn acknowledge(tx: &Transaction<'_>, domain: &str, record: &PendingRecord, system_fields: Option<&[u8]>) -> Result<(), StoreError> {
    tx.execute(include_str!("../sql/cloud_acknowledge.sql"), params![domain, record.model, record.row_id, record.version, system_fields])?;
    Ok(())
}

pub(super) fn write_system_fields(tx: &Transaction<'_>, domain: &str, model: &str, row_id: &str, bytes: Option<&[u8]>) -> Result<(), StoreError> {
    tx.execute(include_str!("../sql/cloud_write_system_fields.sql"), params![domain, model, row_id, bytes])?;
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
    super::cloud_waiting::reset(tx, domain)?;
    tx.execute("DELETE FROM cloud_bootstrap WHERE domain = ?1", [domain])?;
    tx.execute(include_str!("../sql/cloud_reset_records.sql"), [domain])?;
    tx.execute(include_str!("../sql/cloud_reset_progress.sql"), [domain])?;
    tx.execute(include_str!("../sql/cloud_reset_clock.sql"), [domain])?;
    tx.execute(include_str!("../sql/cloud_reset_fields.sql"), [domain])?;
    Ok(())
}

fn read_stamp(row: &rusqlite::Row<'_>) -> rusqlite::Result<CloudStamp> {
    let millis: Vec<u8> = row.get(0)?;
    let replica: Vec<u8> = row.get(2)?;
    let invalid = |index| rusqlite::Error::InvalidColumnType(index, "stamp".into(), rusqlite::types::Type::Blob);
    Ok(CloudStamp {
        millis: u64::from_be_bytes(millis.try_into().map_err(|_| invalid(0))?),
        counter: row.get(1)?,
        replica: replica.try_into().map_err(|_| invalid(2))?,
    })
}

fn clock_stamp(connection: &Connection, domain: &str) -> Result<Option<CloudStamp>, StoreError> {
    Ok(connection.query_row(include_str!("../sql/cloud_read_clock.sql"), [domain], read_stamp).optional()?)
}

fn write_clock(tx: &Transaction<'_>, domain: &str, stamp: CloudStamp) -> Result<(), StoreError> {
    tx.execute(include_str!("../sql/cloud_write_clock.sql"), params![domain, stamp.millis.to_be_bytes().as_slice(), stamp.counter, stamp.replica.as_slice()])?;
    Ok(())
}

fn load_clock(connection: &Connection, domain: &str) -> Result<HybridClock, StoreError> {
    let last = clock_stamp(connection, domain)?;
    let replica = last.map_or_else(|| *uuid::Uuid::new_v4().as_bytes(), |stamp| stamp.replica);
    Ok(HybridClock::new(replica, last))
}

pub(crate) fn tick_stamp(tx: &Transaction<'_>, domain: &str, wall_millis: u64) -> Result<CloudStamp, StoreError> {
    let stamp = load_clock(tx, domain)?.tick(wall_millis).ok_or_else(|| StoreError::Invalid("cloud clock overflow".into()))?;
    write_clock(tx, domain, stamp)?;
    Ok(stamp)
}

pub(crate) fn observe_stamp(tx: &Transaction<'_>, domain: &str, stamp: CloudStamp) -> Result<(), StoreError> {
    let observed = load_clock(tx, domain)?.observe(stamp);
    write_clock(tx, domain, observed)
}

pub(crate) fn write_field_stamp(tx: &Transaction<'_>, domain: &str, model: &str, row_id: &str, path: &[String], stamp: CloudStamp) -> Result<(), StoreError> {
    let path = ListKey::compose(path);
    tx.execute(include_str!("../sql/cloud_write_field.sql"), params![domain, model, row_id, path.as_str(), stamp.millis.to_be_bytes().as_slice(), stamp.counter, stamp.replica.as_slice()])?;
    Ok(())
}

pub(crate) fn field_stamp(connection: &Connection, domain: &str, model: &str, row_id: &str, path: &[String]) -> Result<Option<CloudStamp>, StoreError> {
    let path = ListKey::compose(path);
    Ok(connection.query_row(include_str!("../sql/cloud_read_field.sql"), params![domain, model, row_id, path.as_str()], read_stamp).optional()?)
}
