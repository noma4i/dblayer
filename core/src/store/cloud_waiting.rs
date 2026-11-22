use rusqlite::{params, Connection, Transaction};
use rusqlite::types::Value as SqlValue;
use sha2::{Digest, Sha256};

use crate::cloud::{CloudField, CloudRecord, CloudStamp, StampedValue};
use super::{nodes, StoreError};

#[derive(Debug)]
pub(crate) struct WaitingEntry {
    pub seq: i64,
    pub record: CloudRecord,
    pub system_fields: Option<Vec<u8>>,
}

pub(crate) fn enqueue(tx: &Transaction<'_>, domain: &str, record: &CloudRecord, system_fields: Option<&[u8]>, dependencies: &[(&str, &str)]) -> Result<(), StoreError> {
    let bytes = crate::cloud::encode(record).map_err(|_| StoreError::Invalid("invalid cloud record".into()))?;
    let digest = Sha256::digest(bytes);
    let inserted = tx.execute("INSERT OR IGNORE INTO cloud_waiting(domain, digest, model, row_id, server_revision, deleted, system_fields) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)", params![domain, digest.as_slice(), record.model, record.row_id, record.server_revision, record.deleted, system_fields])?;
    let seq: i64 = tx.query_row("SELECT seq FROM cloud_waiting WHERE domain = ?1 AND digest = ?2", params![domain, digest.as_slice()], |row| row.get(0))?;
    if inserted == 0 {
        tx.execute("UPDATE cloud_waiting SET system_fields = ?1 WHERE seq = ?2", params![system_fields, seq])?;
        tx.execute("DELETE FROM cloud_waiting_dependency WHERE domain = ?1 AND seq = ?2", params![domain, seq])?;
    }
    if inserted != 0 {
        for (field, value) in record.fields.iter().enumerate() {
            let field = field as i64;
            tx.prepare_cached("INSERT INTO cloud_waiting_field VALUES (?1, ?2, ?3, ?4, ?5)")?.execute(params![seq, field, value.data.stamp.millis.to_be_bytes().as_slice(), value.data.stamp.counter, value.data.stamp.replica.as_slice()])?;
            for (position, segment) in value.path.iter().enumerate() {
                tx.prepare_cached("INSERT INTO cloud_waiting_path VALUES (?1, ?2, ?3, ?4)")?.execute(params![seq, field, position as i64, segment])?;
            }
            nodes::write_tree(tx, "cloud_waiting_value", &["seq", "field"], &[SqlValue::Integer(seq), SqlValue::Integer(field)], &value.data.value)?;
        }
    }
    for (model, row_id) in dependencies {
        tx.prepare_cached("INSERT OR IGNORE INTO cloud_waiting_dependency VALUES (?1, ?2, ?3, ?4)")?.execute(params![domain, model, row_id, seq])?;
    }
    Ok(())
}

pub(crate) fn entries(connection: &Connection, domain: &str, model: &str, row_id: &str) -> Result<Vec<WaitingEntry>, StoreError> {
    let mut query = connection.prepare("SELECT w.seq, w.model, w.row_id, w.server_revision, w.deleted, w.system_fields FROM cloud_waiting w JOIN cloud_waiting_dependency d ON d.seq = w.seq AND d.domain = w.domain WHERE d.domain = ?1 AND d.model = ?2 AND d.row_id = ?3 ORDER BY w.seq")?;
    let rows = query.query_map(params![domain, model, row_id], |row| Ok(WaitingEntry {
        seq: row.get(0)?, record: CloudRecord { model: row.get(1)?, row_id: row.get(2)?, server_revision: row.get(3)?, deleted: row.get(4)?, fields: Vec::new() }, system_fields: row.get(5)?,
    }))?;
    let mut entries = rows.collect::<Result<Vec<_>, _>>()?;
    read_fields(connection, &mut entries)?;
    Ok(entries)
}

fn read_fields(connection: &Connection, entries: &mut [WaitingEntry]) -> Result<(), StoreError> {
    for entry in entries {
        let mut fields = connection.prepare_cached("SELECT field, millis, counter, replica FROM cloud_waiting_field WHERE seq = ?1 ORDER BY field")?;
        let stamps = fields.query_map([entry.seq], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?, row.get::<_, u32>(2)?, row.get::<_, Vec<u8>>(3)?)))?;
        for stamp in stamps {
            let (field, millis, counter, replica) = stamp?;
            let invalid = || StoreError::Invalid("invalid waiting cloud stamp".into());
            let stamp = CloudStamp { millis: u64::from_be_bytes(millis.try_into().map_err(|_| invalid())?), counter, replica: replica.try_into().map_err(|_| invalid())? };
            let mut path = connection.prepare_cached("SELECT segment FROM cloud_waiting_path WHERE seq = ?1 AND field = ?2 ORDER BY position")?;
            let path = path.query_map(params![entry.seq, field], |row| row.get(0))?.collect::<Result<Vec<String>, _>>()?;
            let values = nodes::read_trees(connection, "cloud_waiting_value", &["seq", "field"], "seq = ?1 AND field = ?2", &[SqlValue::Integer(entry.seq), SqlValue::Integer(field)])?;
            let value = values.into_iter().next().ok_or_else(|| StoreError::Invalid("missing waiting cloud value".into()))?.1;
            entry.record.fields.push(CloudField { path, data: StampedValue { value, stamp } });
        }
    }
    Ok(())
}

pub(crate) fn next_ready(tx: &Transaction<'_>, domain: &str) -> Result<Option<WaitingEntry>, StoreError> {
    use rusqlite::OptionalExtension;
    let entry = tx.query_row("SELECT w.seq, w.model, w.row_id, w.server_revision, w.deleted, w.system_fields FROM cloud_waiting_ready r JOIN cloud_waiting w ON w.domain = r.domain AND w.seq = r.seq WHERE r.domain = ?1 ORDER BY r.seq LIMIT 1", [domain], |row| Ok(WaitingEntry {
        seq: row.get(0)?, record: CloudRecord { model: row.get(1)?, row_id: row.get(2)?, server_revision: row.get(3)?, deleted: row.get(4)?, fields: Vec::new() }, system_fields: row.get(5)?,
    })).optional()?;
    let Some(mut entry) = entry else { return Ok(None) };
    read_fields(tx, std::slice::from_mut(&mut entry))?;
    Ok(Some(entry))
}

pub(crate) fn mark_all(tx: &Transaction<'_>, domain: &str) -> Result<(), StoreError> {
    tx.execute("INSERT OR IGNORE INTO cloud_waiting_ready SELECT domain, seq FROM cloud_waiting WHERE domain = ?1", [domain])?;
    Ok(())
}

pub(crate) fn mark_changed(tx: &Transaction<'_>, domain: Option<&str>, result: &super::CommitResult) -> Result<(), StoreError> {
    let Some(domain) = domain else { return Ok(()) };
    for change in &result.rows {
        tx.prepare_cached("INSERT OR IGNORE INTO cloud_waiting_ready SELECT domain, seq FROM cloud_waiting_dependency WHERE domain = ?1 AND model = ?2 AND row_id = ?3")?.execute(params![domain, change.row.model.as_str(), change.row.id.as_str()])?;
    }
    Ok(())
}

pub(crate) fn remove(tx: &Transaction<'_>, domain: &str, seq: i64) -> Result<(), StoreError> {
    let filter = "seq IN (SELECT seq FROM cloud_waiting WHERE domain = ?1 AND seq = ?2)";
    let args = params![domain, seq];
    for table in ["cloud_waiting_value", "cloud_waiting_path", "cloud_waiting_field", "cloud_waiting_dependency", "cloud_waiting_ready"] {
        tx.prepare_cached(&format!("DELETE FROM {table} WHERE {filter}"))?.execute(args)?;
    }
    tx.execute("DELETE FROM cloud_waiting WHERE domain = ?1 AND seq = ?2", args)?;
    Ok(())
}

pub(crate) fn reset(tx: &Transaction<'_>, domain: &str) -> Result<(), StoreError> {
    let mut statement = tx.prepare("SELECT seq FROM cloud_waiting WHERE domain = ?1")?;
    let sequences = statement.query_map([domain], |row| row.get::<_, i64>(0))?.collect::<Result<Vec<_>, _>>()?;
    for seq in sequences { remove(tx, domain, seq)?; }
    Ok(())
}
