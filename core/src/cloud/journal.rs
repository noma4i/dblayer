use rusqlite::types::Value as SqlValue;
use rusqlite::{params, Connection, OptionalExtension, Result};
use serde_json::Value;

use crate::nodes::{delete_trees, read_trees, texts, write_tree};

use super::{CloudChange, Hlc};

const FIELD_KEYS: [&str; 3] = ["model", "row_id", "field"];

pub fn replica(connection: &Connection) -> Result<String> {
    if let Some(replica) = connection.query_row("SELECT replica FROM cloud_replica WHERE id = 1", [], |row| row.get(0)).optional()? {
        return Ok(replica);
    }
    let replica = uuid::Uuid::new_v4().to_string();
    connection.execute("INSERT INTO cloud_replica (id, replica, clock_ms, clock_count) VALUES (1, ?1, 0, 0)", params![replica])?;
    Ok(replica)
}

pub fn next_stamp(connection: &Connection, now_ms: i64) -> Result<Hlc> {
    let replica = replica(connection)?;
    let (ms, count): (i64, i64) = connection.query_row("SELECT clock_ms, clock_count FROM cloud_replica WHERE id = 1", [], |row| Ok((row.get(0)?, row.get(1)?)))?;
    let (next_ms, next_count) = if now_ms > ms { (now_ms, 0) } else { (ms, count + 1) };
    connection.execute("UPDATE cloud_replica SET clock_ms = ?1, clock_count = ?2 WHERE id = 1", params![next_ms, next_count])?;
    Ok(Hlc::new(next_ms, next_count as u32, &replica))
}

pub fn observe(connection: &Connection, seen: &Hlc) -> Result<()> {
    replica(connection)?;
    connection.execute(
        "UPDATE cloud_replica SET clock_ms = ?1, clock_count = ?2 WHERE id = 1 AND (clock_ms < ?1 OR (clock_ms = ?1 AND clock_count < ?2))",
        params![seen.ms, seen.count],
    )?;
    Ok(())
}

fn stamp_of(row: &rusqlite::Row<'_>, at: usize) -> Result<Hlc> {
    Ok(Hlc { ms: row.get(at)?, count: row.get::<_, i64>(at + 1)? as u32, replica: row.get(at + 2)? })
}

pub fn read_field(connection: &Connection, model: &str, row_id: &str, field: &str) -> Result<Option<(Hlc, Value)>> {
    let stamp = connection
        .query_row("SELECT clock_ms, clock_count, replica FROM cloud_field WHERE model = ?1 AND row_id = ?2 AND field = ?3", params![model, row_id, field], |row| stamp_of(row, 0))
        .optional()?;
    let Some(stamp) = stamp else { return Ok(None) };
    let owner = texts(&[model, row_id, field]);
    let value = read_trees(connection, "cloud_field_node", &FIELD_KEYS, "model = ?1 AND row_id = ?2 AND field = ?3", &owner)?.into_iter().next().map_or(Value::Null, |(_, value)| value);
    Ok(Some((stamp, value)))
}

pub fn row_fields(connection: &Connection, model: &str, row_id: &str) -> Result<Vec<(String, Hlc, Value)>> {
    let mut statement = connection.prepare_cached("SELECT field FROM cloud_field WHERE model = ?1 AND row_id = ?2 ORDER BY field")?;
    let names: Vec<String> = statement.query_map(params![model, row_id], |row| row.get(0))?.collect::<Result<_>>()?;
    let mut found = Vec::new();
    for name in names {
        if let Some((stamp, value)) = read_field(connection, model, row_id, &name)? {
            found.push((name, stamp, value));
        }
    }
    Ok(found)
}

pub fn write_field(connection: &Connection, model: &str, row_id: &str, field: &str, stamp: &Hlc, value: &Value) -> Result<()> {
    let owner = texts(&[model, row_id, field]);
    delete_trees(connection, "cloud_field_node", "model = ?1 AND row_id = ?2 AND field = ?3", &owner)?;
    connection.execute(
        "INSERT INTO cloud_field (model, row_id, field, clock_ms, clock_count, replica) VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
         ON CONFLICT (model, row_id, field) DO UPDATE SET clock_ms = ?4, clock_count = ?5, replica = ?6",
        params![model, row_id, field, stamp.ms, stamp.count, stamp.replica],
    )?;
    write_tree(connection, "cloud_field_node", &FIELD_KEYS, &owner, value)
}

pub fn tombstone(connection: &Connection, model: &str, row_id: &str) -> Result<Option<Hlc>> {
    connection
        .query_row("SELECT clock_ms, clock_count, replica FROM cloud_row WHERE model = ?1 AND row_id = ?2", params![model, row_id], |row| stamp_of(row, 0))
        .optional()
}

pub fn write_tombstone(connection: &Connection, model: &str, row_id: &str, stamp: &Hlc) -> Result<()> {
    connection.execute(
        "INSERT INTO cloud_row (model, row_id, clock_ms, clock_count, replica) VALUES (?1, ?2, ?3, ?4, ?5) \
         ON CONFLICT (model, row_id) DO UPDATE SET clock_ms = ?3, clock_count = ?4, replica = ?5",
        params![model, row_id, stamp.ms, stamp.count, stamp.replica],
    )?;
    delete_trees(connection, "cloud_field_node", "model = ?1 AND row_id = ?2", &texts(&[model, row_id]))?;
    connection.execute("DELETE FROM cloud_field WHERE model = ?1 AND row_id = ?2", params![model, row_id])?;
    Ok(())
}

fn drop_log(connection: &Connection, filter: &str, params: &[SqlValue]) -> Result<()> {
    let mut statement = connection.prepare_cached(&format!("SELECT seq FROM cloud_log WHERE {filter}"))?;
    let seqs: Vec<i64> = statement.query_map(rusqlite::params_from_iter(params.iter()), |row| row.get(0))?.collect::<Result<_>>()?;
    for seq in seqs {
        delete_trees(connection, "cloud_log_node", "seq = ?1", &[SqlValue::Integer(seq)])?;
        connection.execute("DELETE FROM cloud_log WHERE seq = ?1", params![seq])?;
    }
    Ok(())
}

pub fn log_field(connection: &Connection, model: &str, row_id: &str, field: &str, stamp: &Hlc, value: &Value) -> Result<()> {
    drop_log(connection, "model = ?1 AND row_id = ?2 AND field = ?3", &texts(&[model, row_id, field]))?;
    connection.execute(
        "INSERT INTO cloud_log (model, row_id, field, clock_ms, clock_count, replica) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![model, row_id, field, stamp.ms, stamp.count, stamp.replica],
    )?;
    write_tree(connection, "cloud_log_node", &["seq"], &[SqlValue::Integer(connection.last_insert_rowid())], value)
}

pub fn log_delete(connection: &Connection, model: &str, row_id: &str, stamp: &Hlc) -> Result<()> {
    drop_log(connection, "model = ?1 AND row_id = ?2", &texts(&[model, row_id]))?;
    connection.execute(
        "INSERT INTO cloud_log (model, row_id, field, clock_ms, clock_count, replica) VALUES (?1, ?2, '', ?3, ?4, ?5)",
        params![model, row_id, stamp.ms, stamp.count, stamp.replica],
    )?;
    Ok(())
}

pub fn batch(connection: &Connection, limit: u32) -> Result<Vec<(i64, CloudChange)>> {
    let mut statement = connection.prepare_cached("SELECT seq, model, row_id, field, clock_ms, clock_count, replica FROM cloud_log ORDER BY seq LIMIT ?1")?;
    let heads: Vec<(i64, CloudChange)> = statement
        .query_map(params![limit], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                CloudChange { model: row.get(1)?, row_id: row.get(2)?, field: row.get(3)?, clock_ms: row.get(4)?, clock_count: row.get::<_, i64>(5)? as u32, replica: row.get(6)?, value: Value::Null },
            ))
        })?
        .collect::<Result<_>>()?;
    heads
        .into_iter()
        .map(|(seq, mut change)| {
            let tree = read_trees(connection, "cloud_log_node", &["seq"], "seq = ?1", &[SqlValue::Integer(seq)])?.into_iter().next();
            change.value = tree.map_or(Value::Null, |(_, value)| value);
            Ok((seq, change))
        })
        .collect()
}

pub fn ack(connection: &Connection, seqs: &[i64]) -> Result<()> {
    for seq in seqs {
        delete_trees(connection, "cloud_log_node", "seq = ?1", &[SqlValue::Integer(*seq)])?;
        connection.execute("DELETE FROM cloud_log WHERE seq = ?1", params![seq])?;
    }
    Ok(())
}

pub fn pending(connection: &Connection) -> Result<u64> {
    connection.query_row("SELECT COUNT(*) FROM cloud_log", [], |row| row.get::<_, i64>(0)).map(|count| count as u64)
}

pub fn cursor(connection: &Connection) -> Result<Option<Vec<u8>>> {
    let text: Option<String> = connection.query_row("SELECT token FROM cloud_cursor WHERE id = 1", [], |row| row.get(0)).optional()?;
    Ok(text.map(|text| (0..text.len() / 2).filter_map(|at| u8::from_str_radix(&text[at * 2..at * 2 + 2], 16).ok()).collect()))
}

pub fn write_cursor(connection: &Connection, token: &[u8]) -> Result<()> {
    let text: String = token.iter().map(|byte| format!("{byte:02x}")).collect();
    connection.execute("INSERT INTO cloud_cursor (id, token) VALUES (1, ?1) ON CONFLICT (id) DO UPDATE SET token = ?1", params![text])?;
    Ok(())
}
