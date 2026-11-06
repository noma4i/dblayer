use rusqlite::types::Value as SqlValue;
use rusqlite::{params, Connection};
use serde_json::Value;

use crate::store::nodes::write_tree;
#[cfg(test)]
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QuarantineReason {
    OrphanTempRow,
    ReplaceNormalizeRejected,
    SchemaChanged,
    CorruptResetIntent,
}

impl QuarantineReason {
    pub fn as_str(self) -> &'static str {
        match self {
            QuarantineReason::OrphanTempRow => "orphan-temp-row",
            QuarantineReason::ReplaceNormalizeRejected => "replace-normalize-rejected",
            QuarantineReason::SchemaChanged => "schema-changed",
            QuarantineReason::CorruptResetIntent => "corrupt-reset-intent",
        }
    }
}

pub fn quarantine_add(connection: &Connection, model: Option<&str>, payload: &Value, reason: QuarantineReason) -> rusqlite::Result<i64> {
    connection.execute(include_str!("../sql/operation/quarantine_add.sql"), params![model, reason.as_str()])?;
    let seq = connection.last_insert_rowid();
    write_tree(connection, "quarantine_value", &["seq"], &[SqlValue::Integer(seq)], payload)?;
    Ok(seq)
}

#[cfg(test)]
pub fn quarantine_counts(connection: &Connection) -> rusqlite::Result<HashMap<String, usize>> {
    connection
        .prepare(include_str!("../sql/operation/quarantine_counts.sql"))?
        .query_map([], |row| Ok((row.get(0)?, row.get::<_, i64>(1)? as usize)))?
        .collect()
}
