use rusqlite::{params, Connection};
#[cfg(test)]
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QuarantineReason {
    OrphanTempRow,
    ReplaceNormalizeRejected,
    SchemaChanged,
}

impl QuarantineReason {
    pub fn as_str(self) -> &'static str {
        match self {
            QuarantineReason::OrphanTempRow => "orphan-temp-row",
            QuarantineReason::ReplaceNormalizeRejected => "replace-normalize-rejected",
            QuarantineReason::SchemaChanged => "schema-changed",
        }
    }
}

pub fn quarantine_add(connection: &Connection, model: &str, payload: &[u8], reason: QuarantineReason) -> rusqlite::Result<i64> {
    connection.execute("INSERT INTO quarantine (model, payload, reason) VALUES (?, ?, ?)", params![model, payload, reason.as_str()])?;
    Ok(connection.last_insert_rowid())
}

#[cfg(test)]
pub fn quarantine_counts(connection: &Connection) -> rusqlite::Result<HashMap<String, usize>> {
    connection
        .prepare("SELECT reason, COUNT(*) FROM quarantine GROUP BY reason")?
        .query_map([], |row| Ok((row.get(0)?, row.get::<_, i64>(1)? as usize)))?
        .collect()
}
