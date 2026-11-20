#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoreFailedRow {
    pub table: String,
    pub key: String,
    pub reason: String,
    pub columns: String,
}

impl RestoreFailedRow {
    pub(crate) fn transfer_id(&self) -> Option<u64> {
        (self.table == "sync_upload").then(|| serde_json::from_str::<serde_json::Value>(&self.columns).ok()?.get("transfer_id")?.as_u64()).flatten()
    }
}

pub(super) const SAVED_PREPARE: &str = "sync_upload_prepare";
pub(super) const SAVED_PREPARE_CHILD: &str = "sync_upload_prepare_step";

impl RestoreFailedRow {
    /// `Err` for an element whose key cannot be read: it is never skipped.
    pub(super) fn saved_prepare_transfer(&self) -> Option<Result<u64, String>> {
        if self.table != SAVED_PREPARE && self.table != SAVED_PREPARE_CHILD { return None; }
        let unreadable = || format!("the saved prepare element {} of {} is unreadable", self.key, self.table);
        let columns: serde_json::Value = match serde_json::from_str(&self.columns) { Ok(columns) => columns, Err(_) => return Some(Err(unreadable())) };
        Some(columns.get("transfer_id").and_then(serde_json::Value::as_u64).ok_or_else(unreadable))
    }
}

pub(super) fn read_failed(connection: &rusqlite::Connection) -> rusqlite::Result<Vec<RestoreFailedRow>> {
    let mut statement = connection.prepare(include_str!("sql/restore/restore_failed.sql"))?;
    let rows = statement.query_map([], |row| Ok(RestoreFailedRow { table: row.get(0)?, key: row.get(1)?, reason: row.get(2)?, columns: row.get(3)? }))?.collect();
    rows
}
