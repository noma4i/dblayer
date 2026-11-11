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

pub(super) fn read_failed(connection: &rusqlite::Connection) -> rusqlite::Result<Vec<RestoreFailedRow>> {
    let mut statement = connection.prepare(include_str!("sql/restore/restore_failed.sql"))?;
    let rows = statement.query_map([], |row| Ok(RestoreFailedRow { table: row.get(0)?, key: row.get(1)?, reason: row.get(2)?, columns: row.get(3)? }))?.collect();
    rows
}
