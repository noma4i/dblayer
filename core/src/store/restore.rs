use super::{Store, StoreError};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoreFailedRow {
    pub table: String,
    pub key: String,
    pub reason: String,
    pub columns: String,
}

impl Store {
    pub(crate) fn restore_failed(&self) -> Result<Vec<RestoreFailedRow>, StoreError> {
        self.read(&[], |reader| {
            let mut statement = reader.connection().prepare("SELECT source_table, key, reason, columns FROM restore_failed ORDER BY source_table, key")?;
            let rows = statement
                .query_map([], |row| Ok(RestoreFailedRow { table: row.get(0)?, key: row.get(1)?, reason: row.get(2)?, columns: row.get(3)? }))?
                .collect::<rusqlite::Result<_>>()?;
            Ok(rows)
        })
    }

    pub(crate) fn discard_restore_failed(&self, table: &str, key: &str) -> Result<bool, StoreError> {
        if !self.restore_failed()?.iter().any(|row| row.table == table && row.key == key) {
            return Ok(false);
        }
        self.write_ops(vec![super::RowOp::DiscardRestoreFailed { table: table.to_string(), key: key.to_string() }])?;
        Ok(true)
    }
}
