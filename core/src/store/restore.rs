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
            let mut statement = reader.connection().prepare(include_str!("sql/restore/restore_failed.sql"))?;
            let rows = statement
                .query_map([], |row| Ok(RestoreFailedRow { table: row.get(0)?, key: row.get(1)?, reason: row.get(2)?, columns: row.get(3)? }))?
                .collect::<rusqlite::Result<_>>()?;
            Ok(rows)
        })
    }
}
