use super::Database;
use crate::owners::write::operation_status::Operation;

/// The operations still open in the queue of a database, read from the table `op` the way a restart reads them.
pub(super) fn queued(database: &Database) -> Vec<Operation> {
    let inner = database.db.lock();
    crate::store::operation::open_operations(&inner.connection).unwrap()
}

impl Database {
    pub(crate) fn row_sync_states(&self) -> Vec<String> {
        let inner = self.db.lock();
        let mut statement = inner.connection.prepare("SELECT state FROM row_sync").unwrap();
        statement.query_map([], |row| row.get(0)).unwrap().collect::<Result<Vec<_>, _>>().unwrap()
    }

    pub(crate) fn op_statuses(&self) -> Vec<(i64, String)> {
        let inner = self.db.lock();
        let mut statement = inner.connection.prepare("SELECT seq, status FROM op ORDER BY seq").unwrap();
        statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?))).unwrap().collect::<Result<Vec<_>, _>>().unwrap()
    }
}
