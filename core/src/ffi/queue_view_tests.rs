use super::Database;
use crate::owners::write::operation_status::Operation;

/// The operations still open in the queue of a database, read from the table `op` the way a restart reads them.
pub(super) fn queued(database: &Database) -> Vec<Operation> {
    let inner = database.db.lock();
    crate::store::operation::open_operations(&inner.connection).unwrap()
}
