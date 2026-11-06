use super::Database;
use crate::operation::Operation;

/// The operations still open in the queue of a database, read from the table `op` the way a restart reads them.
pub(super) fn queued(database: &Database) -> Vec<Operation> {
    let inner = database.db.shared.store.lock();
    crate::operation::open_operations(&inner.connection).unwrap()
}
