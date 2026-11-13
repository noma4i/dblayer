use std::path::Path;
use std::sync::{Condvar, Mutex, MutexGuard};

use rusqlite::{Connection, OpenFlags, OptionalExtension};

use super::StoreError;

impl super::Store {
    pub(crate) fn resolve(&self, model: &str, row: &crate::identity::RowId) -> Result<crate::identity::RowId, StoreError> {
        if !row.is_temp() { return Ok(row.clone()); }
        let real: Option<String> = self.read(&[model], |reader| Ok(reader.connection().query_row(include_str!("sql/ddl/read_row_alias.sql"), rusqlite::params![model, row.as_str()], |entry| entry.get(0)).optional()?))?;
        real.map(|real| crate::identity::RowId::new(&real).map_err(|error| StoreError::Constraint(error.to_string()))).unwrap_or_else(|| Ok(row.clone()))
    }
}

#[cfg(test)]
impl super::Store {
    pub(crate) fn hold_readers(&self, held: &crate::test_wait::Flag, release: &crate::test_wait::Flag) {
        let leases: Vec<_> = self.readers.iter().flat_map(|pool| (0..pool.connections.len()).map(|_| pool.lease())).collect();
        held.set();
        release.wait_blocking("release the readers of the pool");
        drop(leases);
    }
}

pub(super) struct Readers {
    connections: Vec<Mutex<Connection>>,
    idle: Mutex<Vec<usize>>,
    available: Condvar,
}

impl Readers {
    pub(super) fn open(path: &Path) -> Result<Self, StoreError> {
        let mut connections = Vec::with_capacity(2);
        for _ in 0..2 {
            let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX)?;
            connection.pragma_update(None, "query_only", true)?;
            connections.push(Mutex::new(connection));
        }
        let idle = Mutex::new((0..connections.len()).collect());
        Ok(Self { connections, idle, available: Condvar::new() })
    }

    pub(super) fn lease(&self) -> Lease<'_> {
        let mut idle = crate::lock::acquire(&self.idle);
        loop {
            if let Some(index) = idle.pop() {
                drop(idle);
                let connection = crate::lock::acquire(&self.connections[index]);
                return Lease { pool: self, connection, index };
            }
            idle = crate::blocking::blocking_unit(|| self.available.wait(idle).unwrap_or_else(|error| error.into_inner()));
        }
    }
}

pub(super) struct Lease<'a> {
    pool: &'a Readers,
    connection: MutexGuard<'a, Connection>,
    index: usize,
}

impl std::ops::Deref for Lease<'_> {
    type Target = Connection;

    fn deref(&self) -> &Connection {
        &self.connection
    }
}

impl Drop for Lease<'_> {
    fn drop(&mut self) {
        let mut idle = self.pool.idle.lock().unwrap_or_else(|error| error.into_inner());
        idle.push(self.index);
        self.pool.available.notify_one(); // lock-ok: condvar schedules an OS thread
    }
}
