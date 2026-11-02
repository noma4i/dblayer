use std::path::Path;
use std::sync::{Condvar, Mutex, MutexGuard};

use rusqlite::{Connection, OpenFlags};

use super::StoreError;

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
        let mut idle = self.idle.lock().unwrap_or_else(|error| error.into_inner());
        loop {
            if let Some(index) = idle.pop() {
                drop(idle);
                let connection = self.connections[index].lock().unwrap_or_else(|error| error.into_inner());
                return Lease { pool: self, connection, index };
            }
            idle = self.available.wait(idle).unwrap_or_else(|error| error.into_inner());
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
