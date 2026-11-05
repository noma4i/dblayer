use crate::lifecycle::authority::Authority;

use super::{Inner, Store, StoreError};

impl Store {
    pub(crate) fn write<T, E>(&self, authority: &Authority, refused: impl FnOnce() -> E, work: impl FnOnce(&Inner) -> Result<T, E>) -> Result<T, E> {
        let inner = self.lock();
        let _line = match authority {
            Authority::Admitted(admitted) => {
                if !std::sync::Arc::ptr_eq(&self.lifecycle, admitted.life()) { return Err(refused()); }
                let line = admitted.life().commit_line();
                if !admitted.is_current() { return Err(refused()); }
                Some(line)
            }
            Authority::Internal(_) | Authority::Owner => None,
        };
        work(&inner)
    }

    pub(crate) fn write_store<T>(&self, authority: &Authority, work: impl FnOnce(&Inner) -> Result<T, StoreError>) -> Result<T, StoreError> {
        self.write(authority, || StoreError::Envelope(crate::commit::EnvelopeFault::StaleGeneration), |inner| {
            inner.ensure_healthy()?;
            work(inner)
        })
    }
}

impl Store {
    pub(crate) fn read_locked<T>(&self, read: impl FnOnce(&rusqlite::Connection) -> rusqlite::Result<T>) -> rusqlite::Result<T> {
        read(&self.lock().connection)
    }
}
