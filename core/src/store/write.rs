use super::Store;
#[cfg(test)]
use super::{Inner, StoreError};

impl Store {
    #[cfg(test)]
    pub(crate) fn write<T>(&self, work: impl FnOnce(&Inner) -> Result<T, StoreError>) -> Result<T, StoreError> {
        let inner = self.lock();
        inner.ensure_healthy()?;
        work(&inner)
    }

    pub(crate) fn read_locked<T>(&self, read: impl FnOnce(&rusqlite::Connection) -> rusqlite::Result<T>) -> rusqlite::Result<T> {
        read(&self.lock().connection)
    }
}
