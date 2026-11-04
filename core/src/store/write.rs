use crate::lifecycle::Authority;

use super::{Inner, Store, StoreError};

impl Store {
    pub(crate) fn write<T, E>(&self, authority: &Authority, refused: impl FnOnce() -> E, work: impl FnOnce(&Inner) -> Result<T, E>) -> Result<T, E> {
        #[cfg(test)]
        self.writes_waiting.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let inner = self.lock();
        let _line = match authority {
            Authority::Epoch(epoch) => {
                let line = epoch.life().commit_line();
                if !epoch.life().epoch_holds(epoch.value()) { return Err(refused()); }
                Some(line)
            }
            Authority::Internal(_) => None,
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
