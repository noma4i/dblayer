use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use super::DbError;

pub(super) struct OpenSlot {
    taken: AtomicBool,
}

pub(super) struct OpenLease {
    slot: Arc<OpenSlot>,
    released: AtomicBool,
}

impl OpenSlot {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self { taken: AtomicBool::new(false) })
    }

    pub(super) fn acquire(self: &Arc<Self>) -> Result<Arc<OpenLease>, DbError> {
        if self.taken.swap(true, Ordering::SeqCst) {
            return Err(DbError::Invalid { detail: "database is already open; open it once at app start".into() });
        }
        Ok(Arc::new(OpenLease { slot: self.clone(), released: AtomicBool::new(false) }))
    }
}

impl OpenLease {
    pub(super) fn release(&self) {
        if !self.released.swap(true, Ordering::SeqCst) {
            self.slot.taken.store(false, Ordering::SeqCst);
        }
    }
}

impl Drop for OpenLease {
    fn drop(&mut self) {
        self.release();
    }
}

#[cfg(test)]
#[path = "open_slot_tests.rs"]
mod tests;
