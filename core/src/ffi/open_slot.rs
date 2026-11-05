use std::sync::Arc;

use super::DbError;

pub(super) use crate::runtime::OpenLease;

pub(super) struct OpenSlot(crate::runtime::OpenSlot);

impl OpenSlot {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self(crate::runtime::OpenSlot::new()))
    }

    pub(super) fn acquire(self: &Arc<Self>) -> Result<Arc<OpenLease>, DbError> {
        self.0.acquire().map(Arc::new).ok_or_else(|| DbError::Invalid { detail: "database is already open; open it once at app start".into() })
    }
}

#[cfg(test)]
#[path = "open_slot_tests.rs"]
mod tests;
