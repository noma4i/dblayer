use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::thread::ThreadId;

static CALLS: OnceLock<Mutex<HashMap<ThreadId, u64>>> = OnceLock::new();

pub(super) fn entered() {
    *crate::db::locked(CALLS.get_or_init(Mutex::default)).entry(std::thread::current().id()).or_default() += 1;
}

#[derive(uniffi::Object)]
pub struct EntityCallProbe { thread: ThreadId, before: u64 }

#[uniffi::export]
impl EntityCallProbe {
    #[uniffi::constructor]
    pub fn new() -> Self {
        let thread = std::thread::current().id();
        let before = crate::db::locked(CALLS.get_or_init(Mutex::default)).get(&thread).copied().unwrap_or(0);
        Self { thread, before }
    }
    pub fn calls(&self) -> u64 {
        crate::db::locked(CALLS.get_or_init(Mutex::default)).get(&self.thread).copied().unwrap_or(0).saturating_sub(self.before)
    }
}

impl Default for EntityCallProbe {
    fn default() -> Self {
        Self::new()
    }
}
