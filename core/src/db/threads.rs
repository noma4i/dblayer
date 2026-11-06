use std::cell::Cell;
use std::sync::{Arc, Condvar, Mutex};

use super::DbError;

thread_local! {
    static HOLDS_SHARED: Cell<bool> = const { Cell::new(false) };
}

pub(crate) struct LiveThreads {
    count: Mutex<usize>,
    ended: Condvar,
}

struct Leave(Arc<LiveThreads>);

impl Drop for Leave {
    fn drop(&mut self) {
        *super::lock::lock(&self.0.count) -= 1;
        self.0.ended.notify_all(); // lock-ok: count guard ended at statement
    }
}

impl LiveThreads {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self { count: Mutex::new(0), ended: Condvar::new() })
    }

    pub(crate) fn start(self: &Arc<Self>, carry: crate::task::Carry, name: &'static str, body: impl FnOnce() + Send + 'static) -> Result<(), DbError> {
        *super::lock::lock(&self.count) += 1;
        let leave = Leave(self.clone());
        spawn_named(carry, name, move || {
            let _leave = leave;
            HOLDS_SHARED.with(|flag| flag.set(true));
            body();
        })
    }

    pub(crate) fn wait_ended(&self) -> Result<(), DbError> {
        if HOLDS_SHARED.with(Cell::get) {
            return Err(DbError::Internal("close cannot run on a database thread".into()));
        }
        let mut count = super::lock::lock(&self.count);
        while *count > 0 {
            count = super::lock::wait(&self.ended, count);
        }
        Ok(())
    }
}

#[cfg(test)]
thread_local! {
    static REFUSED: std::cell::RefCell<Option<&'static str>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
pub(crate) fn refuse_spawn_here(name: &'static str) {
    REFUSED.with(|refused| *refused.borrow_mut() = Some(name));
}

pub(crate) fn spawn_named(carry: crate::task::Carry, name: &'static str, body: impl FnOnce() + Send + 'static) -> Result<(), DbError> {
    #[cfg(test)]
    if REFUSED.with(|refused| *refused.borrow() == Some(name)) {
        return Err(DbError::ThreadSpawn { name, detail: "refused by the test".into() });
    }
    crate::task::spawn_thread(carry, name.to_string(), body)
        .map(drop)
        .map_err(|error| DbError::ThreadSpawn { name, detail: error.to_string() })
}
