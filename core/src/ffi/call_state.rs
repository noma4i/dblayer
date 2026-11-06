use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicU8, Ordering};
#[cfg(test)]
use std::sync::Mutex;

/// A listener call starts only while the subscription is live: starting a call and cancelling are atomic against each other.
pub(crate) struct CallState {
    state: AtomicU8,
    #[cfg(test)]
    pub(crate) before_enter: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl CallState {
    const IDLE: u8 = 0;
    const CALLING: u8 = 1;
    const CANCELLED: u8 = 2;

    pub(crate) fn new() -> Self {
        Self {
            state: AtomicU8::new(Self::IDLE),
            #[cfg(test)]
            before_enter: Mutex::new(None),
        }
    }

    pub(crate) fn cancel(&self) {
        self.state.store(Self::CANCELLED, Ordering::SeqCst);
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.state.load(Ordering::SeqCst) == Self::CANCELLED
    }

    pub(crate) fn call(&self, listener: impl FnOnce()) -> bool {
        #[cfg(test)]
        if let Some(hook) = self.before_enter.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).take() {
            hook();
        }
        if self.state.compare_exchange(Self::IDLE, Self::CALLING, Ordering::SeqCst, Ordering::SeqCst).is_err() {
            return false;
        }
        let outcome = catch_unwind(AssertUnwindSafe(listener));
        let _ = self.state.compare_exchange(Self::CALLING, Self::IDLE, Ordering::SeqCst, Ordering::SeqCst);
        drop(outcome);
        true
    }
}
