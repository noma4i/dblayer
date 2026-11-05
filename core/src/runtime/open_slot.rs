use super::sync::{Arc, AtomicBool, Ordering};

#[derive(Clone)]
pub struct OpenSlot(Arc<AtomicBool>);

pub struct OpenLease {
    slot: Arc<AtomicBool>,
    released: AtomicBool,
}

impl OpenSlot {
    pub fn new() -> Self { Self(Arc::new(AtomicBool::new(false))) }

    pub fn acquire(&self) -> Option<OpenLease> {
        if self.0.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_err() {
            return None;
        }
        Some(OpenLease { slot: self.0.clone(), released: AtomicBool::new(false) })
    }
}

impl Default for OpenSlot {
    fn default() -> Self { Self::new() }
}

impl OpenLease {
    pub fn release(&self) {
        if self.released.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
            self.slot.store(false, Ordering::SeqCst);
        }
    }
}

impl Drop for OpenLease {
    fn drop(&mut self) { self.release(); }
}
