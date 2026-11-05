
use std::sync::Arc;

use super::Wiring;
use crate::owners::lifecycle::LifecycleInput;

pub trait ObserverPort: Send + Sync {
    fn stop(&self, done: StopDone);
}

pub struct StopDone {
    wiring: Option<Arc<Wiring>>,
}

impl StopDone {
    pub(super) fn new(wiring: Arc<Wiring>) -> Self { Self { wiring: Some(wiring) } }

    pub fn finish(self) {}
}

impl Drop for StopDone {
    fn drop(&mut self) {
        if let Some(wiring) = self.wiring.take() {
            wiring.to_lifecycle(LifecycleInput::ObserversStopped);
        }
    }
}
