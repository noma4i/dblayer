use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};

use super::Epoch;
use crate::transport::TransportError;

const CLOSED: u64 = 1;

/// The gate of the epochs of one database: one atomic value, the epoch above and the closed bit below.
pub struct EpochGate(AtomicU64);

impl Default for EpochGate {
    fn default() -> Self { Self(AtomicU64::new(CLOSED)) }
}

impl EpochGate {
    pub fn open(&self, epoch: Epoch) { self.0.store(epoch << 1, Ordering::SeqCst); }

    /// Closes the gate of `epoch`; a gate of another epoch stays as it is.
    pub fn close(&self, epoch: Epoch) {
        let _ = self.0.compare_exchange(epoch << 1, (epoch << 1) | CLOSED, Ordering::SeqCst, Ordering::SeqCst);
    }

    pub fn admits(&self, epoch: Epoch) -> bool { self.0.load(Ordering::SeqCst) == epoch << 1 }
}

/// A request to the network of `epoch`: the gate check and the first poll of the request run in one synchronous stretch, a closed gate ends the request as `Interrupted` without calling the transport. `None` is a step that belongs to no epoch.
pub async fn send_in_epoch<T>(gate: &EpochGate, epoch: Option<Epoch>, request: impl Future<Output = Result<T, TransportError>>) -> Result<T, TransportError> {
    if epoch.is_some_and(|epoch| !gate.admits(epoch)) {
        return Err(TransportError::Interrupted("the epoch ended before the request was sent".into()));
    }
    request.await
}

#[cfg(test)]
#[path = "gate_tests.rs"]
mod tests;
