use std::sync::Arc;

use crate::runtime::EpochStamp;

/// The requests of the network that are on their way, for the tests that wait for a quiet database.
#[cfg(test)]
#[derive(Clone, Default)]
pub struct RequestsOnTheWay(Arc<FlightCount>);

#[cfg(test)]
#[derive(Default)]
struct FlightCount {
    count: std::sync::atomic::AtomicUsize,
    none: tokio::sync::Notify,
}

#[cfg(test)]
impl RequestsOnTheWay {
    pub fn load(&self, order: std::sync::atomic::Ordering) -> usize { self.0.count.load(order) }

    /// Returns when no request is on its way.
    pub async fn none(&self) {
        loop {
            let left = self.0.none.notified();
            tokio::pin!(left);
            left.as_mut().enable();
            if self.load(std::sync::atomic::Ordering::SeqCst) == 0 { return; }
            left.await;
        }
    }
}

#[cfg(test)]
pub(crate) struct InFlight(RequestsOnTheWay);

#[cfg(test)]
impl InFlight {
    pub(crate) fn start(flights: &RequestsOnTheWay) -> Self {
        flights.0.count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Self(flights.clone())
    }
}

#[cfg(test)]
impl Drop for InFlight {
    fn drop(&mut self) {
        if self.0.0.count.fetch_sub(1, std::sync::atomic::Ordering::SeqCst) == 1 {
            crate::lock::effect(|| self.0.0.none.notify_waiters());
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Refused(pub String);

/// The admission of the epoch the request was decided in.
pub type Admit = Arc<dyn Fn(crate::runtime::Epoch) -> Result<(), Refused> + Send + Sync>;

/// Takes the effects that are no IO of a runner (answers of the platform, the end of an owner).
pub type Rest<E> = Box<dyn FnMut(EpochStamp, E) + Send>;
