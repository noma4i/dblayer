use std::sync::Arc;

use crate::runtime::EpochStamp;

/// The requests of the network that are on their way, for the tests that wait for a quiet database.
#[cfg(test)]
pub(crate) type Flights = Arc<std::sync::atomic::AtomicUsize>;

#[cfg(test)]
pub(crate) struct InFlight(Flights);

#[cfg(test)]
impl InFlight {
    pub(crate) fn start(flights: &Flights) -> Self {
        flights.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Self(flights.clone())
    }
}

#[cfg(test)]
impl Drop for InFlight {
    fn drop(&mut self) { self.0.fetch_sub(1, std::sync::atomic::Ordering::SeqCst); }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Refused(pub String);

/// The admission of the epoch the request was decided in.
pub type Admit = Arc<dyn Fn(crate::runtime::Epoch) -> Result<(), Refused> + Send + Sync>;

/// Takes the effects that are no IO of a runner (answers of the platform, the end of an owner).
pub type Rest<E> = Box<dyn FnMut(EpochStamp, E) + Send>;
