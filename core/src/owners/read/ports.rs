use std::sync::Arc;

use crate::lifecycle::Admitted;
use crate::owners::store::StoreBatch;
use crate::runtime::{Epoch, EpochStamp};

/// The way from a runner to the input `Commit` of the owner `StoreWriter`.
pub trait StorePort: Send + Sync + 'static {
    fn commit(&self, stamp: EpochStamp, batch: StoreBatch);
}

/// Gives the admission a transport call needs, `None` once the database is closed.
pub type Admit = Arc<dyn Fn() -> Option<Admitted> + Send + Sync>;

/// Takes the effects that are no IO of a runner (timers, inputs for other owners, answers of the platform).
pub type Rest<E> = Box<dyn FnMut(EpochStamp, E) + Send>;

pub(super) fn epoch_of(stamp: EpochStamp) -> Epoch {
    match stamp {
        EpochStamp::Epoch(epoch) => epoch,
        EpochStamp::Control => 0,
    }
}
