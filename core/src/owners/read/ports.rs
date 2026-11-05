use std::sync::Arc;

use crate::lifecycle::Admitted;
use crate::runtime::EpochStamp;

/// Gives the admission a transport call needs, `None` once the database is closed.
pub type Admit = Arc<dyn Fn() -> Option<Admitted> + Send + Sync>;

/// Takes the effects that are no IO of a runner (answers of the platform, the end of an owner).
pub type Rest<E> = Box<dyn FnMut(EpochStamp, E) + Send>;
