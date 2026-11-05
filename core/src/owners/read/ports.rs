use std::sync::Arc;

use crate::lifecycle::Admitted;
use crate::runtime::EpochStamp;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Refused(pub String);

pub type Admit = Arc<dyn Fn() -> Result<Admitted, Refused> + Send + Sync>;

/// Takes the effects that are no IO of a runner (answers of the platform, the end of an owner).
pub type Rest<E> = Box<dyn FnMut(EpochStamp, E) + Send>;
