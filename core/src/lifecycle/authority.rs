//! What lets a SQL commit run: the epoch of admitted work, the proof of the opening,
//! or a batch of an owner whose epoch and basis the Store owner checked against its `Published` snapshot before the write.

use crate::ffi::admission::Admitted;

pub(crate) enum Authority {
    Admitted(Admitted),
    Internal(Internal),
    Owner,
}

/// The proof of the opening: only this module makes it.
pub(crate) struct Internal(());

impl Internal {
    pub(crate) fn opening() -> Self { Self(()) }

    #[cfg(test)]
    pub(crate) fn for_tests() -> Self { Self(()) }
}

impl From<Admitted> for Authority {
    fn from(admitted: Admitted) -> Self { Self::Admitted(admitted) }
}

impl From<Internal> for Authority {
    fn from(token: Internal) -> Self { Self::Internal(token) }
}
