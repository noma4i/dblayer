//! What lets a SQL commit run: the proof of the opening, or a batch of an owner whose epoch and basis the Store owner checked against its `Published` snapshot before the write.

pub(crate) enum Authority {
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

impl From<Internal> for Authority {
    fn from(token: Internal) -> Self { Self::Internal(token) }
}
