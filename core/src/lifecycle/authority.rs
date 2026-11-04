use super::Lifecycle;

pub(crate) struct Internal(());

impl Internal {
    pub(in crate::lifecycle) fn new() -> Self { Self(()) }

    #[cfg(test)]
    pub(crate) fn for_tests() -> Self { Self(()) }
}

/// What lets a SQL commit run: the epoch of admitted work, or the proof of a lifecycle transition. A commit has no third form.
pub(crate) enum Authority {
    Admitted(super::Admitted),
    Internal(Internal),
}

impl From<super::Admitted> for Authority {
    fn from(admitted: super::Admitted) -> Self { Self::Admitted(admitted) }
}

impl From<Internal> for Authority {
    fn from(token: Internal) -> Self { Self::Internal(token) }
}

#[cfg(test)]
#[path = "authority_tests.rs"]
mod authority_tests;

impl Lifecycle {
    pub(crate) fn opening(&self) -> Option<Internal> {
        (!self.opening_issued.swap(true, std::sync::atomic::Ordering::SeqCst)).then(Internal::new)
    }
}
