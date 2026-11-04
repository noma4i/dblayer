use std::sync::Arc;

use super::Lifecycle;

#[derive(Clone)]
pub(crate) struct Epoch {
    life: Arc<Lifecycle>,
    value: u64,
}

impl Epoch {
    pub(crate) fn new(life: &Arc<Lifecycle>, value: u64) -> Self { Self { life: life.clone(), value } }

    pub(crate) fn value(&self) -> u64 { self.value }

    pub(crate) fn life(&self) -> &Arc<Lifecycle> { &self.life }

    pub(crate) fn pinned<R>(&self, write: impl FnOnce() -> R) -> R {
        let before = self.enter();
        let result = write();
        self.leave(before);
        result
    }

    pub(crate) fn enter(&self) -> Option<Epoch> {
        #[cfg(test)]
        self.life.pinned_writes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        PINNED.replace(Some(self.clone()))
    }

    pub(crate) fn leave(&self, before: Option<Epoch>) {
        PINNED.set(before);
        #[cfg(test)]
        self.life.pinned_writes.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }

    pub(crate) fn scope<F: std::future::Future>(&self, work: F) -> impl std::future::Future<Output = F::Output> { ORIGIN.scope(self.clone(), work) }

    pub(crate) fn ambient() -> Option<Epoch> { PINNED.with(|pinned| pinned.borrow().clone()).or_else(|| ORIGIN.try_with(Clone::clone).ok()) }
}

thread_local! {
    static PINNED: std::cell::RefCell<Option<Epoch>> = const { std::cell::RefCell::new(None) };
}

tokio::task_local! {
    static ORIGIN: Epoch;
}

pub(crate) struct Internal(());

impl Internal {
    pub(in crate::lifecycle) fn new() -> Self { Self(()) }

    #[cfg(test)]
    pub(crate) fn for_tests() -> Self { Self(()) }
}

/// What lets a SQL commit run: the epoch of admitted work, or the proof of a lifecycle transition. A commit has no third form.
pub(crate) enum Authority {
    Epoch(Epoch),
    Internal(Internal),
}

impl From<Epoch> for Authority {
    fn from(epoch: Epoch) -> Self { Self::Epoch(epoch) }
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

    pub(crate) fn epoch_now(self: &Arc<Self>) -> Epoch { Epoch::new(self, self.snapshot().session_epoch) }
}
