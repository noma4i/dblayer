
use std::future::Future;

use tokio::runtime::Handle;
use tokio::task::{AbortHandle, JoinError, JoinHandle, JoinSet};

use crate::lifecycle::{Authority, Epoch};

#[derive(Clone)]
pub(crate) enum Carry {
    Epoch(Epoch),
    Internal,
    Detached,
}

impl Carry {
    pub(crate) fn here() -> Self {
        match Epoch::ambient() {
            Some(epoch) => Self::Epoch(epoch),
            None => Self::Detached,
        }
    }
}

impl From<Epoch> for Carry {
    fn from(epoch: Epoch) -> Self { Self::Epoch(epoch) }
}

impl From<&Authority> for Carry {
    fn from(authority: &Authority) -> Self {
        match authority {
            Authority::Epoch(epoch) => Self::Epoch(epoch.clone()),
            Authority::Internal(_) => Self::Internal,
        }
    }
}

#[allow(clippy::disallowed_methods)]
pub(crate) fn spawn<F>(carry: Carry, work: F) -> JoinHandle<F::Output> where F: Future + Send + 'static, F::Output: Send + 'static {
    match carry {
        Carry::Epoch(epoch) => tokio::spawn(epoch.scope(work)),
        Carry::Internal | Carry::Detached => tokio::spawn(work),
    }
}

#[allow(clippy::disallowed_methods)]
pub(crate) fn spawn_on<F>(carry: Carry, runtime: &Handle, work: F) -> JoinHandle<F::Output> where F: Future + Send + 'static, F::Output: Send + 'static {
    match carry {
        Carry::Epoch(epoch) => runtime.spawn(epoch.scope(work)),
        Carry::Internal | Carry::Detached => runtime.spawn(work),
    }
}

#[allow(clippy::disallowed_methods)]
pub(crate) fn spawn_blocking<R: Send + 'static>(carry: Carry, work: impl FnOnce() -> R + Send + 'static) -> JoinHandle<R> {
    tokio::task::spawn_blocking(bind_once(carry, work))
}

#[allow(clippy::disallowed_methods)]
pub(crate) fn spawn_blocking_on<R: Send + 'static>(carry: Carry, runtime: &Handle, work: impl FnOnce() -> R + Send + 'static) -> JoinHandle<R> {
    runtime.spawn_blocking(bind_once(carry, work))
}

#[allow(clippy::disallowed_methods)]
pub(crate) fn spawn_thread(name: String, body: impl FnOnce() + Send + 'static) -> std::io::Result<std::thread::JoinHandle<()>> {
    std::thread::Builder::new().name(name).spawn(body)
}

fn bind_once<R>(carry: Carry, work: impl FnOnce() -> R) -> impl FnOnce() -> R {
    move || match carry {
        Carry::Epoch(epoch) => epoch.pinned(work),
        Carry::Internal | Carry::Detached => work(),
    }
}

pub(crate) fn bind<A, R>(carry: Carry, callback: impl Fn(A) -> R) -> impl Fn(A) -> R {
    move |argument| match &carry {
        Carry::Epoch(epoch) => epoch.pinned(|| callback(argument)),
        Carry::Internal | Carry::Detached => callback(argument),
    }
}

pub(crate) struct Children<T> {
    set: JoinSet<T>,
}

impl<T> Default for Children<T> {
    fn default() -> Self { Self { set: JoinSet::new() } }
}

impl<T: Send + 'static> Children<T> {
    pub(crate) fn new() -> Self { Self::default() }

    #[allow(clippy::disallowed_methods)]
    pub(crate) fn spawn<F>(&mut self, carry: Carry, work: F) -> AbortHandle where F: Future<Output = T> + Send + 'static {
        match carry {
            Carry::Epoch(epoch) => self.set.spawn(epoch.scope(work)),
            Carry::Internal | Carry::Detached => self.set.spawn(work),
        }
    }

    #[allow(clippy::disallowed_methods)]
    pub(crate) fn spawn_on<F>(&mut self, carry: Carry, work: F, runtime: &Handle) -> AbortHandle where F: Future<Output = T> + Send + 'static {
        match carry {
            Carry::Epoch(epoch) => self.set.spawn_on(epoch.scope(work), runtime),
            Carry::Internal | Carry::Detached => self.set.spawn_on(work, runtime),
        }
    }

    pub(crate) async fn join_next(&mut self) -> Option<Result<T, JoinError>> { self.set.join_next().await }

    pub(crate) fn try_join_next(&mut self) -> Option<Result<T, JoinError>> { self.set.try_join_next() }

    pub(crate) fn abort_all(&mut self) { self.set.abort_all() }

    pub(crate) fn len(&self) -> usize { self.set.len() }

    pub(crate) fn is_empty(&self) -> bool { self.set.is_empty() }
}

#[cfg(test)]
#[path = "task_tests.rs"]
mod task_tests;
