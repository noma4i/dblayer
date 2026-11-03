//! Every task boundary of the library goes through this module. A boundary either carries the session epoch of the work that crosses it (`spawn`, `spawn_on`, `spawn_blocking`, `Children`, `bind`) or says that it carries none (`*_unpinned`): lifecycle-internal work and work without SQL.

use std::future::Future;
use std::sync::Arc;

use tokio::runtime::Handle;
use tokio::task::{AbortHandle, JoinError, JoinHandle, JoinSet};

use crate::lifecycle::Lifecycle;
use crate::store::{pinned_epoch, pinned_to_epoch, ORIGIN};

pub(crate) type Epoch = (Arc<Lifecycle>, u64);

#[allow(clippy::disallowed_methods)]
pub(crate) fn spawn<F>(work: F) -> JoinHandle<F::Output> where F: Future + Send + 'static, F::Output: Send + 'static {
    match pinned_epoch() {
        Some(epoch) => tokio::spawn(ORIGIN.scope(epoch, work)),
        None => tokio::spawn(work),
    }
}

#[allow(clippy::disallowed_methods)]
pub(crate) fn spawn_on<F>(runtime: &Handle, work: F) -> JoinHandle<F::Output> where F: Future + Send + 'static, F::Output: Send + 'static {
    match pinned_epoch() {
        Some(epoch) => runtime.spawn(ORIGIN.scope(epoch, work)),
        None => runtime.spawn(work),
    }
}

#[allow(clippy::disallowed_methods)]
pub(crate) fn spawn_unpinned<F>(work: F) -> JoinHandle<F::Output> where F: Future + Send + 'static, F::Output: Send + 'static {
    tokio::spawn(work)
}

#[allow(clippy::disallowed_methods)]
pub(crate) fn spawn_on_unpinned<F>(runtime: &Handle, work: F) -> JoinHandle<F::Output> where F: Future + Send + 'static, F::Output: Send + 'static {
    runtime.spawn(work)
}

#[allow(clippy::disallowed_methods)]
pub(crate) fn spawn_blocking<R: Send + 'static>(work: impl FnOnce() -> R + Send + 'static) -> JoinHandle<R> {
    tokio::task::spawn_blocking(bind_once(work))
}

#[allow(clippy::disallowed_methods)]
pub(crate) fn spawn_blocking_on<R: Send + 'static>(runtime: &Handle, work: impl FnOnce() -> R + Send + 'static) -> JoinHandle<R> {
    runtime.spawn_blocking(bind_once(work))
}

#[allow(clippy::disallowed_methods)]
pub(crate) fn spawn_blocking_unpinned<R: Send + 'static>(work: impl FnOnce() -> R + Send + 'static) -> JoinHandle<R> {
    tokio::task::spawn_blocking(work)
}

#[allow(clippy::disallowed_methods)]
pub(crate) fn spawn_thread(name: String, body: impl FnOnce() + Send + 'static) -> std::io::Result<std::thread::JoinHandle<()>> {
    std::thread::Builder::new().name(name).spawn(body)
}

fn bind_once<R>(work: impl FnOnce() -> R) -> impl FnOnce() -> R {
    let epoch = pinned_epoch();
    move || match epoch {
        Some((life, value)) => pinned_to_epoch(&life, value, work),
        None => work(),
    }
}

pub(crate) fn bind<A, R>(callback: impl Fn(A) -> R) -> impl Fn(A) -> R {
    let epoch = pinned_epoch();
    move |argument| match &epoch {
        Some((life, value)) => pinned_to_epoch(life, *value, || callback(argument)),
        None => callback(argument),
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
    pub(crate) fn spawn<F>(&mut self, work: F) -> AbortHandle where F: Future<Output = T> + Send + 'static {
        match pinned_epoch() {
            Some(epoch) => self.set.spawn(ORIGIN.scope(epoch, work)),
            None => self.set.spawn(work),
        }
    }

    #[allow(clippy::disallowed_methods)]
    pub(crate) fn spawn_unpinned<F>(&mut self, work: F) -> AbortHandle where F: Future<Output = T> + Send + 'static {
        self.set.spawn(work)
    }

    #[allow(clippy::disallowed_methods)]
    pub(crate) fn spawn_unpinned_on<F>(&mut self, work: F, runtime: &Handle) -> AbortHandle where F: Future<Output = T> + Send + 'static {
        self.set.spawn_on(work, runtime)
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
