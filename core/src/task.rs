
use std::future::Future;

use tokio::runtime::Handle;
use tokio::task::JoinHandle;


#[derive(Clone)]
pub(crate) enum Carry {
    Internal,
    Detached,
}

impl Carry {
    pub(crate) fn here() -> Self { Self::Detached }
}


#[allow(clippy::disallowed_methods)]
pub(crate) fn spawn<F>(_: Carry, work: F) -> JoinHandle<F::Output> where F: Future + Send + 'static, F::Output: Send + 'static {
    tokio::spawn(work)
}

#[allow(clippy::disallowed_methods)]
pub(crate) fn spawn_on<F>(_: Carry, runtime: &Handle, work: F) -> JoinHandle<F::Output> where F: Future + Send + 'static, F::Output: Send + 'static {
    runtime.spawn(work)
}

#[allow(clippy::disallowed_methods)]
pub(crate) fn spawn_blocking<R: Send + 'static>(_: Carry, work: impl FnOnce() -> R + Send + 'static) -> JoinHandle<R> {
    tokio::task::spawn_blocking(work)
}

#[allow(clippy::disallowed_methods)]
pub(crate) fn spawn_blocking_on<R: Send + 'static>(_: Carry, runtime: &Handle, work: impl FnOnce() -> R + Send + 'static) -> JoinHandle<R> {
    runtime.spawn_blocking(work)
}

#[allow(clippy::disallowed_methods)]
pub(crate) fn spawn_thread(_: Carry, name: String, body: impl FnOnce() + Send + 'static) -> std::io::Result<std::thread::JoinHandle<()>> {
    std::thread::Builder::new().name(name).spawn(body)
}
