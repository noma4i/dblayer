
use std::future::Future;

use tokio::runtime::Handle;
use tokio::task::JoinHandle;

use crate::runtime::Launch;

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
pub(crate) fn spawn<F>(work: F) -> JoinHandle<F::Output> where F: Future + Send + 'static, F::Output: Send + 'static {
    tokio::spawn(work)
}

#[allow(clippy::disallowed_methods)]
pub(crate) fn spawn_on<F>(runtime: &Handle, work: F) -> JoinHandle<F::Output> where F: Future + Send + 'static, F::Output: Send + 'static {
    runtime.spawn(work)
}

#[allow(clippy::disallowed_methods)]
pub(crate) fn spawn_blocking<R: Send + 'static>(launch: Option<Launch>, work: impl FnOnce() -> R + Send + 'static) -> JoinHandle<R> {
    tokio::task::spawn_blocking(move || {
        let _launch = launch;
        work()
    })
}

#[allow(clippy::disallowed_methods)]
pub(crate) fn spawn_blocking_on<R: Send + 'static>(runtime: &Handle, launch: Option<Launch>, work: impl FnOnce() -> R + Send + 'static) -> JoinHandle<R> {
    runtime.spawn_blocking(move || {
        let _launch = launch;
        work()
    })
}

#[allow(clippy::disallowed_methods)]
pub(crate) fn spawn_thread(name: String, body: impl FnOnce() + Send + 'static) -> std::io::Result<std::thread::JoinHandle<()>> {
    std::thread::Builder::new().name(name).spawn(body)
}
