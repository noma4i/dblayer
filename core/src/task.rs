
use std::future::Future;

use tokio::runtime::Handle;
use tokio::task::JoinHandle;

use crate::ffi::admission::Admitted;
use crate::lifecycle::authority::Authority;

thread_local! {
    static CURRENT_ADMISSION: std::cell::RefCell<Option<Admitted>> = const { std::cell::RefCell::new(None) };
}

tokio::task_local! {
    static ASYNC_ADMISSION: Admitted;
}

struct Current(Option<Admitted>);

impl Drop for Current {
    fn drop(&mut self) { CURRENT_ADMISSION.set(self.0.take()); }
}

pub(crate) fn scope<F: Future>(admitted: Admitted, work: F) -> impl Future<Output = F::Output> {
    ASYNC_ADMISSION.scope(admitted, work)
}

pub(crate) fn admitted() -> Option<Admitted> {
    CURRENT_ADMISSION.with(|current| current.borrow().clone()).or_else(|| ASYNC_ADMISSION.try_with(Clone::clone).ok())
}

#[derive(Clone)]
pub(crate) enum Carry {
    Admitted(Admitted),
    Internal,
    Detached,
}

impl Carry {
    pub(crate) fn here() -> Self {
        if let Some(admitted) = admitted() {
            return Self::Admitted(admitted);
        }
        Self::Detached
    }
}

impl From<Admitted> for Carry {
    fn from(admitted: Admitted) -> Self { Self::Admitted(admitted) }
}

impl From<&Authority> for Carry {
    fn from(authority: &Authority) -> Self {
        match authority {
            Authority::Admitted(admitted) => Self::Admitted(admitted.clone()),
            Authority::Internal(_) | Authority::Owner => Self::Internal,
        }
    }
}

#[allow(clippy::disallowed_methods)]
pub(crate) fn spawn<F>(carry: Carry, work: F) -> JoinHandle<F::Output> where F: Future + Send + 'static, F::Output: Send + 'static {
    match carry {
        Carry::Admitted(admitted) => tokio::spawn(ASYNC_ADMISSION.scope(admitted, work)),
        Carry::Internal | Carry::Detached => tokio::spawn(work),
    }
}

#[allow(clippy::disallowed_methods)]
pub(crate) fn spawn_on<F>(carry: Carry, runtime: &Handle, work: F) -> JoinHandle<F::Output> where F: Future + Send + 'static, F::Output: Send + 'static {
    match carry {
        Carry::Admitted(admitted) => runtime.spawn(ASYNC_ADMISSION.scope(admitted, work)),
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
pub(crate) fn spawn_thread(carry: Carry, name: String, body: impl FnOnce() + Send + 'static) -> std::io::Result<std::thread::JoinHandle<()>> {
    std::thread::Builder::new().name(name).spawn(bind_once(carry, body))
}

fn bind_once<R>(carry: Carry, work: impl FnOnce() -> R) -> impl FnOnce() -> R {
    move || match carry {
        Carry::Admitted(admitted) => {
            let _current = Current(CURRENT_ADMISSION.replace(Some(admitted)));
            work()
        }
        Carry::Internal | Carry::Detached => work(),
    }
}

#[cfg(test)]
#[path = "task_tests.rs"]
mod task_tests;
