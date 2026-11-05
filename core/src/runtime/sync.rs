#[cfg(not(loom))]
pub(crate) use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
#[cfg(not(loom))]
pub(crate) use std::sync::{Arc, Condvar, Mutex, MutexGuard};

#[cfg(loom)]
pub(crate) use loom::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
#[cfg(loom)]
pub(crate) use loom::sync::{Arc, Condvar, Mutex, MutexGuard};

pub(crate) use std::sync::PoisonError;

pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> { mutex.lock().unwrap_or_else(PoisonError::into_inner) }
