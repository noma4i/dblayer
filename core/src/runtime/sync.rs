#[cfg(not(dblayer_loom))]
pub(crate) use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
#[cfg(test)]
pub(crate) use std::sync::atomic::AtomicUsize;
#[cfg(not(dblayer_loom))]
pub(crate) use std::sync::{Arc, Condvar, Mutex, MutexGuard};

#[cfg(dblayer_loom)]
pub(crate) use loom::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
#[cfg(dblayer_loom)]
pub(crate) use loom::sync::{Arc, Condvar, Mutex, MutexGuard};

pub(crate) use std::sync::PoisonError;

pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> { mutex.lock().unwrap_or_else(PoisonError::into_inner) }
