use std::time::{Duration, Instant};

use super::sync::{lock, Condvar, Mutex, PoisonError};
use crate::blocking::blocking_unit;

pub struct PublishedCell<T> {
    value: Mutex<Option<T>>,
    changed: Condvar,
}

impl<T> Default for PublishedCell<T> {
    fn default() -> Self { Self { value: Mutex::new(None), changed: Condvar::new() } }
}

impl<T: Clone> PublishedCell<T> {
    pub fn publish(&self, value: T) {
        *lock(&self.value) = Some(value);
        crate::db::lock::effect(|| self.changed.notify_all());
    }

    pub fn get(&self) -> Option<T> { lock(&self.value).clone() }

    pub fn read<R>(&self, read: impl FnOnce(Option<&T>) -> R) -> R { read(lock(&self.value).as_ref()) }

    pub fn notify(&self) {
        drop(lock(&self.value));
        crate::db::lock::effect(|| self.changed.notify_all());
    }

    pub fn wait_for<R>(&self, limit: Duration, mut ready: impl FnMut() -> Option<R>) -> Option<R> {
        let deadline = Instant::now() + limit;
        let mut guard = lock(&self.value);
        loop {
            if let Some(found) = ready() {
                return Some(found);
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return None;
            }
            guard = blocking_unit(|| self.changed.wait_timeout(guard, left).unwrap_or_else(PoisonError::into_inner).0);
        }
    }
}
