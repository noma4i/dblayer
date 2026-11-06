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
        crate::lock::effect(|| self.changed.notify_all());
    }

    pub fn get(&self) -> Option<T> { lock(&self.value).clone() }

    pub fn read<R>(&self, read: impl FnOnce(Option<&T>) -> R) -> R { read(lock(&self.value).as_ref()) }

    pub fn notify(&self) {
        drop(lock(&self.value));
        crate::lock::effect(|| self.changed.notify_all());
    }

    pub fn wait_until<R>(&self, mut ready: impl FnMut() -> Option<R>) -> R {
        let mut guard = lock(&self.value);
        loop {
            if let Some(found) = ready() {
                return found;
            }
            guard = blocking_unit(|| self.changed.wait(guard).unwrap_or_else(PoisonError::into_inner));
        }
    }
}
