use std::collections::BTreeMap;

use super::sync::{lock, Arc, Mutex};

pub struct Running(Option<Box<dyn FnOnce() + Send>>);

impl Running {
    pub fn new(cancel: impl FnOnce() + Send + 'static) -> Self { Self(Some(Box::new(cancel))) }

    pub fn cancel(mut self) {
        if let Some(cancel) = self.0.take() {
            cancel();
        }
    }
}

pub struct Flights<K: Ord>(Arc<Mutex<BTreeMap<K, Running>>>);

impl<K: Ord> Clone for Flights<K> {
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

impl<K: Ord> Default for Flights<K> {
    fn default() -> Self { Self(Arc::new(Mutex::new(BTreeMap::new()))) }
}

impl<K: Ord> Flights<K> {
    pub fn put(&self, key: K, running: Running) {
        let old = lock(&self.0).insert(key, running);
        if let Some(old) = old {
            old.cancel();
        }
    }

    pub fn done(&self, key: &K) { drop(lock(&self.0).remove(key)); }

    pub fn cancel(&self, key: &K) {
        let running = lock(&self.0).remove(key);
        if let Some(running) = running {
            running.cancel();
        }
    }

    pub fn len(&self) -> usize { lock(&self.0).len() }

    pub fn is_empty(&self) -> bool { lock(&self.0).is_empty() }
}
