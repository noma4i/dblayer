//! What the executors of direction C use to reach the world: tasks and the network. An executor does IO and returns the result as an input
//! to its own owner (`design.md` F3). It never decides a transition.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};

use crate::lifecycle::Admitted;
use crate::transport::Transport;

pub type Task = Pin<Box<dyn Future<Output = ()> + Send>>;

/// A task that runs; `cancel` stops it and nothing it would post arrives afterwards.
pub struct Running(Option<Box<dyn FnOnce() + Send>>);

impl Running {
    pub fn new(cancel: impl FnOnce() + Send + 'static) -> Self { Self(Some(Box::new(cancel))) }

    pub fn cancel(mut self) {
        if let Some(cancel) = self.0.take() {
            cancel();
        }
    }
}

pub trait Tasks: Send + Sync {
    fn start(&self, task: Task) -> Running;
    /// Blocking IO (file copy) off the owner thread.
    fn start_blocking(&self, job: Box<dyn FnOnce() + Send>) -> Running;
}

/// Production tasks: a tokio runtime, through `crate::task`.
pub struct TokioTasks(pub tokio::runtime::Handle);

impl Tasks for TokioTasks {
    fn start(&self, task: Task) -> Running {
        let handle = crate::task::spawn_on(crate::task::Carry::Internal, &self.0, task);
        Running::new(move || crate::db::lock::effect(|| handle.abort()))
    }

    fn start_blocking(&self, job: Box<dyn FnOnce() + Send>) -> Running {
        let handle = crate::task::spawn_blocking_on(crate::task::Carry::Internal, &self.0, job);
        Running::new(move || crate::db::lock::effect(|| handle.abort()))
    }
}

/// The network of an executor: the transport and the admission of the epoch it works in.
#[derive(Clone)]
pub struct Wire {
    pub transport: Arc<dyn Transport>,
    pub admit: Arc<dyn Fn() -> Option<Admitted> + Send + Sync>,
    pub tasks: Arc<dyn Tasks>,
}

impl Drop for Wire {
    fn drop(&mut self) { crate::db::lock::assert_no_library_lock("Wire"); }
}

/// Tasks in flight by key.
pub struct Flights<K: Ord>(Arc<Mutex<std::collections::BTreeMap<K, Running>>>);

impl<K: Ord> Clone for Flights<K> {
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

impl<K: Ord> Default for Flights<K> {
    fn default() -> Self { Self(Arc::default()) }
}

impl<K: Ord> Flights<K> {
    /// Keeps `running` under `key`; the task it replaces is cancelled after the lock is released.
    pub fn put(&self, key: K, running: Running) {
        let old = self.0.lock().unwrap_or_else(PoisonError::into_inner).insert(key, running);
        if let Some(old) = old {
            old.cancel();
        }
    }

    /// The task ended by itself: forget it without cancelling.
    pub fn done(&self, key: &K) { drop(self.0.lock().unwrap_or_else(PoisonError::into_inner).remove(key)); }

    pub fn cancel(&self, key: &K) {
        let running = self.0.lock().unwrap_or_else(PoisonError::into_inner).remove(key);
        if let Some(running) = running {
            running.cancel();
        }
    }

    pub fn len(&self) -> usize { self.0.lock().unwrap_or_else(PoisonError::into_inner).len() }
}
