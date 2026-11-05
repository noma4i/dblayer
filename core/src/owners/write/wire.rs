//! What the executors of direction C use to reach the world: tasks and the network. An executor does IO and returns the result as an input
//! to its own owner (`design.md` F3). It never decides a transition.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::lifecycle::Admitted;
use crate::transport::Transport;

pub type Task = Pin<Box<dyn Future<Output = ()> + Send>>;

pub use crate::runtime::{Flights, Running};

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
