//! What the executors of direction C use to reach the world: tasks and the network. An executor does IO and returns the result as an input
//! to its own owner. It never decides a transition.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;


pub type Task = Pin<Box<dyn Future<Output = ()> + Send>>;

pub use crate::runtime::{Flights, Running};

pub trait Tasks: Send + Sync {
    fn start(&self, task: Task) -> Running;
    /// Blocking IO (file copy) off the owner thread.
    fn start_blocking(&self, job: Box<dyn FnOnce() + Send>) -> Running;
}

/// Production tasks: a tokio runtime, through `crate::task`.
/// The second field is the owners runtime whose barrier waits for the tasks until their first poll or the end of a blocking job.
pub struct TokioTasks(pub tokio::runtime::Handle, pub Option<crate::runtime::Handle>);

impl Tasks for TokioTasks {
    fn start(&self, task: Task) -> Running {
        let launch = self.1.as_ref().map(crate::runtime::Handle::launch);
        let handle = crate::task::spawn_on(&self.0, crate::runtime::counted(launch, task));
        Running::new(move || crate::lock::effect(|| handle.abort()))
    }

    fn start_blocking(&self, job: Box<dyn FnOnce() + Send>) -> Running {
        let launch = self.1.as_ref().map(crate::runtime::Handle::launch);
        let handle = crate::task::spawn_blocking_on(&self.0, launch, move || {
            job()
        });
        Running::new(move || crate::lock::effect(|| handle.abort()))
    }
}

/// The request id of a step of Transfer: a function of the transfer key, the cycle, the step and the part, so a repeat and a restart carry the same id (bit 62 keeps it apart from the counter ids).
pub fn step_request_id(key: &str, cycle_id: u64, step: crate::owners::write::upload::UploadStage, part: u32) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    let mut feed = |bytes: &[u8]| for byte in bytes { hash = (hash ^ u64::from(*byte)).wrapping_mul(0x0100_0000_01b3); };
    feed(key.as_bytes());
    feed(&cycle_id.to_le_bytes());
    feed(&[step as u8]);
    feed(&part.to_le_bytes());
    (hash & ((1 << 62) - 1)) | (1 << 62)
}

/// The network of an executor: the transport and the gate of the epoch it works in.
#[derive(Clone)]
pub struct Wire {
    pub gate: Arc<crate::runtime::EpochGate>,
    pub tasks: Arc<dyn Tasks>,
}

impl Drop for Wire {
    fn drop(&mut self) { crate::lock::assert_no_library_lock("Wire"); }
}
