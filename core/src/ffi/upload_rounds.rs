//! The platform side of the round of Android: the job the owner Rounds asks for and the three calls that answer it.

use std::sync::{Arc, OnceLock};

use crate::owners::contract::RequestId;
use crate::owners::write::rounds::RoundId;
use crate::owners::write::rounds_executor::RoundJobs;
use crate::runtime::Epoch;

use super::Database;

/// The platform schedules and ends the system job. It answers with `round_started`, `round_refused` and `round_ended`.
#[uniffi::export(with_foreign)]
pub trait RoundJobsSink: Send + Sync {
    fn schedule_job(&self, request_id: u64, epoch: u64);
    fn end_job(&self, round_id: u32);
}

/// The sink the platform sets after `open`: a job asked for before it is set is not scheduled, the round asks again at the next need.
#[derive(Default)]
pub(super) struct RoundJobsSlot(OnceLock<Arc<dyn RoundJobsSink>>);

impl Drop for RoundJobsSlot {
    fn drop(&mut self) { crate::db::lock::assert_no_library_lock("RoundJobsSlot drop"); }
}

impl RoundJobs for RoundJobsSlot {
    fn schedule_job(&self, request_id: RequestId, epoch: Epoch) {
        if let Some(sink) = self.0.get() {
            crate::db::lock::assert_no_library_lock("RoundJobsSink::schedule_job");
            sink.schedule_job(request_id, epoch);
        }
    }

    fn end_job(&self, round_id: RoundId) {
        if let Some(sink) = self.0.get() {
            crate::db::lock::assert_no_library_lock("RoundJobsSink::end_job");
            sink.end_job(round_id);
        }
    }
}

impl RoundJobsSlot {
    pub(super) fn set(&self, sink: Arc<dyn RoundJobsSink>) -> bool { self.0.set(sink).is_ok() }
}

#[uniffi::export]
impl Database {
    pub fn set_round_jobs(&self, sink: Arc<dyn RoundJobsSink>) { let _ = self.jobs.set(sink); }

    pub fn round_started(&self, request_id: u64, round_id: u32, epoch: u64) { self.life.round_started(request_id, round_id, epoch); }

    pub fn round_refused(&self, request_id: u64, epoch: u64, reason: String) { self.life.round_refused(request_id, epoch, reason); }

    pub fn round_ended(&self, round_id: u32, epoch: u64) { self.life.round_ended(round_id, epoch); }
}
