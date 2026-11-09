//! The platform side of the round of Android: the job the owner Rounds asks for and the three calls that answer it.

use std::sync::{Arc, OnceLock};

use crate::owners::contract::RequestId;
use crate::owners::write::rounds::RoundId;
use crate::owners::write::rounds_executor::RoundJobs;
use crate::runtime::Epoch;

use super::Database;

/// The round the platform schedules: the core numbers requests and rounds, the platform returns these values unchanged in `round_started`, `round_refused` and `round_ended`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Record)]
pub struct RoundJob {
    pub request_id: u64,
    pub round_id: u32,
    pub epoch: u64,
}

/// Why the system did not take a round: the platform names the kind of its refusal and passes the text of the system as `detail`, the core words the reason the row shows.
#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum RoundRefusal {
    JobRefused,
    StartNotAllowed,
    SchedulerFailed,
}

impl RoundRefusal {
    fn reason(self, detail: &str) -> String {
        match self {
            Self::JobRefused => "the system refused the user-initiated job while the app was not visible".into(),
            Self::StartNotAllowed => format!("the system refused to start the upload service: {detail}"),
            Self::SchedulerFailed => format!("the system scheduler failed: {detail}"),
        }
    }
}

/// The platform schedules and ends the system job. It answers with `round_started`, `round_refused` and `round_ended`.
#[uniffi::export(with_foreign)]
pub trait RoundJobsSink: Send + Sync {
    fn schedule_job(&self, job: RoundJob);
    fn end_job(&self, round_id: u32);
}

/// The sink the platform sets after `open`: a job asked for before it is set is not scheduled, the round asks again at the next need.
#[derive(Default)]
pub(super) struct RoundJobsSlot(OnceLock<Arc<dyn RoundJobsSink>>);

impl Drop for RoundJobsSlot {
    fn drop(&mut self) { crate::lock::assert_no_library_lock("RoundJobsSlot drop"); }
}

impl RoundJobs for RoundJobsSlot {
    fn schedule_job(&self, request_id: RequestId, epoch: Epoch) {
        if let Some(sink) = self.0.get() {
            crate::lock::assert_no_library_lock("RoundJobsSink::schedule_job");
            sink.schedule_job(RoundJob { request_id, round_id: crate::runtime::next_id(crate::runtime::Counter::Round) as u32, epoch });
        }
    }

    fn end_job(&self, round_id: RoundId) {
        if let Some(sink) = self.0.get() {
            crate::lock::assert_no_library_lock("RoundJobsSink::end_job");
            sink.end_job(round_id);
        }
    }
}

impl RoundJobsSlot {
    pub(super) fn set(&self, sink: Arc<dyn RoundJobsSink>) -> bool { self.0.set(sink).is_ok() }
}

#[uniffi::export]
impl Database {
    pub fn set_round_jobs(&self, sink: Arc<dyn RoundJobsSink>) { if self.jobs.set(sink) {
            self.life.jobs_sink_set();
        }
    }

    pub fn round_started(&self, request_id: u64, round_id: u32, epoch: u64) {
        if self.life.has_rounds() { self.life.round_started(request_id, round_id, epoch); } else { RoundJobs::end_job(&*self.jobs, round_id); }
    }

    pub fn round_refused(&self, request_id: u64, epoch: u64, kind: RoundRefusal, detail: String) { self.life.round_refused(request_id, epoch, kind.reason(&detail)); }

    pub fn round_ended(&self, round_id: u32, epoch: u64) { self.life.round_ended(round_id, epoch); }
}
