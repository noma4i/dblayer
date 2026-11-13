//! Executor of the effects of `Rounds`: it asks the platform for a job and ends a job. The platform answers by calls that become inputs of `Rounds`.

use std::sync::Arc;

use super::rounds::{RoundId, Rounds, RoundsEffect};
use crate::owners::contract::RequestId;
use crate::owners::contract::Drained;
use crate::runtime::{Back, EffectRunner, Epoch, EpochStamp};

/// The platform side of the round (the background job of Android).
pub trait RoundJobs: Send + Sync + 'static {
    fn schedule_job(&self, request_id: RequestId, epoch: Epoch);
    fn end_job(&self, round_id: RoundId);
    fn progress(&self, round_id: RoundId, fraction: u32);
}

pub(crate) struct RoundsExecutor {
    jobs: Arc<dyn RoundJobs>,
    drained: Arc<dyn Fn(Drained) + Send + Sync>,
}

impl RoundsExecutor {
    pub fn new(jobs: Arc<dyn RoundJobs>, drained: Arc<dyn Fn(Drained) + Send + Sync>) -> Self { Self { jobs, drained } }
}

impl EffectRunner<Rounds> for RoundsExecutor {
    fn run(&mut self, _stamp: EpochStamp, effect: RoundsEffect, _back: &Back<Rounds>) {
        match effect {
            RoundsEffect::ScheduleJob { request_id, epoch } => self.jobs.schedule_job(request_id, epoch),
            RoundsEffect::EndJob { round_id } => self.jobs.end_job(round_id),
            RoundsEffect::Progress { round_id, fraction } => self.jobs.progress(round_id, fraction),
            RoundsEffect::Drained(drained) => (self.drained)(drained),
        }
    }
}
