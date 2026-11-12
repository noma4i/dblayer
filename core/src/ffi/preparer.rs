//! The port of the platform that reads and writes media for the prepare step: it reads the metadata of a source and writes the result by a plan of the core.

use std::sync::Arc;
use async_trait::async_trait;
use crate::prepare::{ImageFormat, PhotoPlan, VideoPlan, VideoSource};

#[uniffi::export(with_foreign)]
#[async_trait]
pub trait Preparer: Send + Sync {
    /// Reads the metadata of the file at `path`.
    async fn probe(&self, path: String) -> ProbeOutcome;
    /// Writes `job.destination` by `job.plan`, reports the done part of the work to `progress`.
    async fn run(&self, job: PrepareJob, progress: Arc<PrepareProgress>) -> PrepareOutcome;
    /// Stops the work of the attempt: no file stays and no outcome is needed.
    fn cancel(&self, attempt_id: u64);
}

/// What the platform read from a source file.
#[derive(Debug, Clone, PartialEq, uniffi::Enum)]
pub enum ProbeOutcome {
    Video { source: VideoSource },
    Image { width: u32, height: u32, exif_orientation: u32, format: ImageFormat },
    Failed { message: String },
}

/// One run of the platform: the plan is the whole decision, the platform decides nothing.
#[derive(Debug, Clone, PartialEq, uniffi::Record)]
pub struct PrepareJob {
    pub attempt_id: u64,
    pub source: String,
    pub destination: String,
    pub plan: PreparePlan,
}

#[derive(Debug, Clone, PartialEq, uniffi::Enum)]
pub enum PreparePlan {
    Video { plan: VideoPlan, audio_bitrate: u32, audio_channels: u32, audio_sample_rate: u32, mirror: bool },
    Image { plan: PhotoPlan, quality: f64, format: ImageFormat },
}

#[derive(Debug, Clone, PartialEq, Eq, uniffi::Enum)]
pub enum PrepareOutcome {
    Done { name: Option<String>, mime: Option<String> },
    Failed { message: String },
}

/// The done part of a run, 0.0 to 1.0, as the platform counts it.
#[derive(uniffi::Object)]
pub struct PrepareProgress { report: Box<dyn Fn(f64) + Send + Sync> }

impl PrepareProgress {
    pub fn new(report: impl Fn(f64) + Send + Sync + 'static) -> Self { Self { report: Box::new(report) } }
}

#[uniffi::export]
impl PrepareProgress {
    pub fn report(&self, fraction: f64) {
        crate::lock::assert_no_library_lock("PrepareProgress::report");
        (self.report)(fraction);
    }
}
