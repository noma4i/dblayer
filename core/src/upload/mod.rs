mod config;
mod input;
mod machine;
mod plan;
mod progress;
mod source;
mod state;

pub use config::{UploadConfig, MEBIBYTE};
pub use input::{MultipartSteps, SingleSteps, UploadBatchInput, UploadInput, UploadItem, UploadSteps};
pub use plan::{mime_of, parse_headers, part_count, part_range, scenario_for};
pub use progress::TransferProgress;
pub(crate) use machine::{RunMode, UploadEvent};
#[cfg(test)]
pub(crate) use machine::Refused;
pub use source::{file_len, md5_base64, read_range};
pub use state::{UploadInfo, UploadPart, UploadRequest, UploadRetry, UploadScenario, UploadStage, UploadState, UploadStatus, WaitReason};

#[cfg(test)]
mod config_tests;
#[cfg(test)]
mod machine_tests;
#[cfg(test)]
mod plan_tests;
#[cfg(test)]
mod progress_tests;
#[cfg(test)]
mod source_tests;
#[cfg(test)]
mod state_tests;
