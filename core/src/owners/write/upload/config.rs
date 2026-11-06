use std::time::Duration;

use crate::runtime::{Backoff, RetryPolicy};

pub const MEBIBYTE: u64 = 1024 * 1024;

#[derive(Debug, Clone, PartialEq)]
pub struct UploadConfig {
    pub multipart_threshold: u64,
    pub part_size: u64,
    pub parallel_parts: usize,
    pub max_files: usize,
    pub backoff: Backoff,
    pub put_timeout: Duration,
    pub step_timeout: Duration,
    pub presign_ttl: Duration,
    pub tick: Duration,
    pub failed_ttl: Duration,
}

impl Default for UploadConfig {
    fn default() -> Self {
        Self {
            multipart_threshold: 10 * MEBIBYTE,
            part_size: 5 * MEBIBYTE,
            parallel_parts: 3,
            max_files: 1,
            backoff: RetryPolicy::Upload.backoff().randomized(),
            put_timeout: Duration::from_secs(120),
            step_timeout: Duration::from_secs(45),
            presign_ttl: Duration::from_secs(50 * 60),
            tick: Duration::from_millis(100),
            failed_ttl: Duration::from_secs(7 * 24 * 60 * 60),
        }
    }
}
