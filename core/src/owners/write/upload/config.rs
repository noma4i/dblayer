use std::time::Duration;

pub const MEBIBYTE: u64 = 1024 * 1024;

#[derive(Debug, Clone, PartialEq)]
pub struct UploadConfig {
    pub multipart_threshold: u64,
    pub part_size: u64,
    pub put_timeout: Duration,
    pub step_timeout: Duration,
    pub presign_ttl: Duration,
    pub tick: Duration,
    pub failed_ttl: Duration,
    /// The platform runs a PUT outside the process (`OpenPorts.detached_put`).
    pub detached_put: bool,
}

impl Default for UploadConfig {
    fn default() -> Self {
        Self {
            multipart_threshold: 10 * MEBIBYTE,
            part_size: 5 * MEBIBYTE,
            put_timeout: Duration::from_secs(120),
            step_timeout: Duration::from_secs(45),
            presign_ttl: Duration::from_secs(50 * 60),
            tick: Duration::from_millis(100),
            failed_ttl: Duration::from_secs(7 * 24 * 60 * 60),
            detached_put: false,
        }
    }
}
