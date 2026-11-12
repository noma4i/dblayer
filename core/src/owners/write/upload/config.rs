use std::time::Duration;

pub const MEBIBYTE: u64 = 1024 * 1024;

#[derive(Debug, Clone, PartialEq)]
pub struct UploadConfig {
    pub multipart_threshold: u64,
    pub part_size: u64,
    pub put_timeout: Duration,
}

impl Default for UploadConfig {
    fn default() -> Self {
        Self {
            multipart_threshold: 10 * MEBIBYTE,
            part_size: 5 * MEBIBYTE,
            put_timeout: Duration::from_secs(120),
        }
    }
}
