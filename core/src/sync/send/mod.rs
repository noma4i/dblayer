mod flush;
mod report;
mod settle;
mod variables;

pub use report::{FlushReport, SendResult, SentOperation, SkipReason};

use super::error::SyncError;
use super::Sync;

impl Sync {
    pub async fn flush(&self) -> Result<FlushReport, SyncError> {
        self.inner.flush().await
    }
}

#[cfg(test)]
mod flush_tests;
#[cfg(test)]
mod variables_tests;
