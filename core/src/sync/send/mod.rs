mod flush;
mod pause;
mod report;
mod settle;
mod variables;

pub(crate) use flush::{Delivery, Sending};
pub use report::{FlushReport, SendResult, SentOperation, SkipReason};

use crate::identity::RowId;

use super::error::SyncError;
use super::Sync;

impl Sync {
    pub async fn flush(&self) -> Result<FlushReport, SyncError> {
        let report = self.inner.flush().await?;
        self.start_uploads()?;
        self.arm_wake();
        Ok(report)
    }

    pub async fn retry(&self, model: &str, id: &RowId) -> Result<FlushReport, SyncError> {
        self.inner.db.retry(model, id)?;
        self.flush().await
    }

    pub async fn retry_all(&self) -> Result<FlushReport, SyncError> {
        self.inner.db.retry_all()?;
        self.flush().await
    }
}

#[cfg(test)]
mod flush_tests;
#[cfg(test)]
mod network_tests;
#[cfg(test)]
mod rollback_tests;
#[cfg(test)]
mod variables_tests;
