mod flush;
mod pause;
mod report;
mod settle;
mod variables;

pub(crate) use flush::{Delivery, Sending};
pub use report::{FlushReport, SendResult, SentOperation, SkipReason};

use crate::commit::OperationStatus;
use crate::identity::RowId;

use super::error::SyncError;
use super::{locked, Sync};

impl Sync {
    /// The server id of a record created locally. Waits for the queued `create` to be answered.
    pub async fn confirmed(&self, model: &str, id: &RowId) -> Result<RowId, SyncError> {
        if !id.is_temp() {
            return Ok(id.clone());
        }
        let key = format!("{model}\0{id}");
        loop {
            if let Some(real) = locked(&self.inner.replaced).get(&key) {
                return RowId::new(real).map_err(|error| SyncError::Malformed(error.to_string()));
            }
            let waiting = self.inner.db.outbox()?.iter().any(|entry| entry.operation.temp_id.as_ref() == Some(id) && entry.operation.status == OperationStatus::Pending);
            if !waiting {
                return Err(SyncError::Store(format!("{model} {id} was not confirmed")));
            }
            if !self.inner.connectivity.is_online() {
                return Err(SyncError::Offline);
            }
            self.flush().await?;
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

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
mod failure_tests;
#[cfg(test)]
mod flush_tests;
#[cfg(test)]
mod follower_tests;
#[cfg(test)]
mod network_tests;
#[cfg(test)]
mod order_tests;
#[cfg(test)]
mod pass_tests;
#[cfg(test)]
mod reset_tests;
#[cfg(test)]
mod rollback_tests;
#[cfg(test)]
mod variables_tests;
