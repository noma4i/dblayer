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
    /// Waits until every queued change of the record is answered. Fails with the refusal, when a change is refused or its outcome is unknown.
    pub async fn settled(&self, model: &str, id: &RowId) -> Result<(), SyncError> {
        let waiting = |row: &RowId| -> Result<Vec<i64>, SyncError> {
            Ok(self
                .inner
                .db
                .outbox()?
                .iter()
                .filter(|entry| entry.operation.status == OperationStatus::Pending && (entry.operation.row_ids.contains(row) || entry.operation.temp_id.as_ref() == Some(row)))
                .map(|entry| entry.operation.id.0)
                .collect())
        };
        let mut pending = waiting(id)?;
        while !pending.is_empty() {
            if !self.inner.connectivity.is_online() {
                return Err(SyncError::Offline);
            }
            self.flush().await?;
            let failure = {
                let outcomes = locked(&self.inner.outcomes);
                pending.iter().find_map(|sequence| match outcomes.get(sequence) {
                    Some(SendResult::Rejected(message)) => Some(SyncError::Graphql(message.clone())),
                    Some(SendResult::DeliveryUnknown | SendResult::Quarantined) => Some(SyncError::Store(format!("{model} {id}: the answer is unknown"))),
                    Some(SendResult::Discarded) => Some(SyncError::Cancelled),
                    _ => None,
                })
            };
            if let Some(failure) = failure {
                return Err(failure);
            }
            pending = waiting(id)?;
            if !pending.is_empty() {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
        Ok(())
    }

    /// The server id of a record created locally. Waits for the queued `create` to be answered.
    pub async fn confirmed(&self, model: &str, id: &RowId) -> Result<RowId, SyncError> {
        if !id.is_temp() {
            return Ok(id.clone());
        }
        self.settled(model, id).await?;
        let key = format!("{model}\0{id}");
        let real = locked(&self.inner.replaced).get(&key).cloned();
        match real {
            Some(real) => RowId::new(&real).map_err(|error| SyncError::Malformed(error.to_string())),
            None => Err(SyncError::Store(format!("{model} {id} was not confirmed"))),
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
