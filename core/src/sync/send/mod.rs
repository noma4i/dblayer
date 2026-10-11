mod flush;
mod outcomes;
mod pause;
mod report;
mod settle;
mod variables;

pub(crate) use flush::{Delivery, Sending};
pub(crate) use outcomes::Outcomes;
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
        if let Some(failure) = locked(&self.inner.outcomes).failure_of(model, id, &pending) {
            return Err(failure);
        }
        while !pending.is_empty() {
            if !self.inner.connectivity.is_online() {
                return Err(SyncError::Offline);
            }
            self.flush().await?;
            pending = waiting(id)?;
            if let Some(failure) = locked(&self.inner.outcomes).failure_of(model, id, &pending) {
                return Err(failure);
            }
            if !pending.is_empty() {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
        Ok(())
    }

    /// Waits until the queued action is answered and returns the declared `result` fields of its payload. Fails with the refusal. An answer is read once.
    pub async fn answer(&self, model: &str, operation: i64) -> Result<serde_json::Value, SyncError> {
        let waiting = || -> Result<bool, SyncError> { Ok(self.inner.db.outbox()?.iter().any(|entry| entry.operation.status == OperationStatus::Pending && entry.operation.id.0 == operation)) };
        while waiting()? {
            if !self.inner.connectivity.is_online() {
                return Err(SyncError::Offline);
            }
            self.flush().await?;
            if waiting()? {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
        locked(&self.inner.outcomes).take(model, operation)
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

    /// Opens the model-level subscriptions declared with `listen`.
    pub fn listen(&self) {
        self.inner.listen_all();
    }

    pub fn unlisten(&self) {
        self.inner.unlisten_all();
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
mod answer_tests;
#[cfg(test)]
mod correlate_tests;
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
mod outcomes_tests;
#[cfg(test)]
mod pass_tests;
#[cfg(test)]
mod reset_tests;
#[cfg(test)]
mod rollback_tests;
#[cfg(test)]
mod variables_tests;
