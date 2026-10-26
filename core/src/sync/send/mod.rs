mod flush;
mod outcomes;
mod pause;
#[cfg(test)] mod pause_tests;
mod report;
mod settle;
#[cfg(test)] mod swap_window_tests;
mod variables;

pub(crate) use flush::{Sending, FinalAttempts};
pub(crate) use outcomes::Outcomes;
pub use report::{FlushReport, SendResult, SentOperation, SkipReason};

use crate::operation::OperationStatus;
use crate::identity::RowId;

use super::error::SyncError;
use super::{locked, Inner, Sync};

impl Inner {
    pub(crate) async fn flush_pending(self: &std::sync::Arc<Self>) -> Result<FlushReport, SyncError> {
        if self.upload_housekeeping_due.swap(false, std::sync::atomic::Ordering::SeqCst) {
            if let Err(error) = self.expire_uploads().await {
                self.upload_housekeeping_due.store(true, std::sync::atomic::Ordering::SeqCst);
                return Err(error);
            }
            self.spawn_upload_housekeeping(false);
        }
        let report = self.flush().await?;
        self.start_uploads()?;
        self.arm_wake()?;
        Ok(report)
    }
}

fn failed_upload_error(row: &crate::db::Row, action: Option<&str>) -> Option<crate::syncstate::OperationFailure> {
    let failed = |upload: &crate::upload::UploadInfo| upload.status == crate::upload::UploadStatus::Failed;
    match action {
        Some(action) => row.sync.uploads.get(action).filter(|upload| failed(upload)).and_then(|upload| upload.error.clone()),
        None => row.sync.uploads.values().any(failed).then(|| row.sync.error.clone()).flatten(),
    }
}

impl Sync {
    pub async fn settled(&self, model: &str, id: &RowId, action: Option<&str>) -> Result<(), SyncError> {
        let key = self.inner.db.action_operation_key(model, action)?;
        let waiting = |row: &RowId| -> Result<Vec<i64>, SyncError> {
            if let Some(failure) = self.inner.db.find(model, row)?.and_then(|row| failed_upload_error(&row, action)) {
                return Err(outcomes::error_of(model, &SendResult::Rejected(failure), row.as_str()));
            }
            Ok(locked(self.inner.operation_queue()?).pending_of(&crate::identity::RowRef::new(crate::identity::ModelKey::new(model), row.clone()), key.as_deref()))
        };
        let mut changes = self.inner.db.outbox_changes();
        let mut pending = waiting(id)?;
        if let Some(failure) = locked(&self.inner.outcomes).failure_of(model, key.as_deref(), id, &pending) {
            return Err(failure);
        }
        while !pending.is_empty() {
            if !self.inner.connectivity.is_online() {
                return Err(SyncError::Offline);
            }
            self.flush().await?;
            changes.borrow_and_update();
            pending = waiting(id)?;
            if let Some(failure) = locked(&self.inner.outcomes).failure_of(model, key.as_deref(), id, &pending) {
                return Err(failure);
            }
            if !pending.is_empty() {
                self.park(&mut changes).await;
            }
        }
        Ok(())
    }

    /// Waits until the queued action is answered and returns the declared `result` fields of its payload. Fails with the refusal. An answer is read once.
    pub async fn answer(&self, model: &str, operation: i64) -> Result<serde_json::Value, SyncError> {
        let waiting = || -> Result<bool, SyncError> { Ok(locked(self.inner.operation_queue()?).entries.get(&operation).is_some_and(|entry| entry.operation.status == OperationStatus::Pending)) };
        let mut changes = self.inner.db.outbox_changes();
        while waiting()? {
            if !self.inner.connectivity.is_online() {
                return Err(SyncError::Offline);
            }
            self.flush().await?;
            changes.borrow_and_update();
            if waiting()? {
                self.park(&mut changes).await;
            }
        }
        locked(&self.inner.outcomes).take(model, operation)
    }

    async fn park(&self, changes: &mut tokio::sync::watch::Receiver<u64>) {
        locked(&self.inner.counters).record_settle_park();
        tokio::select! {
            _ = changes.changed() => {}
            () = self.inner.connectivity.offline() => {}
        }
    }

    pub async fn confirmed(&self, model: &str, id: &RowId) -> Result<RowId, SyncError> {
        if !id.is_temp() {
            return Ok(id.clone());
        }
        self.settled(model, id, None).await?;
        let key = format!("{model}\0{id}");
        let real = locked(&self.inner.db.shared.replaced_ids).get(&key).cloned();
        match real {
            Some(real) => RowId::new(&real).map_err(|error| SyncError::Malformed(error.to_string())),
            None => Err(SyncError::Store(format!("{model} {id} was not confirmed"))),
        }
    }

    pub fn listen(&self) {
        self.inner.transport.resume_subscriptions();
        self.inner.restart_live(super::resume_machine::Cause::Explicit);
        self.inner.listen_all();
    }

    pub fn unlisten(&self) {
        self.inner.unlisten_all();
    }

    pub async fn flush(&self) -> Result<FlushReport, SyncError> {
        self.inner.flush_pending().await
    }

    pub async fn retry(&self, model: &str, id: &RowId, action: Option<&str>) -> Result<FlushReport, SyncError> {
        self.inner.db.retry(model, id, action)?;
        self.flush().await
    }

    pub async fn retry_all(&self) -> Result<FlushReport, SyncError> {
        self.inner.db.retry_all()?;
        self.flush().await
    }

    pub async fn discard(&self, model: &str, id: &RowId, action: Option<&str>) -> Result<(), SyncError> {
        self.inner.db.discard(model, id, action)?;
        self.inner.start_uploads()?;
        Ok(())
    }
}

#[cfg(test)]
mod answer_tests;
#[cfg(test)]
pub(super) mod confirmed_tests;
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
mod settle_wait_tests;
#[cfg(test)]
mod rollback_tests;
#[cfg(test)]
mod variables_tests;
#[cfg(test)]
mod idempotency_tests;
#[cfg(test)]
mod created_rows_tests;
