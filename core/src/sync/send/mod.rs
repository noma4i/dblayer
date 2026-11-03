mod flush;
mod outcomes;
#[cfg(test)] mod answer_wait_tests;
mod pause;
#[cfg(test)] mod pause_tests;
mod report;
mod settle;
#[cfg(test)] mod swap_window_tests;
mod variables;

pub(crate) use flush::{Sending, FinalAttempts};
pub(crate) use outcomes::Outcomes;
pub use report::{FlushReport, SendResult, SentOperation, SkipReason};

use std::collections::HashMap;

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
        let lease = self.inner.lease(crate::lifecycle::Origin::Explicit, "settled")?;
        lease.run(Box::pin(self.settled_in(model, id, action))).await?
    }

    async fn settled_in(&self, model: &str, id: &RowId, action: Option<&str>) -> Result<(), SyncError> {
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

    /// The row at the root of the answer to an operation, once: `None` when no answer was published, `Some(None)` when the answer named no row.
    /// The call that queued `operation` waits for its answer: the result stays until the guard is dropped.
    pub fn expect_answer(&self, operation: i64) -> AnswerWish {
        locked(&self.inner.answers).want(operation);
        AnswerWish { inner: self.inner.clone(), operation }
    }

    pub fn answer_root(&self, operation: i64) -> Option<Option<RowId>> {
        locked(&self.inner.answers).take(operation)
    }

    /// Waits until the queued action is answered and returns the declared `result` fields of its payload. Fails with the refusal. An answer is read once.
    pub async fn answer(&self, model: &str, operation: i64) -> Result<serde_json::Value, SyncError> {
        let lease = self.inner.lease(crate::lifecycle::Origin::Explicit, "answer")?;
        lease.run(Box::pin(self.answer_in(model, operation))).await?
    }

    pub async fn settled_operation(&self, model: &str, operation: i64) -> Result<(), SyncError> {
        let lease = self.inner.lease(crate::lifecycle::Origin::Explicit, "settled")?;
        lease.run(Box::pin(async move {
            self.until_answered(operation).await?;
            match locked(&self.inner.outcomes).refusal_of(model, operation) {
                Some(refusal) => Err(refusal),
                None => Ok(()),
            }
        })).await?
    }

    async fn answer_in(&self, model: &str, operation: i64) -> Result<serde_json::Value, SyncError> {
        self.until_answered(operation).await?;
        locked(&self.inner.outcomes).take(model, operation)
    }

    async fn until_answered(&self, operation: i64) -> Result<(), SyncError> {
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
        Ok(())
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
        let key = format!("{model}\0{id}");
        let create = locked(&self.inner.db.shared.creates).get(&key).copied();
        let replaced = || locked(&self.inner.db.shared.replaced_ids).get(&key).cloned();
        match create {
            Some(operation) => self.settled_operation(model, operation).await?,
            None if replaced().is_none() => self.settled(model, id, None).await?,
            None => {}
        }
        let real = replaced();
        match real {
            Some(real) => RowId::new(&real).map_err(|error| SyncError::Malformed(error.to_string())),
            None => Err(SyncError::Store(format!("{model} {id} was not confirmed"))),
        }
    }

    pub async fn confirmed_by(&self, model: &str, id: &RowId, operation: i64) -> Result<RowId, SyncError> {
        self.settled_operation(model, operation).await?;
        let real = locked(&self.inner.db.shared.replaced_ids).get(&format!("{model}\0{id}")).cloned();
        match real {
            Some(real) => RowId::new(&real).map_err(|error| SyncError::Malformed(error.to_string())),
            None => Err(SyncError::Store(format!("{model} {id} was not confirmed"))),
        }
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
mod refusal_window_tests;
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

/// The interest of a waiting call in the answer of one operation. Dropping it, after a read or a cancel, frees the result.
pub struct AnswerWish {
    inner: std::sync::Arc<Inner>,
    operation: i64,
}

impl Drop for AnswerWish {
    fn drop(&mut self) {
        locked(&self.inner.answers).release(self.operation);
    }
}

#[derive(Default)]
pub(crate) struct Answers {
    wanted: std::collections::BTreeSet<i64>,
    roots: HashMap<i64, Option<RowId>>,
}

impl Answers {
    const UNWANTED_LIMIT: usize = 256;

    pub(crate) fn want(&mut self, operation: i64) { self.wanted.insert(operation); }

    pub(crate) fn publish(&mut self, operation: i64, root: Option<RowId>) {
        self.roots.insert(operation, root);
        while self.roots.keys().filter(|key| !self.wanted.contains(key)).count() > Self::UNWANTED_LIMIT {
            let Some(oldest) = self.roots.keys().filter(|key| !self.wanted.contains(key)).min().copied() else { break };
            self.roots.remove(&oldest);
        }
    }

    pub(crate) fn take(&mut self, operation: i64) -> Option<Option<RowId>> { self.roots.remove(&operation) }

    pub(crate) fn forget(&mut self, operation: i64) { self.roots.remove(&operation); }

    pub(crate) fn release(&mut self, operation: i64) {
        self.wanted.remove(&operation);
        self.roots.remove(&operation);
    }

    #[cfg(test)]
    pub(crate) fn held(&self) -> usize { self.roots.len() }
}
