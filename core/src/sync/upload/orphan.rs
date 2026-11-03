use std::sync::atomic::Ordering;
use std::sync::Arc;

use serde_json::json;

use crate::transport::{GraphQLRequest, ResponseOutcome, RetryClass};

use super::super::{locked, Inner, SyncError};

impl Inner {
    pub(crate) fn spawn_upload_housekeeping(self: &Arc<Self>, expire: bool) {
        let Ok(runtime) = tokio::runtime::Handle::try_current() else { return };
        let Ok(lease) = self.lease(crate::lifecycle::Origin::Session, "upload housekeeping") else { return };
        let inner = self.clone();
        let task = self.until_closed(async move {
            if lease.pinned(inner.run_housekeeping(expire)).await.is_err() {
                inner.upload_housekeeping_due.store(true, Ordering::SeqCst);
            }
            drop(lease);
        });
        locked(&self.uploads).tasks.spawn_on(crate::task::Carry::Detached, task, &runtime);
    }

    #[cfg(test)]
    pub(crate) async fn upload_housekeeping(self: &Arc<Self>) -> Result<(), SyncError> {
        self.run_housekeeping(true).await
    }

    async fn run_housekeeping(self: &Arc<Self>, expire: bool) -> Result<(), SyncError> {
        if expire { self.expire_uploads().await?; }
        self.abort_orphan_uploads().await
    }

    pub(crate) async fn expire_uploads(self: &Arc<Self>) -> Result<(), SyncError> {
        self.expire_blobs().await?;
        let limit = i64::try_from(self.config.upload.failed_ttl.as_millis()).unwrap_or(i64::MAX);
        let due = self.now_ms().saturating_sub(limit);
        let running: Vec<i64> = locked(&self.uploads).active.keys().map(|key| key.0).collect();
        let db = self.db.clone();
        crate::task::spawn_blocking(crate::task::Carry::here(), move || {
            let candidates = db.failed_upload_candidates(due)?;
            db.expire_candidates(candidates, due, &running)
        }).await.map_err(|error| SyncError::Store(error.to_string()))??;
        Ok(())
    }

    async fn abort_orphan_uploads(self: &Arc<Self>) -> Result<(), SyncError> {
        let _draining = self.upload_orphan_gate.lock().await;
        if !self.connectivity.is_online() { return Ok(()); }
        let shared = self.db.shared.clone();
        let orphans = crate::task::spawn_blocking(crate::task::Carry::here(), move || shared.store.upload_orphans()).await.map_err(|error| SyncError::Store(error.to_string()))??;
        for (id, key, upload_id, name) in orphans {
            let Some(operation) = locked(&self.operations).get(&name).cloned() else {
                return Err(SyncError::Store(format!("operation {name} is not registered")));
            };
            let request = GraphQLRequest::new(&operation.document, &operation.name, json!({"key": key, "uploadId": upload_id}));
            locked(&self.counters).record_transport_call();
            crate::db::lock::assert_no_library_lock("Transport::mutate");
            let call = crate::pacing::timeout(self.clock.as_ref(), self.config.upload.step_timeout, self.transport.mutate(request)).await;
            let settled = match call {
                Ok(Ok(response)) => {
                    let codes: Vec<&str> = response.errors.iter().filter_map(|error| error.code()).collect();
                    response.shape().outcome() == ResponseOutcome::Commit || RetryClass::classify(false, None, &codes) != RetryClass::Retriable
                }
                _ => false,
            };
            if !settled { continue; }
            let shared = self.db.shared.clone();
            crate::task::spawn_blocking(crate::task::Carry::here(), move || shared.store.settle_upload_orphan(id)).await.map_err(|error| SyncError::Store(error.to_string()))??;
        }
        Ok(())
    }
}
