use std::sync::Arc;

use serde_json::json;

use crate::commit::OperationStatus;
use crate::transport::{GraphQLRequest, ResponseOutcome, RetryClass};

use super::super::{locked, Inner, SyncError};

impl Inner {
    pub(crate) fn spawn_upload_housekeeping(self: &Arc<Self>) {
        let Ok(runtime) = tokio::runtime::Handle::try_current() else { return };
        let inner = self.clone();
        let task = self.until_closed(async move { inner.upload_housekeeping().await });
        locked(&self.uploads).tasks.spawn_on(task, &runtime);
    }

    pub(crate) async fn upload_housekeeping(self: &Arc<Self>) {
        self.expire_uploads().await;
        self.abort_orphan_uploads().await;
    }

    pub(crate) async fn expire_uploads(self: &Arc<Self>) {
        let inner = self.clone();
        let _ = tokio::task::spawn_blocking(move || inner.expire_failed_uploads()).await;
    }

    fn expire_failed_uploads(&self) -> Result<(), SyncError> {
        let limit = i64::try_from(self.config.upload.failed_ttl.as_millis()).unwrap_or(i64::MAX);
        let due = self.now_ms().saturating_sub(limit);
        for entry in self.db.outbox()? {
            let operation = &entry.operation;
            let running = locked(&self.uploads).active.keys().any(|key| key.0 == operation.id.0);
            if operation.status == OperationStatus::Failed && operation.upload.is_some() && operation.since <= due && !running {
                self.db.roll_back(operation.id.0)?;
            }
        }
        Ok(())
    }

    async fn abort_orphan_uploads(self: &Arc<Self>) {
        let _draining = self.upload_orphan_gate.lock().await;
        if !self.connectivity.is_online() { return; }
        let shared = self.db.shared.clone();
        let Ok(Ok(orphans)) = tokio::task::spawn_blocking(move || shared.store.upload_orphans()).await else { return };
        for (id, key, upload_id, name) in orphans {
            let Some(operation) = locked(&self.operations).get(&name).cloned() else { continue };
            let request = GraphQLRequest::new(&operation.document, &operation.name, json!({"key": key, "uploadId": upload_id}));
            locked(&self.counters).record_transport_call();
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
            let _ = tokio::task::spawn_blocking(move || shared.store.settle_upload_orphan(id)).await;
        }
    }
}

