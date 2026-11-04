use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::super::{locked, Inner, SyncError};
use super::steps::transfer_abort_settled;

impl Inner {
    pub(crate) fn spawn_upload_housekeeping(self: &Arc<Self>, expire: bool) {
        let Ok(runtime) = tokio::runtime::Handle::try_current() else { return };
        let Some(epoch) = self.waking_epoch() else { return };
        let admission = self.db.shared.store.lifecycle.admit_woken(crate::lifecycle::Origin::Session, "upload housekeeping", epoch);
        let inner = self.clone();
        let task = self.until_closed(async move {
            let Some(admitted) = admission.await else { return };
            if admitted.run(inner.run_housekeeping(&admitted, expire)).await.is_err() {
                inner.upload_housekeeping_due.store(true, Ordering::SeqCst);
            }
        });
        locked(&self.uploads).tasks.spawn_on(crate::task::Carry::Detached, task, &runtime);
    }

    #[cfg(test)]
    pub(crate) async fn upload_housekeeping(self: &Arc<Self>, admitted: &crate::lifecycle::Admitted) -> Result<(), SyncError> {
        self.run_housekeeping(admitted, true).await
    }

    async fn run_housekeeping(self: &Arc<Self>, admitted: &crate::lifecycle::Admitted, expire: bool) -> Result<(), SyncError> {
        if expire { self.expire_uploads(admitted).await?; }
        self.abort_orphan_uploads(admitted).await
    }

    pub(crate) async fn expire_uploads(self: &Arc<Self>, admitted: &crate::lifecycle::Admitted) -> Result<(), SyncError> {
        self.expire_blobs(admitted).await?;
        let limit = i64::try_from(self.config.upload.failed_ttl.as_millis()).unwrap_or(i64::MAX);
        let due = self.now_ms().saturating_sub(limit);
        let running: Vec<i64> = locked(&self.uploads).active.keys().map(|key| key.0).collect();
        let db = self.db.clone();
        let admitted = admitted.clone();
        crate::task::spawn_blocking(crate::task::Carry::here(), move || {
            let candidates = db.failed_upload_candidates(due)?;
            db.expire_candidates(&admitted, candidates, due, &running)
        }).await.map_err(|error| SyncError::Store(error.to_string()))??;
        Ok(())
    }

    pub(super) async fn abort_orphan_uploads(self: &Arc<Self>, admitted: &crate::lifecycle::Admitted) -> Result<(), SyncError> {
        let _draining = self.upload_orphan_gate.lock().await;
        if !self.connectivity.is_online() { return Ok(()); }
        let shared = self.db.shared.clone();
        let orphans = crate::task::spawn_blocking(crate::task::Carry::here(), move || shared.store.upload_orphans()).await.map_err(|error| SyncError::Store(error.to_string()))??;
        for abort in orphans {
            let (operation, request) = self.transfer_request(&abort.operation, abort.variables(), &abort.transfer, crate::upload::UploadStage::Abort)?;
            let call = self.send_transfer(admitted, request).await;
            let settled = match call {
                Ok(response) => transfer_abort_settled(&abort.operation, &operation, response),
                _ => false,
            };
            if !settled { continue; }
            let shared = self.db.shared.clone();
            let authority = admitted.authority();
            crate::task::spawn_blocking(crate::task::Carry::here(), move || shared.store.settle_upload_orphan(&authority, abort.id)).await.map_err(|error| SyncError::Store(error.to_string()))??;
        }
        Ok(())
    }
}
