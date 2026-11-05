mod flush;
mod outcomes;
mod pause;
#[cfg(test)] mod pause_tests;
mod report;
mod settle;
#[cfg(test)] mod swap_window_tests;
mod variables;

pub(crate) use flush::{DeliveryEffects, Sending, FinalAttempts};
pub(crate) use outcomes::{FinishedOperation, OutcomeTicket, Outcomes};
pub use report::{FlushReport, SendResult, SentOperation, SkipReason};

use crate::identity::RowId;

use super::error::SyncError;
use super::{locked, Inner, Sync};

impl Inner {
    pub(crate) async fn flush_pending(self: &std::sync::Arc<Self>, admitted: &crate::ffi::legacy::Admitted) -> Result<FlushReport, SyncError> {
        if self.upload_housekeeping_due.swap(false, std::sync::atomic::Ordering::SeqCst) {
            if let Err(error) = self.expire_uploads(admitted).await {
                self.upload_housekeeping_due.store(true, std::sync::atomic::Ordering::SeqCst);
                return Err(error);
            }
            self.spawn_upload_housekeeping(false);
        }
        let report = self.flush(admitted).await?;
        self.start_uploads()?;
        self.arm_wake()?;
        Ok(report)
    }
}

impl Sync {
    pub(crate) fn claim_outcome(&self, operation: i64) -> std::sync::Arc<OutcomeTicket> {
        locked(&self.inner.outcomes).claim(operation)
    }

    pub async fn flush(&self, admitted: &crate::ffi::legacy::Admitted) -> Result<FlushReport, SyncError> {
        self.inner.ensure_admitted(admitted)?;
        self.inner.flush_pending(admitted).await
    }

    pub async fn retry(&self, admitted: &crate::ffi::legacy::Admitted, model: &str, id: &RowId, action: Option<&str>) -> Result<FlushReport, SyncError> {
        self.inner.ensure_admitted(admitted)?;
        self.inner.db.retry(admitted, model, id, action)?;
        self.flush(admitted).await
    }

    pub async fn retry_all(&self, admitted: &crate::ffi::legacy::Admitted) -> Result<FlushReport, SyncError> {
        self.inner.ensure_admitted(admitted)?;
        self.inner.db.retry_all(admitted)?;
        self.flush(admitted).await
    }

    pub async fn discard(&self, admitted: &crate::ffi::legacy::Admitted, model: &str, id: &RowId, action: Option<&str>) -> Result<(), SyncError> {
        self.inner.ensure_admitted(admitted)?;
        self.inner.db.discard(admitted, model, id, action)?;
        self.inner.start_uploads()?;
        Ok(())
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
mod pass_tests;
#[cfg(test)]
mod reset_tests;
#[cfg(test)]
mod timer_tests;
#[cfg(test)]
mod rollback_tests;
#[cfg(test)]
mod variables_tests;
#[cfg(test)]
mod idempotency_tests;
#[cfg(test)]
mod created_rows_tests;
