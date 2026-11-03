use std::time::Duration;

use crate::operation::OperationStatus;
use crate::operation::Operation;
use crate::pacing::Backoff;

use super::super::Inner;

impl Inner {
    pub(crate) fn send_backoff(&self, seq: i64) -> Backoff {
        let backoff = self.config.send_backoff;
        Backoff { seed: backoff.seed ^ seq as u64, ..backoff }
    }

    pub(crate) fn retry_due(&self, operation: &Operation) -> Option<Duration> {
        if operation.status != OperationStatus::Pending || operation.transport_started || operation.attempts == 0 {
            return None;
        }
        let since = Duration::from_millis(u64::try_from(operation.since).unwrap_or_default());
        if super::super::locked(&self.network_returned).is_some_and(|returned| since <= returned) { return None; }
        Some(since + self.send_backoff(operation.id.0).delay(operation.attempts - 1))
    }

    pub(crate) fn is_waiting(&self, operation: &Operation) -> bool {
        self.retry_due(operation).is_some_and(|due| due > self.clock.now())
    }
}
