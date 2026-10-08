use std::sync::Arc;
use std::time::Duration;

use tokio::runtime::Handle;

use super::{locked, Sync};

impl Sync {
    pub fn set_online(&self, online: bool) {
        let was_online = self.inner.connectivity.is_online();
        self.inner.connectivity.set(online);
        if online && !was_online {
            self.spawn_flush(Duration::ZERO);
        }
    }

    pub(crate) fn arm_wake(&self) {
        let due = self.inner.db.outbox().ok().and_then(|entries| entries.iter().filter_map(|entry| self.inner.retry_due(&entry.operation)).min());
        let wake = due.filter(|_| self.inner.connectivity.is_online()).map(|due| due.saturating_sub(self.inner.clock.now()).max(Duration::from_millis(1)));
        let task = wake.and_then(|wait| self.spawn_flush(wait));
        if let Some(old) = std::mem::replace(&mut *locked(&self.inner.wake), task) {
            old.abort();
        }
    }

    fn spawn_flush(&self, wait: Duration) -> Option<tokio::task::AbortHandle> {
        let runtime = Handle::try_current().ok()?;
        let inner = Arc::downgrade(&self.inner);
        let task = runtime.spawn(async move {
            tokio::time::sleep(wait).await;
            if let Some(inner) = inner.upgrade() {
                let _ = Sync { inner }.flush().await;
            }
        });
        Some(task.abort_handle())
    }
}
