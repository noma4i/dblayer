use std::sync::Arc;
use std::time::Duration;

use tokio::runtime::Handle;

use super::{locked, Inner, Sync};

impl Sync {
    pub fn set_online(&self, online: bool) {
        let was_online = self.inner.connectivity.is_online();
        self.inner.connectivity.set(online);
        if online && !was_online {
            self.inner.spawn_flush(Duration::ZERO);
        }
    }
}

impl Inner {
    pub(crate) fn arm_wake(self: &Arc<Self>) {
        let due = self.db.outbox().ok().and_then(|entries| entries.iter().filter_map(|entry| self.retry_due(&entry.operation)).min());
        let wake = due.filter(|_| self.connectivity.is_online()).map(|due| due.saturating_sub(self.clock.now()).max(Duration::from_millis(1)));
        let task = wake.and_then(|wait| self.spawn_flush(wait));
        if let Some(old) = std::mem::replace(&mut *locked(&self.wake), task) {
            old.abort();
        }
    }

    pub(crate) fn spawn_flush(self: &Arc<Self>, wait: Duration) -> Option<tokio::task::AbortHandle> {
        let runtime = Handle::try_current().ok()?;
        let inner = Arc::downgrade(self);
        let task = runtime.spawn(self.until_closed(async move {
            tokio::time::sleep(wait).await;
            if let Some(inner) = inner.upgrade() {
                let _ = inner.flush_pending().await;
            }
        }));
        Some(task.abort_handle())
    }
}
