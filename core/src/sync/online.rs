use std::sync::Arc;
use std::time::Duration;

use tokio::runtime::Handle;

use super::load_machine::{Event, LoadKind};
use super::load_owner::Target;
use super::{locked, Inner, Sync};

impl Sync {
    pub fn set_online(&self, online: bool) {
        let was_online = self.inner.connectivity.is_online();
        self.inner.connectivity.set_online(online);
        crate::db::lock::assert_no_library_lock("Transport::set_online");
        self.inner.transport.set_online(online);
        if !online {
            let wake = locked(&self.inner.wake).take();
            if let Some(wake) = wake { crate::db::lock::effect(|| wake.abort()); }
            self.inner.owner.submit(Event::Offline);
        }
        if online && !was_online {
            self.inner.spawn_upload_housekeeping(true);
            self.inner.spawn_flush(Duration::ZERO);
            for (target, kind) in self.inner.owner.online() {
                self.inner.spawn_retry(target, kind);
            }
            self.inner.resume_on_event();
            self.inner.preload_start();
            self.refresh_due_entities(false);
        }
    }
}

impl Inner {
    pub(crate) fn arm_wake(self: &Arc<Self>) -> Result<(), super::SyncError> {
        let due = {
            let queue = self.operation_queue()?;
            let queue = locked(queue);
            queue.retrying.iter().filter_map(|seq| queue.entries.get(seq)).filter_map(|entry| self.retry_due(&entry.operation)).min()
        };
        let wake = due.filter(|_| self.connectivity.is_online()).map(|due| due.saturating_sub(self.clock.now()).max(Duration::from_millis(1)));
        let task = wake.and_then(|wait| self.spawn_flush(wait));
        let old = std::mem::replace(&mut *locked(&self.wake), task);
        if let Some(old) = old {
            crate::db::lock::effect(|| old.abort());
        }
        Ok(())
    }

    pub(crate) fn spawn_flush(self: &Arc<Self>, wait: Duration) -> Option<tokio::task::AbortHandle> {
        if !self.admitted(crate::lifecycle::Origin::Background) { return None; }
        let runtime = Handle::try_current().ok()?;
        let clock = self.clock.clone();
        let deadline = clock.now().saturating_add(wait);
        let inner = Arc::downgrade(self);
        let reserved = if wait.is_zero() { Some(self.lease(crate::lifecycle::Origin::Background, "outbox flush").ok()?) } else { None };
        let task = runtime.spawn(self.until_closed(async move {
            clock.sleep(deadline.saturating_sub(clock.now())).await;
            if let Some(inner) = inner.upgrade() {
                let Some(_lease) = reserved.or_else(|| inner.lease(crate::lifecycle::Origin::Background, "outbox flush").ok()) else { return };
                let _ = inner.flush_pending().await;
            }
        }));
        Some(task.abort_handle())
    }
}

impl Inner {
    fn spawn_retry(self: &Arc<Self>, target: Target, kind: LoadKind) {
        let Ok(runtime) = Handle::try_current() else { return };
        let inner = Arc::downgrade(self);
        runtime.spawn(self.until_closed(async move {
            let Some(inner) = inner.upgrade() else { return };
            let Ok(resolved) = inner.resolve(crate::lifecycle::Origin::Observed, &target.model, &target.relation, &target.params) else { return };
            if resolved.members_complete || resolved.media.is_some() {
                tokio::select! {
                    _ = inner.owner.unobserved(&resolved.window_key) => {},
                    _ = inner.load_members(&resolved, false) => {},
                }
                return;
            }
            let _ = match kind {
                LoadKind::More => inner.load_more(&resolved).await,
                _ => inner.fetch(&resolved, None, true).await,
            };
        }));
    }
}
