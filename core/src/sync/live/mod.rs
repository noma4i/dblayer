use std::time::Duration;

use tokio::runtime::Handle;
use tokio::task::AbortHandle;

use crate::transport::EventSpec;

use super::subscribe::{EventEffect, EventSubscription, SubscriptionDef};
use super::{locked, Inner, Sync};

#[derive(Default)]
pub(crate) struct Live {
    readers: usize,
    subscription: Option<EventSubscription>,
    starting: Option<AbortHandle>,
    closing: Option<AbortHandle>,
}

impl Drop for Live {
    fn drop(&mut self) {
        for handle in [self.starting.take(), self.closing.take()].into_iter().flatten() {
            handle.abort();
        }
    }
}

impl Inner {
    pub(crate) fn acquire_live(self: &std::sync::Arc<Self>, model: &str, relation: &str) -> Option<String> {
        let spec = self.db.spec(model).ok()?;
        let updates = spec.relations.iter().find(|candidate| candidate.name == relation)?.remote.as_ref()?.updates.clone()?;
        let key = format!("{model}|{}", updates.subscription);
        let mut lives = locked(&self.lives);
        let live = lives.entry(key.clone()).or_default();
        live.readers += 1;
        if let Some(closing) = live.closing.take() {
            closing.abort();
        }
        if live.subscription.is_some() || live.starting.is_some() {
            return Some(key);
        }
        let Ok(runtime) = Handle::try_current() else { return Some(key) };
        let inner = self.clone();
        let model = model.to_string();
        let slot = key.clone();
        let task = runtime.spawn(async move {
            let mut def = SubscriptionDef::new(EventSpec::new(updates.subscription.as_str()), &updates.subscription, &model, EventEffect::Upsert);
            def.deleted_by = updates.deleted_by;
            let started = Sync { inner: inner.clone() }.subscribe(def).await;
            let mut lives = locked(&inner.lives);
            let Some(live) = lives.get_mut(&slot) else { return };
            live.starting = None;
            live.subscription = started.ok();
        });
        live.starting = Some(task.abort_handle());
        Some(key)
    }

    pub(crate) fn release_live(self: &std::sync::Arc<Self>, key: &str) {
        let mut lives = locked(&self.lives);
        let Some(live) = lives.get_mut(key) else { return };
        live.readers = live.readers.saturating_sub(1);
        if live.readers > 0 || live.closing.is_some() {
            return;
        }
        let Ok(runtime) = Handle::try_current() else {
            lives.remove(key);
            return;
        };
        let inner = self.clone();
        let slot = key.to_string();
        let linger: Duration = self.config.live_linger;
        let task = runtime.spawn(async move {
            tokio::time::sleep(linger).await;
            let mut lives = locked(&inner.lives);
            if lives.get(&slot).is_some_and(|live| live.readers == 0) {
                if let Some(mut live) = lives.remove(&slot) {
                    live.closing = None;
                }
            }
        });
        if let Some(live) = lives.get_mut(key) {
            live.closing = Some(task.abort_handle());
        }
    }

    #[cfg(test)]
    pub(crate) fn live_state(&self, key: &str) -> Option<(usize, bool)> {
        locked(&self.lives).get(key).map(|live| (live.readers, live.subscription.is_some()))
    }
}

#[cfg(test)]
mod live_tests;
