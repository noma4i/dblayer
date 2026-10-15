use std::time::Duration;

use tokio::runtime::Handle;
use tokio::task::AbortHandle;

use crate::pacing::parse_duration;
use crate::schema::UpdatesSpec;
use crate::transport::EventSpec;

use super::subscribe::{EventEffect, EventSubscription, SubscriptionDef};
use super::{locked, Inner};

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
    pub(crate) fn acquire_live(self: &std::sync::Arc<Self>, model: &str, relation: &str) -> Vec<String> {
        let Ok(spec) = self.db.spec(model) else { return Vec::new() };
        let updates = spec.relations.iter().find(|candidate| candidate.name == relation).and_then(|found| found.remote.as_ref()).map(|remote| remote.updates.clone()).unwrap_or_default();
        updates.into_iter().map(|updates| self.acquire_one(model, updates)).collect()
    }

    /// Starts the model-level subscriptions (`listen` lines). They stay open until `unlisten`.
    pub(crate) fn listen_all(self: &std::sync::Arc<Self>) {
        for spec in self.db.shared.specs.values() {
            for updates in &spec.listens {
                self.acquire_one(spec.key.as_str(), updates.clone());
            }
        }
    }

    pub(crate) fn unlisten_all(self: &std::sync::Arc<Self>) {
        let keys: Vec<String> = self.db.shared.specs.values().flat_map(|spec| spec.listens.iter().map(|updates| format!("{}|{}", spec.key.as_str(), updates.subscription))).collect();
        for key in keys {
            self.release_live(&key);
        }
    }

    fn acquire_one(self: &std::sync::Arc<Self>, model: &str, updates: UpdatesSpec) -> String {
        let key = format!("{model}|{}", updates.subscription);
        let mut lives = locked(&self.lives);
        let live = lives.entry(key.clone()).or_default();
        live.readers += 1;
        if let Some(closing) = live.closing.take() {
            closing.abort();
        }
        if live.subscription.is_some() || live.starting.is_some() {
            return key;
        }
        let Ok(runtime) = Handle::try_current() else { return key };
        let inner = self.clone();
        let model = model.to_string();
        let slot = key.clone();
        let task = runtime.spawn(self.until_closed(async move {
            let mut event = EventSpec::new(updates.subscription.as_str());
            if let Some(interval) = updates.debounce.as_deref().and_then(parse_duration) {
                event = event.debounced(interval).merged_by_row();
            }
            let effect = if updates.removes { EventEffect::Destroy } else { EventEffect::Upsert };
            let mut def = SubscriptionDef::new(event, &updates.subscription, &model, effect);
            def.deleted_by = updates.deleted_by;
            def.set = updates.set;
            def.root = updates.root;
            def.id = updates.id;
            let started = inner.subscribe(def).await;
            let mut lives = locked(&inner.lives);
            let Some(live) = lives.get_mut(&slot) else { return };
            live.starting = None;
            live.subscription = started.ok();
        }));
        live.starting = Some(task.abort_handle());
        key
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
        let task = runtime.spawn(self.until_closed(async move {
            tokio::time::sleep(linger).await;
            let mut lives = locked(&inner.lives);
            if lives.get(&slot).is_some_and(|live| live.readers == 0) {
                if let Some(mut live) = lives.remove(&slot) {
                    live.closing = None;
                }
            }
        }));
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
