use std::time::Duration;

use serde_json::{json, Map, Value};
use tokio::runtime::Handle;
use tokio::task::AbortHandle;

use crate::pacing::parse_duration;
use crate::schema::UpdatesSpec;
use crate::transport::EventSpec;

use super::subscribe::{EventEffect, EventSubscription, SubscriptionDef};
use super::variables::mapped_variables;
use super::{locked, Inner};

#[derive(Default)]
pub(crate) struct Live {
    runtime: Option<Handle>,
    readers: usize,
    pub(crate) subscription: Option<EventSubscription>,
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
    pub(crate) fn restart_live(self: &std::sync::Arc<Self>, cause: super::resume_machine::Cause) {
        let explicit = cause == super::resume_machine::Cause::Explicit;
        if explicit { let _ = self.subscription_failures.send(None); }
        let mut lives = locked(&self.lives);
        for (key, live) in lives.iter_mut() {
            let Some(subscription) = live.subscription.as_ref().filter(|subscription| {
                let status = subscription.status();
                status.stopped && status.failure.as_ref().is_some_and(|failure| explicit || !failure.session_refused())
            }) else { continue };
            let definition = subscription.definition.clone();
            live.subscription = None;
            let inner = self.clone();
            let slot = key.clone();
            let task = tokio::spawn(self.until_closed(async move {
                let started = inner.subscribe(definition).await;
                if let Some(live) = locked(&inner.lives).get_mut(&slot) {
                    live.starting = None;
                    live.subscription = started.ok();
                }
            }));
            live.starting = Some(task.abort_handle());
        }
    }

    pub(crate) fn acquire_live(self: &std::sync::Arc<Self>, model: &str, relation: &str, params: &Map<String, Value>) -> Vec<String> {
        let Ok(spec) = self.db.spec(model) else { return Vec::new() };
        let updates = spec.relations.iter().find(|candidate| candidate.name == relation).map(|relation| relation.updates.clone()).unwrap_or_default();
        let scoped = self.subscription_scope(model, relation, params);
        let mut keys: Vec<_> = updates.into_iter().map(|updates| self.acquire_one(model, updates, scoped.clone())).collect();
        if !spec.changes.is_empty() {
            if let Some(key) = self.acquire_changes() { keys.push(key); }
        }
        keys
    }

    /// Starts the model-level subscriptions (`listen` lines). They stay open until `unlisten`.
    pub(crate) fn listen_all(self: &std::sync::Arc<Self>) {
        self.acquire_changes();
        for spec in self.db.shared.specs.values() {
            for updates in &spec.listens {
                self.acquire_one(spec.key.as_str(), updates.clone(), None);
            }
        }
    }

    pub(crate) fn unlisten_all(self: &std::sync::Arc<Self>) {
        let keys: Vec<String> = self.db.shared.specs.values().flat_map(|spec| spec.listens.iter().map(|updates| format!("{}|{}", spec.key.as_str(), updates.subscription))).collect();
        for key in keys {
            self.release_live(&key);
        }
        if let Some((model, feed)) = self.change_feed() { self.release_live(&format!("{model}|{}", feed.subscription)); }
    }

    pub(super) fn change_feed(&self) -> Option<(String, crate::schema::ChangeFeed)> {
        self.db.shared.specs.values().find_map(|spec| spec.change_feed.as_ref().map(|feed| (spec.key.to_string(), feed.clone())))
    }

    pub(crate) fn acquire_changes(self: &std::sync::Arc<Self>) -> Option<String> {
        let (model, feed) = self.change_feed()?;
        Some(self.acquire_one(&model, UpdatesSpec { apply: None, subscription: feed.subscription, deleted_by: None, set: Default::default(), removes: false, debounce: None, root: None, id: None }, None))
    }

    fn subscription_scope(&self, model: &str, relation: &str, params: &Map<String, Value>) -> Option<(String, Value)> {
        if params.is_empty() { return None; }
        let resolved = self.resolve(model, relation, params).ok()?;
        let variables = mapped_variables(&resolved.params, &resolved.remote.variables);
        Some((resolved.scope.key.to_string(), Value::Object(variables)))
    }

    fn acquire_one(self: &std::sync::Arc<Self>, model: &str, updates: UpdatesSpec, scoped: Option<(String, Value)>) -> String {
        let (key, variables) = match scoped {
            Some((scope, variables)) => (format!("{model}|{}|{scope}", updates.subscription), variables),
            None => (format!("{model}|{}", updates.subscription), json!({})),
        };
        let Ok(runtime) = Handle::try_current() else { return key };
        let mut lives = locked(&self.lives);
        let live = lives.entry(key.clone()).or_default();
        live.runtime = Some(runtime.clone());
        live.readers += 1;
        if let Some(closing) = live.closing.take() {
            closing.abort();
        }
        if live.subscription.is_some() || live.starting.is_some() {
            return key;
        }
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
            def.variables = variables;
            def.deleted_by = updates.deleted_by;
            def.set = updates.set;
            def.root = updates.root;
            def.id = updates.id;
            def.apply = updates.apply;
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
        let Some(runtime) = live.runtime.clone() else {
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
    pub(crate) fn live_status(&self, key: &str) -> Option<(usize, bool)> {
        locked(&self.lives).get(key).map(|live| (live.readers, live.subscription.is_some()))
    }
}

#[cfg(test)]
mod live_tests;
