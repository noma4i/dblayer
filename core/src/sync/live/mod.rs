use std::time::Duration;

use serde_json::{json, Map, Value};
use tokio::runtime::Handle;
use tokio::task::AbortHandle;

use crate::db::dispatch::Effects;
use crate::pacing::parse_duration;
use crate::schema::UpdatesSpec;
use crate::transport::EventSpec;

use super::subscribe::{EventEffect, Updates, SubscriptionDef};
use super::variables::mapped_variables;
use super::{locked, Inner};

#[derive(Default)]
pub(crate) struct Live {
    runtime: Option<Handle>,
    readers: usize,
    receivers: Vec<(String, SubscriptionDef, usize)>,
    pub(crate) subscription: Option<Updates>,
    starting: Option<AbortHandle>,
    closing: Option<AbortHandle>,
    ready: tokio::sync::watch::Sender<Option<Result<(), super::SyncError>>>,
}

impl Live {
    fn started(&mut self, started: Result<Updates, super::SyncError>, effects: &mut Effects) {
        self.starting = None;
        match started {
            Ok(subscription) => effects.release(self.subscription.replace(subscription)),
            Err(error) => self.publish_ready(Some(Err(error)), effects),
        }
    }

    fn publish_ready(&self, ready: Option<Result<(), super::SyncError>>, effects: &mut Effects) {
        let sender = self.ready.clone();
        effects.push(move || { crate::db::lock::effect(|| sender.send_replace(ready)); });
    }
}

impl Drop for Live {
    fn drop(&mut self) {
        for handle in [self.starting.take(), self.closing.take()].into_iter().flatten() {
            crate::db::lock::effect(|| handle.abort());
        }
    }
}

impl Inner {
    pub(crate) fn reset_live_ready(&self) {
        self.live_effects.under(&self.lives, |lives, effects| {
            for live in lives.values() { live.publish_ready(None, effects); }
        });
    }

    pub(crate) fn reopen_live(&self) {
        for live in locked(&self.lives).values() {
            if let Some(subscription) = live.subscription.as_ref() { subscription.reopening(); }
        }
    }

    pub(crate) fn restart_live(self: &std::sync::Arc<Self>, cause: super::resume_machine::Cause) {
        let explicit = cause == super::resume_machine::Cause::Explicit;
        if explicit { crate::db::lock::effect(|| { let _ = self.subscription_failures.send(None); }); }
        self.live_effects.under(&self.lives, |lives, effects| {
            for (key, live) in lives.iter_mut() {
                let Some(subscription) = live.subscription.as_ref().filter(|subscription| {
                    let status = subscription.status();
                    status.stopped && status.failure.as_ref().is_some_and(|failure| explicit || !failure.session_refused())
                }) else { continue };
                let definition = subscription.definition.clone();
                effects.release(live.subscription.take());
                live.publish_ready(None, effects);
                let inner = self.clone();
                let slot = key.clone();
                let ready = live.ready.clone();
                live.starting = Some(self.start_after(&Handle::current(), effects, async move {
                    let started = inner.subscribe(definition, Some(ready)).await;
                    inner.live_effects.under(&inner.lives, |lives, effects| {
                        if let Some(live) = lives.get_mut(&slot) { live.started(started, effects); }
                    });
                }));
            }
        });
    }

    /// The task waits until the effects queued so far have run, so a reset of `ready` cannot land after the result of the start it announces.
    fn start_after(self: &std::sync::Arc<Self>, runtime: &Handle, effects: &mut Effects, work: impl std::future::Future<Output = ()> + Send + 'static) -> AbortHandle {
        let (open, gate) = tokio::sync::oneshot::channel::<()>();
        effects.push(move || { crate::db::lock::effect(|| { let _ = open.send(()); }); });
        runtime.spawn(self.until_closed(async move {
            let _ = gate.await;
            work.await;
        })).abort_handle()
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
        let keys: Vec<String> = self.db.shared.specs.values().flat_map(|spec| spec.listens.iter().map(|updates| receiver_key(spec.key.as_str(), updates, &json!({})))).collect();
        for key in keys {
            self.release_live(&key);
        }
        if let Some((model, feed)) = self.change_feed() { self.release_live(&receiver_key(&model, &changes_updates(feed.subscription), &json!({}))); }
    }

    pub(super) fn change_feed(&self) -> Option<(String, crate::schema::ChangeFeed)> {
        self.change_feed.clone()
    }

    pub(crate) fn acquire_changes(self: &std::sync::Arc<Self>) -> Option<String> {
        let (model, feed) = self.change_feed()?;
        Some(self.acquire_one(&model, changes_updates(feed.subscription), None))
    }

    pub(crate) async fn changes_ready(&self, key: &str) -> Result<(), super::SyncError> {
        let mut ready = locked(&self.lives).get(live_source(key)).ok_or(super::SyncError::Cancelled)?.ready.subscribe();
        let mut stopped = self.stopped.clone();
        loop {
            if self.is_stopped() { return Err(super::SyncError::Cancelled); }
            if let Some(result) = ready.borrow_and_update().clone() { return result; }
            tokio::select! {
                result = ready.changed() => { result.map_err(|_| super::SyncError::Cancelled)?; }
                _ = stopped.wait_for(|stopped| *stopped) => return Err(super::SyncError::Cancelled),
            }
        }
    }

    fn subscription_scope(&self, model: &str, relation: &str, params: &Map<String, Value>) -> Option<(String, Value)> {
        if params.is_empty() { return None; }
        let resolved = self.resolve(crate::lifecycle::Origin::Session, model, relation, params).ok()?;
        let variables = mapped_variables(&resolved.params, &resolved.remote.variables);
        Some((resolved.scope.key.to_string(), Value::Object(variables)))
    }

    pub(super) fn acquire_one(self: &std::sync::Arc<Self>, model: &str, updates: UpdatesSpec, scoped: Option<(String, Value)>) -> String {
        let variables = scoped.map_or_else(|| json!({}), |(_, variables)| variables);
        let key = live_key(&updates.subscription, &variables);
        let receiver = receiver_key(model, &updates, &variables);
        let mut event = EventSpec::new(updates.subscription.as_str());
        if let Some(interval) = updates.debounce.as_deref().and_then(parse_duration) {
            event = event.debounced(interval).merged_by_row();
        }
        let effect = if updates.removes { EventEffect::Destroy } else { EventEffect::Upsert };
        let mut def = SubscriptionDef::new(event, &updates.subscription, model, effect);
        def.variables = variables;
        def.deleted_by = updates.deleted_by;
        def.deleted_unless = updates.deleted_unless;
        def.set = updates.set;
        def.root = updates.root;
        def.id = updates.id;
        def.apply = updates.apply;
        let Ok(runtime) = Handle::try_current() else { return receiver };
        self.live_effects.under(&self.lives, |lives, effects| {
            let live = lives.entry(key.clone()).or_default();
            live.runtime = Some(runtime.clone());
            live.readers += 1;
            match live.receivers.iter_mut().find(|(key, _, _)| key == &receiver) {
                Some((_, _, readers)) => *readers += 1,
                None => live.receivers.push((receiver.clone(), def.clone(), 1)),
            }
            if let Some(closing) = live.closing.take() {
                effects.push(move || crate::db::lock::effect(|| closing.abort()));
            }
            if live.subscription.as_ref().is_some_and(|subscription| { let status = subscription.status(); !status.connected && !status.opening }) {
                effects.release(live.subscription.take());
            }
            if live.subscription.is_some() || live.starting.is_some() {
                return;
            }
            live.publish_ready(None, effects);
            let inner = self.clone();
            let slot = key.clone();
            let ready = live.ready.clone();
            live.starting = Some(self.start_after(&runtime, effects, async move {
                let started = inner.subscribe(def, Some(ready)).await;
                inner.live_effects.under(&inner.lives, |lives, effects| {
                    if let Some(live) = lives.get_mut(&slot) { live.started(started, effects); }
                });
            }));
        });
        receiver
    }

    pub(crate) fn release_live(self: &std::sync::Arc<Self>, key: &str) {
        self.live_effects.under(&self.lives, |lives, effects| {
            let Some(live) = lives.get_mut(live_source(key)) else { return };
            let Some((_, _, readers)) = live.receivers.iter_mut().find(|(receiver, _, _)| receiver == key) else { return };
            if *readers == 0 { return; }
            *readers -= 1;
            live.readers = live.readers.saturating_sub(1);
            if live.readers > 0 || live.closing.is_some() {
                return;
            }
            let Some(runtime) = live.runtime.clone() else {
                effects.release(lives.remove(live_source(key)));
                return;
            };
            let inner = self.clone();
            let slot = live_source(key).to_string();
            let linger: Duration = self.config.live_linger;
            let task = runtime.spawn(self.until_closed(async move {
                tokio::time::sleep(linger).await;
                inner.live_effects.under(&inner.lives, |lives, effects| {
                    if lives.get(&slot).is_some_and(|live| live.readers == 0) {
                        if let Some(mut live) = lives.remove(&slot) {
                            live.closing = None;
                            effects.release(live);
                        }
                    }
                });
            }));
            if let Some(live) = lives.get_mut(live_source(key)) {
                live.closing = Some(task.abort_handle());
            }
        });
    }

    pub(super) fn live_definitions(&self, operation: &str, variables: &Value) -> Vec<SubscriptionDef> {
        locked(&self.lives).get(&live_key(operation, variables)).map(|live| live.receivers.iter().filter(|(_, _, readers)| *readers > 0).map(|(_, definition, _)| definition.clone()).collect()).unwrap_or_default()
    }

    #[cfg(test)]
    pub(crate) fn live_status(&self, key: &str) -> Option<(usize, bool)> {
        locked(&self.lives).get(key).map(|live| (live.readers, live.subscription.is_some()))
    }
}

pub(super) fn live_key(operation: &str, variables: &Value) -> String {
    format!("{operation}|{variables}")
}

pub(super) fn live_source(key: &str) -> &str {
    key.split_once('\n').map_or(key, |(source, _)| source)
}

fn receiver_key(model: &str, updates: &UpdatesSpec, variables: &Value) -> String {
    format!("{}\n{}", live_key(&updates.subscription, variables), json!([model, updates]))
}

fn changes_updates(subscription: String) -> UpdatesSpec {
    UpdatesSpec { apply: None, subscription, deleted_by: None, deleted_unless: None, set: Default::default(), removes: false, debounce: None, root: None, id: Vec::new() }
}

#[cfg(test)]
mod live_tests;
#[cfg(test)]
mod wake_tests;
