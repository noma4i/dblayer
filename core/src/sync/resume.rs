use std::collections::BTreeMap;
use std::sync::Arc;

use serde_json::{Map, Value};
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use crate::task::Children;

use crate::db::dispatch::OrderedEffects;

use super::error::SyncError;
use super::resume_machine::{Cause, ResumeEvent, ResumeMachine};
use super::{locked, Inner, Sync};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ResumeReport {
    pub chunks: usize,
    pub loaded: usize,
    pub failed: usize,
}

type Outcome = Result<ResumeReport, SyncError>;

struct OwnerState {
    machine: ResumeMachine,
    current: Vec<oneshot::Sender<Outcome>>,
    queued: Vec<oneshot::Sender<Outcome>>,
    runner: Option<u64>,
    runners: u64,
}

pub(crate) struct ResumeOwner {
    state: std::sync::Mutex<OwnerState>,
    effects: OrderedEffects,
}

impl ResumeOwner {
    pub(crate) fn new() -> Self {
        Self { state: std::sync::Mutex::new(OwnerState { machine: ResumeMachine::new(), current: Vec::new(), queued: Vec::new(), runner: None, runners: 0 }), effects: OrderedEffects::default() }
    }

    fn request(&self, cause: Cause, waiter: Option<oneshot::Sender<Outcome>>) -> Option<(u64, Cause)> {
        let mut state = locked(&self.state);
        let start = state.machine.handle(&ResumeEvent::Request(cause));
        if let Some(waiter) = waiter {
            if start.is_some() { state.current.push(waiter) } else { state.queued.push(waiter) }
        }
        let first = start?;
        state.runners += 1;
        state.runner = Some(state.runners);
        Some((state.runners, first))
    }

    fn finish(&self, runner: u64, outcome: Outcome) -> Option<Cause> {
        self.effects.under(&self.state, |state, effects| {
            if state.runner != Some(runner) { return None; }
            let answered = std::mem::take(&mut state.current);
            let next = state.machine.handle(&ResumeEvent::Finished);
            if next.is_some() {
                state.current = std::mem::take(&mut state.queued);
            } else {
                state.runner = None;
            }
            effects.push(move || {
                for waiter in answered { crate::db::lock::effect(|| { let _ = waiter.send(outcome.clone()); }); }
            });
            next
        })
    }

    fn abort(&self, runner: u64, error: SyncError) {
        self.effects.under(&self.state, |state, effects| {
            if state.runner != Some(runner) { return; }
            let mut waiters = std::mem::take(&mut state.current);
            waiters.append(&mut state.queued);
            while !state.machine.is_idle() { state.machine.handle(&ResumeEvent::Finished); }
            state.runner = None;
            effects.push(move || {
                for waiter in waiters { crate::db::lock::effect(|| { let _ = waiter.send(Err(error.clone())); }); }
            });
        });
    }
}

/// The right to run the passes of one accepted request chain. Dropping it before the chain ends, even unpolled, fails every waiter.
pub(crate) struct Runner {
    inner: std::sync::Weak<Inner>,
    id: u64,
    first: Cause,
    _lease: crate::lifecycle::Lease,
}

impl Runner {
    pub(crate) async fn run_in_epoch(self) {
        let epoch = self._lease.scope();
        crate::store::ORIGIN.scope(epoch, self.run()).await
    }

    pub(crate) async fn run(self) {
        let mut cause = Some(self.first);
        while let Some(current) = cause {
            let Some(inner) = self.inner.upgrade() else { return };
            let outcome = inner.resume_windows(current).await;
            cause = inner.resume.finish(self.id, outcome);
        }
    }
}

impl Drop for Runner {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.upgrade() { inner.resume.abort(self.id, SyncError::Cancelled); }
    }
}

pub struct Watch {
    inner: Arc<Inner>,
    _read: Option<crate::db::ObservedReadLease>,
    live: Vec<String>,
    failures: Option<tokio::sync::broadcast::Receiver<super::SubscriptionFailureNotice>>,
}

impl Watch {
    pub(crate) fn failures(&mut self) -> Option<Failures> {
        let failures = self.failures.take()?;
        let (inner, live) = (self.inner.clone(), self.live.clone());
        let current: Arc<dyn Fn() -> Option<crate::transport::SubscriptionFailure> + Send + std::marker::Sync> = Arc::new(move || {
            live.iter().find_map(|key| locked(&inner.lives).get(super::live::live_source(key))?.subscription.as_ref()?.status().failure.filter(|failure| matches!(failure.error, crate::transport::TransportError::Session { .. })))
        });
        Some(Failures { current: current(), recheck: current, receiver: failures, sources: self.live.iter().map(|key| super::live::live_source(key).to_string()).collect() })
    }
}

pub(crate) struct Failures {
    pub current: Option<crate::transport::SubscriptionFailure>,
    pub recheck: Arc<dyn Fn() -> Option<crate::transport::SubscriptionFailure> + Send + std::marker::Sync>,
    pub receiver: tokio::sync::broadcast::Receiver<super::SubscriptionFailureNotice>,
    pub sources: Vec<String>,
}

impl Drop for Watch {
    fn drop(&mut self) {
        for key in &self.live {
            self.inner.release_live(key);
        }
    }
}

impl Sync {
    pub fn watch(&self, model: &str, relation: &str, params: &Map<String, Value>) -> Result<Watch, SyncError> {
        let spec = self.inner.db.spec(model)?;
        let declared = spec.relations.iter().find(|candidate| candidate.name == relation).ok_or_else(|| SyncError::UnknownRelation { model:model.into(), relation:relation.into() })?;
        let read = if declared.remote.is_some() {
            let key = self.inner.resolve(crate::lifecycle::Origin::Session, model, relation, params)?.window_key;
            Some(self.inner.db.register_observed_read(crate::db::ObservedRead { model: model.to_string(), relation: relation.to_string(), params: params.clone(), key }))
        } else { None };
        let failures = self.inner.subscription_failures.subscribe();
        let live = self.inner.acquire_live(model, relation, params);
        Ok(Watch { inner: self.inner.clone(), _read: read, live, failures: Some(failures) })
    }

    pub async fn resume(&self) -> Result<ResumeReport, SyncError> {
        let (sender, receiver) = oneshot::channel();
        self.inner.request_resume(Cause::Explicit, Some(sender))?;
        receiver.await.unwrap_or(Err(SyncError::Cancelled))
    }

    pub fn enter_foreground(&self) {
        self.inner.resume_on_event();
    }
}

impl Inner {
    pub(crate) fn resume_on_event(self: &Arc<Self>) {
        if self.connectivity.is_online() {
            let _ = self.request_resume(Cause::Event, None);
        }
    }

    pub(crate) fn begin_resume(self: &Arc<Self>, cause: Cause, waiter: Option<oneshot::Sender<Outcome>>) -> Option<Runner> {
        let Ok(lease) = self.lease(crate::lifecycle::Origin::Session, "resume") else {
            if cause == Cause::Explicit && self.db.shared.store.lifecycle.snapshot().admits(crate::lifecycle::Origin::Explicit) {
                if let Some(waiter) = waiter { crate::db::lock::effect(|| { let _ = waiter.send(Ok(ResumeReport::default())); }); }
            }
            return None;
        };
        self.resume.request(cause, waiter).map(|(id, first)| Runner { inner: Arc::downgrade(self), id, first, _lease: lease })
    }

    pub(crate) fn request_resume(self: &Arc<Self>, cause: Cause, waiter: Option<oneshot::Sender<Outcome>>) -> Result<(), SyncError> {
        let runtime = Handle::try_current().map_err(|_| SyncError::Store("no async runtime to run the resume".into()))?;
        if let Some(runner) = self.begin_resume(cause, waiter) {
            crate::task::spawn_on_unpinned(&runtime, self.until_closed(runner.run_in_epoch()));
        }
        Ok(())
    }

    pub(crate) async fn resume_windows(self: &Arc<Self>, cause: Cause) -> Result<ResumeReport, SyncError> {
        if !self.admitted(crate::lifecycle::Origin::Session) { return Ok(ResumeReport::default()); }
        if cause == Cause::Explicit { self.transport.resume_subscriptions(); }
        self.restart_live(cause);
        let drain = self.config.resume;
        let watched: BTreeMap<String, (String, String, Map<String, Value>)> = self.db.observed_reads()
            .into_iter()
            .map(|entry| (entry.key, (entry.model, entry.relation, entry.params)))
            .collect();
        let now_ms = self.now_ms();
        let stale_ms = i64::try_from(drain.stale_time.as_millis()).unwrap_or(i64::MAX);
        let mut stale = Vec::with_capacity(watched.len());
        let mut unloaded = std::collections::BTreeSet::new();
        for key in watched.keys() {
            if cause == Cause::Event && self.owner.session_refused(key) { continue; }
            let (model, relation, _) = &watched[key];
            let on_foreground = cause == Cause::Event && self.db.spec(model).is_ok_and(|spec| spec.relations.iter().any(|declared| &declared.name == relation && declared.refresh_on_foreground));
            let freshness = self.db.shared.store.window_freshness(key)?;
            if freshness.is_none() { unloaded.insert(key.clone()); }
            if on_foreground || (cause == Cause::Explicit && self.owner.session_refused(key)) || !freshness.is_some_and(|freshness| freshness.is_fresh(now_ms, stale_ms)) {
                stale.push(key.clone());
            }
        }
        let chunks = drain.chunks(&stale).map_err(|error| SyncError::Params(error.to_string()))?;
        let mut report = ResumeReport { chunks: chunks.len(), ..ResumeReport::default() };
        for chunk in chunks {
            let mut loads = Children::new();
            for key in chunk {
                let Some((model, relation, params)) = watched.get(&key).cloned() else { continue };
                let inner = self.clone();
                let force = !unloaded.contains(&key);
                loads.spawn(async move {
                    let resolved = inner.resolve(crate::lifecycle::Origin::Session, &model, &relation, &params)?;
                    if resolved.members_complete || resolved.media.is_some() {
                        tokio::select! {
                            _ = inner.owner.unobserved(&resolved.window_key) => Err(SyncError::Cancelled),
                            result = inner.load_scope(&resolved, force) => result,
                        }
                    } else { inner.fetch(&resolved, None, force).await }
                });
            }
            while let Some(finished) = loads.join_next().await {
                match finished {
                    Ok(Ok(_)) => report.loaded += 1,
                    _ => report.failed += 1,
                }
            }
        }
        Ok(report)
    }
}

#[cfg(test)]
mod wake_tests;
