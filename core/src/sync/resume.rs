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

/// The resume requests of one session epoch: a runner, the callers it answers and the callers queued behind it.
struct EpochState {
    machine: ResumeMachine,
    current: Vec<oneshot::Sender<Outcome>>,
    queued: Vec<oneshot::Sender<Outcome>>,
    runner: Option<u64>,
}

impl EpochState {
    fn new() -> Self { Self { machine: ResumeMachine::new(), current: Vec::new(), queued: Vec::new(), runner: None } }
}

#[derive(Default)]
struct OwnerState {
    epochs: std::collections::HashMap<u64, EpochState>,
    runners: u64,
}

/// Owns the resume of each session epoch apart: the end of the runner of an old epoch answers only the callers of that epoch.
pub(crate) struct ResumeOwner {
    state: std::sync::Mutex<OwnerState>,
    effects: OrderedEffects,
}

impl ResumeOwner {
    pub(crate) fn new() -> Self {
        Self { state: std::sync::Mutex::new(OwnerState::default()), effects: OrderedEffects::default() }
    }

    fn request(&self, epoch: u64, cause: Cause, waiter: Option<oneshot::Sender<Outcome>>) -> Option<(u64, Cause)> {
        let mut state = locked(&self.state);
        let next = state.runners + 1;
        let slot = state.epochs.entry(epoch).or_insert_with(EpochState::new);
        let start = slot.machine.handle(&ResumeEvent::Request(cause));
        if let Some(waiter) = waiter {
            if start.is_some() { slot.current.push(waiter) } else { slot.queued.push(waiter) }
        }
        let first = start?;
        slot.runner = Some(next);
        state.runners = next;
        Some((next, first))
    }

    fn finish(&self, epoch: u64, runner: u64, outcome: Outcome) -> Option<Cause> {
        self.effects.under(&self.state, |state, effects| {
            let slot = state.epochs.get_mut(&epoch).filter(|slot| slot.runner == Some(runner))?;
            let answered = std::mem::take(&mut slot.current);
            let next = slot.machine.handle(&ResumeEvent::Finished);
            if next.is_some() {
                slot.current = std::mem::take(&mut slot.queued);
            } else if let Some(done) = state.epochs.remove(&epoch) {
                effects.release(done);
            }
            effects.push(move || {
                for waiter in answered { crate::db::lock::effect(|| { let _ = waiter.send(outcome.clone()); }); }
            });
            next
        })
    }

    fn abort(&self, epoch: u64, runner: u64, error: SyncError) {
        self.effects.under(&self.state, |state, effects| {
            if !state.epochs.get(&epoch).is_some_and(|slot| slot.runner == Some(runner)) { return; }
            let Some(mut slot) = state.epochs.remove(&epoch) else { return };
            let mut waiters = std::mem::take(&mut slot.current);
            waiters.append(&mut slot.queued);
            effects.release(slot);
            effects.push(move || {
                for waiter in waiters { crate::db::lock::effect(|| { let _ = waiter.send(Err(error.clone())); }); }
            });
        });
    }
}

/// The right to run the passes of one accepted request chain. Dropping it before the chain ends, even unpolled, fails every waiter.
pub(crate) struct Runner {
    inner: std::sync::Weak<Inner>,
    epoch: u64,
    id: u64,
    first: Cause,
    admitted: crate::ffi::legacy::Admitted,
}

impl Runner {
    pub(crate) async fn run_in_epoch(self) {
        let admitted = self.admitted.clone();
        let _ = admitted.run(self.run()).await;
    }

    pub(crate) async fn run(self) {
        let mut cause = Some(self.first);
        while let Some(current) = cause {
            let Some(inner) = self.inner.upgrade() else { return };
            let outcome = inner.resume_windows(&self.admitted, current).await;
            cause = inner.resume.finish(self.epoch, self.id, outcome);
        }
    }
}

impl Drop for Runner {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.upgrade() { inner.resume.abort(self.epoch, self.id, SyncError::Cancelled); }
    }
}

pub struct Watch {
    inner: Arc<Inner>,
    _read: Option<crate::db::ObservedReadLease>,
    _state: Option<super::load_owner::RelationWatch>,
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
        let (read, state) = if declared.remote.is_some() {
            let resolved = self.inner.resolve(crate::ffi::legacy::Origin::Session, model, relation, params)?;
            let key = resolved.window_key.clone();
            let state = self.inner.owner.observe(&key, resolved.target(), None);
            let read = self.inner.db.register_observed_read(crate::db::ObservedRead { model: model.to_string(), relation: relation.to_string(), params: params.clone(), key });
            (Some(read), Some(state))
        } else { (None, None) };
        let failures = self.inner.subscription_failures.subscribe();
        let live = self.inner.acquire_live(model, relation, params);
        Ok(Watch { inner: self.inner.clone(), _read: read, _state: state, live, failures: Some(failures) })
    }

    pub async fn resume(&self, admitted: &crate::ffi::legacy::Admitted) -> Result<ResumeReport, SyncError> {
        self.inner.ensure_admitted(admitted)?;
        if !self.inner.admitted(crate::ffi::legacy::Origin::Session) { return Ok(ResumeReport::default()); }
        admitted.run(async {
            let (sender, receiver) = oneshot::channel();
            self.inner.request_resume(admitted, Cause::Explicit, Some(sender))?;
            receiver.await.unwrap_or(Err(SyncError::Cancelled))
        }).await?
    }

    pub fn enter_foreground(&self) {
        self.inner.resume_on_event();
    }
}

impl Inner {
    pub(crate) fn resume_on_event(self: &Arc<Self>) {
        if !self.connectivity.is_online() { return; }
        let (Ok(runtime), Some(epoch)) = (Handle::try_current(), self.waking_epoch()) else { return };
        let admission = self.db.shared.store.lifecycle.admit_woken(crate::ffi::legacy::Origin::Session, "resume", epoch);
        let inner = Arc::downgrade(self);
        crate::task::spawn_on(crate::task::Carry::Detached, &runtime, self.until_closed(async move {
            let Some(admitted) = admission.await else { return };
            if let Some(inner) = inner.upgrade() { let _ = inner.request_resume(&admitted, Cause::Event, None); }
        }));
    }

    pub(crate) fn begin_resume(self: &Arc<Self>, admitted: &crate::ffi::legacy::Admitted, cause: Cause, waiter: Option<oneshot::Sender<Outcome>>) -> Option<Runner> {
        let epoch = admitted.value();
        self.resume.request(epoch, cause, waiter).map(|(id, first)| Runner { inner: Arc::downgrade(self), epoch, id, first, admitted: admitted.clone() })
    }

    pub(crate) fn request_resume(self: &Arc<Self>, admitted: &crate::ffi::legacy::Admitted, cause: Cause, waiter: Option<oneshot::Sender<Outcome>>) -> Result<(), SyncError> {
        let runtime = Handle::try_current().map_err(|_| SyncError::Store("no async runtime to run the resume".into()))?;
        let runner = {
            let life = &self.db.shared.store.lifecycle;
            let _line = life.commit_line();
            if !std::sync::Arc::ptr_eq(admitted.life(), life) || !admitted.is_current() { return Err(SyncError::Cancelled); }
            self.begin_resume(admitted, cause, waiter)
        };
        if let Some(runner) = runner {
            crate::task::spawn_on(crate::task::Carry::Detached, &runtime, self.until_closed(runner.run_in_epoch()));
        }
        Ok(())
    }

    pub(crate) async fn resume_windows(self: &Arc<Self>, admitted: &crate::ffi::legacy::Admitted, cause: Cause) -> Result<ResumeReport, SyncError> {
        if !self.admitted(crate::ffi::legacy::Origin::Session) { return Ok(ResumeReport::default()); }
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
        let mut unforced = std::collections::BTreeSet::new();
        for key in watched.keys() {
            if cause == Cause::Event && self.owner.session_refused(key) { continue; }
            let (model, relation, _) = &watched[key];
            let on_foreground = cause == Cause::Event && self.db.spec(model).is_ok_and(|spec| spec.relations.iter().any(|declared| &declared.name == relation && declared.refresh_on_foreground));
            let freshness = self.db.shared.store.window_freshness(key)?;
            if freshness.as_ref().is_none_or(|freshness| freshness.stale_since.is_some()) { unforced.insert(key.clone()); }
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
                let admitted = admitted.clone();
                let force = !unforced.contains(&key);
                loads.spawn(crate::task::Carry::here(), async move {
                    let resolved = inner.resolve(crate::ffi::legacy::Origin::Session, &model, &relation, &params)?;
                    if resolved.members_complete || resolved.media.is_some() {
                        tokio::select! {
                            _ = inner.owner.unobserved(&resolved.window_key) => Err(SyncError::Cancelled),
                            result = inner.load_scope(&admitted, &resolved, force) => result,
                        }
                    } else { inner.fetch(&admitted, &resolved, None, force).await }
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
