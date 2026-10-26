use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use serde_json::{Map, Value};
use tokio::runtime::Handle;
use tokio::sync::{watch, Semaphore};
use tokio::task::JoinSet;

use crate::schema::{PreloadChoice, PreloadSource};

use super::error::SyncError;
use super::load_machine::LoadError;
use super::load_owner::FlightTicket;
use super::{locked, Inner, Sync};

#[derive(Default)]
struct State {
    due: bool,
    running: bool,
    session: Option<String>,
    power_saving: bool,
    low_ram: bool,
    suppressed: bool,
    unseen: usize,
    app_loads: usize,
    first_snapshot: bool,
}

impl State {
    fn held(&self) -> bool {
        self.suppressed || self.power_saving || self.low_ram
    }

    fn yielding(&self) -> bool {
        !self.first_snapshot && (self.unseen > 0 || self.app_loads > 0)
    }
}

const PRELOAD_PARALLEL: usize = 3;

#[derive(Debug, Clone)]
pub struct PreloadFailure {
    pub model: String,
    pub relation: String,
    pub params: Map<String, Value>,
    pub error: SyncError,
}

pub(crate) struct PreloadOwner {
    state: Mutex<State>,
    busy: watch::Sender<usize>,
    inner: OnceLock<Weak<Inner>>,
    runtime: OnceLock<Handle>,
}

impl PreloadOwner {
    pub(crate) fn attach(&self, inner: Weak<Inner>) {
        let _ = self.inner.set(inner);
    }
}

impl Default for PreloadOwner {
    fn default() -> Self {
        Self { state: Mutex::new(State::default()), busy: watch::channel(0).0, inner: OnceLock::new(), runtime: OnceLock::new() }
    }
}

pub(crate) struct AppLoadHold(Weak<Inner>);

impl Drop for AppLoadHold {
    fn drop(&mut self) {
        let Some(inner) = self.0.upgrade() else { return };
        {
            let mut state = locked(&inner.preload.state);
            state.app_loads -= 1;
            inner.preload.busy.send_replace(state.app_loads);
        }
        inner.preload_start();
    }
}

pub(crate) struct FirstScreenHold {
    inner: Weak<Inner>,
    delivered: bool,
}

impl FirstScreenHold {
    pub(crate) fn delivered(mut self) {
        self.delivered = true;
    }
}

impl Drop for FirstScreenHold {
    fn drop(&mut self) {
        let Some(inner) = self.inner.upgrade() else { return };
        {
            let mut state = locked(&inner.preload.state);
            state.unseen -= 1;
            state.first_snapshot |= self.delivered;
        }
        inner.preload_start();
    }
}

type Window = (String, String, Map<String, Value>);

impl Inner {
    fn preloaded_windows(&self) -> Result<Vec<Window>, SyncError> {
        let mut windows = Vec::new();
        for (model, spec) in &self.db.shared.specs {
            for relation in &spec.relations {
                let Some(values) = &relation.preload else { continue };
                let mut combinations = vec![Map::new()];
                for (name, choice) in values {
                    let choices = match choice {
                        PreloadChoice::Listed(listed) => listed.clone(),
                        PreloadChoice::RowsOf { from } => self.source_ids(from)?,
                    };
                    combinations = combinations.into_iter().flat_map(|base| choices.iter().map(move |choice| {
                        let mut next = base.clone();
                        next.insert(name.clone(), choice.clone());
                        next
                    })).collect();
                }
                windows.extend(combinations.into_iter().map(|params| (model.clone(), relation.name.clone(), params)));
            }
        }
        Ok(windows)
    }

    fn source_ids(&self, source: &PreloadSource) -> Result<Vec<Value>, SyncError> {
        let spec = self.db.spec(&source.model).map_err(|_| SyncError::UnknownModel(source.model.clone()))?;
        let key = spec.selection_key(&source.relation, &Map::new()).map_err(|error| SyncError::Params(error.to_string()))?;
        let members = self.db.shared.store.scope_members(&source.model, key.as_str())?;
        Ok(members.into_iter().map(|(id, _)| Value::String(id)).collect())
    }

    pub(crate) fn preload_on_source(&self, model: &str, relation: &str) {
        let feeds = self.db.shared.specs.values().flat_map(|spec| &spec.relations).filter_map(|declared| declared.preload.as_ref()).flat_map(BTreeMap::values);
        if !feeds.into_iter().any(|choice| matches!(choice, PreloadChoice::RowsOf { from } if from.model == model && from.relation == relation)) { return; }
        locked(&self.preload.state).due = true;
        if let Some(inner) = self.preload.inner.get().and_then(Weak::upgrade) { inner.preload_start(); }
    }

    pub(crate) fn preload_on_event(self: &Arc<Self>) {
        locked(&self.preload.state).due = true;
        self.preload_start();
    }

    pub(crate) fn preload_start(self: &Arc<Self>) {
        if let Ok(runtime) = Handle::try_current() { let _ = self.preload.runtime.set(runtime); }
        let mut state = locked(&self.preload.state);
        if !state.due || state.running || state.held() || state.yielding() || !self.connectivity.is_online() { return; }
        let Some(runtime) = Handle::try_current().ok().or_else(|| self.preload.runtime.get().cloned()) else { return };
        state.running = true;
        let inner = Arc::downgrade(self);
        runtime.spawn(self.until_closed(async move {
            while let Some(inner) = inner.upgrade() {
                if !inner.preload_take() { return; }
                inner.preload_windows().await;
            }
        }));
    }

    fn preload_take(&self) -> bool {
        let signed_in = self.transport.session();
        let mut state = locked(&self.preload.state);
        let ready = state.due && !state.held() && !state.yielding() && self.connectivity.is_online() && matches!(signed_in, Ok(Some(_)));
        if ready { state.due = false; } else { state.running = false; }
        drop(state);
        if let Err(error) = signed_in { self.preload_failed(&SyncError::Transport(error)); }
        ready
    }

    fn preload_failed(&self, error: &SyncError) {
        let Ok(windows) = self.preloaded_windows() else { return };
        for (model, relation, params) in windows {
            let Ok(resolved) = self.resolve(&model, &relation, &params) else { continue };
            FlightTicket::new(&self.owner, &resolved.window_key, resolved.target(), false, self.stored(&resolved)).failed(LoadError::of(error));
        }
    }

    async fn preload_windows(self: &Arc<Self>) {
        let lanes = Arc::new(Semaphore::new(PRELOAD_PARALLEL));
        let windows = match self.preloaded_windows() {
            Ok(windows) => windows,
            Err(_) => return,
        };
        let mut loads = JoinSet::new();
        for (model, relation, params) in windows {
            let (inner, lanes) = (self.clone(), lanes.clone());
            loads.spawn(async move {
                let _lane = lanes.acquire_owned().await;
                inner.app_loads_settled().await;
                let resolved = inner.resolve(&model, &relation, &params)?;
                inner.fetch(&resolved, None, false).await
            });
        }
        while let Some(finished) = loads.join_next().await {
            if matches!(finished, Ok(Err(SyncError::Offline))) { locked(&self.preload.state).due = true; }
        }
    }

    async fn app_loads_settled(&self) {
        let mut busy = self.preload.busy.subscribe();
        let _ = busy.wait_for(|loads| *loads == 0).await;
    }

    pub(crate) fn app_load(self: &Arc<Self>) -> AppLoadHold {
        let mut state = locked(&self.preload.state);
        state.app_loads += 1;
        self.preload.busy.send_replace(state.app_loads);
        AppLoadHold(Arc::downgrade(self))
    }

    pub(crate) fn first_screen(self: &Arc<Self>) -> FirstScreenHold {
        locked(&self.preload.state).unseen += 1;
        FirstScreenHold { inner: Arc::downgrade(self), delivered: false }
    }

    pub(crate) fn preload_on_session(self: &Arc<Self>) {
        match self.transport.session() {
            Ok(current) => {
                let changed = {
                    let mut state = locked(&self.preload.state);
                    let changed = std::mem::replace(&mut state.session, current.clone()) != current;
                    if changed { state.first_snapshot = false; }
                    changed
                };
                if changed && current.is_some() { self.preload_on_event(); }
            }
            Err(error) => {
                locked(&self.preload.state).due = true;
                self.preload_failed(&SyncError::Transport(error));
            }
        }
    }
}

impl Sync {
    pub(crate) fn app_load(&self) -> AppLoadHold {
        self.inner.app_load()
    }

    pub(crate) fn first_screen(&self) -> FirstScreenHold {
        self.inner.first_screen()
    }

    pub fn set_power_saving(&self, saving: bool) {
        let was_saving = std::mem::replace(&mut locked(&self.inner.preload.state).power_saving, saving);
        if was_saving && !saving { self.inner.preload_start(); }
    }

    pub async fn refresh_preloads(&self) -> Result<Vec<PreloadFailure>, SyncError> {
        let _app = self.app_load();
        if !self.inner.connectivity.is_online() { return Err(SyncError::Offline); }
        let lanes = Arc::new(Semaphore::new(PRELOAD_PARALLEL));
        let mut refreshed = HashSet::new();
        let mut failures = Vec::new();
        loop {
            let windows: Vec<Window> = self.inner.preloaded_windows()?.into_iter().filter(|(model, relation, params)| refreshed.insert(format!("{model}|{relation}|{params:?}"))).collect();
            if windows.is_empty() { return Ok(failures); }
            let mut loads = JoinSet::new();
            for window in windows {
                let (inner, lanes) = (self.inner.clone(), lanes.clone());
                loads.spawn(async move {
                    let _lane = lanes.acquire_owned().await;
                    let (model, relation, params) = window;
                    let fetched = match inner.resolve(&model, &relation, &params) {
                        Ok(resolved) => inner.fetch(&resolved, None, true).await.map(|outcome| outcome.kept_error),
                        Err(error) => Err(error),
                    };
                    match fetched {
                        Ok(None) => None,
                        Ok(Some(error)) | Err(error) => Some(PreloadFailure { model, relation, params, error }),
                    }
                });
            }
            while let Some(finished) = loads.join_next().await {
                if let Ok(Some(failure)) = finished { failures.push(failure); }
            }
        }
    }

    pub fn set_preload(&self, enabled: bool) {
        locked(&self.inner.preload.state).suppressed = !enabled;
    }

    pub fn set_low_ram(&self, low_ram: bool) {
        locked(&self.inner.preload.state).low_ram = low_ram;
    }

    pub fn preload_on_session(&self) {
        self.inner.preload_on_session();
    }
}

#[cfg(test)]
impl Sync {
    pub(crate) fn preload_running(&self) -> bool {
        locked(&self.inner.preload.state).running
    }

    pub(super) fn preload_taken(&self) -> bool {
        self.inner.preload_take()
    }
}
