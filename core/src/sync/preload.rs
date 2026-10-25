use std::sync::{Arc, Mutex};

use serde_json::{Map, Value};
use tokio::runtime::Handle;
use tokio::task::JoinSet;

use super::error::SyncError;
use super::load_machine::LoadError;
use super::load_owner::FlightTicket;
use super::{locked, Inner, Sync};

#[derive(Default)]
struct State {
    due: bool,
    running: bool,
    session: Option<String>,
}

#[derive(Default)]
pub(crate) struct PreloadOwner {
    state: Mutex<State>,
}

type Window = (String, String, Map<String, Value>);

impl Inner {
    fn preloaded_windows(&self) -> Vec<Window> {
        let mut windows = Vec::new();
        for (model, spec) in &self.db.shared.specs {
            for relation in &spec.relations {
                let Some(values) = &relation.preload else { continue };
                let mut combinations = vec![Map::new()];
                for (name, choices) in values {
                    combinations = combinations.into_iter().flat_map(|base| choices.iter().map(move |choice| {
                        let mut next = base.clone();
                        next.insert(name.clone(), choice.clone());
                        next
                    })).collect();
                }
                windows.extend(combinations.into_iter().map(|params| (model.clone(), relation.name.clone(), params)));
            }
        }
        windows
    }

    pub(crate) fn preload_on_event(self: &Arc<Self>) {
        locked(&self.preload.state).due = true;
        self.preload_start();
    }

    pub(crate) fn preload_start(self: &Arc<Self>) {
        let mut state = locked(&self.preload.state);
        if !state.due || state.running || !self.connectivity.is_online() { return; }
        let Ok(runtime) = Handle::try_current() else { return };
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
        let ready = state.due && self.connectivity.is_online() && matches!(signed_in, Ok(Some(_)));
        if ready { state.due = false; } else { state.running = false; }
        drop(state);
        if let Err(error) = signed_in { self.preload_failed(&SyncError::Transport(error)); }
        ready
    }

    fn preload_failed(&self, error: &SyncError) {
        for (model, relation, params) in self.preloaded_windows() {
            let Ok(resolved) = self.resolve(&model, &relation, &params) else { continue };
            FlightTicket::new(&self.owner, &resolved.window_key, resolved.target(), false, self.stored(&resolved)).failed(LoadError::of(error));
        }
    }

    async fn preload_windows(self: &Arc<Self>) {
        let mut loads = JoinSet::new();
        for (model, relation, params) in self.preloaded_windows() {
            let inner = self.clone();
            loads.spawn(async move {
                let resolved = inner.resolve(&model, &relation, &params)?;
                inner.fetch(&resolved, None, false).await
            });
        }
        while let Some(finished) = loads.join_next().await {
            if matches!(finished, Ok(Err(SyncError::Offline))) { locked(&self.preload.state).due = true; }
        }
    }

    pub(crate) fn preload_on_session(self: &Arc<Self>) {
        match self.transport.session() {
            Ok(current) => {
                let changed = std::mem::replace(&mut locked(&self.preload.state).session, current.clone()) != current;
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
    pub fn preload_on_session(&self) {
        self.inner.preload_on_session();
    }
}

#[cfg(test)]
impl Sync {
    pub(super) fn preload_running(&self) -> bool {
        locked(&self.inner.preload.state).running
    }
}
