use std::collections::HashMap;

use serde_json::{Map, Value};
use tokio::sync::watch;

use super::error::SyncError;
use super::{locked, FlightResult, Inner, Sync};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadPhase {
    Idle,
    Loading,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadError {
    pub kind: &'static str,
    pub message: String,
}

/// What a reader of a remote relation needs for its screen: is a load running, did it fail, is there a next page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationState {
    pub phase: LoadPhase,
    pub has_more: bool,
    pub total: Option<i64>,
    pub error: Option<LoadError>,
    pub loaded_at: Option<i64>,
}

pub(crate) struct StateEntry {
    sender: watch::Sender<RelationState>,
    loading: u32,
}

pub(crate) type States = HashMap<String, StateEntry>;

impl LoadError {
    fn of(error: &SyncError) -> Self {
        let kind = match error {
            SyncError::Offline | SyncError::Transport(_) => "network",
            SyncError::Graphql(_) => "server",
            SyncError::Malformed(_) => "unrecognized",
            _ => "other",
        };
        Self { kind, message: error.to_string() }
    }
}

impl Inner {
    fn stored_state(&self, window_key: &str) -> RelationState {
        let store = &self.db.shared.store;
        let window = store.window(window_key).ok().flatten();
        let has_more = window.as_ref().is_some_and(|window| window.has_next);
        let total = window.and_then(|window| window.total);
        let loaded_at = store.window_freshness(window_key).ok().flatten().map(|freshness| freshness.landed_at);
        RelationState { phase: LoadPhase::Idle, has_more, total, error: None, loaded_at }
    }

    fn state_entry<'a>(&self, states: &'a mut States, window_key: &str) -> &'a mut StateEntry {
        states.entry(window_key.to_string()).or_insert_with(|| StateEntry { sender: watch::channel(self.stored_state(window_key)).0, loading: 0 })
    }

    pub(crate) fn begin_load(&self, window_key: &str) {
        let mut states = locked(&self.states);
        let entry = self.state_entry(&mut states, window_key);
        entry.loading += 1;
        entry.sender.send_modify(|state| {
            state.phase = LoadPhase::Loading;
            state.error = None;
        });
    }

    pub(crate) fn end_load(&self, window_key: &str, result: &FlightResult) {
        let settled = self.stored_state(window_key);
        let mut states = locked(&self.states);
        let entry = self.state_entry(&mut states, window_key);
        entry.loading = entry.loading.saturating_sub(1);
        let loading = entry.loading > 0;
        entry.sender.send_modify(|state| match result {
            Ok(_) => {
                state.has_more = settled.has_more;
                state.total = settled.total;
                state.loaded_at = settled.loaded_at;
                state.error = None;
                state.phase = if loading { LoadPhase::Loading } else { LoadPhase::Idle };
            }
            Err(error) => {
                state.error = Some(LoadError::of(error));
                state.phase = if loading { LoadPhase::Loading } else { LoadPhase::Failed };
            }
        });
    }
}

impl Sync {
    pub fn relation_state(&self, model: &str, relation: &str, params: &Map<String, Value>) -> Result<watch::Receiver<RelationState>, SyncError> {
        let resolved = self.inner.resolve(model, relation, params)?;
        let mut states = locked(&self.inner.states);
        Ok(self.inner.state_entry(&mut states, &resolved.window_key).sender.subscribe())
    }
}
