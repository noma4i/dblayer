use serde_json::{Map, Value};

use super::error::SyncError;
use super::load::Resolved;
use super::load_machine::{Data, LoadError, Order, Stored};
use super::load_owner::{RelationWatch, Target};
use super::Inner;
use super::Sync;

impl LoadError {
    pub(super) fn of(error: &SyncError) -> Self {
        let kind = match error {
            SyncError::Offline | SyncError::Transport(_) => "network",
            SyncError::Graphql { .. } => "server",
            SyncError::Malformed(_) => "unrecognized",
            SyncError::Store(_) => "database",
            _ => "other",
        };
        Self { kind, message: error.to_string(), cause: Some(error.clone()) }
    }
}

impl Resolved<'_> {
    pub(super) fn target(&self) -> Target {
        Target { model: self.model.clone(), relation: self.relation.clone(), params: self.params.clone() }
    }
}

impl Inner {
    pub(crate) fn stored(&self, resolved: &Resolved<'_>) -> Option<Stored> {
        self.stored_of(&resolved.model, resolved.scope.key.as_str(), &resolved.window_key)
    }

    fn stored_of(&self, model: &str, scope_key: &str, window_key: &str) -> Option<Stored> {
        let epoch = self.owner.epoch();
        let store = &self.db.shared.store;
        let window = store.window(window_key).ok()?;
        let freshness = store.window_freshness(window_key).ok()?;
        let has_rows = store.read(&[model], |reader| reader.scope_first_key(model, scope_key)).ok()?.is_some();
        Some(match (window, freshness) {
            (Some(window), Some(freshness)) => Stored {
                epoch,
                order: Order { applied: window.applied, pages: window.pages },
                data: Data::Landed { has_more: window.has_next, total: window.total, loaded_at: freshness.landed_at },
                has_rows,
                revision: 0,
            },
            _ => Stored { epoch, order: Order::NONE, data: if has_rows { Data::Local } else { Data::NotLoaded }, has_rows, revision: 0 },
        })
    }
}

impl Sync {
    pub async fn relation_state(&self, model: &str, relation: &str, params: &Map<String, Value>) -> Result<RelationWatch, SyncError> {
        let resolved = self.inner.resolve(model, relation, params)?;
        let watch = self.inner.owner.observe(&resolved.window_key, resolved.target(), None);
        let (inner, window, model, scope) = (self.inner.clone(), resolved.window_key.clone(), resolved.model.clone(), resolved.scope.key.as_str().to_string());
        let seeded = tokio::task::spawn_blocking(move || {
            let seed = inner.stored_of(&model, &scope, &window);
            inner.owner.seed_observed(&window, model, seed);
        });
        seeded.await.map_err(|error| SyncError::Event(error.to_string()))?;
        Ok(watch)
    }
}
