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
        let epoch = self.owner.epoch();
        let store = &self.db.shared.store;
        let window = store.window(&resolved.window_key).ok()?;
        let freshness = store.window_freshness(&resolved.window_key).ok()?;
        let has_rows = store.read(&[resolved.model.as_str()], |reader| reader.scope_first_key(resolved.model.as_str(), resolved.scope.key.as_str())).ok()?.is_some();
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
    pub fn relation_state(&self, model: &str, relation: &str, params: &Map<String, Value>) -> Result<RelationWatch, SyncError> {
        let resolved = self.inner.resolve(model, relation, params)?;
        Ok(self.inner.owner.observe(&resolved.window_key, resolved.target(), self.inner.stored(&resolved)))
    }
}
