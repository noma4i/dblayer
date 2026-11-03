use serde_json::{Map, Value};

use super::error::SyncError;
use super::load::Resolved;
use super::load_machine::{Data, Event, LoadError, Order, Stored};
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
    pub(crate) fn stored(&self, resolved: &Resolved<'_>) -> Result<Option<Stored>, SyncError> {
        self.stored_of(&resolved.model, resolved.scope.key.as_str(), &resolved.window_key)
    }

    fn stored_of(&self, model: &str, scope_key: &str, window_key: &str) -> Result<Option<Stored>, SyncError> {
        crate::blocking::blocking_unit(|| self.stored_unit(model, scope_key, window_key))
    }

    fn stored_unit(&self, model: &str, scope_key: &str, window_key: &str) -> Result<Option<Stored>, SyncError> {
        let epoch = self.owner.epoch();
        let store = &self.db.shared.store;
        let rows = store.read(&[model], |reader| reader.scope_first_key(model, scope_key));
        let result = (|| {
            let has_rows = rows.as_ref().map_err(|error| SyncError::Store(error.to_string()))?.is_some();
            let window = store.window(window_key)?;
            let freshness = store.window_freshness(window_key)?;
            Ok(Some(match (window, freshness) {
                (Some(window), Some(freshness)) => Stored {
                    epoch,
                    order: Order { applied: window.applied, pages: window.pages },
                    data: Data::Landed { has_more: window.has_next, total: window.total, loaded_at: freshness.landed_at },
                    has_rows,
                    revision: 0,
                },
                _ => Stored { epoch, order: Order::NONE, data: if has_rows { Data::Local } else { Data::NotLoaded }, has_rows, revision: 0 },
            }))
        })();
        if let Err(error) = &result {
            let seed = match &rows {
                Ok(row) => Some(Stored { epoch, order: Order::NONE, data: if row.is_some() { Data::Local } else { Data::NotLoaded }, has_rows: row.is_some(), revision: 0 }),
                Err(_) => None,
            };
            self.owner.seed_observed(window_key, model.to_string(), seed);
            self.owner.submit(Event::ReadFailed { window: window_key.to_string(), error: LoadError::of(error) });
        } else if let Ok(Some(seed)) = &result {
            self.owner.submit(Event::ReadOk { window: window_key.to_string(), has_rows: seed.has_rows });
        }
        result
    }
}

impl Sync {
    pub async fn relation_state(&self, model: &str, relation: &str, params: &Map<String, Value>) -> Result<RelationWatch, SyncError> {
        let resolved = self.inner.resolve(crate::lifecycle::Origin::Session, model, relation, params)?;
        let watch = self.inner.owner.observe(&resolved.window_key, resolved.target(), None);
        let (inner, window, model, scope) = (self.inner.clone(), resolved.window_key.clone(), resolved.model.clone(), resolved.scope.key.as_str().to_string());
        let seeded = tokio::task::spawn_blocking(move || {
            let seed = inner.stored_of(&model, &scope, &window);
            if let Ok(seed) = seed { inner.owner.seed_observed(&window, model, seed); }
        });
        seeded.await.map_err(|error| SyncError::Event(error.to_string()))?;
        Ok(watch)
    }
}
