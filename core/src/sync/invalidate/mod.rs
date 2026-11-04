mod mark;

use std::collections::BTreeMap;

use serde_json::{Map, Value};

use super::error::SyncError;
use super::load::{LoadOptions, LoadOutcome};
use super::{Inner, Sync};

impl Inner {
    pub(crate) async fn reload_watched(&self, model: &str, relations: Option<&std::collections::BTreeSet<String>>, only_stale: bool) -> Result<usize, SyncError> {
        let targets: BTreeMap<String, (String, Map<String, Value>)> = self.db.observed_reads()
            .into_iter()
            .filter(|entry| entry.model == model && relations.is_none_or(|names| names.contains(&entry.relation)))
            .map(|entry| (entry.key, (entry.relation, entry.params)))
            .collect();
        let mut reloaded = 0;
        let mut errors = Vec::new();
        for (key, (relation, params)) in targets {
            let result = async {
                if only_stale && !self.db.shared.store.window_freshness(&key)?.is_some_and(|freshness| freshness.invalidated) { return Ok(false); }
                let resolved = self.resolve(crate::lifecycle::Origin::Session, model, &relation, &params)?;
                let outcome = if only_stale { self.fetch_after_flights(&resolved).await? } else { self.fetch(&resolved, None, false).await? };
                if let Some(error) = outcome.kept_error { return Err(error); }
                Ok(true)
            }.await;
            match result {
                Ok(loaded) => reloaded += usize::from(loaded),
                Err(error) => errors.push(format!("{model}.{relation}: {error}")),
            }
        }
        if errors.is_empty() { Ok(reloaded) } else { Err(SyncError::Event(errors.join("\n"))) }
    }
}

impl Inner {
    /// Reads the watched lists of `relations` that are stale. A list with a load in flight is read by a task of its own after that load ends: its caller never waits for a read it did not ask for.
    pub(crate) async fn reload_stale(self: &std::sync::Arc<Self>, model: &str, relations: &std::collections::BTreeSet<String>) -> Result<usize, SyncError> {
        let targets: BTreeMap<String, (String, Map<String, Value>)> = self.db.observed_reads()
            .into_iter()
            .filter(|entry| entry.model == model && relations.contains(&entry.relation))
            .map(|entry| (entry.key, (entry.relation, entry.params)))
            .collect();
        let mut reloaded = 0;
        let mut errors = Vec::new();
        for (key, (relation, params)) in targets {
            let result = async {
                if !self.db.shared.store.window_freshness(&key)?.is_some_and(|freshness| freshness.invalidated) { return Ok(false); }
                let resolved = self.resolve(crate::lifecycle::Origin::Session, model, &relation, &params)?;
                if self.flights_pending(&resolved) {
                    let (inner, model, relation, params) = (self.clone(), model.to_string(), relation.clone(), params.clone());
                    crate::task::spawn(crate::task::Carry::here(), async move { inner.reload_after_flights(&model, &relation, &params).await });
                    return Ok(true);
                }
                let outcome = self.fetch_after_flights(&resolved).await?;
                if let Some(error) = outcome.kept_error { return Err(error); }
                Ok(true)
            }.await;
            match result {
                Ok(loaded) => reloaded += usize::from(loaded),
                Err(error) => errors.push(format!("{model}.{relation}: {error}")),
            }
        }
        if errors.is_empty() { Ok(reloaded) } else { Err(SyncError::Event(errors.join("\n"))) }
    }

    async fn reload_after_flights(&self, model: &str, relation: &str, params: &Map<String, Value>) {
        let Ok(resolved) = self.resolve(crate::lifecycle::Origin::Session, model, relation, params) else { return };
        self.flights_settled(&resolved).await;
        let _ = self.fetch(&resolved, None, true).await;
    }
}

impl Inner {
    pub(crate) async fn expire_and_reload_watched(&self, model: &str) -> Result<usize, SyncError> {
        let keys: Vec<String> = self.db.observed_reads().into_iter().filter(|entry| entry.model == model).map(|entry| entry.key).collect();
        self.db.shared.store.invalidate_windows(&self.db.authority()?, &keys)?;
        self.reload_watched(model, None, true).await
    }
}

impl Sync {
    pub async fn refresh(&self, model: &str, relation: &str, params: &Map<String, Value>) -> Result<LoadOutcome, SyncError> {
        self.load(model, relation, params, LoadOptions { force: true }).await
    }
}

#[cfg(test)]
mod mark_tests;
