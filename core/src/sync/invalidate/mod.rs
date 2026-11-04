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

/// A reread owed after one commit: the lists of `names` of `model`, the epoch of the work that queued it, and the stamp (`next_issued`) taken at the mark. A load that began before the stamp does not settle it.
pub(crate) struct StaleMark {
    model: String,
    names: std::collections::BTreeSet<String>,
    since: i64,
    epoch: crate::lifecycle::Epoch,
}

impl Inner {
    pub(crate) async fn reload_stale(self: &std::sync::Arc<Self>, mark: &StaleMark) -> Result<usize, SyncError> {
        if !self.db.shared.store.lifecycle.admits_epoch(mark.epoch.value()) { return Ok(0); }
        let model = mark.model.as_str();
        let targets: BTreeMap<String, (String, Map<String, Value>)> = self.db.observed_reads()
            .into_iter()
            .filter(|entry| entry.model == model && mark.names.contains(&entry.relation))
            .map(|entry| (entry.key, (entry.relation, entry.params)))
            .collect();
        let mut reloaded = 0;
        let mut errors = Vec::new();
        for (key, (relation, params)) in targets {
            let result = async {
                let store = &self.db.shared.store;
                let invalidated = store.window_freshness(&key)?.is_some_and(|freshness| freshness.invalidated);
                let resolved = self.resolve(crate::lifecycle::Origin::Session, model, &relation, &params)?;
                let predates_mark = store.window(&key)?.map_or_else(|| self.flight_pending(&resolved), |window| window.applied < mark.since);
                if !invalidated && !predates_mark { return Ok(false); }
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
