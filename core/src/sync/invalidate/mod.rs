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
