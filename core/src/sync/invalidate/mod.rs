mod mark;

#[derive(Debug, Clone)]
pub(crate) enum WindowEvent {
    Mutation { model: String, action: String },
    DestroyRefused { model: String },
}

#[derive(Debug, Clone)]
pub(crate) enum WindowTrigger {
    Lost { model: String, subscription: u64, first: u64, last: u64 },
    ParentMissing { model: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WindowMark {
    pub(crate) since: i64,
    pub(crate) epoch: u64,
    pub(crate) families: Vec<(String, String)>,
}

use std::collections::BTreeMap;

use serde_json::{Map, Value};

use super::error::SyncError;
use super::load::{LoadOptions, LoadOutcome};
use super::{Inner, Sync};

impl Inner {
    pub(crate) async fn reload_watched(&self, admitted: &crate::lifecycle::Admitted, model: &str, relations: Option<&std::collections::BTreeSet<String>>, only_stale: bool) -> Result<usize, SyncError> {
        let targets: BTreeMap<String, (String, Map<String, Value>)> = self.db.observed_reads()
            .into_iter()
            .filter(|entry| entry.model == model && relations.is_none_or(|names| names.contains(&entry.relation)))
            .map(|entry| (entry.key, (entry.relation, entry.params)))
            .collect();
        let mut reloaded = 0;
        let mut errors = Vec::new();
        for (key, (relation, params)) in targets {
            let result = async {
                if only_stale && !self.db.shared.store.window_freshness(&key)?.is_some_and(|freshness| freshness.stale_since.is_some()) { return Ok(false); }
                let resolved = self.resolve(crate::lifecycle::Origin::Session, model, &relation, &params)?;
                let outcome = if only_stale {
                    self.flights_settled(&resolved).await;
                    if !self.db.shared.store.window_freshness(&key)?.is_some_and(|freshness| freshness.stale_since.is_some()) { return Ok(false); }
                    self.fetch(admitted, &resolved, None, true).await?
                } else { self.fetch(admitted, &resolved, None, false).await? };
                outcome.paid()?;
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
    pub(crate) async fn expire_and_reload_watched(&self, admitted: &crate::lifecycle::Admitted, model: &str, since: Option<i64>) -> Result<usize, SyncError> {
        let keys: Vec<String> = self.db.observed_reads().into_iter().filter(|entry| entry.model == model).map(|entry| entry.key).collect();
        let since = match since { Some(since) => since, None => self.next_issued()? };
        self.db.shared.store.invalidate_windows(&admitted.authority(), &keys, since)?;
        self.reload_watched(admitted, model, None, true).await
    }
}

impl Sync {
    pub async fn refresh(&self, admitted: &crate::lifecycle::Admitted, model: &str, relation: &str, params: &Map<String, Value>) -> Result<LoadOutcome, SyncError> {
        self.inner.ensure_admitted(admitted)?;
        self.load(admitted, model, relation, params, LoadOptions { force: true }).await
    }
}

#[cfg(test)]
mod mark_tests;
