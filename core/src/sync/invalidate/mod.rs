mod mark;

use std::collections::BTreeMap;

use serde_json::{Map, Value};

use super::error::SyncError;
use super::load::{LoadOptions, LoadOutcome};
use super::{locked, Inner, Sync};

impl Inner {
    pub(crate) async fn reload_watched(&self, model: &str, relations: Option<&std::collections::BTreeSet<String>>, only_stale: bool) -> usize {
        let targets: BTreeMap<String, (String, Map<String, Value>)> = locked(&self.watches)
            .values()
            .filter(|entry| entry.model == model && relations.is_none_or(|names| names.contains(&entry.relation)))
            .map(|entry| (entry.key.clone(), (entry.relation.clone(), entry.params.clone())))
            .collect();
        let mut reloaded = 0;
        for (key, (relation, params)) in targets {
            let stale = self.db.shared.store.window_freshness(&key).ok().flatten().is_some_and(|freshness| freshness.invalidated);
            if only_stale && !stale {
                continue;
            }
            let Ok(resolved) = self.resolve(model, &relation, &params) else { continue };
            let fetched = if only_stale { self.fetch_after_flights(&resolved).await } else { self.fetch(&resolved, None, false).await };
            reloaded += usize::from(fetched.is_ok());
        }
        reloaded
    }
}

impl Sync {
    pub async fn refresh(&self, model: &str, relation: &str, params: &Map<String, Value>) -> Result<LoadOutcome, SyncError> {
        self.load(model, relation, params, LoadOptions { force: true }).await
    }
}

#[cfg(test)]
mod mark_tests;
