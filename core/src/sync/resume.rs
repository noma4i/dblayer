use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Map, Value};
use tokio::task::JoinSet;

use crate::pacing::StaleScopeCandidate;

use super::error::SyncError;
use super::{locked, Inner, Sync, WatchEntry};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ResumeReport {
    pub chunks: usize,
    pub loaded: usize,
    pub failed: usize,
}

pub struct Watch {
    inner: Arc<Inner>,
    id: u64,
    live: Option<String>,
}

impl Drop for Watch {
    fn drop(&mut self) {
        locked(&self.inner.watches).remove(&self.id);
        if let Some(key) = &self.live {
            self.inner.release_live(key);
        }
    }
}

impl Sync {
    pub fn watch(&self, model: &str, relation: &str, params: &Map<String, Value>) -> Result<Watch, SyncError> {
        let key = self.inner.resolve(model, relation, params)?.window_key;
        let id = self.inner.next_watch.fetch_add(1, Ordering::SeqCst) as u64;
        let entry = WatchEntry { model: model.to_string(), relation: relation.to_string(), params: params.clone(), key };
        locked(&self.inner.watches).insert(id, entry);
        let live = self.inner.acquire_live(model, relation);
        Ok(Watch { inner: self.inner.clone(), id, live })
    }

    pub async fn resume(&self) -> Result<ResumeReport, SyncError> {
        let now = self.inner.clock.now();
        let drain = self.inner.config.resume;
        let watched: BTreeMap<String, (String, String, Map<String, Value>)> = locked(&self.inner.watches)
            .values()
            .map(|entry| (entry.key.clone(), (entry.model.clone(), entry.relation.clone(), entry.params.clone())))
            .collect();
        let due = now.saturating_sub(drain.stale_time);
        let mut candidates = Vec::with_capacity(watched.len());
        for key in watched.keys() {
            let last_fetched_at = match self.inner.db.shared.store.window_freshness(key)? {
                Some(freshness) if !freshness.invalidated => Duration::from_millis(freshness.landed_at.max(0) as u64),
                _ => due,
            };
            candidates.push(StaleScopeCandidate { key: key.clone(), last_fetched_at, has_readers: true });
        }
        let chunks = drain.chunks(&candidates, now).map_err(|error| SyncError::Params(error.to_string()))?;
        let mut report = ResumeReport { chunks: chunks.len(), ..ResumeReport::default() };
        for chunk in chunks {
            let mut loads = JoinSet::new();
            for key in chunk {
                let Some((model, relation, params)) = watched.get(&key).cloned() else { continue };
                let inner = self.inner.clone();
                loads.spawn(async move {
                    let resolved = inner.resolve(&model, &relation, &params)?;
                    inner.fetch(&resolved, None, true).await
                });
            }
            while let Some(finished) = loads.join_next().await {
                match finished {
                    Ok(Ok(_)) => report.loaded += 1,
                    _ => report.failed += 1,
                }
            }
        }
        Ok(report)
    }
}
