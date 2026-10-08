use std::sync::Mutex;
use tokio::sync::watch;
use crate::db::{Change, ObserverHandle};
use super::row_json;

#[derive(Clone, Debug, uniffi::Record)]
pub struct LiveChange {
    pub revision: i64,
    pub rows: Vec<String>,
    pub count: Option<i64>,
    pub diff_json: String,
    pub related_json: String,
    pub error: Option<String>,
}

impl LiveChange {
    pub(super) fn rows(change: &Change) -> Self {
        let related: serde_json::Map<String, serde_json::Value> = change.related.iter().map(|(name, parents)| {
            let values: serde_json::Map<String, serde_json::Value> = parents.iter().map(|(id, rows)| {
                (id.to_string(), serde_json::json!(rows.iter().map(row_json).collect::<Vec<_>>()))
            }).collect();
            (name.clone(), serde_json::Value::Object(values))
        }).collect();
        Self {
            revision: change.revision, rows: change.rows.iter().map(row_json).collect(), count: None,
            diff_json: serde_json::json!({ "inserted": change.diff.inserted, "removed": change.diff.removed,
                "updated": change.diff.updated, "moved": change.diff.moved, "replaced": change.diff.replaced }).to_string(),
            related_json: serde_json::Value::Object(related).to_string(), error: None,
        }
    }
}

#[uniffi::export(rust, foreign)]
pub trait QueryListener: Send + Sync { fn changed(&self, change: LiveChange); }

pub(super) struct LiveHandles {
    pub _observer: ObserverHandle,
    pub _watch: Option<crate::sync::Watch>,
    pub load: Option<tokio::task::AbortHandle>,
}

impl Drop for LiveHandles {
    fn drop(&mut self) { if let Some(load) = self.load.take() { load.abort(); } }
}

#[derive(uniffi::Object)]
pub struct LiveSubscription {
    pub(super) latest: watch::Sender<Option<LiveChange>>,
    pub(super) handles: Mutex<Option<LiveHandles>>,
}

#[uniffi::export]
impl LiveSubscription {
    pub async fn current(&self) -> LiveChange {
        let mut receiver = self.latest.subscribe();
        let value = receiver.wait_for(Option::is_some).await.expect("live owns sender").as_ref().expect("first delivery").clone();
        value
    }
    pub fn cancel(&self) { self.handles.lock().expect("live handles").take(); }
}
