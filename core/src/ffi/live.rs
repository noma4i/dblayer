use std::sync::Mutex;
use tokio::sync::watch;
use crate::db::{Change, ObserverHandle, WindowHandle};
use super::wire::{WireModel, WireRow};

#[derive(Clone, Debug, uniffi::Record)]
pub struct Placement {
    pub id: String,
    pub index: u32,
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct Replacement {
    pub from: String,
    pub to: String,
}

/// The edges of a window: whether the query has local rows before or after it, and how many rows the query has.
#[derive(Clone, Debug, uniffi::Record)]
pub struct LiveWindowEdge {
    pub has_before: bool,
    pub has_after: bool,
    pub total: Option<u32>,
}

/// With `full` the `rows` are the whole list in order. Otherwise they are the rows that were inserted, updated or replaced, `row_indexes` holds the final index of each, and the wrapper rebuilds its array in this order:
/// 1. rename every `replaced.from` to its `to`, 2. drop `removed`, 3. take every `placed` id that is in the array out of it, then insert the placed rows in ascending index, each at its index (a row that was in the array keeps its value, a new one comes from `rows`), 4. put every changed row at its row index.
#[derive(Clone, Debug, uniffi::Record)]
pub struct LiveChange {
    pub revision: i64,
    pub full: bool,
    pub size: u32,
    pub rows: Vec<WireRow>,
    pub row_indexes: Vec<u32>,
    pub removed: Vec<String>,
    pub placed: Vec<Placement>,
    pub replaced: Vec<Replacement>,
    pub renamed: Vec<Replacement>,
    pub count: Option<i64>,
    pub error: Option<String>,
    #[uniffi(default = None)]
    pub window: Option<LiveWindowEdge>,
}

impl LiveChange {
    pub(super) fn rows(change: &Change, wire: &WireModel) -> Self {
        let diff = &change.diff;
        let row_indexes = if change.full { Vec::new() } else { change.rows.iter().map(|row| diff.at[&row.id] as u32).collect() };
        Self {
            revision: change.revision, full: change.full, size: change.size as u32, rows: change.rows.iter().map(|row| wire.row(row)).collect(), row_indexes,
            removed: diff.removed.iter().map(ToString::to_string).collect(),
            placed: diff.placed().into_iter().map(|(id, index)| Placement { id: id.to_string(), index: index as u32 }).collect(),
            replaced: diff.replaced.iter().map(|(from, to)| Replacement { from: from.to_string(), to: to.to_string() }).collect(),
            renamed: diff.renamed.iter().map(|(from, to)| Replacement { from: from.to_string(), to: to.to_string() }).collect(),
            count: None, error: None,
            window: change.window.map(|edge| LiveWindowEdge { has_before: edge.has_before, has_after: edge.has_after, total: edge.total.map(|total| total as u32) }),
        }
    }

    fn empty(revision: i64) -> Self {
        Self { revision, full: false, size: 0, rows: Vec::new(), row_indexes: Vec::new(), removed: Vec::new(), placed: Vec::new(), replaced: Vec::new(), renamed: Vec::new(), count: None, error: None, window: None }
    }

    pub(super) fn whole(revision: i64, rows: Vec<WireRow>) -> Self {
        Self { full: true, size: rows.len() as u32, rows, ..Self::empty(revision) }
    }

    pub(super) fn counted(revision: i64, count: i64) -> Self {
        Self { count: Some(count), ..Self::empty(revision) }
    }

    pub(super) fn failed(revision: i64, error: String) -> Self {
        Self { error: Some(error), ..Self::empty(revision) }
    }
}

#[uniffi::export(rust, foreign)]
pub trait QueryListener: Send + Sync { fn changed(&self, change: LiveChange); }

pub(super) struct LiveHandles {
    pub _observer: Option<ObserverHandle>,
    pub window: Option<WindowHandle>,
    pub _watch: Option<crate::sync::Watch>,
    pub load: Option<tokio::task::AbortHandle>,
}

impl Drop for LiveHandles {
    fn drop(&mut self) { if let Some(load) = self.load.take() { load.abort(); } }
}

#[derive(uniffi::Object)]
pub struct LiveSubscription {
    pub(super) latest: watch::Sender<Option<LiveChange>>,
    pub(super) initial_load: watch::Receiver<Option<Result<(), crate::sync::SyncError>>>,
    pub(super) handles: Mutex<Option<LiveHandles>>,
}

#[uniffi::export]
impl LiveSubscription {
    pub async fn wait_for_initial_load(&self) -> Result<(), super::DbError> {
        let mut receiver = self.initial_load.clone();
        loop {
            if let Some(result) = receiver.borrow_and_update().clone() {
                return result.map_err(Into::into);
            }
            receiver.changed().await
                .map_err(|_| super::DbError::Interrupted { detail: "initial load cancelled".into() })?;
        }
    }
    pub async fn current(&self) -> LiveChange {
        let mut receiver = self.latest.subscribe();
        let value = receiver.wait_for(Option::is_some).await.expect("live owns sender").as_ref().expect("first delivery").clone();
        value
    }
    pub fn cancel(&self) { crate::db::locked(&self.handles).take(); }
    pub fn expand_before(&self, rows: u32) {
        if let Some(window) = crate::db::locked(&self.handles).as_ref().and_then(|handles| handles.window.as_ref()) { window.expand_before(rows as usize); }
    }
    pub fn expand_after(&self, rows: u32) {
        if let Some(window) = crate::db::locked(&self.handles).as_ref().and_then(|handles| handles.window.as_ref()) { window.expand_after(rows as usize); }
    }
}
