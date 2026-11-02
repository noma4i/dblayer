use std::sync::{Arc, Mutex};
use crate::db::{Change, Feed, ObserverHandle, WindowHandle};
use super::live_feed::{LiveFeed, Readiness};
use super::wire::{WireModel, WireRow};

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum LivePhase {
    NotLoaded,
    Loading,
    Loaded,
    Empty,
    Failed,
    Closed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum LoadMoreResult {
    Loaded { has_more: bool },
    NoMore,
    AlreadyLoading,
}

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
    pub error: Option<super::DbError>,
    #[uniffi(default = None)]
    pub window: Option<LiveWindowEdge>,
    #[uniffi(default = [])]
    pub inserted: Vec<u32>,
    #[uniffi(default = [])]
    pub updated: Vec<u32>,
    #[uniffi(default = [])]
    pub moved: Vec<Placement>,
    #[uniffi(default = 0)]
    pub seq: u64,
    #[uniffi(default = None)]
    pub phase: Option<LivePhase>,
    #[uniffi(default = false)]
    pub initial: bool,
    #[uniffi(default = false)]
    pub refreshing: bool,
    #[uniffi(default = false)]
    pub fetching_more: bool,
    #[uniffi(default = false)]
    pub has_more: bool,
    #[uniffi(default = false)]
    pub complete: bool,
    #[uniffi(default = None)]
    pub total: Option<i64>,
    #[uniffi(default = None)]
    pub loaded_at: Option<i64>,
    #[uniffi(default = None)]
    pub error_kind: Option<String>,
}

impl LiveChange {
    pub(super) fn rows(change: &Change, wire: &WireModel) -> Self {
        let diff = &change.diff;
        let inserted: std::collections::HashSet<_> = diff.inserted.iter().collect();
        let (inserted, updated) = change.rows.iter().enumerate().partition::<Vec<_>, _>(|(_, row)| change.full || inserted.contains(&row.id));
        let row_indexes = if change.full { Vec::new() } else { change.rows.iter().map(|row| diff.at[&row.id] as u32).collect() };
        Self {
            revision: change.revision, full: change.full, size: change.size as u32, rows: change.rows.iter().map(|row| wire.row(row)).collect(), row_indexes,
            removed: diff.removed.iter().map(ToString::to_string).collect(),
            placed: diff.placed().into_iter().map(|(id, index)| Placement { id: id.to_string(), index: index as u32 }).collect(),
            replaced: diff.replaced.iter().map(|(from, to)| Replacement { from: from.to_string(), to: to.to_string() }).collect(),
            renamed: diff.renamed.iter().map(|(from, to)| Replacement { from: from.to_string(), to: to.to_string() }).collect(),
            count: None, error: None,
            window: change.window.map(|edge| LiveWindowEdge { has_before: edge.has_before, has_after: edge.has_after, total: edge.total.map(|total| total as u32) }),
            inserted: inserted.into_iter().map(|(index, _)| index as u32).collect(),
            updated: updated.into_iter().map(|(index, _)| index as u32).collect(),
            moved: diff.moved.iter().map(|id| Placement { id: id.to_string(), index: diff.at[id] as u32 }).collect(),
            ..Self::empty(change.revision)
        }
    }

    pub(super) fn carry_state(&mut self, from: &LiveChange) {
        self.seq = from.seq;
        self.phase = from.phase;
        self.initial = from.initial;
        self.refreshing = from.refreshing;
        self.fetching_more = from.fetching_more;
        self.has_more = from.has_more;
        self.complete = from.complete;
        self.total = from.total;
        self.loaded_at = from.loaded_at;
        self.error_kind.clone_from(&from.error_kind);
    }

    pub(super) fn empty(revision: i64) -> Self {
        Self {
            revision, full: false, size: 0, rows: Vec::new(), row_indexes: Vec::new(), removed: Vec::new(), placed: Vec::new(), replaced: Vec::new(), renamed: Vec::new(), count: None, error: None, window: None, inserted: Vec::new(), updated: Vec::new(), moved: Vec::new(),
            seq: 0, phase: None, initial: false, refreshing: false, fetching_more: false, has_more: false, complete: false, total: None, loaded_at: None, error_kind: None,
        }
    }

    pub(super) fn whole(revision: i64, rows: Vec<WireRow>) -> Self {
        Self { full: true, size: rows.len() as u32, inserted: (0..rows.len() as u32).collect(), rows, ..Self::empty(revision) }
    }

    pub(super) fn counted(revision: i64, count: i64) -> Self {
        Self { count: Some(count), ..Self::empty(revision) }
    }
}

#[uniffi::export(rust, foreign)]
pub trait QueryListener: Send + Sync { fn changed(&self, change: LiveChange); }

pub(super) struct LiveHandles {
    pub _observer: Option<ObserverHandle>,
    pub window: Option<WindowHandle>,
    pub _watch: Option<crate::sync::Watch>,
    pub load: Option<tokio::task::AbortHandle>,
    pub failure: Option<tokio::task::AbortHandle>,
    pub state: Option<tokio::task::AbortHandle>,
    pub targets: Option<Arc<Mutex<std::collections::HashMap<crate::identity::RowId, ObserverHandle>>>>,
}

impl Drop for LiveHandles {
    fn drop(&mut self) {
        for task in [self.load.take(), self.failure.take(), self.state.take()].into_iter().flatten() { task.abort(); }
        if let Some(targets) = self.targets.take() {
            let retired = std::mem::take(&mut *targets.lock().unwrap_or_else(std::sync::PoisonError::into_inner));
            drop(retired);
        }
    }
}

pub(super) fn follow_failures(watch: Option<&mut crate::sync::Watch>, feed: Arc<LiveFeed>) -> Option<tokio::task::AbortHandle> {
    let failures = watch?.failures()?;
    Some(follow(failures, feed))
}

pub(super) fn follow(failures: crate::sync::Failures, feed: Arc<LiveFeed>) -> tokio::task::AbortHandle {
    let crate::sync::Failures { current, recheck, mut receiver, sources } = failures;
    let post = |feed: &LiveFeed, failure: crate::transport::SubscriptionFailure| feed.subscription_failed(Some(super::DbError::from(crate::sync::SyncError::Transport(failure.error))));
    let mut failed = current.is_some();
    if let Some(failure) = current { post(&feed, failure); }
    tokio::spawn(async move {
        loop {
            match receiver.recv().await {
                Ok(None) => { failed = false; feed.subscription_failed(None); }
                Ok(Some((source, failure))) if sources.contains(&source) && !failed => { post(&feed, failure); failed = true; }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => match recheck() {
                    Some(failure) if !failed => { post(&feed, failure); failed = true; }
                    None if failed => { failed = false; feed.subscription_failed(None); }
                    _ => {}
                },
                _ => {}
            }
        }
    }).abort_handle()
}

pub(super) fn intake(feed: &LiveFeed, wire: &WireModel, item: Feed) {
    match item {
        Feed::Rows(change) => feed.rows(LiveChange::rows(&change, wire)),
        Feed::Count(change) => feed.count(LiveChange::counted(change.revision, change.count)),
        Feed::Reached(revision) => feed.reached(revision),
        Feed::Failed(detail) => feed.read_failed(detail),
    }
}

#[derive(uniffi::Object)]
pub struct LiveSubscription {
    pub(super) feed: Arc<LiveFeed>,
    pub(super) handles: Mutex<Option<LiveHandles>>,
}

impl LiveSubscription {
    fn window_control(&self) -> Option<crate::db::ObserverControl> {
        let handles = crate::db::locked(&self.handles);
        handles.as_ref().and_then(|handles| handles.window.as_ref()).map(WindowHandle::control)
    }
}

#[uniffi::export]
impl LiveSubscription {
    pub async fn wait_for_initial_load(&self) -> Result<(), super::DbError> {
        let mut receiver = self.feed.readiness.subscribe();
        let seen = *receiver.wait_for(|readiness| *readiness != Readiness::Waiting).await
            .map_err(|_| super::DbError::Interrupted { detail: "initial load cancelled".into() })?;
        match seen {
            Readiness::Ended => Err(super::DbError::Interrupted { detail: "initial load cancelled".into() }),
            _ => self.feed.load_error().map_or(Ok(()), Err),
        }
    }
    pub async fn current(&self) -> LiveChange {
        let mut receiver = self.feed.latest.subscribe();
        let value = receiver.wait_for(Option::is_some).await.expect("live owns sender").as_ref().expect("first delivery").clone();
        value
    }
    pub fn cancel(&self) {
        let handles = crate::db::locked(&self.handles).take();
        drop(handles);
        self.feed.end();
    }
    /// The wrapper could not apply the last delivery to its list. The core sends the whole list again, and a failure once that did not help twice in a row.
    pub fn report_desync(&self) {
        if self.feed.resync() {
            let handles = crate::db::locked(&self.handles).take();
            drop(handles);
        }
    }
    pub fn expand_before(&self, rows: u32) {
        if let Some(window) = self.window_control() { window.grow(rows as usize, 0); }
    }
    pub fn expand_after(&self, rows: u32) {
        if let Some(window) = self.window_control() { window.grow(0, rows as usize); }
    }
}
