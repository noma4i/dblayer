use std::collections::VecDeque;
use std::sync::{Arc, Mutex, Weak};
use super::{Database, DbError, LiveChange, LiveSubscription, QueryListener};

const CHANGE_QUEUE_LIMIT: usize = 64;

#[derive(Default)]
struct State {
    pending: VecDeque<LiveChange>,
    rows: Vec<super::WireRow>,
    closed: bool,
    min_revision: i64,
}

impl State {
    fn apply(&mut self, change: &LiveChange) {
        if change.full { self.rows.clone_from(&change.rows); return; }
        for replacement in &change.replaced {
            if let Some(row) = self.rows.iter_mut().find(|row| row.id == replacement.from) { row.id.clone_from(&replacement.to); }
        }
        self.rows.retain(|row| !change.removed.contains(&row.id));
        let mut placed = change.placed.clone();
        placed.sort_by_key(|placement| placement.index);
        let mut values = std::collections::HashMap::new();
        self.rows.retain(|row| {
            if placed.iter().any(|placement| placement.id == row.id) { values.insert(row.id.clone(), row.clone()); false } else { true }
        });
        values.extend(change.rows.iter().map(|row| (row.id.clone(), row.clone())));
        for placement in placed {
            if let Some(row) = values.remove(&placement.id) { self.rows.insert(placement.index as usize, row); }
        }
        for row in &change.rows {
            if let Some(current) = self.rows.iter_mut().find(|current| current.id == row.id) { current.clone_from(row); }
        }
    }
}

#[derive(uniffi::Object)]
pub struct LiveChanges {
    registration: std::sync::OnceLock<crate::lifecycle::Registration>,
    state: Mutex<State>,
    ready: tokio::sync::Notify,
    subscription: Mutex<Option<Arc<LiveSubscription>>>,
    context: std::sync::OnceLock<(crate::db::Db, String)>,
}

struct Listener(Weak<LiveChanges>);
impl QueryListener for Listener {
    fn changed(&self, change: LiveChange) {
        if let Some(queue) = self.0.upgrade() { queue.push(change); }
    }
}

impl LiveChanges {
    fn new() -> Arc<Self> {
        Arc::new(Self { state: Mutex::new(State::default()), ready: tokio::sync::Notify::new(), subscription: Mutex::new(None), registration: Default::default(), context: Default::default() })
    }

    fn with_state(&self, change: impl FnOnce(&mut State)) {
        change(&mut crate::db::locked(&self.state));
        crate::db::lock::effect(|| self.ready.notify_one());
    }

    fn push(&self, change: LiveChange) {
        self.with_state(|state| {
            if state.closed || change.revision < state.min_revision { return; }
            state.apply(&change);
            if state.pending.len() >= CHANGE_QUEUE_LIMIT {
                let mut replacement = LiveChange::whole(change.revision, state.rows.clone());
                replacement.error = change.error.clone();
                replacement.window = change.window.clone();
                replacement.carry_state(&change);
                state.pending.clear();
                state.pending.push_back(replacement);
            } else {
                state.pending.push_back(change);
            }
        });
    }
}

#[uniffi::export]
impl Database {
    pub fn changes(&self, query_json: String) -> Result<Arc<LiveChanges>, DbError> {
        let queue = LiveChanges::new();
        let model = serde_json::from_str::<serde_json::Value>(&query_json)?["model"].as_str().unwrap_or_default().to_string();
        let _ = queue.context.set((self.db.clone(), model));
        self.streams.enroll(&queue);
        let subscription = self.live_query(query_json, Arc::new(Listener(Arc::downgrade(&queue))));
        *crate::db::locked(&queue.subscription) = Some(subscription);
        Ok(queue)
    }
}

impl crate::lifecycle::Member for LiveChanges {
    fn close(&self) { self.cancel(); }
    fn registration(&self) -> &std::sync::OnceLock<crate::lifecycle::Registration> { &self.registration }
    fn lifecycle(&self, snapshot: &crate::lifecycle::Snapshot) {
        let Some((db, model)) = self.context.get() else { return };
        let ended = matches!(snapshot.change, crate::lifecycle::Change::EndSession);
        if !ended && !(matches!(snapshot.change, crate::lifecycle::Change::Reset(_)) && snapshot.rereads(model)) { return; }
        let Ok(revision) = db.revision_seq() else { return };
        self.with_state(|state| {
            state.pending.clear();
            state.rows.clear();
            state.min_revision = if ended { revision + 1 } else { revision };
            state.pending.push_back(LiveChange::whole(revision, Vec::new()));
        });
    }
}

#[cfg(test)]
impl LiveChanges {
    pub(super) fn queued(&self) -> usize { crate::db::locked(&self.state).pending.len() }
}

#[uniffi::export]
impl LiveChanges {
    pub async fn next(&self) -> Option<LiveChange> {
        loop {
            let notified = self.ready.notified();
            {
                let mut state = crate::db::locked(&self.state);
                if let Some(change) = state.pending.pop_front() { return Some(change); }
                if state.closed { return None; }
            }
            notified.await;
        }
    }

    pub fn cancel(&self) {
        if let Some(registration) = self.registration.get() { registration.remove(); }
        let subscription = crate::db::locked(&self.subscription).take();
        if let Some(subscription) = subscription { subscription.cancel(); }
        self.with_state(|state| {
            state.closed = true;
            state.pending.clear();
            state.rows.clear();
        });
        crate::db::lock::effect(|| self.ready.notify_waiters());
    }
}

#[cfg(test)]
#[path = "changes_tests.rs"]
mod tests;
