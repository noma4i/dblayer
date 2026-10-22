use std::collections::VecDeque;
use std::sync::{Arc, Mutex, Weak};
use super::{Database, DbError, LiveChange, LiveSubscription, QueryListener};

const CHANGE_QUEUE_LIMIT: usize = 64;

#[derive(Default)]
struct State {
    pending: VecDeque<LiveChange>,
    rows: Vec<super::WireRow>,
    closed: bool,
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
    state: Mutex<State>,
    ready: tokio::sync::Notify,
    subscription: Mutex<Option<Arc<LiveSubscription>>>,
}

struct Listener(Weak<LiveChanges>);
impl QueryListener for Listener {
    fn changed(&self, change: LiveChange) {
        if let Some(queue) = self.0.upgrade() { queue.push(change); }
    }
}

impl LiveChanges {
    fn new() -> Arc<Self> {
        Arc::new(Self { state: Mutex::new(State::default()), ready: tokio::sync::Notify::new(), subscription: Mutex::new(None) })
    }

    fn push(&self, change: LiveChange) {
        let mut state = crate::db::locked(&self.state);
        if state.closed { return; }
        state.apply(&change);
        if state.pending.len() >= CHANGE_QUEUE_LIMIT {
            let mut replacement = LiveChange::whole(change.revision, state.rows.clone());
            replacement.error = change.error.clone();
            replacement.window = change.window.clone();
            state.pending.clear();
            state.pending.push_back(replacement);
        } else {
            state.pending.push_back(change);
        }
        self.ready.notify_one();
    }
}

#[uniffi::export]
impl Database {
    pub fn changes(&self, query_json: String) -> Result<Arc<LiveChanges>, DbError> {
        let queue = LiveChanges::new();
        let subscription = self.live_query(query_json, Arc::new(Listener(Arc::downgrade(&queue))))?;
        *crate::db::locked(&queue.subscription) = Some(subscription);
        Ok(queue)
    }
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
        if let Some(subscription) = crate::db::locked(&self.subscription).take() { subscription.cancel(); }
        let mut state = crate::db::locked(&self.state);
        state.closed = true;
        state.pending.clear();
        state.rows.clear();
        self.ready.notify_one();
    }
}

#[cfg(test)]
#[path = "changes_tests.rs"]
mod tests;
