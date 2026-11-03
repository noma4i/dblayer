use std::collections::VecDeque;
use std::sync::{Arc, Weak};

use crate::db::Row;

use super::error::SyncError;
use super::{locked, Inner, Sync};

pub(crate) const EVENT_QUEUE_LIMIT: usize = 256;

/// A source event as it was received: the database state at that moment, its area and its kind.
#[derive(Clone, Copy)]
pub(crate) struct Heard<'a> {
    pub stamp: crate::lifecycle::Stamp,
    pub area: &'a str,
    pub kind: &'a str,
}

#[derive(Debug, Clone)]
pub struct ModelEvent {
    pub model: String,
    pub area: String,
    pub kind: String,
    pub id: String,
    pub before: Option<Row>,
    pub after: Option<Row>,
}

#[derive(Debug, Clone)]
pub enum ModelEventItem {
    Event(ModelEvent),
    Missed(u64),
}

enum Slot {
    Event(ModelEvent),
    Missed(u64),
}

#[derive(Default)]
struct Queue {
    slots: VecDeque<Slot>,
    events: usize,
    closed: bool,
}

impl Queue {
    fn gap(&mut self, count: u64) {
        match self.slots.back_mut() {
            Some(Slot::Missed(missed)) => *missed += count,
            _ => self.slots.push_back(Slot::Missed(count)),
        }
    }
}

pub(crate) struct EventQueue {
    registration: std::sync::OnceLock<crate::lifecycle::Registration>,
    lifecycle: Arc<crate::lifecycle::Lifecycle>,
    model: String,
    state: std::sync::Mutex<Queue>,
    ready: tokio::sync::Notify,
}

impl crate::lifecycle::Member for EventQueue {
    fn registration(&self) -> &std::sync::OnceLock<crate::lifecycle::Registration> { &self.registration }

    fn close(&self) {
        if let Some(registration) = self.registration.get() { registration.remove(); }
        let mut state = locked(&self.state);
        state.closed = true;
        state.slots.clear();
        state.events = 0;
        drop(state);
        crate::db::lock::effect(|| self.ready.notify_waiters());
    }

    fn lifecycle(&self, snapshot: &crate::lifecycle::Snapshot) {
        if matches!(snapshot.change, crate::lifecycle::Change::EndSession) || matches!(snapshot.change, crate::lifecycle::Change::Reset(_)) && snapshot.rereads(&self.model) {
            let mut state = locked(&self.state);
            state.slots.clear();
            state.events = 0;
        }
    }
}

#[cfg(test)]
impl EventQueue {
    pub(super) fn state_free(&self) -> bool { crate::test_wait::mutex_free(&self.state) }
    pub(super) fn ready(&self) -> &tokio::sync::Notify { &self.ready }
}

impl EventQueue {
    /// An event of an ended session never enters the queue, and one of an older generation leaves a gap, since the reader cannot know what it covered. The check shares the queue lock with the clearing that follows each transition.
    fn push(&self, event: &ModelEvent, stamp: crate::lifecycle::Stamp) {
        let mut state = locked(&self.state);
        let now = self.lifecycle.snapshot().stamp();
        if now.session_epoch != stamp.session_epoch { return; }
        if now.generation != stamp.generation || state.events >= EVENT_QUEUE_LIMIT {
            state.gap(1);
        } else {
            state.slots.push_back(Slot::Event(event.clone()));
            state.events += 1;
        }
        drop(state);
        crate::db::lock::effect(|| self.ready.notify_one());
    }

    fn push_gap(&self) {
        locked(&self.state).gap(1);
        crate::db::lock::effect(|| self.ready.notify_one());
    }
}

impl Inner {
    fn readers_of(&self, model: &str) -> Vec<Arc<EventQueue>> {
        let mut queues = locked(&self.model_events);
        queues.retain(|queue| queue.strong_count() > 0);
        queues.iter().filter_map(Weak::upgrade).filter(|queue| queue.model == model && !locked(&queue.state).closed).collect()
    }

    pub(crate) fn has_event_readers(&self, model: &str) -> bool {
        !self.readers_of(model).is_empty()
    }

    /// `stamp` is the database state at the moment the source event was received, never the one at publication.
    pub(crate) fn publish_model_event(&self, stamp: crate::lifecycle::Stamp, event: ModelEvent) {
        for queue in self.readers_of(&event.model) { queue.push(&event, stamp); }
    }

    pub(crate) fn publish_model_gap(&self, model: &str) {
        for queue in self.readers_of(model) { queue.push_gap(); }
    }

    /// A failed read is published as a gap in the queue, never as a row that was not there.
    pub(crate) fn publish_rows(&self, heard: Heard<'_>, model: &str, id: &str, before: Result<Option<Row>, crate::db::DbError>, after: Result<Option<Row>, crate::db::DbError>) -> bool {
        let (Ok(before), Ok(after)) = (before, after) else {
            self.publish_model_gap(model);
            return false;
        };
        self.publish_model_event(heard.stamp, ModelEvent { model: model.to_string(), area: heard.area.to_string(), kind: heard.kind.to_string(), id: id.to_string(), before, after });
        true
    }
}

pub struct ModelEvents {
    inner: Arc<Inner>,
    queue: Arc<EventQueue>,
    keys: std::sync::Mutex<Vec<String>>,
}

#[cfg(test)]
impl ModelEvents {
    pub(super) fn queue(&self) -> Arc<EventQueue> { self.queue.clone() }
}

impl ModelEvents {
    pub async fn next(&self) -> Option<ModelEventItem> {
        loop {
            let notified = self.queue.ready.notified();
            {
                let mut state = locked(&self.queue.state);
                match state.slots.pop_front() {
                    Some(Slot::Event(event)) => {
                        state.events -= 1;
                        return Some(ModelEventItem::Event(event));
                    }
                    Some(Slot::Missed(count)) => return Some(ModelEventItem::Missed(count)),
                    None => {}
                }
                if state.closed { return None; }
            }
            notified.await;
        }
    }

    pub fn close(&self) {
        crate::lifecycle::Member::close(&*self.queue);
        let keys = std::mem::take(&mut *locked(&self.keys));
        for key in keys { self.inner.release_live(&key); }
    }
}

impl Drop for ModelEvents {
    fn drop(&mut self) {
        self.close();
    }
}

impl Sync {
    pub async fn open_model_events(&self, model: &str) -> Result<ModelEvents, SyncError> {
        let spec = self.inner.db.spec(model)?;
        if spec.changes.is_empty() && spec.listens.is_empty() { return Err(SyncError::Event(format!("{model} declares no event source"))); }
        let queue = Arc::new(EventQueue { registration: Default::default(), lifecycle: self.inner.db.shared.store.lifecycle.clone(), model: model.to_string(), state: Default::default(), ready: tokio::sync::Notify::new() });
        self.inner.db.shared.store.lifecycle.enroll(&queue);
        locked(&self.inner.model_events).push(Arc::downgrade(&queue));
        let mut keys = Vec::new();
        if !spec.changes.is_empty() { keys.push(self.inner.acquire_changes().ok_or_else(|| SyncError::Event("no changes source declared".into()))?); }
        for updates in &spec.listens { keys.push(self.inner.acquire_one(model, updates.clone(), None)); }
        let events = ModelEvents { inner: self.inner.clone(), queue, keys: std::sync::Mutex::new(keys.clone()) };
        for key in keys { self.inner.changes_ready(&key).await?; }
        Ok(events)
    }
}
