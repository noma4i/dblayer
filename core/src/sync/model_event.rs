use std::collections::VecDeque;
use std::sync::{Arc, Weak};

use crate::db::Row;

use super::error::SyncError;
use super::{locked, Inner, Sync};

pub(crate) const EVENT_QUEUE_LIMIT: usize = 256;

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
    model: String,
    state: std::sync::Mutex<Queue>,
    ready: tokio::sync::Notify,
}

impl EventQueue {
    fn push(&self, event: &ModelEvent) {
        let mut state = locked(&self.state);
        if state.events >= EVENT_QUEUE_LIMIT {
            state.gap(1);
        } else {
            state.slots.push_back(Slot::Event(event.clone()));
            state.events += 1;
        }
        drop(state);
        self.ready.notify_one();
    }

    fn push_gap(&self) {
        locked(&self.state).gap(1);
        self.ready.notify_one();
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

    pub(crate) fn publish_model_event(&self, event: ModelEvent) {
        for queue in self.readers_of(&event.model) { queue.push(&event); }
    }

    pub(crate) fn publish_model_gap(&self, model: &str) {
        for queue in self.readers_of(model) { queue.push_gap(); }
    }

    /// A failed read is published as a gap in the queue, never as a row that was not there.
    pub(crate) fn publish_rows(&self, model: &str, area: &str, kind: &str, id: &str, before: Result<Option<Row>, crate::db::DbError>, after: Result<Option<Row>, crate::db::DbError>) -> bool {
        let (Ok(before), Ok(after)) = (before, after) else {
            self.publish_model_gap(model);
            return false;
        };
        self.publish_model_event(ModelEvent { model: model.to_string(), area: area.to_string(), kind: kind.to_string(), id: id.to_string(), before, after });
        true
    }
}

pub struct ModelEvents {
    inner: Arc<Inner>,
    queue: Arc<EventQueue>,
    key: std::sync::Mutex<Option<String>>,
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
        locked(&self.queue.state).closed = true;
        self.queue.ready.notify_one();
        if let Some(key) = locked(&self.key).take() { self.inner.release_live(&key); }
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
        if spec.changes.is_empty() { return Err(SyncError::Event(format!("{model} declares no changed_by"))); }
        let queue = Arc::new(EventQueue { model: model.to_string(), state: Default::default(), ready: tokio::sync::Notify::new() });
        locked(&self.inner.model_events).push(Arc::downgrade(&queue));
        let key = self.inner.acquire_changes().ok_or_else(|| SyncError::Event("no changes source declared".into()))?;
        let events = ModelEvents { inner: self.inner.clone(), queue, key: std::sync::Mutex::new(Some(key.clone())) };
        self.inner.changes_ready(&key).await?;
        Ok(events)
    }
}
