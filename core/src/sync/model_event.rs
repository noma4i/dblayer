use std::sync::{Arc, Weak};

use crate::db::Row;

use super::error::SyncError;
use super::{locked, Inner, Sync};

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

/// A reader of the events of one model. The source hands it each event with the database state at the moment the event was received, and each event it could not read.
pub(crate) trait EventReader: Send + std::marker::Sync {
    fn model(&self) -> &str;
    fn heard(&self, event: &ModelEvent, stamp: crate::lifecycle::Stamp);
    fn unread(&self, count: u64);
}

static READERS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

impl Inner {
    fn readers_of(&self, model: &str) -> Vec<Arc<dyn EventReader>> {
        let mut readers = locked(&self.model_events);
        readers.retain(|(_, reader)| reader.strong_count() > 0);
        readers.iter().filter_map(|(_, reader)| reader.upgrade()).filter(|reader| reader.model() == model).collect()
    }

    pub(crate) fn has_event_readers(&self, model: &str) -> bool {
        !self.readers_of(model).is_empty()
    }

    /// `stamp` is the database state at the moment the source event was received, never the one at publication.
    pub(crate) fn publish_model_event(&self, stamp: crate::lifecycle::Stamp, event: ModelEvent) {
        for reader in self.readers_of(&event.model) { reader.heard(&event, stamp); }
    }

    pub(crate) fn publish_model_gap(&self, model: &str) { self.publish_model_loss(model, 1); }

    pub(crate) fn publish_model_loss(&self, model: &str, count: u64) {
        for reader in self.readers_of(model) { reader.unread(count); }
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

/// The sources a reader of model events holds. Dropping it stops the reader hearing events and releases the sources.
pub struct ModelEvents {
    inner: Arc<Inner>,
    reader: u64,
    keys: std::sync::Mutex<Vec<String>>,
}

impl ModelEvents {
    pub fn close(&self) {
        locked(&self.inner.model_events).retain(|(id, _)| *id != self.reader);
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
    /// Registers the reader, then opens the sources of the events of the model and returns once they are ready or one failed.
    pub(crate) async fn open_model_events(&self, model: &str, reader: Weak<dyn EventReader>) -> Result<ModelEvents, SyncError> {
        let spec = self.inner.db.spec(model)?;
        if spec.changes.is_empty() && spec.listens.is_empty() { return Err(SyncError::Event(format!("{model} declares no event source"))); }
        let id = READERS.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        locked(&self.inner.model_events).push((id, reader));
        let events = ModelEvents { inner: self.inner.clone(), reader: id, keys: std::sync::Mutex::new(Vec::new()) };
        let mut keys = Vec::new();
        if !spec.changes.is_empty() { keys.push(self.inner.acquire_changes().ok_or_else(|| SyncError::Event("no changes source declared".into()))?); }
        for updates in &spec.listens { keys.push(self.inner.acquire_one(model, updates.clone(), None)); }
        locked(&events.keys).clone_from(&keys);
        for key in keys { self.inner.changes_ready(&key).await?; }
        Ok(events)
    }
}
