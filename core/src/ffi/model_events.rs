use std::sync::Arc;

use super::database::executor;
use super::wire::{WireModel, WireRow};
use super::{Database, DbError};

#[derive(Clone, Debug, uniffi::Record)]
pub struct WireModelEvent {
    pub area: String,
    pub kind: String,
    pub id: String,
    pub before: Option<WireRow>,
    pub after: Option<WireRow>,
}

#[derive(Clone, Debug, uniffi::Enum)]
pub enum WireModelEventItem {
    Event { event: Box<WireModelEvent> },
    Missed { count: u64 },
}

#[derive(uniffi::Object)]
pub struct ModelEventFeed {
    registration: std::sync::OnceLock<super::open_streams::Registration>,
    events: Arc<crate::sync::ModelEvents>,
    wire: Arc<WireModel>,
}

#[uniffi::export]
impl Database {
    pub async fn open_model_events(&self, model: String) -> Result<Arc<ModelEventFeed>, DbError> {
        let wire = self.wire(&model)?.clone();
        let sync = self.sync()?.clone();
        let events = super::database::cancellable(async move { sync.open_model_events(&model).await }).await??;
        let feed = Arc::new(ModelEventFeed { events: Arc::new(events), wire, registration: Default::default() });
        self.streams.enroll(&feed);
        Ok(feed)
    }
}

impl super::open_streams::Closable for ModelEventFeed {
    fn close(&self) { self.events.close(); }
    fn registration(&self) -> &std::sync::OnceLock<super::open_streams::Registration> { &self.registration }
}

async fn joined<T>(task: tokio::task::JoinHandle<Option<T>>) -> Result<Option<T>, DbError> {
    task.await.map_err(|error| DbError::Invalid { detail: error.to_string() })
}

#[uniffi::export]
impl ModelEventFeed {
    pub async fn next(self: Arc<Self>) -> Result<Option<WireModelEventItem>, DbError> {
        let feed = self.clone();
        let task = executor()?.spawn(async move {
            feed.events.next().await.map(|item| match item {
                crate::sync::ModelEventItem::Missed(count) => WireModelEventItem::Missed { count },
                crate::sync::ModelEventItem::Event(event) => WireModelEventItem::Event { event: Box::new(WireModelEvent { area: event.area, kind: event.kind, id: event.id, before: event.before.as_ref().map(|row| feed.wire.row(row)), after: event.after.as_ref().map(|row| feed.wire.row(row)) }) },
            })
        });
        joined(task).await
    }

    pub fn cancel(&self) {
        self.events.close();
    }
}

#[cfg(test)]
#[path = "model_events_tests.rs"]
mod tests;
