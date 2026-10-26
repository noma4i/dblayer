use std::sync::Arc;

use crate::db::{EntityChange, EntityObserver};
use crate::sync::RefreshError;
use crate::identity::RowId;

use super::wire::{WireModel, WireRow};
use super::DbError;

#[derive(Clone, Debug, uniffi::Record)]
pub struct EntityDelivery {
    pub revision: i64,
    pub rows: Vec<WireRow>,
    pub removed: Vec<String>,
    pub failure: Option<String>,
    pub refreshes: Vec<EntityRefresh>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum EntityFetch {
    None,
    Stale,
    Force,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum EntityRefreshError {
    Offline,
    Failed { message: String },
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct EntityRefresh {
    pub id: String,
    pub error: Option<EntityRefreshError>,
}

impl EntityDelivery {
    pub(super) fn of(change: &EntityChange, wire: &WireModel) -> Self {
        Self { revision: change.revision, rows: change.rows.iter().map(|row| wire.row(row)).collect(), removed: change.removed.iter().map(ToString::to_string).collect(), failure: change.failure.clone(), refreshes: Vec::new() }
    }
}

#[uniffi::export(rust, foreign)]
pub trait EntityListener: Send + Sync { fn changed(&self, delivery: EntityDelivery); }

#[derive(uniffi::Object)]
pub struct LiveEntities {
    pub(super) observer: EntityObserver,
    model: String,
    sync: Option<crate::sync::Sync>,
    listener: Arc<dyn EntityListener>,
}

fn row_ids(ids: &[String]) -> Result<Vec<RowId>, DbError> {
    ids.iter().map(|id| RowId::new(id).map_err(|error| DbError::Invalid { detail: error.to_string() })).collect()
}

#[uniffi::export]
impl LiveEntities {
    pub fn retain(&self, ids: Vec<String>, fetch: EntityFetch) -> Result<(), DbError> {
        self.observer.retain(&row_ids(&ids)?)?;
        if fetch == EntityFetch::None { return Ok(()); }
        let sync = self.sync.as_ref().ok_or_else(|| DbError::Invalid { detail: "entity fetch needs a network session".into() })?;
        let listener = self.listener.clone();
        let sink: crate::sync::RefreshSink = Arc::new(move |id, error| {
            let error = error.map(|error| match error {
                RefreshError::Offline => EntityRefreshError::Offline,
                RefreshError::Failed(message) => EntityRefreshError::Failed { message },
            });
            listener.changed(EntityDelivery { revision: i64::MIN, rows: Vec::new(), removed: Vec::new(), failure: None, refreshes: vec![EntityRefresh { id: id.to_string(), error }] });
        });
        let _entered = super::database::executor()?.enter();
        Ok(sync.refresh_entities(&self.model, &ids, fetch == EntityFetch::Force, sink)?)
    }
    pub fn release(&self, ids: Vec<String>) -> Result<(), DbError> { self.observer.release(&row_ids(&ids)?); Ok(()) }
}

pub(super) fn open(db: &crate::db::Db, sync: Option<crate::sync::Sync>, model: &str, wire: Arc<WireModel>, listener: Arc<dyn EntityListener>) -> Result<Arc<LiveEntities>, DbError> {
    let told = listener.clone();
    let observer = db.observe_entities(model, move |change| told.changed(EntityDelivery::of(change, &wire)))?;
    Ok(Arc::new(LiveEntities { observer, model: model.to_string(), sync, listener }))
}
