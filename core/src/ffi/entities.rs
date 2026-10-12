use std::sync::Arc;

use crate::db::{EntityChange, EntityObserver};
use crate::identity::RowId;

use super::{row_json, DbError};

#[derive(Clone, Debug, uniffi::Record)]
pub struct EntityDelivery {
    pub revision: i64,
    pub rows: Vec<String>,
    pub removed: Vec<String>,
}

impl EntityDelivery {
    pub(super) fn of(change: &EntityChange) -> Self {
        Self { revision: change.revision, rows: change.rows.iter().map(row_json).collect(), removed: change.removed.iter().map(ToString::to_string).collect() }
    }
}

#[uniffi::export(rust, foreign)]
pub trait EntityListener: Send + Sync { fn changed(&self, delivery: EntityDelivery); }

#[derive(uniffi::Object)]
pub struct LiveEntities {
    pub(super) observer: EntityObserver,
}

fn row_ids(ids: &[String]) -> Result<Vec<RowId>, DbError> {
    ids.iter().map(|id| RowId::new(id).map_err(|error| DbError::Invalid { detail: error.to_string() })).collect()
}

#[uniffi::export]
impl LiveEntities {
    pub fn retain(&self, ids: Vec<String>) -> Result<(), DbError> { Ok(self.observer.retain(&row_ids(&ids)?)?) }
    pub fn release(&self, ids: Vec<String>) -> Result<(), DbError> { self.observer.release(&row_ids(&ids)?); Ok(()) }
}

pub(super) fn open(db: &crate::db::Db, model: &str, listener: Arc<dyn EntityListener>) -> Result<Arc<LiveEntities>, DbError> {
    let observer = db.observe_entities(model, move |change| listener.changed(EntityDelivery::of(change)))?;
    Ok(Arc::new(LiveEntities { observer }))
}
