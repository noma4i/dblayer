use std::sync::Arc;
use tokio::runtime::Handle;
use crate::identity::Generation;
use crate::schema::NodeRead;
use crate::transport::CancelToken;
use super::error::SyncError;
use super::{locked, Sync};

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum EntityFetch { None, Stale, Force, IfMissing }

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RefreshError { Offline, Failed(String) }

pub type RefreshSink = Arc<dyn Fn(&str, Option<RefreshError>) + Send + std::marker::Sync>;

#[derive(Clone)]
pub(crate) struct DueRefresh {
    model: String,
    relation: String,
    id: String,
    fetch: EntityFetch,
    generation: Generation,
    cancel: CancelToken,
    sink: RefreshSink,
}

impl Sync {
    pub fn refresh_entities(&self, model: &str, ids: &[String], fetch: EntityFetch, sink: RefreshSink) -> Result<CancelToken, SyncError> {
        let cancel = CancelToken::new();
        if fetch == EntityFetch::None { return Ok(cancel); }
        let spec = self.inner.db.spec(model).map_err(|_| SyncError::UnknownModel(model.into()))?;
        let relation = spec.relations.iter().find(|declared| declared.node == Some(NodeRead::One))
            .ok_or_else(|| SyncError::UnknownRelation { model: model.into(), relation: "node".into() })?.name.clone();
        let runtime = Handle::try_current().map_err(|_| SyncError::Store("no async runtime to refresh entities".into()))?;
        let generation = self.inner.db.generation();
        for id in ids {
            let due = DueRefresh { model: model.into(), relation: relation.clone(), id: id.clone(), fetch, generation, cancel: cancel.clone(), sink: sink.clone() };
            runtime.spawn(self.inner.until_closed(self.clone().refresh_entity(due)));
        }
        Ok(cancel)
    }

    async fn refresh_entity(self, due: DueRefresh) {
        if due.cancel.is_cancelled() || due.generation != self.inner.db.generation() { return; }
        let work = async {
            if due.fetch == EntityFetch::IfMissing {
                let db = self.inner.db.clone();
                let (model, id) = (due.model.clone(), due.id.clone());
                let present = tokio::task::spawn_blocking(move || db.entity_present(&model, &id)).await
                    .map_err(|error| SyncError::Store(error.to_string()))??;
                if present { return Ok(()); }
                if due.cancel.is_cancelled() || due.generation != self.inner.db.generation() { return Err(SyncError::Cancelled); }
            }
            self.node(&due.model, &due.relation, &due.id, matches!(due.fetch, EntityFetch::Force | EntityFetch::IfMissing)).await.map(|_| ())
        };
        let result = tokio::select! { biased; _ = due.cancel.cancelled() => return, result = work => result };
        if due.cancel.is_cancelled() || due.generation != self.inner.db.generation() { return; }
        let outcome = match result {
            Ok(()) => None,
            Err(SyncError::Offline) => {
                locked(&self.inner.entity_due).push(due.clone());
                Some(RefreshError::Offline)
            }
            Err(error) => Some(RefreshError::Failed(error.to_string())),
        };
        (due.sink)(&due.id, outcome);
    }

    pub fn cancel_entity_refresh(&self, cancel: &CancelToken) {
        cancel.cancel();
        locked(&self.inner.entity_due).retain(|due| !due.cancel.is_cancelled());
    }

    pub(crate) fn refresh_due_entities(&self) {
        let waiting = std::mem::take(&mut *locked(&self.inner.entity_due));
        let Ok(runtime) = Handle::try_current() else { return };
        for due in waiting.into_iter().filter(|due| !due.cancel.is_cancelled() && due.generation == self.inner.db.generation()) {
            runtime.spawn(self.inner.until_closed(self.clone().refresh_entity(due)));
        }
    }
}
