use std::sync::Arc;
use tokio::runtime::Handle;
use crate::identity::Generation;
use crate::schema::NodeRead;
use crate::transport::CancelToken;
use super::error::SyncError;
use super::{locked, Inner, Sync};

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
    session: bool,
}

impl Sync {
    pub fn refresh_entities(&self, model: &str, ids: &[String], fetch: EntityFetch, sink: RefreshSink) -> Result<CancelToken, SyncError> {
        let cancel = CancelToken::new();
        if fetch == EntityFetch::None { return Ok(cancel); }
        if !self.inner.admitted(crate::lifecycle::Origin::Session) {
            for id in ids { sink(id, None); }
            return Ok(cancel);
        }
        let spec = self.inner.db.spec(model).map_err(|_| SyncError::UnknownModel(model.into()))?;
        let relation = spec.relations.iter().find(|declared| declared.node == Some(NodeRead::One))
            .ok_or_else(|| SyncError::UnknownRelation { model: model.into(), relation: "node".into() })?.name.clone();
        let runtime = Handle::try_current().map_err(|_| SyncError::Store("no async runtime to refresh entities".into()))?;
        let generation = self.inner.db.generation();
        for id in ids {
            let due = DueRefresh { model: model.into(), relation: relation.clone(), id: id.clone(), fetch, generation, cancel: cancel.clone(), sink: sink.clone(), session: false };
            runtime.spawn(self.inner.until_closed(Self::refresh_entity(self.inner.clone(), due)));
        }
        Ok(cancel)
    }

    async fn refresh_entity(inner: Arc<Inner>, mut due: DueRefresh) {
        if due.cancel.is_cancelled() || due.generation != inner.db.generation() { return; }
        let work = async {
            if due.fetch == EntityFetch::IfMissing {
                let db = inner.db.clone();
                let (model, id) = (due.model.clone(), due.id.clone());
                let present = tokio::task::spawn_blocking(move || db.entity_present(&model, &id)).await
                    .map_err(|error| SyncError::Store(error.to_string()))??;
                if present { return Ok(()); }
                if due.cancel.is_cancelled() || due.generation != inner.db.generation() { return Err(SyncError::Cancelled); }
            }
            inner.node(crate::lifecycle::Origin::Session, &due.model, &due.relation, &due.id, matches!(due.fetch, EntityFetch::Force | EntityFetch::IfMissing)).await.map(|_| ())
        };
        let result = tokio::select! { biased; _ = due.cancel.cancelled() => return, result = work => result };
        if due.cancel.is_cancelled() || due.generation != inner.db.generation() { return; }
        let outcome = match result {
            Ok(()) => None,
            Err(error @ (SyncError::Offline | SyncError::Transport(crate::transport::TransportError::Session { .. }))) => {
                due.session = error != SyncError::Offline;
                locked(&inner.entity_due).push(due.clone());
                Some(if error == SyncError::Offline { RefreshError::Offline } else { RefreshError::Failed(error.to_string()) })
            }
            Err(error) => Some(RefreshError::Failed(error.to_string())),
        };
        (due.sink)(&due.id, outcome);
    }

    pub async fn load_entity(&self, model: &str, id: &str) -> Result<(), SyncError> {
        let spec = self.inner.db.spec(model).map_err(|_| SyncError::UnknownModel(model.into()))?;
        let relation = spec.relations.iter().find(|declared| declared.node == Some(NodeRead::One))
            .ok_or_else(|| SyncError::UnknownRelation { model: model.into(), relation: "node".into() })?.name.clone();
        self.inner.node(crate::lifecycle::Origin::Explicit, model, &relation, id, true).await.map(|_| ())
    }

    pub fn cancel_entity_refresh(&self, cancel: &CancelToken) {
        cancel.cancel();
        let cancelled = {
            let mut waiting = locked(&self.inner.entity_due);
            let (cancelled, kept) = std::mem::take(&mut *waiting).into_iter().partition(|due| due.cancel.is_cancelled());
            *waiting = kept;
            cancelled
        };
        drop::<Vec<DueRefresh>>(cancelled);
    }

    pub(crate) fn refresh_due_entities(&self, session: bool) {
        self.inner.refresh_due_entities(session);
    }
}

impl Inner {
    pub(crate) fn drop_due_refreshes(&self, models: Option<&[String]>) {
        let dropped: Vec<DueRefresh> = {
            let mut due = locked(&self.entity_due);
            let (dropped, kept) = std::mem::take(&mut *due).into_iter().partition(|due| models.is_none_or(|models| models.contains(&due.model)));
            *due = kept;
            dropped
        };
        drop(dropped);
    }

    pub(crate) fn refresh_due_entities(self: &Arc<Self>, session: bool) {
        let waiting: Vec<DueRefresh> = {
            let mut due = locked(&self.entity_due);
            let (waiting, kept) = std::mem::take(&mut *due).into_iter().partition(|due| due.session == session);
            *due = kept;
            waiting
        };
        let Ok(runtime) = Handle::try_current() else { return };
        for due in waiting.into_iter().filter(|due| !due.cancel.is_cancelled() && due.generation == self.db.generation()) {
            runtime.spawn(self.until_closed(Sync::refresh_entity(self.clone(), due)));
        }
    }
}
