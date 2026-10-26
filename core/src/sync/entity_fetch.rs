use std::sync::Arc;

use tokio::runtime::Handle;

use crate::schema::NodeRead;

use super::error::SyncError;
use super::{locked, Sync};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RefreshError {
    Offline,
    Failed(String),
}

pub type RefreshSink = Arc<dyn Fn(&str, Option<RefreshError>) + Send + std::marker::Sync>;

#[derive(Clone)]
pub(crate) struct DueRefresh {
    model: String,
    relation: String,
    id: String,
    force: bool,
    sink: RefreshSink,
}

impl Sync {
    pub fn refresh_entities(&self, model: &str, ids: &[String], force: bool, sink: RefreshSink) -> Result<(), SyncError> {
        let spec = self.inner.db.spec(model).map_err(|_| SyncError::UnknownModel(model.to_string()))?;
        let relation = spec
            .relations
            .iter()
            .find(|declared| declared.node == Some(NodeRead::One))
            .ok_or_else(|| SyncError::UnknownRelation { model: model.to_string(), relation: "node".to_string() })?
            .name
            .clone();
        let runtime = Handle::try_current().map_err(|_| SyncError::Store("no async runtime to refresh entities".into()))?;
        for id in ids {
            let due = DueRefresh { model: model.to_string(), relation: relation.clone(), id: id.clone(), force, sink: sink.clone() };
            runtime.spawn(self.inner.until_closed(self.clone().refresh_entity(due)));
        }
        Ok(())
    }

    async fn refresh_entity(self, due: DueRefresh) {
        let outcome = match self.node(&due.model, &due.relation, &due.id, due.force).await {
            Ok(_) => None,
            Err(SyncError::Offline) => {
                let mut waiting = locked(&self.inner.entity_due);
                if !waiting.iter().any(|held| held.model == due.model && held.id == due.id) {
                    waiting.push(due.clone());
                }
                Some(RefreshError::Offline)
            }
            Err(error) => Some(RefreshError::Failed(error.to_string())),
        };
        (due.sink)(&due.id, outcome);
    }

    pub(crate) fn refresh_due_entities(&self) {
        let waiting = std::mem::take(&mut *locked(&self.inner.entity_due));
        let Ok(runtime) = Handle::try_current() else { return };
        for due in waiting {
            runtime.spawn(self.inner.until_closed(self.clone().refresh_entity(due)));
        }
    }
}
