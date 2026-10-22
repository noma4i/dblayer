use std::sync::{Arc, Mutex};

use crate::cloud::{Cloud, CloudError, CloudIo, CloudState, MergeSource};

use super::database::{cancellable, executor};
use super::{Database, DbError, StateSubscription};

#[uniffi::export(with_foreign)]
pub trait CloudStateListener: Send + Sync {
    fn changed(&self, state: CloudState);
}

#[derive(uniffi::Object)]
pub struct CloudLink {
    cloud: Cloud,
}

#[uniffi::export]
impl Database {
    pub fn connect_cloud(&self, io: Arc<dyn CloudIo>) -> Result<Arc<CloudLink>, DbError> {
        let _entered = executor()?.enter();
        let cloud = Cloud::attach(&self.db, io, self.clock.clone(), self.online.clone());
        cloud.start();
        Ok(Arc::new(CloudLink { cloud }))
    }
}

#[uniffi::export]
impl CloudLink {
    pub fn remote_changed(&self) {
        self.cloud.remote_changed();
    }

    pub fn state(&self) -> CloudState {
        self.cloud.state()
    }

    pub fn listen(&self, listener: Arc<dyn CloudStateListener>) -> Result<Arc<StateSubscription>, DbError> {
        let mut changes = self.cloud.state_changes();
        let task = executor()?.spawn(async move {
            listener.changed(changes.borrow_and_update().clone());
            while changes.changed().await.is_ok() {
                let state = changes.borrow_and_update().clone();
                listener.changed(state);
            }
        });
        Ok(Arc::new(StateSubscription { task: Mutex::new(Some(task.abort_handle())) }))
    }

    pub async fn sync_now(&self) -> Result<(), CloudError> {
        let cloud = self.cloud.clone();
        cancellable(async move { cloud.sync_now().await }).await.map_err(|error| CloudError::Rejected { detail: error.to_string() })?
    }

    pub async fn resolve_initial_merge(&self, source: MergeSource) -> Result<(), CloudError> {
        let cloud = self.cloud.clone();
        cancellable(async move { cloud.resolve_initial_merge(source).await }).await.map_err(|error| CloudError::Rejected { detail: error.to_string() })?
    }

    pub fn stop(&self) {
        self.cloud.stop();
    }
}
