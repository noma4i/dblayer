use std::sync::Arc;

use crate::cloud::{Cloud, CloudError, CloudIo, CloudState, MergeSource};

use super::database::{cancellable, executor};
use super::{Database, DbError, StateSubscription};

#[uniffi::export(with_foreign)]
pub trait CloudStateListener: Send + Sync {
    fn changed(&self, state: CloudState);
}

#[derive(uniffi::Object)]
pub struct CloudLink {
    pub(super) cloud: Cloud,
    registration: std::sync::OnceLock<super::open_streams::Registration>,
}

#[uniffi::export]
impl Database {
    pub fn connect_cloud(&self, io: Arc<dyn CloudIo>) -> Result<Arc<CloudLink>, DbError> {
        let _entered = executor()?.enter();
        let cloud = Cloud::attach(&self.db, io, self.clock.clone());
        cloud.start();
        let link = Arc::new(CloudLink { cloud, registration: Default::default() });
        self.streams.enroll(&link);
        Ok(link)
    }
}

impl super::open_streams::Closable for CloudLink {
    fn close(&self) { self.cloud.stop(); }
    fn registration(&self) -> &std::sync::OnceLock<super::open_streams::Registration> { &self.registration }
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
        StateSubscription::follow(self.cloud.state_changes(), move |state| {
            crate::db::lock::assert_no_library_lock("CloudStateListener::changed");
            listener.changed(state)
        })
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
