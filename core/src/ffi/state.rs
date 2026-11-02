use std::sync::{Arc, Mutex};

#[derive(uniffi::Object)]
pub struct StateSubscription {
    pub(super) task: Mutex<Option<tokio::task::AbortHandle>>,
    pub(crate) calls: Arc<crate::db::CallState>,
}

impl StateSubscription {
    pub(super) fn follow<T: Clone + Send + Sync + 'static>(mut changes: tokio::sync::watch::Receiver<T>, deliver: impl Fn(T) + Send + 'static) -> Result<Arc<Self>, super::DbError> {
        let calls = Arc::new(crate::db::CallState::new());
        let task = super::database::executor()?.spawn({
            let calls = calls.clone();
            async move {
                let first = changes.borrow_and_update().clone();
                calls.call(|| deliver(first));
                while changes.changed().await.is_ok() {
                    let state = changes.borrow_and_update().clone();
                    calls.call(|| deliver(state));
                }
            }
        });
        Ok(Arc::new(Self { task: Mutex::new(Some(task.abort_handle())), calls }))
    }
}

#[uniffi::export]
impl StateSubscription {
    pub fn cancel(&self) {
        self.calls.cancel();
        let task = crate::db::locked(&self.task).take();
        if let Some(task) = task { task.abort(); }
    }
}

impl Drop for StateSubscription {
    fn drop(&mut self) { self.cancel(); }
}
