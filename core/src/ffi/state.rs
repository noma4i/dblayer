use std::sync::Mutex;

#[derive(uniffi::Object)]
pub struct StateSubscription {
    pub(super) task: Mutex<Option<tokio::task::AbortHandle>>,
}

#[uniffi::export]
impl StateSubscription {
    pub fn cancel(&self) {
        if let Some(task) = crate::db::locked(&self.task).take() { task.abort(); }
    }
}

impl Drop for StateSubscription {
    fn drop(&mut self) { self.cancel(); }
}
