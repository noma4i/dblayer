use std::sync::Arc;

use tokio::sync::watch;

#[derive(Clone)]
pub struct CancelToken {
    sender: Arc<watch::Sender<bool>>,
}

impl CancelToken {
    pub fn new() -> Self {
        Self { sender: Arc::new(watch::channel(false).0) }
    }

    pub fn cancel(&self) {
        crate::db::lock::effect(|| self.sender.send_replace(true));
    }

    pub fn is_cancelled(&self) -> bool {
        *self.sender.borrow()
    }

    pub async fn cancelled(&self) {
        let mut receiver = self.sender.subscribe();
        let _ = receiver.wait_for(|cancelled| *cancelled).await;
    }
}

impl Default for CancelToken {
    fn default() -> Self {
        Self::new()
    }
}
