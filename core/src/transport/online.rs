use std::sync::Arc;

use tokio::sync::watch;

#[derive(Clone)]
pub struct Connectivity {
    sender: Arc<watch::Sender<bool>>,
}

impl Connectivity {
    pub fn new() -> Self {
        Self { sender: Arc::new(watch::channel(true).0) }
    }

    pub fn set(&self, online: bool) {
        self.sender.send_replace(online);
    }

    pub fn is_online(&self) -> bool {
        *self.sender.borrow()
    }

    pub async fn offline(&self) {
        let mut receiver = self.sender.subscribe();
        let _ = receiver.wait_for(|online| !*online).await;
    }

    pub async fn online(&self) {
        let mut receiver = self.sender.subscribe();
        let _ = receiver.wait_for(|online| *online).await;
    }
}

impl Default for Connectivity {
    fn default() -> Self {
        Self::new()
    }
}
