use std::sync::Arc;

use tokio::sync::watch;

#[derive(uniffi::Object)]
pub struct Connectivity {
    sender: watch::Sender<bool>,
    /// Counts the returns of the network, so a reader that polls late still sees a short loss.
    returns: watch::Sender<u64>,
}

#[uniffi::export]
impl Connectivity {
    #[uniffi::constructor]
    pub fn new(online: bool) -> Arc<Self> {
        Arc::new(Self { sender: watch::channel(online).0, returns: watch::channel(0).0 })
    }

    pub fn set_online(&self, online: bool) {
        self.change(online);
    }
}
impl Connectivity {
    pub(crate) fn change(&self, online: bool) -> bool {
        crate::db::lock::effect(|| {
            let was = self.sender.send_replace(online);
            if online && !was { self.returns.send_modify(|count| *count += 1); }
            was
        })
    }

    pub(crate) fn returns(&self) -> watch::Receiver<u64> { self.returns.subscribe() }

    pub fn is_online(&self) -> bool {
        *self.sender.borrow()
    }

    pub(crate) async fn offline(&self) {
        let mut receiver = self.sender.subscribe();
        let _ = receiver.wait_for(|online| !*online).await;
    }

    pub(crate) async fn online(&self) {
        let mut receiver = self.sender.subscribe();
        let _ = receiver.wait_for(|online| *online).await;
    }

    pub(crate) async fn wait(&self, clock: &dyn crate::pacing::Clock, delay: std::time::Duration) {
        let deadline = clock.now().saturating_add(delay);
        loop {
            self.online().await;
            tokio::select! {
                () = clock.sleep(deadline.saturating_sub(clock.now())) => return,
                () = self.offline() => {}
            }
        }
    }
}
