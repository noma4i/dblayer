use std::sync::Arc;

use tokio::sync::watch;

#[derive(uniffi::Object)]
pub struct Connectivity {
    sender: watch::Sender<bool>,
}

#[uniffi::export]
impl Connectivity {
    #[uniffi::constructor]
    pub fn new(online: bool) -> Arc<Self> {
        Arc::new(Self { sender: watch::channel(online).0 })
    }

    pub fn set_online(&self, online: bool) {
        self.change(online);
    }
}
impl Connectivity {
    pub(crate) fn change(&self, online: bool) -> bool {
        crate::lock::effect(|| {
            self.sender.send_replace(online)
        })
    }

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

    pub(crate) async fn wait(&self, clock: &dyn crate::runtime::Clock, delay: std::time::Duration) {
        let deadline = clock.now().saturating_add(delay);
        clock.armed(1);
        loop {
            self.online().await;
            tokio::select! {
                () = clock.sleep_until(deadline) => return,
                () = self.offline() => {}
            }
        }
    }
}
