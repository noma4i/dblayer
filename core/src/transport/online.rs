use std::sync::Arc;

use tokio::sync::watch;

#[derive(uniffi::Object)]
pub struct Connectivity {
    sender: watch::Sender<Applied>,
}

#[derive(Clone, Copy)]
struct Applied { stamp: u64, online: bool }

#[uniffi::export]
impl Connectivity {
    #[uniffi::constructor]
    pub fn new(online: bool) -> Arc<Self> {
        Arc::new(Self { sender: watch::channel(Applied { stamp: 0, online }).0 })
    }

    pub fn set_online(&self, online: bool) {
        self.change(online, crate::ffi::next_stamp());
    }
}
impl Connectivity {
    pub(crate) fn change(&self, online: bool, stamp: u64) {
        crate::lock::effect(|| {
            self.sender.send_if_modified(|applied| {
                if stamp <= applied.stamp { return false; }
                applied.stamp = stamp;
                std::mem::replace(&mut applied.online, online) != online
            })
        });
    }

    pub fn is_online(&self) -> bool {
        self.sender.borrow().online
    }

    pub(crate) async fn offline(&self) {
        let mut receiver = self.sender.subscribe();
        let _ = receiver.wait_for(|applied| !applied.online).await;
    }

    pub(crate) async fn online(&self) {
        let mut receiver = self.sender.subscribe();
        let _ = receiver.wait_for(|applied| applied.online).await;
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
