use std::sync::Arc;

use tokio::sync::watch;

use super::reading::Reader;
use super::{OnlineSink, PlatformObserver};

pub(crate) fn network_value() -> &'static watch::Sender<Option<bool>> {
    static VALUE: std::sync::OnceLock<watch::Sender<Option<bool>>> = std::sync::OnceLock::new();
    VALUE.get_or_init(|| watch::channel(None).0)
}

pub(crate) fn record_network(online: bool) {
    crate::lock::effect(|| network_value().send_if_modified(|value| std::mem::replace(value, Some(online)) != Some(online)));
}

#[derive(uniffi::Object)]
pub struct NetworkStatus;

#[uniffi::export]
impl NetworkStatus {
    /// The first call starts `observer` once for the process, it feeds the value through `OnlineSink.set_online` and is never stopped. Later calls ignore their `observer`.
    #[uniffi::constructor]
    pub fn new(observer: Arc<dyn PlatformObserver>) -> Arc<Self> {
        static STARTED: std::sync::OnceLock<Arc<dyn PlatformObserver>> = std::sync::OnceLock::new();
        STARTED.get_or_init(|| {
            crate::lock::effect(|| observer.start(OnlineSink::of_status()));
            observer
        });
        Arc::new(Self)
    }

    pub fn current(&self) -> Option<bool> { *network_value().borrow() }

    pub fn subscribe(&self) -> Arc<NetworkSubscription> { Arc::new(NetworkSubscription(Reader::of(network_value().subscribe()))) }
}

#[derive(uniffi::Object)]
pub struct NetworkSubscription(Reader<Option<bool>>);

#[uniffi::export]
impl NetworkSubscription {
    pub async fn next(&self) -> Option<bool> {
        loop {
            match self.0.next().await? {
                Some(online) => return Some(online),
                None => continue,
            }
        }
    }

    pub fn cancel(&self) { self.0.cancel(); }
}
