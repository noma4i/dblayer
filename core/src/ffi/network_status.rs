use std::sync::Arc;

use tokio::sync::watch;

use super::reading::Reader;
use super::{OnlineSink, PlatformObserver};

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Processed { pub(crate) stamp: u64, pub(crate) online: Option<bool> }

pub(crate) fn network_value() -> &'static watch::Sender<Processed> {
    static VALUE: std::sync::OnceLock<watch::Sender<Processed>> = std::sync::OnceLock::new();
    VALUE.get_or_init(|| watch::channel(Processed::default()).0)
}

/// Writes the value of the process, only the status input of the process calls it: readers wake when the value changes, not when the stamp grows.
pub(crate) fn record_network(online: bool) -> u64 {
    let mut stamp = 0;
    crate::lock::effect(|| network_value().send_if_modified(|value| {
        value.stamp += 1;
        stamp = value.stamp;
        std::mem::replace(&mut value.online, Some(online)) != Some(online)
    }));
    stamp
}

/// The next stamp of the process for a direct input: the value of the process stays.
pub(crate) fn next_stamp() -> u64 {
    let mut stamp = 0;
    crate::lock::effect(|| network_value().send_if_modified(|value| {
        value.stamp += 1;
        stamp = value.stamp;
        false
    }));
    stamp
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

    pub fn current(&self) -> Option<bool> { network_value().borrow().online }

    pub fn subscribe(&self) -> Arc<NetworkSubscription> { Arc::new(NetworkSubscription(Reader::of(network_value().subscribe()))) }
}

#[derive(uniffi::Object)]
pub struct NetworkSubscription(Reader<Processed>);

#[uniffi::export]
impl NetworkSubscription {
    pub async fn next(&self) -> Option<bool> {
        loop {
            if let Some(online) = self.0.next().await?.online { return Some(online); }
        }
    }

    pub fn cancel(&self) { self.0.cancel(); }
}
