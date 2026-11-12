use std::sync::Arc;

use tokio::sync::watch;

use super::reading::Reader;

/// The last value the platform gave `OnlineSink.set_online`, `None` before the first one.
pub(crate) fn network_value() -> &'static watch::Sender<Option<bool>> {
    static VALUE: std::sync::OnceLock<watch::Sender<Option<bool>>> = std::sync::OnceLock::new();
    VALUE.get_or_init(|| watch::channel(None).0)
}

/// The only writer: a value equal to the current one wakes nobody.
pub(crate) fn record_network(online: bool) {
    crate::lock::effect(|| network_value().send_if_modified(|value| std::mem::replace(value, Some(online)) != Some(online)));
}

/// The network of the phone as the platform reports it, independent of the server, the cable, the retries and the session.
#[derive(uniffi::Object)]
pub struct NetworkStatus;

#[uniffi::export]
impl NetworkStatus {
    #[uniffi::constructor]
    pub fn new() -> Arc<Self> { Arc::new(Self) }

    pub fn current(&self) -> Option<bool> { *network_value().borrow() }

    /// A reader of the values: no frame before the first value, then the current one, then each newer one.
    pub fn subscribe(&self) -> Arc<NetworkSubscription> { Arc::new(NetworkSubscription(Reader::of(network_value().subscribe()))) }
}

#[derive(uniffi::Object)]
pub struct NetworkSubscription(Reader<Option<bool>>);

#[uniffi::export]
impl NetworkSubscription {
    /// The next value, `None` once the reader was cancelled.
    pub async fn next(&self) -> Option<bool> {
        loop {
            match self.0.next().await? {
                Some(online) => return Some(online),
                None => continue,
            }
        }
    }

    /// Ends this reader: a waiting `next` and every later one return `None`.
    pub fn cancel(&self) { self.0.cancel(); }
}
