use std::sync::Arc;

use tokio::sync::watch;

use super::reading::Reader;
use super::{OnlineSink, PlatformObserver};

/// The kind of the default network of the phone, `None` only while the phone is offline.
#[derive(uniffi::Enum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum NetworkKind { Wifi, Cellular, Other, None }

/// The default network of the phone: `network` grows with each change of the default network, the core does not read it.
#[derive(uniffi::Record, Clone, Copy, Debug, PartialEq, Eq)]
pub struct NetworkPath { pub online: bool, pub kind: NetworkKind, pub network: u64 }

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Processed { pub(crate) stamp: u64, pub(crate) path: Option<NetworkPath> }

pub(crate) fn network_value() -> &'static watch::Sender<Processed> {
    static VALUE: std::sync::OnceLock<watch::Sender<Processed>> = std::sync::OnceLock::new();
    VALUE.get_or_init(|| watch::channel(Processed::default()).0)
}

/// Writes the value of the process, only the status input of the process calls it: readers wake when the value changes, not when the stamp grows.
pub(crate) fn record_network(path: NetworkPath) -> u64 {
    let mut stamp = 0;
    crate::lock::effect(|| network_value().send_if_modified(|value| {
        value.stamp += 1;
        stamp = value.stamp;
        value.path.replace(path) != Some(path)
    }));
    stamp
}

/// The path of an input that knows only the online flag: the kind is unknown and the network number stays.
pub(crate) fn path_of_online(online: bool) -> NetworkPath {
    let network = network_value().borrow().path.map_or(0, |path| path.network);
    NetworkPath { online, kind: if online { NetworkKind::Other } else { NetworkKind::None }, network }
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
    /// The first call starts `observer` once for the process, it feeds the value through `OnlineSink.set_path` and is never stopped. Later calls ignore their `observer`.
    #[uniffi::constructor]
    pub fn new(observer: Arc<dyn PlatformObserver>) -> Arc<Self> {
        static STARTED: std::sync::OnceLock<Arc<dyn PlatformObserver>> = std::sync::OnceLock::new();
        STARTED.get_or_init(|| {
            if let Ok(executor) = super::database::executor() {
                let started = observer.clone();
                crate::task::spawn_on(executor.handle(), async move { crate::lock::effect(|| started.start(OnlineSink::of_status())); });
            }
            observer
        });
        Arc::new(Self)
    }

    pub fn current(&self) -> Option<NetworkPath> { network_value().borrow().path }

    pub fn subscribe(&self) -> Arc<NetworkSubscription> { Arc::new(NetworkSubscription(Reader::of(network_value().subscribe()))) }
}

#[derive(uniffi::Object)]
pub struct NetworkSubscription(Reader<Processed>);

#[uniffi::export]
impl NetworkSubscription {
    pub async fn next(&self) -> Option<NetworkPath> {
        loop {
            if let Some(path) = self.0.next().await?.path { return Some(path); }
        }
    }

    pub fn cancel(&self) { self.0.cancel(); }
}
