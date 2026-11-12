use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use super::encrypted_tests::{SubscriptionsOnly, ThroughServer};
use super::session_guards_tests::{Rig, SOCIAL_BODY};
use super::{Config, Database, EncryptedConfig, NetworkStatus, OnlineSink, OpenPorts};
use crate::test_wait::{in_own_process, within, within_named};
use crate::transport::encrypted::tests::vectors_tests::{origin, pin, public_key_der_b64};

fn run(work: impl std::future::Future<Output = ()>) { tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap().block_on(work); }

/// The observer of the platform: it keeps the input of the process value it was started with.
struct ProcessDouble { sink: Mutex<Option<Arc<OnlineSink>>> }

impl super::PlatformObserver for ProcessDouble {
    fn start(&self, sink: Arc<OnlineSink>) { *self.sink.lock().unwrap() = Some(sink); }
    fn stop(&self) {}
}

/// The status of the process over its one observer: the returned sink is the OS value of the network.
fn process() -> (Arc<NetworkStatus>, Arc<OnlineSink>) {
    let double = Arc::new(ProcessDouble { sink: Default::default() });
    let status = NetworkStatus::new(double.clone());
    let sink = double.sink.lock().unwrap().clone().expect("the first status starts the observer");
    (status, sink)
}

fn config() -> EncryptedConfig { EncryptedConfig { origin: origin().into(), server_key: public_key_der_b64().into(), pin: pin().into(), ..crate::transport::encrypted::tests::protocol_tests::config() } }

/// Opens like the platform does for an app that never read the status: no observer is passed to the database.
async fn open_database(rig: &Rig) -> Arc<Database> {
    let ports = OpenPorts { transport: Some(Arc::new(SubscriptionsOnly)), encrypted: Some(config()), credentials: Some(rig.credentials.clone()), http: Some(Arc::new(ThroughServer(rig.server.clone()))), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() };
    within(Database::open(Config { path: None, data_version: None }, crate::ffi::EMPTY_SPECS.to_string(), ports)).await.unwrap()
}

type Call = tokio::task::JoinHandle<Result<String, super::DbError>>;

/// A call of the application that needs the network.
fn call(database: &Arc<Database>) -> Call {
    let database = database.clone();
    tokio::spawn(async move { database.session_call("social".into(), SOCIAL_BODY.into()).await })
}

async fn quiet(database: &Arc<Database>) { crate::test_wait::settled(database, "the owners heard the input", || true).await; }

/// Counts the posts of the bootstrap and answers each one.
struct Posts(AtomicUsize);

#[async_trait::async_trait]
impl super::FakeHttp for Posts {
    async fn post(&self, _: String, body: String, _: crate::transport::NetworkPurpose) -> Result<super::EncryptedReply, super::DbError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        let reply = crate::transport::encrypted::tests::bootstrap_tests::good_reply(&body);
        Ok(super::EncryptedReply { status: reply.status, body: reply.body })
    }
}

/// Requirement D-351: `open` takes the value the process already has. Input: the OS value reached the process before `open`, `open` with no observer of its own, then a call that needs the network. Observable: the call reaches the server with `true`, never reaches it with `false`. Catches: the database starting offline until its own observer reports.
#[test]
fn net_open_takes_the_process_value() {
    in_own_process(module_path!(), "net_open_takes_the_process_value", &["online", "offline"], |value| run(async {
        let (_status, os) = process();
        os.set_online(value == "online");
        let rig = Rig::new();
        let database = open_database(&rig).await;
        let before = rig.server.executed().len();
        let call = call(&database);
        if value == "online" {
            within(call).await.unwrap().unwrap();
            assert_eq!(rig.server.executed().len(), before + 1, "the call left at once");
        } else {
            quiet(&database).await;
            assert!(!call.is_finished(), "the offline call must not end with the server's answer");
            assert_eq!(rig.server.executed().len(), before, "no request leaves while the process value is false");
            call.abort();
        }
        within(database.close_database()).await.unwrap();
    }));
}

/// Requirement D-351: with no process value the base is offline until the first OS value. Input: `open` before any value, a call, then the first OS value `true`. Observable: nothing leaves before the value, the call leaves once after it. Catches: the database never hearing the first value of the process observer.
#[test]
fn net_open_before_first_value() {
    in_own_process(module_path!(), "net_open_before_first_value", &["status"], |_| run(async {
        let (status, os) = process();
        assert_eq!(status.current(), None);
        let rig = Rig::new();
        let database = open_database(&rig).await;
        let before = rig.server.executed().len();
        let call = call(&database);
        quiet(&database).await;
        assert!(!call.is_finished(), "the call waits for the first value");
        assert_eq!(rig.server.executed().len(), before, "nothing leaves before the first value");
        os.set_online(true);
        within(call).await.unwrap().unwrap();
        assert_eq!(rig.server.executed().len(), before + 1, "the first value turns the network on once");
        within(database.close_database()).await.unwrap();
    }));
}

/// Requirement D-351: one OS input is the public value, the database flag and the wait of `serverConfig`; the single writer is the process observer. Input: a database open and a `serverConfig` fetch waiting, then one `true`. Observable: `current`, one frame of a reader, the call of the database leaves, the fetch posts once and returns. Catches: the database or `serverConfig` keeping an own network observer, or a second writer waking the reader twice.
#[test]
fn net_status_feeds_database_and_config() {
    in_own_process(module_path!(), "net_status_feeds_database_and_config", &["status"], |_| run(async {
        let (status, os) = process();
        let rig = Rig::new();
        let database = open_database(&rig).await;
        let posts = Arc::new(Posts(AtomicUsize::new(0)));
        let ports = super::FetchPorts { http: posts.clone(), retries: None, observers: None };
        let fetch = tokio::spawn(super::fetch_server_config_with_clock(crate::transport::encrypted::tests::bootstrap_tests::configuration(), crate::transport::AppPlatform::Ios, "1.2".into(), None, ports, super::TestClock::new(1_800_000_000_000)));
        let reader = status.subscribe();
        let before = rig.server.executed().len();
        let call = call(&database);
        quiet(&database).await;
        assert!(!call.is_finished() && !fetch.is_finished(), "both wait for the value");
        assert_eq!((posts.0.load(Ordering::SeqCst), rig.server.executed().len()), (0, before), "nothing leaves before the value");
        os.set_online(true);
        assert_eq!(within(reader.next()).await.map(|path| path.online), Some(true));
        within(call).await.unwrap().unwrap();
        within_named(8, "the fetch got the value", fetch).await.unwrap().unwrap();
        assert_eq!((posts.0.load(Ordering::SeqCst), rig.server.executed().len()), (1, before + 1), "each wait left once");
        let mut next = Box::pin(reader.next());
        assert!(std::future::Future::poll(next.as_mut(), &mut std::task::Context::from_waker(std::task::Waker::noop())).is_pending(), "one input gives one frame");
        assert_eq!(status.current().map(|path| path.online), Some(true));
        within(database.close_database()).await.unwrap();
    }));
}

/// Requirement D-355: a call of `set_online` of the database is no value of the process. Input: the status of the process, a database, its `set_online(true)`, a close, a second database, a call that needs the network. Observable: the public value and the requests of the second database. Assert: the value stays `None` and the second database sends nothing.
#[test]
fn net_database_input_keeps_the_process_value() {
    in_own_process(module_path!(), "net_database_input_keeps_the_process_value", &["direct"], |_| run(async {
        let (status, _os) = process();
        let first = open_database(&Rig::new()).await;
        first.set_online(true).unwrap();
        assert_eq!(status.current(), None, "the call of the database moved the value of the process");
        within(first.close_database()).await.unwrap();
        let rig = Rig::new();
        let second = open_database(&rig).await;
        let before = rig.server.executed().len();
        let call = call(&second);
        quiet(&second).await;
        assert!(!call.is_finished(), "the second database took the value of the first one");
        assert_eq!(rig.server.executed().len(), before, "no request leaves before a value of the process");
        call.abort();
        within(second.close_database()).await.unwrap();
    }));
}

/// Requirement D-355: `Connectivity` applies only a value with a stamp above the applied one. Input: stamp 2, then stamp 1. Observable: `is_online`. Assert: the older value changes nothing.
#[test]
fn net_connectivity_stale_stamp() {
    let connectivity = crate::transport::Connectivity::new(false);
    connectivity.change(true, 2);
    connectivity.change(false, 1);
    assert!(connectivity.is_online(), "a value older than the applied one changed the connectivity");
}
