use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use super::{DbError, EncryptedReply, FakeHttp, OnlineSink, PlatformObserver, RetryProgress, TestClock};
use crate::test_wait::{recv_within, within, yield_until};
use crate::transport::encrypted::vectors_tests::{origin, pin, public_key_der_b64};
use crate::transport::{AppPlatform, EncryptedConfig, RetrySnapshot};

struct Platform {
    sink: Mutex<Option<Arc<OnlineSink>>>,
    started: Mutex<std::sync::mpsc::Sender<()>>,
    stops: AtomicUsize,
    at_start: Vec<bool>,
}

impl PlatformObserver for Platform {
    fn start(&self, sink: Arc<OnlineSink>) {
        *self.sink.lock().unwrap() = Some(sink.clone());
        for online in &self.at_start { sink.set_online(*online); }
        self.started.lock().unwrap().send(()).unwrap();
    }
    fn stop(&self) { self.stops.fetch_add(1, Ordering::SeqCst); }
}

impl Platform {
    fn offline() -> (Arc<Self>, std::sync::mpsc::Receiver<()>) { Self::reporting(&[]) }
    fn reporting(at_start: &[bool]) -> (Arc<Self>, std::sync::mpsc::Receiver<()>) {
        let (started, heard) = std::sync::mpsc::channel();
        (Arc::new(Self { sink: Mutex::default(), started: Mutex::new(started), stops: AtomicUsize::new(0), at_start: at_start.to_vec() }), heard)
    }
    fn go_online(&self) {
        let sink = self.sink.lock().unwrap().clone().unwrap();
        sink.set_online(true);
    }
}

struct Statuses {
    statuses: Mutex<VecDeque<u16>>,
    requests: AtomicUsize,
    online: AtomicBool,
}

#[async_trait]
impl FakeHttp for Statuses {
    async fn post(&self, _: String, _: String, _: crate::transport::NetworkPurpose) -> Result<EncryptedReply, DbError> {
        assert!(self.online.load(Ordering::SeqCst), "the fetch sent a request while the platform was offline");
        self.requests.fetch_add(1, Ordering::SeqCst);
        Ok(EncryptedReply { status: self.statuses.lock().unwrap().pop_front().expect("a scripted status"), body: String::new() })
    }
}

fn statuses(list: &[u16]) -> Arc<Statuses> {
    Arc::new(Statuses { statuses: Mutex::new(list.iter().copied().collect()), requests: AtomicUsize::new(0), online: AtomicBool::new(false) })
}

fn config() -> EncryptedConfig {
    EncryptedConfig { origin: origin().into(), pin: pin().into(), server_key: public_key_der_b64().into(), ..crate::transport::encrypted::protocol_tests::config() }
}

type Fetched = tokio::task::JoinHandle<Result<super::ServerConfig, DbError>>;

fn fetch(http: &Arc<Statuses>, clock: &Arc<TestClock>, retries: Option<Arc<RetryProgress>>, platform: Arc<Platform>) -> Fetched {
    let (http, clock) = (http.clone(), clock.clone());
    tokio::spawn(async move { super::fetch_server_config_with_clock(config(), AppPlatform::Ios, "1.2".into(), None, super::FetchPorts { http, retries, observers: Some(platform) }, clock).await })
}

#[tokio::test(flavor = "multi_thread")]
async fn life_bootstrap_waits_online() {
    let (platform, started) = Platform::offline();
    let http = statuses(&[400]);
    let clock = TestClock::new(1_800_000_000_000);
    let call = fetch(&http, &clock, None, platform.clone());
    recv_within(&started);
    assert_eq!((http.requests.load(Ordering::SeqCst), clock.armed()), (0, 0));
    http.online.store(true, Ordering::SeqCst);
    platform.go_online();
    let result = within(call).await.unwrap();
    assert!(matches!(result, Err(DbError::Http { status: 400, .. })), "{result:?}");
    assert_eq!((http.requests.load(Ordering::SeqCst), clock.armed(), platform.stops.load(Ordering::SeqCst)), (1, 0, 1), "an online wait arms no timer");
}

#[tokio::test(flavor = "multi_thread")]
async fn life_bootstrap_retry() {
    let (platform, started) = Platform::offline();
    let http = statuses(&[503, 429, 400]);
    http.online.store(true, Ordering::SeqCst);
    let clock = TestClock::new(1_800_000_000_000);
    let retries = RetryProgress::new();
    let call = fetch(&http, &clock, Some(retries.clone()), platform.clone());
    recv_within(&started);
    platform.go_online();
    within(clock.await_armed(0)).await;
    assert_eq!((http.requests.load(Ordering::SeqCst), clock.armed()), (1, 1));
    assert_eq!(retries.current().last_attempt.map(|attempt| (attempt.attempt, attempt.reason)), Some((1, "HTTP 503".to_string())));
    clock.advance(1_000);
    within(clock.await_armed(1)).await;
    assert_eq!((http.requests.load(Ordering::SeqCst), clock.armed()), (2, 2));
    assert_eq!(retries.current().last_attempt.map(|attempt| (attempt.attempt, attempt.reason)), Some((2, "HTTP 429".to_string())));
    clock.advance(2_000);
    let result = within(call).await.unwrap();
    assert!(matches!(result, Err(DbError::Http { status: 400, .. })), "{result:?}");
    assert_eq!((http.requests.load(Ordering::SeqCst), clock.armed()), (3, 2));
    assert_eq!(retries.current(), RetrySnapshot::default());
}

struct Heard(tokio::sync::watch::Sender<Option<RetrySnapshot>>);
impl super::RetryProgressListener for Heard {
    fn changed(&self, snapshot: RetrySnapshot) { self.0.send_replace(Some(snapshot)); }
}

#[tokio::test(flavor = "multi_thread")]
async fn life_bootstrap_cancel() {
    let (platform, started) = Platform::offline();
    let http = statuses(&[503]);
    http.online.store(true, Ordering::SeqCst);
    let clock = TestClock::new(1_800_000_000_000);
    let retries = RetryProgress::new();
    let (heard, mut seen) = tokio::sync::watch::channel(None);
    let subscription = retries.subscribe(Arc::new(Heard(heard))).unwrap();
    let call = fetch(&http, &clock, Some(retries.clone()), platform.clone());
    recv_within(&started);
    platform.go_online();
    within(clock.await_armed(0)).await;
    within(seen.wait_for(|snapshot| snapshot.as_ref().is_some_and(|snapshot| snapshot.active_calls == 1))).await.unwrap();
    call.abort();
    assert!(within(call).await.unwrap_err().is_cancelled());
    within(seen.wait_for(|snapshot| snapshot.as_ref().is_some_and(|snapshot| snapshot.active_calls == 0 && snapshot.last_attempt.is_none()))).await.unwrap();
    yield_until("the platform observers stopped with the cancelled call", || platform.stops.load(Ordering::SeqCst) == 1).await;
    subscription.cancel();
    yield_until("the subscription task ended", || subscription.task.is_finished()).await;
    assert_eq!(http.requests.load(Ordering::SeqCst), 1);
}

struct Answering { requests: AtomicUsize, clock: Arc<TestClock> }

#[async_trait]
impl FakeHttp for Answering {
    async fn post(&self, _: String, body: String, _: crate::transport::NetworkPurpose) -> Result<EncryptedReply, DbError> {
        assert_eq!(self.requests.fetch_add(1, Ordering::SeqCst), 0, "the fetch sent a second request");
        assert_eq!(self.clock.armed(), 0, "a timer was armed before the first answer");
        let reply = crate::transport::encrypted::bootstrap_tests::good_reply(&body);
        Ok(EncryptedReply { status: reply.status, body: reply.body })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn life_prop_bootstrap_start_orders() {
    let mut configs = Vec::new();
    for (order, at_start, after_call) in [("online before the call", &[true][..], false), ("online after the call", &[][..], true), ("flap before the first request", &[true, false][..], true)] {
        let (platform, started) = Platform::reporting(at_start);
        let clock = TestClock::new(1_800_000_000_000);
        let http = Arc::new(Answering { requests: AtomicUsize::new(0), clock: clock.clone() });
        let call = tokio::spawn({
            let (http, clock, platform) = (http.clone(), clock.clone(), platform.clone());
            async move { super::fetch_server_config_with_clock(crate::transport::encrypted::bootstrap_tests::configuration(), AppPlatform::Ios, "1.2".into(), None, super::FetchPorts { http, retries: None, observers: Some(platform) }, clock).await }
        });
        recv_within(&started);
        if after_call { platform.go_online(); }
        let config = within(call).await.unwrap().unwrap_or_else(|error| panic!("{order}: {error:?}"));
        assert_eq!((http.requests.load(Ordering::SeqCst), clock.armed()), (1, 0), "{order}");
        configs.push(config);
    }
    assert!(configs.iter().all(|config| *config == configs[0]), "the result differs across orders");
}
