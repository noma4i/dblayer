use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use super::encrypted_tests::{SubscriptionsOnly, ThroughServer};
use super::session_guards_tests::{Rig, SOCIAL_BODY};
use super::{Config, Database, EncryptedConfig, NetworkStatus, OnlineSink, OpenPorts, TestClock};
use crate::test_wait::{in_own_process, within, within_named};
use crate::transport::encrypted::tests::vectors_tests::{origin, pin, public_key_der_b64};

/// The two platform entries of `set_online`: the database after `open` and `Connectivity` before it.
const TARGETS: [&str; 2] = ["connectivity", "database"];

type Frame = Pin<Box<dyn Future<Output = Option<bool>> + Send>>;

fn run(work: impl Future<Output = ()>) { tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap().block_on(work); }

fn poll_once(call: &mut Frame) -> Poll<Option<bool>> { call.as_mut().poll(&mut Context::from_waker(Waker::noop())) }

macro_rules! reading {
    ($reader:expr) => {{
        let reader = $reader.clone();
        let call: Frame = Box::pin(async move { within_named(8, "a read of the public status", reader.next()).await });
        call
    }};
}

struct Platform {
    sink: Arc<OnlineSink>,
    rig: Rig,
    database: Option<Arc<Database>>,
}

fn config() -> EncryptedConfig { EncryptedConfig { origin: origin().into(), server_key: public_key_der_b64().into(), pin: pin().into(), ..crate::transport::encrypted::tests::protocol_tests::config() } }

async fn open_database(rig: &Rig) -> Arc<Database> {
    let ports = OpenPorts { transport: Some(Arc::new(SubscriptionsOnly)), encrypted: Some(config()), credentials: Some(rig.credentials.clone()), http: Some(Arc::new(ThroughServer(rig.server.clone()))), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() };
    within(Database::open(Config { path: None, data_version: None }, crate::ffi::EMPTY_SPECS.to_string(), ports)).await.unwrap()
}

async fn platform(target: &str) -> Platform {
    let rig = Rig::new();
    match target {
        "connectivity" => Platform { sink: OnlineSink::of_connectivity(crate::transport::Connectivity::new(false)), rig, database: None },
        _ => {
            let database = open_database(&rig).await;
            Platform { sink: database.online_sink().unwrap(), rig, database: Some(database) }
        }
    }
}

impl Platform {
    async fn quiet(&self) { if let Some(database) = &self.database { crate::test_wait::settled(database, "the owners heard the platform", || true).await; } }

    async fn set(&self, online: bool) {
        self.sink.set_online(online);
        self.quiet().await;
    }
}

/// Requirement: the first value of the platform is the public value and reaches every reader once. Observable: `current` and the frames. Assert: `None` and no frame before, `Some(v)` and the frame `v` on both entries after.
#[test]
fn net_status_first_value() {
    in_own_process(module_path!(), "net_status_first_value", &TARGETS, |target| run(async {
        let platform = platform(target).await;
        let status = NetworkStatus::new();
        assert_eq!(status.current(), None, "{target}: no value before the first input");
        let (first, second) = (status.subscribe(), status.subscribe());
        let (mut one, mut two) = (reading!(&first), reading!(&second));
        assert!(poll_once(&mut one).is_pending() && poll_once(&mut two).is_pending(), "{target}: no frame before the first input");
        platform.set(true).await;
        assert_eq!((within(one).await, within(two).await), (Some(true), Some(true)), "{target}: each reader gets the first value");
        assert_eq!(status.current(), Some(true), "{target}");
    }));
}

/// Requirement: a changed value reaches each reader once. Observable: the frames of two readers. Assert: the changed value arrives once per reader, no frame follows it.
#[test]
fn net_status_change() {
    in_own_process(module_path!(), "net_status_change", &TARGETS, |target| run(async {
        let platform = platform(target).await;
        let status = NetworkStatus::new();
        platform.set(false).await;
        let (first, second) = (status.subscribe(), status.subscribe());
        assert_eq!((within(first.next()).await, within(second.next()).await), (Some(false), Some(false)), "{target}: the first read is the current value");
        let (mut one, mut two) = (reading!(&first), reading!(&second));
        assert!(poll_once(&mut one).is_pending() && poll_once(&mut two).is_pending(), "{target}");
        platform.set(true).await;
        assert_eq!((within(one).await, within(two).await), (Some(true), Some(true)), "{target}: each reader gets the change");
        let (mut again_one, mut again_two) = (reading!(&first), reading!(&second));
        assert!(poll_once(&mut again_one).is_pending() && poll_once(&mut again_two).is_pending(), "{target}: the change comes once");
        assert_eq!(status.current(), Some(true), "{target}");
    }));
}

/// Requirement: the same value again wakes nobody. Observable: a waiting read. Assert: it stays pending after repeats, and the next frame is the next real change.
#[test]
fn net_status_same_value_silent() {
    in_own_process(module_path!(), "net_status_same_value_silent", &TARGETS, |target| run(async {
        let platform = platform(target).await;
        let status = NetworkStatus::new();
        platform.set(true).await;
        let reader = status.subscribe();
        assert_eq!(within(reader.next()).await, Some(true), "{target}");
        let mut held = reading!(&reader);
        assert!(poll_once(&mut held).is_pending(), "{target}");
        for _ in 0..3 { platform.set(true).await; }
        assert!(poll_once(&mut held).is_pending(), "{target}: the repeats woke the reader");
        assert_eq!(status.current(), Some(true), "{target}");
        platform.set(false).await;
        assert_eq!(within(held).await, Some(false), "{target}: the first frame after the repeats is the real change");
    }));
}

/// Requirement: a new reader gets the current value at once. Observable: the first frame. Assert: it is the newest value, without waiting for a change.
#[test]
fn net_status_reader_gets_current() {
    in_own_process(module_path!(), "net_status_reader_gets_current", &TARGETS, |target| run(async {
        let platform = platform(target).await;
        platform.set(true).await;
        platform.set(false).await;
        let late = NetworkStatus::new().subscribe();
        assert_eq!(within(late.next()).await, Some(false), "{target}: the first frame is the current value");
    }));
}

/// Requirement: with no value a new reader waits. Observable: the first read. Assert: pending after unrelated work, then the first platform value.
#[test]
fn net_status_reader_waits_first_value() {
    in_own_process(module_path!(), "net_status_reader_waits_first_value", &TARGETS, |target| run(async {
        let platform = platform(target).await;
        let reader = NetworkStatus::new().subscribe();
        let mut held = reading!(&reader);
        assert!(poll_once(&mut held).is_pending(), "{target}");
        platform.set_power_saving_noise().await;
        assert!(poll_once(&mut held).is_pending(), "{target}: no frame before the first value");
        platform.set(false).await;
        assert_eq!(within(held).await, Some(false), "{target}");
    }));
}

impl Platform {
    /// An input of the platform that is not the network value.
    async fn set_power_saving_noise(&self) {
        self.sink.set_power_saving(true);
        self.quiet().await;
    }
}

/// Requirement: a cancel ends only its reader. Observable: waiting reads of two readers. Assert: the cancelled one gives `None` now and later, the other still gets the next value.
#[test]
fn net_status_cancel_one_reader() {
    in_own_process(module_path!(), "net_status_cancel_one_reader", &TARGETS, |target| run(async {
        let platform = platform(target).await;
        let status = NetworkStatus::new();
        platform.set(false).await;
        let (gone, kept) = (status.subscribe(), status.subscribe());
        assert_eq!((within(gone.next()).await, within(kept.next()).await), (Some(false), Some(false)), "{target}");
        let (mut gone_call, mut kept_call) = (reading!(&gone), reading!(&kept));
        assert!(poll_once(&mut gone_call).is_pending() && poll_once(&mut kept_call).is_pending(), "{target}");
        gone.cancel();
        assert_eq!(within(gone_call).await, None, "{target}: the waiting read of the cancelled reader ends");
        assert!(poll_once(&mut kept_call).is_pending(), "{target}: the other reader goes on");
        platform.set(true).await;
        assert_eq!(within(kept_call).await, Some(true), "{target}: the other reader gets the change");
        assert_eq!(within(gone.next()).await, None, "{target}: a read after the cancel gives nothing");
    }));
}

struct Refusing;

#[async_trait::async_trait]
impl super::FakeHttp for Refusing {
    async fn post(&self, _: String, _: String, _: crate::transport::NetworkPurpose) -> Result<super::EncryptedReply, super::DbError> { Ok(super::EncryptedReply { status: 503, body: String::new() }) }
}

/// Requirement: nothing but the platform changes the value. Observable: `current` and a waiting read over server answers, a refusal, a retry write, a session reset, a close and a reopen. Assert: the value stays `Some(true)` and the read stays pending through all of them.
#[test]
fn net_status_ignores_server_and_session() {
    in_own_process(module_path!(), "net_status_ignores_server_and_session", &["database"], |_| run(async {
        let platform = platform("database").await;
        let database = platform.database.clone().unwrap();
        let status = NetworkStatus::new();
        platform.set(true).await;
        let reader = status.subscribe();
        assert_eq!(within(reader.next()).await, Some(true));
        let mut held = reading!(&reader);
        assert!(poll_once(&mut held).is_pending());
        let unchanged = |step: &str, held: &mut Frame| {
            assert_eq!(status.current(), Some(true), "{step}: the value moved");
            assert!(poll_once(held).is_pending(), "{step}: the reader woke");
        };

        within(database.session_call("social".into(), SOCIAL_BODY.into())).await.unwrap();
        platform.quiet().await;
        unchanged("a server answer", &mut held);

        platform.rig.server.refuse_next("FORBIDDEN", true);
        within(database.session_call("confirm".into(), r#"{"code":"1"}"#.into())).await.unwrap_err();
        platform.quiet().await;
        unchanged("a refusal of the server", &mut held);

        let clock = TestClock::new(1_800_000_000_000);
        let retries = super::RetryProgress::new();
        let call = tokio::spawn({
            let (retries, clock) = (retries.clone(), clock.manual());
            async move { super::encrypted::fetch_server_config_with(config(), crate::transport::AppPlatform::Ios, "1.2".into(), None, Arc::new(Refusing), Some(retries), super::encrypted::Waits { connectivity: crate::transport::Connectivity::new(true), clock }).await }
        });
        within_named(8, "the retry timer is armed", clock.await_armed(0)).await;
        unchanged("a write of the retry progress", &mut held);
        call.abort();

        within(database.session_reset()).await.unwrap();
        platform.quiet().await;
        unchanged("a session reset", &mut held);

        within(database.close_database()).await.unwrap();
        unchanged("a close of the database", &mut held);

        let reopened = open_database(&platform.rig).await;
        unchanged("a reopen of the database", &mut held);
        within(reopened.close_database()).await.unwrap();
        unchanged("the close of the reopened database", &mut held);
    }));
}

/// Property P-NET-11: the public value is the last value the platform gave, on any trace. Observable: `current` and the frame count. Assert: over scripted traces mixing platform values with server and session events, `current` equals the last platform value and a reader got one frame per change; a trace without a platform value gives `None` and 0 frames.
#[test]
fn net_prop_status_follows_only_the_platform() {
    in_own_process(module_path!(), "net_prop_status_follows_only_the_platform", &TARGETS, |target| run(async {
        let platform = platform(target).await;
        let status = NetworkStatus::new();
        if let Some(database) = &platform.database {
            within(database.close_database()).await.unwrap();
        }
        assert_eq!(status.current(), None, "{target}: no platform value yet");
        let mut silent = reading!(&status.subscribe());
        assert!(poll_once(&mut silent).is_pending(), "{target}: no frame without a platform value");
        let platform = self::platform(target).await;
        let reader = status.subscribe();
        let mut held = reading!(&reader);
        let (mut last, mut frames, mut changes) = (None, 0, 0);
        for round in 0..24u32 {
            let value = (round.wrapping_mul(2_654_435_761) >> 7) % 3 != 0;
            if let Some(database) = platform.database.as_ref().filter(|_| round % 5 == 4) {
                if last == Some(true) { let _ = within(database.session_call("social".into(), SOCIAL_BODY.into())).await; }
                let _ = within(database.session_reset()).await;
                platform.quiet().await;
            }
            platform.set(value).await;
            if last != Some(value) { changes += 1; }
            last = Some(value);
            if let Poll::Ready(frame) = poll_once(&mut held) {
                assert_eq!(frame, last, "{target} round {round}: the frame is the newest value");
                frames += 1;
                held = reading!(&reader);
            }
            assert_eq!(status.current(), last, "{target} round {round}");
        }
        assert_eq!(frames, changes, "{target}: one frame per change");
    }));
}

/// Property P-NET-12: frames of a reader <= changes, the slow reader gets the newest value, idle gives 0 wake-ups. Observable: frames of a slow and a fast reader. Assert: the slow reader reads once after 5 changes and gets the last value, the fast reader gets 5 frames, idle polls stay pending.
#[test]
fn net_prop_status_newest_wins() {
    in_own_process(module_path!(), "net_prop_status_newest_wins", &TARGETS, |target| run(async {
        let platform = platform(target).await;
        let status = NetworkStatus::new();
        platform.set(true).await;
        let (slow, fast) = (status.subscribe(), status.subscribe());
        assert_eq!((within(slow.next()).await, within(fast.next()).await), (Some(true), Some(true)), "{target}");
        let mut fast_frames = Vec::new();
        for value in [false, true, false, true, false] {
            let mut held = reading!(&fast);
            assert!(poll_once(&mut held).is_pending(), "{target}: idle");
            platform.set(value).await;
            fast_frames.push(within(held).await);
        }
        assert_eq!(fast_frames, [false, true, false, true, false].map(Some), "{target}: the fast reader sees every change");
        assert_eq!(within(slow.next()).await, Some(false), "{target}: the slow reader gets the newest value");
        let (mut slow_idle, mut fast_idle) = (reading!(&slow), reading!(&fast));
        assert!(poll_once(&mut slow_idle).is_pending() && poll_once(&mut fast_idle).is_pending(), "{target}: idle gives no frame");
    }));
}
