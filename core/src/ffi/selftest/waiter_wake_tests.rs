use std::cell::Cell;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use crate::ffi::transport::OnlineSink;
use crate::ffi::{NetworkKind, NetworkPath, NetworkStatus, PlatformObserver};
use crate::test_wait::{in_own_process, Flag};

thread_local! { static INSIDE_INPUT: Cell<bool> = const { Cell::new(false) }; }

struct Silent;

impl PlatformObserver for Silent {
    fn start(&self, _sink: Arc<OnlineSink>) {}

    fn stop(&self) {}
}

/// The wake-up of the waiting entry: it notes whether it ran on a thread that is inside an input of the platform.
#[derive(Default)]
struct Probe {
    inside_input: AtomicBool,
    woken: Flag,
}

impl Wake for Probe {
    fn wake(self: Arc<Self>) {
        if INSIDE_INPUT.get() { self.inside_input.store(true, Ordering::SeqCst); }
        self.woken.set();
    }
}

/// Requirement P-BR-15: the continuation of a waiting `async` entry does not start on the thread of the input of the platform that wrote the value.
/// Observable: the thread of the wake-up of the waiter and the value the waiter receives. The waiting entries without an input of the platform (`RetrySubscription.next`, `LoadRequest.result`, `TestClock.await_armed`) are woken by tasks of the core and are not in the table.
/// Defect caught: the wake-up inside `set_path` calls the continuation of the platform inside `start` of the observer, a nested callback that aborts the Android process.
#[test]
fn bridge_prop_waiter_wakes_off_the_platform_input() {
    in_own_process(module_path!(), "bridge_prop_waiter_wakes_off_the_platform_input", &["set_path", "set_online"], |variant| {
        let variant = variant.to_string();
        crate::test_wait::limited(Duration::from_secs(18), run(variant));
    });
}

async fn run(variant: String) {
    let status = NetworkStatus::new(Arc::new(Silent));
    let subscription = status.subscribe();
    let mut next = Box::pin(subscription.next());
    let probe = Arc::new(Probe::default());
    let waker = Waker::from(probe.clone());
    let mut context = Context::from_waker(&waker);
    assert!(next.as_mut().poll(&mut context).is_pending(), "{variant}: the status has no value yet, the first poll must wait");

    let returned = Arc::new(Flag::default());
    let done = returned.clone();
    let input = variant.clone();
    crate::task::spawn_thread("platform-input".into(), move || {
        INSIDE_INPUT.set(true);
        let sink = OnlineSink::of_status();
        match input.as_str() {
            "set_path" => sink.set_path(NetworkPath { online: true, kind: NetworkKind::Wifi, network: 1 }),
            _ => sink.set_online(true),
        }
        INSIDE_INPUT.set(false);
        done.set();
    }).unwrap();
    returned.wait("the input of the platform returned").await;
    probe.woken.wait("the waiting entry was woken").await;
    assert!(!probe.inside_input.load(Ordering::SeqCst), "{variant}: the waiter was woken on the thread of the input of the platform");
    match next.as_mut().poll(&mut context) {
        Poll::Ready(Some(path)) => assert!(path.online, "{variant}: the waiter got {path:?}"),
        other => panic!("{variant}: the waiter did not get the value after its wake-up: {other:?}"),
    }
    assert!(status.current().is_some_and(|path| path.online), "{variant}: current() does not see the value after the input returned");
}
