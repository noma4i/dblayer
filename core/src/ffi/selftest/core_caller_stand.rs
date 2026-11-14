//! The stand of P-BR-09: the database is held at the readers of the pool, the writer lock and the owner `Network`, and a platform entry runs on a thread that has no runtime of the core.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use crate::ffi::Database;
use crate::owners::contract::NetworkEvent;
use crate::owners::lifecycle::Peer;
use crate::test_wait::{signalled, Flag};

/// Both readers of the pool and the writer lock are taken, and the owner `Network` stands inside a sink of its broadcast: it answers no input until `release`.
pub(super) struct Hold {
    release: Arc<Flag>,
}

impl Hold {
    pub(super) async fn take(database: &Arc<Database>) -> Hold {
        let release = Arc::new(Flag::default());
        let (readers, writer, owner) = (Arc::new(Flag::default()), Arc::new(Flag::default()), Arc::new(Flag::default()));
        let (store, held, gate) = (database.db.clone(), readers.clone(), release.clone());
        crate::task::spawn_thread("hold-readers".into(), move || store.hold_readers(&held, &gate)).unwrap();
        let (store, held, gate) = (database.db.clone(), writer.clone(), release.clone());
        crate::task::spawn_thread("hold-writer".into(), move || {
            let transaction = store.lock();
            held.set();
            gate.wait_blocking("release the writer");
            drop(transaction);
        }).unwrap();
        let (entered, gate, armed) = (owner.clone(), release.clone(), AtomicBool::new(false));
        database.life.on_network(Peer::Windows, move |event| {
            if matches!(event, NetworkEvent::Foreground(_)) && !armed.swap(true, Ordering::SeqCst) {
                entered.set();
                gate.wait_blocking("release the owner Network");
            }
        });
        drop(database.life.background());
        drop(database.life.foreground());
        readers.wait("both readers of the pool are held").await;
        writer.wait("the writer holds its lock").await;
        owner.wait("the owner Network stands in its sink").await;
        Hold { release }
    }

    pub(super) fn release(&self) { self.release.set(); }
}

impl Drop for Hold {
    fn drop(&mut self) { self.release.set(); }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(super) enum Returned {
    /// The call came back with its result.
    Result,
    /// The first poll of an `async` entry answered pending.
    Pending,
    /// The first poll of an `async` entry already had the result.
    Ready,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Event {
    Returned(&'static str, Returned),
    Settled(&'static str, String),
    Polling(&'static str),
    Parked(&'static str),
}

#[derive(Default)]
pub(super) struct Collector {
    events: Mutex<Vec<Event>>,
    signal: tokio::sync::Notify,
}

impl Collector {
    pub(super) fn push(&self, event: Event) {
        self.events.lock().unwrap().push(event);
        crate::lock::effect(|| self.signal.notify_waiters());
    }

    pub(super) fn events(&self) -> Vec<Event> { self.events.lock().unwrap().clone() }

    pub(super) fn returned(&self, name: &str) -> Option<Returned> {
        self.events().into_iter().find_map(|event| match event { Event::Returned(found, how) if found == name => Some(how), _ => None })
    }

    pub(super) fn settled(&self, name: &str) -> Option<String> { Self::settled_in(&self.events(), name) }

    /// The state of `name` in one snapshot of the events: the last event of its polls is `Parked`.
    pub(super) fn parked_in(events: &[Event], name: &str) -> bool {
        let last = events.iter().rev().find_map(|event| match event {
            Event::Polling(found) if *found == name => Some(false),
            Event::Parked(found) if *found == name => Some(true),
            Event::Settled(found, _) if *found == name => Some(false),
            _ => None,
        });
        last == Some(true)
    }

    pub(super) fn settled_in(events: &[Event], name: &str) -> Option<String> {
        events.iter().find_map(|event| match event { Event::Settled(found, outcome) if *found == name => Some(outcome.clone()), _ => None })
    }

    /// Waits at most 8 s for `ready`. The panic of the wait stays in its task, so the test goes on to name every case that is missing.
    pub(super) async fn arrived(self: &Arc<Self>, what: &'static str, ready: impl Fn(&Collector) -> bool + Send + Sync + 'static) -> bool {
        let collector = self.clone();
        crate::task::spawn(crate::test_wait::within_named(8, what, async move { signalled(&collector.signal, what, || ready(&collector)).await })).await.is_ok()
    }
}

/// The report of one case, made on the thread of the case.
pub(super) struct Report {
    pub(super) name: &'static str,
    pub(super) collector: Arc<Collector>,
}

impl Report {
    pub(super) fn returned(&self, how: Returned) { self.collector.push(Event::Returned(self.name, how)); }

    pub(super) fn settled(&self, outcome: String) { self.collector.push(Event::Settled(self.name, outcome)); }

    /// Polls an `async` entry on this thread at every wake-up and reports what each poll gave: the first poll, then parked until the next wake-up.
    pub(super) fn drive<T: std::fmt::Debug>(&self, future: impl Future<Output = T>) -> String {
        let mut future = std::pin::pin!(future);
        let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
        let mut context = Context::from_waker(&waker);
        let mut first = true;
        loop {
            self.collector.push(Event::Polling(self.name));
            let polled = future.as_mut().poll(&mut context);
            if first { self.returned(if polled.is_pending() { Returned::Pending } else { Returned::Ready }); }
            first = false;
            match polled {
                Poll::Ready(value) => return format!("{value:?}"),
                Poll::Pending => {
                    self.collector.push(Event::Parked(self.name));
                    std::thread::park();
                }
            }
        }
    }
}

struct Unpark(std::thread::Thread);

impl Wake for Unpark {
    fn wake(self: Arc<Self>) { self.0.unpark(); }
}
