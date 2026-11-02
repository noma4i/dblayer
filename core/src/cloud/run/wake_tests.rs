use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use serde_json::json;

use super::{Cloud, Inner, PhaseEvent};
use crate::cloud::fixtures::*;
use crate::cloud::{CloudState, MergeSource};
use crate::db::Db;
use crate::identity::Generation;
use crate::test_wait::{join_within, mutex_free, ProbeWaker};

fn poll<F: Future>(future: &mut std::pin::Pin<Box<F>>, waker: Arc<impl Wake + Send + std::marker::Sync + 'static>) -> Poll<F::Output> {
    future.as_mut().poll(&mut Context::from_waker(&Waker::from(waker)))
}

struct ReadsStore {
    db: Db,
    generation: Generation,
    unstamped: Mutex<Vec<usize>>,
    wakes: AtomicUsize,
}

impl Wake for ReadsStore {
    fn wake(self: Arc<Self>) {
        let left = self.db.cloud_unstamped(self.generation).unwrap().len();
        self.unstamped.lock().unwrap().push(left);
        self.wakes.fetch_add(1, Ordering::SeqCst);
    }
}

struct Publishes {
    inner: Arc<Inner>,
    wakes: AtomicUsize,
}

impl Wake for Publishes {
    fn wake(self: Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
        self.inner.publish(PhaseEvent::Done);
    }
}

fn state_probe(cloud: &Cloud) -> Arc<ProbeWaker> {
    let inner = cloud.inner.clone();
    ProbeWaker::new(move || mutex_free(&inner.phase) && inner.effects.is_free())
}

#[tokio::test]
async fn cloud_stamp_wake_reads_store() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("a.db");
    {
        let plain = Db::open(&path, vec![notes_spec(&[]), profiles_spec()], None).unwrap();
        plain.create("notes", obj(json!({"id": "n1", "title": "local"}))).unwrap();
    }
    let a = device_with(Db::open(&path, specs(), None).unwrap(), &FakeCloud::new(), 1_000);
    let generation = a.db.generation();
    let rows = a.db.cloud_unstamped(generation).unwrap();
    assert_eq!(rows.len(), 1);
    let reads = Arc::new(ReadsStore { db: a.db.clone(), generation, unstamped: Mutex::default(), wakes: AtomicUsize::new(0) });
    let mut changes = a.db.cloud_changes();
    changes.mark_unchanged();
    let mut waiting = Box::pin(changes.changed());
    assert!(poll(&mut waiting, reads.clone()).is_pending());

    let db = a.db.clone();
    join_within(std::thread::spawn(move || db.cloud_stamp_rows(generation, &rows, MergeSource::UseLocal).unwrap()));

    assert_eq!(reads.wakes.load(Ordering::SeqCst), 1);
    assert_eq!(*reads.unstamped.lock().unwrap(), vec![0]);
    assert!(poll(&mut waiting, reads).is_ready());
}

#[tokio::test]
async fn cloud_state_publish_wakes_with_no_guards() {
    let a = device(&FakeCloud::new(), 1_000);
    let mut states = a.cloud.state_changes();
    states.mark_unchanged();
    let probe = state_probe(&a.cloud);
    let mut waiting = Box::pin(states.changed());
    assert!(poll(&mut waiting, probe.clone()).is_pending());

    let inner = a.cloud.inner.clone();
    join_within(std::thread::spawn(move || inner.publish(PhaseEvent::Start)));

    assert_eq!((probe.wakes(), probe.violations()), (1, 0));
    assert_eq!(a.cloud.state(), CloudState::Syncing);
}

#[tokio::test]
async fn cloud_state_wake_may_publish_again() {
    let a = device(&FakeCloud::new(), 1_000);
    let mut states = a.cloud.state_changes();
    states.mark_unchanged();
    let again = Arc::new(Publishes { inner: a.cloud.inner.clone(), wakes: AtomicUsize::new(0) });
    let mut waiting = Box::pin(states.changed());
    assert!(poll(&mut waiting, again.clone()).is_pending());

    let inner = a.cloud.inner.clone();
    join_within(std::thread::spawn(move || inner.publish(PhaseEvent::Start)));

    assert_eq!(again.wakes.load(Ordering::SeqCst), 1);
    assert_eq!(a.cloud.state(), CloudState::Idle);
}
