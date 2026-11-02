use std::sync::mpsc;
use std::time::Duration;

use crate::commit::{CommitEnvelope, WriteOrigin};
use crate::sync::rig::rig;
use crate::sync::SyncError;
use crate::test_wait::{join_within, recv_within, within_secs};

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn busy_store_does_not_stop_async_progress() {
    let rig = rig();
    let inner = rig.sync.inner.clone();
    let generation = inner.db.generation();
    let shared = rig.db.shared.clone();
    let (held, writer_held) = mpsc::channel();
    let (release, released) = mpsc::channel::<()>();
    let holder = std::thread::spawn(move || {
        let writer = shared.store.lock();
        held.send(()).unwrap();
        let _ = released.recv_timeout(Duration::from_secs(20));
        drop(writer);
    });
    recv_within(&writer_held);
    let (started, running) = mpsc::channel();
    let committing = tokio::spawn({
        let inner = inner.clone();
        async move {
            started.send(()).unwrap();
            inner.commit(generation, |_| Ok::<_, SyncError>(Some(CommitEnvelope::new(WriteOrigin::Response, generation))))
        }
    });
    recv_within(&running);
    let (signal, signalled) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let _ = signal.send(());
    });
    within_secs(10, signalled).await.expect("the control task finished");
    assert!(!committing.is_finished(), "the commit still waits for the writer");
    release.send(()).unwrap();
    within_secs(10, committing).await.expect("the commit task joined").expect("the commit returned");
    join_within(holder);
}
