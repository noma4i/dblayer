use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use super::upload_tests::{blob, created, file, input, rid, small, upload_rig};
use crate::sync::locked;
use crate::sync::rig::Rig;
use crate::sync::Sync;
use crate::test_wait::{join_within, mutex_free, within, yield_until, ProbeWaker};
use crate::transport::{PutReply, PutTask, PutTaskState, RequestOwner};
use crate::upload::UploadState;

fn probe_for(rig: &Rig) -> Arc<ProbeWaker> {
    let inner = rig.sync.inner.clone();
    ProbeWaker::new(move || mutex_free(&inner.uploads) && inner.upload_effects.is_free())
}

fn poll<F: Future>(future: &mut Pin<Box<F>>, waker: Arc<impl Wake + Send + std::marker::Sync + 'static>) -> Poll<F::Output> {
    future.as_mut().poll(&mut Context::from_waker(&Waker::from(waker)))
}

fn task(tag: &str, state: PutTaskState) -> PutTask {
    PutTask { tag: tag.into(), state }
}

fn finished() -> PutTaskState {
    PutTaskState::Finished { status: 200, etag: Some("e".into()) }
}

struct Reenters {
    sync: Sync,
    wakes: AtomicUsize,
}

impl Wake for Reenters {
    fn wake(self: Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
        self.sync.put_tasks(vec![task("reentered", PutTaskState::Running)]);
        drop(locked(&self.sync.inner.uploads));
    }
}

async fn claimed(rig: &Rig) -> (i64, super::UploadResume, u64) {
    let source = file(b"abcdef");
    let seq = rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), "upload", &input(&source), UploadState::queued()).unwrap();
    let owner = RequestOwner { seq, item_position: 0, stage: "prepare".into() };
    let resume = rig.sync.inner.claim_upload_resume(&owner).await.unwrap().unwrap();
    let run = locked(&rig.sync.inner.uploads).active[&(seq, 0)].run;
    (seq, resume, run)
}

#[tokio::test]
async fn upload_finish_wakes_waiters_with_no_guards() {
    let rig = upload_rig(small());
    let inner = rig.sync.inner.clone();
    let (seq, resume, run) = claimed(&rig).await;
    let tag = format!("{seq}.0.1");
    rig.sync.put_tasks(vec![task(&tag, PutTaskState::Running)]);
    let probe = probe_for(&rig);
    let mut done = inner.running_uploads(seq, None).remove(0).1;
    let mut adopted = inner.adopted_put(&tag).expect("the adopted PUT is registered");
    let mut finishing = Box::pin(done.wait_for(|finished| *finished));
    let mut changing = Box::pin(adopted.changed());
    assert!(poll(&mut finishing, probe.clone()).is_pending());
    assert!(poll(&mut changing, probe.clone()).is_pending());
    drop(resume);
    assert_eq!((probe.wakes(), probe.violations()), (2, 0), "the done signal and the adopted sender drop wake with the uploads guard and the effect runner free");
    assert!(!inner.is_current(seq, 0, run), "the finished run is no longer current");
    rig.db.discard("moments", &rid("m1"), None).unwrap();
}

#[tokio::test]
async fn adopted_put_update_and_removal_wake_with_no_guards() {
    let rig = upload_rig(small());
    let inner = rig.sync.inner.clone();
    let tag = "7.0.1";
    rig.sync.put_tasks(vec![task(tag, PutTaskState::Running)]);
    let probe = probe_for(&rig);
    let mut adopted = inner.adopted_put(tag).unwrap();
    let mut changing = Box::pin(adopted.changed());
    assert!(poll(&mut changing, probe.clone()).is_pending());
    rig.sync.put_tasks(vec![task(tag, finished())]);
    assert_eq!((probe.wakes(), probe.violations()), (1, 0), "an update of an adopted PUT wakes its waiter with the uploads guard free");
    drop(changing);
    assert!(matches!(&*adopted.borrow_and_update(), PutTaskState::Finished { status: 200, .. }), "the waiter sees the update");
    let mut removed = Box::pin(adopted.changed());
    assert!(poll(&mut removed, probe.clone()).is_pending());
    inner.forget_put(tag);
    assert_eq!((probe.wakes(), probe.violations()), (2, 0), "dropping the adopted sender wakes its waiter with the uploads guard free");
    drop(removed);
    assert!(adopted.has_changed().is_err(), "the waiter sees the sender gone");
}

#[tokio::test]
async fn upload_wake_may_call_back_into_the_upload_map() {
    let rig = upload_rig(small());
    let (seq, resume, _) = claimed(&rig).await;
    let tag = format!("{seq}.0.1");
    rig.sync.put_tasks(vec![task(&tag, PutTaskState::Running)]);
    let mut done = rig.sync.inner.running_uploads(seq, None).remove(0).1;
    let mut adopted = rig.sync.inner.adopted_put(&tag).unwrap();
    let reenter = Arc::new(Reenters { sync: rig.sync.clone(), wakes: AtomicUsize::new(0) });
    let mut finishing = Box::pin(done.wait_for(|finished| *finished));
    let mut changing = Box::pin(adopted.changed());
    assert!(poll(&mut finishing, reenter.clone()).is_pending());
    assert!(poll(&mut changing, reenter.clone()).is_pending());
    let handle = tokio::runtime::Handle::current();
    let thread = std::thread::spawn(move || {
        let _entered = handle.enter();
        drop(resume);
    });
    join_within(thread);
    assert_eq!(reenter.wakes.load(Ordering::SeqCst), 2, "both waiters woke");
    assert!(rig.sync.inner.adopted_put("reentered").is_some(), "the call made from inside the wake reached the map");
    rig.db.discard("moments", &rid("m1"), None).unwrap();
}

#[tokio::test(start_paused = true)]
async fn upload_cancel_and_finish_reenter() {
    let rig = upload_rig(small());
    let inner = rig.sync.inner.clone();
    rig.transport.enqueue("CreateBlob", blob().after(Duration::from_secs(60)));
    rig.transport.enqueue("CreateMoment", created("m9"));
    let source = file(b"abcdef");
    let id = rid("m1");
    let seq = rig.sync.upload("moments", &id, "upload", input(&source)).await.unwrap();
    yield_until("the run started its CreateBlob call", || rig.transport.call_count("CreateBlob") == 1).await;
    let tag = format!("{seq}.0.1");
    rig.sync.put_tasks(vec![task(&tag, PutTaskState::Running)]);
    tokio::time::advance(Duration::from_secs(60)).await;
    yield_until("the run waits for the adopted PUT", || locked(&inner.uploads).adopted.get(&tag).is_some_and(|sender| sender.receiver_count() > 0)).await;

    let probe = probe_for(&rig);
    let (token, _) = inner.running_uploads(seq, None).remove(0);
    let mut cancelled = Box::pin(token.cancelled());
    assert!(poll(&mut cancelled, probe.clone()).is_pending());
    let mut cancel = Box::pin(rig.sync.cancel_upload("moments", &id, "upload"));
    assert!(poll(&mut cancel, probe.clone()).is_pending(), "cancel waits for the running upload to finish");
    assert!(token.is_cancelled(), "cancel reached the token");
    rig.sync.put_tasks(vec![task(&tag, finished())]);

    let mut outcome = None;
    yield_until("the cancelled upload finished", || {
        if let Poll::Ready(result) = poll(&mut cancel, probe.clone()) { outcome = Some(result); }
        outcome.is_some()
    }).await;
    assert!(matches!(outcome, Some(Ok(true))), "cancel reports one cancellation");
    assert_eq!(probe.violations(), 0, "no wake of the token, the done signal or the adopted PUT ran under the uploads guard");
    assert!(probe.wakes() >= 2, "the token waiter and the cancel waiter both woke");
    within(rig.sync.wait_uploads()).await;
    assert!(rig.db.outbox().unwrap().is_empty(), "the cancelled upload left no operation");
    assert_eq!(rig.transport.call_count("CreateMoment"), 0, "the late adopted completion did not start the mutation");
    assert!(locked(&inner.uploads).active.is_empty());
}

#[tokio::test(start_paused = true)]
async fn discard_blob_cancels_the_run_with_the_map_free() {
    let rig = upload_rig(small());
    let inner = rig.sync.inner.clone();
    rig.transport.enqueue("CreateBlob", blob().after(Duration::from_secs(3600)));
    rig.transport.enqueue_put("https://s3/single", PutReply::etag("e"));
    let source = file(b"abcdef");
    let id = rig.sync.upload_blob("video_intro", "CreateBlob", input(&source)).await.unwrap();
    let seq: i64 = rig.db.shared.read(&[], |reader| Ok(reader.connection().query_row("SELECT seq FROM blob_upload WHERE id = ?1", [&id], |row| row.get(0))?)).unwrap();
    yield_until("the blob run is active", || !inner.running_uploads(seq, None).is_empty()).await;
    let probe = probe_for(&rig);
    let (token, _) = inner.running_uploads(seq, None).remove(0);
    let mut cancelled = Box::pin(token.cancelled());
    assert!(poll(&mut cancelled, probe.clone()).is_pending());
    within(rig.sync.discard_blob(&id)).await.unwrap();
    assert_eq!((probe.wakes(), probe.violations()), (1, 0), "discarding wakes the token waiter with the uploads guard free");
    assert!(locked(&inner.uploads).active.is_empty(), "the discarded blob's run ended");
}
