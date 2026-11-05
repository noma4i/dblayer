use std::time::Duration;

use super::super::rig::*;
use super::upload_tests::{blob, created, file, input, rid, single_replies, small, upload_rig};
use crate::transport::PutReply;

#[test]
fn inv_fresh_03_a_step_answered_after_reset_all_sends_nothing_and_leaves_nothing() { crate::test_wait::paused(async {
    let rig = upload_rig(small());
    rig.transport.enqueue("CreateBlob", blob().after(Duration::from_secs(5)));
    let source = file(b"abcdef");
    crate::sync::rig::app(&rig.sync, rig.sync.upload(&rig.sync.inner.db.admit().unwrap(), "moments", &rid("m1"), "upload", input(&source))).await.unwrap();
    rig.transport.await_calls("CreateBlob", 1).await;
    rig.db.reset_all().unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!((rig.transport.put_calls().len(), rig.transport.call_count("CreateMoment")), (0, 0));
    assert!(rig.db.outbox().unwrap().is_empty());
    assert_eq!(rig.db.sync_info("moments", &rid("m1")).unwrap().uploads.get("upload").cloned(), None);
}) }

#[test]
fn inv_fresh_03_the_mutation_answered_after_reset_all_does_not_bring_the_row_back() { crate::test_wait::paused(async {
    let rig = upload_rig(small());
    rig.transport.enqueue("CreateBlob", blob());
    rig.transport.enqueue("CreateMoment", created("m9").after(Duration::from_secs(5)));
    let source = file(b"abcdef");
    crate::sync::rig::app(&rig.sync, rig.sync.upload(&rig.sync.inner.db.admit().unwrap(), "moments", &rid("m1"), "upload", input(&source))).await.unwrap();
    crate::test_wait::pass(Duration::from_secs(1)).await;
    assert_eq!(rig.transport.call_count("CreateMoment"), 1);
    rig.db.reset_all().unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(text_of(&rig, "moments", "m9"), None);
    assert!(rig.db.outbox().unwrap().is_empty());
}) }

#[test]
fn inv_upload_04_a_transfer_in_flight_during_a_reset_does_not_send_its_mutation_or_bring_back_rows() { crate::test_wait::paused(async {
    let rig = upload_rig(small());
    single_replies(&rig);
    rig.transport.enqueue_put("https://s3/single", PutReply::etag("e").after(Duration::from_secs(4)));
    let source = file(b"abcdef");
    crate::sync::rig::app(&rig.sync, rig.sync.upload(&rig.sync.inner.db.admit().unwrap(), "moments", &rid("m1"), "upload", input(&source))).await.unwrap();
    rig.transport.await_put_calls(1).await;
    rig.db.reset(&["moments"]).unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.call_count("CreateMoment"), 0);
    assert!(rig.db.outbox().unwrap().is_empty());
    assert_eq!(rig.db.count(&crate::query::Query::new("moments")).unwrap(), 0);
}) }

#[test]
fn ending_the_session_cancels_the_put_in_flight_and_frees_the_upload_slot() { crate::test_wait::paused(async {
    let rig = upload_rig(small());
    single_replies(&rig);
    rig.transport.enqueue_put("https://s3/single", PutReply::etag("e").after(Duration::from_secs(4)));
    let source = file(b"abcdef");
    crate::sync::rig::app(&rig.sync, rig.sync.upload(&rig.sync.inner.db.admit().unwrap(), "moments", &rid("m1"), "upload", input(&source))).await.unwrap();
    rig.transport.await_put_calls(1).await;
    rig.sync.lose_session_for_test();
    crate::test_wait::yield_until("the run left its slot", || crate::sync::locked(&rig.sync.inner.uploads).active.is_empty()).await;
    crate::test_wait::pass(Duration::from_secs(10)).await;
    assert_eq!(rig.transport.call_count("CreateMoment"), 0, "the answer of the ended session sent the mutation");
    assert_eq!(rig.db.outbox().unwrap().len(), 1, "the pending upload operation was dropped instead of kept for the next session");
}) }

#[test]
fn ending_the_session_keeps_the_queued_upload_for_the_next_session() { crate::test_wait::paused(async {
    let rig = upload_rig(crate::upload::UploadConfig { max_files: 1, ..small() });
    for id in ["m9", "m10"] {
        rig.transport.enqueue("CreateBlob", blob());
        rig.transport.enqueue("CreateMoment", created(id));
    }
    rig.transport.enqueue_put("https://s3/single", PutReply::etag("e").after(Duration::from_secs(4)));
    rig.transport.enqueue_put("https://s3/single", PutReply::etag("e"));
    rig.transport.enqueue_put("https://s3/single", PutReply::etag("e"));
    let (first, second) = (file(b"abcdef"), file(b"ghijkl"));
    crate::sync::rig::app(&rig.sync, rig.sync.upload(&rig.sync.inner.db.admit().unwrap(), "moments", &rid("m1"), "upload", input(&first))).await.unwrap();
    crate::sync::rig::app(&rig.sync, rig.sync.upload(&rig.sync.inner.db.admit().unwrap(), "moments", &rid("m2"), "upload", input(&second))).await.unwrap();
    rig.transport.await_put_calls(1).await;
    assert_eq!(crate::sync::locked(&rig.sync.inner.uploads).ready.len(), 1, "the second file waits behind the file limit");
    rig.sync.lose_session_for_test();
    crate::test_wait::yield_until("the run left its slot", || crate::sync::locked(&rig.sync.inner.uploads).active.is_empty()).await;
    assert_eq!(rig.db.outbox().unwrap().len(), 2, "an operation was dropped with the session");
    rig.transport.set_session(Some("second"));
    rig.sync.commit_session_for_test();
    crate::test_wait::pass(Duration::from_secs(30)).await;
    assert_eq!(rig.transport.call_count("CreateMoment"), 2, "an upload queued at the end of the session never started again");
    assert!(rig.db.outbox().unwrap().is_empty());
}) }

#[test]
fn ending_the_session_walks_only_the_uploads_running_now() { crate::test_wait::paused(async {
    let rig = upload_rig(small());
    let mut runs = crate::sync::locked(&rig.sync.inner.uploads);
    for seq in 0..2000 { runs.register(seq, 0, false); }
    runs.active.retain(|key, _| key.0 == 7);
    let (tokens, restart) = runs.end_all();
    assert_eq!((tokens.len(), restart.len(), runs.walked()), (1, 1, 1), "the walk visited more than the uploads running now");
}) }
