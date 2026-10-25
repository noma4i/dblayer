use std::time::Duration;

use super::*;
use super::upload_tests::{blob, created, file, input, rid, single_replies, small, upload_rig};
use crate::transport::PutReply;
use crate::upload::{UploadConfig, UploadStage};

#[tokio::test(start_paused = true)]
async fn a_final_mutation_does_not_hold_a_file_permit() {
    for transferred in [false, true] {
        let rig = upload_rig(small());
        rig.transport.enqueue("CreateBlob", blob());
        rig.transport.enqueue("CreateMoment", created("m9").after(Duration::from_secs(10)));
        let source = file(b"abcdef");
        let state = if transferred {
            UploadState { transferred: true, signed_id: Some("sid".into()), blob_id: Some("b1".into()), ..UploadState::queued() }
        } else { UploadState::queued() };
        let seq = rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), &input(&source), state).unwrap();
        let permits = locked(&rig.sync.inner.uploads).permits.clone();
        let held = if transferred { Some(permits.acquire_owned().await.unwrap()) } else { None };
        rig.sync.inner.spawn_run(seq, 0, Mode::Transfer);
        rig.transport.await_calls("CreateMoment", 1).await;
        assert_eq!(rig.transport.call_count("CreateMoment"), 1, "transferred={transferred}");
        drop(held);
        assert_eq!(locked(&rig.sync.inner.uploads).permits.available_permits(), 1);
        rig.sync.wait_uploads().await;
    }
}

#[tokio::test(start_paused = true)]
async fn only_one_attempt_can_send_and_settle_the_same_upload() {
    let rig = upload_rig(UploadConfig { max_files: 2, ..small() });
    rig.transport.enqueue("CreateMoment", created("m9").after(Duration::from_secs(10)));
    let source = file(b"abcdef");
    let state = UploadState { transferred: true, signed_id: Some("sid".into()), blob_id: Some("b1".into()), ..UploadState::queued() };
    let seq = rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), &input(&source), state).unwrap();
    let (run, token, permits) = rig.sync.inner.register(seq, 0).unwrap();
    tokio::join!(
        super::run::drive(rig.sync.inner.clone(), seq, 0, run, token.clone(), permits.clone(), Mode::Transfer, None),
        super::run::drive(rig.sync.inner.clone(), seq, 0, run, token, permits, Mode::Transfer, None)
    );
    assert_eq!(rig.transport.call_count("CreateMoment"), 1);
    assert!(rig.db.outbox().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn files_of_one_operation_share_the_file_limit() {
    let rig = upload_rig(UploadConfig { max_files: 2, ..small() });
    single_replies(&rig);
    rig.transport.enqueue_put("https://s3/single", PutReply::etag("e").after(Duration::from_secs(10)));
    let source = file(b"abcdef");
    let seq = rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), &input(&source), UploadState::queued()).unwrap();
    {
        let stored = rig.db.shared.store.lock();
        let connection = &stored.connection;
        crate::operation::write_upload(connection, seq, 1, &UploadState::queued()).unwrap();
        connection.execute("UPDATE op_item SET file_id = (SELECT file_id FROM op_item WHERE seq = ?1 AND position = 0) WHERE seq = ?1 AND position = 1", [seq]).unwrap();
        let input = crate::operation::read_item_value(connection, seq, 0, "input").unwrap();
        crate::operation::write_item_value(connection, seq, 1, "input", input.as_ref()).unwrap();
    }
    rig.sync.flush().await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.put_calls().len(), 2);
    assert_eq!(rig.transport.parallel_puts_peak(), 2);
    assert_eq!(rig.transport.call_count("CreateMoment"), 1);
    assert!(rig.db.outbox().unwrap().is_empty());
}


#[tokio::test(start_paused = true)]
async fn a_saved_single_grant_is_put_before_the_mutation_without_creating_another_blob() {
    let rig = upload_rig(small());
    let source = file(b"abcdef");
    let state = UploadState {
        signed_id: Some("sid".into()), blob_id: Some("blob".into()), put_url: Some("https://s3/saved".into()),
        headers: vec![("x-stored".into(), "yes".into())], total_bytes: 6, stage: UploadStage::Put,
        ..UploadState::queued()
    };
    rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), &input(&source), state).unwrap();
    rig.transport.enqueue("CreateMoment", upload_tests::created("m9"));
    rig.sync.flush().await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.put_calls().len(), 1);
    assert_eq!(rig.transport.put_calls()[0].url, "https://s3/saved");
    assert_eq!(rig.transport.call_count("CreateBlob"), 0);
    assert_eq!(rig.transport.call_count("CreateMoment"), 1);
}


#[tokio::test(start_paused = true)]
async fn an_unwritable_owned_file_is_failed_without_restarting_the_runner() {
    let rig = upload_rig(small());
    let source = file(b"abcdef");
    let seq = rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), &input(&source), crate::upload::UploadState::queued()).unwrap();
    let id = rig.db.shared.read(&[], |reader| {
        Ok(reader.connection().query_row("SELECT file_id FROM op_item WHERE seq = ?1 AND position = 0", [seq], |row| row.get::<_, String>(0))?)
    }).unwrap();
    let cached = rig.db.shared.files.materialize(&id, "clip.mp4", b"abcdef").unwrap();
    let parent = cached.parent().unwrap();
    std::fs::remove_dir_all(parent).unwrap();
    std::fs::write(parent, b"obstacle").unwrap();
    rig.sync.inner.start_uploads().unwrap();
    let (finished, ()) = tokio::join!(
        tokio::time::timeout(Duration::from_secs(1), rig.sync.wait_uploads()),
        async { tokio::task::yield_now().await; tokio::time::advance(Duration::from_secs(1)).await; }
    );
    assert!(finished.is_ok(), "the source error restarted the runner instead of failing the operation");
    assert_eq!(rig.db.upload_info("moments", &rid("m1")).unwrap().unwrap().status, crate::upload::UploadStatus::Failed);
    assert!(rig.transport.calls().is_empty());
}
