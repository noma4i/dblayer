use std::time::Duration;

use super::*;
use super::upload_tests::{blob, created, file, input, rid, single_replies, small, upload_rig};
use crate::transport::PutReply;
use crate::upload::{UploadConfig, UploadStage, UploadStatus};

#[tokio::test]
async fn inv_upload_18_materialization_and_cleanup_leave_the_async_executor() {
    let rig = upload_rig(small());
    let source = file(b"inline");
    let seq = rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), "upload", &input(&source), UploadState::queued()).unwrap();
    let id = rig.db.shared.read(&[], |reader| Ok(reader.connection().query_row("SELECT file_id FROM op_item WHERE seq = ?1", [seq], |row| row.get::<_, String>(0))?)).unwrap();
    crate::files::watch_file_io(&id);
    rig.transport.enqueue("CreateBlob", blob());
    rig.transport.enqueue("CreateMoment", created("m1"));
    let executor = std::thread::current().id();
    rig.sync.flush().await.unwrap();
    rig.sync.wait_uploads().await;
    let events = crate::files::take_file_io(&id);
    assert_eq!(events.iter().map(|(operation, _)| *operation).collect::<Vec<_>>(), ["materialize", "discard"]);
    assert!(events.iter().all(|(_, thread)| *thread != executor), "file IO used executor {executor:?}: {events:?}");
    assert_eq!(rig.transport.put_calls()[0].body, b"inline");
    assert!(rig.db.shared.files.cache_entries().unwrap().is_empty());
    assert!(rig.db.outbox().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn queued_files_start_in_order_without_waiting_tasks() {
    let rig = upload_rig(small());
    rig.sync.set_online(false);
    let source = file(b"abcdef");
    let seq = rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), "upload", &input(&source), UploadState::queued()).unwrap();
    {
        let stored = rig.db.shared.store.lock();
        let connection = &stored.connection;
        let input = crate::operation::read_item_value(connection, seq, 0, "input").unwrap();
        for position in 1..4 {
            crate::operation::write_upload(connection, seq, position, &UploadState::queued()).unwrap();
            connection.execute("UPDATE op_item SET file_id = (SELECT file_id FROM op_item WHERE seq = ?1 AND position = 0) WHERE seq = ?1 AND position = ?2", rusqlite::params![seq, position]).unwrap();
            crate::operation::write_item_value(connection, seq, position, "input", input.as_ref()).unwrap();
        }
    }
    rig.sync.inner.start_uploads().unwrap();
    let running: Vec<_> = locked(&rig.sync.inner.uploads).active.keys().copied().collect();
    assert_eq!(running, vec![(seq, 0)], "only the first file may start before a file slot is free");
    rig.transport.respond_with(|request| match request.operation_name.as_str() {
        "CreateBlob" => Some(blob()),
        "CreateMoment" => Some(created("m9")),
        _ => None,
    });
    rig.sync.set_online(true);
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.put_calls().len(), 4);
    assert_eq!(rig.transport.parallel_puts_peak(), 1);
    assert_eq!(rig.transport.call_count("CreateMoment"), 1);
}

#[test]
fn cancellation_does_not_wait_for_queued_file_io() {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().max_blocking_threads(1).build().unwrap();
    runtime.block_on(async {
        let rig = upload_rig(small());
        let source = file(b"abcdef");
        let seq = rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), "upload", &input(&source), UploadState::queued()).unwrap();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let occupied = tokio::task::spawn_blocking(move || {
            entered_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        });
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let mut changes = rig.db.outbox_changes();
        rig.sync.inner.spawn_run(seq, 0, RunMode::Transfer).unwrap();
        crate::sync::rig::await_outbox(&mut changes, || rig.db.sync_info("moments", &rid("m1")).unwrap().uploads.get("upload").cloned().is_some_and(|upload| upload.status == UploadStatus::Preparing)).await;
        let cancelled = tokio::time::timeout(Duration::from_secs(1), rig.sync.cancel_upload("moments", &rid("m1"), "upload")).await;
        release_tx.send(()).unwrap();
        occupied.await.unwrap();
        rig.sync.wait_uploads().await;
        assert!(matches!(cancelled, Ok(Ok(true))), "cancellation waited for file IO: {cancelled:?}");
        assert!(rig.transport.calls().is_empty());
        assert!(rig.transport.put_calls().is_empty());
    });
}

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
        let seq = rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), "upload", &input(&source), state).unwrap();
        if transferred { locked(&rig.sync.inner.uploads).active_files = 1; }
        rig.sync.inner.spawn_run(seq, 0, RunMode::Transfer).unwrap();
        rig.transport.await_calls("CreateMoment", 1).await;
        assert_eq!(rig.transport.call_count("CreateMoment"), 1, "transferred={transferred}");
        assert_eq!(locked(&rig.sync.inner.uploads).active_files, usize::from(transferred));
        rig.sync.wait_uploads().await;
    }
}

#[tokio::test(start_paused = true)]
async fn only_one_attempt_can_send_and_settle_the_same_upload() {
    let rig = upload_rig(UploadConfig { max_files: 2, ..small() });
    rig.transport.enqueue("CreateMoment", created("m9").after(Duration::from_secs(10)));
    let source = file(b"abcdef");
    let state = UploadState { transferred: true, signed_id: Some("sid".into()), blob_id: Some("b1".into()), ..UploadState::queued() };
    let seq = rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), "upload", &input(&source), state).unwrap();
    let (run, token) = rig.sync.inner.register(seq, 0).unwrap();
    tokio::join!(
        super::run::drive(rig.sync.inner.clone(), seq, 0, run, token.clone(), RunMode::Transfer, None),
        super::run::drive(rig.sync.inner.clone(), seq, 0, run, token, RunMode::Transfer, None)
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
    let seq = rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), "upload", &input(&source), UploadState::queued()).unwrap();
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
    rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), "upload", &input(&source), state).unwrap();
    rig.transport.enqueue("CreateMoment", upload_tests::created("m9"));
    rig.sync.flush().await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.put_calls().len(), 1);
    assert_eq!(rig.transport.put_calls()[0].url, "https://s3/saved");
    assert_eq!(rig.transport.call_count("CreateBlob"), 0);
    assert_eq!(rig.transport.call_count("CreateMoment"), 1);
}


#[tokio::test]
async fn an_unwritable_owned_file_is_failed_without_restarting_the_runner() {
    let rig = upload_rig(small());
    let source = file(b"abcdef");
    let seq = rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), "upload", &input(&source), crate::upload::UploadState::queued()).unwrap();
    let id = rig.db.shared.read(&[], |reader| {
        Ok(reader.connection().query_row("SELECT file_id FROM op_item WHERE seq = ?1 AND position = 0", [seq], |row| row.get::<_, String>(0))?)
    }).unwrap();
    let cached = rig.db.shared.files.materialize(&id, "clip.mp4", b"abcdef").unwrap();
    let parent = cached.parent().unwrap();
    std::fs::remove_dir_all(parent).unwrap();
    std::fs::write(parent, b"obstacle").unwrap();
    rig.sync.inner.start_uploads().unwrap();
    let finished = tokio::time::timeout(Duration::from_secs(5), rig.sync.wait_uploads()).await;
    assert!(finished.is_ok(), "the source error restarted the runner instead of failing the operation");
    assert_eq!(rig.db.sync_info("moments", &rid("m1")).unwrap().uploads.get("upload").cloned().unwrap().status, crate::upload::UploadStatus::Failed);
    assert!(rig.transport.calls().is_empty());
}

#[test]
fn a_runner_dropped_outside_its_runtime_removes_its_cache_file_on_the_blocking_pool() {
    let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(1).enable_all().build().unwrap();
    let caller = std::thread::current().id();
    let (rig, runner, id, path) = runtime.block_on(async {
        let rig = upload_rig(small());
        let source = file(b"inline");
        let seq = rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), "upload", &input(&source), UploadState::queued()).unwrap();
        let id = rig.db.shared.read(&[], |reader| Ok(reader.connection().query_row("SELECT file_id FROM op_item WHERE seq = ?1", [seq], |row| row.get::<_, String>(0))?)).unwrap();
        crate::files::watch_file_io(&id);
        let runner = std::sync::Arc::new(super::run::Runner::load(rig.sync.inner.clone(), seq, 0, 0, crate::transport::CancelToken::new(), true).unwrap().unwrap());
        assert!(runner.materialize().await.is_ok());
        let path = std::path::PathBuf::from(&runner.input.path);
        assert!(path.exists());
        let generation = rig.db.generation();
        rig.sync.inner.commit(generation, |_| {
            let mut envelope = crate::commit::CommitEnvelope::new(crate::commit::WriteOrigin::Response, generation);
            envelope.operation_transitions.push(crate::commit::OperationTransition::RemoveUploadItem { seq, position: 0, inputs: Vec::new() });
            Ok(Some(envelope))
        }).unwrap();
        (rig, runner, id, path)
    });
    drop(runner);
    runtime.block_on(rig.sync.wait_uploads());
    let events = crate::files::take_file_io(&id);
    assert_eq!(events.iter().map(|(operation, _)| *operation).collect::<Vec<_>>(), ["materialize", "discard"]);
    assert!(events.iter().all(|(_, thread)| *thread != caller), "file IO used the dropping thread: {events:?}");
    assert!(!path.exists());
}

#[test]
fn queued_cleanup_survives_dropping_the_sync_that_queued_it() {
    let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(1).max_blocking_threads(1).enable_all().build().unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let root = tempfile::tempdir().unwrap();
    let (rig, runner, path) = runtime.block_on(async {
        let db = crate::db::Db::open(&root.path().join("42.sqlite"), crate::sync::rig::specs(), None).unwrap();
        let rig = crate::sync::rig::rig_on(db, crate::transport::MockTransport::new(), crate::sync::SyncConfig { upload: small(), ..crate::sync::SyncConfig::default() });
        crate::sync::rig::put(&rig, "moments", crate::sync::rig::moment("m1", "x"));
        let source = file(b"inline");
        let seq = rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), "upload", &input(&source), UploadState::queued()).unwrap();
        let runner = std::sync::Arc::new(super::run::Runner::load(rig.sync.inner.clone(), seq, 0, 0, crate::transport::CancelToken::new(), true).unwrap().unwrap());
        assert!(runner.materialize().await.is_ok());
        let path = std::path::PathBuf::from(&runner.input.path);
        let generation = rig.db.generation();
        rig.sync.inner.commit(generation, |_| {
            let mut envelope = crate::commit::CommitEnvelope::new(crate::commit::WriteOrigin::Response, generation);
            envelope.operation_transitions.push(crate::commit::OperationTransition::RemoveUploadItem { seq, position: 0, inputs: Vec::new() });
            Ok(Some(envelope))
        }).unwrap();
        drop(tokio::task::spawn_blocking(move || { entered_tx.send(()).unwrap(); release_rx.recv_timeout(Duration::from_secs(10)).unwrap(); }));
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        (rig, runner, path)
    });
    assert!(path.exists());
    drop(runner);
    let inner = std::sync::Arc::downgrade(&rig.sync.inner);
    drop(rig);
    assert!(inner.upgrade().is_none(), "the sync stayed alive");
    release_tx.send(()).unwrap();
    for _ in 0..5000 {
        if !path.exists() { break; }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(!path.exists(), "the queued cleanup was lost with the sync");
}
