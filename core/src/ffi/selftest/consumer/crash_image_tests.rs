//! C6: crash and upgrade injection. A crash image is the database file with its write-ahead log copied while the app runs (the state a killed process leaves). After each step of a real scenario (sign in, three upload batches) the image is opened by a fresh core: the durable intents (operations, rows with an upload) equal the live set and the session survives. Then the format version of the file is changed to an older one and the file is reopened, and an integer column is corrupted and the open must answer an error, not panic.
use std::path::Path;
use std::sync::Arc;

use serde_json::json;

use super::stand::Stand;
use crate::ffi::selftest::rig_tests::bound;
use crate::ffi::tests::session_guards_tests::WatchedHeld;
use crate::ffi::upload_wire::{WireUploadBatch, WireUploadItem};
use crate::ffi::{CredentialStore, ReadRequest, ReadValue};
use crate::test_matrix::Matrix;

async fn operations(stand: &Stand) -> Vec<(i64, String, String)> {
    let epoch = stand.database.session_epoch();
    let stream = stand.database.subscribe_values(epoch, ReadRequest::Operations);
    let first = bound(stream.pull_next()).await;
    match first {
        Some(ReadValue::Operations { items, .. }) => items.into_iter().map(|item| (item.seq, item.row_id, item.action)).collect(),
        other => panic!("operations snapshot missing: {other:?}"),
    }
}

fn copy_image(from: &Path, to: &Path) {
    for suffix in ["", "-wal"] {
        let (source, target) = (format!("{}{suffix}", from.display()), format!("{}{suffix}", to.display()));
        if Path::new(&source).exists() { std::fs::copy(source, target).unwrap(); }
    }
}

fn copy_files(from: &Path, to: &Path) {
    if !from.exists() { return; }
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_dir() { copy_files(&entry.path(), &to.join(entry.file_name())); }
        else { std::fs::copy(entry.path(), to.join(entry.file_name())).unwrap(); }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_too_large_answer_consistent_at_every_commit() {
    use super::stand::chat;
    use crate::ffi::{DbError, ReadFetch};
    use rusqlite::OptionalExtension;

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("live.sqlite");
    let base = directory.path().join("base.sqlite");
    let credentials = Arc::new(WatchedHeld::default());
    let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
    stand.sign_in().await;
    stand.answer("ChatNode", json!({"node": chat("c1", "before")}));
    let rows = stand.entity("Chat", "c1", ReadFetch::Stale);
    crate::test_wait::settled(&stand, "the chat is loaded", || stand.text_of(&rows, "Chat", "c1", "name").as_deref() == Some("before")).await;
    stand.database.set_online(false).unwrap();
    let receipt = bound(stand.database.act(stand.database.session_epoch(), "Chat".into(), "c1".into(), "pin".into(), "{}".into(), json!({"pin": true}).to_string())).await.unwrap();
    let operation = receipt.operation.unwrap();
    stand.settle().await;
    let connection = rusqlite::Connection::open(&path).unwrap();
    let checkpoint: (i64, i64, i64) = connection.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).unwrap();
    assert_eq!(checkpoint.0, 0);
    drop(connection);
    let answered = stand.answered_count();
    stand.fail_next("ChatPin", DbError::ResponseTooLarge { limit: 1024 });
    stand.database.set_online(true).unwrap();
    stand.answered(answered + 1).await;
    stand.settle().await;
    let connection = rusqlite::Connection::open(&path).unwrap();
    let pair: (String, i64) = connection.query_row("SELECT request_id, sequence FROM encrypted_pending", [], |row| Ok((row.get(0)?, row.get(1)?))).unwrap();
    drop(connection);
    copy_image(&path, &base);
    let wal = std::fs::read(format!("{}-wal", base.display())).unwrap();
    let page_size = u32::from_be_bytes(wal[8..12].try_into().unwrap()) as usize;
    let frame_size = 24 + if page_size == 1 { 65536 } else { page_size };
    let commits: Vec<_> = (32..wal.len()).step_by(frame_size).filter(|offset| u32::from_be_bytes(wal[offset + 4..offset + 8].try_into().unwrap()) != 0).map(|offset| offset + frame_size).collect();
    let mut checked = 0;
    for (index, end) in commits.into_iter().enumerate() {
        let image = directory.path().join(format!("image-{index}.sqlite"));
        std::fs::copy(&base, &image).unwrap();
        std::fs::write(format!("{}-wal", image.display()), &wal[..end]).unwrap();
        let connection = rusqlite::Connection::open(&image).unwrap();
        let saved: Option<(String, i64)> = connection.query_row("SELECT request_id, sequence FROM encrypted_pending", [], |row| Ok((row.get(0)?, row.get(1)?))).optional().unwrap();
        drop(connection);
        let Some(saved) = saved else {
            assert_eq!(checked, 0, "WAL commit {index} lost the retained pair");
            continue;
        };
        checked += 1;
        assert_eq!(saved, pair, "WAL commit {index}");
        let reopened = Stand::prepare_online(Some(image.to_string_lossy().into_owned()), credentials.clone(), None, false).await;
        assert!(operations(&reopened).await.iter().any(|(seq, _, _)| *seq == operation), "WAL commit {index}");
        reopened.answer("ChatPin", json!({"chatPin": {"chat": chat("c1", "confirmed")}}));
        reopened.database.set_online(true).unwrap();
        reopened.clock.advance(60_000);
        crate::test_wait::settled(&reopened, "the retained pair is retried", || reopened.requests("ChatPin") == 1).await;
        reopened.settle().await;
        assert_eq!(reopened.sequenced().iter().filter(|(name, _)| name == "ChatPin").map(|(_, sequence)| *sequence).collect::<Vec<_>>(), [pair.1]);
        assert_eq!(reopened.request_ids().iter().filter(|(name, _)| name == "ChatPin").map(|(_, id)| id.as_str().unwrap()).collect::<Vec<_>>(), [pair.0.as_str()]);
        assert!(!operations(&reopened).await.iter().any(|(seq, _, _)| *seq == operation), "WAL commit {index}");
        bound(reopened.database.close_database()).await.unwrap();
    }
    assert!(checked > 0, "the images include the retained pair");
    stand.answer("ChatPin", json!({"chatPin": {"chat": chat("c1", "confirmed")}}));
    stand.clock.advance(60_000);
    bound(stand.database.confirmed(receipt, false)).await.unwrap();
    bound(stand.database.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_rejected_mutation_consistent_at_every_commit() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("live.sqlite");
    let base = directory.path().join("base.sqlite");
    let credentials = Arc::new(WatchedHeld::default());
    let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
    stand.sign_in().await;
    stand.database.set_online(false).unwrap();
    let epoch = stand.database.session_epoch();
    let values = json!({"uuid":"local-chat","kind":"GROUP","status":"PRIMARY","lists":["PRIMARY"],"premium":false,"pinned":false,"muted":false,"read":true,"serverRevision":1,"messagesCount":0,"membersCount":2,"lastActivityAt":"2026-11-08T00:00:00Z","createdAt":"2026-11-08T00:00:00Z","updatedAt":"2026-11-08T00:00:00Z","name":"group"});
    let receipt = bound(stand.database.create(epoch, "Chat".into(), values.to_string(), r#"{"userIds":[]}"#.into())).await.unwrap();
    let id = receipt.optimistic_value.as_ref().unwrap().id.clone();
    let operation = receipt.operation.unwrap();
    stand.settle().await;
    assert!(stand.database.peek_row(epoch, "Chat".into(), id.clone()).unwrap().is_some());
    let db = rusqlite::Connection::open(&path).unwrap();
    let checkpoint: (i64, i64, i64) = db.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).unwrap();
    assert_eq!(checkpoint.0, 0);
    drop(db);
    stand.answer_next("ChatGroupCreate", json!({"errors":[{"message":"refused","extensions":{"code":"FORBIDDEN"}}]}));
    stand.database.set_online(true).unwrap();
    let outcome = bound(stand.database.confirmed(receipt, false)).await;
    assert!(matches!(outcome, Err(crate::ffi::DbError::Rejected { kept: false, .. })), "{outcome:?}");
    stand.settle().await;
    copy_image(&path, &base);
    let wal = std::fs::read(format!("{}-wal", base.display())).unwrap();
    assert!(wal.len() >= 32);
    let page_size = u32::from_be_bytes(wal[8..12].try_into().unwrap()) as usize;
    let frame_size = 24 + if page_size == 1 { 65536 } else { page_size };
    assert!(page_size >= 512 && (wal.len() - 32) % frame_size == 0);
    let commits: Vec<_> = (32..wal.len()).step_by(frame_size).filter(|offset| u32::from_be_bytes(wal[offset + 4..offset + 8].try_into().unwrap()) != 0).map(|offset| offset + frame_size).collect();
    assert!(!commits.is_empty());
    for (k, end) in std::iter::once(32).chain(commits).enumerate() {
        let image = directory.path().join(format!("rejected-{k}.sqlite"));
        std::fs::copy(&base, &image).unwrap();
        std::fs::write(format!("{}-wal", image.display()), &wal[..end]).unwrap();
        let reopened = Stand::prepare_online(Some(image.to_string_lossy().into_owned()), credentials.clone(), None, false).await;
        reopened.settle().await;
        let pending = operations(&reopened).await.iter().any(|(seq, _, _)| *seq == operation);
        let row = reopened.database.peek_row(reopened.database.session_epoch(), "Chat".into(), id.clone()).unwrap().is_some();
        assert_eq!(pending, row, "WAL commit {k}: operation={pending}, optimistic row={row}");
        bound(reopened.database.close_database()).await.unwrap();
    }
    bound(stand.database.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_retry_upload_consistent_at_every_commit() {
    use crate::ffi::command::{CommandRequest, CommandUpload};
    use crate::ffi::upload_wire::{WireSource, WireUploadRequest};
    use crate::ffi::wire::WireUploadStatus;

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("live.sqlite");
    let base = directory.path().join("base.sqlite");
    let files = directory.path().join("before.files");
    let source = directory.path().join("source");
    std::fs::write(&source, b"image bytes").unwrap();
    let credentials = Arc::new(WatchedHeld::default());
    let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
    stand.sign_in().await;
    let epoch = stand.database.session_epoch();
    stand.answer_next("CreateBlob", json!({"errors":[{"message":"refused","extensions":{"code":"FORBIDDEN"}}]}));
    let receipt = bound(stand.database.send(CommandRequest {
        epoch, model: "Moment".into(), values_json: json!({"createdAt":"2026-11-08T00:00:00Z", "updatedAt":"2026-11-08T00:00:00Z"}).to_string(), args_json: "{}".into(),
        upload: Some(CommandUpload { action: "create_file".into(), request: WireUploadRequest { source: WireSource::File { path: source.to_string_lossy().into_owned(), name: None, mime: None }, variables_json: "{}".into() } }),
    })).await.unwrap();
    let id = receipt.optimistic_value.as_ref().unwrap().id.clone();
    let failed = bound(stand.database.confirmed(receipt, false)).await;
    assert!(matches!(failed, Err(crate::ffi::DbError::Rejected { kept: true, .. })), "{failed:?}");
    stand.database.set_online(false).unwrap();
    stand.settle().await;
    let db = rusqlite::Connection::open(&path).unwrap();
    let (transfer, cycle): (i64, i64) = db.query_row("SELECT transfer_id, cycle_id FROM sync_upload WHERE row_id = ?1 AND action = 'create_file'", [&id], |row| Ok((row.get(0)?, row.get(1)?))).unwrap();
    let checkpoint: (i64, i64, i64) = db.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).unwrap();
    assert_eq!(checkpoint.0, 0);
    drop(db);
    copy_files(Path::new(&format!("{}.files", path.display())), &files);
    bound(stand.database.retry(epoch, "Moment".into(), id.clone(), Some("create_file".into()), None)).await.unwrap();
    stand.settle().await;
    copy_image(&path, &base);
    let wal = std::fs::read(format!("{}-wal", base.display())).unwrap();
    assert!(wal.len() >= 32);
    let page_size = u32::from_be_bytes(wal[8..12].try_into().unwrap()) as usize;
    let frame_size = 24 + if page_size == 1 { 65536 } else { page_size };
    assert!(page_size >= 512 && (wal.len() - 32) % frame_size == 0);
    let commits: Vec<_> = (32..wal.len()).step_by(frame_size).filter(|offset| u32::from_be_bytes(wal[offset + 4..offset + 8].try_into().unwrap()) != 0).map(|offset| offset + frame_size).collect();
    assert!(!commits.is_empty());
    for (k, end) in std::iter::once(32).chain(commits).enumerate() {
        let image = directory.path().join(format!("retry-{k}.sqlite"));
        std::fs::copy(&base, &image).unwrap();
        std::fs::write(format!("{}-wal", image.display()), &wal[..end]).unwrap();
        copy_files(&files, Path::new(&format!("{}.files", image.display())));
        let reopened = Stand::prepare_online(Some(image.to_string_lossy().into_owned()), credentials.clone(), None, false).await;
        reopened.settle().await;
        let db = rusqlite::Connection::open(&image).unwrap();
        let uploads: i64 = db.query_row("SELECT count(*) FROM sync_upload WHERE row_id = ?1 AND action = 'create_file'", [&id], |row| row.get(0)).unwrap();
        assert_eq!(uploads, 1, "WAL commit {k}: duplicate or missing upload");
        let (stored_cycle, status, op): (i64, String, Option<i64>) = db.query_row("SELECT cycle_id, status, op_id FROM sync_upload WHERE transfer_id = ?1", [transfer], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).unwrap();
        let old: Option<String> = rusqlite::OptionalExtension::optional(db.query_row("SELECT kind FROM transfer_outcome WHERE transfer_id = ?1 AND cycle_id = ?2", rusqlite::params![transfer, cycle], |row| row.get(0))).unwrap();
        let active: i64 = db.query_row("SELECT count(*) FROM op WHERE seq = ?1", [op], |row| row.get(0)).unwrap();
        let row = reopened.database.peek_row(reopened.database.session_epoch(), "Moment".into(), id.clone()).unwrap().unwrap();
        let upload = row.uploads.iter().find(|entry| entry.action == "create_file").unwrap();
        if stored_cycle == cycle {
            assert_eq!(status, "failed", "WAL commit {k}");
            assert_eq!(upload.upload.status, WireUploadStatus::Failed, "WAL commit {k}");
        } else {
            assert_eq!(stored_cycle, cycle + 1, "WAL commit {k}");
            assert_eq!(old.as_deref(), Some("failed"), "WAL commit {k}: old outcome {old:?}");
            let new_outcomes: i64 = db.query_row("SELECT count(*) FROM transfer_outcome WHERE transfer_id = ?1 AND cycle_id = ?2", rusqlite::params![transfer, stored_cycle], |row| row.get(0)).unwrap();
            assert_eq!(new_outcomes, 0, "WAL commit {k}: new cycle already ended");
            assert_ne!(upload.upload.status, WireUploadStatus::Failed, "WAL commit {k}");
        }
        assert_eq!(active, 1, "WAL commit {k}: missing operation");
        drop(db);
        bound(reopened.database.close_database()).await.unwrap();
    }
    bound(stand.database.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_replace_failed_upload_consistent_at_every_commit() {
    use crate::ffi::command::{CommandRequest, CommandUpload};
    use crate::ffi::upload_wire::{WireSource, WireUploadRequest};
    use crate::ffi::wire::WireUploadStatus;

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("live.sqlite");
    let base = directory.path().join("base.sqlite");
    let before_files = directory.path().join("before.files");
    let source = directory.path().join("source");
    std::fs::write(&source, b"image bytes").unwrap();
    let request = || WireUploadRequest { source: WireSource::File { path: source.to_string_lossy().into_owned(), name: None, mime: None }, variables_json: "{}".into() };
    let credentials = Arc::new(WatchedHeld::default());
    let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
    stand.sign_in().await;
    let epoch = stand.database.session_epoch();
    stand.answer_next("CreateBlob", json!({"errors":[{"message":"refused","extensions":{"code":"FORBIDDEN"}}]}));
    let old_receipt = bound(stand.database.send(CommandRequest {
        epoch, model: "Moment".into(), values_json: json!({"createdAt":"2026-11-08T00:00:00Z", "updatedAt":"2026-11-08T00:00:00Z"}).to_string(), args_json: "{}".into(),
        upload: Some(CommandUpload { action: "create_file".into(), request: request() }),
    })).await.unwrap();
    let id = old_receipt.optimistic_value.as_ref().unwrap().id.clone();
    assert!(matches!(bound(stand.database.confirmed(old_receipt, false)).await, Err(crate::ffi::DbError::Rejected { kept: true, .. })));
    stand.database.set_online(false).unwrap();
    stand.settle().await;
    let db = rusqlite::Connection::open(&path).unwrap();
    let (old, cycle): (i64, i64) = db.query_row("SELECT transfer_id, cycle_id FROM sync_upload WHERE row_id = ?1 AND action = 'create_file'", [&id], |row| Ok((row.get(0)?, row.get(1)?))).unwrap();
    let checkpoint: (i64, i64, i64) = db.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).unwrap();
    assert_eq!(checkpoint.0, 0);
    drop(db);
    copy_files(Path::new(&format!("{}.files", path.display())), &before_files);
    assert!(before_files.join(format!("transfers/{old}")).exists());
    bound(stand.database.upload(epoch, "Moment".into(), id.clone(), "create_file".into(), request())).await.unwrap();
    stand.settle().await;
    copy_image(&path, &base);
    let wal = std::fs::read(format!("{}-wal", base.display())).unwrap();
    assert!(wal.len() >= 32);
    let page_size = u32::from_be_bytes(wal[8..12].try_into().unwrap()) as usize;
    let frame_size = 24 + if page_size == 1 { 65536 } else { page_size };
    assert!(page_size >= 512 && (wal.len() - 32) % frame_size == 0);
    let commits: Vec<_> = (32..wal.len()).step_by(frame_size).filter(|offset| u32::from_be_bytes(wal[offset + 4..offset + 8].try_into().unwrap()) != 0).map(|offset| offset + frame_size).collect();
    assert!(!commits.is_empty());
    for (k, end) in std::iter::once(32).chain(commits).enumerate() {
        let image = directory.path().join(format!("replace-{k}.sqlite"));
        std::fs::copy(&base, &image).unwrap();
        std::fs::write(format!("{}-wal", image.display()), &wal[..end]).unwrap();
        let image_files = std::path::PathBuf::from(format!("{}.files", image.display()));
        copy_files(&before_files, &image_files);
        copy_files(Path::new(&format!("{}.files", path.display())), &image_files);
        let reopened = Stand::prepare_online(Some(image.to_string_lossy().into_owned()), credentials.clone(), None, false).await;
        reopened.settle().await;
        let db = rusqlite::Connection::open(&image).unwrap();
        let count: i64 = db.query_row("SELECT count(*) FROM sync_upload WHERE row_id = ?1 AND action = 'create_file'", [&id], |row| row.get(0)).unwrap();
        assert_eq!(count, 1, "WAL commit {k}: duplicate or missing upload");
        let (current, op): (i64, Option<i64>) = db.query_row("SELECT transfer_id, op_id FROM sync_upload WHERE row_id = ?1 AND action = 'create_file'", [&id], |row| Ok((row.get(0)?, row.get(1)?))).unwrap();
        let operation: i64 = db.query_row("SELECT count(*) FROM op WHERE seq = ?1", [op], |row| row.get(0)).unwrap();
        assert_eq!(operation, 1, "WAL commit {k}: upload without operation");
        let row = reopened.database.peek_row(reopened.database.session_epoch(), "Moment".into(), id.clone()).unwrap().unwrap();
        let upload = row.uploads.iter().find(|entry| entry.action == "create_file").unwrap();
        let old_copy = image_files.join(format!("transfers/{old}")).exists();
        if current == old {
            assert_eq!(upload.upload.status, WireUploadStatus::Failed, "WAL commit {k}");
            assert!(old_copy, "WAL commit {k}: old copy missing");
        } else {
            let outcome: Option<String> = rusqlite::OptionalExtension::optional(db.query_row("SELECT kind FROM transfer_outcome WHERE transfer_id = ?1 AND cycle_id = ?2", rusqlite::params![old, cycle], |row| row.get(0))).unwrap();
            assert_eq!(outcome.as_deref(), Some("discarded"), "WAL commit {k}: old outcome");
            assert!(!old_copy, "WAL commit {k}: orphaned old copy");
        }
        drop(db);
        bound(reopened.database.close_database()).await.unwrap();
    }
    bound(stand.database.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_server_delete_pending_operation_consistent_at_every_commit() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("live.sqlite");
    let base = directory.path().join("base.sqlite");
    let credentials = Arc::new(WatchedHeld::default());
    let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
    stand.sign_in().await;
    let epoch = stand.database.session_epoch();
    stand.answer("chatThread", super::thread_pages_tests::page(1, 1, false, 1));
    bound(stand.database.refresh(epoch, "Message".into(), Some("thread".into()), r#"{"chatId":"c1"}"#.into())).await.unwrap();
    let (events, token) = stand.events("Message").await;
    stand.hold(&["MessageUpdate"]);
    let mark = stand.request_mark();
    let receipt = bound(stand.database.update(epoch, "Message".into(), "m1".into(), r#"{"body":"edited"}"#.into(), "{}".into())).await.unwrap();
    let operation = receipt.operation.unwrap();
    stand.receive_after(mark, "MessageUpdate", None, 1).await;
    let db = rusqlite::Connection::open(&path).unwrap();
    let checkpoint: (i64, i64, i64) = db.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).unwrap();
    assert_eq!(checkpoint.0, 0);
    drop(db);
    stand.push(&token, json!({"area":"MESSAGE", "kind":"DELETED", "entityId":"m1", "parentId":"c1"}));
    assert!(matches!(bound(events.pull_next()).await, Some(ReadValue::Event { ref id, .. }) if id == "m1"));
    let outcome = bound(stand.database.confirmed(receipt, false)).await;
    assert!(matches!(outcome, Err(crate::ffi::DbError::Interrupted { ref detail }) if detail.to_lowercase().contains("discard")), "{outcome:?}");
    stand.release(&["MessageUpdate"]);
    stand.settle().await;
    copy_image(&path, &base);
    let wal = std::fs::read(format!("{}-wal", base.display())).unwrap();
    assert!(wal.len() >= 32);
    let page_size = u32::from_be_bytes(wal[8..12].try_into().unwrap()) as usize;
    let frame_size = 24 + if page_size == 1 { 65536 } else { page_size };
    assert!(page_size >= 512 && (wal.len() - 32) % frame_size == 0);
    let commits: Vec<_> = (32..wal.len()).step_by(frame_size).filter(|offset| u32::from_be_bytes(wal[offset + 4..offset + 8].try_into().unwrap()) != 0).map(|offset| offset + frame_size).collect();
    assert!(!commits.is_empty());
    for (k, end) in std::iter::once(32).chain(commits).enumerate() {
        let image = directory.path().join(format!("deleted-{k}.sqlite"));
        std::fs::copy(&base, &image).unwrap();
        std::fs::write(format!("{}-wal", image.display()), &wal[..end]).unwrap();
        let reopened = Stand::prepare_online(Some(image.to_string_lossy().into_owned()), credentials.clone(), None, false).await;
        reopened.settle().await;
        let row = reopened.database.peek_row(reopened.database.session_epoch(), "Message".into(), "m1".into()).unwrap();
        let pending = operations(&reopened).await.iter().any(|(seq, _, _)| *seq == operation);
        assert!(row.is_some() || !pending, "WAL commit {k}: operation {operation} remains on the deleted row");
        bound(reopened.database.close_database()).await.unwrap();
    }
    bound(stand.database.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_confirmed_upload_batch_consistent_at_every_commit() {
    use crate::ffi::upload_wire::WireSource;

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("live.sqlite");
    let base = directory.path().join("base.sqlite");
    let files = directory.path().join("before.files");
    let credentials = Arc::new(WatchedHeld::default());
    let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
    stand.sign_in().await;
    stand.answer("CreateBlob", json!({"directUploadCreate":{"directUpload":{"url":"https://storage/file","headers":[],"signedId":"signed","blobId":"blob"}}}));
    stand.script_put(200, Some("etag"));
    stand.script_put(200, Some("etag"));
    let messages: Vec<_> = (0..2).map(|index| {
        let mut message = super::stand::message(&format!("batch-server-{index}"), "c1", "");
        message["clientKey"] = json!(format!("batch-{index}"));
        message
    }).collect();
    stand.answer("MessageAlbumSend", json!({"messageAlbumSend":{"messages":messages}}));
    stand.hold(&["MessageAlbumSend"]);
    let mark = stand.request_mark();
    let items = (0..2).map(|index| {
        let source = directory.path().join(format!("source-{index}"));
        std::fs::write(&source, format!("bytes-{index}")).unwrap();
        WireUploadItem {
            values_json: json!({"chatId":"c1","kind":"TEXT","clientKey":format!("batch-{index}"),"createdAt":"2026-11-08T00:00:00Z","updatedAt":"2026-11-08T00:00:00Z","mediaGroup":{"id":"batch","position":index,"size":2}}).to_string(),
            source: WireSource::File { path: source.to_string_lossy().into_owned(), name: None, mime: None }, variables_json: "{}".into(),
        }
    }).collect();
    let receipt = bound(stand.database.upload_create_batch(stand.database.session_epoch(), "Message".into(), items, WireUploadBatch { operation: "send_files".into(), variables_json: r#"{"chatId":"c1"}"#.into() })).await.unwrap();
    stand.receive_after(mark, "MessageAlbumSend", None, 1).await;
    let db = rusqlite::Connection::open(&path).unwrap();
    let transfers: Vec<(i64, i64)> = receipt.items.iter().map(|item| db.query_row("SELECT transfer_id, cycle_id FROM sync_upload WHERE model = 'Message' AND row_id = ?1 AND action = 'send_files'", [&item.optimistic_value.id], |row| Ok((row.get(0)?, row.get(1)?))).unwrap()).collect();
    assert_eq!(transfers.len(), 2);
    let checkpoint: (i64, i64, i64) = db.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).unwrap();
    assert_eq!(checkpoint.0, 0);
    drop(db);
    copy_files(Path::new(&format!("{}.files", path.display())), &files);
    stand.release(&["MessageAlbumSend"]);
    for item in receipt.items { assert!(bound(stand.database.confirmed(item.receipt, false)).await.is_ok()); }
    stand.settle().await;
    copy_image(&path, &base);
    let wal = std::fs::read(format!("{}-wal", base.display())).unwrap();
    assert!(wal.len() >= 32);
    let page_size = u32::from_be_bytes(wal[8..12].try_into().unwrap()) as usize;
    let frame_size = 24 + if page_size == 1 { 65536 } else { page_size };
    assert!(page_size >= 512 && (wal.len() - 32) % frame_size == 0);
    let commits: Vec<_> = (32..wal.len()).step_by(frame_size).filter(|offset| u32::from_be_bytes(wal[offset + 4..offset + 8].try_into().unwrap()) != 0).map(|offset| offset + frame_size).collect();
    assert!(!commits.is_empty());
    for (k, end) in std::iter::once(32).chain(commits).enumerate() {
        let image = directory.path().join(format!("batch-{k}.sqlite"));
        std::fs::copy(&base, &image).unwrap();
        std::fs::write(format!("{}-wal", image.display()), &wal[..end]).unwrap();
        copy_files(&files, Path::new(&format!("{}.files", image.display())));
        let reopened = Stand::prepare_online(Some(image.to_string_lossy().into_owned()), credentials.clone(), None, false).await;
        reopened.settle().await;
        let db = rusqlite::Connection::open(&image).unwrap();
        let mut present = 0;
        let mut confirmed = 0;
        for (id, cycle) in &transfers {
            present += db.query_row("SELECT count(*) FROM sync_upload WHERE transfer_id = ?1", [id], |row| row.get::<_, i64>(0)).unwrap();
            confirmed += db.query_row("SELECT count(*) FROM transfer_outcome WHERE transfer_id = ?1 AND cycle_id = ?2 AND kind = 'confirmed'", rusqlite::params![id, cycle], |row| row.get::<_, i64>(0)).unwrap();
        }
        assert!(present == 0 || present == 2, "WAL commit {k}: {present} of 2 uploads remain");
        if present == 0 {
            assert_eq!(confirmed, 2, "WAL commit {k}: confirmed outcomes");
            for index in 0..2 {
                assert!(reopened.database.peek_row(reopened.database.session_epoch(), "Message".into(), format!("batch-server-{index}")).unwrap().is_some(), "WAL commit {k}: server row {index} missing");
            }
        } else {
            assert_eq!(confirmed, 0, "WAL commit {k}: uploads wait but {confirmed} outcomes are confirmed");
        }
        drop(db);
        bound(reopened.database.close_database()).await.unwrap();
    }
    bound(stand.database.close_database()).await.unwrap();
}

async fn discard_consistent_at_every_commit(group: bool) {
    use crate::ffi::command::{CommandRequest, CommandUpload};
    use crate::ffi::upload_wire::{WireSource, WireUploadRequest};
    use crate::ffi::wire::WireUploadStatus;

    {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("live.sqlite");
        let base = directory.path().join("base.sqlite");
        let saved_files = directory.path().join("before.files");
        let source = directory.path().join("source");
        std::fs::write(&source, b"image bytes").unwrap();
        let source_file = || WireSource::File { path: source.to_string_lossy().into_owned(), name: None, mime: None };
        let credentials = Arc::new(WatchedHeld::default());
        let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
        stand.sign_in().await;
        let epoch = stand.database.session_epoch();
        let (model, action, ids) = if group {
            stand.database.set_online(false).unwrap();
            let items = (0..2).map(|index| WireUploadItem {
                values_json: json!({"chatId":"c1", "kind":"TEXT", "clientKey":format!("discard-{index}"), "createdAt":"2026-11-08T00:00:00Z", "updatedAt":"2026-11-08T00:00:00Z"}).to_string(),
                source: source_file(), variables_json: "{}".into(),
            }).collect();
            let receipt = bound(stand.database.upload_create_batch(epoch, "Message".into(), items, WireUploadBatch { operation: "send_files".into(), variables_json: r#"{"chatId":"c1"}"#.into() })).await.unwrap();
            ("Message", "send_files", receipt.items.iter().map(|item| item.optimistic_value.id.clone()).collect::<Vec<_>>())
        } else {
            stand.answer_next("CreateBlob", json!({"errors":[{"message":"refused","extensions":{"code":"FORBIDDEN"}}]}));
            let receipt = bound(stand.database.send(CommandRequest {
                epoch, model: "Moment".into(), values_json: json!({"createdAt":"2026-11-08T00:00:00Z", "updatedAt":"2026-11-08T00:00:00Z"}).to_string(), args_json: "{}".into(),
                upload: Some(CommandUpload { action: "create_file".into(), request: WireUploadRequest { source: source_file(), variables_json: "{}".into() } }),
            })).await.unwrap();
            let id = receipt.optimistic_value.unwrap().id;
            stand.settle().await;
            let row = stand.database.peek_row(epoch, "Moment".into(), id.clone()).unwrap().unwrap();
            assert_eq!(row.uploads[0].upload.status, WireUploadStatus::Failed);
            stand.database.set_online(false).unwrap();
            ("Moment", "create_file", vec![id])
        };
        stand.settle().await;
        let db = rusqlite::Connection::open(&path).unwrap();
        let transfer_ids: Vec<i64> = ids.iter().map(|id| db.query_row("SELECT transfer_id FROM sync_upload WHERE row_id = ?1 AND action = ?2", rusqlite::params![id, action], |row| row.get(0)).unwrap()).collect();
        let operation_ids: Vec<Option<i64>> = transfer_ids.iter().map(|id| db.query_row("SELECT op_id FROM sync_upload WHERE transfer_id = ?1", [id], |row| row.get(0)).unwrap()).collect();
        let operations_before: Vec<i64> = operation_ids.iter().map(|id| db.query_row("SELECT count(*) FROM op WHERE seq = ?1", [id], |row| row.get(0)).unwrap()).collect();
        assert!(operations_before.iter().all(|count| *count > 0), "each accepted transfer must have an operation: {operations_before:?}");
        let baseline_outcome: Option<(i64, String, i64)> = db.query_row("SELECT cycle_id, kind, terminal_at FROM transfer_outcome WHERE transfer_id = ?1 ORDER BY cycle_id DESC LIMIT 1", [transfer_ids[0]], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).ok();
        let baseline_upload: (String, String, Option<i64>, Option<i64>) = db.query_row("SELECT status, stage, op_id, cycle_id FROM sync_upload WHERE transfer_id = ?1", [transfer_ids[0]], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))).unwrap();
        eprintln!("DISCARD BASE group={group} transfer={} upload={baseline_upload:?} operations={operations_before:?} outcome={baseline_outcome:?}", transfer_ids[0]);
        let checkpoint: (i64, i64, i64) = db.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).unwrap();
        assert_eq!(checkpoint.0, 0, "checkpoint: {checkpoint:?}");
        drop(db);
        copy_files(Path::new(&format!("{}.files", path.display())), &saved_files);
        for id in &transfer_ids { assert!(saved_files.join(format!("transfers/{id}")).exists()); }
        bound(stand.database.discard(epoch, model.into(), ids[0].clone(), Some(action.into()))).await.unwrap();
        stand.settle().await;
        copy_image(&path, &base);
        let wal = std::fs::read(format!("{}-wal", base.display())).unwrap();
        let page_size = u32::from_be_bytes(wal[8..12].try_into().unwrap()) as usize;
        let frame_size = 24 + if page_size == 1 { 65536 } else { page_size };
        assert!(wal.len() >= 32 && (wal.len() - 32) % frame_size == 0);
        let commits: Vec<_> = (32..wal.len()).step_by(frame_size).filter(|offset| u32::from_be_bytes(wal[offset + 4..offset + 8].try_into().unwrap()) != 0).map(|offset| offset + frame_size).collect();
        assert!(!commits.is_empty(), "discard must write at least one commit");
        for (index, end) in commits.iter().enumerate() {
            let image = directory.path().join(format!("image-{group}-{index}.sqlite"));
            std::fs::copy(&base, &image).unwrap();
            std::fs::write(format!("{}-wal", image.display()), &wal[..*end]).unwrap();
            let image_files = std::path::PathBuf::from(format!("{}.files", image.display()));
            copy_files(&saved_files, &image_files);
            let before = rusqlite::Connection::open(&image).unwrap();
            let before_operation: i64 = before.query_row("SELECT count(*) FROM op WHERE seq = ?1", [operation_ids[0]], |row| row.get(0)).unwrap();
            let before_upload: i64 = before.query_row("SELECT count(*) FROM sync_upload WHERE transfer_id = ?1", [transfer_ids[0]], |row| row.get(0)).unwrap();
            let before_outcome: Option<(i64, String, i64)> = before.query_row("SELECT cycle_id, kind, terminal_at FROM transfer_outcome WHERE transfer_id = ?1 ORDER BY cycle_id DESC LIMIT 1", [transfer_ids[0]], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).ok();
            let before_copy = image_files.join(format!("transfers/{}", transfer_ids[0])).exists();
            drop(before);
            let reopened = Stand::prepare_online(Some(image.to_string_lossy().into_owned()), credentials.clone(), None, false).await;
            reopened.settle().await;
            let epoch = reopened.database.session_epoch();
            let row = reopened.database.peek_row(epoch, model.into(), ids[0].clone()).unwrap();
            let db = rusqlite::Connection::open(&image).unwrap();
            let upload: i64 = db.query_row("SELECT count(*) FROM sync_upload WHERE transfer_id = ?1", [transfer_ids[0]], |row| row.get(0)).unwrap();
            let operation: i64 = db.query_row("SELECT count(*) FROM op WHERE seq = ?1", [operation_ids[0]], |row| row.get(0)).unwrap();
            let copy = image_files.join(format!("transfers/{}", transfer_ids[0])).exists();
            let outcome: Option<(i64, String, i64)> = db.query_row("SELECT cycle_id, kind, terminal_at FROM transfer_outcome WHERE transfer_id = ?1 ORDER BY cycle_id DESC LIMIT 1", [transfer_ids[0]], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).ok();
            eprintln!("DISCARD group={group} k={} before=({before_operation},{before_upload},{before_copy},{before_outcome:?}) after=({operation},{upload},{copy},{outcome:?})", index + 1);
            if upload == 1 {
                assert!(operation > 0 && row.as_ref().is_some_and(|row| row.uploads.iter().any(|entry| entry.action == action)) && copy, "group={group} k={}: pending upload lost its operation, row or file", index + 1);
            } else {
                assert!(operation == 0 && row.as_ref().is_none_or(|row| !row.uploads.iter().any(|entry| entry.action == action)) && !copy, "group={group} k={}: removed upload left its operation, row or file", index + 1);
                if let Some(expected) = &baseline_outcome {
                    assert_eq!(outcome.as_ref(), Some(expected), "group={group} k={}: retained outcome", index + 1);
                } else if !group {
                    assert!(outcome.as_ref().is_some_and(|(cycle, kind, _)| Some(*cycle) == baseline_upload.3 && kind == "failed"), "group={group} k={}: failed outcome {outcome:?}", index + 1);
                } else {
                    assert!(outcome.as_ref().is_some_and(|(cycle, kind, _)| Some(*cycle) == baseline_upload.3 && kind == "discarded"), "group={group} k={}: outcome {outcome:?}", index + 1);
                }
            }
            if group {
                let other_operation: i64 = db.query_row("SELECT count(*) FROM op WHERE seq = ?1", [operation_ids[1]], |row| row.get(0)).unwrap();
                assert!(other_operation > 0, "group k={}: second operation missing", index + 1);
                let other = reopened.database.peek_row(epoch, model.into(), ids[1].clone()).unwrap().unwrap();
                assert!(other.uploads.iter().any(|entry| entry.action == action), "group k={}: second upload missing", index + 1);
                assert!(image_files.join(format!("transfers/{}", transfer_ids[1])).exists(), "group k={}: second file missing", index + 1);
            }
            drop(db);
            bound(reopened.database.close_database()).await.unwrap();
        }
        bound(stand.database.close_database()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_discard_consistent_single() { discard_consistent_at_every_commit(false).await; }

#[tokio::test(flavor = "multi_thread")]
async fn consumer_discard_consistent_group() { discard_consistent_at_every_commit(true).await; }

async fn batch(stand: &Stand, index: usize) -> Vec<String> {
    let epoch = stand.database.session_epoch();
    let items = (0..2).map(|item| WireUploadItem {
        values_json: json!({"chatId": "c1", "kind": "TEXT", "clientKey": format!("k{index}-{item}"), "createdAt": "2026-11-08T00:00:00Z", "updatedAt": "2026-11-08T00:00:00Z"}).to_string(),
        source: crate::ffi::upload_wire::WireSource::Pending, variables_json: "{}".into(),
    }).collect();
    let receipt = bound(stand.database.upload_create_batch(epoch, "Message".into(), items, WireUploadBatch { operation: "send_files".into(), variables_json: r#"{"chatId":"c1"}"#.into() })).await.unwrap();
    receipt.items.iter().map(|item| item.optimistic_value.id.clone()).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn crash_images_keep_the_durable_intents() {
    let mut matrix = Matrix::new("C6-crash-image");
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("app.sqlite");
    let credentials = Arc::new(WatchedHeld::default());
    let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
    stand.sign_in().await;
    stand.database.set_online(false).unwrap();
    let mut created: Vec<String> = Vec::new();
    for step in 0..4usize {
        if step > 0 { created.extend(batch(&stand, step).await); }
        stand.settle().await;
        let live = operations(&stand).await;
        let image = directory.path().join(format!("image-{step}.sqlite"));
        copy_image(&path, &image);
        let reopened = Stand::prepare_on(Some(image.to_string_lossy().into_owned()), credentials.clone()).await;
        reopened.settle().await;
        let after = operations(&reopened).await;
        let label = format!("after step {step}");
        matrix.check(&label, "operations of the image", "same set as live", &format!("{live:?}"), &format!("{after:?}"));
        let epoch = reopened.database.session_epoch();
        let missing: Vec<&String> = created.iter().filter(|id| reopened.database.peek_row(epoch, "Message".into(), (*id).clone()).ok().flatten().is_none()).collect();
        matrix.check(&label, "rows of the batches in the image", "none missing", "none missing", if missing.is_empty() { "none missing" } else { "rows missing" });
        matrix.check(&label, "session of the image", "kept", "kept", if step == 0 || reopened.database.session_epoch() > 0 { "kept" } else { "lost" });
        bound(reopened.database.close_database()).await.unwrap();
    }
    let live = operations(&stand).await;
    bound(stand.database.close_database()).await.unwrap();
    drop(stand);

    let broken = directory.path().join("corrupt.sqlite");
    copy_image(&path, &broken);
    let connection = rusqlite::Connection::open(&path).unwrap();
    crate::store::ddl::manifest_write(&connection, crate::store::ddl::FORMAT_VERSION_KEY, "older-format").unwrap();
    drop(connection);
    let upgraded = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
    upgraded.settle().await;
    let after = operations(&upgraded).await;
    matrix.check("format version bump", "operations survive", "same set as before", &format!("{:?}", live.iter().map(|op| (&op.1, &op.2)).collect::<Vec<_>>()), &format!("{:?}", after.iter().map(|op| (&op.1, &op.2)).collect::<Vec<_>>()));
    matrix.check("format version bump", "session survives", "kept", "kept", if upgraded.database.session_epoch() > 0 { "kept" } else { "lost" });
    bound(upgraded.database.close_database()).await.unwrap();
    drop(upgraded);

    let connection = rusqlite::Connection::open(&broken).unwrap();
    let corrupted = connection.execute("UPDATE op SET attempts = 99999999999", []).map_err(|error| panic!("{error}")).unwrap();
    drop(connection);
    let opened = tokio::spawn({
        let (path, credentials) = (broken.clone(), credentials.clone());
        async move { Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials).await.settle().await }
    }).await;
    matrix.check("corrupt integer column", "rows corrupted (value above u32 in a STRICT integer column)", ">= 1", ">= 1", if corrupted >= 1 { ">= 1" } else { "0 rows" });
    let answer = match opened {
        Ok(()) => "opened".to_string(),
        Err(error) => { let text = error.into_panic().downcast_ref::<String>().cloned().unwrap_or_default(); if text.contains("Storage") { "Storage error".to_string() } else { format!("other panic: {text}") } }
    };
    matrix.check("corrupt integer column", "open of a corrupt file", "attempts above u32", "Storage error", &answer);
    matrix.finish();
}

#[tokio::test(flavor = "multi_thread")]
async fn life_end_update_format() {
    use crate::ffi::{CredentialStore, SessionStanding};

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("session.sqlite");
    let credentials = Arc::new(WatchedHeld::default());
    let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
    stand.sign_in().await;
    stand.settle().await;
    let before = stand.database.session_status().unwrap();
    assert!(matches!(before.phase, SessionStanding::Active));
    let secret = credentials.get().unwrap().expect("login saves the credential");
    bound(stand.database.close_database()).await.unwrap();
    drop(stand);

    let connection = rusqlite::Connection::open(&path).unwrap();
    crate::store::ddl::manifest_write(&connection, crate::store::ddl::FORMAT_VERSION_KEY, "older-format").unwrap();
    drop(connection);

    let reopened = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
    reopened.settle().await;
    let after = reopened.database.session_status().unwrap();
    assert!(matches!(after.phase, SessionStanding::Active));
    assert_eq!(after.identity, before.identity, "format change preserves the signed-in identity");
    assert_eq!(credentials.get().unwrap().as_deref(), Some(secret.as_str()), "format change preserves the credential");
    bound(reopened.database.close_database()).await.unwrap();
}

#[derive(Default)]
struct HeldClear {
    credentials: WatchedHeld,
    entered: crate::test_wait::Flag,
    release: crate::test_wait::Flag,
    hold: std::sync::atomic::AtomicBool,
}

impl crate::ffi::CredentialStore for HeldClear {
    fn get(&self) -> Result<Option<String>, crate::ffi::DbError> { self.credentials.get() }
    fn set(&self, value: String) -> Result<(), crate::ffi::DbError> { self.credentials.set(value) }
    fn clear(&self) -> Result<(), crate::ffi::DbError> {
        if self.hold.swap(false, std::sync::atomic::Ordering::SeqCst) {
            self.entered.set();
            self.release.wait_blocking("release credential deletion");
        }
        self.credentials.clear()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn life_end_crash_before_secret_clear() {
    use crate::ffi::{CredentialStore, SessionStanding};

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("live.sqlite");
    let image = directory.path().join("image.sqlite");
    let credentials = Arc::new(HeldClear::default());
    let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
    stand.sign_in().await;
    stand.database.set_online(false).unwrap();
    let ids = batch(&stand, 0).await;
    stand.settle().await;
    let secret = credentials.get().unwrap().expect("login saves the credential");
    credentials.hold.store(true, std::sync::atomic::Ordering::SeqCst);
    let database = stand.database.clone();
    let reset = tokio::spawn(async move { database.session_reset().await });
    credentials.entered.wait("the reset reaches credential deletion").await;
    assert!(!reset.is_finished(), "reset must wait for credential deletion");
    copy_image(&path, &image);
    credentials.release.set();
    bound(reset).await.unwrap().unwrap();
    bound(stand.database.close_database()).await.unwrap();
    drop(stand);

    let restored_credentials = Arc::new(WatchedHeld::default());
    restored_credentials.set(secret).unwrap();
    let reopened = Stand::prepare_online(Some(image.to_string_lossy().into_owned()), restored_credentials.clone(), None, false).await;
    reopened.settle().await;
    assert_eq!(reopened.database.session_status().unwrap().phase, SessionStanding::None);
    assert!(restored_credentials.get().unwrap().is_none(), "open clears the old credential");
    reopened.database.set_online(true).unwrap();
    bound(reopened.database.session_call("login_link".into(), json!({"account":"B"}).to_string())).await.unwrap();
    reopened.settle().await;
    assert!(operations(&reopened).await.is_empty(), "old operations are gone");
    for id in ids {
        assert!(reopened.database.peek_row(reopened.database.session_epoch(), "Message".into(), id).unwrap().is_none());
    }
    assert_eq!(reopened.requests("MessageAlbumSend"), 0, "open must not send the old work");
    bound(reopened.database.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn life_end_crash_before_intent() {
    use crate::ffi::SessionStanding;

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("live.sqlite");
    let image = directory.path().join("image.sqlite");
    let credentials = Arc::new(HeldClear::default());
    let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
    stand.sign_in().await;
    stand.database.set_online(false).unwrap();
    let ids = batch(&stand, 0).await;
    stand.settle().await;
    let expected = stand.database.session_status().unwrap();
    let secret = credentials.get().unwrap().expect("login saves the credential");
    copy_image(&path, &image);
    credentials.hold.store(true, std::sync::atomic::Ordering::SeqCst);
    let database = stand.database.clone();
    let reset = tokio::spawn(async move { database.session_reset().await });
    credentials.entered.wait("the reset reaches credential deletion").await;
    assert!(!reset.is_finished(), "the reset has not answered");
    credentials.release.set();
    bound(reset).await.unwrap().unwrap();
    bound(stand.database.close_database()).await.unwrap();
    drop(stand);

    let restored_credentials = Arc::new(WatchedHeld::default());
    restored_credentials.set(secret.clone()).unwrap();
    let reopened = Stand::prepare_online(Some(image.to_string_lossy().into_owned()), restored_credentials.clone(), None, false).await;
    reopened.settle().await;
    let actual = reopened.database.session_status().unwrap();
    assert_eq!(actual.phase, SessionStanding::Active);
    assert_eq!(actual.identity, expected.identity);
    assert_eq!(restored_credentials.get().unwrap().as_deref(), Some(secret.as_str()));
    assert_eq!(operations(&reopened).await.len(), ids.len());
    for id in ids {
        assert!(reopened.database.peek_row(actual.epoch, "Message".into(), id).unwrap().is_some());
    }
    bound(reopened.database.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn life_open_finishes_ending() {
    use crate::ffi::SessionStanding;

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("live.sqlite");
    let base = directory.path().join("base.sqlite");
    let credentials = Arc::new(HeldClear::default());
    let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
    stand.sign_in().await;
    stand.database.set_online(false).unwrap();
    let ids = batch(&stand, 0).await;
    stand.settle().await;
    let secret = credentials.get().unwrap().expect("login saves the credential");
    let connection = rusqlite::Connection::open(&path).unwrap();
    let checkpoint: (i64, i64, i64) = connection.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).unwrap();
    assert_eq!(checkpoint.0, 0, "checkpoint must complete: {checkpoint:?}");
    drop(connection);
    credentials.hold.store(true, std::sync::atomic::Ordering::SeqCst);
    let database = stand.database.clone();
    let reset = tokio::spawn(async move { database.session_reset().await });
    credentials.entered.wait("the reset reaches credential deletion").await;
    copy_image(&path, &base);
    let wal = std::fs::read(format!("{}-wal", base.display())).unwrap();
    assert!(wal.len() >= 32, "a file-backed database must have a WAL header");
    let page_size = u32::from_be_bytes(wal[8..12].try_into().unwrap()) as usize;
    let page_size = if page_size == 1 { 65536 } else { page_size };
    let frame_size = 24 + page_size;
    assert!(page_size >= 512 && (wal.len() - 32) % frame_size == 0, "complete WAL frames");
    let commits: Vec<usize> = (32..wal.len()).step_by(frame_size)
        .filter(|offset| u32::from_be_bytes(wal[offset + 4..offset + 8].try_into().unwrap()) != 0)
        .map(|offset| offset + frame_size).collect();
    assert!(commits.len() >= 2, "the WAL must show the publication and erasure commits");
    assert!((wal.len() - 32) / frame_size < 1000, "the WAL must not be auto-checkpointed");
    credentials.release.set();
    bound(reset).await.unwrap().unwrap();
    bound(stand.database.close_database()).await.unwrap();
    drop(stand);

    for (index, end) in commits.into_iter().enumerate() {
        let image = directory.path().join(format!("image-{index}.sqlite"));
        std::fs::copy(&base, &image).unwrap();
        std::fs::write(format!("{}-wal", image.display()), &wal[..end]).unwrap();
        let restored_credentials = Arc::new(WatchedHeld::default());
        restored_credentials.set(secret.clone()).unwrap();
        let reopened = Stand::prepare_online(Some(image.to_string_lossy().into_owned()), restored_credentials.clone(), None, false).await;
        reopened.settle().await;
        assert_eq!(reopened.database.session_status().unwrap().phase, SessionStanding::None, "WAL commit {} must finish the accepted end", index + 1);
        assert!(restored_credentials.get().unwrap().is_none(), "old credential remains at WAL commit {}", index + 1);
        reopened.database.set_online(true).unwrap();
        bound(reopened.database.session_call("login_link".into(), json!({"account":"B"}).to_string())).await.unwrap();
        reopened.settle().await;
        assert!(operations(&reopened).await.is_empty(), "old operations remain at WAL commit {}", index + 1);
        for id in &ids {
            assert!(reopened.database.peek_row(reopened.database.session_epoch(), "Message".into(), id.clone()).unwrap().is_none());
        }
        assert_eq!(reopened.requests("MessageAlbumSend"), 0, "old requests escaped at WAL commit {}", index + 1);
        bound(reopened.database.close_database()).await.unwrap();
    }
}


#[derive(Default)]
struct RefusedClear {
    credentials: WatchedHeld,
    refused: std::sync::atomic::AtomicBool,
}

impl CredentialStore for RefusedClear {
    fn get(&self) -> Result<Option<String>, crate::ffi::DbError> { self.credentials.get() }
    fn set(&self, value: String) -> Result<(), crate::ffi::DbError> { self.credentials.set(value) }
    fn clear(&self) -> Result<(), crate::ffi::DbError> {
        if self.refused.swap(false, std::sync::atomic::Ordering::SeqCst) {
            return Err(crate::ffi::DbError::Storage { detail: "credential deletion refused".into() });
        }
        self.credentials.clear()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn life_end_clear_secret_failure() {
    use crate::ffi::SessionStanding;

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("live.sqlite");
    let credentials = Arc::new(RefusedClear::default());
    let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
    stand.sign_in().await;
    stand.database.set_online(false).unwrap();
    batch(&stand, 0).await;
    stand.settle().await;
    let secret = credentials.get().unwrap().expect("login saves the credential");
    credentials.refused.store(true, std::sync::atomic::Ordering::SeqCst);
    bound(stand.database.session_reset()).await.unwrap();
    assert_eq!(credentials.get().unwrap().as_deref(), Some(secret.as_str()));
    bound(stand.database.close_database()).await.unwrap();
    drop(stand);
    credentials.refused.store(true, std::sync::atomic::Ordering::SeqCst);
    let reopened = Stand::prepare_online(Some(path.to_string_lossy().into_owned()), credentials.clone(), None, false).await;
    reopened.settle().await;
    assert_eq!(reopened.database.session_status().unwrap().phase, SessionStanding::None);
    assert_eq!(credentials.get().unwrap().as_deref(), Some(secret.as_str()));
    assert_eq!(reopened.requests("MessageAlbumSend"), 0);
    bound(reopened.database.close_database()).await.unwrap();
    drop(reopened);
    let last = Stand::prepare_online(Some(path.to_string_lossy().into_owned()), credentials.clone(), None, false).await;
    last.settle().await;
    assert_eq!(last.database.session_status().unwrap().phase, SessionStanding::None);
    assert!(credentials.get().unwrap().is_none(), "a later open finishes the credential deletion");
    assert_eq!(last.requests("MessageAlbumSend"), 0);
    bound(last.database.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn life_end_crash_during_replay() {
    use crate::ffi::SessionStanding;

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("live.sqlite");
    let first = directory.path().join("first.sqlite");
    let second = directory.path().join("second.sqlite");
    let credentials = Arc::new(HeldClear::default());
    let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
    stand.sign_in().await;
    stand.database.set_online(false).unwrap();
    batch(&stand, 0).await;
    stand.settle().await;
    let secret = credentials.get().unwrap().expect("login saves the credential");
    credentials.hold.store(true, std::sync::atomic::Ordering::SeqCst);
    let database = stand.database.clone();
    let reset = tokio::spawn(async move { database.session_reset().await });
    credentials.entered.wait("the reset reaches credential deletion").await;
    copy_image(&path, &first);
    credentials.release.set();
    bound(reset).await.unwrap().unwrap();
    bound(stand.database.close_database()).await.unwrap();
    drop(stand);

    let restoring = Arc::new(HeldClear::default());
    restoring.set(secret.clone()).unwrap();
    restoring.hold.store(true, std::sync::atomic::Ordering::SeqCst);
    let opening = tokio::spawn({
        let path = first.to_string_lossy().into_owned();
        let credentials = restoring.clone();
        async move { Stand::prepare_online(Some(path), credentials, None, false).await }
    });
    restoring.entered.wait("recovery reaches credential deletion").await;
    copy_image(&first, &second);
    restoring.release.set();
    let recovered = bound(opening).await.unwrap();
    bound(recovered.database.close_database()).await.unwrap();
    drop(recovered);

    let last_credentials = Arc::new(WatchedHeld::default());
    last_credentials.set(secret).unwrap();
    let reopened = Stand::prepare_online(Some(second.to_string_lossy().into_owned()), last_credentials.clone(), None, false).await;
    reopened.settle().await;
    assert_eq!(reopened.database.session_status().unwrap().phase, SessionStanding::None);
    assert!(last_credentials.get().unwrap().is_none(), "credential deletion survives a second crash");
    assert_eq!(reopened.requests("MessageAlbumSend"), 0);
    bound(reopened.database.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_direct_call_answer_survives_crash() {
    let directory = tempfile::tempdir().unwrap();
    let live = directory.path().join("live.sqlite");
    let pending = directory.path().join("pending.sqlite");
    let base = directory.path().join("answered.sqlite");
    let credentials = Arc::new(WatchedHeld::default());
    let first = Stand::prepare_on(Some(live.to_string_lossy().into_owned()), credentials.clone()).await;
    first.sign_in().await;
    first.settle().await;
    first.hold(&["otp"]);
    let mark = first.request_mark();
    let database = first.database.clone();
    let call = tokio::spawn(async move { database.session_call("otp".into(), r#"{"phone":"test"}"#.into()).await });
    first.receive_after(mark, "otp", None, 1).await;
    assert!(!call.is_finished());
    copy_image(&live, &pending);

    let resumed = Stand::prepare_online(Some(pending.to_string_lossy().into_owned()), credentials.clone(), None, false).await;
    let connection = rusqlite::Connection::open(&pending).unwrap();
    let saved: i64 = connection.query_row("SELECT count(*) FROM encrypted_pending", [], |row| row.get(0)).unwrap();
    assert_eq!(saved, 1, "the first image must contain the direct pending call");
    let checkpoint: (i64, i64, i64) = connection.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).unwrap();
    assert_eq!(checkpoint.0, 0, "checkpoint must complete: {checkpoint:?}");
    drop(connection);
    let mark = resumed.request_mark();
    resumed.database.set_online(true).unwrap();
    resumed.receive_after(mark, "otp", None, 1).await;
    resumed.settle().await;
    assert_eq!(resumed.requests_after(mark, "otp", None), 1);
    copy_image(&pending, &base);
    let wal = std::fs::read(format!("{}-wal", base.display())).unwrap();
    let page_size = u32::from_be_bytes(wal[8..12].try_into().unwrap()) as usize;
    let page_size = if page_size == 1 { 65536 } else { page_size };
    let frame_size = 24 + page_size;
    assert!(page_size >= 512 && (wal.len() - 32) % frame_size == 0);
    let commits: Vec<_> = (32..wal.len()).step_by(frame_size)
        .filter(|offset| u32::from_be_bytes(wal[offset + 4..offset + 8].try_into().unwrap()) != 0)
        .map(|offset| offset + frame_size).collect();
    assert!(!commits.is_empty(), "the answer must commit before the second crash");
    let mut answered_images = 0;
    for (index, end) in commits.into_iter().enumerate() {
        let image = directory.path().join(format!("answer-{index}.sqlite"));
        std::fs::copy(&base, &image).unwrap();
        std::fs::write(format!("{}-wal", image.display()), &wal[..end]).unwrap();
        let reader = rusqlite::Connection::open(&image).unwrap();
        let pending_count: i64 = reader.query_row("SELECT count(*) FROM encrypted_pending", [], |row| row.get(0)).unwrap();
        drop(reader);
        if pending_count != 0 { continue; }
        answered_images += 1;
        let reopened = Stand::prepare_online(Some(image.to_string_lossy().into_owned()), credentials.clone(), None, false).await;
        reopened.settle().await;
        let answer = bound(reopened.database.interrupted_session_answer()).await.unwrap();
        let answer = answer.map(|body| serde_json::from_str::<serde_json::Value>(&body).unwrap());
        assert_eq!(answer, Some(json!({"sent":true,"marker":"resume-after-crash"})), "answer after WAL commit {}", index + 1);
        assert_eq!(bound(reopened.database.interrupted_session_answer()).await.unwrap(), None);
        assert_eq!(reopened.requests("otp"), 0, "the answered call must not be sent again");
        bound(reopened.database.close_database()).await.unwrap();
    }
    assert!(answered_images > 0, "the WAL must include a committed answer");
    first.release(&["otp"]);
    bound(call).await.unwrap().unwrap();
    bound(resumed.database.close_database()).await.unwrap();
    bound(first.database.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_resume_take_during_close() {
    use crate::ffi::DbError;
    use std::sync::atomic::{AtomicBool, Ordering};

    let directory = tempfile::tempdir().unwrap();
    let live = directory.path().join("live.sqlite");
    let pending = directory.path().join("pending.sqlite");
    let base = directory.path().join("answered.sqlite");
    let credentials = Arc::new(WatchedHeld::default());
    let first = Stand::prepare_on(Some(live.to_string_lossy().into_owned()), credentials.clone()).await;
    first.sign_in().await;
    first.settle().await;
    first.hold(&["otp"]);
    let mark = first.request_mark();
    let database = first.database.clone();
    let call = tokio::spawn(async move { database.session_call("otp".into(), r#"{"phone":"test"}"#.into()).await });
    first.receive_after(mark, "otp", None, 1).await;
    copy_image(&live, &pending);
    first.release(&["otp"]);
    bound(call).await.unwrap().unwrap();
    bound(first.database.close_database()).await.unwrap();
    drop(first);

    let restored = Stand::prepare_online(Some(pending.to_string_lossy().into_owned()), credentials.clone(), None, true).await;
    restored.settle().await;
    copy_image(&pending, &base);
    bound(restored.database.close_database()).await.unwrap();
    drop(restored);

    for case in ["committed", "rejected", "concurrent"] {
        let image = directory.path().join(format!("{case}.sqlite"));
        copy_image(&base, &image);
        let stand = Stand::prepare_online(Some(image.to_string_lossy().into_owned()), credentials.clone(), None, false).await;
        stand.settle().await;
        if case == "rejected" {
            let fail = Arc::new(AtomicBool::new(true));
            stand.database.db.lock().connection.progress_handler(1, Some(move || fail.swap(false, Ordering::SeqCst))).unwrap();
        }
        let (answer, close) = if case == "concurrent" {
            let database = stand.database.clone();
            bound(async { tokio::join!(database.interrupted_session_answer(), database.close_database()) }).await
        } else {
            let answer = bound(stand.database.interrupted_session_answer()).await;
            if case == "rejected" { stand.database.db.lock().connection.progress_handler(0, None::<fn() -> bool>).unwrap(); }
            (answer, bound(stand.database.close_database()).await)
        };
        close.unwrap();
        if case == "rejected" { assert!(matches!(&answer, Err(DbError::Storage { .. })), "{answer:?}"); }
        let reopened = Stand::prepare_online(Some(image.to_string_lossy().into_owned()), credentials.clone(), None, false).await;
        let after = bound(reopened.database.interrupted_session_answer()).await.unwrap();
        let expected = json!({"sent":true,"marker":"resume-after-crash"});
        let parsed = |body: &str| serde_json::from_str::<serde_json::Value>(body).unwrap();
        match case {
            "committed" => { assert_eq!(answer.unwrap().as_deref().map(parsed), Some(expected)); assert_eq!(after, None); }
            "rejected" => assert_eq!(after.as_deref().map(parsed), Some(expected)),
            _ => match answer {
                Ok(Some(body)) => { assert_eq!(parsed(&body), expected); assert_eq!(after, None); }
                Err(DbError::Closed { .. }) => assert_eq!(after.as_deref().map(parsed), Some(expected)),
                other => panic!("concurrent close lost the answer: {other:?}, reopened={after:?}"),
            },
        }
        bound(reopened.database.close_database()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_attached_file_consistent_at_every_commit() {
    use crate::ffi::{Config, Database, OpenPorts};
    use crate::schema::{FieldSpec, ModelSpec, Storage};
    use crate::value::ScalarKind;

    fn files_under(root: &Path) -> Vec<std::path::PathBuf> {
        if !root.exists() { return Vec::new(); }
        let mut files = Vec::new();
        for entry in std::fs::read_dir(root).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() { files.extend(files_under(&entry.path())); }
            else { files.push(entry.path()); }
        }
        files
    }

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("live.sqlite");
    let base = directory.path().join("base.sqlite");
    let files = directory.path().join("saved.files");
    let source = directory.path().join("source");
    let mut model = ModelSpec::new("Files", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::file("photo").nullable()]);
    model.storage = Storage::Local;
    let spec = serde_json::to_string(&vec![model]).unwrap();
    let config = |path: &Path| Config { path: Some(path.to_string_lossy().into_owned()), data_version: None };
    let db = bound(Database::open(config(&path), crate::ffi::specs(spec.clone()), OpenPorts::default())).await.unwrap();
    let epoch = db.session_epoch();
    let id = bound(db.create(epoch, "Files".into(), "{}".into(), "{}".into())).await.unwrap().optimistic_value.unwrap().id;
    let connection = rusqlite::Connection::open(&path).unwrap();
    let checkpoint: (i64, i64, i64) = connection.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).unwrap();
    assert_eq!(checkpoint.0, 0);
    drop(connection);
    for (size, byte) in [(3, 1), (300 * 1024, 9)] {
        std::fs::write(&source, vec![byte; size]).unwrap();
        bound(db.attach_file(epoch, "Files".into(), id.clone(), "photo".into(), source.to_string_lossy().into_owned(), "photo.bin".into(), "application/octet-stream".into())).await.unwrap();
        copy_files(Path::new(&format!("{}.files", path.display())), &files);
        assert!(bound(db.detach(epoch, "Files".into(), id.clone(), "photo".into())).await.unwrap());
    }
    copy_image(&path, &base);
    let wal = std::fs::read(format!("{}-wal", base.display())).unwrap();
    assert!(wal.len() >= 32);
    let page_size = u32::from_be_bytes(wal[8..12].try_into().unwrap()) as usize;
    let frame_size = 24 + if page_size == 1 { 65536 } else { page_size };
    assert!(page_size >= 512 && (wal.len() - 32) % frame_size == 0);
    let commits: Vec<_> = (32..wal.len()).step_by(frame_size).filter(|offset| u32::from_be_bytes(wal[offset + 4..offset + 8].try_into().unwrap()) != 0).map(|offset| offset + frame_size).collect();
    assert!(commits.len() >= 4, "attach and detach must each commit");
    let mut seen = [false; 3];
    for (k, end) in std::iter::once(32).chain(commits).enumerate() {
        let image = directory.path().join(format!("file-{k}.sqlite"));
        std::fs::copy(&base, &image).unwrap();
        std::fs::write(format!("{}-wal", image.display()), &wal[..end]).unwrap();
        let image_files = std::path::PathBuf::from(format!("{}.files", image.display()));
        copy_files(&files, &image_files);
        let reopened = bound(Database::open(config(&image), crate::ffi::specs(spec.clone()), OpenPorts::default())).await.unwrap();
        let epoch = reopened.session_epoch();
        let descriptor = bound(reopened.file(epoch, "Files".into(), id.clone(), "photo".into())).await.unwrap();
        match descriptor.as_ref().map(|file| file.size) {
            None => seen[0] = true,
            Some(3) => seen[1] = true,
            Some(307_200) => seen[2] = true,
            other => panic!("WAL commit {k}: unexpected descriptor {other:?}"),
        }
        for path in files_under(&image_files) {
            let relative = path.strip_prefix(&image_files).unwrap();
            let held = descriptor.as_ref().is_some_and(|file| {
                (file.size > 256 * 1024 && relative == Path::new(&file.id)) || relative == Path::new("cache").join(&file.id)
            });
            assert!(held, "WAL commit {k}: orphan file {path:?}");
        }
        let url = bound(reopened.url(epoch, "Files".into(), id.clone(), "photo".into())).await.unwrap();
        assert_eq!(descriptor.is_some(), url.is_some(), "WAL commit {k}: descriptor and bytes disagree");
        if let (Some(file), Some(url)) = (descriptor, url) {
            let (size, byte) = match file.size { 3 => (3, 1), 307_200 => (300 * 1024, 9), other => panic!("WAL commit {k}: unexpected size {other}") };
            assert_eq!(std::fs::read(url).unwrap(), vec![byte; size], "WAL commit {k}");
        }
        bound(reopened.close_database()).await.unwrap();
    }
    assert_eq!(seen, [true; 3], "images must include detached, small and large files");
    bound(db.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_temporary_message_id_consistent_at_every_commit() {
    use crate::ffi::command::CommandRequest;

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("live.sqlite");
    let base = directory.path().join("base.sqlite");
    let credentials = Arc::new(WatchedHeld::default());
    let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
    stand.sign_in().await;
    let epoch = stand.database.session_epoch();
    stand.answer("ChatNode", json!({"node":super::stand::chat("c1", "before")}));
    bound(stand.database.node(epoch, "Chat".into(), "node".into(), "c1".into(), true)).await.unwrap().unwrap();
    stand.hold(&["MessageSend"]);
    let mark = stand.request_mark();
    let values = json!({"chatId":"c1", "body":"first", "kind":"TEXT", "createdAt":"2026-11-08T00:00:00Z", "updatedAt":"2026-11-08T00:00:00Z"}).to_string();
    let first = bound(stand.database.send(CommandRequest { epoch, model: "Message".into(), values_json: values, args_json: "{}".into(), upload: None })).await.unwrap();
    let temporary = first.optimistic_value.as_ref().unwrap().id.clone();
    let operation = first.operation.unwrap();
    stand.receive_after(mark, "MessageSend", None, 1).await;
    let db = rusqlite::Connection::open(&path).unwrap();
    let before: Option<String> = db.query_row("SELECT last_message_id FROM row_Chat WHERE id = 'c1'", [], |row| row.get(0)).unwrap();
    assert_eq!(before.as_deref(), Some(temporary.as_str()));
    let checkpoint: (i64, i64, i64) = db.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).unwrap();
    assert_eq!(checkpoint.0, 0);
    drop(db);
    stand.answer_next("MessageSend", json!({"data":{"messageSend":{"message":super::stand::message("server-message", "c1", "first")}}}));
    stand.release(&["MessageSend"]);
    let confirmed = bound(stand.database.confirmed(first, false)).await.unwrap();
    assert_eq!(confirmed.value.unwrap().id, "server-message");
    stand.database.set_online(false).unwrap();
    stand.settle().await;
    copy_image(&path, &base);
    let wal = std::fs::read(format!("{}-wal", base.display())).unwrap();
    assert!(wal.len() >= 32);
    let page_size = u32::from_be_bytes(wal[8..12].try_into().unwrap()) as usize;
    let frame_size = 24 + if page_size == 1 { 65536 } else { page_size };
    assert!(page_size >= 512 && (wal.len() - 32) % frame_size == 0);
    let commits: Vec<_> = (32..wal.len()).step_by(frame_size).filter(|offset| u32::from_be_bytes(wal[offset + 4..offset + 8].try_into().unwrap()) != 0).map(|offset| offset + frame_size).collect();
    assert!(!commits.is_empty());
    let mut seen = [false; 2];
    for (k, end) in std::iter::once(32).chain(commits).enumerate() {
        let image = directory.path().join(format!("swap-{k}.sqlite"));
        std::fs::copy(&base, &image).unwrap();
        std::fs::write(format!("{}-wal", image.display()), &wal[..end]).unwrap();
        let reopened = Stand::prepare_online(Some(image.to_string_lossy().into_owned()), credentials.clone(), None, false).await;
        reopened.settle().await;
        let db = rusqlite::Connection::open(&image).unwrap();
        let exists = |id: &str| db.query_row("SELECT count(*) FROM row_Message WHERE id = ?1", [id], |row| row.get::<_, i64>(0)).unwrap();
        let pending: i64 = db.query_row("SELECT count(*) FROM op WHERE seq = ?1", [operation], |row| row.get(0)).unwrap();
        let (old, new) = (exists(&temporary), exists("server-message"));
        let reference: Option<String> = db.query_row("SELECT last_message_id FROM row_Chat WHERE id = 'c1'", [], |row| row.get(0)).unwrap();
        if pending == 1 {
            seen[0] = true;
            assert_eq!((old, new, reference.as_deref()), (1, 0, Some(temporary.as_str())), "WAL commit {k}");
        } else {
            seen[1] = true;
            assert_eq!((pending, old, new, reference.as_deref()), (0, 0, 1, Some("server-message")), "WAL commit {k}");
        }
        drop(db);
        bound(reopened.database.close_database()).await.unwrap();
    }
    assert_eq!(seen, [true; 2], "images must include both sides of the swap");
    bound(stand.database.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_window_page_consistent_at_every_commit() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("live.sqlite");
    let base = directory.path().join("base.sqlite");
    let credentials = Arc::new(WatchedHeld::default());
    let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
    stand.sign_in().await;
    let epoch = stand.database.session_epoch();
    let params = json!({"chatId":"c1"});
    let spec = stand.database.db.spec("Message").unwrap();
    let scope = spec.selection_key("thread", params.as_object().unwrap()).unwrap();
    let key = crate::store::window_key(scope.as_str(), crate::schema::PageDirection::Forward);
    let mut first = super::thread_pages_tests::page(1, 1, true, 1);
    first["node"]["messages"]["pageInfo"]["endCursor"] = json!("old-cursor");
    stand.answer("chatThread", first);
    bound(stand.database.refresh(epoch, "Message".into(), Some("thread".into()), params.to_string())).await.unwrap();
    stand.settle().await;
    let db = rusqlite::Connection::open(&path).unwrap();
    let before: (Option<String>, Option<i64>) = db.query_row("SELECT cursor, total FROM query_window WHERE key = ?1", [&key], |row| Ok((row.get(0)?, row.get(1)?))).unwrap();
    assert_eq!(before, (Some("old-cursor".into()), Some(1)));
    let checkpoint: (i64, i64, i64) = db.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).unwrap();
    assert_eq!(checkpoint.0, 0);
    drop(db);
    let mut next = super::thread_pages_tests::page(1, 2, false, 2);
    next["node"]["messages"]["pageInfo"]["endCursor"] = json!("new-cursor");
    stand.answer("chatThread", next);
    bound(stand.database.refresh(epoch, "Message".into(), Some("thread".into()), params.to_string())).await.unwrap();
    stand.settle().await;
    copy_image(&path, &base);
    let wal = std::fs::read(format!("{}-wal", base.display())).unwrap();
    assert!(wal.len() >= 32);
    let page_size = u32::from_be_bytes(wal[8..12].try_into().unwrap()) as usize;
    let frame_size = 24 + if page_size == 1 { 65536 } else { page_size };
    assert!(page_size >= 512 && (wal.len() - 32) % frame_size == 0);
    let commits: Vec<_> = (32..wal.len()).step_by(frame_size).filter(|offset| u32::from_be_bytes(wal[offset + 4..offset + 8].try_into().unwrap()) != 0).map(|offset| offset + frame_size).collect();
    assert!(!commits.is_empty());
    let mut seen = [false; 2];
    for (k, end) in std::iter::once(32).chain(commits).enumerate() {
        let image = directory.path().join(format!("window-{k}.sqlite"));
        std::fs::copy(&base, &image).unwrap();
        std::fs::write(format!("{}-wal", image.display()), &wal[..end]).unwrap();
        let reopened = Stand::prepare_online(Some(image.to_string_lossy().into_owned()), credentials.clone(), None, false).await;
        reopened.settle().await;
        let db = rusqlite::Connection::open(&image).unwrap();
        let window: (Option<String>, Option<i64>) = db.query_row("SELECT cursor, total FROM query_window WHERE key = ?1", [&key], |row| Ok((row.get(0)?, row.get(1)?))).unwrap();
        let mut statement = db.prepare("SELECT row_id FROM scope_member WHERE model = 'Message' AND scope_key = ?1 ORDER BY row_id").unwrap();
        let members: Vec<String> = statement.query_map([scope.as_str()], |row| row.get(0)).unwrap().collect::<Result<_, _>>().unwrap();
        drop(statement);
        let mut statement = db.prepare("SELECT id, body FROM row_Message ORDER BY id").unwrap();
        let rows: Vec<(String, String)> = statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?))).unwrap().collect::<Result<_, _>>().unwrap();
        drop(statement);
        let old = window == (Some("old-cursor".into()), Some(1)) && members == ["m1"] && rows == [("m1".into(), "message 1".into())];
        let new = window == (None, Some(2)) && members == ["m1", "m2"] && rows == [("m1".into(), "message 1".into()), ("m2".into(), "message 2".into())];
        assert!(old || new, "WAL commit {k}: window={window:?}, members={members:?}, rows={rows:?}");
        seen[0] |= old;
        seen[1] |= new;
        drop(db);
        bound(reopened.database.close_database()).await.unwrap();
    }
    assert_eq!(seen, [true; 2], "images must include both page versions");
    bound(stand.database.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_local_value_consistent_at_every_commit() {
    use crate::ffi::wire::Cell;
    use rusqlite::OptionalExtension;

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("live.sqlite");
    let server_base = directory.path().join("server-base.sqlite");
    let migration = directory.path().join("migration.sqlite");
    let migration_base = directory.path().join("migration-base.sqlite");
    let credentials = Arc::new(WatchedHeld::default());
    let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
    stand.sign_in().await;
    let epoch = stand.database.session_epoch();
    stand.answer("MomentNode", json!({"node":super::stand::moment("local-row", 1)}));
    let original = bound(stand.database.node(epoch, "Moment".into(), "node".into(), "local-row".into(), true)).await.unwrap().unwrap();
    assert_eq!(stand.cell("Moment", &original, "visitsCount"), Cell::Int { value: 1 });
    bound(stand.database.set_local(epoch, "Moment".into(), "local-row".into(), "localPreviewUrl".into(), json!("file:///local-value").to_string())).await.unwrap();
    stand.settle().await;
    let connection = rusqlite::Connection::open(&path).unwrap();
    let checkpoint: (i64, i64, i64) = connection.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).unwrap();
    assert_eq!(checkpoint.0, 0);
    drop(connection);
    stand.answer("MomentNode", json!({"node":super::stand::moment("local-row", 2)}));
    let updated = bound(stand.database.node(epoch, "Moment".into(), "node".into(), "local-row".into(), true)).await.unwrap().unwrap();
    assert_eq!(stand.cell("Moment", &updated, "visitsCount"), Cell::Int { value: 2 });
    stand.settle().await;
    copy_image(&path, &server_base);
    copy_image(&path, &migration);
    let connection = rusqlite::Connection::open(&migration).unwrap();
    let checkpoint: (i64, i64, i64) = connection.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).unwrap();
    assert_eq!(checkpoint.0, 0);
    drop(connection);
    let mut changed: serde_json::Value = serde_json::from_str(super::stand::SPEC).unwrap();
    let models = changed["models"].as_array_mut().unwrap();
    let model = models.iter_mut().find(|model| model["key"] == "Moment").unwrap();
    let fields = model["fields"].as_array_mut().unwrap();
    let mut added = fields.iter().find(|field| field["name"] == "localPreviewUrl").unwrap().clone();
    added["name"] = json!("newOptionalValue");
    added["merge"] = serde_json::Value::Null;
    fields.push(added);
    let changed = changed.to_string();
    let migrated = Stand::prepare_at_with_spec(Some(migration.to_string_lossy().into_owned()), credentials.clone(), None, false, 1_000_000, changed.clone()).await;
    migrated.settle().await;
    assert!(migrated.database.peek_row(migrated.database.session_epoch(), "Moment".into(), "local-row".into()).unwrap().is_none(), "the changed fingerprint must clear the server row");
    let connection = rusqlite::Connection::open(&migration).unwrap();
    let carried: String = connection.query_row("SELECT text FROM local_pending WHERE model = 'Moment' AND row_id = 'local-row' AND path = 'localPreviewUrl' AND node = 0", [], |row| row.get(0)).unwrap();
    assert_eq!(carried, "file:///local-value");
    drop(connection);
    copy_image(&migration, &migration_base);
    for (phase, base, spec) in [("server", &server_base, super::stand::SPEC.to_string()), ("migration", &migration_base, changed)] {
        let wal = std::fs::read(format!("{}-wal", base.display())).unwrap();
        assert!(wal.len() >= 32, "{phase}");
        let page_size = u32::from_be_bytes(wal[8..12].try_into().unwrap()) as usize;
        let frame_size = 24 + if page_size == 1 { 65536 } else { page_size };
        assert!(page_size >= 512 && (wal.len() - 32) % frame_size == 0, "{phase}");
        let commits: Vec<_> = (32..wal.len()).step_by(frame_size).filter(|offset| u32::from_be_bytes(wal[offset + 4..offset + 8].try_into().unwrap()) != 0).map(|offset| offset + frame_size).collect();
        assert!(!commits.is_empty(), "{phase} must commit");
        for (k, end) in std::iter::once(32).chain(commits).enumerate() {
            let image = directory.path().join(format!("local-{phase}-{k}.sqlite"));
            std::fs::copy(base, &image).unwrap();
            std::fs::write(format!("{}-wal", image.display()), &wal[..end]).unwrap();
            let reopened = Stand::prepare_at_with_spec(Some(image.to_string_lossy().into_owned()), credentials.clone(), None, false, 1_000_000, spec.clone()).await;
            reopened.settle().await;
            let row = reopened.database.peek_row(reopened.database.session_epoch(), "Moment".into(), "local-row".into()).unwrap();
            let row_value = row.as_ref().and_then(|row| match reopened.cell("Moment", row, "localPreviewUrl") { Cell::Text { value } => Some(value), _ => None });
            let db = rusqlite::Connection::open(&image).unwrap();
            let pending: Option<String> = db.query_row("SELECT text FROM local_pending WHERE model = 'Moment' AND row_id = 'local-row' AND path = 'localPreviewUrl' AND node = 0", [], |row| row.get(0)).optional().unwrap();
            assert!(row_value.is_some() ^ pending.is_some(), "{phase} WAL commit {k}: row={row_value:?}, pending={pending:?}");
            assert_eq!(row_value.or(pending).as_deref(), Some("file:///local-value"), "{phase} WAL commit {k}");
            drop(db);
            bound(reopened.database.close_database()).await.unwrap();
        }
    }
    bound(migrated.database.close_database()).await.unwrap();
    bound(stand.database.close_database()).await.unwrap();
}
