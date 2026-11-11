use std::path::Path;
use std::sync::Arc;

use serde_json::json;
use super::stand::Stand;
use crate::ffi::command::{CommandRequest, CommandUpload};
use crate::ffi::selftest::rig_tests::bound;
use crate::ffi::tests::session_guards_tests::WatchedHeld;
use crate::ffi::upload_wire::{WireSource, WireUploadRequest, WireUploadSource};
use crate::ffi::wire::WireUploadStatus;

fn copy_files(from: &Path, to: &Path) {
    if !from.exists() { return; }
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_dir() { copy_files(&entry.path(), &to.join(entry.file_name())); }
        else { std::fs::copy(entry.path(), to.join(entry.file_name())).unwrap(); }
    }
}

fn files_under(root: &Path) -> Vec<std::path::PathBuf> {
    if !root.exists() { return Vec::new(); }
    std::fs::read_dir(root).unwrap().flat_map(|entry| {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_dir() { files_under(&entry.path()) } else { vec![entry.path()] }
    }).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_pending_source_consistent_at_every_commit() {
    for fail in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("live.sqlite");
        let credentials = Arc::new(WatchedHeld::default());
        let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
        stand.sign_in().await;
        stand.database.set_online(false).unwrap();
        let epoch = stand.database.session_epoch();
        let receipt = bound(stand.database.send(CommandRequest {
            epoch, model: "Message".into(), values_json: json!({"chatId":"c1","kind":"VIDEO","clientKey":"pending","media":{"kind":"VIDEO"}}).to_string(), args_json: "{}".into(),
            upload: Some(CommandUpload { action: "send_file".into(), request: WireUploadRequest { source: WireSource::Pending, variables_json: r#"{"chatId":"c1","kind":"VIDEO"}"#.into() } }),
        })).await.unwrap();
        let id = receipt.optimistic_value.unwrap().id;
        stand.settle().await;
        let db = rusqlite::Connection::open(&path).unwrap();
        let checkpoint: (i64, i64, i64) = db.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).unwrap();
        assert_eq!(checkpoint.0, 0);
        drop(db);
        if fail {
            bound(stand.database.fail_source(epoch, "Message".into(), id.clone(), "send_file".into(), "preparation".into())).await.unwrap();
        } else {
            let source = directory.path().join("source");
            std::fs::write(&source, b"image bytes").unwrap();
            bound(stand.database.supply_source(epoch, "Message".into(), id.clone(), "send_file".into(), WireUploadSource { path: source.to_string_lossy().into_owned(), name: None, mime: None, values_json: "{}".into(), variables_json: "{}".into() })).await.unwrap();
        }
        stand.settle().await;
        let base = directory.path().join("base.sqlite");
        std::fs::copy(&path, &base).unwrap();
        let wal = std::fs::read(format!("{}-wal", path.display())).unwrap();
        let size = u32::from_be_bytes(wal[8..12].try_into().unwrap()) as usize;
        let frame = 24 + if size == 1 { 65536 } else { size };
        let commits: Vec<_> = (32..wal.len()).step_by(frame).filter(|off| u32::from_be_bytes(wal[off + 4..off + 8].try_into().unwrap()) != 0).map(|off| off + frame).collect();
        assert!(!commits.is_empty());
        for (k, end) in std::iter::once(32).chain(commits).enumerate() {
            let image = directory.path().join(format!("image-{k}.sqlite"));
            std::fs::copy(&base, &image).unwrap();
            std::fs::write(format!("{}-wal", image.display()), &wal[..end]).unwrap();
            copy_files(Path::new(&format!("{}.files", path.display())), Path::new(&format!("{}.files", image.display())));
            let db = rusqlite::Connection::open(&image).unwrap();
            let before: String = db.query_row("SELECT status FROM sync_upload WHERE row_id = ?1 AND action = 'send_file'", [&id], |r| r.get(0)).unwrap();
            drop(db);
            let reopened = Stand::prepare_online(Some(image.to_string_lossy().into_owned()), credentials.clone(), None, false).await;
            reopened.settle().await;
            let row = reopened.database.peek_row(reopened.database.session_epoch(), "Message".into(), id.clone()).unwrap().expect("optimistic row");
            let upload = &row.uploads.iter().find(|u| u.action == "send_file").expect("upload status").upload;
            if let Some(url) = &upload.local_url {
                assert_eq!(std::fs::read(url.strip_prefix("file://").unwrap()).unwrap(), b"image bytes", "fail={fail}, k={k}");
                assert!(!fail, "failed source has a copy, k={k}");
            } else {
                assert_eq!(upload.status, WireUploadStatus::Failed, "fail={fail}, k={k}");
                let expected = if before == "failed" { "source_failed" } else { "missing_file" };
                assert_eq!(upload.error.as_ref().unwrap().kind, expected, "fail={fail}, k={k}");
            }
            let held = upload.local_url.as_deref().map(|url| std::path::PathBuf::from(url.strip_prefix("file://").unwrap()));
            let db = rusqlite::Connection::open(&image).unwrap();
            let transfer: i64 = db.query_row("SELECT transfer_id FROM sync_upload WHERE row_id = ?1 AND action = 'send_file'", [&id], |r| r.get(0)).unwrap();
            let transfer_file = std::path::PathBuf::from(format!("{}.files/transfers/{transfer}", image.display()));
            for file in files_under(Path::new(&format!("{}.files", image.display()))) {
                assert!(Some(&file) == held.as_ref() || file == transfer_file, "orphan file for fail={fail}, k={k}: {file:?}");
            }
            drop(db);
            bound(reopened.database.close_database()).await.unwrap();
        }
        bound(stand.database.close_database()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_subscription_event_consistent_at_every_commit() {
    use crate::ffi::{QuerySpec, ReadRequest, ReadValue};
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("live.sqlite");
    let credentials = Arc::new(WatchedHeld::default());
    let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
    stand.sign_in().await;
    let epoch = stand.database.session_epoch();
    let params = json!({"chatId":"c1"});
    let scope = stand.database.db.spec("Message").unwrap().selection_key("thread", params.as_object().unwrap()).unwrap();
    stand.answer("chatThread", super::thread_pages_tests::page(1, 1, false, 1));
    bound(stand.database.refresh(epoch, "Message".into(), Some("thread".into()), params.to_string())).await.unwrap();
    let window = stand.database.subscribe_values(epoch, ReadRequest::List { query: QuerySpec::json(r#"{"model":"Message","relation":"thread","params":{"chatId":"c1"}}"#) });
    bound(window.pull_next()).await.unwrap();
    let (events, token) = stand.events("Message").await;
    stand.settle().await;
    let db = rusqlite::Connection::open(&path).unwrap();
    let checkpoint: (i64, i64, i64) = db.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).unwrap();
    assert_eq!(checkpoint.0, 0);
    drop(db);
    let changed = super::stand::message("m1", "c1", "changed");
    stand.push(&token, json!({"area":"MESSAGE", "kind":"UPDATED", "entityId":"m1", "parentId":"c1", "message":changed}));
    assert!(matches!(bound(events.pull_next()).await, Some(ReadValue::Event { ref id, .. }) if id == "m1"));
    stand.settle().await;
    stand.push(&token, json!({"area":"MESSAGE", "kind":"DELETED", "entityId":"m1", "parentId":"c1"}));
    assert!(matches!(bound(events.pull_next()).await, Some(ReadValue::Event { ref id, .. }) if id == "m1"));
    stand.settle().await;
    let base = directory.path().join("base.sqlite");
    std::fs::copy(&path, &base).unwrap();
    let wal = std::fs::read(format!("{}-wal", path.display())).unwrap();
    let size = u32::from_be_bytes(wal[8..12].try_into().unwrap()) as usize;
    let frame = 24 + if size == 1 { 65536 } else { size };
    let commits: Vec<_> = (32..wal.len()).step_by(frame).filter(|off| u32::from_be_bytes(wal[off + 4..off + 8].try_into().unwrap()) != 0).map(|off| off + frame).collect();
    assert!(!commits.is_empty());
    let mut seen = [false; 3];
    for (k, end) in std::iter::once(32).chain(commits).enumerate() {
        let image = directory.path().join(format!("event-{k}.sqlite"));
        std::fs::copy(&base, &image).unwrap();
        std::fs::write(format!("{}-wal", image.display()), &wal[..end]).unwrap();
        let reopened = Stand::prepare_online(Some(image.to_string_lossy().into_owned()), credentials.clone(), None, false).await;
        reopened.settle().await;
        let db = rusqlite::Connection::open(&image).unwrap();
        let row: Option<String> = rusqlite::OptionalExtension::optional(db.query_row("SELECT body FROM row_Message WHERE id = 'm1'", [], |r| r.get(0))).unwrap();
        let member: i64 = db.query_row("SELECT count(*) FROM scope_member WHERE model = 'Message' AND scope_key = ?1 AND row_id = 'm1'", [scope.as_str()], |r| r.get(0)).unwrap();
        let tomb: i64 = db.query_row("SELECT count(*) FROM tombstone WHERE model = 'Message' AND id = 'm1'", [], |r| r.get(0)).unwrap();
        let old = row.as_deref() == Some("message 1") && member == 1 && tomb == 0;
        let new = row.as_deref() == Some("changed") && member == 1 && tomb == 0;
        let deleted = row.is_none() && member == 0 && tomb == 1;
        assert!(old || new || deleted, "WAL commit {k}: row={row:?}, member={member}, tombstone={tomb}");
        seen[0] |= old;
        seen[1] |= new;
        seen[2] |= deleted;
        drop(db);
        bound(reopened.database.close_database()).await.unwrap();
    }
    assert_eq!(seen, [true; 3]);
    drop(window);
    bound(stand.database.close_database()).await.unwrap();
}
#[tokio::test(flavor = "multi_thread")]
async fn consumer_cancel_upload_consistent_at_every_commit() {
    for sent in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("live.sqlite");
        let source = directory.path().join("source");
        std::fs::write(&source, b"image").unwrap();
        let credentials = Arc::new(WatchedHeld::default());
        let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
        stand.sign_in().await;
        if sent {
            stand.answer("CreateBlob", json!({"directUploadCreate":{"directUpload":{"url":"https://storage/file","headers":[],"signedId":"signed","blobId":"blob"}}}));
            stand.script_put(200, Some("etag"));
            stand.hold(&["MomentCreateFile"]);
        } else { stand.database.set_online(false).unwrap(); }
        let mark = stand.request_mark();
        let epoch = stand.database.session_epoch();
        let receipt = bound(stand.database.send(CommandRequest {
            epoch, model: "Moment".into(), values_json: json!({"createdAt":"2026-11-08T00:00:00Z","updatedAt":"2026-11-08T00:00:00Z"}).to_string(), args_json: "{}".into(),
            upload: Some(CommandUpload { action: "create_file".into(), request: WireUploadRequest { source: WireSource::File { path: source.to_string_lossy().into_owned(), name: None, mime: None }, variables_json: "{}".into() } }),
        })).await.unwrap();
        let id = receipt.optimistic_value.unwrap().id;
        if sent { stand.receive_after(mark, "MomentCreateFile", None, 1).await; } else { stand.settle().await; }
        let db = rusqlite::Connection::open(&path).unwrap();
        let (transfer, cycle, operation): (i64, i64, Option<i64>) = db.query_row("SELECT transfer_id, cycle_id, op_id FROM sync_upload WHERE row_id = ?1 AND action = 'create_file'", [&id], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).unwrap();
        let checkpoint: (i64, i64, i64) = db.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).unwrap();
        assert_eq!(checkpoint.0, 0);
        drop(db);
        let files = directory.path().join("saved.files");
        copy_files(Path::new(&format!("{}.files", path.display())), &files);
        let cancelled = bound(stand.database.cancel_upload(epoch, "Moment".into(), id.clone(), "create_file".into())).await.unwrap();
        assert_eq!(cancelled, !sent);
        if !sent { stand.settle().await; }
        let base = directory.path().join("base.sqlite");
        std::fs::copy(&path, &base).unwrap();
        let wal_path = format!("{}-wal", path.display());
        let wal = std::fs::read(&wal_path).unwrap_or_default();
        let commits = if wal.len() >= 32 {
            let size = u32::from_be_bytes(wal[8..12].try_into().unwrap()) as usize;
            let frame = 24 + if size == 1 { 65536 } else { size };
            (32..wal.len()).step_by(frame).filter(|off| u32::from_be_bytes(wal[off + 4..off + 8].try_into().unwrap()) != 0).map(|off| off + frame).collect::<Vec<_>>()
        } else { Vec::new() };
        if !sent { assert!(!commits.is_empty()); }
        for (k, end) in std::iter::once(0).chain(commits).enumerate() {
            let image = directory.path().join(format!("cancel-{k}.sqlite"));
            std::fs::copy(&base, &image).unwrap();
            if end != 0 { std::fs::write(format!("{}-wal", image.display()), &wal[..end]).unwrap(); }
            copy_files(&files, Path::new(&format!("{}.files", image.display())));
            let reopened = Stand::prepare_online(Some(image.to_string_lossy().into_owned()), credentials.clone(), None, false).await;
            reopened.settle().await;
            let db = rusqlite::Connection::open(&image).unwrap();
            let upload: i64 = db.query_row("SELECT count(*) FROM sync_upload WHERE transfer_id = ?1", [transfer], |r| r.get(0)).unwrap();
            let outcome: i64 = db.query_row("SELECT count(*) FROM transfer_outcome WHERE transfer_id = ?1 AND cycle_id = ?2 AND kind = 'discarded'", rusqlite::params![transfer, cycle], |r| r.get(0)).unwrap();
            let row = reopened.database.peek_row(reopened.database.session_epoch(), "Moment".into(), id.clone()).unwrap();
            let op: i64 = db.query_row("SELECT count(*) FROM op WHERE seq = ?1", [operation], |r| r.get(0)).unwrap();
            assert!((upload == 1 && row.is_some() && op == 1) || (upload == 0 && row.is_none() && op == 0 && outcome == 1), "sent={sent}, k={k}: upload={upload}, row={}, op={op}, outcome={outcome}", row.is_some());
            if sent { assert_eq!(upload, 1, "sent cancellation changed upload at k={k}"); }
            drop(db);
            bound(reopened.database.close_database()).await.unwrap();
        }
        if sent { stand.release(&["MomentCreateFile"]); }
        stand.database.close_now();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_batch_failed_item_consistent_at_every_commit() {
    use crate::ffi::upload_wire::{WireUploadBatch, WireUploadItem};
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("live.sqlite");
    let credentials = Arc::new(WatchedHeld::default());
    let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
    stand.sign_in().await;
    stand.database.set_online(false).unwrap();
    let items = (0..2).map(|index| {
        let source = directory.path().join(format!("source-{index}"));
        std::fs::write(&source, format!("bytes-{index}")).unwrap();
        WireUploadItem { values_json: json!({"chatId":"c1","kind":"TEXT","clientKey":format!("batch-{index}"),"createdAt":"2026-11-08T00:00:00Z","updatedAt":"2026-11-08T00:00:00Z","mediaGroup":{"id":"batch","position":index,"size":2}}).to_string(), source: WireSource::File { path: source.to_string_lossy().into_owned(), name: None, mime: None }, variables_json: "{}".into() }
    }).collect();
    let receipt = bound(stand.database.upload_create_batch(stand.database.session_epoch(), "Message".into(), items, WireUploadBatch { operation: "send_files".into(), variables_json: r#"{"chatId":"c1"}"#.into() })).await.unwrap();
    stand.settle().await;
    let ids: Vec<_> = receipt.items.iter().map(|item| item.optimistic_value.id.clone()).collect();
    let db = rusqlite::Connection::open(&path).unwrap();
    let transfers: Vec<i64> = ids.iter().map(|id| db.query_row("SELECT transfer_id FROM sync_upload WHERE row_id = ?1 AND action = 'send_files'", [id], |r| r.get(0)).unwrap()).collect();
    let checkpoint: (i64, i64, i64) = db.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).unwrap();
    assert_eq!(checkpoint.0, 0);
    drop(db);
    let files = directory.path().join("saved.files");
    copy_files(Path::new(&format!("{}.files", path.display())), &files);
    stand.answer("CreateBlob", json!({"directUploadCreate":{"directUpload":{"url":"https://storage/file","headers":[],"signedId":"signed","blobId":"blob"}}}));
    stand.script_put(200, Some("etag"));
    stand.script_put(400, None);
    stand.database.set_online(true).unwrap();
    stand.settle().await;
    assert_eq!(stand.requests("MessageAlbumSend"), 0);
    let failed = ids.iter().find(|id| stand.database.peek_row(stand.database.session_epoch(), "Message".into(), (*id).clone()).unwrap().unwrap().uploads.iter().any(|u| u.upload.status == WireUploadStatus::Failed)).expect("one item failed");
    let failure = stand.database.peek_row(stand.database.session_epoch(), "Message".into(), failed.clone()).unwrap().unwrap().uploads[0].upload.error.clone().unwrap();
    let ready = ids.iter().find(|id| *id != failed).unwrap();
    let ready_row = stand.database.peek_row(stand.database.session_epoch(), "Message".into(), ready.clone()).unwrap().unwrap();
    assert_eq!(ready_row.uploads[0].upload.status, WireUploadStatus::Sending);
    assert_eq!(ready_row.uploads[0].upload.stage, "mutation");
    let base = directory.path().join("base.sqlite");
    std::fs::copy(&path, &base).unwrap();
    let wal = std::fs::read(format!("{}-wal", path.display())).unwrap();
    let size = u32::from_be_bytes(wal[8..12].try_into().unwrap()) as usize;
    let frame = 24 + if size == 1 { 65536 } else { size };
    let commits: Vec<_> = (32..wal.len()).step_by(frame).filter(|off| u32::from_be_bytes(wal[off + 4..off + 8].try_into().unwrap()) != 0).map(|off| off + frame).collect();
    assert!(!commits.is_empty());
    let mut saw_failed = false;
    for (k, end) in std::iter::once(32).chain(commits).enumerate() {
        let image = directory.path().join(format!("batch-{k}.sqlite"));
        std::fs::copy(&base, &image).unwrap();
        std::fs::write(format!("{}-wal", image.display()), &wal[..end]).unwrap();
        copy_files(&files, Path::new(&format!("{}.files", image.display())));
        let reopened = Stand::prepare_online(Some(image.to_string_lossy().into_owned()), credentials.clone(), None, false).await;
        reopened.settle().await;
        let db = rusqlite::Connection::open(&image).unwrap();
        for (index, id) in ids.iter().enumerate() {
            let row = reopened.database.peek_row(reopened.database.session_epoch(), "Message".into(), id.clone()).unwrap().expect("group row");
            let upload = &row.uploads.iter().find(|entry| entry.action == "send_files").expect("group upload").upload;
            let present: i64 = db.query_row("SELECT count(*) FROM sync_upload WHERE transfer_id = ?1", [transfers[index]], |r| r.get(0)).unwrap();
            assert_eq!(present, 1, "WAL commit {k}: missing transfer {index}");
            let url = upload.local_url.as_ref().expect("group file");
            assert_eq!(std::fs::read(url.strip_prefix("file://").unwrap()).unwrap(), format!("bytes-{index}").as_bytes(), "WAL commit {k}");
            if upload.status == WireUploadStatus::Failed {
                assert_eq!(id, failed, "WAL commit {k}: wrong item failed");
                assert_eq!(upload.error.as_ref(), Some(&failure), "WAL commit {k}: failure changed");
                saw_failed = true;
            }
        }
        assert_eq!(reopened.requests("MessageAlbumSend"), 0, "WAL commit {k}");
        for file in files_under(Path::new(&format!("{}.files", image.display()))) {
            assert!(transfers.iter().any(|id| file == Path::new(&format!("{}.files/transfers/{id}", image.display()))), "WAL commit {k}: orphan {file:?}");
        }
        drop(db);
        bound(reopened.database.close_database()).await.unwrap();
    }
    assert!(saw_failed);
    bound(stand.database.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_session_reset_consistent_at_every_commit() {
    use crate::ffi::{CredentialStore, SessionStanding};
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("live.sqlite");
    let source = directory.path().join("source");
    std::fs::write(&source, b"upload bytes").unwrap();
    let credentials = Arc::new(WatchedHeld::default());
    let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
    stand.sign_in().await;
    let epoch = stand.database.session_epoch();
    stand.answer("chatThread", super::thread_pages_tests::page(1, 1, false, 1));
    bound(stand.database.refresh(epoch, "Message".into(), Some("thread".into()), r#"{"chatId":"c1"}"#.into())).await.unwrap();
    stand.database.set_online(false).unwrap();
    let receipt = bound(stand.database.send(CommandRequest {
        epoch, model: "Moment".into(), values_json: json!({"createdAt":"2026-11-08T00:00:00Z","updatedAt":"2026-11-08T00:00:00Z"}).to_string(), args_json: "{}".into(),
        upload: Some(CommandUpload { action: "create_file".into(), request: WireUploadRequest { source: WireSource::File { path: source.to_string_lossy().into_owned(), name: None, mime: None }, variables_json: "{}".into() } }),
    })).await.unwrap();
    let id = receipt.optimistic_value.unwrap().id;
    stand.settle().await;
    let secret = credentials.get().unwrap().unwrap();
    let db = rusqlite::Connection::open(&path).unwrap();
    let transfer: i64 = db.query_row("SELECT transfer_id FROM sync_upload WHERE row_id = ?1", [&id], |r| r.get(0)).unwrap();
    let checkpoint: (i64, i64, i64) = db.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).unwrap();
    assert_eq!(checkpoint.0, 0);
    drop(db);
    let files = directory.path().join("saved.files");
    copy_files(Path::new(&format!("{}.files", path.display())), &files);
    bound(stand.database.session_reset()).await.unwrap();
    stand.settle().await;
    let base = directory.path().join("base.sqlite");
    std::fs::copy(&path, &base).unwrap();
    let wal = std::fs::read(format!("{}-wal", path.display())).unwrap();
    let size = u32::from_be_bytes(wal[8..12].try_into().unwrap()) as usize;
    let frame = 24 + if size == 1 { 65536 } else { size };
    let commits: Vec<_> = (32..wal.len()).step_by(frame).filter(|off| u32::from_be_bytes(wal[off + 4..off + 8].try_into().unwrap()) != 0).map(|off| off + frame).collect();
    assert!(!commits.is_empty());
    let mut seen = [false; 2];
    for (k, end) in std::iter::once(32).chain(commits).enumerate() {
        let image = directory.path().join(format!("reset-{k}.sqlite"));
        std::fs::copy(&base, &image).unwrap();
        std::fs::write(format!("{}-wal", image.display()), &wal[..end]).unwrap();
        copy_files(&files, Path::new(&format!("{}.files", image.display())));
        let restored_credentials = Arc::new(WatchedHeld::default());
        restored_credentials.set(secret.clone()).unwrap();
        let reopened = Stand::prepare_online(Some(image.to_string_lossy().into_owned()), restored_credentials.clone(), None, false).await;
        reopened.settle().await;
        let status = reopened.database.session_status().unwrap().phase;
        let db = rusqlite::Connection::open(&image).unwrap();
        let counts: Vec<i64> = ["row_Message", "row_Moment", "query_window", "op", "sync_upload", "transfer_outcome"].iter().map(|table| db.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0)).unwrap()).collect();
        let copy = Path::new(&format!("{}.files/transfers/{transfer}", image.display())).exists();
        if status == SessionStanding::Active {
            assert!(counts[0] > 0 && counts[1] > 0 && counts[2] > 0 && counts[3] > 0 && counts[4] > 0 && copy, "WAL {k}: active counts={counts:?}, copy={copy}");
            assert_eq!(restored_credentials.get().unwrap().as_deref(), Some(secret.as_str()));
            seen[0] = true;
        } else {
            assert_eq!(status, SessionStanding::None, "WAL {k}");
            assert!(counts.iter().all(|count| *count == 0) && !copy, "WAL {k}: cleared counts={counts:?}, copy={copy}");
            assert!(restored_credentials.get().unwrap().is_none(), "WAL {k}: credential retained");
            seen[1] = true;
        }
        drop(db);
        bound(reopened.database.close_database()).await.unwrap();
    }
    assert_eq!(seen, [true; 2]);
    bound(stand.database.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_model_fingerprint_consistent_at_every_commit() {
    use crate::ffi::wire::Cell;
    use crate::schema::{FieldSpec, ModelSpec, Storage};
    use crate::value::ScalarKind;
    use rusqlite::OptionalExtension;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("live.sqlite");
    let base = directory.path().join("base.sqlite");
    let credentials = Arc::new(WatchedHeld::default());
    let mut local = ModelSpec::new("LocalNote", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str)]);
    local.storage = Storage::Local;
    let mut original: serde_json::Value = serde_json::from_str(super::stand::SPEC).unwrap();
    original["models"].as_array_mut().unwrap().push(serde_json::to_value(local).unwrap());
    let first = Stand::prepare_at_with_spec(Some(path.to_string_lossy().into_owned()), credentials.clone(), None, false, 1_000_000, original.to_string()).await;
    first.database.set_online(true).unwrap();
    first.sign_in().await;
    let epoch = first.database.session_epoch();
    let note = bound(first.database.create(epoch, "LocalNote".into(), json!({"text":"kept"}).to_string(), "{}".into())).await.unwrap().optimistic_value.unwrap().id;
    first.answer("chatThread", super::thread_pages_tests::page(1, 1, false, 1));
    bound(first.database.refresh(epoch, "Message".into(), Some("thread".into()), r#"{"chatId":"c1"}"#.into())).await.unwrap();
    first.answer("MomentNode", json!({"node":super::stand::moment("m1", 1)}));
    bound(first.database.node(epoch, "Moment".into(), "node".into(), "m1".into(), true)).await.unwrap().unwrap();
    bound(first.database.set_local(epoch, "Moment".into(), "m1".into(), "localPreviewUrl".into(), json!("file:///local-value").to_string())).await.unwrap();
    first.settle().await;
    bound(first.database.close_database()).await.unwrap();
    drop(first);
    let db = rusqlite::Connection::open(&path).unwrap();
    let checkpoint: (i64, i64, i64) = db.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).unwrap();
    assert_eq!(checkpoint.0, 0);
    drop(db);
    let model = original["models"].as_array_mut().unwrap().iter_mut().find(|model| model["key"] == "Moment").unwrap();
    let fields = model["fields"].as_array_mut().unwrap();
    let mut added = fields.iter().find(|field| field["name"] == "localPreviewUrl").unwrap().clone();
    added["name"] = json!("fingerprintChange");
    added["merge"] = serde_json::Value::Null;
    fields.push(added);
    let changed = original.to_string();
    let migrating = Stand::prepare_at_with_spec(Some(path.to_string_lossy().into_owned()), credentials.clone(), None, false, 1_000_000, changed.clone()).await;
    migrating.settle().await;
    std::fs::copy(&path, &base).unwrap();
    let wal = std::fs::read(format!("{}-wal", path.display())).unwrap();
    let size = u32::from_be_bytes(wal[8..12].try_into().unwrap()) as usize;
    let frame = 24 + if size == 1 { 65536 } else { size };
    let commits: Vec<_> = (32..wal.len()).step_by(frame).filter(|off| u32::from_be_bytes(wal[off + 4..off + 8].try_into().unwrap()) != 0).map(|off| off + frame).collect();
    assert!(!commits.is_empty());
    for (k, end) in std::iter::once(32).chain(commits).enumerate() {
        let image = directory.path().join(format!("fingerprint-{k}.sqlite"));
        std::fs::copy(&base, &image).unwrap();
        std::fs::write(format!("{}-wal", image.display()), &wal[..end]).unwrap();
        let reopened = Stand::prepare_at_with_spec(Some(image.to_string_lossy().into_owned()), credentials.clone(), None, false, 1_000_000, changed.clone()).await;
        reopened.settle().await;
        let epoch = reopened.database.session_epoch();
        let note_row = reopened.database.peek_row(epoch, "LocalNote".into(), note.clone()).unwrap().expect("local row");
        assert_eq!(reopened.cell("LocalNote", &note_row, "text"), Cell::Text { value: "kept".into() }, "WAL {k}");
        let row = reopened.database.peek_row(epoch, "Moment".into(), "m1".into()).unwrap();
        let message = reopened.database.peek_row(epoch, "Message".into(), "m1".into()).unwrap();
        assert_eq!(row.is_some(), message.is_some(), "WAL {k}: server models were not cleared together");
        let value = row.as_ref().and_then(|row| match reopened.cell("Moment", row, "localPreviewUrl") { Cell::Text { value } => Some(value), _ => None });
        let db = rusqlite::Connection::open(&image).unwrap();
        let pending: Option<String> = db.query_row("SELECT text FROM local_pending WHERE model = 'Moment' AND row_id = 'm1' AND path = 'localPreviewUrl' AND node = 0", [], |r| r.get(0)).optional().unwrap();
        assert!(value.is_some() ^ pending.is_some(), "WAL {k}: row={value:?}, pending={pending:?}");
        assert_eq!(value.or(pending).as_deref(), Some("file:///local-value"), "WAL {k}");
        drop(db);
        bound(reopened.database.close_database()).await.unwrap();
    }
    bound(migrating.database.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_server_revoked_consistent_at_every_commit() {
    use crate::ffi::{CredentialStore, SessionStanding};
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("live.sqlite");
    let source = directory.path().join("source");
    std::fs::write(&source, b"upload bytes").unwrap();
    let credentials = Arc::new(WatchedHeld::default());
    let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
    stand.sign_in().await;
    let epoch = stand.database.session_epoch();
    stand.answer("chatThread", super::thread_pages_tests::page(1, 1, false, 1));
    bound(stand.database.refresh(epoch, "Message".into(), Some("thread".into()), r#"{"chatId":"c1"}"#.into())).await.unwrap();
    stand.database.set_online(false).unwrap();
    let receipt = bound(stand.database.send(CommandRequest {
        epoch, model: "Moment".into(), values_json: json!({"createdAt":"2026-11-08T00:00:00Z","updatedAt":"2026-11-08T00:00:00Z"}).to_string(), args_json: "{}".into(),
        upload: Some(CommandUpload { action: "create_file".into(), request: WireUploadRequest { source: WireSource::File { path: source.to_string_lossy().into_owned(), name: None, mime: None }, variables_json: "{}".into() } }),
    })).await.unwrap();
    let id = receipt.optimistic_value.unwrap().id;
    let mutation = bound(stand.database.update(epoch, "Message".into(), "m1".into(), r#"{"body":"edited"}"#.into(), "{}".into())).await.unwrap();
    stand.settle().await;
    let secret = credentials.get().unwrap().unwrap();
    let db = rusqlite::Connection::open(&path).unwrap();
    let transfer: i64 = db.query_row("SELECT transfer_id FROM sync_upload WHERE row_id = ?1", [&id], |r| r.get(0)).unwrap();
    let checkpoint: (i64, i64, i64) = db.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).unwrap();
    assert_eq!(checkpoint.0, 0);
    drop(db);
    let files = directory.path().join("saved.files");
    copy_files(Path::new(&format!("{}.files", path.display())), &files);
    stand.hold(&["CreateBlob"]);
    stand.answer_next("MessageUpdate", json!({"errors":[{"message":"session ended","extensions":{"code":"SESSION_ENDED","reasonCode":"session_ended"}}],"data":{"messageUpdate":null}}));
    stand.database.set_online(true).unwrap();
    let outcome = bound(stand.database.confirmed(mutation, false)).await;
    assert!(outcome.is_err(), "revoked call must fail: {outcome:?}");
    stand.settle().await;
    let base = directory.path().join("base.sqlite");
    std::fs::copy(&path, &base).unwrap();
    let wal = std::fs::read(format!("{}-wal", path.display())).unwrap();
    let size = u32::from_be_bytes(wal[8..12].try_into().unwrap()) as usize;
    let frame = 24 + if size == 1 { 65536 } else { size };
    let commits: Vec<_> = (32..wal.len()).step_by(frame).filter(|off| u32::from_be_bytes(wal[off + 4..off + 8].try_into().unwrap()) != 0).map(|off| off + frame).collect();
    assert!(!commits.is_empty());
    let mut seen = [false; 2];
    for (k, end) in std::iter::once(32).chain(commits).enumerate() {
        let image = directory.path().join(format!("revoked-{k}.sqlite"));
        std::fs::copy(&base, &image).unwrap();
        std::fs::write(format!("{}-wal", image.display()), &wal[..end]).unwrap();
        copy_files(&files, Path::new(&format!("{}.files", image.display())));
        let restored_credentials = Arc::new(WatchedHeld::default());
        restored_credentials.set(secret.clone()).unwrap();
        let reopened = Stand::prepare_online(Some(image.to_string_lossy().into_owned()), restored_credentials.clone(), None, false).await;
        reopened.settle().await;
        let status = reopened.database.session_status().unwrap().phase;
        let db = rusqlite::Connection::open(&image).unwrap();
        let counts: Vec<i64> = ["row_Message", "row_Moment", "query_window", "op", "sync_upload", "transfer_outcome"].iter().map(|table| db.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0)).unwrap()).collect();
        let copy = Path::new(&format!("{}.files/transfers/{transfer}", image.display())).exists();
        if status == SessionStanding::Active {
            assert!(counts[0] > 0 && counts[1] > 0 && counts[2] > 0 && counts[3] > 0 && counts[4] > 0 && copy, "WAL {k}: active counts={counts:?}, copy={copy}");
            assert_eq!(restored_credentials.get().unwrap().as_deref(), Some(secret.as_str()));
            seen[0] = true;
        } else {
            assert_eq!(status, SessionStanding::None, "WAL {k}");
            assert!(counts.iter().all(|count| *count == 0) && !copy, "WAL {k}: cleared counts={counts:?}, copy={copy}");
            assert!(restored_credentials.get().unwrap().is_none(), "WAL {k}: credential retained");
            seen[1] = true;
        }
        drop(db);
        bound(reopened.database.close_database()).await.unwrap();
    }
    assert_eq!(seen, [true; 2]);
    stand.release(&["CreateBlob"]);
    bound(stand.database.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_mutation_pair_consistent_at_every_commit() {
    use rusqlite::OptionalExtension;
    for dropped in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("live.sqlite");
        let credentials = Arc::new(WatchedHeld::default());
        let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
        stand.sign_in().await;
        let epoch = stand.database.session_epoch();
        stand.answer("chatThread", super::thread_pages_tests::page(1, 1, false, 1));
        bound(stand.database.refresh(epoch, "Message".into(), Some("thread".into()), r#"{"chatId":"c1"}"#.into())).await.unwrap();
        stand.database.set_online(false).unwrap();
        let receipt = bound(stand.database.update(epoch, "Message".into(), "m1".into(), r#"{"body":"first"}"#.into(), "{}".into())).await.unwrap();
        let op = receipt.operation.unwrap();
        stand.settle().await;
        let db = rusqlite::Connection::open(&path).unwrap();
        let start: i64 = db.query_row("SELECT sequence FROM encrypted_session", [], |r| r.get(0)).unwrap();
        let checkpoint: (i64, i64, i64) = db.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).unwrap();
        assert_eq!(checkpoint.0, 0);
        drop(db);
        stand.answer("MessageUpdate", json!({"messageUpdate":{"message":super::stand::message("m1", "c1", "first")}}));
        stand.hold(&["MessageUpdate"]);
        let mark = stand.request_mark();
        stand.database.set_online(true).unwrap();
        stand.receive_after(mark, "MessageUpdate", None, 1).await;
        let db = rusqlite::Connection::open(&path).unwrap();
        let pair: (String, i64) = db.query_row("SELECT request_id, sequence FROM encrypted_pending", [], |r| Ok((r.get(0)?, r.get(1)?))).unwrap();
        assert_eq!(pair.1, start + 1);
        drop(db);
        if dropped {
            stand.database.set_online(false).unwrap();
            stand.settle().await;
            let db = rusqlite::Connection::open(&path).unwrap();
            let retained: (String, i64) = db.query_row("SELECT request_id, sequence FROM encrypted_pending", [], |r| Ok((r.get(0)?, r.get(1)?))).unwrap();
            assert_eq!(retained, pair);
            drop(db);
            stand.database.set_online(true).unwrap();
            stand.receive_after(mark, "MessageUpdate", None, 2).await;
        }
        stand.release(&["MessageUpdate"]);
        assert!(bound(stand.database.confirmed(receipt, false)).await.is_ok());
        stand.settle().await;
        let base = directory.path().join("base.sqlite");
        std::fs::copy(&path, &base).unwrap();
        let wal = std::fs::read(format!("{}-wal", path.display())).unwrap();
        let size = u32::from_be_bytes(wal[8..12].try_into().unwrap()) as usize;
        let frame = 24 + if size == 1 { 65536 } else { size };
        let commits: Vec<_> = (32..wal.len()).step_by(frame).filter(|off| u32::from_be_bytes(wal[off + 4..off + 8].try_into().unwrap()) != 0).map(|off| off + frame).collect();
        assert!(!commits.is_empty());
        let mut saw_pending = false;
        for (k, end) in std::iter::once(32).chain(commits).enumerate() {
            let image = directory.path().join(format!("pair-{k}.sqlite"));
            std::fs::copy(&base, &image).unwrap();
            std::fs::write(format!("{}-wal", image.display()), &wal[..end]).unwrap();
            let db = rusqlite::Connection::open(&image).unwrap();
            let saved: Option<(String, i64)> = db.query_row("SELECT request_id, sequence FROM encrypted_pending", [], |r| Ok((r.get(0)?, r.get(1)?))).optional().unwrap();
            if let Some(saved) = &saved { assert_eq!(saved, &pair, "dropped={dropped}, WAL {k}"); saw_pending = true; }
            let queued: i64 = db.query_row("SELECT count(*) FROM op WHERE seq = ?1 AND status IN ('pending', 'sending')", [op], |r| r.get(0)).unwrap();
            let answered: i64 = db.query_row("SELECT count(*) FROM encrypted_answer WHERE operation_id = ?1", [op], |r| r.get(0)).unwrap();
            drop(db);
            let reopened = Stand::prepare_online(Some(image.to_string_lossy().into_owned()), credentials.clone(), None, false).await;
            reopened.answer("MessageUpdate", json!({"messageUpdate":{"message":super::stand::message("m1", "c1", "first")}}));
            if queued != 0 && answered == 0 {
                reopened.hold(&["MessageUpdate"]);
                let mark = reopened.request_mark();
                reopened.database.set_online(true).unwrap();
                reopened.receive_after(mark, "MessageUpdate", None, 1).await;
                if saved.is_some() {
                    let sent = reopened.request_ids().into_iter().find(|(name, _)| name == "MessageUpdate").unwrap().1;
                    assert_eq!(sent.as_str(), Some(pair.0.as_str()), "WAL {k}");
                }
                reopened.release(&["MessageUpdate"]);
                reopened.settle().await;
            } else { reopened.database.set_online(true).unwrap(); }
            reopened.answer("MessageUpdate", json!({"messageUpdate":{"message":super::stand::message("m1", "c1", "second")}}));
            let next = bound(reopened.database.update(reopened.database.session_epoch(), "Message".into(), "m1".into(), r#"{"body":"second"}"#.into(), "{}".into())).await.unwrap();
            assert!(bound(reopened.database.confirmed(next, false)).await.is_ok(), "WAL {k}");
            let sent = reopened.request_ids();
            let next_id = sent.iter().rev().find(|(name, _)| name == "MessageUpdate").unwrap().1.as_str().unwrap();
            assert_ne!(next_id, pair.0, "WAL {k}: a new mutation reused the previous request id");
            let db = rusqlite::Connection::open(&image).unwrap();
            let sequence: i64 = db.query_row("SELECT sequence FROM encrypted_session", [], |r| r.get(0)).unwrap();
            assert_eq!(sequence, start + 2, "dropped={dropped}, WAL {k}");
            drop(db);
            bound(reopened.database.close_database()).await.unwrap();
        }
        assert!(saw_pending);
        bound(stand.database.close_database()).await.unwrap();
    }
}
