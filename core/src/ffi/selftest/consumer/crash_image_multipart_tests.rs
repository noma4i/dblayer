use std::path::Path;
use std::sync::Arc;
use serde_json::json;
use super::stand::Stand;
use crate::ffi::command::{CommandRequest, CommandUpload};
use crate::ffi::selftest::rig_tests::bound;
use crate::ffi::tests::session_guards_tests::WatchedHeld;
use crate::ffi::upload_wire::{WireSource, WireUploadRequest};

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
async fn consumer_multipart_consistent_at_every_commit() {
    use rusqlite::OptionalExtension;
    for discarded in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("live.sqlite");
        let source = directory.path().join("source");
        let part_size = crate::owners::write::upload::UploadConfig::default().part_size;
        std::fs::File::create(&source).unwrap().set_len(part_size * 2).unwrap();
        let credentials = Arc::new(WatchedHeld::default());
        let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
        stand.sign_in().await;
        stand.answer_multipart();
        for part in 1..=2 {
            stand.answer_next("MultipartUploadPresignParts", json!({"data":{"multipartUploadPresign":{"parts":[{"partNumber":part,"url":format!("https://storage/{part}")}]}}}));
            stand.script_put(200, Some("etag"));
        }
        if discarded { stand.fail_next("MultipartUploadComplete", crate::ffi::DbError::Http { status: 400, detail: "refused".into() }); }
        stand.answer("MomentCreateFile", json!({"momentCreate":{"moment":super::stand::moment("server", 1)}}));
        stand.hold_put();
        let epoch = stand.database.session_epoch();
        let receipt = bound(stand.database.send(CommandRequest { epoch, model: "Moment".into(), values_json: json!({"createdAt":"2026-11-08T00:00:00Z","updatedAt":"2026-11-08T00:00:00Z"}).to_string(), args_json: "{}".into(), upload: Some(CommandUpload { action: "create_file".into(), request: WireUploadRequest { source: WireSource::File { path: source.to_string_lossy().into_owned(), name: None, mime: None }, variables_json: "{}".into() } }) })).await.unwrap();
        let id = receipt.optimistic_value.as_ref().unwrap().id.clone();
        stand.put_started().await;
        let db = rusqlite::Connection::open(&path).unwrap();
        let checkpoint: (i64, i64, i64) = db.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).unwrap();
        assert_eq!(checkpoint.0, 0);
        drop(db);
        let files = directory.path().join("saved.files");
        copy_files(Path::new(&format!("{}.files", path.display())), &files);
        stand.release_put();
        let result = bound(stand.database.confirmed(receipt, false)).await;
        if discarded {
            assert!(result.is_err(), "complete must fail");
            stand.database.set_online(false).unwrap();
            stand.settle().await;
            let db = rusqlite::Connection::open(&path).unwrap();
            let parts: i64 = db.query_row("SELECT count(*) FROM sync_upload_part WHERE row_id = ?1 AND etag IS NOT NULL", [&id], |r| r.get(0)).unwrap();
            assert_eq!(parts, 2);
            let upload: String = db.query_row("SELECT upload_id FROM sync_upload WHERE row_id = ?1", [&id], |r| r.get(0)).unwrap();
            assert!(upload.contains("upload-1"));
            drop(db);
            bound(stand.database.discard(epoch, "Moment".into(), id.clone(), Some("create_file".into()))).await.unwrap();
        } else { assert!(result.is_ok(), "completion: {result:?}"); }
        stand.settle().await;
        let base = directory.path().join("base.sqlite");
        std::fs::copy(&path, &base).unwrap();
        let wal = std::fs::read(format!("{}-wal", path.display())).unwrap();
        let size = u32::from_be_bytes(wal[8..12].try_into().unwrap()) as usize;
        let frame = 24 + if size == 1 { 65536 } else { size };
        let commits: Vec<_> = (32..wal.len()).step_by(frame).filter(|off| u32::from_be_bytes(wal[off + 4..off + 8].try_into().unwrap()) != 0).map(|off| off + frame).collect();
        assert!(!commits.is_empty());
        for (k, end) in std::iter::once(32).chain(commits).enumerate() {
            let image = directory.path().join(format!("multipart-{k}.sqlite"));
            std::fs::copy(&base, &image).unwrap();
            std::fs::write(format!("{}-wal", image.display()), &wal[..end]).unwrap();
            copy_files(&files, Path::new(&format!("{}.files", image.display())));
            let db = rusqlite::Connection::open(&image).unwrap();
            let saved: Option<(String, Option<String>)> = db.query_row("SELECT status, upload_id FROM sync_upload WHERE row_id = ?1", [&id], |r| Ok((r.get(0)?, r.get(1)?))).optional().unwrap();
            let mut q = db.prepare("SELECT number FROM sync_upload_part WHERE row_id = ?1 AND etag IS NOT NULL ORDER BY number").unwrap();
            let parts: Vec<i64> = q.query_map([&id], |r| r.get(0)).unwrap().collect::<Result<_,_>>().unwrap();
            drop(q);
            let orphan: i64 = db.query_row("SELECT count(*) FROM upload_orphan WHERE upload_id LIKE '%upload-1%'", [], |r| r.get(0)).unwrap();
            if saved.is_some() { assert_eq!(orphan, 0, "WAL {k}: live upload has abort debt"); }
            if let Some(upload) = saved.as_ref().and_then(|(_, upload)| upload.as_ref()) { assert!(upload.contains("upload-1"), "WAL {k}: wrong upload id {upload}"); }
            if !parts.is_empty() { assert!(saved.as_ref().and_then(|(_, upload)| upload.as_ref()).is_some(), "WAL {k}: parts without upload id"); }
            drop(db);
            let reopened = Stand::prepare_online(Some(image.to_string_lossy().into_owned()), credentials.clone(), None, false).await;
            reopened.settle().await;
            if let Some((status, _)) = saved {
                reopened.answer_multipart();
                for part in 1..=2 {
                    if !parts.contains(&part) {
                        reopened.answer_next("MultipartUploadPresignParts", json!({"data":{"multipartUploadPresign":{"parts":[{"partNumber":part,"url":format!("https://storage/{part}")}]}}}));
                        reopened.script_put(200, Some("etag"));
                    }
                }
                reopened.answer("MomentCreateFile", json!({"momentCreate":{"moment":super::stand::moment("server", 1)}}));
                reopened.database.set_online(true).unwrap();
                reopened.settle().await;
                if let Some(row) = reopened.database.peek_row(reopened.database.session_epoch(), "Moment".into(), id.clone()).unwrap() {
                    if row.uploads.iter().any(|upload| upload.upload.status == crate::ffi::wire::WireUploadStatus::Failed) {
                        bound(reopened.database.retry(reopened.database.session_epoch(), "Moment".into(), id.clone(), Some("create_file".into()), None)).await.unwrap();
                        reopened.settle().await;
                    }
                }
                for put in reopened.put_files() { assert!(!parts.contains(&((put.offset / part_size) as i64 + 1)), "WAL {k}: confirmed part was resent"); }
                assert_eq!(reopened.puts(), 2 - parts.len(), "WAL {k}: remaining parts");
                assert_eq!(reopened.requests("MultipartUploadAbort"), 0, "WAL {k}");
                assert!(reopened.database.peek_row(reopened.database.session_epoch(), "Moment".into(), "server".into()).unwrap().is_some(), "discarded={discarded}, WAL {k}: status={status}, parts={parts:?}, requests={:?}, final mutation did not land", reopened.asked());
            } else if discarded {
                assert_eq!(orphan, 1, "WAL {k}: discarded upload has no abort debt");
            }
            bound(reopened.database.close_database()).await.unwrap();
        }
        bound(stand.database.close_database()).await.unwrap();
    }
}
