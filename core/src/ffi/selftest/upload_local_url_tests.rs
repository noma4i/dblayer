use std::path::Path;
use std::sync::Arc;

use super::rig_tests::bound;
use super::upload_prepare_tests::{probed_image, send, shrink, PREPARED, SOURCE};
use super::upload_stage_tests::{answers, COMPLETE, LANDED, OPERATIONS as STAGE_OPERATIONS};
use super::upload_batch_shift_tests::{blob, OPERATIONS};
use super::upload_stand::{request, ScriptedPreparer, Stand, BLOB};
use crate::ffi::upload_wire::{WireSource, WireUploadBatch, WireUploadItem, WireUploadRequest};
use crate::ffi::tape::Rows;
use crate::ffi::wire::WireUploadStatus;
use crate::ffi::{Database, ReadRequest, ReadValue};

const ASSETS: &str = r#"{"data":{"createAssets":{"assets":[{"id":"a0","clientKey":"key-0","size":3,"position":0},{"id":"a1","clientKey":"key-1","size":3,"position":1},{"id":"a2","clientKey":"key-2","size":3,"position":2}]}}}"#;

#[derive(Clone, Copy, Debug)]
enum Entry {
    Upload,
    Batch,
    Stage,
}

/// What the places show of the uploads of a call: the file URL and whether the status is `sending`. The rows are read the way a window reads them, the staged file (no row yet) through the stream of its key.
struct Places {
    entry: Entry,
}

impl Places {
    fn from_rows(&self, database: &Database, ids: &[String]) -> Vec<(Option<String>, bool)> {
        let (model, action) = if matches!(self.entry, Entry::Batch) { ("Asset", "create_files") } else { ("Moment", "upload_clip") };
        ids.iter().filter_map(|id| database.peek_row_now(database.session_epoch(), model.into(), id.clone()).unwrap()).filter_map(|row| row.uploads.into_iter().find(|upload| upload.action == action)).map(|upload| (upload.upload.local_url.clone(), upload.upload.status == WireUploadStatus::Sending)).collect()
    }

    async fn wait(&self, database: &Database, ids: &[String], what: &str, ready: impl Fn(&[(Option<String>, bool)]) -> bool) -> Vec<(Option<String>, bool)> {
        if matches!(self.entry, Entry::Stage) && database.peek_row(database.session_epoch(), "Moment".into(), ids[0].clone()).await.unwrap().is_none() {
            let stream = database.subscribe_values(database.session_epoch(), ReadRequest::Blob { model: "Moment".into(), id: ids[0].clone(), action: "upload_clip".into() });
            loop {
                let Some(ReadValue::Status { upload, .. }) = bound(stream.pull_next()).await else { panic!("{what}: the stream ended") };
                let shown = vec![(upload.local_url, upload.status == crate::ffi::WireUploadStatus::Sending)];
                if ready(&shown) { return shown; }
            }
        }
        crate::test_wait::settled_showing(database, what, || format!(" {:?}", self.from_rows(database, ids)), || ready(&self.from_rows(database, ids))).await;
        self.from_rows(database, ids)
    }
}

/// Every upload of the call shows a file URL whose file exists and is not empty.
async fn assert_url(database: &Database, places: &Places, ids: &[String], point: &str, prepared: bool) -> Vec<String> {
    let entry = places.entry;
    let count = ids.len();
    let shown = places.wait(database, ids, point, |shown| shown.len() == count).await;
    shown.into_iter().map(|(url, _)| {
        let url = url.unwrap_or_else(|| panic!("{entry:?} prepared={prepared} {point}: localUrl is None"));
        let path = url.strip_prefix("file://").unwrap();
        assert!(std::fs::metadata(path).is_ok_and(|meta| meta.len() > 0), "{entry:?} prepared={prepared} {point}: no file at {url}");
        url
    }).collect()
}

fn source_files(directory: &Path, count: usize) -> Vec<std::path::PathBuf> {
    (0..count).map(|index| {
        let path = directory.join(format!("{index}.jpg"));
        std::fs::write(&path, SOURCE).unwrap();
        path
    }).collect()
}

fn remove_all(paths: &[std::path::PathBuf]) { for path in paths { std::fs::remove_file(path).unwrap(); } }

fn file_source(path: &Path, prepared: bool) -> WireSource {
    WireSource::File { path: path.to_str().unwrap().into(), name: None, mime: None, prepare: prepared.then(shrink), max_bytes: None }
}

/// P-XFER-23 (D-386): `localUrl` of every upload of a call (a single upload, each item of a batch, a staged file), with and without the prepare step, is the URL of a readable file from the return of the call to the end of the transfer, while the file of the caller is already deleted. Input: the file of the caller deleted at once; the points: call returned, prepare running, prepare ended with the PUT held, the final mutation held (`sending`), the end. Assert: at each point every upload shows a URL with a non-empty file; after the end no URL. Catches: a status whose URL depends on the written size or on the copy of the source that the prepare step removes.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_prop_local_url() {
    for entry in [Entry::Upload, Entry::Batch, Entry::Stage] {
        for prepared in [false, true] {
            let preparer = ScriptedPreparer::new(probed_image(), PREPARED, &[]);
            let stand = if prepared { Stand::new().with_preparer(preparer.clone()) } else { Stand::new() };
            let operations: Vec<(&str, &str)> = if matches!(entry, Entry::Batch) { OPERATIONS.to_vec() } else { STAGE_OPERATIONS.to_vec() };
            let directory = tempfile::tempdir().unwrap();
            let database = stand.open(Some(&directory.path().join("db.sqlite")), &operations).await;
            let epoch = database.session_epoch();
            let sources = source_files(directory.path(), if matches!(entry, Entry::Batch) { 3 } else { 1 });
            let (run_hold, put_hold) = (preparer.hold_next(), stand.puts.hold_next());
            let mut mutation = None;
            let frames = Arc::new(Rows::default());
            let _watch = database.watch_query(&format!(r#"{{"model":"{}"}}"#, if matches!(entry, Entry::Batch) { "Asset" } else { "Moment" }), frames.clone());
            bound(_watch.wait_for_initial_load()).await.unwrap();
            let call: tokio::task::JoinHandle<(Vec<String>, Vec<_>, Option<String>)> = match entry {
                Entry::Upload => {
                    stand.script.data("CreateBlob", BLOB);
                    mutation = Some(stand.script.hold("CreateMoment", LANDED));
                    let (database, source) = (database.clone(), sources[0].clone());
                    tokio::spawn(async move {
                        let receipt = bound(database.send(if prepared { send(epoch, &source, shrink()) } else { super::upload_stand::clip("Moment", r#"{"text":"A"}"#, "upload_clip", &source, epoch) })).await.unwrap();
                        (vec![receipt.optimistic_value.as_ref().unwrap().id.clone()], vec![receipt], None)
                    })
                }
                Entry::Batch => {
                    for index in 0..3 { stand.script.data("CreateBlob", &blob(index)); }
                    mutation = Some(stand.script.hold("CreateAssets", ASSETS));
                    let items: Vec<WireUploadItem> = sources.iter().enumerate().map(|(index, path)| WireUploadItem { values_json: format!(r#"{{"clientKey":"key-{index}","size":3,"position":{index}}}"#), source: file_source(path, prepared), variables_json: "{}".into() }).collect();
                    let database = database.clone();
                    tokio::spawn(async move {
                        let batch = bound(database.upload_create_batch(epoch, "Asset".into(), items, WireUploadBatch { operation: "create_files".into(), variables_json: "{}".into() })).await.unwrap();
                        (batch.items.iter().map(|item| item.optimistic_value.id.clone()).collect(), batch.items.into_iter().map(|item| item.receipt).collect(), None)
                    })
                }
                Entry::Stage => {
                    answers(&stand);
                    stand.script.data("CompleteMultipart", COMPLETE);
                    let upload = if prepared { WireUploadRequest { source: file_source(&sources[0], true), variables_json: "{}".into() } } else { request(&sources[0]) };
                    let database = database.clone();
                    tokio::spawn(async move {
                        let staged = bound(database.stage(epoch, "Moment".into(), "upload_clip".into(), upload)).await.unwrap();
                        (vec![staged.row_id.clone()], vec![staged.receipt], Some(staged.row_id))
                    })
                }
            };
            let places = Places { entry };
            let (ids, mut receipts, commit) = call.await.unwrap();
            remove_all(&sources);
            assert_url(&database, &places, &ids, "the call returned", prepared).await;
            if prepared {
                run_hold.entered().await;
                assert_url(&database, &places, &ids, "the prepare runs", prepared).await;
                run_hold.release();
            }
            put_hold.entered().await;
            assert_url(&database, &places, &ids, "the PUT is held", prepared).await;
            put_hold.release();
            if let Some(row) = &commit {
                crate::test_wait::settled(&database, "the parts are on the server", || stand.puts.seen.lock().unwrap().len() == 1).await;
                mutation = Some(stand.script.hold("CreateMoment", LANDED));
                let committed = bound(database.commit_staged(epoch, "Moment".into(), row.clone(), "upload_clip".into(), r#"{"text":"hi"}"#.into(), "{}".into())).await.unwrap();
                receipts.push(committed);
            }
            places.wait(&database, &ids, "every upload waits for the final mutation", |shown| shown.len() == ids.len() && shown.iter().all(|(_, sending)| *sending)).await;
            let kept = assert_url(&database, &places, &ids, "sending", prepared).await;
            mutation.unwrap().release();
            for receipt in receipts { bound(database.confirmed(receipt, false)).await.unwrap(); }
            places.wait(&database, &ids, "the transfers end", |shown| shown.iter().all(|(url, _)| url.is_none())).await;
            assert!(kept.iter().all(|url| !Path::new(url.strip_prefix("file://").unwrap()).exists()), "{entry:?} prepared={prepared}: the file is removed with the transfer");
            let frames_seen = frames.1.lock().unwrap();
            let shown: Vec<_> = frames_seen.iter().flat_map(|frame| frame.rows.iter()).flat_map(|row| row.uploads.iter()).collect();
            assert!(!shown.is_empty(), "{entry:?} prepared={prepared}: the frames show the upload");
            for upload in shown { assert!(upload.upload.local_url.is_some(), "{entry:?} prepared={prepared}: a frame shows the upload {:?} without localUrl", upload.upload.status); }
            drop(frames_seen);
            database.close_now();
        }
    }
}

fn readable_url(database: &Database, model: &str, id: &str, action: &str) -> Option<Vec<u8>> {
    let row = database.peek_row_now(database.session_epoch(), model.into(), id.into()).unwrap()?;
    let url = row.uploads.into_iter().find(|upload| upload.action == action)?.upload.local_url?;
    std::fs::read(url.strip_prefix("file://").unwrap()).ok()
}

/// P-XFER-23 (D-386): a supplied source and a retry with a new source show the file of the library from the return of their call, with the file of the caller already deleted; a row that waits for its source shows none.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_local_url_after_supply_source_and_retry_with_source() {
    let directory = tempfile::tempdir().unwrap();
    let file = |name: &str, bytes: &[u8]| { let path = directory.path().join(name); std::fs::write(&path, bytes).unwrap(); path };
    let source_of = |path: &Path| crate::ffi::upload_wire::WireUploadSource { path: path.to_str().unwrap().into(), name: None, mime: None, values_json: "{}".into(), variables_json: "{}".into(), prepare: None, max_bytes: None };

    let stand = Stand::new();
    let database = stand.open(Some(&directory.path().join("supply.sqlite")), &OPERATIONS).await;
    let epoch = database.session_epoch();
    let items = (0..2).map(|index| WireUploadItem { values_json: format!(r#"{{"clientKey":"key-{index}","size":2,"position":{index}}}"#), source: WireSource::Pending, variables_json: "{}".into() }).collect();
    let batch = bound(database.upload_create_batch(epoch, "Asset".into(), items, WireUploadBatch { operation: "create_files".into(), variables_json: "{}".into() })).await.unwrap();
    let ids: Vec<String> = batch.items.iter().map(|item| item.optimistic_value.id.clone()).collect();
    assert_eq!(readable_url(&database, "Asset", &ids[0], "create_files"), None, "a row that waits for its source shows no file");
    stand.script.data("CreateBlob", &blob(0));
    let put = stand.puts.hold_next();
    let supplied = file("supplied", b"supplied bytes");
    bound(database.supply_source(epoch, "Asset".into(), ids[0].clone(), "create_files".into(), source_of(&supplied))).await.unwrap();
    std::fs::remove_file(&supplied).unwrap();
    assert_eq!(readable_url(&database, "Asset", &ids[0], "create_files"), Some(b"supplied bytes".to_vec()), "the call returned");
    put.entered().await;
    assert_eq!(readable_url(&database, "Asset", &ids[0], "create_files"), Some(b"supplied bytes".to_vec()), "the PUT is held");
    assert_eq!(readable_url(&database, "Asset", &ids[1], "create_files"), None, "the other item still waits");
    put.release();
    database.close_now();

    let stand = Stand::new();
    let database = stand.open(Some(&directory.path().join("retry.sqlite")), &STAGE_OPERATIONS).await;
    let epoch = database.session_epoch();
    stand.script.enqueue("CreateBlob", super::upload_stand::Answer::Status(422));
    let first = file("first", b"first bytes");
    let id = bound(database.send(super::upload_stand::clip("Moment", r#"{"text":"A"}"#, "upload_clip", &first, epoch))).await.unwrap().optimistic_value.unwrap().id;
    let rows = super::upload_stand::watched(&database, "Moment", &id);
    crate::test_wait::settled(&database, "the upload failed", || super::upload_stand::shows(&rows, crate::ffi::wire::WireSyncState::Failed)).await;
    stand.script.data("CreateBlob", BLOB);
    let put = stand.puts.hold_next();
    let second = file("second", b"second bytes");
    bound(database.retry(epoch, "Moment".into(), id.clone(), Some("upload_clip".into()), Some(source_of(&second)))).await.unwrap();
    std::fs::remove_file(&second).unwrap();
    assert_eq!(readable_url(&database, "Moment", &id, "upload_clip"), Some(b"second bytes".to_vec()), "the retry returned");
    put.entered().await;
    assert_eq!(readable_url(&database, "Moment", &id, "upload_clip"), Some(b"second bytes".to_vec()), "the PUT of the new file is held");
    put.release();
    database.close_now();
}
