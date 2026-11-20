use std::sync::Arc;

use super::crash_image_support::advancing_by;
use super::rig_tests::bound;
use super::upload_stand::{clip, shows, watched, Answer, Stand, BLOB, CREATE_MOMENT};
use crate::ffi::wire::{WireRow, WireSyncState};
use crate::ffi::{Database, DbError};
use crate::owners::write::upload::MEBIBYTE;
use crate::test_wait::settled;

const ROW: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m1","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;
const EXPIRY_OPERATIONS: [(&str, &str); 2] = [("CreateBlob", "mutation CreateBlob { createBlob { signedId } }"), ("CreateMoment", "mutation CreateMoment { createMoment { id } }")];

#[tokio::test(flavor = "multi_thread")]
async fn a_refused_upload_mutation_retries_without_uploading_the_file_again() {
    for failure in [Answer::Data(r#"{"errors":[{"message":"Publication rejected"}]}"#.into()), Answer::Status(422)] {
        for action in [None, Some("upload_clip".to_string())] {
            let stand = Stand::new();
            let database = stand.open(None, &[CREATE_MOMENT]).await;
            let directory = tempfile::tempdir().unwrap();
            let source = directory.path().join("photo.jpg");
            std::fs::write(&source, b"photo").unwrap();
            stand.script.data("CreateBlob", BLOB);
            let refusal = std::sync::Arc::new(super::upload_stand::Hold::default());
            stand.script.enqueue("CreateMoment", Answer::Held(refusal.clone(), Box::new(failure.clone())));
            let epoch = database.session_epoch();
            let sent = bound(database.send(clip("Moment", r#"{"text":"photo"}"#, "upload_clip", &source, epoch))).await.unwrap();
            let id = sent.optimistic_value.as_ref().unwrap().id.clone();
            let stream = database.subscribe_values(epoch, crate::ffi::ReadRequest::Entity { model: "Moment".into(), id: id.clone(), fetch: crate::ffi::ReadFetch::Never });
            let original = tokio::spawn(crate::runtime::counted(Some(database.life.launch()), {
                let database = database.clone();
                async move { database.confirmed(sent, false).await }
            }));
            refusal.entered().await;
            refusal.release();
            loop {
                let rows = match bound(stream.pull_next()).await.unwrap() {
                    crate::ffi::ReadValue::Snapshot { rows, .. } => rows,
                    crate::ffi::ReadValue::Delta { edits, .. } => crate::ffi::RowEdit::carried(&edits),
                    _ => continue,
                };
                if rows.iter().any(|row| row.sync.state == WireSyncState::Failed || row.uploads.iter().any(|entry| entry.upload.status == crate::ffi::WireUploadStatus::Failed)) { break; }
            }
            stand.script.data("CreateMoment", ROW);

            let retried = bound(database.retry(epoch, "Moment".into(), id, action.clone(), None)).await.unwrap();
            let confirmed = bound(database.confirmed(retried, false)).await.unwrap();

            let refused = bound(original).await.unwrap();
            assert!(matches!(refused, Err(DbError::Rejected { .. })), "{action:?}: {refused:?}");
            assert_eq!(confirmed.value.unwrap().id, "m1", "{action:?}");
            assert_eq!((stand.script.count("CreateBlob"), stand.puts.bodies.lock().unwrap().len(), stand.script.count("CreateMoment")), (1, 1, 2), "{action:?}");
            bound(database.close_database()).await.unwrap();
        }
    }
}

async fn row(database: &Database, id: &str) -> Option<WireRow> { bound(database.read_row(database.session_epoch(), "Moment".into(), id.into())).await.unwrap() }

async fn until_state(database: &Database, id: &str, wanted: WireSyncState) {
    let rows = watched(database, "Moment", id);
    settled(&database, "the row reaches the state", || shows(&rows, wanted)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn out_response_limit_keeps_pending() {
    let mut stand = Stand::new().keep_moment();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("queue.sqlite");
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
    let clock = crate::ffi::TestClock::new(now);
    stand.rig.server = crate::transport::encrypted::tests::fake_server_tests::FakeServer::new(clock.manual());
    let database = stand.open_with(Some(&path), &[CREATE_MOMENT], Some(clock.clone())).await;
    let limit = crate::transport::MAX_HTTP_RESPONSE_BYTES;
    for _ in 0..50 { stand.script.enqueue("CreateMoment", Answer::ResponseTooLarge(limit)); }
    let receipt = bound(database.send(super::upload_stand::plain("Moment", r#"{"text":"held"}"#, database.session_epoch()))).await.unwrap();
    let operation = receipt.operation.unwrap();
    let db = database.clone();
    let waiting = tokio::spawn(crate::runtime::counted(Some(database.life.launch()), async move { db.confirmed(receipt, false).await }));
    let result = advancing_by(&clock, 60_000, waiting).await.unwrap();
    assert!(matches!(result, Err(DbError::Rejected { kept: true, ref failure }) if failure.kind == "response_too_large" && failure.response_limit == Some(limit)), "{result:?}");
    let pairs = stand.script.operation_pairs("CreateMoment");
    assert!(pairs.len() >= 2 && !pairs[0].0.is_empty() && pairs[0].1.is_some());
    assert!(pairs.iter().all(|pair| pair == &pairs[0]));
    assert!(crate::ffi::tests::queue_view_tests::queued(&database).iter().any(|entry| entry.id.0 == operation));
    database.close_now();
}

/// xfer_prop_failure_carries_step_error (P-XFER-39, D-147): a step refused with a GraphQL error fails the upload with that error, the outcome of the send shows the same one.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_prop_failure_carries_step_error() {
    let stand = Stand::new();
    let database = stand.open(None, &EXPIRY_OPERATIONS).await;
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("video");
    std::fs::write(&source, b"video").unwrap();
    stand.script.data("CreateBlob", r#"{"errors":[{"message":"BLOB_REFUSED","extensions":{"code":"BLOB_REFUSED"}}]}"#);

    let sent = bound(database.send(clip("Moment", r#"{"text":"video"}"#, "upload_clip", &source, database.session_epoch()))).await.unwrap();
    let id = sent.optimistic_value.clone().unwrap().id;
    assert!(matches!(bound(database.confirmed(sent, false)).await, Err(DbError::Rejected { failure, kept: true }) if failure.message == "BLOB_REFUSED" && failure.kind == "rejected"));

    until_state(&database, &id, WireSyncState::Failed).await;
    let failed = row(&database, &id).await.unwrap();
    let upload = &failed.uploads.iter().find(|entry| entry.action == "upload_clip").unwrap().upload;
    let error = upload.error.as_ref().expect("the failed upload shows its error");
    assert_eq!((error.message.as_str(), error.kind.as_str()), ("BLOB_REFUSED", "rejected"));
    assert_eq!(error.extensions.as_ref().and_then(|extensions| extensions.code.as_deref()), Some("BLOB_REFUSED"));
    database.close_now();
}

/// xfer_prop_step_root_declared (P-XFER-40, D-148): the root of the answer of a step is the `root:` of `uploads do`, else the root of the registered operation.
#[tokio::test(flavor = "multi_thread")]
async fn xfer_prop_step_root_declared() {
    let nested = r#"{"data":{"a":{"b":{"url":"https://storage.test/nested","headers":{},"signedId":"signed"}}}}"#;
    let flat = r#"{"data":{"a":{"url":"https://storage.test/flat","headers":{},"signedId":"signed"}}}"#;
    let missing = r#"{"data":{"a":{"b":{"signedId":"signed"}}}}"#;
    let operations = [("CreateBlob", "mutation CreateBlob { a { url signedId } }"), CREATE_MOMENT];
    for (root, answer, put, failure) in [
        (Some("a.b"), nested, Some("https://storage.test/nested"), None),
        (None, flat, Some("https://storage.test/flat"), None),
        (Some("a.b"), missing, None, Some("CreateBlob: a.b has no url")),
    ] {
        let stand = Stand::new().declaring_root(root);
        let database = stand.open(None, &operations).await;
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("video");
        std::fs::write(&source, b"video").unwrap();
        stand.script.data("CreateBlob", answer);
        stand.script.data("CreateMoment", ROW);

        let sent = bound(database.send(clip("Moment", r#"{"text":"video"}"#, "upload_clip", &source, database.session_epoch()))).await.unwrap();
        let id = sent.optimistic_value.unwrap().id;
        match (put, failure) {
            (Some(url), _) => {
                settled(&database, "the file went to the url of the answer", || stand.puts.seen.lock().unwrap().iter().any(|seen| seen.url == url)).await;
            }
            (None, Some(text)) => {
                until_state(&database, &id, WireSyncState::Failed).await;
                let failed = row(&database, &id).await.unwrap();
                let upload = &failed.uploads.iter().find(|entry| entry.action == "upload_clip").unwrap().upload;
                let message = upload.error.as_ref().expect("the failed upload shows its error").message.clone();
                assert!(message.contains(text), "{message}");
                assert!(stand.puts.seen.lock().unwrap().is_empty());
            }
            _ => unreachable!(),
        }
        database.close_now();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_http_refusal_of_a_photo_keeps_the_failed_record_until_discard() {
    let stand = Stand::new();
    let database = stand.open(None, &[CREATE_MOMENT]).await;
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("photo.jpg");
    std::fs::write(&source, b"image").unwrap();
    stand.script.data("CreateBlob", BLOB);
    stand.script.enqueue("CreateMoment", Answer::Status(422));
    let epoch = database.session_epoch();

    let receipt = bound(database.send(clip("Moment", r#"{"text":"group"}"#, "upload_clip", &source, epoch))).await.unwrap();
    let id = receipt.optimistic_value.clone().unwrap().id;
    assert!(matches!(bound(database.confirmed(receipt, false)).await, Err(DbError::Rejected { kept: true, .. })));

    assert_eq!(row(&database, &id).await.map(|row| row.sync.state), Some(WireSyncState::Failed));
    bound(database.discard(epoch, "Moment".into(), id.clone(), None)).await.unwrap();
    assert!(row(&database, &id).await.is_none());
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_offline_discard_routes_its_reopened_abort_as_a_transfer() {
    let stand = Stand::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    let source = directory.path().join("video");
    std::fs::File::create(&source).unwrap().set_len(11 * MEBIBYTE).unwrap();
    let operations = [CREATE_MOMENT];
    let database = stand.open(Some(&path), &operations).await;
    stand.script.data("InitMultipart", r#"{"data":{"initMultipart":{"multipartUpload":{"key":"key","uploadId":"upload"}}}}"#);
    let presign = stand.script.hold("PresignParts", r#"{"data":{"presignParts":{"parts":[{"partNumber":1,"url":"https://storage/1"}]}}}"#);
    let epoch = database.session_epoch();
    let sent = bound(database.send(clip("Moment", r#"{"text":"video"}"#, "upload_clip", &source, epoch))).await.unwrap();
    let id = sent.optimistic_value.unwrap().id;
    presign.entered().await;
    let files = directory.path().join("db.sqlite.files/transfers");
    assert_eq!(std::fs::read_dir(&files).unwrap().count(), 1);
    database.set_online(false).await.unwrap();
    presign.dropped().await;

    bound(database.discard(epoch, "Moment".into(), id, Some("upload_clip".into()))).await.unwrap();

    settled(&database, "the copy is gone", || std::fs::read_dir(&files).unwrap().count() == 0).await;
    assert!(source.exists());
    presign.release();
    database.close_now();
    let abort = stand.script.hold("AbortMultipart", r#"{"data":{"abortMultipart":{"ok":true}}}"#);
    let reopened: Arc<Database> = stand.open(Some(&path), &operations).await;
    abort.entered().await;
    abort.release();
    bound(crate::ffi::settled(&reopened)).await.unwrap();
    assert_eq!(stand.script.count("AbortMultipart"), 1, "the reopened abort was answered and its orphan record removed before the network flaps");
    reopened.set_online(false).await.unwrap();
    bound(crate::ffi::settled(&reopened)).await.unwrap();
    reopened.set_online(true).await.unwrap();
    bound(crate::ffi::settled(&reopened)).await.unwrap();
    assert_eq!(stand.script.names(), ["InitMultipart", "PresignParts", "PresignParts", "AbortMultipart"]);
    let asked = stand.script.variables("PresignParts");
    assert_eq!(asked[0], asked[1], "the second presign is the replay of the first");
    assert_eq!(stand.script.purposes("AbortMultipart"), [crate::transport::NetworkPurpose::Transfer]);
    reopened.close_now();
}
