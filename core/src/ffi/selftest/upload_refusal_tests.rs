use std::sync::Arc;

use super::rig_tests::bound;
use super::upload_stand::{clip, shows, watched, Answer, Stand, BLOB, CREATE_MOMENT};
use crate::ffi::wire::{WireRow, WireSyncState};
use crate::ffi::{Database, DbError, WireOperationReceipt, WireReceiptTicket};
use crate::owners::write::upload::MEBIBYTE;
use crate::test_wait::settled;

const ROW: &str = r#"{"data":{"createMoment":{"__typename":"Moment","id":"m1","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#;
const EXPIRY_OPERATIONS: [(&str, &str); 2] = [("CreateBlob", "mutation CreateBlob { createBlob { signedId } }"), ("CreateMoment", "mutation CreateMoment { createMoment { id } }")];

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
    let mut waiting = tokio::spawn(crate::runtime::counted(Some(database.life.launch()), async move { db.confirmed(receipt, false).await }));
    let mut armed = clock.armed();
    let result = loop {
        tokio::select! {
            result = &mut waiting => break result.unwrap(),
            () = clock.await_armed(armed) => { armed = clock.armed(); clock.advance(60_000); }
        }
    };
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
async fn a_complete_failure_then_retry_keeps_the_confirmed_parts() {
    let stand = Stand::new();
    let database = stand.open(None, &[CREATE_MOMENT]).await;
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("video");
    std::fs::File::create(&source).unwrap().set_len(11 * MEBIBYTE).unwrap();
    stand.script.data("InitMultipart", r#"{"data":{"initMultipart":{"key":"key","uploadId":"upload"}}}"#);
    for _ in 0..3 {
        stand.script.data("PresignParts", r#"{"data":{"presignParts":{"parts":[{"partNumber":1,"url":"https://storage/1"},{"partNumber":2,"url":"https://storage/2"},{"partNumber":3,"url":"https://storage/3"}]}}}"#);
    }
    stand.script.enqueue("CompleteMultipart", Answer::Status(422));
    stand.script.data("CompleteMultipart", r#"{"data":{"completeMultipart":{"signedId":"signed","blobId":"blob"}}}"#);
    stand.script.data("CreateMoment", ROW);
    let epoch = database.session_epoch();

    let sent = bound(database.send(clip("Moment", r#"{"text":"video"}"#, "upload_clip", &source, epoch))).await.unwrap();
    let id = sent.optimistic_value.unwrap().id;
    until_state(&database, &id, WireSyncState::Failed).await;
    let again = bound(database.retry(epoch, "Moment".into(), id.clone(), Some("upload_clip".into()), None)).await.unwrap();
    let held = &again.ticket;
    let receipt_revision = held.revision;
    let ticket = WireReceiptTicket { ticket: held.ticket.clone(), owner: held.owner.clone(), epoch: held.epoch, model: held.model.clone(), operation: held.operation, revision: held.revision, optimistic_value: None };
    let late = WireOperationReceipt { optimistic_value: None, ticket: Arc::new(ticket), ..again };
    let value = bound(database.confirmed(late, false)).await.unwrap().value;
    let shown = value.map(|row| (row.id, row.sync.state));
    if shown.is_none() {
        let found = |row: &str| crate::ffi::database::id(row).ok().map(|row| database.optimistic("Moment", &row).map(|row| row.map(|row| (row.id, row.sync.state))));
        let resolved = database.resolved(epoch, "Moment", crate::ffi::database::id(&id).unwrap()).await;
        let published = database.db.published_revision().load(std::sync::atomic::Ordering::Acquire);
        panic!("the confirmed outcome carried no row: temporary {id} resolved {resolved:?}, find(temporary) {:?}, find(resolved) {:?}, find(m1) {:?}, published revision {published}, receipt revision {}", found(&id), resolved.as_ref().ok().and_then(|row| found(row.as_str())), found("m1"), receipt_revision);
    }
    assert_eq!(shown, Some(("m1".to_string(), WireSyncState::Synced)));

    let names = stand.script.names();
    let count = |name: &str| names.iter().filter(|seen| *seen == name).count();
    assert_eq!((count("InitMultipart"), count("PresignParts"), count("CompleteMultipart")), (1, 3, 2));
    let mut urls: Vec<String> = stand.puts.seen.lock().unwrap().iter().map(|put| put.url.clone()).collect();
    urls.sort();
    assert_eq!(urls, ["https://storage/1", "https://storage/2", "https://storage/3"]);
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
    stand.script.data("InitMultipart", r#"{"data":{"initMultipart":{"key":"key","uploadId":"upload"}}}"#);
    let presign = stand.script.hold("PresignParts", r#"{"data":{"presignParts":{"parts":[{"partNumber":1,"url":"https://storage/1"}]}}}"#);
    let epoch = database.session_epoch();
    let sent = bound(database.send(clip("Moment", r#"{"text":"video"}"#, "upload_clip", &source, epoch))).await.unwrap();
    let id = sent.optimistic_value.unwrap().id;
    presign.entered().await;
    let files = directory.path().join("db.sqlite.files/transfers");
    assert_eq!(std::fs::read_dir(&files).unwrap().count(), 1);
    database.set_online(false).unwrap();
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
    reopened.set_online(false).unwrap();
    bound(crate::ffi::settled(&reopened)).await.unwrap();
    reopened.set_online(true).unwrap();
    bound(crate::ffi::settled(&reopened)).await.unwrap();
    assert_eq!(stand.script.names(), ["InitMultipart", "PresignParts", "PresignParts", "AbortMultipart"]);
    let asked = stand.script.variables("PresignParts");
    assert_eq!(asked[0], asked[1], "the second presign is the replay of the first");
    assert_eq!(stand.script.purposes("AbortMultipart"), [crate::transport::NetworkPurpose::Transfer]);
    reopened.close_now();
}
