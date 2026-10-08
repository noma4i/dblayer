use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};

use crate::commit::{CommitEnvelope, OperationStatus, OperationTransition, WriteOrigin};
use crate::identity::RowId;
use crate::pacing::Backoff;
use crate::syncstate::{FailureKind, OperationFailure, SyncState};
use crate::transport::{PutReply, Reply, TransportError};
use crate::upload::{UploadConfig, UploadInput, UploadPart, UploadScenario, UploadStage, UploadState, UploadStatus, UploadSteps};

use super::super::rig::*;
use super::super::{OperationDef, SyncConfig};

fn rid(text: &str) -> RowId {
    RowId::new(text).unwrap()
}

fn small() -> UploadConfig {
    UploadConfig {
        multipart_threshold: 10,
        part_size: 5,
        parallel_parts: 2,
        backoff: Backoff::new(Duration::from_secs(1), Duration::from_secs(15)),
        ..UploadConfig::default()
    }
}

fn upload_rig(config: UploadConfig) -> Rig {
    let rig = rig_with(SyncConfig { upload: config, ..SyncConfig::default() });
    for (name, root) in [
        ("CreateBlob", "createBlob"),
        ("InitMultipart", "initMultipart"),
        ("PresignParts", "presignParts"),
        ("CompleteMultipart", "completeMultipart"),
        ("AbortMultipart", "abortMultipart"),
    ] {
        rig.sync.register(OperationDef::new(name, "operation x { x }", root));
    }
    put(&rig, "moments", moment("m1", "x"));
    put(&rig, "moments", moment("m2", "y"));
    rig
}

fn steps() -> UploadSteps {
    UploadSteps::new("CreateMoment").single("CreateBlob").multipart("InitMultipart", "PresignParts", "CompleteMultipart", "AbortMultipart")
}

fn file(bytes: &[u8]) -> tempfile::NamedTempFile {
    let mut file = tempfile::Builder::new().suffix(".mp4").tempfile().unwrap();
    file.write_all(bytes).unwrap();
    file
}

fn input(file: &tempfile::NamedTempFile) -> UploadInput {
    UploadInput::new(file.path().to_str().unwrap(), steps()).variable("text", json!("clip"))
}

fn blob() -> Reply {
    Reply::data(json!({"createBlob": {"url": "https://s3/single", "headers": {"x-a": "1"}, "signedId": "sid", "blobId": "b1"}}))
}

fn init() -> Reply {
    Reply::data(json!({"initMultipart": {"key": "k1", "uploadId": "u1"}}))
}

fn presign(urls: &[(u32, &str)]) -> Reply {
    let parts: Vec<Value> = urls.iter().map(|(number, url)| json!({"partNumber": number, "url": url})).collect();
    Reply::data(json!({"presignParts": {"parts": parts}}))
}

fn complete() -> Reply {
    Reply::data(json!({"completeMultipart": {"signedId": "sid", "blobId": "b1"}}))
}

fn abort() -> Reply {
    Reply::data(json!({"abortMultipart": {"ok": true}}))
}

fn created(id: &str) -> Reply {
    Reply::data(json!({"createMoment": {"id": id, "text": "clip", "userId": null}}))
}

fn multipart_replies(rig: &Rig) {
    rig.transport.enqueue("InitMultipart", init());
    rig.transport.enqueue("PresignParts", presign(&[(1, "https://s3/p1"), (2, "https://s3/p2"), (3, "https://s3/p3")]));
    rig.transport.enqueue("CompleteMultipart", complete());
    rig.transport.enqueue("AbortMultipart", abort());
    rig.transport.enqueue("CreateMoment", created("m9"));
}

fn single_replies(rig: &Rig) {
    rig.transport.enqueue("CreateBlob", blob());
    rig.transport.enqueue("CreateMoment", created("m9"));
}

fn variables(rig: &Rig, operation: &str) -> Vec<Value> {
    rig.transport.calls().into_iter().filter(|call| call.operation_name == operation).map(|call| call.variables).collect()
}

fn upload_of(rig: &Rig, id: &str) -> Option<crate::upload::UploadInfo> {
    rig.db.upload_info("moments", &rid(id)).unwrap()
}

fn stored_state(rig: &Rig) -> UploadState {
    rig.db.outbox().unwrap().into_iter().find_map(|entry| entry.operation.upload).unwrap()
}

const TWELVE: &[u8] = b"0123456789ab";

#[tokio::test(start_paused = true)]
async fn single_upload_creates_puts_and_sends_the_mutation_once() {
    let rig = upload_rig(small());
    single_replies(&rig);
    let source = file(b"abcdef");
    rig.sync.upload("moments", &rid("m1"), input(&source)).await.unwrap();
    rig.sync.wait_uploads().await;
    let puts = rig.transport.put_calls();
    assert_eq!((puts.len(), puts[0].url.as_str(), puts[0].body.as_slice()), (1, "https://s3/single", b"abcdef".as_slice()));
    assert_eq!(puts[0].headers, [("x-a".to_string(), "1".to_string())]);
    let create = &variables(&rig, "CreateBlob")[0];
    assert_eq!((create["filename"].as_str().unwrap().ends_with(".mp4"), &create["contentType"], &create["byteSize"]), (true, &json!("video/mp4"), &json!(6)));
    assert_eq!(create["checksum"], json!("6AtQFwmJUPxYqtg8jBSXjg=="));
    assert_eq!(variables(&rig, "CreateMoment"), [json!({"text": "clip", "signedId": "sid", "blobId": "b1"})]);
    assert!(rig.db.outbox().unwrap().is_empty());
    assert_eq!(upload_of(&rig, "m1"), None);
    assert_eq!(text_of(&rig, "moments", "m9"), Some("clip".to_string()));
}

#[tokio::test(start_paused = true)]
async fn observers_and_reads_see_the_upload_state_and_bytes_while_it_runs() {
    let rig = upload_rig(small());
    single_replies(&rig);
    rig.transport.enqueue_put("https://s3/single", PutReply::etag("e").after(Duration::from_secs(4)));
    type Seen = Vec<Option<(UploadStatus, UploadStage, u64)>>;
    let seen: Arc<Mutex<Seen>> = Arc::default();
    let sink = seen.clone();
    let _observer = rig
        .db
        .observe_row("moments", &rid("m1"), move |change| {
            let upload = change.rows.first().and_then(|row| row.upload()).map(|upload| (upload.status, upload.stage, upload.sent_bytes));
            sink.lock().unwrap().push(upload);
        })
        .unwrap();
    let source = file(b"abcdef");
    rig.sync.upload("moments", &rid("m1"), input(&source)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    rig.db.settle();
    let running = upload_of(&rig, "m1").unwrap();
    assert_eq!((running.status, running.stage, running.total_bytes), (UploadStatus::Uploading, UploadStage::Put, 6));
    assert!(running.sent_bytes >= 1 && running.fraction() > 0.0);
    assert_eq!(rig.db.sync_info("moments", &rid("m1")).unwrap().state, SyncState::Sending);
    let last = seen.lock().unwrap().last().cloned().unwrap().unwrap();
    assert_eq!((last.0, last.1), (UploadStatus::Uploading, UploadStage::Put));
    rig.sync.wait_uploads().await;
    rig.db.settle();
    assert_eq!(seen.lock().unwrap().last().cloned().unwrap(), None);
}

#[tokio::test(start_paused = true)]
async fn multipart_runs_init_presign_parts_and_complete_in_part_order() {
    let rig = upload_rig(small());
    multipart_replies(&rig);
    for url in ["https://s3/p1", "https://s3/p2", "https://s3/p3"] {
        rig.transport.enqueue_put(url, PutReply::etag(&format!("e-{url}")).after(Duration::from_secs(2)));
    }
    let source = file(TWELVE);
    rig.sync.upload("moments", &rid("m1"), input(&source)).await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(variables(&rig, "InitMultipart").len(), 1);
    assert_eq!(variables(&rig, "PresignParts"), [json!({"key": "k1", "uploadId": "u1", "partNumbers": [1, 2, 3]})]);
    let mut bodies: Vec<_> = rig.transport.put_calls().into_iter().map(|call| (call.url, call.body)).collect();
    bodies.sort();
    assert_eq!(
        bodies,
        [
            ("https://s3/p1".to_string(), b"01234".to_vec()),
            ("https://s3/p2".to_string(), b"56789".to_vec()),
            ("https://s3/p3".to_string(), b"ab".to_vec()),
        ]
    );
    let sent = &variables(&rig, "CompleteMultipart")[0];
    assert_eq!(
        sent["parts"],
        json!([{"partNumber": 1, "etag": "e-https://s3/p1"}, {"partNumber": 2, "etag": "e-https://s3/p2"}, {"partNumber": 3, "etag": "e-https://s3/p3"}])
    );
    assert_eq!(rig.transport.parallel_puts_peak(), 2);
    assert_eq!(variables(&rig, "CreateMoment").len(), 1);
    assert_eq!(rig.transport.call_count("AbortMultipart"), 0);
    assert!(rig.db.outbox().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn one_parallel_part_means_sequential_mode() {
    let rig = upload_rig(UploadConfig { parallel_parts: 1, ..small() });
    multipart_replies(&rig);
    for url in ["https://s3/p1", "https://s3/p2", "https://s3/p3"] {
        rig.transport.enqueue_put(url, PutReply::etag("e").after(Duration::from_secs(2)));
    }
    let source = file(TWELVE);
    rig.sync.upload("moments", &rid("m1"), input(&source)).await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.parallel_puts_peak(), 1);
    assert_eq!(rig.transport.put_calls().len(), 3);
}

#[tokio::test(start_paused = true)]
async fn a_fatal_part_aborts_the_multipart_and_retry_starts_over_without_the_mutation_in_between() {
    let rig = upload_rig(small());
    multipart_replies(&rig);
    rig.transport.enqueue_put("https://s3/p2", PutReply::status(400));
    rig.transport.enqueue_put("https://s3/p2", PutReply::etag("e2"));
    let source = file(TWELVE);
    rig.sync.upload("moments", &rid("m1"), input(&source)).await.unwrap();
    rig.sync.wait_uploads().await;
    let failed = upload_of(&rig, "m1").unwrap();
    assert_eq!((failed.status, failed.stage), (UploadStatus::Failed, UploadStage::Part));
    assert!(!failed.is_retryable());
    assert_eq!(variables(&rig, "AbortMultipart"), [json!({"key": "k1", "uploadId": "u1"})]);
    assert_eq!(rig.transport.call_count("CreateMoment"), 0);
    let state = stored_state(&rig);
    assert_eq!((state.upload_id, state.parts.len(), state.sent_bytes), (None, 0, 0));
    rig.sync.retry("moments", &rid("m1")).await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.call_count("InitMultipart"), 2);
    assert_eq!(rig.transport.call_count("CompleteMultipart"), 1);
    assert_eq!(rig.transport.call_count("CreateMoment"), 1);
    assert!(rig.db.outbox().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn transient_errors_back_off_and_retry_the_put() {
    let rig = upload_rig(small());
    single_replies(&rig);
    rig.transport.enqueue_put("https://s3/single", PutReply::status(503));
    rig.transport.enqueue_put("https://s3/single", PutReply::etag("e"));
    let source = file(b"abcdef");
    let started = tokio::time::Instant::now();
    rig.sync.upload("moments", &rid("m1"), input(&source)).await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.put_count("https://s3/single"), 2);
    assert!(started.elapsed() >= Duration::from_secs(1));
    assert_eq!(rig.transport.call_count("CreateMoment"), 1);
}

#[tokio::test(start_paused = true)]
async fn running_out_of_attempts_fails_with_a_retryable_error_and_the_attempt_count() {
    let rig = upload_rig(UploadConfig { backoff: Backoff::new(Duration::from_secs(1), Duration::from_secs(15)).limited(2), ..small() });
    single_replies(&rig);
    rig.transport.enqueue_put("https://s3/single", PutReply::status(503));
    let source = file(b"abcdef");
    rig.sync.upload("moments", &rid("m1"), input(&source)).await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.put_count("https://s3/single"), 3);
    let failed = upload_of(&rig, "m1").unwrap();
    assert_eq!((failed.status, failed.stage, failed.attempts), (UploadStatus::Failed, UploadStage::Put, 2));
    assert_eq!(failed.error.as_ref().map(|error| error.kind), Some(FailureKind::Server));
    assert!(failed.is_retryable());
    assert_eq!(rig.transport.call_count("CreateMoment"), 0);
}

#[tokio::test(start_paused = true)]
async fn offline_waits_without_spending_attempts() {
    let rig = upload_rig(UploadConfig { backoff: Backoff::new(Duration::from_secs(1), Duration::from_secs(15)).limited(0), ..small() });
    single_replies(&rig);
    rig.sync.set_online(false);
    let source = file(b"abcdef");
    rig.sync.upload("moments", &rid("m1"), input(&source)).await.unwrap();
    tokio::time::sleep(Duration::from_secs(100)).await;
    assert_eq!(rig.transport.call_count("CreateBlob"), 0);
    let waiting = upload_of(&rig, "m1").unwrap();
    assert_eq!((waiting.status, waiting.attempts), (UploadStatus::Uploading, 0));
    rig.sync.set_online(true);
    rig.sync.wait_uploads().await;
    assert!(rig.db.outbox().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_network_error_while_offline_is_not_an_attempt() {
    let rig = upload_rig(UploadConfig { backoff: Backoff::new(Duration::from_secs(1), Duration::from_secs(15)).limited(0), ..small() });
    single_replies(&rig);
    rig.transport.enqueue_put("https://s3/single", PutReply::fail(TransportError::Network("lost".into())).after(Duration::from_secs(8)));
    rig.transport.enqueue_put("https://s3/single", PutReply::etag("e"));
    let source = file(b"abcdef");
    rig.sync.upload("moments", &rid("m1"), input(&source)).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    rig.sync.set_online(false);
    tokio::time::sleep(Duration::from_secs(60)).await;
    let waiting = upload_of(&rig, "m1").unwrap();
    assert_eq!((waiting.status, waiting.attempts), (UploadStatus::Uploading, 0));
    rig.sync.set_online(true);
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.put_count("https://s3/single"), 2);
    assert!(rig.db.outbox().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn an_expired_part_url_gets_a_fresh_presign_for_that_part_only() {
    let rig = upload_rig(UploadConfig { parallel_parts: 1, ..small() });
    rig.transport.enqueue("InitMultipart", init());
    rig.transport.enqueue("PresignParts", presign(&[(1, "https://s3/p1"), (2, "https://s3/p2"), (3, "https://s3/p3")]));
    rig.transport.enqueue("PresignParts", presign(&[(2, "https://s3/p2b")]));
    rig.transport.enqueue("CompleteMultipart", complete());
    rig.transport.enqueue("CreateMoment", created("m9"));
    rig.transport.enqueue_put("https://s3/p2", PutReply::status(403));
    let source = file(TWELVE);
    rig.sync.upload("moments", &rid("m1"), input(&source)).await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(variables(&rig, "PresignParts")[1], json!({"key": "k1", "uploadId": "u1", "partNumbers": [2]}));
    assert_eq!((rig.transport.put_count("https://s3/p2"), rig.transport.put_count("https://s3/p2b")), (1, 1));
    assert_eq!(rig.transport.call_count("CreateMoment"), 1);
}

#[tokio::test(start_paused = true)]
async fn urls_older_than_the_presign_ttl_are_requested_again_for_unfinished_parts() {
    let rig = upload_rig(UploadConfig { parallel_parts: 1, ..small() });
    rig.transport.enqueue("InitMultipart", init());
    rig.transport.enqueue("PresignParts", presign(&[(1, "https://s3/p1"), (2, "https://s3/p2"), (3, "https://s3/p3")]));
    rig.transport.enqueue("PresignParts", presign(&[(2, "https://s3/p2b"), (3, "https://s3/p3b")]));
    rig.transport.enqueue("CompleteMultipart", complete());
    rig.transport.enqueue("CreateMoment", created("m9"));
    rig.transport.enqueue_put("https://s3/p1", PutReply::etag("e1").after(Duration::from_secs(8)));
    let source = file(TWELVE);
    rig.sync.upload("moments", &rid("m1"), input(&source)).await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    rig.clock.advance(Duration::from_secs(51 * 60));
    rig.sync.wait_uploads().await;
    assert_eq!(variables(&rig, "PresignParts")[1]["partNumbers"], json!([2, 3]));
    assert_eq!((rig.transport.put_count("https://s3/p2"), rig.transport.put_count("https://s3/p2b"), rig.transport.put_count("https://s3/p3b")), (0, 1, 1));
}

#[tokio::test(start_paused = true)]
async fn an_unanswered_mutation_is_delivery_unknown_never_repeated_and_retry_skips_the_upload() {
    let rig = upload_rig(small());
    rig.transport.enqueue("CreateBlob", blob());
    rig.transport.enqueue("CreateMoment", Reply::fail(TransportError::Interrupted("reset".into())));
    rig.transport.enqueue("CreateMoment", created("m9"));
    let source = file(b"abcdef");
    rig.sync.upload("moments", &rid("m1"), input(&source)).await.unwrap();
    rig.sync.wait_uploads().await;
    rig.sync.flush().await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.call_count("CreateMoment"), 1);
    let unknown = upload_of(&rig, "m1").unwrap();
    assert_eq!((unknown.status, unknown.stage), (UploadStatus::DeliveryUnknown, UploadStage::Mutation));
    let state = stored_state(&rig);
    assert_eq!((state.signed_id.as_deref(), state.blob_id.as_deref()), (Some("sid"), Some("b1")));
    rig.sync.retry("moments", &rid("m1")).await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!((rig.transport.call_count("CreateBlob"), rig.transport.put_calls().len(), rig.transport.call_count("CreateMoment")), (1, 1, 2));
    assert!(rig.db.outbox().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn an_unreached_mutation_waits_pending_and_is_sent_again_without_a_new_transfer() {
    let rig = upload_rig(small());
    rig.transport.enqueue("CreateBlob", blob());
    rig.transport.enqueue("CreateMoment", Reply::fail(TransportError::Network("no connection".into())));
    rig.transport.enqueue("CreateMoment", created("m9"));
    let source = file(b"abcdef");
    rig.sync.upload("moments", &rid("m1"), input(&source)).await.unwrap();
    rig.sync.wait_uploads().await;
    let waiting = rig.db.sync_info("moments", &rid("m1")).unwrap();
    assert_eq!((waiting.state, waiting.attempts), (crate::syncstate::SyncState::Pending, 1));
    assert_eq!(rig.transport.call_count("CreateMoment"), 1);
    rig.clock.advance(Duration::from_secs(2));
    rig.sync.flush().await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!((rig.transport.call_count("CreateBlob"), rig.transport.put_calls().len(), rig.transport.call_count("CreateMoment")), (1, 1, 2));
    assert!(rig.db.outbox().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn the_mutation_waits_offline_without_a_call_and_goes_out_when_online() {
    let rig = upload_rig(small());
    single_replies(&rig);
    rig.transport.enqueue_put("https://s3/single", PutReply::etag("e").after(Duration::from_secs(8)));
    let source = file(b"abcdef");
    rig.sync.upload("moments", &rid("m1"), input(&source)).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    rig.sync.set_online(false);
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert_eq!(rig.transport.call_count("CreateMoment"), 0);
    assert_eq!(rig.db.sync_info("moments", &rid("m1")).unwrap().attempts, 0);
    rig.sync.set_online(true);
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.call_count("CreateMoment"), 1);
    assert!(rig.db.outbox().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_rejected_mutation_fails_without_repeating_and_discard_removes_the_operation() {
    let rig = upload_rig(small());
    rig.transport.enqueue("CreateBlob", blob());
    rig.transport.enqueue("CreateMoment", Reply::graphql_error("nope", "FORBIDDEN"));
    let source = file(b"abcdef");
    rig.sync.upload("moments", &rid("m1"), input(&source)).await.unwrap();
    rig.sync.wait_uploads().await;
    let failed = upload_of(&rig, "m1").unwrap();
    assert_eq!((failed.status, failed.stage, failed.is_retryable()), (UploadStatus::Failed, UploadStage::Mutation, false));
    assert_eq!(rig.transport.call_count("CreateMoment"), 1);
    assert_eq!(rig.db.discard("moments", &rid("m1")).unwrap(), 1);
    assert_eq!(upload_of(&rig, "m1"), None);
    assert!(rig.db.outbox().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn cancel_stops_the_transfer_aborts_the_multipart_and_skips_the_mutation() {
    let rig = upload_rig(small());
    multipart_replies(&rig);
    for url in ["https://s3/p1", "https://s3/p2", "https://s3/p3"] {
        rig.transport.enqueue_put(url, PutReply::etag("e").after(Duration::from_secs(60)));
    }
    let source = file(TWELVE);
    rig.sync.upload("moments", &rid("m1"), input(&source)).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(rig.sync.cancel_upload("moments", &rid("m1")).await.unwrap());
    assert_eq!(variables(&rig, "AbortMultipart"), [json!({"key": "k1", "uploadId": "u1"})]);
    assert_eq!(rig.transport.call_count("CreateMoment"), 0);
    assert!(rig.db.outbox().unwrap().is_empty());
    assert_eq!(upload_of(&rig, "m1"), None);
    rig.sync.wait_uploads().await;
    assert!(!rig.sync.cancel_upload("moments", &rid("m1")).await.unwrap());
}

#[tokio::test(start_paused = true)]
async fn files_at_a_time_are_limited_and_the_waiting_one_is_queued() {
    let rig = upload_rig(small());
    single_replies(&rig);
    rig.transport.enqueue_put("https://s3/single", PutReply::etag("e").after(Duration::from_secs(10)));
    let (first, second) = (file(b"abcdef"), file(b"ghijkl"));
    rig.sync.upload("moments", &rid("m1"), input(&first)).await.unwrap();
    rig.sync.upload("moments", &rid("m2"), input(&second)).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(upload_of(&rig, "m1").unwrap().status, UploadStatus::Uploading);
    assert_eq!(upload_of(&rig, "m2").unwrap().status, UploadStatus::Queued);
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.parallel_puts_peak(), 1);
    assert_eq!(rig.transport.call_count("CreateMoment"), 2);

    let wide = upload_rig(UploadConfig { max_files: 2, ..small() });
    single_replies(&wide);
    wide.transport.enqueue_put("https://s3/single", PutReply::etag("e").after(Duration::from_secs(10)));
    wide.sync.upload("moments", &rid("m1"), input(&first)).await.unwrap();
    wide.sync.upload("moments", &rid("m2"), input(&second)).await.unwrap();
    wide.sync.wait_uploads().await;
    assert_eq!(wide.transport.parallel_puts_peak(), 2);
}

#[tokio::test(start_paused = true)]
async fn a_second_upload_for_the_same_record_is_refused() {
    let rig = upload_rig(small());
    single_replies(&rig);
    let source = file(b"abcdef");
    rig.sync.upload("moments", &rid("m1"), input(&source)).await.unwrap();
    assert!(rig.sync.upload("moments", &rid("m1"), input(&source)).await.is_err());
    rig.sync.wait_uploads().await;
}

#[tokio::test(start_paused = true)]
async fn an_unreadable_source_fails_at_prepare_without_touching_the_transport() {
    let rig = upload_rig(small());
    rig.sync.upload("moments", &rid("m1"), UploadInput::new("/nonexistent/clip.mp4", steps())).await.unwrap();
    rig.sync.wait_uploads().await;
    let failed = upload_of(&rig, "m1").unwrap();
    assert_eq!((failed.status, failed.stage, failed.is_retryable()), (UploadStatus::Failed, UploadStage::Prepare, false));
    assert!(rig.transport.calls().is_empty());
}

fn resumable(parts: Vec<UploadPart>) -> UploadState {
    UploadState {
        phase: UploadStatus::Uploading,
        stage: UploadStage::Part,
        scenario: Some(UploadScenario::Multipart),
        total_bytes: 12,
        sent_bytes: 5,
        key: Some("k1".to_string()),
        upload_id: Some("u1".to_string()),
        parts,
        ..UploadState::queued()
    }
}

#[tokio::test(start_paused = true)]
async fn a_stored_multipart_resumes_from_its_finished_parts_after_a_restart() {
    let rig = upload_rig(small());
    rig.transport.enqueue("PresignParts", presign(&[(2, "https://s3/p2"), (3, "https://s3/p3")]));
    rig.transport.enqueue("CompleteMultipart", complete());
    rig.transport.enqueue("CreateMoment", created("m9"));
    let source = file(TWELVE);
    let state = resumable(vec![UploadPart { number: 1, etag: "e1".to_string() }]);
    rig.sync.inner.begin_upload("moments", &rid("m1"), &input(&source), state).unwrap();
    let report = rig.sync.flush().await.unwrap();
    assert!(report.operations.is_empty());
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.call_count("InitMultipart"), 0);
    assert_eq!(variables(&rig, "PresignParts"), [json!({"key": "k1", "uploadId": "u1", "partNumbers": [2, 3]})]);
    assert_eq!(rig.transport.put_calls().len(), 2);
    assert_eq!(variables(&rig, "CompleteMultipart")[0]["parts"][0], json!({"partNumber": 1, "etag": "e1"}));
    assert!(rig.db.outbox().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn an_orphaned_multipart_of_a_failed_upload_is_aborted_on_start() {
    let rig = upload_rig(small());
    rig.transport.enqueue("AbortMultipart", abort());
    let source = file(TWELVE);
    let seq = rig.sync.inner.begin_upload("moments", &rid("m1"), &input(&source), resumable(Vec::new())).unwrap();
    let mut envelope = CommitEnvelope::new(WriteOrigin::Response, rig.db.generation());
    envelope.operation_transitions.push(OperationTransition::Fail {
        seq,
        status: OperationStatus::Failed,
        failure: OperationFailure::new(FailureKind::Network, "abort lost"),
    });
    rig.db.shared.store.commit(envelope).unwrap();
    rig.sync.flush().await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(variables(&rig, "AbortMultipart"), [json!({"key": "k1", "uploadId": "u1"})]);
    assert_eq!(stored_state(&rig).upload_id, None);
    assert_eq!(upload_of(&rig, "m1").unwrap().status, UploadStatus::Failed);
}
